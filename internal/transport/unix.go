package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"glock/internal/protocol"
	"io"
	"net"
	"os"
	"sync"
)

const (
	maxPacketSize = 10 * 1024 * 1024 // 10MB
	maxBufferSize = 1 * 1024 // 1KB
)

var (
	ErrInternal = errors.New("internal error")
)

var byteBufferPool = sync.Pool{
	New: func() any {
		return make([]byte, 128)
	},
}

type UnixSocketBackend struct {
	socketPath string
	handler    Handler

	listener net.Listener
	conns    map[*Conn]struct{}
	connMu   sync.Mutex
}

func NewUnixSocketBackend(socketPath string) *UnixSocketBackend {
	return &UnixSocketBackend{
		socketPath: socketPath,
		conns:      make(map[*Conn]struct{}),
		connMu:     sync.Mutex{},
	}
}

func (b *UnixSocketBackend) Start(handler Handler) error {
	listener, err := net.Listen("unix", b.socketPath)
	if err != nil {
		return err
	}

	b.connMu.Lock()
	b.listener = listener
	b.handler = handler
	b.connMu.Unlock()
	defer listener.Close()

	for {
		c, err := listener.Accept()
		if err != nil {
			return err
		}

		ctx, cancel := context.WithCancel(context.Background())
		conn := &Conn{
			ctx:    ctx,
			nConn:  c,
			cancel: cancel,
		}

		b.connMu.Lock()
		b.conns[conn] = struct{}{}
		b.connMu.Unlock()

		conn.ctx = b.handler.OnConnect(ctx)

		go b.handleConn(conn)
	}
}

func (b *UnixSocketBackend) disconnect(conn *Conn, cv *sync.Cond) {
	b.connMu.Lock()
	conn.Close()
	delete(b.conns, conn)
	b.connMu.Unlock()

	cv.Broadcast()
}

func (b *UnixSocketBackend) handleConn(conn *Conn) {
	var mu sync.Mutex
	var cv = sync.NewCond(&mu)
	var buf = byteBufferPool.Get().([]byte)
	var writeBuf = byteBufferPool.Get().([]byte)

	pending := true
	handling := false

	readerPacket := &protocol.Packet{}
	writerPacket := &protocol.Packet{}

	defer func() {
		if cap(buf) < maxBufferSize {
			byteBufferPool.Put(buf[:0])
		}
		if cap(writeBuf) < maxBufferSize {
			byteBufferPool.Put(writeBuf[:0])
		}
	}()

	go func() {
		var header [protocol.PacketHeaderSize]byte

		for {
			_, err := io.ReadFull(conn.nConn, header[:])
			if err != nil {
				b.disconnect(conn, cv)
				return
			}

			mu.Lock()
			if handling || conn.ctx.Err() != nil {
				b.disconnect(conn, cv)
				mu.Unlock()
				return
			}

			for !pending && conn.ctx.Err() == nil {
				cv.Wait()
			}

			if conn.ctx.Err() != nil {
				mu.Unlock()
				return
			}

			plen := binary.BigEndian.Uint32(header[2:])
			if plen > maxPacketSize {
				readerPacket.Reset()
				readerPacket.LoadError(protocol.ErrPacketTooLarge)
				conn.writePacket(&writeBuf, readerPacket)
				b.disconnect(conn, cv)
				mu.Unlock()
				return
			}

			if cap(buf) < int(plen) {
				buf = make([]byte, plen)
			} else {
				buf = buf[:plen]
			}

			n, err := io.ReadFull(conn.nConn, buf)
			if n != int(plen) || err != nil {
				readerPacket.Reset()
				readerPacket.LoadError(protocol.ErrInvalidPacket)
				conn.writePacket(&writeBuf, readerPacket)
				b.disconnect(conn, cv)
				mu.Unlock()
				return
			}

			readerPacket.Reset()
			readerPacket.Version = header[0]
			readerPacket.Type = protocol.PacketType(header[1])
			readerPacket.PayloadLength = plen
			if err = readerPacket.DeserializePayload(buf[:n]); err != nil {
				readerPacket.Reset()
				readerPacket.LoadError(err)
				conn.writePacket(&writeBuf, readerPacket)
				b.disconnect(conn, cv)
				mu.Unlock()
				return
			}
			handling = true
			pending = false
			cv.Broadcast()
			mu.Unlock()
		}
	}()

	for {
		mu.Lock()
		for !handling && conn.ctx.Err() == nil {
			cv.Wait()
		}

		if conn.ctx.Err() != nil {
			mu.Unlock()
			return
		}

		writerPacket.Reset()
		err := b.handler.Handle(readerPacket, writerPacket, conn.ctx)
		if err != nil {
			writerPacket.Reset()
			writerPacket.LoadError(ErrInternal)
			conn.writePacket(&writeBuf, writerPacket)
			b.disconnect(conn, cv)
			mu.Unlock()
			return
		}

		handling = false

		err = conn.writePacket(&writeBuf, writerPacket)
		if err != nil {
			b.disconnect(conn, cv)
			mu.Unlock()
			return
		}

		pending = true
		cv.Broadcast()
		mu.Unlock()
	}
}

func (b *UnixSocketBackend) Shutdown(ctx context.Context) error {
	b.connMu.Lock()
	listener := b.listener
	b.connMu.Unlock()
	if listener != nil {
		listener.Close()
	}

	b.connMu.Lock()
	defer b.connMu.Unlock()

	var shutdownErr error
	for conn := range b.conns {
		select {
		case <-ctx.Done():
			shutdownErr = ctx.Err()
		default:
			conn.Close()
		}
	}

	if err := os.Remove(b.socketPath); err != nil && !os.IsNotExist(err) {
		if shutdownErr == nil {
			shutdownErr = err
		}
	}

	return shutdownErr
}
