package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"glock/internal/protocol"
	"io"
	"net"
	"os"
	"sync"
)

var packetBufferPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 512))
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
	// TODO unlink a leftover socket path so Start works after a crash.
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

func (b *UnixSocketBackend) disconnect(conn *Conn) {
	b.connMu.Lock()
	defer b.connMu.Unlock()
	conn.Close()
	delete(b.conns, conn)
}

func (b *UnixSocketBackend) handleConn(conn *Conn) {
	var header [protocol.PacketHeaderSize]byte

	reqBuf := packetBufferPool.Get().(*bytes.Buffer)
	respBuf := packetBufferPool.Get().(*bytes.Buffer)

	defer func() {
		b.disconnect(conn)

		reqBuf.Reset()
		respBuf.Reset()

		// TODO check buffers size and put back if it's too small
		packetBufferPool.Put(reqBuf)
		packetBufferPool.Put(respBuf)
	}()

	rq := protocol.Packet{}
	rp := protocol.Packet{}

	for {
		_, err := io.ReadFull(conn.nConn, header[:])
		if err != nil {
			return
		}

		version := header[0]
		pType := header[1]
		pLen := binary.BigEndian.Uint32(header[2:])
		// TODO cap payload length; a huge pLen can OOM.

		reqBuf.Grow(int(pLen))
		n, err := io.CopyN(reqBuf, conn.nConn, int64(pLen))
		if err != nil || n != int64(pLen) {
			conn.writePacket(
				respBuf,
				protocol.NewErrPacket(errors.Join(err, protocol.ErrInvalidPacketLength)),
			)
			return
		}

		rq.Version = version
		rq.Type = protocol.PacketType(pType)
		rq.PayloadLength = pLen

		rp.Version = protocol.ProtoVersion

		err = rq.DeserializePayload(reqBuf.Bytes())
		if err != nil {
			conn.writePacket(respBuf, protocol.NewErrPacket(err))
			return
		}

		/**
		Handle is responsible for writing the response packet to the response buffer.
		If it returns an error, the connection is closed with error packet.
		*/
		err = b.handler.Handle(&rq, &rp, conn.ctx)
		if err != nil {
			conn.writePacket(respBuf, protocol.NewErrPacket(err))
			return
		}

		if err := conn.writePacket(respBuf, &rp); err != nil {
			return
		}

		reqBuf.Reset()
		respBuf.Reset()
		rq.Reset()
		rp.Reset()
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
