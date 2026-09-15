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

var (
	ErrInternal = errors.New("internal error")
)

var byteBufferPool = sync.Pool{
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
	rpChan := make(chan protocol.Packet, 1)

	go func() {
		var header [protocol.PacketHeaderSize]byte
		var err error = nil
		readerBuf := byteBufferPool.Get().(*bytes.Buffer)

		defer func() {
			b.closeBuffer(readerBuf)
			close(rpChan)
		}()

		for {
			_, err = io.ReadFull(conn.nConn, header[:])
			if err != nil {
				readerBuf.Reset()
				conn.writePacket(readerBuf, protocol.NewErrPacket(err))
				b.disconnect(conn)
				return
			}

			plen := binary.BigEndian.Uint32(header[2:])
			// TODO cap plen; a huge length can OOM.
			// TODO ReadFull into a per-conn []byte; CopyN allocates a scratch buffer then copies into readerBuf.

			readerBuf.Grow(int(plen))
			n, err := io.CopyN(readerBuf, conn.nConn, int64(plen))
			if err != nil || n != int64(plen) {
				readerBuf.Reset()
				conn.writePacket(readerBuf, protocol.NewErrPacket(err))
				b.disconnect(conn)
				return
			}

			pBytes := bytes.Clone(readerBuf.Bytes())
			// TODO drop Clone: ping-pong two payload buffers (or two Packets) so Handle can keep the bytes while the reader fills the other.

			p := protocol.Packet{}
			p.Version = header[0]
			p.Type = protocol.PacketType(header[1])
			p.PayloadLength = plen
			err = p.DeserializePayload(pBytes)
			if err != nil {
				readerBuf.Reset()
				conn.writePacket(readerBuf, protocol.NewErrPacket(err))
				b.disconnect(conn)
				return
			}

			// TODO send *Packet, not a copy of Blocks slice headers, once packets are reused per conn.
			rpChan <- p
			readerBuf.Reset()
		}
	}()

	writerPacket := &protocol.Packet{}
	writerBuffer := byteBufferPool.Get().(*bytes.Buffer)
	defer func() {
		b.closeBuffer(writerBuffer)
		b.disconnect(conn)
	}()

	for {
		select {
			case <-conn.ctx.Done():
				return
			case packet := <-rpChan:
				err := b.handler.Handle(&packet, writerPacket, conn.ctx)
				if err != nil {
					conn.writePacket(writerBuffer, protocol.NewErrPacket(err))
					b.disconnect(conn)
					return
				}
				err = conn.writePacket(writerBuffer, writerPacket)
				if err != nil {
					b.disconnect(conn)
					return
				}

				// Reset the writer packet.
				{
					writerPacket.Type = 0
					writerPacket.Version = 0
					writerPacket.PayloadLength = 0
					clear(writerPacket.Blocks[:])
				}
		}
	}
}

func (b *UnixSocketBackend) closeBuffer(buf *bytes.Buffer) {
	const mb = 1024 * 1024

	buf.Reset()
	if buf.Cap() < mb {
		byteBufferPool.Put(buf)
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
