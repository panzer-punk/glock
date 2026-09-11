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
	handler Handler

	listener net.Listener
	// TODO replace with slice of pointers
	conns    map[*Conn]*Conn
	connMu   sync.Mutex
}

func NewUnixSocketBackend(socketPath string) *UnixSocketBackend {
	return &UnixSocketBackend{
		socketPath: socketPath,
		conns: make(map[*Conn]*Conn),
		connMu: sync.Mutex{},
	}
}

func (b *UnixSocketBackend) Start(handler Handler) error {
	listener, err := net.Listen("unix", b.socketPath)
	if err != nil {
		return err
	}
	b.listener = listener
	defer listener.Close()
	b.handler = handler

	for {
		c, err := listener.Accept()
		if err != nil {
			return err
		}

		ctx, cancel := context.WithCancel(context.Background())
		conn := &Conn{
			ctx: ctx,
			nConn: c,
			cancel: cancel,
		}

		b.connMu.Lock()
		b.conns[conn] = conn
		b.connMu.Unlock()

		conn.ctx = b.handler.OnConnect(ctx)
		go b.handleConn(conn)
	}
}

func (b *UnixSocketBackend) handleConn(conn *Conn) {
	defer func() {
		conn.Close()
		b.connMu.Lock()
		defer b.connMu.Unlock()
		delete(b.conns, conn)
	}()

	var header [protocol.PacketHeaderSize]byte

	reqBuf := packetBufferPool.Get().(*bytes.Buffer)
	respBuf := packetBufferPool.Get().(*bytes.Buffer)

	defer func() {
		reqBuf.Reset()
		respBuf.Reset()

		// TODO check buffers size and put back if it's too small
		packetBufferPool.Put(reqBuf)
		packetBufferPool.Put(respBuf)
	}()

	packet := protocol.Packet{}

	// TODO check err on write errors
	for {
		_, err := io.ReadFull(conn.nConn, header[:])
		if err != nil {
			return
		}

		version := header[0]
		pType := header[1]
		pLen := binary.BigEndian.Uint32(header[2:])

		reqBuf.Grow(int(pLen))
		n, err := io.CopyN(reqBuf, conn.nConn, int64(pLen))
		if err != nil || n != int64(pLen) {
			errPkt := protocol.NewErrPacket(errors.Join(err, errors.New("invalid packet length")))
			errPkt.Serialize(respBuf)
			conn.nConn.Write(respBuf.Bytes())
			return
		}

		packet.Version = version
		packet.Type = protocol.PacketType(pType)
		packet.PayloadLength = pLen
	
		err = packet.DeserializePayload(reqBuf.Bytes())
		if err != nil {
			errPkt := protocol.NewErrPacket(err)
			errPkt.Serialize(respBuf)
			conn.nConn.Write(respBuf.Bytes())
			return
		}

		resp, err := b.handler.Handle(&packet, conn.ctx)
		if err != nil {
			errPkt := protocol.NewErrPacket(err)
			errPkt.Serialize(respBuf)
			conn.nConn.Write(respBuf.Bytes())
			return
		}

		resp.Serialize(respBuf)
		_, err = conn.nConn.Write(respBuf.Bytes())
		if err != nil {
			return
		}

		reqBuf.Reset()
		respBuf.Reset()
		packet.Reset()
	}
}

func (b *UnixSocketBackend) Shutdown(ctx context.Context) error {
	if b.listener != nil {
		b.listener.Close()
	}

	b.connMu.Lock()
	defer b.connMu.Unlock()

	var shutdownErr error
	for _, conn := range b.conns {
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