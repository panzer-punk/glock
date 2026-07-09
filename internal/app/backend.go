package app

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"slices"
	"sync"
)

type Backend interface {
	Start() error
	Shutdown(context.Context) error
}

type UnixSocketBackend struct {
	socketPath string
	app        *App

	stopCh   chan struct{}
	listener net.Listener
	conns    []net.Conn
	connMu   sync.Mutex
}

type CommandResult struct {
	Success bool
	Error string
	Data map[string]any
}

func NewUnixSocketBackend(socketPath string, app *App) *UnixSocketBackend {
	return &UnixSocketBackend{
		socketPath: socketPath,
		app:        app,
		stopCh:     make(chan struct{}, 1),
	}
}

func (b *UnixSocketBackend) Start() error {
	listener, err := net.Listen("unix", b.socketPath)
	if err != nil {
		return err
	}
	b.listener = listener
	defer listener.Close()

	for {
		select {
		case <-b.stopCh:
			return errors.New("stopped")
		default:
			conn, err := listener.Accept()
			if err != nil {
				return err
			}

			func () {
				b. connMu.Lock()
				defer b.connMu.Unlock()
				b.conns = append(b.conns, conn)
			}()

			go b.handleConn(conn)
		}
	}
}

func (b *UnixSocketBackend) handleConn(conn net.Conn) {
	defer func() {
		conn.Close()
		b.connMu.Lock()
		defer b.connMu.Unlock()
		b.conns = slices.DeleteFunc(b.conns, func(c net.Conn) bool {
			return c != conn
		})
	}()

	header := make([]byte, PacketHeaderSize)

	for {
		_, err := io.ReadFull(conn, header)
		if err != nil {
			return
		}

		pLen := binary.BigEndian.Uint32(header[ProtoVersionHeaderSize+PacketTypeHeaderSize:])
		payload := make([]byte, pLen)
		_, err = io.ReadFull(conn, payload)
		if err != nil {
			return
		}

		packet := Packet{}
		packet.Version = header[0]
		packet.Type = PacketType(header[1])
		packet.PayloadLength = pLen

		err = packet.DeserializePayload(payload)
		if err != nil {
			return
		}

		response := b.app.HandlePacket(&packet, context.Background())
		conn.Write(response.Serialize())
	}
}

func (b *UnixSocketBackend) Shutdown(ctx context.Context) error {
	select {
	case b.stopCh <- struct{}{}:
	default:
	}

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