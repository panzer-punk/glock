package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"glock/internal/protocol"
	"io"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

type handlerStub struct {
	onConnect func(context.Context) context.Context
	handle    func(rq *protocol.Packet, rp *protocol.Packet, ctx context.Context) error
}

func (h *handlerStub) OnConnect(ctx context.Context) context.Context {
	if h.onConnect != nil {
		return h.onConnect(ctx)
	}
	return ctx
}

func (h *handlerStub) Handle(rq *protocol.Packet, rp *protocol.Packet, ctx context.Context) error {
	if h.handle != nil {
		return h.handle(rq, rp, ctx)
	}
	rp.Type = protocol.PacketTypeSuccess
	return nil
}

func startTestBackend(t *testing.T, h Handler) (*UnixSocketBackend, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "glock.sock")
	b := NewUnixSocketBackend(path)
	errCh := make(chan error, 1)
	go func() {
		errCh <- b.Start(h)
	}()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Error("Start did not return after Shutdown")
		}
	})

	waitUntilListening(t, path)
	return b, path
}

func waitUntilListening(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.Dial("unix", path)
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("server did not start listening")
}

func dialTest(t *testing.T, path string) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := c.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func writePacket(t *testing.T, c net.Conn, p protocol.Packet) {
	t.Helper()
	var buf bytes.Buffer
	p.Serialize(&buf)
	if _, err := c.Write(buf.Bytes()); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func readPacket(t *testing.T, c net.Conn) protocol.Packet {
	t.Helper()
	var header [protocol.PacketHeaderSize]byte
	if _, err := io.ReadFull(c, header[:]); err != nil {
		t.Fatalf("read header: %v", err)
	}

	pLen := binary.BigEndian.Uint32(header[2:])
	payload := make([]byte, pLen)
	if pLen > 0 {
		if _, err := io.ReadFull(c, payload); err != nil {
			t.Fatalf("read payload: %v", err)
		}
	}

	var p protocol.Packet
	p.Version = header[0]
	p.Type = protocol.PacketType(header[1])
	p.PayloadLength = pLen
	if err := p.DeserializePayload(payload); err != nil {
		t.Fatalf("deserialize: %v", err)
	}
	return p
}

func lockRequest(key string) protocol.Packet {
	p := protocol.NewPacket(protocol.PacketTypeLock)
	p.AddBlock(protocol.NewPayloadBlock(protocol.PayloadBlockTypeKey, []byte(key)))
	return p
}

func TestUnixSocketBackend_RequestResponse(t *testing.T) {
	h := &handlerStub{
		handle: func(rq *protocol.Packet, rp *protocol.Packet, ctx context.Context) error {
			if rq.Type != protocol.PacketTypeLock {
				t.Errorf("type: got %d, want lock", rq.Type)
			}
			key, ok := rq.FindBlock(protocol.PayloadBlockTypeKey)
			if !ok || string(key.Value) != "order:1" {
				t.Errorf("key: ok=%v value=%q", ok, key.Value)
			}
			rp.Type = protocol.PacketTypeSuccess
			rp.AddBlock(protocol.NewPayloadBlock(protocol.PayloadBlockTypeSecret, []byte("sec")))
			return nil
		},
	}

	_, path := startTestBackend(t, h)
	c := dialTest(t, path)
	writePacket(t, c, lockRequest("order:1"))
	resp := readPacket(t, c)

	if resp.Type != protocol.PacketTypeSuccess {
		t.Fatalf("response type: got %d", resp.Type)
	}
	sec, ok := resp.FindBlock(protocol.PayloadBlockTypeSecret)
	if !ok || string(sec.Value) != "sec" {
		t.Fatalf("secret: ok=%v value=%q", ok, sec.Value)
	}
}

func TestUnixSocketBackend_OnConnectContext(t *testing.T) {
	type ctxKey struct{}

	h := &handlerStub{
		onConnect: func(ctx context.Context) context.Context {
			return context.WithValue(ctx, ctxKey{}, "session")
		},
		handle: func(rq *protocol.Packet, rp *protocol.Packet, ctx context.Context) error {
			if ctx.Value(ctxKey{}) != "session" {
				return errors.New("session missing from context")
			}
			rp.Type = protocol.PacketTypeSuccess
			return nil
		},
	}

	_, path := startTestBackend(t, h)
	c := dialTest(t, path)
	writePacket(t, c, lockRequest("k"))
	resp := readPacket(t, c)
	if resp.Type != protocol.PacketTypeSuccess {
		t.Fatalf("response type: got %d", resp.Type)
	}
}

func TestUnixSocketBackend_TwoRequestsSameConnection(t *testing.T) {
	var n atomic.Int32
	h := &handlerStub{
		handle: func(rq *protocol.Packet, rp *protocol.Packet, ctx context.Context) error {
			key, _ := rq.FindBlock(protocol.PayloadBlockTypeKey)
			n.Add(1)
			rp.Type = protocol.PacketTypeSuccess
			rp.AddBlock(protocol.NewPayloadBlock(protocol.PayloadBlockTypeSecret, key.Value))
			return nil
		},
	}

	_, path := startTestBackend(t, h)
	c := dialTest(t, path)

	writePacket(t, c, lockRequest("first"))
	first := readPacket(t, c)
	sec, _ := first.FindBlock(protocol.PayloadBlockTypeSecret)
	if string(sec.Value) != "first" {
		t.Fatalf("first secret: %q", sec.Value)
	}

	writePacket(t, c, lockRequest("second"))
	second := readPacket(t, c)
	sec, _ = second.FindBlock(protocol.PayloadBlockTypeSecret)
	if string(sec.Value) != "second" {
		t.Fatalf("second secret: %q", sec.Value)
	}

	if n.Load() != 2 {
		t.Fatalf("handles: got %d, want 2", n.Load())
	}
}

func TestUnixSocketBackend_InvalidPayloadClosesConnection(t *testing.T) {
	_, path := startTestBackend(t, &handlerStub{})
	c := dialTest(t, path)

	var header [protocol.PacketHeaderSize]byte
	header[0] = protocol.ProtoVersion
	header[1] = byte(protocol.PacketTypeLock)
	binary.BigEndian.PutUint32(header[2:], 1)
	if _, err := c.Write(header[:]); err != nil {
		t.Fatalf("write header: %v", err)
	}
	if _, err := c.Write([]byte{0}); err != nil {
		t.Fatalf("write payload: %v", err)
	}

	resp := readPacket(t, c)
	if resp.Type != protocol.PacketTypeError {
		t.Fatalf("expected error packet, got %d", resp.Type)
	}

	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected connection to close after invalid payload")
	}
}

func TestUnixSocketBackend_ClientDisconnectDropsConn(t *testing.T) {
	b, path := startTestBackend(t, &handlerStub{})
	c, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for {
		b.connMu.Lock()
		n := len(b.conns)
		b.connMu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected 1 tracked conn, got %d", n)
		}
		time.Sleep(5 * time.Millisecond)
	}

	c.Close()

	deadline = time.Now().Add(time.Second)
	for {
		b.connMu.Lock()
		n := len(b.conns)
		b.connMu.Unlock()
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected conn to be dropped, still %d", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestUnixSocketBackend_ShutdownClosesClients(t *testing.T) {
	path := filepath.Join(t.TempDir(), "glock.sock")
	b := NewUnixSocketBackend(path)
	errCh := make(chan error, 1)
	go func() {
		errCh <- b.Start(&handlerStub{})
	}()
	waitUntilListening(t, path)

	c, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := b.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	buf := make([]byte, 1)
	if _, err := c.Read(buf); err == nil {
		t.Fatal("expected read to fail after shutdown")
	}

	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after Shutdown")
	}
}

func TestUnixSocketBackend_StartFailsIfPathBusy(t *testing.T) {
	_, path := startTestBackend(t, &handlerStub{})
	busy := NewUnixSocketBackend(path)
	err := busy.Start(&handlerStub{})
	if err == nil {
		t.Fatal("expected listen error on busy path")
	}
}

func TestUnixSocketBackend_TwoClients(t *testing.T) {
	var n atomic.Int32
	h := &handlerStub{
		handle: func(rq *protocol.Packet, rp *protocol.Packet, ctx context.Context) error {
			n.Add(1)
			rp.Type = protocol.PacketTypeSuccess
			return nil
		},
	}

	_, path := startTestBackend(t, h)
	c1 := dialTest(t, path)
	c2 := dialTest(t, path)

	writePacket(t, c1, lockRequest("a"))
	writePacket(t, c2, lockRequest("b"))
	if readPacket(t, c1).Type != protocol.PacketTypeSuccess {
		t.Fatal("client 1")
	}
	if readPacket(t, c2).Type != protocol.PacketTypeSuccess {
		t.Fatal("client 2")
	}
	if n.Load() != 2 {
		t.Fatalf("handles: got %d, want 2", n.Load())
	}
}
