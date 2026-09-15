package app

import (
	"bytes"
	"context"
	"encoding/binary"
	"glock/internal/protocol"
	"glock/internal/transport"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func startAppBackend(t *testing.T, a *App) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "glock.sock")
	b := transport.NewUnixSocketBackend(path)
	errCh := make(chan error, 1)
	go func() {
		errCh <- b.Start(a)
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

	waitUnixListening(t, path)
	return path
}

func waitUnixListening(t *testing.T, path string) {
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

func dialApp(t *testing.T, path string) net.Conn {
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

func writeAppPacket(t *testing.T, c net.Conn, p *protocol.Packet) {
	t.Helper()
	var buf bytes.Buffer
	p.Serialize(&buf)
	if _, err := c.Write(buf.Bytes()); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func readAppPacket(t *testing.T, c net.Conn) protocol.Packet {
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

func tryLockSuccess(p protocol.Packet) bool {
	if p.Type != protocol.PacketTypeSuccess {
		return false
	}
	success, ok := p.FindBlock(protocol.PayloadBlockTypeSuccess)
	return ok && len(success.Value) > 0 && success.Value[0] == 1
}

func TestApp_DisconnectReleasesLock(t *testing.T) {
	a := newTestApp(t)
	path := startAppBackend(t, a)

	holder := dialApp(t, path)
	writeAppPacket(t, holder, pktLock("default", "res"))
	locked := readAppPacket(t, holder)
	if locked.Type != protocol.PacketTypeSuccess {
		t.Fatalf("lock type: got %d", locked.Type)
	}
	if _, ok := blockString(&locked, protocol.PayloadBlockTypeSecret); !ok {
		t.Fatal("expected secret")
	}

	waiter := dialApp(t, path)
	writeAppPacket(t, waiter, pktTryLock("default", "res", time.Minute))
	busy := readAppPacket(t, waiter)
	if tryLockSuccess(busy) {
		t.Fatal("expected lock to be held while holder is connected")
	}

	if err := holder.Close(); err != nil {
		t.Fatalf("close holder: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for {
		if err := waiter.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatalf("deadline: %v", err)
		}
		writeAppPacket(t, waiter, pktTryLock("default", "res", time.Minute))
		free := readAppPacket(t, waiter)
		if tryLockSuccess(free) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("lock was not released after client disconnect")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
