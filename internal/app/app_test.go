package app

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"glock/internal/lock"
	"glock/internal/protocol"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func seqSecretFactory() lock.SecretFactory {
	var n atomic.Uint64
	return func() string {
		return fmt.Sprintf("sec-%d", n.Add(1))
	}
}

func newTestApp(t *testing.T) *App {
	t.Helper()
	factory := seqSecretFactory()
	lm := lock.NewLockService()
	if err := lm.AddNamespace("default", 16, factory); err != nil {
		t.Fatalf("add default namespace: %v", err)
	}
	if err := lm.AddNamespace("other", 16, factory); err != nil {
		t.Fatalf("add other namespace: %v", err)
	}
	return NewApp(&Config{
		DefaultNamespace: "default",
		LockManager:      lm,
	})
}

func connectedCtx(t *testing.T, a *App) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return a.OnConnect(ctx)
}

func pktLock(ns, key string) *protocol.Packet {
	p := protocol.NewPacket(protocol.PacketTypeLock)
	if ns != "" {
		p.AddBlock(protocol.NewPayloadBlock(protocol.PayloadBlockTypeNamespace, []byte(ns)))
	}
	if key != "" {
		p.AddBlock(protocol.NewPayloadBlock(protocol.PayloadBlockTypeKey, []byte(key)))
	}
	return &p
}

func pktTryLock(ns, key string, ttl time.Duration) *protocol.Packet {
	p := protocol.NewPacket(protocol.PacketTypeTryLock)
	if ns != "" {
		p.AddBlock(protocol.NewPayloadBlock(protocol.PayloadBlockTypeNamespace, []byte(ns)))
	}
	if key != "" {
		p.AddBlock(protocol.NewPayloadBlock(protocol.PayloadBlockTypeKey, []byte(key)))
	}
	if ttl > 0 {
		raw := make([]byte, 8)
		binary.BigEndian.PutUint64(raw, uint64(ttl))
		p.AddBlock(protocol.NewPayloadBlock(protocol.PayloadBlockTypeTTL, raw))
	}
	return &p
}

func pktLockTTL(ns, key string, ttl time.Duration) *protocol.Packet {
	p := pktLock(ns, key)
	if ttl > 0 {
		raw := make([]byte, 8)
		binary.BigEndian.PutUint64(raw, uint64(ttl))
		p.AddBlock(protocol.NewPayloadBlock(protocol.PayloadBlockTypeTTL, raw))
	}
	return p
}

func pktUnlock(ns, key, secret string) *protocol.Packet {
	p := protocol.NewPacket(protocol.PacketTypeUnlock)
	if ns != "" {
		p.AddBlock(protocol.NewPayloadBlock(protocol.PayloadBlockTypeNamespace, []byte(ns)))
	}
	if key != "" {
		p.AddBlock(protocol.NewPayloadBlock(protocol.PayloadBlockTypeKey, []byte(key)))
	}
	if secret != "" {
		p.AddBlock(protocol.NewPayloadBlock(protocol.PayloadBlockTypeSecret, []byte(secret)))
	}
	return &p
}

func callHandle(a *App, rq *protocol.Packet, ctx context.Context) (*protocol.Packet, error) {
	var rp protocol.Packet
	rp.Reset()
	err := a.Handle(rq, &rp, ctx)
	return &rp, err
}

func blockString(p *protocol.Packet, tp protocol.PayloadBlockType) (string, bool) {
	b, ok := p.FindBlock(tp)
	if !ok {
		return "", false
	}
	return string(b.Value), true
}

func TestApp_HandleSessionNotFound(t *testing.T) {
	a := newTestApp(t)
	p := pktLock("default", "k")
	_, err := callHandle(a, p, context.Background())
	if !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("expected ErrSessionNotFound, got %v", err)
	}
}

func TestApp_LockMissingKey(t *testing.T) {
	a := newTestApp(t)
	ctx := connectedCtx(t, a)

	rp, err := callHandle(a, pktLock("default", ""), ctx)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	if rp.Type != protocol.PacketTypeError {
		t.Fatalf("type: got %d", rp.Type)
	}
	msg, ok := blockString(rp, protocol.PayloadBlockTypeError)
	if !ok || msg != ErrMissingKey.Error() {
		t.Fatalf("error: ok=%v msg=%q", ok, msg)
	}
}

func TestApp_LockUnlock(t *testing.T) {
	a := newTestApp(t)
	ctx := connectedCtx(t, a)

	locked, err := callHandle(a, pktLock("default", "res"), ctx)
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if locked.Type != protocol.PacketTypeSuccess {
		t.Fatalf("lock type: got %d", locked.Type)
	}
	sec, ok := blockString(locked, protocol.PayloadBlockTypeSecret)
	if !ok || sec == "" {
		t.Fatalf("secret: ok=%v value=%q", ok, sec)
	}

	unlocked, err := callHandle(a, pktUnlock("default", "res", sec), ctx)
	if err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if unlocked.Type != protocol.PacketTypeSuccess {
		t.Fatalf("unlock type: got %d", unlocked.Type)
	}

	again, err := callHandle(a, pktTryLock("default", "res", time.Minute), ctx)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	success, ok := again.FindBlock(protocol.PayloadBlockTypeSuccess)
	if !ok || success.Value[0] != 1 {
		t.Fatal("expected key to be free after unlock")
	}
}

func TestApp_LockTTLExpires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := newTestApp(t)
		ctx := connectedCtx(t, a)

		if _, err := callHandle(a, pktLockTTL("default", "res", time.Minute), ctx); err != nil {
			t.Fatalf("lock: %v", err)
		}

		busy, err := callHandle(a, pktTryLock("default", "res", time.Minute), ctx)
		if err != nil {
			t.Fatalf("try lock while held: %v", err)
		}
		success, ok := busy.FindBlock(protocol.PayloadBlockTypeSuccess)
		if !ok || success.Value[0] != 0 {
			t.Fatal("expected lock to be held before ttl")
		}

		time.Sleep(time.Minute)
		synctest.Wait()

		free, err := callHandle(a, pktTryLock("default", "res", time.Minute), ctx)
		if err != nil {
			t.Fatalf("try lock after ttl: %v", err)
		}
		success, ok = free.FindBlock(protocol.PayloadBlockTypeSuccess)
		if !ok || success.Value[0] != 1 {
			t.Fatal("expected lock to expire")
		}
	})
}

func TestApp_LockDefaultNamespace(t *testing.T) {
	a := newTestApp(t)
	ctx := connectedCtx(t, a)

	locked, err := callHandle(a, pktLock("", "res"), ctx)
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	sec, ok := blockString(locked, protocol.PayloadBlockTypeSecret)
	if !ok {
		t.Fatal("expected secret")
	}

	if _, err := callHandle(a, pktUnlock("", "res", sec), ctx); err != nil {
		t.Fatalf("unlock via default namespace: %v", err)
	}
}

func TestApp_LockUnknownNamespace(t *testing.T) {
	a := newTestApp(t)
	ctx := connectedCtx(t, a)

	rp, err := callHandle(a, pktLock("missing", "res"), ctx)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	if rp.Type != protocol.PacketTypeError {
		t.Fatalf("type: got %d", rp.Type)
	}
	msg, ok := blockString(rp, protocol.PayloadBlockTypeError)
	if !ok || msg != lock.ErrNamespaceNotFound.Error() {
		t.Fatalf("error: ok=%v msg=%q", ok, msg)
	}
}

func TestApp_LockCancelledContext(t *testing.T) {
	a := newTestApp(t)
	holder := connectedCtx(t, a)
	if _, err := callHandle(a, pktLock("default", "res"), holder); err != nil {
		t.Fatalf("holder lock: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	waiter := a.OnConnect(ctx)

	_, err := callHandle(a, pktLock("default", "res"), waiter)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestApp_TryLockMissingKey(t *testing.T) {
	a := newTestApp(t)
	ctx := connectedCtx(t, a)

	rp, err := callHandle(a, pktTryLock("default", "", time.Minute), ctx)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	if rp.Type != protocol.PacketTypeError {
		t.Fatalf("type: got %d", rp.Type)
	}
}

func TestApp_TryLockBusy(t *testing.T) {
	a := newTestApp(t)
	holder := connectedCtx(t, a)
	if _, err := callHandle(a, pktLock("default", "res"), holder); err != nil {
		t.Fatalf("holder lock: %v", err)
	}

	ctx := connectedCtx(t, a)
	rp, err := callHandle(a, pktTryLock("default", "res", time.Minute), ctx)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	success, ok := rp.FindBlock(protocol.PayloadBlockTypeSuccess)
	if !ok || success.Value[0] != 0 {
		t.Fatal("expected try lock to fail while held")
	}
}

func TestApp_TryLockSuccess(t *testing.T) {
	a := newTestApp(t)
	ctx := connectedCtx(t, a)

	rp, err := callHandle(a, pktTryLock("other", "res", time.Minute), ctx)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	if rp.Type != protocol.PacketTypeSuccess {
		t.Fatalf("type: got %d", rp.Type)
	}
	success, ok := rp.FindBlock(protocol.PayloadBlockTypeSuccess)
	if !ok || success.Value[0] != 1 {
		t.Fatal("expected try lock to succeed")
	}
	if _, ok := blockString(rp, protocol.PayloadBlockTypeSecret); !ok {
		t.Fatal("expected secret")
	}
}

func TestApp_UnlockMissingSecret(t *testing.T) {
	a := newTestApp(t)
	ctx := connectedCtx(t, a)

	rp, err := callHandle(a, pktUnlock("default", "res", ""), ctx)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	if rp.Type != protocol.PacketTypeError {
		t.Fatalf("type: got %d", rp.Type)
	}
	msg, ok := blockString(rp, protocol.PayloadBlockTypeError)
	if !ok || msg != ErrMissingSecret.Error() {
		t.Fatalf("error: ok=%v msg=%q", ok, msg)
	}
}

func TestApp_UnlockWrongSecret(t *testing.T) {
	a := newTestApp(t)
	ctx := connectedCtx(t, a)
	if _, err := callHandle(a, pktLock("default", "res"), ctx); err != nil {
		t.Fatalf("lock: %v", err)
	}

	rp, err := callHandle(a, pktUnlock("default", "res", "wrong"), ctx)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	if rp.Type != protocol.PacketTypeError {
		t.Fatalf("type: got %d", rp.Type)
	}
	msg, ok := blockString(rp, protocol.PayloadBlockTypeError)
	if !ok || msg != lock.ErrInvalidSecret.Error() {
		t.Fatalf("error: ok=%v msg=%q", ok, msg)
	}

	busy, err := callHandle(a, pktTryLock("default", "res", time.Minute), ctx)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	success, ok := busy.FindBlock(protocol.PayloadBlockTypeSuccess)
	if !ok || success.Value[0] != 0 {
		t.Fatal("wrong secret must not release the lock")
	}
}

func TestApp_UnknownPacketType(t *testing.T) {
	a := newTestApp(t)
	ctx := connectedCtx(t, a)

	p := protocol.NewPacket(protocol.PacketType(99))
	rp, err := callHandle(a, &p, ctx)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	if rp.Type != protocol.PacketTypeError {
		t.Fatalf("type: got %d", rp.Type)
	}
	msg, ok := blockString(rp, protocol.PayloadBlockTypeError)
	if !ok || msg != ErrUnknownPacket.Error() {
		t.Fatalf("error: ok=%v msg=%q", ok, msg)
	}
}

func TestApp_SessionCloseReleasesLock(t *testing.T) {
	a := newTestApp(t)
	ctx, cancel := context.WithCancel(context.Background())
	sessionCtx := a.OnConnect(ctx)

	if _, err := callHandle(a, pktLock("default", "res"), sessionCtx); err != nil {
		t.Fatalf("lock: %v", err)
	}
	cancel()

	deadline := time.Now().Add(time.Second)
	other := connectedCtx(t, a)
	for {
		rp, err := callHandle(a, pktTryLock("default", "res", time.Minute), other)
		if err != nil {
			t.Fatalf("try lock: %v", err)
		}
		success, ok := rp.FindBlock(protocol.PayloadBlockTypeSuccess)
		if ok && success.Value[0] == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("lock was not released after session context cancel")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestApp_NamespacesAreIsolated(t *testing.T) {
	a := newTestApp(t)
	ctx := connectedCtx(t, a)

	if _, err := callHandle(a, pktLock("default", "res"), ctx); err != nil {
		t.Fatalf("lock default: %v", err)
	}

	rp, err := callHandle(a, pktTryLock("other", "res", time.Minute), ctx)
	if err != nil {
		t.Fatalf("try lock other: %v", err)
	}
	success, ok := rp.FindBlock(protocol.PayloadBlockTypeSuccess)
	if !ok || success.Value[0] != 1 {
		t.Fatal("expected same key to be free in another namespace")
	}
}
