package app

import (
	"context"
	"glock/internal/lock"
	"testing"
	"time"
)

func sessionLockManager(t *testing.T) *lock.LockManager {
	t.Helper()
	factory := seqSecretFactory()
	lm := lock.NewLockService()
	lm.AddNamespace("ns", 16, factory)
	return lm
}

func acquireSessionLock(t *testing.T, lm *lock.LockManager, key string) SessionLock {
	t.Helper()
	sec, err := lm.Lock("ns", key, context.Background())
	if err != nil {
		t.Fatalf("lock %q: %v", key, err)
	}
	return SessionLock{
		Namespace: "ns",
		Key:       key,
		Secret:    sec,
	}
}

func assertLockFree(t *testing.T, lm *lock.LockManager, key string, wantFree bool) {
	t.Helper()
	sec, ok := lm.TryLock("ns", key, time.Minute, context.Background())
	if ok {
		_ = lm.Unlock("ns", key, sec)
	}
	if ok != wantFree {
		t.Fatalf("key %q free=%v, want %v", key, ok, wantFree)
	}
}

func TestSession_CloseReleasesRememberedLocks(t *testing.T) {
	lm := sessionLockManager(t)
	s := NewSession()
	s.RememberLock("a", acquireSessionLock(t, lm, "a"))
	s.RememberLock("b", acquireSessionLock(t, lm, "b"))

	if err := s.Close(lm); err != nil {
		t.Fatalf("close: %v", err)
	}

	assertLockFree(t, lm, "a", true)
	assertLockFree(t, lm, "b", true)
}

func TestSession_ForgetLockSkipsRelease(t *testing.T) {
	lm := sessionLockManager(t)
	s := NewSession()
	s.RememberLock("a", acquireSessionLock(t, lm, "a"))
	s.ForgetLock("a")

	if err := s.Close(lm); err != nil {
		t.Fatalf("close: %v", err)
	}

	assertLockFree(t, lm, "a", false)
}

func TestSession_RememberLockReplacesRelease(t *testing.T) {
	lm := sessionLockManager(t)
	s := NewSession()
	s.RememberLock("slot", acquireSessionLock(t, lm, "first"))
	s.RememberLock("slot", acquireSessionLock(t, lm, "second"))

	if err := s.Close(lm); err != nil {
		t.Fatalf("close: %v", err)
	}

	assertLockFree(t, lm, "first", false)
	assertLockFree(t, lm, "second", true)
}

func TestSession_CloseIdempotent(t *testing.T) {
	lm := sessionLockManager(t)
	s := NewSession()
	s.RememberLock("a", acquireSessionLock(t, lm, "a"))

	if err := s.Close(lm); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := s.Close(lm); err != nil {
		t.Fatalf("second close: %v", err)
	}

	assertLockFree(t, lm, "a", true)
}
