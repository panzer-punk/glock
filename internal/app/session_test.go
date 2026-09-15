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
	if err := lm.AddNamespace("ns", 16, factory); err != nil {
		t.Fatalf("add namespace: %v", err)
	}
	return lm
}

func acquireSessionLock(t *testing.T, lm *lock.LockManager, key string) SessionLock {
	t.Helper()
	sec, err := lm.Lock("ns", key, 0, context.Background())
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
	sec, ok, err := lm.TryLock("ns", key, time.Minute, context.Background())
	if err != nil {
		t.Fatalf("try lock %q: %v", key, err)
	}
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
	s.RememberLock(acquireSessionLock(t, lm, "a"))
	s.RememberLock(acquireSessionLock(t, lm, "b"))

	if err := s.Close(lm); err != nil {
		t.Fatalf("close: %v", err)
	}

	assertLockFree(t, lm, "a", true)
	assertLockFree(t, lm, "b", true)
}

func TestSession_ForgetLockSkipsRelease(t *testing.T) {
	lm := sessionLockManager(t)
	s := NewSession()
	held := acquireSessionLock(t, lm, "a")
	s.RememberLock(held)
	s.ForgetLock(held.Namespace, held.Key)

	if err := s.Close(lm); err != nil {
		t.Fatalf("close: %v", err)
	}

	assertLockFree(t, lm, "a", false)
}

func TestSession_RememberLockReplacesRelease(t *testing.T) {
	lm := sessionLockManager(t)
	s := NewSession()
	held := acquireSessionLock(t, lm, "a")
	s.RememberLock(SessionLock{Namespace: held.Namespace, Key: held.Key, Secret: "old-secret"})
	s.RememberLock(held)

	if err := s.Close(lm); err != nil {
		t.Fatalf("close: %v", err)
	}

	assertLockFree(t, lm, "a", true)
}

func TestSession_CloseIdempotent(t *testing.T) {
	lm := sessionLockManager(t)
	s := NewSession()
	s.RememberLock(acquireSessionLock(t, lm, "a"))

	if err := s.Close(lm); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := s.Close(lm); err != nil {
		t.Fatalf("second close: %v", err)
	}

	assertLockFree(t, lm, "a", true)
}
