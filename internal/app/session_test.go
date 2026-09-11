package app

import (
	"sync/atomic"
	"testing"
)

func TestSession_CloseReleasesRememberedLocks(t *testing.T) {
	s := NewSession()
	var released atomic.Int32
	s.RememberLock("a", func() { released.Add(1) })
	s.RememberLock("b", func() { released.Add(1) })

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if released.Load() != 2 {
		t.Fatalf("released %d, want 2", released.Load())
	}
}

func TestSession_ForgetLockSkipsRelease(t *testing.T) {
	s := NewSession()
	var released atomic.Int32
	s.RememberLock("a", func() { released.Add(1) })
	s.ForgetLock("a")

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if released.Load() != 0 {
		t.Fatalf("released %d, want 0", released.Load())
	}
}

func TestSession_RememberLockReplacesRelease(t *testing.T) {
	s := NewSession()
	var first, second atomic.Int32
	s.RememberLock("a", func() { first.Add(1) })
	s.RememberLock("a", func() { second.Add(1) })

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if first.Load() != 0 {
		t.Fatalf("first release called %d times", first.Load())
	}
	if second.Load() != 1 {
		t.Fatalf("second release called %d times, want 1", second.Load())
	}
}

func TestSession_CloseIdempotent(t *testing.T) {
	s := NewSession()
	var released atomic.Int32
	s.RememberLock("a", func() { released.Add(1) })

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if released.Load() != 1 {
		t.Fatalf("released %d, want 1", released.Load())
	}
}
