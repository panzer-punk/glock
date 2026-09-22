package lock

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func newTestNamespace(t *testing.T) *Namespace {
	t.Helper()
	ns, err := NewNamespace("test", 16, UUIDSecretFactory)
	if err != nil {
		t.Fatalf("NewNamespace: %v", err)
	}
	return ns
}

func tryLockNS(t *testing.T, ns *Namespace, key string) (string, bool) {
	t.Helper()
	sec, ok, err := ns.TryLock(key, time.Minute, context.Background())
	if err != nil {
		t.Fatalf("try lock %q: %v", key, err)
	}
	return sec, ok
}

func TestNewNamespace(t *testing.T) {
	ns := newTestNamespace(t)
	if ns.Name != "test" {
		t.Fatalf("name: got %q, want test", ns.Name)
	}
	if ns.BucketsCnt != 16 {
		t.Fatalf("buckets count: got %d, want 16", ns.BucketsCnt)
	}
	if len(ns.buckets) != 16 {
		t.Fatalf("buckets: got %d, want 16", len(ns.buckets))
	}
}

func TestNamespace_LockUnlock(t *testing.T) {
	ns := newTestNamespace(t)

	secret, err := ns.Lock("resource", 0, context.Background())
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if secret == "" {
		t.Fatal("expected non-empty secret")
	}

	if err := ns.Unlock("resource", secret); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestNamespace_LockTTLExpires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ns := newTestNamespace(t)

		if _, err := ns.Lock("resource", time.Minute, context.Background()); err != nil {
			t.Fatalf("lock: %v", err)
		}

		if _, ok := tryLockNS(t, ns, "resource"); ok {
			t.Fatal("expected lock to be held before ttl")
		}

		time.Sleep(time.Minute)
		synctest.Wait()

		if _, ok := tryLockNS(t, ns, "resource"); !ok {
			t.Fatal("expected lock to expire")
		}
	})
}

func TestNamespace_UnlockNotFound(t *testing.T) {
	ns := newTestNamespace(t)

	err := ns.Unlock("missing", "any-secret")
	if !errors.Is(err, ErrLockNotFound) {
		t.Fatalf("expected ErrLockNotFound, got %v", err)
	}
}

func TestNamespace_UnlockWrongSecret(t *testing.T) {
	ns := newTestNamespace(t)

	owner, err := ns.Lock("resource", 0, context.Background())
	if err != nil {
		t.Fatalf("lock: %v", err)
	}

	wrong := "wrong-secret"
	err = ns.Unlock("resource", wrong)
	if !errors.Is(err, ErrInvalidSecret) {
		t.Fatalf("expected ErrInvalidSecret, got %v", err)
	}

	if _, ok := tryLockNS(t, ns, "resource"); ok {
		t.Fatal("lock was released by wrong secret unlock")
	}

	if err := ns.Unlock("resource", owner); err != nil {
		t.Fatalf("owner unlock after wrong secret attempt: %v", err)
	}
}

func TestNamespace_TryLockSuccess(t *testing.T) {
	ns := newTestNamespace(t)

	secret, ok := tryLockNS(t, ns, "resource")
	if !ok {
		t.Fatal("expected try lock to succeed")
	}
	if secret == "" {
		t.Fatal("expected non-empty secret")
	}

	if err := ns.Unlock("resource", secret); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestNamespace_TryLockAlreadyHeld(t *testing.T) {
	ns := newTestNamespace(t)

	owner, err := ns.Lock("resource", 0, context.Background())
	if err != nil {
		t.Fatalf("lock: %v", err)
	}

	secret, ok := tryLockNS(t, ns, "resource")
	if ok {
		t.Fatal("expected try lock to fail when already held")
	}
	if secret != "" {
		t.Fatal("expected empty secret when try lock fails")
	}

	if err := ns.Unlock("resource", owner); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestNamespace_DifferentKeysAreIndependent(t *testing.T) {
	ns := newTestNamespace(t)

	first, err := ns.Lock("a", 0, context.Background())
	if err != nil {
		t.Fatalf("lock a: %v", err)
	}

	second, ok := tryLockNS(t, ns, "b")
	if !ok {
		t.Fatal("expected try lock on b to succeed")
	}

	if err := ns.Unlock("b", second); err != nil {
		t.Fatalf("unlock b: %v", err)
	}
	if err := ns.Unlock("a", first); err != nil {
		t.Fatalf("unlock a: %v", err)
	}
}

func TestNamespace_LockBlocksSameKey(t *testing.T) {
	ns := newTestNamespace(t)

	owner, err := ns.Lock("resource", 0, context.Background())
	if err != nil {
		t.Fatalf("lock: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, err := ns.Lock("resource", 0, ctx)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected deadline exceeded, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("lock did not respect context timeout")
	}

	if err := ns.Unlock("resource", owner); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestNamespace_LockCancelledContext(t *testing.T) {
	ns := newTestNamespace(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	secret, err := ns.Lock("resource", 0, ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if secret != "" {
		t.Fatal("expected empty secret on error")
	}
}

func TestNamespace_ConcurrentDifferentKeys(t *testing.T) {
	ns := newTestNamespace(t)

	const goroutines = 32
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(id int) {
			defer wg.Done()
			key := string(rune('a' + id))

			secret, err := ns.Lock(key, 0, context.Background())
			if err != nil {
				t.Errorf("lock %q: %v", key, err)
				return
			}
			if err := ns.Unlock(key, secret); err != nil {
				t.Errorf("unlock %q: %v", key, err)
			}
		}(i)
	}

	wg.Wait()
}

func TestNamespace_ConcurrentSameKey(t *testing.T) {
	ns := newTestNamespace(t)

	const goroutines = 16
	var success atomic.Int32

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			secret, ok, err := ns.TryLock("shared", time.Minute, context.Background())
			if err != nil {
				t.Errorf("try lock: %v", err)
				return
			}
			if !ok {
				return
			}
			success.Add(1)
			time.Sleep(10 * time.Millisecond)
			_ = ns.Unlock("shared", secret)
		}()
	}
	wg.Wait()

	if success.Load() != 1 {
		t.Fatalf("expected exactly 1 successful try lock, got %d", success.Load())
	}
}

func TestNamespace_ConcurrentLockHandoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ns := newTestNamespace(t)

		const waiters = 10
		var acquired atomic.Int32

		secret, err := ns.Lock("resource", 0, context.Background())
		if err != nil {
			t.Fatalf("lock: %v", err)
		}

		var wg sync.WaitGroup
		wg.Add(waiters)
		for range waiters {
			go func() {
				defer wg.Done()
				s, err := ns.Lock("resource", 0, context.Background())
				if err != nil {
					t.Errorf("lock: %v", err)
					return
				}
				acquired.Add(1)
				if err := ns.Unlock("resource", s); err != nil {
					t.Errorf("unlock: %v", err)
				}
			}()
		}

		synctest.Wait()

		if err := ns.Unlock("resource", secret); err != nil {
			t.Fatalf("unlock holder: %v", err)
		}

		wg.Wait()

		if acquired.Load() != waiters {
			t.Fatalf("acquired %d locks, want %d", acquired.Load(), waiters)
		}
	})
}

func TestNamespace_bucketNumStable(t *testing.T) {
	ns := newTestNamespace(t)

	first := ns.bucketNum("stable-key")
	second := ns.bucketNum("stable-key")
	if first != second {
		t.Fatalf("expected stable bucket, got %d and %d", first, second)
	}
	if int(first) >= len(ns.buckets) {
		t.Fatalf("bucket %d out of range %d", first, len(ns.buckets))
	}
}
