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

const testSecret = "test-secret"

func TestLock_LockUnlock(t *testing.T) {
	l := New()

	if err := l.Lock(context.Background(), testSecret); err != nil {
		t.Fatalf("lock: %v", err)
	}
	if err := l.Unlock(testSecret); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestLock_EmptySecret(t *testing.T) {
	l := New()

	err := l.Lock(context.Background(), "")
	if !errors.Is(err, ErrNilSecret) {
		t.Fatalf("expected ErrNilSecret, got %v", err)
	}
}

func TestLock_NilContext(t *testing.T) {
	var nilCtx context.Context
	l := New()

	err := l.Lock(nilCtx, testSecret)
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf("expected ErrNilContext, got %v", err)
	}
}

func TestLock_CancelledContext(t *testing.T) {
	l := New()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := l.Lock(ctx, testSecret)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}

	if err := l.Lock(context.Background(), testSecret); err != nil {
		t.Fatalf("lock after cancelled acquire: %v", err)
	}
	if err := l.Unlock(testSecret); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestTryLock_EmptySecret(t *testing.T) {
	l := New()

	ok, err := l.TryLock(context.Background(), time.Minute, "")
	if !errors.Is(err, ErrNilSecret) {
		t.Fatalf("expected ErrNilSecret, got %v", err)
	}
	if ok {
		t.Fatal("expected TryLock to fail")
	}
}

func TestTryLock_InvalidTTL(t *testing.T) {
	l := New()

	ok, err := l.TryLock(context.Background(), 0, testSecret)
	if !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("expected ErrInvalidTTL, got %v", err)
	}
	if ok {
		t.Fatal("expected TryLock to fail")
	}

	ok, err = l.TryLock(context.Background(), time.Minute, testSecret)
	if err != nil {
		t.Fatalf("try lock on free lock: %v", err)
	}
	if !ok {
		t.Fatal("expected invalid ttl attempt to leave lock free")
	}
	if err := l.Unlock(testSecret); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestTryLock_NilContext(t *testing.T) {
	var nilCtx context.Context
	l := New()

	ok, err := l.TryLock(nilCtx, time.Minute, testSecret)
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf("expected ErrNilContext, got %v", err)
	}
	if ok {
		t.Fatal("expected TryLock to fail with nil context")
	}
}

func TestTryLock_CancelledContextWhileFree(t *testing.T) {
	l := New()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ok, err := l.TryLock(ctx, time.Minute, testSecret)
	if ok {
		t.Fatal("expected TryLock to fail")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}

	ok, err = l.TryLock(context.Background(), time.Minute, testSecret)
	if err != nil {
		t.Fatalf("try lock after cancelled attempt: %v", err)
	}
	if !ok {
		t.Fatal("expected lock to be free after cancelled TryLock")
	}
	if err := l.Unlock(testSecret); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestTryLock_CancelledContextWhileHeld(t *testing.T) {
	l := New()

	if err := l.Lock(context.Background(), testSecret); err != nil {
		t.Fatalf("lock: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ok, err := l.TryLock(ctx, time.Minute, testSecret)
	if ok {
		t.Fatal("expected TryLock to fail while lock is held")
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected nil or context canceled, got %v", err)
	}

	if err := l.Unlock(testSecret); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestTryLock_Success(t *testing.T) {
	l := New()

	ok, err := l.TryLock(context.Background(), time.Minute, testSecret)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	if !ok {
		t.Fatal("expected TryLock to succeed on free lock")
	}
	if err := l.Unlock(testSecret); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestUnlock_EmptySecret(t *testing.T) {
	l := New()

	if err := l.Lock(context.Background(), testSecret); err != nil {
		t.Fatalf("lock: %v", err)
	}

	err := l.Unlock("")
	if !errors.Is(err, ErrNilSecret) {
		t.Fatalf("expected ErrNilSecret, got %v", err)
	}

	ok, err := l.TryLock(context.Background(), time.Minute, testSecret)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	if ok {
		t.Fatal("expected lock to remain held after unlock with empty secret")
	}

	if err := l.Unlock(testSecret); err != nil {
		t.Fatalf("owner unlock after empty secret attempt: %v", err)
	}
}

func TestLockWithTTL_NilContext(t *testing.T) {
	var nilCtx context.Context
	l := New()

	err := l.LockWithTTL(nilCtx, time.Minute, testSecret)
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf("expected ErrNilContext, got %v", err)
	}
}

func TestLockWithTTL_EmptySecret(t *testing.T) {
	l := New()

	err := l.LockWithTTL(context.Background(), time.Minute, "")
	if !errors.Is(err, ErrNilSecret) {
		t.Fatalf("expected ErrNilSecret, got %v", err)
	}
}

func TestLockWithTTL_InvalidTTL(t *testing.T) {
	l := New()

	err := l.LockWithTTL(context.Background(), 0, testSecret)
	if !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("expected ErrInvalidTTL, got %v", err)
	}
}

func TestLock_CancelledContextAfterAcquire(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := New()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		holder := "holder-secret"
		if err := l.Lock(context.Background(), holder); err != nil {
			t.Fatalf("lock holder: %v", err)
		}

		errCh := make(chan error, 1)
		go func() {
			errCh <- l.Lock(ctx, testSecret)
		}()

		synctest.Wait()

		if err := l.Unlock(holder); err != nil {
			t.Fatalf("unlock holder: %v", err)
		}

		synctest.Wait()

		if err := <-errCh; !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context canceled, got %v", err)
		}

		if err := l.Lock(context.Background(), testSecret); err != nil {
			t.Fatalf("lock after cancelled acquire: %v", err)
		}
		if err := l.Unlock(testSecret); err != nil {
			t.Fatalf("unlock: %v", err)
		}
	})
}

func TestTryLock_CancelledContextAfterAcquire(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := New()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		holder := "holder-secret"
		if err := l.Lock(context.Background(), holder); err != nil {
			t.Fatalf("lock holder: %v", err)
		}

		errCh := make(chan struct {
			ok  bool
			err error
		}, 1)
		go func() {
			ok, err := l.TryLock(ctx, time.Minute, testSecret)
			errCh <- struct {
				ok  bool
				err error
			}{ok, err}
		}()

		synctest.Wait()

		if err := l.Unlock(holder); err != nil {
			t.Fatalf("unlock holder: %v", err)
		}

		synctest.Wait()

		result := <-errCh
		if result.ok {
			t.Fatal("expected TryLock to fail with cancelled context")
		}
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("expected context canceled, got %v", result.err)
		}

		ok, err := l.TryLock(context.Background(), time.Minute, testSecret)
		if err != nil {
			t.Fatalf("try lock after cancelled acquire: %v", err)
		}
		if !ok {
			t.Fatal("expected lock to be free after cancelled acquire")
		}
		if err := l.Unlock(testSecret); err != nil {
			t.Fatalf("unlock: %v", err)
		}
	})
}

func TestLock_AlreadyLocked(t *testing.T) {
	l := New()

	if err := l.Lock(context.Background(), testSecret); err != nil {
		t.Fatalf("lock: %v", err)
	}

	ok, err := l.TryLock(context.Background(), time.Minute, testSecret)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	if ok {
		t.Fatal("expected TryLock to fail when already locked")
	}
}

func TestLock_BlocksUntilContextCancelled(t *testing.T) {
	l := New()

	if err := l.Lock(context.Background(), testSecret); err != nil {
		t.Fatalf("lock: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		done <- l.Lock(ctx, testSecret)
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected deadline exceeded, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Lock did not respect context timeout")
	}
}

func TestLock_WrongSecretOnUnlock(t *testing.T) {
	sec1 := "123e4567-e89b-12d3-a456-426614174000"
	sec2 := "00000000-0000-0000-0000-000000000001"

	l := New()
	if err := l.Lock(context.Background(), sec1); err != nil {
		t.Fatalf("lock: %v", err)
	}

	err := l.Unlock(sec2)
	if !errors.Is(err, ErrInvalidSecret) {
		t.Fatalf("expected ErrInvalidSecret, got %v", err)
	}

	ok, err := l.TryLock(context.Background(), time.Minute, sec1)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	if ok {
		t.Fatal("lock was released by wrong secret unlock")
	}

	if err := l.Unlock(sec1); err != nil {
		t.Fatalf("owner unlock after wrong secret attempt: %v", err)
	}
}

func TestLock_UnlockWhenNotLocked(t *testing.T) {
	l := New()

	err := l.Unlock(testSecret)
	if !errors.Is(err, ErrNotLocked) {
		t.Fatalf("expected ErrNotLocked, got %v", err)
	}
}

func TestLockWithTTL_Expires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := New()

		if err := l.LockWithTTL(context.Background(), time.Minute, testSecret); err != nil {
			t.Fatalf("lock with ttl: %v", err)
		}

		ok, err := l.TryLock(context.Background(), time.Minute, testSecret)
		if err != nil {
			t.Fatalf("try lock while held: %v", err)
		}
		if ok {
			t.Fatal("expected lock to still be held before ttl expires")
		}

		time.Sleep(time.Minute)
		synctest.Wait()

		ok, err = l.TryLock(context.Background(), time.Minute, testSecret)
		if err != nil {
			t.Fatalf("try lock after ttl: %v", err)
		}
		if !ok {
			t.Fatal("expected lock to expire and allow re-acquire")
		}
	})
}

func TestLockWithTTL_StillHeldBeforeExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := New()

		const ttl = 2 * time.Minute
		const elapsed = ttl - time.Second // 1:59

		if err := l.LockWithTTL(context.Background(), ttl, testSecret); err != nil {
			t.Fatalf("lock with ttl: %v", err)
		}

		time.Sleep(elapsed)
		synctest.Wait()

		ok, err := l.TryLock(context.Background(), time.Minute, testSecret)
		if err != nil {
			t.Fatalf("try lock before ttl: %v", err)
		}
		if ok {
			t.Fatalf("expected lock to still be held after %v with ttl %v", elapsed, ttl)
		}
	})
}

func TestLockWithTTL_ExpireDoesNotReleaseRelockedWithoutTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first := "123e4567-e89b-12d3-a456-426614174000"
		second := "00000000-0000-0000-0000-000000000001"

		l := New()

		if err := l.LockWithTTL(context.Background(), time.Minute, first); err != nil {
			t.Fatalf("lock with ttl: %v", err)
		}

		if err := l.Unlock(first); err != nil {
			t.Fatalf("unlock: %v", err)
		}

		if err := l.Lock(context.Background(), second); err != nil {
			t.Fatalf("lock without ttl: %v", err)
		}

		time.Sleep(time.Minute)
		synctest.Wait()

		ok, err := l.TryLock(context.Background(), time.Minute, second)
		if err != nil {
			t.Fatalf("try lock: %v", err)
		}
		if ok {
			t.Fatal("stale ttl expiration released a re-locked lock without ttl")
		}

		if err := l.Unlock(second); err != nil {
			t.Fatalf("unlock: %v", err)
		}
	})
}

func TestLock_ConcurrentLockMutualExclusion(t *testing.T) {
	l := New()

	const goroutines = 32
	const iterations = 50

	var inCriticalSection atomic.Bool

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range iterations {
				if err := l.Lock(context.Background(), testSecret); err != nil {
					t.Errorf("lock: %v", err)
					return
				}

				if !inCriticalSection.CompareAndSwap(false, true) {
					t.Error("multiple goroutines hold the lock at the same time")
					_ = l.Unlock(testSecret)
					return
				}

				inCriticalSection.Store(false)
				if err := l.Unlock(testSecret); err != nil {
					t.Errorf("unlock: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestLock_ConcurrentBlockingLockHandoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := New()

		const waiters = 20
		var acquired atomic.Int32

		if err := l.Lock(context.Background(), testSecret); err != nil {
			t.Fatalf("lock: %v", err)
		}

		var wg sync.WaitGroup
		wg.Add(waiters)
		for range waiters {
			go func() {
				defer wg.Done()
				if err := l.Lock(context.Background(), testSecret); err != nil {
					t.Errorf("lock: %v", err)
					return
				}
				acquired.Add(1)
				if err := l.Unlock(testSecret); err != nil {
					t.Errorf("unlock: %v", err)
				}
			}()
		}

		synctest.Wait()

		if err := l.Unlock(testSecret); err != nil {
			t.Fatalf("unlock: %v", err)
		}

		wg.Wait()

		if acquired.Load() != waiters {
			t.Fatalf("acquired %d locks, want %d", acquired.Load(), waiters)
		}
	})
}

func TestLock_ConcurrentTryLock(t *testing.T) {
	l := New()

	const goroutines = 32
	var winners atomic.Int32

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			ok, err := l.TryLock(context.Background(), time.Minute, testSecret)
			if err != nil {
				t.Errorf("try lock: %v", err)
				return
			}
			if ok {
				winners.Add(1)
			}
		}()
	}
	wg.Wait()

	if winners.Load() != 1 {
		t.Fatalf("expected exactly 1 successful try lock, got %d", winners.Load())
	}

	if err := l.Unlock(testSecret); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestLock_ConcurrentTryLockAfterRelease(t *testing.T) {
	l := New()

	const contenders = 16

	if err := l.Lock(context.Background(), testSecret); err != nil {
		t.Fatalf("lock: %v", err)
	}

	var winners atomic.Int32
	var wg sync.WaitGroup
	wg.Add(contenders)
	for range contenders {
		go func() {
			defer wg.Done()
			ok, err := l.TryLock(context.Background(), time.Minute, testSecret)
			if err != nil {
				t.Errorf("try lock: %v", err)
				return
			}
			if ok {
				winners.Add(1)
			}
		}()
	}
	wg.Wait()

	if winners.Load() != 0 {
		t.Fatalf("expected 0 winners while held, got %d", winners.Load())
	}

	if err := l.Unlock(testSecret); err != nil {
		t.Fatalf("unlock: %v", err)
	}

	wg = sync.WaitGroup{}
	wg.Add(contenders)
	for range contenders {
		go func() {
			defer wg.Done()
			ok, err := l.TryLock(context.Background(), time.Minute, testSecret)
			if err != nil {
				t.Errorf("try lock: %v", err)
				return
			}
			if ok {
				winners.Add(1)
			}
		}()
	}
	wg.Wait()

	if winners.Load() != 1 {
		t.Fatalf("expected 1 winner after release, got %d", winners.Load())
	}

	if err := l.Unlock(testSecret); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestLock_ConcurrentUnlockOnlyOneSucceeds(t *testing.T) {
	l := New()

	if err := l.Lock(context.Background(), testSecret); err != nil {
		t.Fatalf("lock: %v", err)
	}

	const goroutines = 16
	var successes atomic.Int32

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			if err := l.Unlock(testSecret); err == nil {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()

	if successes.Load() != 1 {
		t.Fatalf("expected exactly 1 successful unlock, got %d", successes.Load())
	}
}

func TestLock_ConcurrentWrongSecretUnlockDoesNotRelease(t *testing.T) {
	owner := "123e4567-e89b-12d3-a456-426614174000"
	wrong := "00000000-0000-0000-0000-000000000001"

	l := New()
	if err := l.Lock(context.Background(), owner); err != nil {
		t.Fatalf("lock: %v", err)
	}

	const goroutines = 16
	var invalidSecret atomic.Int32

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			err := l.Unlock(wrong)
			switch {
			case err == nil:
				t.Error("unlock with wrong secret succeeded")
			case errors.Is(err, ErrInvalidSecret):
				invalidSecret.Add(1)
			default:
				t.Errorf("unlock with wrong secret: %v", err)
			}
		}()
	}
	wg.Wait()

	if invalidSecret.Load() != goroutines {
		t.Fatalf("expected %d ErrInvalidSecret, got %d", goroutines, invalidSecret.Load())
	}

	ok, err := l.TryLock(context.Background(), time.Minute, owner)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	if ok {
		t.Fatal("lock was released by wrong secret unlock attempts")
	}

	if err := l.Unlock(owner); err != nil {
		t.Fatalf("owner unlock after concurrent wrong secret attempts: %v", err)
	}
}
