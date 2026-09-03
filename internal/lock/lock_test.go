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

func TestLock_LockUnlock(t *testing.T) {
	sec, _ := (&NullSecretFactory{}).FromValue("")
	l := New()

	if err := l.Lock(context.Background(), sec); err != nil {
		t.Fatalf("lock: %v", err)
	}
	if err := l.Unlock(sec); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestLock_NilSecret(t *testing.T) {
	l := New()

	err := l.Lock(context.Background(), nil)
	if !errors.Is(err, ErrNilSecret) {
		t.Fatalf("expected ErrNilSecret, got %v", err)
	}
}

func TestLock_NilContext(t *testing.T) {
	sec, _ := (&NullSecretFactory{}).FromValue("")
	l := New()

	if err := l.Lock(nil, sec); err != nil {
		t.Fatalf("lock: %v", err)
	}
	if err := l.Unlock(sec); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestLock_CancelledContext(t *testing.T) {
	sec, _ := (&NullSecretFactory{}).FromValue("")
	l := New()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := l.Lock(ctx, sec)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}

func TestTryLock_NilSecret(t *testing.T) {
	l := New()

	ok, err := l.TryLock(context.Background(), time.Minute, nil)
	if !errors.Is(err, ErrNilSecret) {
		t.Fatalf("expected ErrNilSecret, got %v", err)
	}
	if ok {
		t.Fatal("expected TryLock to fail")
	}
}

func TestTryLock_InvalidTTL(t *testing.T) {
	sec, _ := (&NullSecretFactory{}).FromValue("")
	l := New()

	ok, err := l.TryLock(context.Background(), 0, sec)
	if !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("expected ErrInvalidTTL, got %v", err)
	}
	if ok {
		t.Fatal("expected TryLock to fail")
	}

	ok, err = l.TryLock(context.Background(), time.Minute, sec)
	if err != nil {
		t.Fatalf("try lock after rollback: %v", err)
	}
	if !ok {
		t.Fatal("expected lock to be free after invalid ttl rollback")
	}
	if err := l.Unlock(sec); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestTryLock_NilContext(t *testing.T) {
	sec, _ := (&NullSecretFactory{}).FromValue("")
	l := New()

	ok, err := l.TryLock(nil, time.Minute, sec)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	if !ok {
		t.Fatal("expected TryLock to succeed")
	}
	if err := l.Unlock(sec); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestTryLock_CancelledContext(t *testing.T) {
	sec, _ := (&NullSecretFactory{}).FromValue("")
	l := New()

	if err := l.Lock(context.Background(), sec); err != nil {
		t.Fatalf("lock: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for range 100 {
		ok, err := l.TryLock(ctx, time.Minute, sec)
		if errors.Is(err, context.Canceled) {
			if ok {
				t.Fatal("expected TryLock to fail")
			}
			if err := l.Unlock(sec); err != nil {
				t.Fatalf("unlock: %v", err)
			}
			return
		}
	}

	t.Fatal("TryLock never returned context.Canceled")
}

func TestTryLock_Success(t *testing.T) {
	sec, _ := (&NullSecretFactory{}).FromValue("")
	l := New()

	ok, err := l.TryLock(context.Background(), time.Minute, sec)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	if !ok {
		t.Fatal("expected TryLock to succeed on free lock")
	}
	if err := l.Unlock(sec); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestUnlock_NilSecret(t *testing.T) {
	sec, _ := (&NullSecretFactory{}).FromValue("")
	l := New()

	if err := l.Lock(context.Background(), sec); err != nil {
		t.Fatalf("lock: %v", err)
	}

	err := l.Unlock(nil)
	if !errors.Is(err, ErrNilSecret) {
		t.Fatalf("expected ErrNilSecret, got %v", err)
	}
}

func TestUnlock_PanicsWhenBroken(t *testing.T) {
	sec, _ := (&NullSecretFactory{}).FromValue("")
	l := New()
	l.locked.Store(true)
	l.secret = sec

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on broken lock")
		}
	}()

	_ = l.Unlock(sec)
}

func TestLockWithTTL_NilContext(t *testing.T) {
	sec, _ := (&NullSecretFactory{}).FromValue("")
	l := New()

	if err := l.LockWithTTL(nil, time.Minute, sec); err != nil {
		t.Fatalf("lock with ttl: %v", err)
	}
	if err := l.Unlock(sec); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestLockWithTTL_NilSecret(t *testing.T) {
	l := New()

	err := l.LockWithTTL(context.Background(), time.Minute, nil)
	if !errors.Is(err, ErrNilSecret) {
		t.Fatalf("expected ErrNilSecret, got %v", err)
	}
}

func TestLock_CancelledContextAfterAcquire(t *testing.T) {
	sec, _ := (&NullSecretFactory{}).FromValue("")
	l := New()

	l.lock <- struct{}{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := l.accquire(ctx, sec); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}

	if err := l.Lock(context.Background(), sec); err != nil {
		t.Fatalf("lock after cancelled acquire: %v", err)
	}
	if err := l.Unlock(sec); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestTryLock_CancelledContextAfterAcquire(t *testing.T) {
	sec, _ := (&NullSecretFactory{}).FromValue("")
	l := New()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	l.lock <- struct{}{}
	if err := l.accquire(ctx, sec); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}

	ok, err := l.TryLock(context.Background(), time.Minute, sec)
	if err != nil {
		t.Fatalf("try lock after cancelled acquire: %v", err)
	}
	if !ok {
		t.Fatal("expected lock to be free after cancelled acquire")
	}
	if err := l.Unlock(sec); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestAccquire_CancelledContextReleasesToken(t *testing.T) {
	sec, _ := (&NullSecretFactory{}).FromValue("")
	l := New()

	l.lock <- struct{}{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := l.accquire(ctx, sec); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if l.locked.Load() {
		t.Fatal("expected lock to remain unlocked")
	}

	if err := l.Lock(context.Background(), sec); err != nil {
		t.Fatalf("lock after released token: %v", err)
	}
	if err := l.Unlock(sec); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestInitTTL_SetsExpiration(t *testing.T) {
	sec, _ := (&NullSecretFactory{}).FromValue("")
	l := New()

	before := time.Now()
	if err := l.initTTL(time.Minute, sec); err != nil {
		t.Fatalf("init ttl: %v", err)
	}
	if l.expiresAt.Before(before) {
		t.Fatal("expected expiration in the future")
	}
	if l.expirationCncl == nil {
		t.Fatal("expected expiration cancel func")
	}
}

func TestInitTTL_CancelledCallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sec, _ := (&NullSecretFactory{}).FromValue("")
		l := New()

		if err := l.LockWithTTL(context.Background(), time.Minute, sec); err != nil {
			t.Fatalf("lock with ttl: %v", err)
		}
		if err := l.Unlock(sec); err != nil {
			t.Fatalf("unlock: %v", err)
		}

		time.Sleep(time.Minute)
		synctest.Wait()
	})
}

func TestLock_AlreadyLocked(t *testing.T) {
	sec, _ := (&NullSecretFactory{}).FromValue("")
	l := New()

	if err := l.Lock(context.Background(), sec); err != nil {
		t.Fatalf("lock: %v", err)
	}

	ok, err := l.TryLock(context.Background(), time.Minute, sec)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	if ok {
		t.Fatal("expected TryLock to fail when already locked")
	}
}

func TestLock_BlocksUntilContextCancelled(t *testing.T) {
	sec, _ := (&NullSecretFactory{}).FromValue("")
	l := New()

	if err := l.Lock(context.Background(), sec); err != nil {
		t.Fatalf("lock: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		done <- l.Lock(ctx, sec)
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
	factory := &UUIDSecretFactory{}
	sec1, _ := factory.FromValue("123e4567-e89b-12d3-a456-426614174000")
	sec2, _ := factory.FromValue("00000000-0000-0000-0000-000000000001")

	l := New()
	if err := l.Lock(context.Background(), sec1); err != nil {
		t.Fatalf("lock: %v", err)
	}

	err := l.Unlock(sec2)
	if !errors.Is(err, ErrInvalidSecret) {
		t.Fatalf("expected ErrInvalidSecret, got %v", err)
	}
}

func TestLock_UnlockWhenNotLocked(t *testing.T) {
	sec, _ := (&NullSecretFactory{}).FromValue("")
	l := New()

	err := l.Unlock(sec)
	if !errors.Is(err, ErrNotLocked) {
		t.Fatalf("expected ErrNotLocked, got %v", err)
	}
}

func TestLockWithTTL_Expires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sec, _ := (&NullSecretFactory{}).FromValue("")
		l := New()

		if err := l.LockWithTTL(context.Background(), time.Minute, sec); err != nil {
			t.Fatalf("lock with ttl: %v", err)
		}

		ok, err := l.TryLock(context.Background(), time.Minute, sec)
		if err != nil {
			t.Fatalf("try lock while held: %v", err)
		}
		if ok {
			t.Fatal("expected lock to still be held before ttl expires")
		}

		time.Sleep(time.Minute)
		synctest.Wait()

		ok, err = l.TryLock(context.Background(), time.Minute, sec)
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
		sec, _ := (&NullSecretFactory{}).FromValue("")
		l := New()

		const ttl = 2 * time.Minute
		const elapsed = ttl - time.Second // 1:59

		if err := l.LockWithTTL(context.Background(), ttl, sec); err != nil {
			t.Fatalf("lock with ttl: %v", err)
		}

		time.Sleep(elapsed)
		synctest.Wait()

		ok, err := l.TryLock(context.Background(), time.Minute, sec)
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
		factory := &UUIDSecretFactory{}
		first, _ := factory.FromValue("123e4567-e89b-12d3-a456-426614174000")
		second, _ := factory.FromValue("00000000-0000-0000-0000-000000000001")

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

func TestLockWithTTL_InvalidTTL(t *testing.T) {
	sec, _ := (&NullSecretFactory{}).FromValue("")
	l := New()

	err := l.LockWithTTL(context.Background(), 0, sec)
	if !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("expected ErrInvalidTTL, got %v", err)
	}
}

func TestLock_ConcurrentLockMutualExclusion(t *testing.T) {
	sec, _ := (&NullSecretFactory{}).FromValue("")
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
				if err := l.Lock(context.Background(), sec); err != nil {
					t.Errorf("lock: %v", err)
					return
				}

				if !inCriticalSection.CompareAndSwap(false, true) {
					t.Error("multiple goroutines hold the lock at the same time")
					_ = l.Unlock(sec)
					return
				}

				inCriticalSection.Store(false)
				if err := l.Unlock(sec); err != nil {
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
		sec, _ := (&NullSecretFactory{}).FromValue("")
		l := New()

		const waiters = 20
		var acquired atomic.Int32

		if err := l.Lock(context.Background(), sec); err != nil {
			t.Fatalf("lock: %v", err)
		}

		var wg sync.WaitGroup
		wg.Add(waiters)
		for range waiters {
			go func() {
				defer wg.Done()
				if err := l.Lock(context.Background(), sec); err != nil {
					t.Errorf("lock: %v", err)
					return
				}
				acquired.Add(1)
				if err := l.Unlock(sec); err != nil {
					t.Errorf("unlock: %v", err)
				}
			}()
		}

		synctest.Wait()

		if err := l.Unlock(sec); err != nil {
			t.Fatalf("unlock: %v", err)
		}

		wg.Wait()

		if acquired.Load() != waiters {
			t.Fatalf("acquired %d locks, want %d", acquired.Load(), waiters)
		}
	})
}

func TestLock_ConcurrentTryLock(t *testing.T) {
	sec, _ := (&NullSecretFactory{}).FromValue("")
	l := New()

	const goroutines = 32
	var winners atomic.Int32

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			ok, err := l.TryLock(context.Background(), time.Minute, sec)
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

	if err := l.Unlock(sec); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestLock_ConcurrentTryLockAfterRelease(t *testing.T) {
	sec, _ := (&NullSecretFactory{}).FromValue("")
	l := New()

	const contenders = 16

	if err := l.Lock(context.Background(), sec); err != nil {
		t.Fatalf("lock: %v", err)
	}

	var winners atomic.Int32
	var wg sync.WaitGroup
	wg.Add(contenders)
	for range contenders {
		go func() {
			defer wg.Done()
			ok, err := l.TryLock(context.Background(), time.Minute, sec)
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

	if err := l.Unlock(sec); err != nil {
		t.Fatalf("unlock: %v", err)
	}

	wg = sync.WaitGroup{}
	wg.Add(contenders)
	for range contenders {
		go func() {
			defer wg.Done()
			ok, err := l.TryLock(context.Background(), time.Minute, sec)
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

	if err := l.Unlock(sec); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestLock_ConcurrentUnlockOnlyOneSucceeds(t *testing.T) {
	sec, _ := (&NullSecretFactory{}).FromValue("")
	l := New()

	if err := l.Lock(context.Background(), sec); err != nil {
		t.Fatalf("lock: %v", err)
	}

	const goroutines = 16
	var successes atomic.Int32

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			if err := l.Unlock(sec); err == nil {
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
	factory := &UUIDSecretFactory{}
	owner, _ := factory.FromValue("123e4567-e89b-12d3-a456-426614174000")
	wrong, _ := factory.FromValue("00000000-0000-0000-0000-000000000001")

	l := New()
	if err := l.Lock(context.Background(), owner); err != nil {
		t.Fatalf("lock: %v", err)
	}

	const goroutines = 16
	var invalidSecret atomic.Int32
	var notLocked atomic.Int32

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
			case errors.Is(err, ErrNotLocked):
				notLocked.Add(1)
			default:
				t.Errorf("unlock with wrong secret: %v", err)
			}
		}()
	}
	wg.Wait()

	if invalidSecret.Load() != 1 {
		t.Fatalf("expected 1 ErrInvalidSecret, got %d", invalidSecret.Load())
	}
	if notLocked.Load() != goroutines-1 {
		t.Fatalf("expected %d ErrNotLocked, got %d", goroutines-1, notLocked.Load())
	}

	ok, err := l.TryLock(context.Background(), time.Minute, owner)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	if ok {
		t.Fatal("lock was released by wrong secret unlock attempts")
	}
}
