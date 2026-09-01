package lock

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

var (
	ErrNotLocked     = errors.New("lock is not locked")
	ErrInvalidSecret = errors.New("invalid secret")
	ErrNilSecret     = errors.New("secret is nil")
	ErrInvalidTTL    = errors.New("ttl must be positive")
)

type Lock struct {
	lock   chan struct{}
	locked *atomic.Bool
	secret Secret

	expirationCncl context.CancelFunc
	expiresAt      time.Time
}

func New() *Lock {
	locked := atomic.Bool{}
	locked.Store(false)

	l := &Lock{
		lock: make(chan struct{}, 1),
		locked: &locked,
	}

	return l
}

func (l *Lock) Lock(ctx context.Context, secret Secret) error {
	if secret == nil {
		return ErrNilSecret
	}

	if ctx == nil {
		ctx = context.Background()
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case l.lock <- struct{}{}:
		l.secret = secret
		l.locked.Store(true)
		return nil
	}
}

func (l *Lock) LockWithTTL(ctx context.Context, ttl time.Duration, secret Secret) error {
	if ttl <= 0 {
		return ErrInvalidTTL
	}

	if ctx == nil {
		ctx = context.Background()
	}

	if err := l.Lock(ctx, secret); err != nil {
		return err
	}

	l.initTTL(ttl)

	return nil
}

func (l *Lock) initTTL(ttl time.Duration) {
	if ttl == 0 {
		return
	}

	l.expiresAt = time.Now().Add(ttl)
	expirationCtx, expirationCncl := context.WithCancel(context.Background())
	l.expirationCncl = expirationCncl
	time.AfterFunc(ttl, func() {
		l.expire(expirationCtx)
	})
}

func (l *Lock) TryLock(ctx context.Context, ttl time.Duration, secret Secret) (bool, error) {
	if secret == nil {
		return false, ErrNilSecret
	}

	if ttl <= 0 {
		return false, ErrInvalidTTL
	}

	if ctx == nil {
		ctx = context.Background()
	}

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case l.lock <- struct{}{}:
		l.secret = secret
		l.initTTL(ttl)
		return true, nil
	default:
		return false, nil
	}
}

func (l *Lock) expire(ctx context.Context) bool {
	if l.expiresAt.IsZero() {
		return false
	}

	select {
	case <-ctx.Done():
		return true
	default:
		return l.release() == nil
	}
}

func (l *Lock) Unlock(secret Secret) error {
	if l.secret == nil {
		return ErrNotLocked
	}

	if secret == nil {
		return ErrInvalidSecret
	}

	if !l.secret.Check(secret) {
		return ErrInvalidSecret
	}

	return l.release()
}

func (l *Lock) release() error {
	if (!l.locked.CompareAndSwap(true, false)) {
		return ErrNotLocked
	}

	if l.expirationCncl != nil {
		l.expirationCncl()
		l.expirationCncl = nil
	}
	l.secret = nil
	l.expiresAt = time.Time{}

	select {
	case <-l.lock:
		return nil
	default:
		panic("lock is broken")
	}
}
