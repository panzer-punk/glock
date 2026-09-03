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
		return l.accquire(ctx, secret)
	}
}

func (l *Lock) accquire(ctx context.Context, secret Secret) error {
	select {
	case <-ctx.Done():
		<-l.lock
		return ctx.Err()
	default:
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

	{ //sync point
		if err := l.Lock(ctx, secret); err != nil {
			return err
		}
		l.initTTL(ttl, secret)
	}

	return nil
}

func (l *Lock) initTTL(ttl time.Duration, secret Secret) error {
	l.expiresAt = time.Now().Add(ttl)
	ctx, expirationCncl := context.WithCancel(context.Background())
	l.expirationCncl = expirationCncl
	time.AfterFunc(ttl, func() {
		select {
		case <-ctx.Done():
			return
		default:
			l.Unlock(secret)
		}
	})

	return nil
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
		{ //sync point
			if err := l.accquire(ctx, secret); err != nil {
				return false, err
			}
			l.initTTL(ttl, secret)
			return true, nil
		}
	default:
		return false, nil
	}
}

func (l *Lock) Unlock(secret Secret) error {
	if (!l.locked.CompareAndSwap(true, false)) {
		return ErrNotLocked
	}

	if secret == nil {
		return ErrNilSecret
	}

	if !l.secret.Check(secret) {
		return ErrInvalidSecret
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
