package lock

import (
	"context"
	"errors"
	"sync"
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
	mu     sync.Mutex
	secret Secret

	expirationCncl context.CancelFunc
	expiresAt      time.Time
}

func New() *Lock {
	l := &Lock{
		lock: make(chan struct{}, 1),
		mu:   sync.Mutex{},
	}

	return l
}

func (l *Lock) Lock(ctx context.Context, secret Secret) error {
	return l.accquire(ctx, secret, nil)
}

func (l *Lock) accquire(ctx context.Context, secret Secret, f func()) error {
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
		err := l.sync(ctx, func() {
			l.secret = secret
			if f != nil {
				f()
			}
		})

		if err == nil {
			return nil
		}

		<-l.lock

		return err
	}
}

func (l *Lock) sync(ctx context.Context, f func()) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		l.mu.Lock()
		defer l.mu.Unlock()
		f()
		return nil
	}
}

func (l *Lock) LockWithTTL(ctx context.Context, ttl time.Duration, secret Secret) error {
	if ttl <= 0 {
		return ErrInvalidTTL
	}

	return l.accquire(ctx, secret, func() {
		l.initTTL(ttl, secret)
	})
}

func (l *Lock) initTTL(ttl time.Duration, secret Secret) {
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
		err := l.sync(ctx, func() {
			l.secret = secret
			l.initTTL(ttl, secret)
		})

		if err == nil {
			return true, nil
		}

		<-l.lock

		return false, err
	default:
		return false, nil
	}
}

func (l *Lock) Unlock(secret Secret) error {
	if secret == nil {
		return ErrNilSecret
	}

	var err error

	l.sync(context.Background(), func() {
		if l.secret == nil {
			err = ErrNotLocked
			return
		}

		if !l.secret.Check(secret) {
			err = ErrInvalidSecret
			return
		}
		if l.expirationCncl != nil {
			l.expirationCncl()
			l.expirationCncl = nil
		}
		l.secret = nil
		l.expiresAt = time.Time{}
	})

	if err != nil {
		return err
	}

	select {
	case <-l.lock:
		return nil
	default:
		panic("lock is broken")
	}
}
