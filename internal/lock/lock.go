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
	ErrNilContext    = errors.New("context is nil")
)

type Lock struct {
	lock   chan struct{}
	mu     sync.Mutex
	secret string

	expirationCncl context.CancelFunc
	expiresAt      time.Time
}

func NewLock() *Lock {
	l := &Lock{
		lock: make(chan struct{}, 1),
		mu:   sync.Mutex{},
	}

	return l
}

func (l *Lock) Lock(secret string, ttl time.Duration, ctx context.Context) error {
	err := l.Check(secret, ttl, ctx)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case l.lock <- struct{}{}:
		err := l.sync(func() {
			l.secret = secret

			if ttl > 0 {
				l.initTTL(ttl, secret)
			}
		}, ctx)

		if err == nil {
			return nil
		}

		<-l.lock

		return err
	}
}

func (l *Lock) Check(secret string, ttl time.Duration, ctx context.Context) error {
	if secret == "" {
		return ErrNilSecret
	}

	if ctx == nil {
		return ErrNilContext
	}

	if ttl < 0 {
		return ErrInvalidTTL
	}

	return nil
}


func (l *Lock) sync(f func(), ctx context.Context) error {
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

func (l *Lock) initTTL(ttl time.Duration, secret string) {
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

func (l *Lock) TryLock(ttl time.Duration, secret string, ctx context.Context) (bool, error) {
	err := l.Check(secret, ttl, ctx)
	if err != nil {
		return false, err
	}

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case l.lock <- struct{}{}:
		err := l.sync(func() {
			l.secret = secret

			if ttl > 0 {
				l.initTTL(ttl, secret)
			}
		}, ctx)

		if err == nil {
			return true, nil
		}

		<-l.lock

		return false, err
	default:
		return false, nil
	}
}

func (l *Lock) Unlock(secret string) error {
	if secret == "" {
		return ErrNilSecret
	}

	var err error

	l.sync(func() {
		if l.secret == "" {
			err = ErrNotLocked
			return
		}

		if l.secret != secret {
			err = ErrInvalidSecret
			return
		}
		if l.expirationCncl != nil {
			l.expirationCncl()
			l.expirationCncl = nil
		}
		l.secret = ""
		l.expiresAt = time.Time{}
	}, context.Background())

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
