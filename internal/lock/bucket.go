package lock

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrLockNotFound = errors.New("lock not found")

type Bucket struct {
	mu sync.RWMutex
	locks map[string]*Lock
}

func NewBucket() *Bucket {
	return  &Bucket{mu: sync.RWMutex{}, locks: make(map[string]*Lock)}
}

func (b *Bucket) Lock(key string, sec string, ctx context.Context) error {
	l := b.findLock(key)

	return l.Lock(ctx, sec)
}

func (b *Bucket) findLock(key string) *Lock {
	b.mu.Lock()
	defer b.mu.Unlock()

	l, ok := b.locks[key]

	if !ok {
		l = NewLock()
		b.locks[key] = l
	}

	return l
}

func (b *Bucket) TryLock(key string, ttl time.Duration, sec string, ctx context.Context) bool {
	l := b.findLock(key)

	ok, _ := l.TryLock(ctx, ttl, sec)

	return ok
}

func (b *Bucket) Unlock(key string, sec string) error {
	b.mu.RLock()
	l, ok := b.locks[key]
	b.mu.RUnlock()

	if !ok {
		return ErrLockNotFound
	}

	return l.Unlock(sec)
}
