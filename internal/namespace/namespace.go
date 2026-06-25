package namespace

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"time"

	"glock/internal/lock"
)

var ErrLockNotFound = errors.New("lock not found")

type Namespace struct {
	Name    string
	Options *Options
	buckets []sync.Mutex
	locks   map[string]*lock.Lock
}

func New(name string, options *Options) *Namespace {
	if options == nil {
		options = DefaultOptions()
	}

	return &Namespace{
		Name:    name,
		Options: options,
		buckets: make([]sync.Mutex, options.Buckets),
		locks:   make(map[string]*lock.Lock),
	}
}

func (ns *Namespace) Lock(key string, ctx context.Context) (lock.Secret, error) {
	l := ns.getLock(key)
	secret := ns.Options.SecretFactory()

	err := l.Lock(ctx, secret)
	if err != nil {
		return nil, err
	}

	return secret, nil
}

func (ns *Namespace) TryLock(key string, ttl time.Duration, ctx context.Context) (lock.Secret, bool) {
	l := ns.getLock(key)
	secret := ns.Options.SecretFactory()

	ok, _ := l.TryLock(ctx, ttl, secret)
	if !ok {
		return nil, false
	}

	return secret, true
}

func (ns *Namespace) getLock(key string) *lock.Lock {
	bucket := ns.bucketLock(key)
	bucket.Lock()
	defer bucket.Unlock()

	l, ok := ns.locks[key]
	if !ok {
		l = lock.New()
		ns.locks[key] = l
	}

	return l
}

func (ns *Namespace) bucketLock(key string) *sync.Mutex {
	bucket := ns.bucketNum(key)
	return &ns.buckets[bucket]
}

func (ns *Namespace) Unlock(key string, secret lock.Secret) error {
	bucket := ns.bucketLock(key)
	bucket.Lock()
	defer bucket.Unlock()

	l, ok := ns.locks[key]
	if !ok {
		return ErrLockNotFound
	}

	err := l.Unlock(secret)
	if err != nil {
		return err
	}

	return nil
}

func (ns *Namespace) bucketNum(key string) uint32 {
	hash := fnv.New32a()
	hash.Write([]byte(key))

	return hash.Sum32() & (uint32(len(ns.buckets)) - 1)
}
