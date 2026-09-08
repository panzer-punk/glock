package namespace

import (
	"context"
	"errors"
	"hash/fnv"
	"time"

	"glock/internal/lock"
)

var ErrLockNotFound = errors.New("lock not found")

type Namespace struct {
	Name    string
	BucketsCnt uint32
	secFactory lock.SecretFactory
	buckets []*Bucket
	locks   map[string]*lock.Lock
}

func New(name string, bucketsCnt uint32, secFactory lock.SecretFactory) *Namespace {

	buckets := make([]*Bucket, bucketsCnt)

	for i := range bucketsCnt {
		buckets[i] = NewBucket()
	}

	return &Namespace{
		Name:    name,
		BucketsCnt: bucketsCnt,
		secFactory: secFactory,
		buckets: buckets,
		locks:   make(map[string]*lock.Lock),
	}
}

func (ns *Namespace) Lock(key string, ctx context.Context) (string, error) {
	b := ns.getBucket(key)
	sec := ns.secFactory()

	err := b.Lock(key, sec, ctx)
	if err != nil {
		return "", err
	}

	return sec, nil
}

func (ns *Namespace) TryLock(key string, ttl time.Duration, ctx context.Context) (string, bool) {
	b := ns.getBucket(key)
	sec := ns.secFactory()

	ok := b.TryLock(key, ttl, sec, ctx)
	if !ok {
		return "", false
	}

	return sec, true
}

func (ns *Namespace) getBucket(key string) *Bucket {
	i := ns.bucketNum(key)

	return ns.buckets[i]
}

func (ns *Namespace) Unlock(key string, secret string) error {
	b := ns.getBucket(key)

	err := b.Unlock(key, secret)
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
