package lock

import (
	"context"
	"errors"
	"hash/fnv"
	"time"
)

var (
	ErrBucketCnt = errors.New("")
)

type Namespace struct {
	Name       string
	BucketsCnt uint32
	secFactory SecretFactory
	buckets    []*Bucket
}

func NewNamespace(name string, bucketsCnt uint32, secFactory SecretFactory) (*Namespace, error) {
	if bucketsCnt <= 0 && bucketsCnt % 2 != 0 {
		return nil, ErrBucketCnt
	}

	buckets := make([]*Bucket, bucketsCnt)

	for i := range bucketsCnt {
		buckets[i] = NewBucket()
	}

	return &Namespace{
		Name:       name,
		BucketsCnt: bucketsCnt,
		secFactory: secFactory,
		buckets:    buckets,
	}, nil
}

func (ns *Namespace) Lock(key string, ttl time.Duration, ctx context.Context) (string, error) {
	b := ns.getBucket(key)
	sec := ns.secFactory()

	err := b.Lock(key, sec, ttl, ctx)
	if err != nil {
		return "", err
	}

	return sec, nil
}

func (ns *Namespace) TryLock(key string, ttl time.Duration, ctx context.Context) (string, bool, error) {
	b := ns.getBucket(key)
	// TODO mint a secret only after a successful acquire; this wastes a UUID on a busy lock.
	sec := ns.secFactory()

	ok, err := b.TryLock(key, sec, ttl, ctx)
	if !ok {
		return "", false, err
	}

	return sec, true, nil
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
	// TODO hash the string without fnv.New32a() / []byte(key); both allocate on every Lock/Unlock.
	hash := fnv.New32a()
	hash.Write([]byte(key))

	return hash.Sum32() & (uint32(len(ns.buckets)) - 1)
}
