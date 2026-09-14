package lock

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrNamespaceNotFound = errors.New("namespace not found")

type LockManager struct {
	namespaces map[string]*Namespace
	nsMu       sync.RWMutex
}

func NewLockService() *LockManager {
	return &LockManager{
		namespaces: make(map[string]*Namespace),
	}
}

func (ls *LockManager) AddNamespace(name string, bucketsCnt uint32, secFactory SecretFactory) {
	ls.nsMu.Lock()
	defer ls.nsMu.Unlock()

	ns := NewNamespace(name, bucketsCnt, secFactory)
	ls.namespaces[name] = ns
}

func (ls *LockManager) DeleteNamespace(name string) {
	ls.nsMu.Lock()
	defer ls.nsMu.Unlock()

	delete(ls.namespaces, name)
}

func (ls *LockManager) getNamespace(name string) (*Namespace, bool) {
	ls.nsMu.RLock()
	defer ls.nsMu.RUnlock()

	ns, ok := ls.namespaces[name]
	return ns, ok
}

func (ls *LockManager) Lock(namespace, key string, ttl time.Duration, ctx context.Context) (string, error) {
	ns, ok := ls.getNamespace(namespace)
	if !ok {
		return "", ErrNamespaceNotFound
	}

	return ns.Lock(key, ttl, ctx)
}

func (ls *LockManager) TryLock(namespace, key string, ttl time.Duration, ctx context.Context) (string, bool) {
	ns, ok := ls.getNamespace(namespace)
	if !ok {
		return "", false
	}

	return ns.TryLock(key, ttl, ctx)
}

func (ls *LockManager) Unlock(namespace, key string, secret string) error {
	ns, ok := ls.getNamespace(namespace)
	if !ok {
		return ErrNamespaceNotFound
	}

	return ns.Unlock(key, secret)
}
