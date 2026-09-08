package service

import (
	"context"
	"errors"
	"sync"
	"time"

	"glock/internal/lock"
)

var ErrNamespaceNotFound = errors.New("namespace not found")

type LockService struct {
	namespaces map[string]*lock.Namespace
	nsMu       sync.RWMutex
}

func NewLockService() *LockService {
	return &LockService{
		namespaces: make(map[string]*lock.Namespace),
	}
}

func (ls *LockService) AddNamespace(name string, bucketsCnt uint32, secFactory lock.SecretFactory) {
	ls.nsMu.Lock()
	defer ls.nsMu.Unlock()

	ns := lock.NewNamespace(name, bucketsCnt, secFactory)
	ls.namespaces[name] = ns
}

func (ls *LockService) DeleteNamespace(name string) {
	ls.nsMu.Lock()
	defer ls.nsMu.Unlock()

	delete(ls.namespaces, name)
}

func (ls *LockService) getNamespace(name string) (*lock.Namespace, bool) {
	ls.nsMu.RLock()
	defer ls.nsMu.RUnlock()

	ns, ok := ls.namespaces[name]
	return ns, ok
}

func (ls *LockService) Lock(namespace, key string, ctx context.Context) (string, error) {
	ns, ok := ls.getNamespace(namespace)
	if !ok {
		return "", ErrNamespaceNotFound
	}

	return ns.Lock(key, ctx)
}

func (ls *LockService) TryLock(namespace, key string, ttl time.Duration, ctx context.Context) (string, bool) {
	ns, ok := ls.getNamespace(namespace)
	if !ok {
		return "", false
	}

	return ns.TryLock(key, ttl, ctx)
}

func (ls *LockService) Unlock(namespace, key string, secret string) error {
	ns, ok := ls.getNamespace(namespace)
	if !ok {
		return ErrNamespaceNotFound
	}

	return ns.Unlock(key, secret)
}
