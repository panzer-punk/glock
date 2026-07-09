package service

import (
	"context"
	"errors"
	"sync"
	"time"

	"glock/internal/lock"
	"glock/internal/namespace"
)

var ErrNamespaceNotFound = errors.New("namespace not found")

type LockService struct {
	namespaces map[string]*namespace.Namespace
	nsMu       sync.RWMutex
	secretsMu  sync.RWMutex
}

func NewLockService() *LockService {
	return &LockService{
		namespaces: make(map[string]*namespace.Namespace),
	}
}

func (ls *LockService) AddNamespace(name string, options *namespace.Options) {
	ls.nsMu.Lock()
	defer ls.nsMu.Unlock()

	ns := namespace.New(name, options)
	ls.namespaces[name] = ns
}

func (ls *LockService) DeleteNamespace(name string) {
	ls.nsMu.Lock()
	defer ls.nsMu.Unlock()

	delete(ls.namespaces, name)
}

func (ls *LockService) getNamespace(name string) (*namespace.Namespace, bool) {
	ls.nsMu.RLock()
	defer ls.nsMu.RUnlock()

	ns, ok := ls.namespaces[name]
	return ns, ok
}

func (ls *LockService) Lock(namespace, key string, ctx context.Context) (lock.Secret, error) {
	ns, ok := ls.getNamespace(namespace)
	if !ok {
		return nil, ErrNamespaceNotFound
	}

	return ns.Lock(key, ctx)
}

func (ls *LockService) TryLock(namespace, key string, ttl time.Duration, ctx context.Context) (lock.Secret, bool) {
	ns, ok := ls.getNamespace(namespace)
	if !ok {
		return nil, false
	}

	return ns.TryLock(key, ttl, ctx)
}

func (ls *LockService) Unlock(namespace, key string, secret lock.Secret) error {
	ns, ok := ls.getNamespace(namespace)
	if !ok {
		return ErrNamespaceNotFound
	}

	return ns.Unlock(key, secret)
}
