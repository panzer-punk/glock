package glock

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"time"
)

type NamespaceObserver interface {
	OnLockExpired(namespace *Namespace, key string)
	OnLockReleased(namespace *Namespace, key string)
	OnLockAcquired(namespace *Namespace, key string)
}

type Namespace struct {
	Name string
	buckets []sync.Mutex
	locks map[string]*Lock
	observers []NamespaceObserver
}

var (
	ErrLockNotFound      = errors.New("lock not found")
	ErrLockNotLocked     = errors.New("lock is not locked")
	ErrNamespaceNotFound = errors.New("namespace not found")
)

func NewNamespace(name string, buckets uint32) *Namespace {
	ns := &Namespace{
		Name: name,
		buckets: make([]sync.Mutex, buckets),
		locks: make(map[string]*Lock),
		observers: make([]NamespaceObserver, 0),
	}

	return ns
}

func (ns *Namespace) AddObserver(observer NamespaceObserver) {
	ns.observers = append(ns.observers, observer)
}

func (ns *Namespace) RemoveObserver(observer NamespaceObserver) {
	for i, o := range ns.observers {
		if o == observer {
			ns.observers = append(ns.observers[:i], ns.observers[i+1:]...)
		}
	}
}

func (ns *Namespace) notifyObservers(fn func(observer NamespaceObserver)) {
	for _, observer := range ns.observers {
		fn(observer)
	}
}

//@todo add cancellation if unlocked before expiration
func (ns *Namespace) expireLock(key string) {
	bucket := ns.bucketLock(key)
	bucket.Lock()
	defer bucket.Unlock()

	lock, ok := ns.locks[key]
	if !ok {
		return
	}

	lock.Unlock()
	delete(ns.locks, key)

	ns.notifyObservers(func(observer NamespaceObserver) {
		observer.OnLockExpired(ns, key)
	})
}

func (ns *Namespace) Lock(key string, ttl time.Duration, ctx context.Context) error {
	lock := ns.getLock(key, ttl)

	err := lock.Lock(ctx)

	if err != nil {
		return err
	}

	if ttl > 0 {
		time.AfterFunc(ttl, func() {
			ns.expireLock(key)
		})
	}

	ns.notifyObservers(func(observer NamespaceObserver) {
		observer.OnLockAcquired(ns, key)
	})

	return nil
}

func (ns *Namespace) TryLock(key string, ttl time.Duration, ctx context.Context) bool {
	lock := ns.getLock(key, ttl)

	ok := lock.TryLock(ctx)

	if ok {
		ns.notifyObservers(func(observer NamespaceObserver) {
			observer.OnLockAcquired(ns, key)
		})

		if ttl > 0 {
			time.AfterFunc(ttl, func() {
				ns.expireLock(key)
			})
		}
	}

	return ok
}

func (ns *Namespace) getLock(key string, ttl time.Duration) *Lock {
	bucket := ns.bucketLock(key)
	bucket.Lock()
	defer bucket.Unlock()

	lock, ok := ns.locks[key]
	if !ok {
		lock = newLock(ttl)
		ns.locks[key] = lock
	}

	return lock
}

func (ns *Namespace) bucketLock(key string) *sync.Mutex {
	bucket := ns.bucketNum(key)
	return &ns.buckets[bucket]
}

func (ns *Namespace) Unlock(key string) error {
	bucket := ns.bucketLock(key)
	bucket.Lock()
	defer bucket.Unlock()

	lock, ok := ns.locks[key]
	if !ok {
		return ErrLockNotFound
	}

	err := lock.Unlock()
	if err != nil {
		return err
	}

	ns.notifyObservers(func(observer NamespaceObserver) {
		observer.OnLockReleased(ns, key)
	})

	return nil
}

func (ns *Namespace) bucketNum(key string) uint32 {
	hash := fnv.New32a()
	hash.Write([]byte(key))

	return hash.Sum32() & (uint32(len(ns.buckets)) - 1)
}

//@todo add expiration 
type Lock struct {
	lock chan struct{}
	expiresAt time.Time
}

func newLock(ttl time.Duration) *Lock {
	var expiresAt time.Time
	
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	l := &Lock{
		lock: make(chan struct{}, 1),
		expiresAt: expiresAt,
	}

	return l
}

func (l *Lock) Lock(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case l.lock <- struct{}{}:
			return nil
		}
	}
}

func (l *Lock) TryLock(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case l.lock <- struct{}{}:
		return true
	default:
		return false
	}
}

func (l *Lock) Unlock() error {
	select {
	case <-l.lock:
		return nil
	default:
		return ErrLockNotLocked
	}
}

//@todo add lock secret support in LockService
type Secret interface {
	Check(secret Secret) bool
}

type LockService struct {
	namespaces map[string]*Namespace
	nsMu sync.RWMutex
	secrets map[string]*Secret
	secretsMu sync.RWMutex
}

func NewLockService() *LockService {
	return &LockService{
		namespaces: make(map[string]*Namespace),
	}
}

//@todo secret lock per namespace
func (ls *LockService) OnLockExpired(namespace *Namespace, key string) {
	ls.secretsMu.Lock()
	defer ls.secretsMu.Unlock()

	_, ok := ls.secrets[key]
	if ok {
		delete(ls.secrets, key)
	}
}

func (ls *LockService) OnLockReleased(namespace *Namespace, key string) {

}

func (ls *LockService) OnLockAcquired(namespace *Namespace, key string) {

}

func (ls *LockService) AddNamespace(name string, buckets uint32) {
	ls.nsMu.Lock()
	defer ls.nsMu.Unlock()

	ns := NewNamespace(name, buckets)
	ns.AddObserver(ls)
	ls.namespaces[name] = ns
}

func (ls *LockService) DeleteNamespace(name string) {
	ls.nsMu.Lock()
	defer ls.nsMu.Unlock()

	ns, ok := ls.getNamespace(name)
	if ok {
		ns.RemoveObserver(ls)
		delete(ls.namespaces, name)
	}
}

func (ls *LockService) getNamespace(name string) (*Namespace, bool) {
	ls.nsMu.RLock()
	defer ls.nsMu.RUnlock()

	ns, ok := ls.namespaces[name]
	return ns, ok
}

func (ls *LockService) Lock(namespace, key string, ttl time.Duration, ctx context.Context) error {
	ns, ok := ls.getNamespace(namespace)
	if !ok {
		return ErrNamespaceNotFound
	}

	return ns.Lock(key, ttl, ctx)
}

func (ls *LockService) TryLock(namespace, key string, ttl time.Duration, ctx context.Context) bool {
	ns, ok := ls.getNamespace(namespace)
	if !ok {
		return false
	}

	return ns.TryLock(key, ttl, ctx)
}

func (ls *LockService) Unlock(namespace, key string) error {
	ns, ok := ls.getNamespace(namespace)
	if !ok {
		return ErrNamespaceNotFound
	}

	return ns.Unlock(key)
}

func main() {
	
}