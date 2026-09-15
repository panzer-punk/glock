package app

import (
	"context"
	"glock/internal/lock"
)

type SessionLock struct {
	Namespace string
	Key       string
	Secret    string
}

type Session struct {
	Ctx context.Context
	locks map[string]SessionLock
}

func NewSession() *Session {
	return &Session{
		locks: make(map[string]SessionLock),
	}
}

func (s *Session) RememberLock(lock SessionLock) {
	key := s.lockKey(lock.Namespace, lock.Key)
	s.locks[key] = lock
}

func (s *Session) lockKey(n, k string) string {
	// TODO map by struct{ns, key string} instead of concatenating a new string per Remember/Forget.
	return n + ":" + k
}

func (s *Session) ForgetLock(n, k string) {
	key := s.lockKey(n, k)
	delete(s.locks, key)
}

func (s *Session) Close(mngr *lock.LockManager) error {
	for _, lock := range s.locks {
		mngr.Unlock(lock.Namespace, lock.Key, lock.Secret)
		s.ForgetLock(lock.Namespace, lock.Key)
	}

	return nil
}
