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
	Ctx   context.Context
	locks map[string]SessionLock
}

func NewSession() *Session {
	return &Session{
		locks: make(map[string]SessionLock),
	}
}

func (s *Session) RememberLock(k string, lock SessionLock) {
	s.locks[k] = lock
}

func (s *Session) ForgetLock(k string) {
	delete(s.locks, k)
}

func (s *Session) Close(mngr *lock.LockManager) error {
	for k, lock := range s.locks {
		mngr.Unlock(lock.Namespace, lock.Key, lock.Secret)
		s.ForgetLock(k)
	}

	return nil
}
