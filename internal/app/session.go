package app

import "context"

type LockRelease func()

type Session struct {
	Ctx   context.Context
	//TODO refactor session to use a map of locks or simpler data structure
	locks map[string]LockRelease
}

func NewSession() *Session {
	return &Session{
		locks: make(map[string]LockRelease),
	}
}

func (s *Session) RememberLock(k string, release LockRelease) {
	s.locks[k] = release
}

func (s *Session) ForgetLock(k string) {
	delete(s.locks, k)
}

func (s *Session) Close() error {
	for k, release := range s.locks {
		release()
		s.ForgetLock(k)
	}

	return nil
}
