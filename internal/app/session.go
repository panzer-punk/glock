package app

type LockRelease func()

type Session struct {
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