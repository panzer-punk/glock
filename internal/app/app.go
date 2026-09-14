package app

import (
	"context"
	"encoding/binary"
	"errors"
	"glock/internal/lock"
	"glock/internal/protocol"
	"time"
)

const sessionKey = "session"

var (
	ErrSessionNotFound = errors.New("session not found")
	ErrMissingKey      = errors.New("missing key")
	ErrMissingSecret   = errors.New("missing secret")
	ErrUnknownPacket   = errors.New("unknown packet type")
)

type Config struct {
	DefaultNamespace string
	DefaultTTL       time.Duration

	SecretFactory *lock.SecretFactory
	LockManager   *lock.LockManager
}

type App struct {
	conf *Config
}

func NewApp(conf *Config) *App {
	return &App{
		conf: conf,
	}
}

func (a *App) OnConnect(ctx context.Context) context.Context {
	s := NewSession()
	s.Ctx = ctx
	context.AfterFunc(ctx, func() {
		s.Close()
	})

	return context.WithValue(ctx, sessionKey, s)
}

func (a *App) Handle(rq *protocol.Packet, rp *protocol.Packet, ctx context.Context) error {
	s, ok := ctx.Value(sessionKey).(*Session)
	if !ok {
		return ErrSessionNotFound
	}

	var err error
	switch rq.Type {
	case protocol.PacketTypeLock:
		err = a.handleLock(rq, rp, s)
	case protocol.PacketTypeTryLock:
		err = a.handleTryLock(rq, rp, s)
	case protocol.PacketTypeUnlock:
		err = a.handleUnlock(rq, rp, s)
	default:
		rp.Type = protocol.PacketTypeError
		rp.AddBlock(protocol.NewPayloadBlock(protocol.PayloadBlockTypeError, []byte(ErrUnknownPacket.Error())))
	}

	return err
}

func (a *App) handleLock(rq *protocol.Packet, rp *protocol.Packet, s *Session) error {
	ns, ok := a.getBlockValue(rq, protocol.PayloadBlockTypeNamespace)
	if !ok {
		ns = []byte(a.conf.DefaultNamespace)
	}

	k, ok := a.getBlockValue(rq, protocol.PayloadBlockTypeKey)
	if !ok {
		rp.Type = protocol.PacketTypeError
		rp.AddBlock(protocol.NewPayloadBlock(protocol.PayloadBlockTypeError, []byte(ErrMissingKey.Error())))
		return nil
	}

	sec, err := a.conf.LockManager.Lock(string(ns), string(k), s.Ctx)
	if err != nil {
		return a.replyOrFail(rp, err)
	}

	sessionNs := string(ns)
	sessionKey := string(k)
	sessionSec := sec
	s.RememberLock(sessionKey, func() {
		a.conf.LockManager.Unlock(sessionNs, sessionKey, sessionSec)
	})
	rp.AddBlock(protocol.NewPayloadBlock(protocol.PayloadBlockTypeSecret, []byte(sec)))

	return nil
}

func (a *App) handleTryLock(rq *protocol.Packet, rp *protocol.Packet, s *Session) error {
	ns, ok := a.getBlockValue(rq, protocol.PayloadBlockTypeNamespace)
	if !ok {
		ns = []byte(a.conf.DefaultNamespace)
	}

	k, ok := a.getBlockValue(rq, protocol.PayloadBlockTypeKey)
	if !ok {
		rp.Type = protocol.PacketTypeError
		rp.AddBlock(protocol.NewPayloadBlock(protocol.PayloadBlockTypeError, []byte(ErrMissingKey.Error())))
		return nil
	}

	var ttlDuration time.Duration
	ttl, ok := a.getBlockValue(rq, protocol.PayloadBlockTypeTTL)
	if !ok {
		ttlDuration = a.conf.DefaultTTL
	} else {
		ttlDuration = time.Duration(binary.BigEndian.Uint64(ttl)) * time.Nanosecond
	}

	sec, ok := a.conf.LockManager.TryLock(string(ns), string(k), ttlDuration, s.Ctx)

	var success byte
	var secValue []byte

	if ok {
		success = 1
		secValue = []byte(sec)
		sessionNs := string(ns)
		sessionKey := string(k)
		sessionSec := sec
		s.RememberLock(sessionKey, func() {
			a.conf.LockManager.Unlock(sessionNs, sessionKey, sessionSec)
		})
	} else {
		success = 0
		secValue = []byte{0}
	}

	rp.AddBlock(protocol.NewPayloadBlock(protocol.PayloadBlockTypeSecret, secValue))
	rp.AddBlock(protocol.NewPayloadBlock(protocol.PayloadBlockTypeSuccess, []byte{success}))

	return nil
}

func (a *App) handleUnlock(rq *protocol.Packet, rp *protocol.Packet, s *Session) error {
	ns, ok := a.getBlockValue(rq, protocol.PayloadBlockTypeNamespace)
	if !ok {
		ns = []byte(a.conf.DefaultNamespace)
	}

	k, ok := a.getBlockValue(rq, protocol.PayloadBlockTypeKey)
	if !ok {
		rp.Type = protocol.PacketTypeError
		rp.AddBlock(protocol.NewPayloadBlock(protocol.PayloadBlockTypeError, []byte(ErrMissingKey.Error())))
		return nil
	}

	sec, ok := a.getBlockValue(rq, protocol.PayloadBlockTypeSecret)
	if !ok {
		rp.Type = protocol.PacketTypeError
		rp.AddBlock(protocol.NewPayloadBlock(protocol.PayloadBlockTypeError, []byte(ErrMissingSecret.Error())))
		return nil
	}

	err := a.conf.LockManager.Unlock(string(ns), string(k), string(sec))
	if err != nil {
		return a.replyOrFail(rp, err)
	}

	s.ForgetLock(string(k))

	return nil
}

func (a *App) replyOrFail(rp *protocol.Packet, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	rp.Type = protocol.PacketTypeError
	rp.AddBlock(protocol.NewPayloadBlock(protocol.PayloadBlockTypeError, []byte(err.Error())))
	return nil
}

func (a *App) getBlockValue(rq *protocol.Packet, tp protocol.PayloadBlockType) ([]byte, bool) {
	b, ok := rq.FindBlock(tp)
	if !ok {
		return nil, false
	}

	return b.Value, true
}
