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

	emptySecret = []byte{0}
)

type Config struct {
	DefaultNamespace string

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
		s.Close(a.conf.LockManager)
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

	ttl := a.getTTL(rq)
	namespace := string(ns)
	key := string(k)
	sec, err := a.conf.LockManager.Lock(namespace, key, ttl, s.Ctx)
	if err != nil {
		return a.replyOrFail(rp, err)
	}

	sessionSec := sec

	if ttl == 0 {
		s.RememberLock(SessionLock{
			Namespace: namespace,
			Key:       key,
			Secret:    sessionSec,
		})
	}

	rp.AddBlock(protocol.NewPayloadBlock(protocol.PayloadBlockTypeSecret, []byte(sec)))
	rp.Type = protocol.PacketTypeSuccess

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

	ttl := a.getTTL(rq)
	namespace := string(ns)
	key := string(k)

	sec, ok, err := a.conf.LockManager.TryLock(namespace, key, ttl, s.Ctx)
	if err != nil {
		return a.replyOrFail(rp, err)
	}

	var success byte
	var secValue []byte

	if ok {
		success = 1
		secValue = []byte(sec)
	} else {
		success = 0
		secValue = emptySecret
	}

	if ok && ttl == 0 {
		s.RememberLock(SessionLock{
			Namespace: namespace,
			Key: key,
			Secret: sec,
		})
	}

	rp.Type = protocol.PacketTypeSuccess
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

	namespace := string(ns)
	key := string(k)
	secret := string(sec)

	err := a.conf.LockManager.Unlock(namespace, key, secret)
	if err != nil {
		return a.replyOrFail(rp, err)
	}

	s.ForgetLock(namespace, key)

	rp.Type = protocol.PacketTypeSuccess

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

func (a *App) getTTL(rq *protocol.Packet) time.Duration {
	raw, ok := a.getBlockValue(rq, protocol.PayloadBlockTypeTTL)
	if !ok {
		return 0
	}

	// TODO reject TTL payloads that are not 8 bytes; Uint64 panics on a short slice.
	return time.Duration(binary.BigEndian.Uint64(raw)) * time.Nanosecond
}
