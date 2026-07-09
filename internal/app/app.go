package app

import (
	"context"
	"encoding/binary"
	"glock/internal/lock"
	"glock/internal/service"
	"time"
)

type Container struct {
	LockService *service.LockService
	// NamespaceService *service.NamespaceService
	Config AppConfig
}

type AppConfig struct {
	DefaultNamespace string
	DefaultTTL time.Duration
	DefaultSecretFactory lock.SecretFactory
}

type App struct {
	Container *Container
	Routes map[PacketType]func(*Packet, context.Context) Packet
}

func NewApp(container *Container) *App {
	app := &App{
		Container: container,
		Routes: make(map[PacketType]func(*Packet, context.Context) Packet),
	}

	initRouting(app)

	return app
}

func initRouting(app *App) error {
	app.Routes[PacketTypeLock] = app.handleLock
	app.Routes[PacketTypeTryLock] = app.handleTryLock
	app.Routes[PacketTypeUnlock] = app.handleUnlock

	return nil
}

func (a *App)HandlePacket(p *Packet, ctx context.Context) Packet {
	tp := p.Type
	h, ok := a.Routes[tp]
	if !ok {
		rp := NewPacket(PacketTypeError)
		rp.AddBlock(NewPayloadBlock(PayloadBlockTypeError, []byte("Unknow packet type")))
		return rp
	}

	return h(p, ctx)
}

func (a *App) handleLock(p *Packet, ctx context.Context) Packet {
	rp := NewPacket(PacketTypeSuccess)

	ns, ok := getBlockValue(p, PayloadBlockTypeNamespace)
	if !ok {
		ns = []byte(a.Container.Config.DefaultNamespace)
	}

	k, ok := getBlockValue(p, PayloadBlockTypeKey)
	if !ok {
		rp.Type = PacketTypeError
		rp.AddBlock(NewPayloadBlock(PayloadBlockTypeError, []byte("Missing key")))
		return rp
	}

	sec, err := a.Container.LockService.Lock(string(ns), string(k), ctx)
	if err != nil {
		rp.Type = PacketTypeError
		rp.AddBlock(NewPayloadBlock(PayloadBlockTypeError, []byte(err.Error())))
		return rp
	}

	rp.AddBlock(NewPayloadBlock(PayloadBlockTypeSecret, []byte(sec.Value())))

	return rp
}

func getBlockValue(p *Packet, tp PayloadBlockType) ([]byte, bool) {
	b, ok := p.FindBlock(tp)
	if !ok {
		return nil, false
	}

	return b.Value, true
}

func (a *App) handleTryLock(p *Packet, ctx context.Context) Packet {
	rp := NewPacket(PacketTypeSuccess)

	ns, ok := getBlockValue(p, PayloadBlockTypeNamespace)
	if !ok {
		ns = []byte(a.Container.Config.DefaultNamespace)
	}

	k, ok := getBlockValue(p, PayloadBlockTypeKey)
	if !ok {
		rp.Type = PacketTypeError
		rp.AddBlock(NewPayloadBlock(PayloadBlockTypeError, []byte("Missing key")))
		return rp
	}

	ttl, ok := getBlockValue(p, PayloadBlockTypeTTL)
	if !ok {
		ttl = binary.BigEndian.AppendUint64(make([]byte, 8), uint64(a.Container.Config.DefaultTTL))
	}

	ttlDuration := time.Duration(binary.BigEndian.Uint64(ttl))
	sec, ok := a.Container.LockService.TryLock(string(ns), string(k), ttlDuration, ctx)

	var success byte
	var secValue []byte

	if ok {
		success = 1
		secValue = []byte(sec.Value())
	} else {
		success = 0
		secValue = []byte{0}
	}

	rp.AddBlock(NewPayloadBlock(PayloadBlockTypeSecret, secValue))
	rp.AddBlock(NewPayloadBlock(PayloadBlockTypeSuccess, []byte{success}))

	return rp
}

func (a *App) handleUnlock(p *Packet, ctx context.Context) Packet {
	rp := NewPacket(PacketTypeSuccess)

	ns, ok := getBlockValue(p, PayloadBlockTypeNamespace)
	if !ok {
		ns = []byte(a.Container.Config.DefaultNamespace)
	}

	k, ok := getBlockValue(p, PayloadBlockTypeKey)
	if !ok {
		rp.Type = PacketTypeError
		rp.AddBlock(NewPayloadBlock(PayloadBlockTypeError, []byte("Missing key")))
		return rp
	}

	sec, ok := getBlockValue(p, PayloadBlockTypeSecret)
	if !ok {
		rp.Type = PacketTypeError
		rp.AddBlock(NewPayloadBlock(PayloadBlockTypeError, []byte("Missing secret")))
		return rp
	}

	secObj, err := a.Container.Config.DefaultSecretFactory.FromValue(string(sec))
	if err != nil {
		rp.Type = PacketTypeError
		rp.AddBlock(NewPayloadBlock(PayloadBlockTypeError, []byte(err.Error())))
		return rp
	}

	err = a.Container.LockService.Unlock(string(ns), string(k), secObj)
	if err != nil {
		rp.Type = PacketTypeError
		rp.AddBlock(NewPayloadBlock(PayloadBlockTypeError, []byte(err.Error())))
		return rp
	}

	return rp
}