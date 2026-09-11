package app

import (
	"context"
	"errors"
	"glock/internal/lock"
	"glock/internal/protocol"
	"glock/internal/transport"
	"time"
)

const sessionKey = "session"

type Config struct {
	DefaultNamespace string
	DefaultTTL time.Duration

	SecretFactory *lock.SecretFactory
	LockManager *lock.LockManager
}

type App struct {
	conf *Config
	sessions map[*transport.Conn]*Session
}

func NewApp(conf *Config) *App {
	return &App{
		conf: conf,
		sessions: make(map[*transport.Conn]*Session),
	}
}

func (a *App) OnConnect(ctx context.Context) context.Context {
	s := NewSession()
	context.AfterFunc(ctx, func() {
		s.Close()
	})

	return context.WithValue(ctx, sessionKey, s)
}

func (a *App) Handle(pkt *protocol.Packet, ctx context.Context) (*protocol.Packet, error) {
	s, ok := ctx.Value(sessionKey).(*Session)
	if !ok {
		return nil, errors.New("session not found")
	}
}