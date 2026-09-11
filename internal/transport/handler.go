package transport

import (
	"context"
	"glock/internal/protocol"
)

type Handler interface {
	OnConnect(ctx context.Context) context.Context
	Handle(pkt *protocol.Packet, ctx context.Context) (*protocol.Packet, error)
}