package transport

import (
	"context"
	"glock/internal/protocol"
)

type Handler interface {
	OnConnect(ctx context.Context) context.Context
	//TODO pass response packet to the handler to reduce allocations
	Handle(pkt *protocol.Packet, ctx context.Context) (*protocol.Packet, error)
}