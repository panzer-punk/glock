package transport

import (
	"context"
	"glock/internal/protocol"
)

type Handler interface {
	OnConnect(ctx context.Context) context.Context
	Handle(rq *protocol.Packet, rp *protocol.Packet, ctx context.Context) error
}