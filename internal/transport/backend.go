package transport

import (
	"context"
	"net"
)

type Backend interface {
	Start(*Handler) error
	Shutdown(context.Context) error
}

type Conn struct {
	nConn net.Conn
	ctx context.Context
	cancel context.CancelFunc
}

func (c *Conn) Close() {
	c.cancel()
	c.nConn.Close()
}