package transport

import (
	"context"
	"glock/internal/protocol"
	"net"
)

type Backend interface {
	Start(Handler) error
	Shutdown(context.Context) error
}

type Conn struct {
	nConn  net.Conn
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *Conn) Close() {
	c.cancel()
	c.nConn.Close()
}

func (c *Conn) writePacket(buf *[]byte, pkt *protocol.Packet) error {
	*buf = pkt.Serialize((*buf)[:0])
	_, err := c.nConn.Write(*buf)
	return err
}
