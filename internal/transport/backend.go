package transport

import (
	"bytes"
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

func (c *Conn) writePacket(buf *bytes.Buffer, pkt *protocol.Packet) error {
	buf.Reset()
	pkt.Serialize(buf)
	_, err := c.nConn.Write(buf.Bytes())
	return err
}
