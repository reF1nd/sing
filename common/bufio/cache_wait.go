package bufio

import (
	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

var (
	_ N.PacketReadWaitCreator               = (*CachedPacketConn)(nil)
	_ N.PacketBatchReadWaitCreator          = (*CachedPacketConn)(nil)
	_ N.ConnectedPacketBatchReadWaitCreator = (*CachedPacketConn)(nil)
)

func (c *CachedPacketConn) CreateReadWaiter() (N.PacketReadWaiter, bool) {
	readWaiter, created := CreatePacketReadWaiter(c.PacketConn)
	if !created {
		return nil, false
	}
	if c.taken.Load() {
		return readWaiter, true
	}
	return &waitCachedPacketConn{CachedPacketConn: c, readWaiter: readWaiter}, true
}

func (c *CachedPacketConn) CreatePacketBatchReadWaiter() (N.PacketBatchReadWaiter, bool) {
	readWaiter, created := CreatePacketBatchReadWaiter(c.PacketConn)
	if !created {
		return nil, false
	}
	if c.taken.Load() {
		return readWaiter, true
	}
	return &batchWaitCachedPacketConn{CachedPacketConn: c, readWaiter: readWaiter}, true
}

func (c *CachedPacketConn) CreateConnectedPacketBatchReadWaiter() (N.ConnectedPacketBatchReadWaiter, bool) {
	readWaiter, created := CreateConnectedPacketBatchReadWaiter(c.PacketConn)
	if !created {
		return nil, false
	}
	if c.taken.Load() {
		return readWaiter, true
	}
	return &connectedBatchWaitCachedPacketConn{CachedPacketConn: c, readWaiter: readWaiter}, true
}

func (c *CachedPacketConn) waitCachedPacket(options N.ReadWaitOptions) (*buf.Buffer, M.Socksaddr, bool) {
	packet := c.ReadCachedPacket()
	if packet == nil {
		return nil, M.Socksaddr{}, false
	}
	buffer := options.Copy(packet.Buffer)
	destination := packet.Destination
	N.PutPacketBuffer(packet)
	return buffer, destination, true
}

type waitCachedPacketConn struct {
	*CachedPacketConn
	readWaiter N.PacketReadWaiter
	options    N.ReadWaitOptions
}

func (c *waitCachedPacketConn) InitializeReadWaiter(options N.ReadWaitOptions) (needCopy bool) {
	c.options = options
	return c.readWaiter.InitializeReadWaiter(options)
}

func (c *waitCachedPacketConn) WaitReadPacket() (*buf.Buffer, M.Socksaddr, error) {
	buffer, destination, loaded := c.waitCachedPacket(c.options)
	if loaded {
		return buffer, destination, nil
	}
	return c.readWaiter.WaitReadPacket()
}

type batchWaitCachedPacketConn struct {
	*CachedPacketConn
	readWaiter N.PacketBatchReadWaiter
	options    N.ReadWaitOptions
}

func (c *batchWaitCachedPacketConn) InitializeReadWaiter(options N.ReadWaitOptions) (needCopy bool) {
	c.options = options
	return c.readWaiter.InitializeReadWaiter(options)
}

func (c *batchWaitCachedPacketConn) WaitReadPackets() ([]*buf.Buffer, []M.Socksaddr, error) {
	buffer, destination, loaded := c.waitCachedPacket(c.options)
	if loaded {
		return []*buf.Buffer{buffer}, []M.Socksaddr{destination}, nil
	}
	return c.readWaiter.WaitReadPackets()
}

type connectedBatchWaitCachedPacketConn struct {
	*CachedPacketConn
	readWaiter N.ConnectedPacketBatchReadWaiter
	options    N.ReadWaitOptions
}

func (c *connectedBatchWaitCachedPacketConn) InitializeReadWaiter(options N.ReadWaitOptions) (needCopy bool) {
	c.options = options
	return c.readWaiter.InitializeReadWaiter(options)
}

func (c *connectedBatchWaitCachedPacketConn) WaitReadConnectedPackets() ([]*buf.Buffer, M.Socksaddr, error) {
	buffer, destination, loaded := c.waitCachedPacket(c.options)
	if loaded {
		return []*buf.Buffer{buffer}, destination, nil
	}
	return c.readWaiter.WaitReadConnectedPackets()
}
