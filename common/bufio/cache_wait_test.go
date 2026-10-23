package bufio

import (
	"io"
	"net/netip"
	"testing"

	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

func TestCopyPacketCachedBehindNATUsesBatchPath(t *testing.T) {
	t.Parallel()
	origin := M.SocksaddrFrom(netip.MustParseAddr("198.18.0.1"), 443)
	destination := M.ParseSocksaddrHostPort("example.com", 443)
	source := &testBatchReadPacketConn{reader: testPacketBatchReader{
		batches: []testPacketBatch{{
			payloads:     [][]byte{[]byte("a"), []byte("bc")},
			destinations: []M.Socksaddr{origin, origin},
		}},
	}}
	cached := NewCachedPacketConn(NewCachedPacketConn(source, testBuffers("second")[0], origin), testBuffers("first")[0], origin)
	conn := NewNATPacketConn(NewNetPacketConn(cached), origin, destination)
	writer := &testPacketBatchWriter{}
	n, err := CopyPacket(writer, conn)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, int64(len("first")+len("second")+len("abc")), n)
	require.True(t, writer.usedBatch)
	require.Equal(t, [][]byte{[]byte("first"), []byte("second"), []byte("a"), []byte("bc")}, writer.payloads)
	require.Equal(t, []M.Socksaddr{destination, destination, destination, destination}, writer.destinations)
}

func TestCopyPacketCachedBehindNATUsesConnectedBatchPath(t *testing.T) {
	t.Parallel()
	origin := M.SocksaddrFrom(netip.MustParseAddr("198.18.0.1"), 443)
	destination := M.ParseSocksaddrHostPort("example.com", 443)
	source := &testConnectedBatchReadPacketConn{reader: testConnectedPacketBatchReader{
		destination: origin,
		batches:     [][]byte{[]byte("a"), []byte("bc")},
	}}
	conn := NewNATPacketConn(NewNetPacketConn(NewCachedPacketConn(source, testBuffers("cached")[0], origin)), origin, destination)
	writer := &testPacketBatchWriter{}
	n, err := CopyPacket(writer, conn)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, int64(len("cached")+len("abc")), n)
	require.True(t, writer.usedBatch)
	require.Equal(t, [][]byte{[]byte("cached"), []byte("a"), []byte("bc")}, writer.payloads)
	require.Equal(t, []M.Socksaddr{destination, destination, destination}, writer.destinations)
}

func TestCachedPacketConnReadWaiter(t *testing.T) {
	t.Parallel()
	origin := M.SocksaddrFrom(netip.MustParseAddr("198.18.0.1"), 443)
	source := &testReadWaiterPacketConn{payloads: [][]byte{[]byte("a")}, destination: origin}
	cached := NewCachedPacketConn(source, testBuffers("cached")[0], origin)
	readWaiter, created := CreatePacketReadWaiter(cached)
	require.True(t, created)
	require.False(t, readWaiter.InitializeReadWaiter(N.ReadWaitOptions{FrontHeadroom: 8, RearHeadroom: 4}))

	buffer, packetDestination, err := readWaiter.WaitReadPacket()
	require.NoError(t, err)
	require.Equal(t, []byte("cached"), buffer.Bytes())
	require.GreaterOrEqual(t, buffer.Start(), 8)
	require.GreaterOrEqual(t, buffer.FreeLen(), 4)
	require.Equal(t, origin, packetDestination)
	buffer.Release()

	buffer, packetDestination, err = readWaiter.WaitReadPacket()
	require.NoError(t, err)
	require.Equal(t, []byte("a"), buffer.Bytes())
	require.Equal(t, origin, packetDestination)
	buffer.Release()

	_, _, err = readWaiter.WaitReadPacket()
	require.ErrorIs(t, err, io.EOF)
}

func TestCachedPacketConnTakenReturnsUpstreamWaiter(t *testing.T) {
	t.Parallel()
	origin := M.SocksaddrFrom(netip.MustParseAddr("198.18.0.1"), 443)
	source := &testReadWaiterPacketConn{destination: origin}
	cached := NewCachedPacketConn(source, testBuffers("cached")[0], origin)
	packet := cached.ReadCachedPacket()
	require.NotNil(t, packet)
	packet.Buffer.Release()
	N.PutPacketBuffer(packet)
	readWaiter, created := CreatePacketReadWaiter(cached)
	require.True(t, created)
	require.Same(t, source, readWaiter)
}

func TestCachedPacketConnWithoutUpstreamWaiter(t *testing.T) {
	t.Parallel()
	origin := M.SocksaddrFrom(netip.MustParseAddr("198.18.0.1"), 443)
	cached := NewCachedPacketConn(&testNetPacketConn{}, testBuffers("cached")[0], origin)
	_, created := CreatePacketReadWaiter(cached)
	require.False(t, created)
	_, created = CreatePacketBatchReadWaiter(cached)
	require.False(t, created)
	_, created = CreateConnectedPacketBatchReadWaiter(cached)
	require.False(t, created)
	packet := cached.ReadCachedPacket()
	require.NotNil(t, packet)
	require.Equal(t, []byte("cached"), packet.Buffer.Bytes())
	packet.Buffer.Release()
	N.PutPacketBuffer(packet)
}

type testBatchReadPacketConn struct {
	N.PacketConn
	reader testPacketBatchReader
}

func (c *testBatchReadPacketConn) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	return c.reader.ReadPacket(buffer)
}

func (c *testBatchReadPacketConn) InitializeReadWaiter(options N.ReadWaitOptions) (needCopy bool) {
	return c.reader.InitializeReadWaiter(options)
}

func (c *testBatchReadPacketConn) WaitReadPackets() ([]*buf.Buffer, []M.Socksaddr, error) {
	return c.reader.WaitReadPackets()
}

type testConnectedBatchReadPacketConn struct {
	N.PacketConn
	reader testConnectedPacketBatchReader
}

func (c *testConnectedBatchReadPacketConn) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	return c.reader.ReadPacket(buffer)
}

func (c *testConnectedBatchReadPacketConn) InitializeReadWaiter(options N.ReadWaitOptions) (needCopy bool) {
	return c.reader.InitializeReadWaiter(options)
}

func (c *testConnectedBatchReadPacketConn) WaitReadConnectedPackets() ([]*buf.Buffer, M.Socksaddr, error) {
	return c.reader.WaitReadConnectedPackets()
}

type testReadWaiterPacketConn struct {
	N.PacketConn
	payloads    [][]byte
	destination M.Socksaddr
}

func (c *testReadWaiterPacketConn) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	return M.Socksaddr{}, io.ErrUnexpectedEOF
}

func (c *testReadWaiterPacketConn) InitializeReadWaiter(options N.ReadWaitOptions) (needCopy bool) {
	return false
}

func (c *testReadWaiterPacketConn) WaitReadPacket() (*buf.Buffer, M.Socksaddr, error) {
	if len(c.payloads) == 0 {
		return nil, M.Socksaddr{}, io.EOF
	}
	buffer := testBuffersBytes(c.payloads[0])[0]
	c.payloads = c.payloads[1:]
	return buffer, c.destination, nil
}
