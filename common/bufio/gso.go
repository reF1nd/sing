package bufio

import "net"

// UDPConnWithoutGSO retains the UDP socket interfaces and packet batch I/O while
// prohibiting send segmentation. Wrap the socket before adding protocol or
// accounting wrappers, so writers created by those wrappers inherit the policy.
type UDPConnWithoutGSO struct {
	*ExtendedUDPConn
}

func NewUDPConnWithoutGSO(conn *net.UDPConn) *UDPConnWithoutGSO {
	return &UDPConnWithoutGSO{ExtendedUDPConn: &ExtendedUDPConn{UDPConn: conn}}
}

func (*UDPConnWithoutGSO) DisableGSO() bool { return true }
