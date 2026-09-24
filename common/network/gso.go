package network

import (
	"net"

	"github.com/sagernet/sing/common"
)

// GSOControl is an optional, immutable send policy on a UDP connection or dialer.
// Disabling GSO must not disable other batch I/O or socket optimizations.
type GSOControl interface {
	DisableGSO() bool
}

// IsGSODisabled finds the send policy through connection wrappers. Connections
// without a policy keep automatic GSO detection. A policy must not change during use.
func IsGSODisabled(conn any) bool {
	for {
		if policy, loaded := conn.(GSOControl); loaded {
			return policy.DisableGSO()
		}
		if writer, loaded := conn.(WithUpstreamWriter); loaded {
			if upstream := writer.UpstreamWriter(); upstream != nil {
				conn = upstream
				continue
			}
		}
		switch upstream := conn.(type) {
		case common.WithUpstream:
			conn = upstream.Upstream()
		case interface{ NetConn() net.Conn }:
			conn = upstream.NetConn()
		default:
			return false
		}
	}
}
