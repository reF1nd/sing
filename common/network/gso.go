package network

import "github.com/sagernet/sing/common"

// GSOControl is an optional, immutable send policy on a UDP connection or dialer.
// Disabling GSO must not disable other batch I/O or socket optimizations.
type GSOControl interface {
	DisableGSO() bool
}

// IsGSODisabled finds the policy through connection wrappers. Connections without
// a policy keep automatic GSO detection. A policy must not change during use.
func IsGSODisabled(conn any) bool {
	policy, loaded := common.Cast[GSOControl](conn)
	return loaded && policy.DisableGSO()
}
