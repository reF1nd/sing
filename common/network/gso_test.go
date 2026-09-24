package network

import "testing"

type testGSOPolicy bool

func (p testGSOPolicy) DisableGSO() bool { return bool(p) }

type testGSOUpstream struct{ upstream any }

func (w testGSOUpstream) Upstream() any { return w.upstream }

type testGSOWriter struct {
	testGSOUpstream
	writer any
}

func (w testGSOWriter) UpstreamWriter() any { return w.writer }

func TestGSOSendPolicy(t *testing.T) {
	for _, test := range []struct {
		name string
		conn any
		want bool
	}{
		{"automatic", struct{}{}, false},
		{"nested", testGSOUpstream{testGSOUpstream{testGSOPolicy(true)}}, true},
		{"writer disabled", testGSOWriter{testGSOUpstream{testGSOPolicy(false)}, testGSOPolicy(true)}, true},
		{"writer automatic", testGSOWriter{testGSOUpstream{testGSOPolicy(true)}, struct{}{}}, false},
		{"fallback writer", testGSOWriter{testGSOUpstream{testGSOPolicy(true)}, nil}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := IsGSODisabled(test.conn); got != test.want {
				t.Fatalf("GSO disabled=%v, want %v", got, test.want)
			}
		})
	}
}
