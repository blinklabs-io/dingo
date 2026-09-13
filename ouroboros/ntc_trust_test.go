// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ouroboros

import (
	"testing"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/stretchr/testify/assert"
)

// TestIsTrustedNtCListener is the blinklabs-io/dingo#4183 review regression:
// ConfigureListeners used to grant every UseNtC listener gouroboros' relaxed
// mux/query timeouts and 2GiB reassembly buffer unconditionally, on the
// premise that "NtC is a trusted local channel" -- true for a Unix socket,
// but not for internal/node/node.go's other UseNtC listener,
// cfg.PrivateBindAddr:cfg.PrivatePort, an ordinary operator-configurable TCP
// address with no code-enforced loopback restriction. An operator who
// widens PrivateBindAddr beyond loopback (or reaches it via a Unix socket
// from any local user/process) would give any client that completes an NtC
// handshake an unbounded mux segment-read timeout, an unbounded
// LocalStateQuery timeout, and a 2GiB-per-connection reassembly buffer --
// gouroboros' own anti-DoS defaults exist specifically to bound this for an
// untrusted remote peer.
func TestIsTrustedNtCListener(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		l    connmanager.ListenerConfig
		want bool
	}{
		{
			name: "unix socket is always trusted",
			l: connmanager.ListenerConfig{
				ListenNetwork: "unix",
				ListenAddress: "/tmp/dingo.socket",
				UseNtC:        true,
			},
			want: true,
		},
		{
			name: "tcp bound to IPv4 loopback is trusted",
			l: connmanager.ListenerConfig{
				ListenNetwork: "tcp",
				ListenAddress: "127.0.0.1:3002",
				UseNtC:        true,
			},
			want: true,
		},
		{
			name: "tcp bound to IPv6 loopback is trusted",
			l: connmanager.ListenerConfig{
				ListenNetwork: "tcp",
				ListenAddress: "[::1]:3002",
				UseNtC:        true,
			},
			want: true,
		},
		{
			name: "tcp bound to localhost hostname resolves as loopback",
			l: connmanager.ListenerConfig{
				ListenNetwork: "tcp",
				ListenAddress: "localhost:3002",
				UseNtC:        true,
			},
			want: true,
		},
		{
			name: "tcp bound to a wildcard address is not trusted",
			l: connmanager.ListenerConfig{
				ListenNetwork: "tcp",
				ListenAddress: "0.0.0.0:3002",
				UseNtC:        true,
			},
			want: false,
		},
		{
			name: "tcp bound to a routable address is not trusted",
			l: connmanager.ListenerConfig{
				ListenNetwork: "tcp",
				ListenAddress: "10.0.0.5:3002",
				UseNtC:        true,
			},
			want: false,
		},
		{
			name: "unparseable tcp address is not trusted",
			l: connmanager.ListenerConfig{
				ListenNetwork: "tcp",
				ListenAddress: "not-a-valid-address",
				UseNtC:        true,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, isTrustedNtCListener(tt.l))
		})
	}
}

// TestConfigureListeners_UntrustedNtCListenerSkipsRelaxedTimeout covers the
// actual fix, not just the trust decision in isolation: a UseNtC listener
// ConfigureListeners cannot verify is local-only must not have
// WithMuxerSegmentReadTimeout(0) among its ConnectionOpts at all, so
// gouroboros' own 120s default mux segment-read timeout stays in force for
// it. There is no exported way to inspect a built ouroboros.ConnectionOptionFunc
// slice's effect directly, so this counts the length of ConnectionOpts a
// trusted vs. an untrusted NtC listener receive: the untrusted listener must
// end up with exactly one fewer option (the omitted WithMuxerSegmentReadTimeout).
func TestConfigureListeners_UntrustedNtCListenerSkipsRelaxedTimeout(t *testing.T) {
	t.Parallel()

	o := &Ouroboros{
		config: OuroborosConfig{},
	}

	trustedListener := connmanager.ListenerConfig{
		ListenNetwork: "unix",
		ListenAddress: "/tmp/dingo-test.socket",
		UseNtC:        true,
	}
	untrustedListener := connmanager.ListenerConfig{
		ListenNetwork: "tcp",
		ListenAddress: "0.0.0.0:3002",
		UseNtC:        true,
	}

	configured := o.ConfigureListeners(
		[]connmanager.ListenerConfig{trustedListener, untrustedListener},
	)
	assert.Len(t, configured, 2)

	assert.Equal(
		t,
		len(configured[0].ConnectionOpts),
		len(configured[1].ConnectionOpts)+1,
		"an untrusted NtC listener must receive exactly one fewer "+
			"ConnectionOpts entry than a trusted one -- the omitted "+
			"WithMuxerSegmentReadTimeout(0)",
	)
}
