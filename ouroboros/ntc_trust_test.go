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
	"net"
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
func TestConfigureListeners_UntrustedNtCListenerSkipsRelaxedTimeout(
	t *testing.T,
) {
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

// TestConfigureListeners_NormalizesTCPListenAddressToNumeric is the
// blinklabs-io/dingo#4183 review regression for a TOCTOU in
// isTrustedNtCListener: it resolved l.ListenAddress to classify the
// listener, but connmanager's startListener later binds the same
// listener's ListenAddress by calling net.Listen on the original,
// unresolved string -- a second, independent DNS lookup. If a hostname
// (or "localhost") resolved differently between the two lookups, a
// listener classified trusted from the first answer could bind to a
// different, non-loopback address on the second, handing that listener
// the relaxed timeouts and 2GiB reassembly buffer meant only for a
// verified-local one.
//
// ConfigureListeners now resolves a TCP NtC listener's address once and
// rewrites ListenAddress to the resulting numeric form before
// classifying it, so classification and the later bind are guaranteed to
// use the exact same literal address -- there is no second lookup left
// to disagree with the first. This proves that rewrite actually happens:
// a "localhost:0" input must come back as a numeric loopback address,
// not the original hostname string.
func TestConfigureListeners_NormalizesTCPListenAddressToNumeric(t *testing.T) {
	t.Parallel()

	o := &Ouroboros{
		config: OuroborosConfig{},
	}

	configured := o.ConfigureListeners([]connmanager.ListenerConfig{
		{
			ListenNetwork: "tcp",
			ListenAddress: "localhost:0",
			UseNtC:        true,
		},
	})
	assert.Len(t, configured, 1)
	assert.NotEqual(
		t,
		"localhost:0",
		configured[0].ListenAddress,
		"ConfigureListeners must rewrite a hostname ListenAddress to its "+
			"resolved numeric form, not leave it for a second, "+
			"independent resolution at bind time",
	)
	host, _, err := net.SplitHostPort(configured[0].ListenAddress)
	assert.NoError(t, err)
	assert.NotNil(
		t,
		net.ParseIP(host),
		"the rewritten ListenAddress %q must have a literal IP host",
		configured[0].ListenAddress,
	)
}
