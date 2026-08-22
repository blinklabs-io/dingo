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

package dingo

import (
	"testing"
	"time"

	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestProgrammaticConfigRejectsBothKESSources pins that the mutually exclusive
// KES key sources are enforced on the library path.
//
// internal/config's Validate rejected the combination, but that runs only from
// cmd/dingo; Node construction never calls it. A library consumer that set both
// got kesAgentEnabled() true, the agent silently preferred, and the local
// --shelley-kes-key file ignored — the operator's explicit choice of key source
// discarded with no diagnostic.
func TestProgrammaticConfigRejectsBothKESSources(t *testing.T) {
	// A fresh registry per node: the same one cannot serve two nodes, since
	// each registers the same build-info collector.
	producerOpts := func(extra ...ConfigOptionFunc) []ConfigOptionFunc {
		return append([]ConfigOptionFunc{
			WithNetworkMagic(1),
			WithPrometheusRegistry(prometheus.NewRegistry()),
			WithListeners(ListenerConfig{
				ListenNetwork: "tcp",
				ListenAddress: "127.0.0.1:0",
			}),
			WithBlockProducer(true),
			WithShelleyVRFKey("/keys/vrf.skey"),
			WithShelleyOperationalCertificate("/keys/opcert.cert"),
		}, extra...)
	}

	_, err := New(NewConfig(producerOpts(
		WithShelleyKESKey("/keys/kes.skey"),
		WithShelleyKESAgentSocket("/run/kes-agent.sock"),
	)...))
	require.ErrorContains(
		t,
		err,
		"cannot set both shelleyKesKey and shelleyKesAgentSocket",
	)

	// Either source alone must still be accepted, or the check would break the
	// feature it guards.
	for _, tc := range []struct {
		name string
		opt  ConfigOptionFunc
	}{
		{"key file only", WithShelleyKESKey("/keys/kes.skey")},
		{
			"agent socket only",
			WithShelleyKESAgentSocket("/run/kes-agent.sock"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, err := New(NewConfig(producerOpts(tc.opt)...))
			require.NoError(t, err)
			// New starts the event bus' background goroutines; Stop releases
			// them.
			t.Cleanup(func() { _ = n.Stop() })
		})
	}
}

// TestKESAgentSignTimeoutOption pins that the configured sign timeout reaches
// the mirror the agent client is built from. The default exceeded a mainnet
// slot and nothing could change it.
func TestKESAgentSignTimeoutOption(t *testing.T) {
	const want = 250 * time.Millisecond
	cfg := &Config{cfg: &internalconfig.Config{}}
	WithShelleyKESAgentSignTimeout(want)(cfg)
	require.Equal(t, want, cfg.shelleyKESAgentSignTimeout)
	require.Equal(t, want, cfg.cfg.ShelleyKESAgentSignTimeout)
}
