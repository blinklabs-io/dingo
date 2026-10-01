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
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/leader"
	"github.com/blinklabs-io/dingo/ledger/leios"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/blinklabs-io/dingo/topology"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPrototypeTrustBypassesRejectedOnStandardNetworks proves a node cannot be
// constructed with a configuration that would hand the Musashi prototype's
// consensus/ledger trust bypasses to preview, preprod, or mainnet.
func TestPrototypeTrustBypassesRejectedOnStandardNetworks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		opts    []ConfigOptionFunc
		wantErr string
	}{
		{
			name: "preview cannot borrow the prototype magic",
			opts: []ConfigOptionFunc{
				WithNetwork("preview"),
				WithNetworkMagic(164),
			},
			wantErr: `network identity conflict: network "preview" with networkMagic 164`,
		},
		{
			name: "preprod cannot borrow the prototype magic",
			opts: []ConfigOptionFunc{
				WithNetwork("preprod"),
				WithNetworkMagic(164),
			},
			wantErr: `network identity conflict: network "preprod" with networkMagic 164`,
		},
		{
			name: "mainnet cannot borrow the prototype magic",
			opts: []ConfigOptionFunc{
				WithNetwork("mainnet"),
				WithNetworkMagic(164),
			},
			wantErr: `network identity conflict: network "mainnet" with networkMagic 164`,
		},
		{
			// The handshake uses the magic, so this configuration actually
			// joins preview while claiming the prototype's trust rules.
			name: "prototype name cannot borrow preview's magic",
			opts: []ConfigOptionFunc{
				WithNetwork("musashi"),
				WithNetworkMagic(2),
			},
			wantErr: `network identity conflict: network "musashi" with networkMagic 2`,
		},
		{
			name: "prototype name cannot borrow preprod's magic",
			opts: []ConfigOptionFunc{
				WithNetwork("musashi"),
				WithNetworkMagic(1),
			},
			wantErr: `network identity conflict: network "musashi" with networkMagic 1`,
		},
		{
			name: "musashi by name is still accepted",
			opts: []ConfigOptionFunc{WithNetwork("musashi")},
		},
		{
			name: "musashi by name and matching magic is still accepted",
			opts: []ConfigOptionFunc{
				WithNetwork("musashi"),
				WithNetworkMagic(164),
			},
		},
		{
			name: "preview is still accepted",
			opts: []ConfigOptionFunc{WithNetwork("preview")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := []ConfigOptionFunc{
				WithPrometheusRegistry(prometheus.NewRegistry()),
				WithListeners(ListenerConfig{
					ListenNetwork: "tcp",
					ListenAddress: "127.0.0.1:0",
				}),
			}
			opts = append(opts, tt.opts...)
			n, err := New(NewConfig(opts...))
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			// New starts the event bus' background goroutines; Stop releases them.
			t.Cleanup(func() { _ = n.Stop() })
		})
	}
}

// TestPrototypeTrustBypassesEnabledOnlyForMusashi asserts the predicate that
// gates SkipLeaderStakeThresholdCheck and SkipDijkstraTxValidation. This is
// the last line of defence: an embedder that constructs a Config directly and
// never runs startup validation still must not get the bypasses on a standard
// network.
func TestPrototypeTrustBypassesEnabledOnlyForMusashi(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		network      string
		networkMagic uint32
		want         bool
	}{
		{name: "musashi by name", network: "musashi", want: true},
		{
			name:         "musashi by name and magic",
			network:      "musashi",
			networkMagic: 164,
			want:         true,
		},
		{name: "musashi by magic only", networkMagic: 164, want: true},
		{name: "preview", network: "preview", networkMagic: 2},
		{name: "preprod", network: "preprod", networkMagic: 1},
		{name: "mainnet", network: "mainnet", networkMagic: 764824073},
		{name: "devnet", network: "devnet", networkMagic: 42},
		// Conflicting identities never enable the bypasses, even unvalidated.
		{
			name:         "preview with prototype magic",
			network:      "preview",
			networkMagic: 164,
		},
		{
			name:         "preprod with prototype magic",
			network:      "preprod",
			networkMagic: 164,
		},
		{
			name:         "musashi name with preview magic",
			network:      "musashi",
			networkMagic: 2,
		},
		{
			name:         "musashi name with preprod magic",
			network:      "musashi",
			networkMagic: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{cfg: &internalconfig.Config{
				Network:      tt.network,
				NetworkMagic: tt.networkMagic,
			}}
			assert.Equal(
				t,
				tt.want,
				c.prototypeTrustBypassesEnabled(),
				"prototypeTrustBypassesEnabled",
			)
		})
	}
}

// TestPrototypeTrustBypassesOffWithoutConfig guards the nil-config path used by
// zero-value Config values in tests and embedders.
func TestPrototypeTrustBypassesOffWithoutConfig(t *testing.T) {
	t.Parallel()

	c := &Config{}
	assert.False(t, c.prototypeTrustBypassesEnabled())
}

// TestMusashiProfileTrustBypassScope is the change-bar guard for the Musashi
// prototype profile: it documents exactly which LedgerStateConfig settings
// relax validation, and which of those the network profile is allowed to
// switch on by itself.
//
// The accepted non-validating behaviour on Musashi is limited to two settings:
//
//   - SkipLeaderStakeThresholdCheck downgrades a failed stake-derived leader
//     eligibility check to a warning. Every cryptographic header check (KES,
//     VRF proof, registered-VRF-key binding, opcert) still applies.
//   - SkipDijkstraTxValidation skips the per-transaction rule set for
//     Dijkstra-era transactions only; earlier eras are still validated (see
//     ledger.TestSkipDijkstraTxValidationScope).
//
// TrustedReplay is listed as known but is *not* network-derived: it is set by
// internal/node/load.go when replaying blocks this node already validated
// locally, which is a different trust context from following an untrusted
// network.
//
// A new Skip*/Trust*/Unsafe* field on LedgerStateConfig fails this test on
// purpose. Adding one is a deliberate widening of where dingo stops validating,
// and it should be classified here — prototype-only or not — rather than
// picking up a network default silently.
func TestMusashiProfileTrustBypassScope(t *testing.T) {
	t.Parallel()

	// Settings that relax validation, and whether the Musashi network profile
	// is permitted to enable them on its own.
	knownTrustSettings := map[string]bool{
		"SkipLeaderStakeThresholdCheck": true,
		"SkipDijkstraTxValidation":      true,
		"TrustedReplay":                 false,
	}

	cfgType := reflect.TypeFor[ledger.LedgerStateConfig]()
	found := make(map[string]bool)
	for field := range cfgType.Fields() {
		name := field.Name
		if strings.HasPrefix(name, "Skip") ||
			strings.HasPrefix(name, "Trust") ||
			strings.HasPrefix(name, "Unsafe") {
			found[name] = true
		}
	}
	for name := range found {
		assert.Contains(
			t,
			knownTrustSettings,
			name,
			"new validation-relaxing setting %q must be classified as "+
				"prototype-only or not; see this test's doc comment",
			name,
		)
	}
	for name := range knownTrustSettings {
		assert.Contains(
			t,
			found,
			name,
			"%q no longer exists; drop it from the known set",
			name,
		)
	}
}

// TestBlockPipelineRejectedOnMusashi proves a node cannot be constructed with
// the block-decode pipeline (issue #1894 phase 1) enabled against the
// Musashi prototype network. The vendored pipeline decode stage has no hook
// for dingo's Leios-extended-header Conway fallback
// (database/models.DecodeConwayBlock), so a Leios-extended block would fail
// strict decode and silently stall chain replay -- see configValidate's
// BlockPipelineEnabled/isMusashiNetwork check.
func TestBlockPipelineRejectedOnMusashi(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		network              string
		blockPipelineEnabled bool
		wantErr              string
	}{
		{
			name:                 "musashi with pipeline enabled is rejected",
			network:              "musashi",
			blockPipelineEnabled: true,
			wantErr:              "block pipeline is not supported on the Musashi prototype network",
		},
		{
			name:                 "musashi with pipeline disabled is accepted",
			network:              "musashi",
			blockPipelineEnabled: false,
		},
		{
			name:                 "preview with pipeline enabled is accepted",
			network:              "preview",
			blockPipelineEnabled: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewConfig(
				WithPrometheusRegistry(prometheus.NewRegistry()),
				WithListeners(ListenerConfig{
					ListenNetwork: "tcp",
					ListenAddress: "127.0.0.1:0",
				}),
				WithNetwork(tt.network),
			)
			cfg.cfg.BlockPipelineEnabled = tt.blockPipelineEnabled
			n, err := New(cfg)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			// New starts the event bus' background goroutines; Stop releases them.
			t.Cleanup(func() { _ = n.Stop() })
		})
	}
}

// TestBlockPipelineValidateRequiresPipelineEnabled proves a node cannot be
// constructed with the block pipeline's VRF/KES validate stage (issue #1894
// phase 3) enabled unless the block pipeline itself is also enabled -- see
// configValidate's BlockPipelineValidateEnabled check.
func TestBlockPipelineValidateRequiresPipelineEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                         string
		blockPipelineEnabled         bool
		blockPipelineValidateEnabled bool
		wantErr                      string
	}{
		{
			name:                         "validate without pipeline is rejected",
			blockPipelineEnabled:         false,
			blockPipelineValidateEnabled: true,
			wantErr:                      "requires block-pipeline-enabled",
		},
		{
			name:                         "validate with pipeline is accepted",
			blockPipelineEnabled:         true,
			blockPipelineValidateEnabled: true,
		},
		{
			name:                         "pipeline without validate is accepted",
			blockPipelineEnabled:         true,
			blockPipelineValidateEnabled: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewConfig(
				WithPrometheusRegistry(prometheus.NewRegistry()),
				WithListeners(ListenerConfig{
					ListenNetwork: "tcp",
					ListenAddress: "127.0.0.1:0",
				}),
				WithNetwork("preview"),
			)
			cfg.cfg.BlockPipelineEnabled = tt.blockPipelineEnabled
			cfg.cfg.BlockPipelineValidateEnabled = tt.blockPipelineValidateEnabled
			n, err := New(cfg)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			// New starts the event bus' background goroutines; Stop releases them.
			t.Cleanup(func() { _ = n.Stop() })
		})
	}
}

// peerSnapshotTopology builds a topology carrying a peer snapshot for the
// given network magic, plus a configured bootstrap peer. The bootstrap peer
// matters because a node that accepts the snapshot drops it.
func peerSnapshotTopology(snapshotMagic uint32) *topology.TopologyConfig {
	return &topology.TopologyConfig{
		BootstrapPeers: []topology.TopologyConfigP2PBootstrapPeer{
			{Address: "backup.example", Port: 3001},
		},
		PeerSnapshot: &topology.PeerSnapshotConfig{
			NetworkMagic:        snapshotMagic,
			NodeToClientVersion: 23,
			Point: topology.PeerSnapshotPoint{
				BlockPointHash: "d6792f8031323804b7ac44a67747de78ed70fd307bb5ffddc5147844d9363b30",
				BlockPointSlot: 110741160,
			},
			AllLedgerPools: []topology.PeerSnapshotLedgerPool{
				{
					Relays: []topology.TopologyConfigP2PAccessPoint{
						{Address: "relay.example", Port: 3001},
					},
				},
			},
		},
	}
}

// TestPeerSnapshotFromAnotherNetworkRejected proves a peer snapshot naming a
// different network cannot start the node.
//
// The snapshot's relays replace the configured bootstrap peers during Genesis
// selection, so accepting a foreign snapshot points the node at another
// network's relays and discards the only addresses that could have worked.
// Each of those relays is then denied at the handshake on a network-magic
// mismatch, which leaves the node with no peers at all and no way back to the
// bootstrap list — a failure that looks like a network outage rather than the
// misconfiguration it is.
func TestPeerSnapshotFromAnotherNetworkRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		network       string
		snapshotMagic uint32
		wantErr       string
	}{
		{
			name:          "preview node given a mainnet snapshot",
			network:       "preview",
			snapshotMagic: 764824073,
			wantErr:       "network magic 764824073 does not match configured network magic 2",
		},
		{
			name:          "mainnet node given a preprod snapshot",
			network:       "mainnet",
			snapshotMagic: 1,
			wantErr:       "network magic 1 does not match configured network magic 764824073",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(NewConfig(
				WithPrometheusRegistry(prometheus.NewRegistry()),
				WithListeners(ListenerConfig{
					ListenNetwork: "tcp",
					ListenAddress: "127.0.0.1:0",
				}),
				WithNetwork(tt.network),
				WithTopologyConfig(peerSnapshotTopology(tt.snapshotMagic)),
			))
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestPeerSnapshotMatchingNetworkAccepted is the negative case: a snapshot for
// the node's own network must still start, or the check would break every
// Genesis bootstrap it is meant to protect.
func TestPeerSnapshotMatchingNetworkAccepted(t *testing.T) {
	t.Parallel()

	n, err := New(NewConfig(
		WithPrometheusRegistry(prometheus.NewRegistry()),
		WithListeners(ListenerConfig{
			ListenNetwork: "tcp",
			ListenAddress: "127.0.0.1:0",
		}),
		WithNetwork("preview"),
		WithTopologyConfig(peerSnapshotTopology(2)),
	))
	require.NoError(t, err)
	require.NotNil(t, n)
}

func TestPeerSnapshotWithoutNetworkMagicRejected(t *testing.T) {
	t.Parallel()

	_, err := New(NewConfig(
		WithPrometheusRegistry(prometheus.NewRegistry()),
		WithListeners(ListenerConfig{
			ListenNetwork: "tcp",
			ListenAddress: "127.0.0.1:0",
		}),
		WithNetwork("preview"),
		WithTopologyConfig(peerSnapshotTopology(0)),
	))
	require.ErrorContains(t, err, "network magic must be specified")
}

// TestStopWithDeadlineReturnsWhenStopReturns covers the ordinary case: a
// component that stops promptly must return its own error (or nil) unchanged,
// with no drain escalation attached.
func TestStopWithDeadlineReturnsWhenStopReturns(t *testing.T) {
	t.Parallel()

	require.NoError(t, stopWithDeadline(
		time.Minute,
		"prompt component",
		func() error { return nil },
	))

	sentinel := errors.New("stop failed")
	err := stopWithDeadline(
		time.Minute,
		"failing component",
		func() error { return sentinel },
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
	assert.NotErrorIs(t, err, errStorageDrainUnconfirmed,
		"a component that reported a failure did stop; only an unfinished "+
			"wait leaves a goroutine possibly still using storage")
}

// TestStopWithDeadlineEscalatesAnUnfinishedStop is the point of the change.
//
// These Stop calls cancel their context and then wait on a WaitGroup with no
// bound, so a goroutine that does not observe the cancellation blocks the
// whole live restore/truncate indefinitely — past the configured shutdown
// timeout, with no error and no way for the caller to react. Once bounded, an
// unfinished stop has to escalate to errStorageDrainUnconfirmed: the goroutine
// may still be reading or writing n.db, so Restore/Truncate must abandon the
// operation and force a supervised restart rather than reopen storage
// underneath it.
func TestStopWithDeadlineEscalatesAnUnfinishedStop(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	err := stopWithDeadline(
		10*time.Millisecond,
		"wedged component",
		func() error {
			<-release
			return nil
		},
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, errStorageDrainUnconfirmed,
		"an unfinished stop must force a supervised restart, not a resume")
	assert.ErrorContains(t, err, "wedged component")
}

// TestStopWithDeadlineIgnoresCallerCancellation pins the deliberate choice not
// to consult the caller's context.
//
// Cancelling a restore must not escalate a component that would have stopped
// cleanly into a supervised restart, so a cancelled context neither shortens
// the wait nor changes the result. The deadline alone bounds it.
func TestStopWithDeadlineIgnoresCallerCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stopped := make(chan struct{})
	err := stopWithDeadline(
		time.Minute,
		"prompt component",
		func() error { close(stopped); return nil },
	)
	require.NoError(t, err, "a clean stop under a cancelled caller is clean")
	assert.NotErrorIs(t, err, errStorageDrainUnconfirmed)
	select {
	case <-stopped:
	default:
		t.Fatal("stop should have run to completion")
	}
	_ = ctx
}

// TestQuiesceComponentStopsCoverEveryUnboundedStop pins the set of components
// routed through the deadline.
//
// Each of these has a Stop that cancels its own context and then waits on a
// sync.WaitGroup with no deadline of its own, so a call site that went back to
// calling Stop directly would drop out of this list and escape the bound —
// which is exactly what happened to the database lifecycle manager before.
func TestQuiesceComponentStopsCoverEveryUnboundedStop(t *testing.T) {
	t.Parallel()

	n := &Node{
		blockForger:          &forging.BlockForger{},
		leaderElection:       &leader.Election{},
		leiosPipelineManager: &leios.PipelineManager{},
		leiosVoteManager:     &leios.VoteManager{},
		snapshotMgr:          &snapshot.Manager{},
		dbLifecycleMgr:       &dblifecycle.Manager{},
	}

	var names []string
	for _, cs := range n.quiesceComponentStops() {
		names = append(names, cs.name)
	}
	assert.Equal(t, []string{
		"block forger",
		"leader election",
		"leios pipeline manager",
		"leios vote manager",
		"snapshot manager",
		"database lifecycle manager",
	}, names)
}

// TestQuiesceComponentStopsSkipsAbsentComponents covers a node that never
// built the optional components, which is the ordinary case for a
// non-block-producing or non-Leios node.
func TestQuiesceComponentStopsSkipsAbsentComponents(t *testing.T) {
	t.Parallel()

	n := &Node{snapshotMgr: &snapshot.Manager{}}

	stops := n.quiesceComponentStops()
	require.Len(t, stops, 1)
	assert.Equal(t, "snapshot manager", stops[0].name)
}

// TestQuiesceEscalatesAStopThatNeverReturns drives the production quiesce path
// with a component whose Stop blocks until released.
//
// This is what the stopWithDeadline unit tests above cannot show: that
// quiesceForLiveLifecycleOp actually routes its component stops through the
// deadline and surfaces the escalation. Restore and Truncate branch on
// errStorageDrainUnconfirmed to force a supervised restart instead of
// reopening storage, so a quiesce that swallowed or never reached it would let
// them resume on a database a live goroutine may still be using.
// Not t.Parallel: swaps the package-level componentStopsForQuiesce seam.
func TestQuiesceEscalatesAStopThatNeverReturns(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	previous := componentStopsForQuiesce
	t.Cleanup(func() { componentStopsForQuiesce = previous })
	componentStopsForQuiesce = func(*Node) []namedStop {
		return []namedStop{{
			name: "wedged component",
			stop: func() error {
				<-release
				return nil
			},
		}}
	}

	n := &Node{}
	n.config.cfg = &internalconfig.Config{ShutdownTimeout: "20ms"}
	n.config.logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	// Bounded here too: a quiesce that called the stop directly would block on
	// the wedged component forever, and a hung test reports far worse than a
	// failing one.
	done := make(chan error, 1)
	go func() { done <- n.quiesceForLiveLifecycleOp(context.Background()) }()

	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("quiesce did not return; its component stops are not bounded")
	}
	require.Error(t, err)
	assert.ErrorIs(t, err, errStorageDrainUnconfirmed,
		"Restore/Truncate branch on this to force a supervised restart")
	assert.ErrorContains(t, err, "wedged component")
}

// TestQuiesceReportsAStopFailureWithoutEscalating is the negative case. A
// component that returns an error has stopped, so the caller may still resume
// on the untouched data directory; only an unfinished wait means a goroutine
// might still be using the database.
func TestQuiesceReportsAStopFailureWithoutEscalating(t *testing.T) {
	previous := componentStopsForQuiesce
	t.Cleanup(func() { componentStopsForQuiesce = previous })
	sentinel := errors.New("stop reported a failure")
	componentStopsForQuiesce = func(*Node) []namedStop {
		return []namedStop{{
			name: "failing component",
			stop: func() error { return sentinel },
		}}
	}

	n := &Node{}
	n.config.cfg = &internalconfig.Config{ShutdownTimeout: "1m"}
	n.config.logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	err := n.quiesceForLiveLifecycleOp(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
	assert.NotErrorIs(t, err, errStorageDrainUnconfirmed)
}
