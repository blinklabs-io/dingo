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
	"errors"
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/event"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/ledger/leios"
	hostplugin "github.com/blinklabs-io/dingo/plugin"
	"github.com/blinklabs-io/gouroboros/cbor"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

type fakeQuorumSource struct {
	possible bool
	err      error
	epoch    uint64
	tau      *big.Rat
	calls    int
}

func (f *fakeQuorumSource) CommitteeCanReachQuorum(
	epoch uint64,
	tau *big.Rat,
) (bool, error) {
	f.calls++
	f.epoch, f.tau = epoch, tau
	return f.possible, f.err
}

func dijkstraQuorumPParams(tau *big.Rat) *gdijkstra.DijkstraProtocolParameters {
	return &gdijkstra.DijkstraProtocolParameters{
		LeiosCommitteeSize:        900,
		LeiosQuorumStakeThreshold: &cbor.Rat{Rat: tau},
	}
}

func TestLeiosQuorumPossibleForAsksTheCommitteeForTheCurrentThreshold(
	t *testing.T,
) {
	t.Parallel()
	source := &fakeQuorumSource{possible: true}
	require.True(
		t,
		leiosQuorumPossibleFor(source, 9, dijkstraQuorumPParams(big.NewRat(3, 4))),
	)
	require.Equal(t, 1, source.calls)
	require.Equal(t, uint64(9), source.epoch)
	require.Zero(t, big.NewRat(3, 4).Cmp(source.tau))

	source.possible = false
	require.False(
		t,
		leiosQuorumPossibleFor(source, 9, dijkstraQuorumPParams(big.NewRat(3, 4))),
	)
}

// Anything short of a committee known to reach its quorum keeps the
// non-Leios capacity.
func TestLeiosQuorumPossibleForFallsBackWithoutAnAnswer(t *testing.T) {
	t.Parallel()
	failing := &fakeQuorumSource{possible: true, err: errors.New("no snapshot")}
	require.False(
		t,
		leiosQuorumPossibleFor(failing, 9, dijkstraQuorumPParams(big.NewRat(3, 4))),
		"a failed committee lookup",
	)
	require.False(
		t,
		leiosQuorumPossibleFor(&fakeQuorumSource{possible: true}, 9, nil),
		"no current parameters",
	)
	require.False(
		t,
		leiosQuorumPossibleFor(
			&fakeQuorumSource{possible: true}, 9, &gdijkstra.DijkstraProtocolParameters{},
		),
		"no quorum threshold in the parameters",
	)
	require.False(
		t,
		leiosQuorumPossibleFor(nil, 9, dijkstraQuorumPParams(big.NewRat(3, 4))),
		"no vote manager",
	)
}

func TestLeiosGatedMempoolCapacityFollowsQuorum(t *testing.T) {
	t.Parallel()
	enlarged := int64(internalconfig.DefaultMempoolCapacityLeios)
	praos := int64(internalconfig.DefaultMempoolCapacityPraos)
	possible := false
	capacity := leiosGatedMempoolCapacity(enlarged, func() bool { return possible })

	require.Equal(t, praos, capacity(), "committee below quorum")
	possible = true
	require.Equal(t, enlarged, capacity(), "committee reaches quorum")
	possible = false
	require.Equal(t, praos, capacity(), "quorum lost again")
}

// A configured capacity below the Praos default is never raised by waiting.
func TestLeiosGatedMempoolCapacityNeverExceedsTheConfiguredCapacity(
	t *testing.T,
) {
	t.Parallel()
	small := int64(internalconfig.DefaultMempoolCapacityPraos) / 2
	for _, possible := range []bool{false, true} {
		capacity := leiosGatedMempoolCapacity(small, func() bool { return possible })
		require.Equal(t, small, capacity())
	}
}

func newMempoolCapacityNode(
	capacity any,
	leiosDefault bool,
) *Node {
	var cfg internalconfig.Config
	cfg.Plugins.Mempool = hostplugin.Selection{
		Provider: "default",
		Config:   map[string]any{"capacity": capacity},
	}
	return &Node{config: Config{
		cfg: &cfg,
		pluginSelections: map[hostplugin.Capability]hostplugin.Selection{
			hostplugin.CapabilityMempool: cfg.Plugins.Mempool,
		},
		mempoolCapacityLeiosDefault: leiosDefault,
	}}
}

// The mempool's capacity function follows quorum only for a defaulted Leios
// capacity, and keeps the Praos capacity while no committee can be consulted.
func TestMempoolDependenciesCapacityFunc(t *testing.T) {
	t.Parallel()
	enlarged := int64(internalconfig.DefaultMempoolCapacityLeios)
	praos := int64(internalconfig.DefaultMempoolCapacityPraos)

	t.Run("explicit capacity is fixed", func(t *testing.T) {
		t.Parallel()
		deps := newMempoolCapacityNode(enlarged, false).mempoolDependencies()
		require.Nil(t, deps.CapacityFunc)
	})
	t.Run("defaulted capacity without a committee", func(t *testing.T) {
		t.Parallel()
		deps := newMempoolCapacityNode(enlarged, true).mempoolDependencies()
		require.NotNil(t, deps.CapacityFunc)
		require.Equal(t, praos, deps.CapacityFunc())
	})
	t.Run("defaulted capacity with a vote manager but no ledger", func(t *testing.T) {
		t.Parallel()
		n := newMempoolCapacityNode(enlarged, true)
		voteManager, err := leios.NewVoteManager(leios.VoteManagerConfig{
			Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
			EventBus:       event.NewEventBus(nil, nil),
			StakeProvider:  startupLeiosStakeProvider{},
			EpochProvider:  startupLeiosEpochProvider{},
			ParamsProvider: startupLeiosParamsProvider{},
			KeyProvider:    startupLeiosKeyProvider{},
		})
		require.NoError(t, err)
		n.leiosCertificateVerifier.Store(voteManager)
		deps := n.mempoolDependencies()
		require.NotNil(t, deps.CapacityFunc)
		require.Equal(t, praos, deps.CapacityFunc())
	})
}
