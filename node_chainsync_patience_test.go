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
	"net"
	"testing"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChainsyncConfigCarriesLimitOnPatience pins the composition the live
// restore/truncate rebuild shares with Run: the configured Limit on Patience
// and the Genesis gate that decides when it applies.
func TestChainsyncConfigCarriesLimitOnPatience(t *testing.T) {
	t.Parallel()
	n := &Node{config: NewConfig(WithGenesisLimitOnPatience(true, 7, 3))}

	cfg := n.chainsyncConfig()
	assert.Equal(t, chainsync.PatienceConfig{
		Enabled:  true,
		Capacity: 7,
		Rate:     3,
	}, cfg.Patience)
	require.NotNil(t, cfg.PatienceActiveFunc)
	require.NotNil(t, cfg.ObservedHeaderLimitFunc)
	assert.False(t, cfg.PatienceActiveFunc(), "no chain selector yet")

	n.chainSelector = chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{
			GenesisMode:        true,
			GenesisWindowSlots: 30,
		},
	)
	assert.True(t, cfg.PatienceActiveFunc())
	assert.Equal(t, 30, cfg.ObservedHeaderLimitFunc())

	n.chainSelector = chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{},
	)
	assert.False(t, cfg.PatienceActiveFunc(), "Praos selection")
}

func TestChainsyncConfigLimitOnPatienceDefaults(t *testing.T) {
	t.Parallel()
	n := &Node{config: NewConfig()}
	assert.Equal(t, chainsync.PatienceConfig{Enabled: true},
		n.chainsyncConfig().Patience,
		"enabled by default; zero capacity and rate select package defaults")
}

// TestLiveTruncateKeepsLimitOnPatience pins that the chainsync state rebuilt
// by a live truncate uses the configured Limit on Patience rather than the
// package defaults.
func TestLiveTruncateKeepsLimitOnPatience(t *testing.T) {
	t.Parallel()
	n, points := newLiveLifecycleTestNode(t, 25)
	require.NotNil(t, n.config.cfg)
	n.config.cfg.GenesisBootstrap.LimitOnPatienceEnabled = true
	n.config.cfg.GenesisBootstrap.LimitOnPatienceCapacity = 7

	targetSlot := points[10].Slot
	_, err := n.Truncate(context.Background(), dblifecycle.TruncateTarget{
		Slot: &targetSlot,
	})
	require.NoError(t, err)

	conn := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4001},
	}
	require.True(t, n.chainsyncState.AddClientConnId(conn))
	tc := n.chainsyncState.GetTrackedClient(conn)
	require.NotNil(t, tc)
	assert.InDelta(t, 7, tc.Patience.Tokens, 1e-9)
}
