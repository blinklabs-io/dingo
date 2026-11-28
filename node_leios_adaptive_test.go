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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/leios"
	sdkchain "github.com/blinklabs-io/dingo/sdk/chain"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func adaptiveNodeConfig(
	t *testing.T,
	section string,
) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg, err := cardano.NewCardanoNodeConfigFromReader(
		strings.NewReader(section),
	)
	require.NoError(t, err)
	return cfg
}

func adaptiveTestNode(
	t *testing.T,
	section string,
	producer bool,
	runMode string,
) *Node {
	t.Helper()
	n := &Node{
		config: NewConfig(
			WithCardanoNodeConfig(adaptiveNodeConfig(t, section)),
			WithBlockProducer(producer),
			WithRunMode(runMode),
		),
	}
	n.leiosVoteManager = &leios.VoteManager{}
	n.chainSelector = chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{},
	)
	return n
}

// The node follows missed quorums only where the scale is used: a block
// producer that forges endorser blocks, with the section enabled.
func TestInitLeiosAdaptiveEndorsementsGates(t *testing.T) {
	t.Parallel()
	const enabled = `{"LeiosAdaptiveEndorsements": {"Enabled": true}}`
	for _, tc := range []struct {
		name     string
		section  string
		producer bool
		runMode  string
		want     bool
	}{
		{"enabled producer", enabled, true, "leios", true},
		{"section absent", `{}`, true, "leios", false},
		{"explicitly disabled", `{"LeiosAdaptiveEndorsements": {"Enabled": false}}`, true, "leios", false},
		{"relay", enabled, false, "leios", false},
		{"not a Leios network", enabled, true, "serve", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			n := adaptiveTestNode(t, tc.section, tc.producer, tc.runMode)
			n.eventBus = event.NewEventBus(nil, nil)
			t.Cleanup(n.eventBus.Close)
			require.NoError(t, n.initLeiosAdaptiveEndorsements(t.Context()))
			t.Cleanup(func() {
				if n.leiosAdaptiveEb != nil {
					_ = n.leiosAdaptiveEb.Stop()
				}
			})
			require.Equal(t, tc.want, n.leiosAdaptiveEb != nil)

			fc := &forging.ForgerConfig{}
			n.applyLeiosAdaptiveTuning(fc)
			require.Equal(t, tc.want, fc.LeiosEBCapacityScale != nil)
			if tc.want {
				require.Equal(t, 1.0, fc.LeiosEBCapacityScale())
			}
		})
	}
}

// A start that finds no use for the controller leaves none behind: a
// controller of an earlier start was stopped, and its frozen scale must not
// reach the forger.
func TestInitLeiosAdaptiveEndorsementsDropsAStaleController(t *testing.T) {
	t.Parallel()
	n := adaptiveTestNode(t,
		`{"LeiosAdaptiveEndorsements": {"Enabled": true}}`, true, "leios")
	n.eventBus = event.NewEventBus(nil, nil)
	t.Cleanup(n.eventBus.Close)
	require.NoError(t, n.initLeiosAdaptiveEndorsements(t.Context()))
	require.NotNil(t, n.leiosAdaptiveEb)
	require.NoError(t, n.leiosAdaptiveEb.Stop())

	n.leiosVoteManager = nil
	require.NoError(t, n.initLeiosAdaptiveEndorsements(t.Context()))
	require.Nil(t, n.leiosAdaptiveEb)
	fc := &forging.ForgerConfig{}
	n.applyLeiosAdaptiveTuning(fc)
	require.Nil(t, fc.LeiosEBCapacityScale)
}

// A node that is not caught up is syncing, and a syncing node has no votes
// for the announcements it passes. A selector that has heard from no peer is
// not caught up in any selection mode.
func TestLeiosAdaptiveCaughtUp(t *testing.T) {
	t.Parallel()
	require.False(t, leiosAdaptiveCaughtUp(nil))
	require.False(t, leiosAdaptiveCaughtUp(chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{GenesisMode: true},
	)))
	require.False(t, leiosAdaptiveCaughtUp(chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{},
	)))
}

func adaptiveTestTip(n uint64) ochainsync.Tip {
	return ochainsync.Tip{
		Point: ocommon.Point{
			Slot: n,
			Hash: []byte(fmt.Sprintf("h%d", n)),
		},
		BlockNumber: n,
	}
}

// The embedded musashi configuration names PraosMode, which leaves the
// selector in Praos selection for its whole life. The caught-up judgement
// must still follow the sync state: a producer syncing from genesis, or
// catching up after downtime, is not caught up and does not sample
// announcements whose votes it never saw.
func TestLeiosAdaptiveCaughtUpFollowsSyncInPraosMode(t *testing.T) {
	t.Parallel()
	cardanoCfg, err := cardano.NewCardanoNodeConfigFromEmbedFS(
		cardano.EmbeddedConfigFS,
		cardano.EmbeddedConfigPath("musashi"),
	)
	require.NoError(t, err)
	_, enabled := (&Node{config: NewConfig(
		WithCardanoNodeConfig(cardanoCfg),
	)}).leiosAdaptiveEbConfig()
	require.True(t, enabled, "musashi ships with the section enabled")
	require.True(t, cardanoCfg.PraosConsensusMode())

	n := &Node{config: NewConfig(
		WithCardanoNodeConfig(cardanoCfg),
		WithBlockProducer(true),
		WithRunMode("leios"),
		WithGenesisBootstrap(true),
	)}
	require.False(t, n.config.genesisSelectionEligible())
	genesisMode := n.initialGenesisSelectionMode()
	require.False(t, genesisMode, "PraosMode starts in Praos selection")

	// The tip is two hours old, so catching up needs no young tip, and the
	// short maximum age lets the age run out within the test.
	start := time.Now().Add(-2 * time.Hour)
	var mu sync.Mutex
	onChain := map[uint64]struct{}{}
	cfg := n.buildChainSelectorConfig(10, genesisMode, 0)
	cfg.DisableEventSubscriptions = true
	cfg.MaxCaughtUpAge = 50 * time.Millisecond
	cfg.RandomBonus = func(time.Duration) time.Duration { return 0 }
	cfg.SlotTime = func(slot uint64) (time.Time, error) {
		return start.Add(time.Duration(slot) * time.Second), nil
	}
	cfg.ConnectionLive = func(ouroboros.ConnectionId) bool { return true }
	cfg.ChainPointOnLocalChain = func(p sdkchain.Point) bool {
		mu.Lock()
		defer mu.Unlock()
		_, ok := onChain[p.Slot]
		return ok
	}
	cs := chainselection.NewChainSelector(cfg)
	require.Equal(t, chainselection.SelectionModePraos, cs.SelectionMode())

	require.False(t, leiosAdaptiveCaughtUp(cs), "nothing synced yet")

	conn := newNodeTestConnId(9301)
	for i := uint64(1); i <= 5; i++ {
		tip := adaptiveTestTip(i)
		mu.Lock()
		onChain[i] = struct{}{}
		mu.Unlock()
		cs.SetLocalTip(tip)
		require.True(t, cs.UpdatePeerTip(conn, adaptiveTestTip(i+3), nil))
	}
	require.False(t, leiosAdaptiveCaughtUp(cs),
		"a peer that is ahead and still streaming has a better chain")

	for i := uint64(6); i <= 8; i++ {
		mu.Lock()
		onChain[i] = struct{}{}
		mu.Unlock()
		cs.SetLocalTip(adaptiveTestTip(i))
	}
	cs.SetPeerIdling(conn, adaptiveTestTip(8).Point)
	require.True(t, leiosAdaptiveCaughtUp(cs),
		"every peer is idle at the node's tip")
	require.Equal(t, chainselection.SelectionModePraos, cs.SelectionMode())

	require.Eventually(t, func() bool {
		return !leiosAdaptiveCaughtUp(cs)
	}, 10*time.Second, 10*time.Millisecond,
		"a tip past the maximum caught-up age is syncing again")
}

func TestLeiosAdaptiveEbConfigFollowsTheNodeConfig(t *testing.T) {
	t.Parallel()
	n := adaptiveTestNode(t, `{"LeiosAdaptiveEndorsements": {
		"Decrease": 0.5, "Increase": 0.05, "MinScale": 0.25}}`, true, "leios")
	cfg, ok := n.leiosAdaptiveEbConfig()
	require.True(t, ok)
	require.Equal(t,
		leios.AdaptiveEbConfig{Decrease: 0.5, Increase: 0.05, MinScale: 0.25},
		cfg,
	)
}
