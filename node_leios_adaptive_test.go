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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/leios"
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

// A node that has not left Genesis selection is still syncing, and a syncing
// node has no votes for the announcements it passes.
func TestLeiosAdaptiveCaughtUp(t *testing.T) {
	t.Parallel()
	require.False(t, leiosAdaptiveCaughtUp(nil))
	require.False(t, leiosAdaptiveCaughtUp(chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{GenesisMode: true},
	)))
	require.True(t, leiosAdaptiveCaughtUp(chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{},
	)))
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
