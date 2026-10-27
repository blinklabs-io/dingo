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
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	nodeconsensus "github.com/blinklabs-io/dingo/consensus"
	"github.com/blinklabs-io/dingo/database"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/plugin"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func tachysTestConfig(t *testing.T) Config {
	t.Helper()
	nodeCfg, err := cardano.NewCardanoNodeConfigFromFile(
		"config/cardano/devnet/config.json",
	)
	require.NoError(t, err)
	nodeCfg.ShelleyGenesis().ActiveSlotsCoeff.Rat = big.NewRat(1, 1)
	cfg := NewConfig(WithCardanoNodeConfig(nodeCfg))
	cfg.genesisBootstrap = false
	cfg.cfg.Mithril.Enabled = false
	return cfg
}

func TestTachysConfigurationRequiresSupportedNetworkShape(t *testing.T) {
	t.Parallel()

	provider := nodeconsensus.NewTachysProvider()
	t.Run("supported", func(t *testing.T) {
		cfg := tachysTestConfig(t)
		require.NoError(t, validateConsensusConfiguration(&cfg, provider))
	})
	t.Run("sparse slots", func(t *testing.T) {
		cfg := tachysTestConfig(t)
		cfg.CardanoNodeConfig().ShelleyGenesis().ActiveSlotsCoeff.Rat =
			big.NewRat(1, 2)
		require.ErrorContains(
			t,
			validateConsensusConfiguration(&cfg, provider),
			"activeSlotsCoeff 1/1",
		)
	})
	t.Run("pre-Babbage history", func(t *testing.T) {
		cfg := tachysTestConfig(t)
		epoch := uint64(1)
		cfg.CardanoNodeConfig().TestBabbageHardForkAtEpoch = &epoch
		require.ErrorContains(
			t,
			validateConsensusConfiguration(&cfg, provider),
			"babbage at genesis",
		)
	})
	t.Run("Dijkstra transition", func(t *testing.T) {
		cfg := tachysTestConfig(t)
		epoch := uint64(2)
		enabled := true
		cfg.CardanoNodeConfig().TestDijkstraHardForkAtEpoch = &epoch
		cfg.CardanoNodeConfig().ExperimentalHardForksEnabled = &enabled
		require.ErrorContains(
			t,
			validateConsensusConfiguration(&cfg, provider),
			"Dijkstra transition",
		)
	})
}

type consensusIdentityMemoryStore struct {
	value string
	tip   ochainsync.Tip
}

func (s *consensusIdentityMemoryStore) GetTip(
	*database.Txn,
) (ochainsync.Tip, error) {
	return s.tip, nil
}

func (s *consensusIdentityMemoryStore) GetSyncState(
	string,
	*database.Txn,
) (string, error) {
	return s.value, nil
}

func (s *consensusIdentityMemoryStore) SetSyncState(
	_ string,
	value string,
	_ *database.Txn,
) error {
	s.value = value
	return nil
}

func TestConsensusDatabaseIdentityCompatibility(t *testing.T) {
	t.Parallel()

	store := &consensusIdentityMemoryStore{}
	require.NoError(t, enforceConsensusIdentity(
		store,
		nodeconsensus.PraosIdentity,
	))
	require.Equal(t, nodeconsensus.PraosIdentity, store.value)
	require.NoError(t, enforceConsensusIdentity(
		store,
		nodeconsensus.PraosIdentity,
	))
	require.ErrorContains(t, enforceConsensusIdentity(
		store,
		"tachys/v1",
	), "does not match")
}

func TestUnmarkedDatabaseIsPraos(t *testing.T) {
	t.Parallel()

	err := enforceConsensusIdentity(
		&consensusIdentityMemoryStore{tip: ochainsync.Tip{
			Point:       ocommon.Point{Slot: 1, Hash: []byte{1}},
			BlockNumber: 1,
		}},
		"tachys/v1",
	)
	require.ErrorContains(t, err, "unmarked database uses consensus")
}

func TestFreshDatabaseAdoptsSelectedConsensus(t *testing.T) {
	t.Parallel()

	store := &consensusIdentityMemoryStore{}
	require.NoError(t, enforceConsensusIdentity(store, "tachys/v1"))
	require.Equal(t, "tachys/v1", store.value)
}

func TestConsensusNetworkMagicSeparatesProviders(t *testing.T) {
	t.Parallel()

	const base = uint32(42)
	require.Equal(
		t,
		base,
		consensusNetworkMagic(base, nodeconsensus.NewPraosProvider()),
	)
	tachysMagic := consensusNetworkMagic(
		base,
		nodeconsensus.NewTachysProvider(),
	)
	require.NotZero(t, tachysMagic)
	require.NotEqual(t, base, tachysMagic)
	require.Equal(
		t,
		tachysMagic,
		consensusNetworkMagic(base, nodeconsensus.NewTachysProvider()),
	)
	n := &Node{
		config:            NewConfig(WithNetworkMagic(base)),
		consensusProvider: nodeconsensus.NewTachysProvider(),
	}
	wireConfig := n.newOuroborosConfig(false, 0, 0)
	require.Equal(t, base, wireConfig.NetworkMagic)
	require.Equal(t, tachysMagic, wireConfig.NodeToNodeNetworkMagic)
}

func TestConsensusPluginSelectionDefaultsEmptyProviderToPraos(t *testing.T) {
	t.Parallel()

	cfg, err := NewConfigFromInternal(
		&internalconfig.Config{},
		nil, nil, nil, nil,
	)
	require.NoError(t, err)
	require.Empty(t, cfg.pluginSelections[plugin.CapabilityConsensus].Provider)
	require.Equal(
		t,
		nodeconsensus.PraosProviderName,
		cfg.consensusPluginSelection().Provider,
	)

	cfg, err = NewConfigFromInternal(
		&internalconfig.Config{
			Plugins: internalconfig.PluginsConfig{
				Consensus: plugin.Selection{Provider: "custom"},
			},
		},
		nil, nil, nil, nil,
	)
	require.NoError(t, err)
	require.Equal(t, "custom", cfg.consensusPluginSelection().Provider)
}
