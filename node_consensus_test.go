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

	nodeconsensus "github.com/blinklabs-io/dingo/consensus"
	"github.com/blinklabs-io/dingo/database"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

type consensusIdentityMemoryStore struct {
	value string
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
		&consensusIdentityMemoryStore{},
		"tachys/v1",
	)
	require.ErrorContains(t, err, "unmarked database uses consensus")
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
