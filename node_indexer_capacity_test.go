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
	"bytes"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/plugin"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/require"
)

type capacityState map[string]string

func (s capacityState) GetSyncState(key string, _ *database.Txn) (string, error) {
	return s[key], nil
}

func (s capacityState) SetSyncState(key, value string, _ *database.Txn) error {
	s[key] = value
	return nil
}

func (s capacityState) DeleteSyncState(key string, _ *database.Txn) error {
	delete(s, key)
	return nil
}

func (s capacityState) ListSyncStateKeysByPrefix(
	string,
	*database.Txn,
) ([]string, error) {
	return nil, nil
}

func TestMainnetIndexerCapacityWarning(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	n := &Node{config: Config{
		cfg: &internalconfig.Config{
			NetworkMagic: ouroboros.NetworkCardanoMainnet.NetworkMagic,
		},
		dataDir: "unused",
		logger:  slog.New(slog.NewTextHandler(&logs, nil)),
		pluginSelections: map[plugin.Capability]plugin.Selection{
			plugin.CapabilityStorageMetadata: {Provider: "sqlite"},
		},
	}}
	store, err := indexerdb.NewStore(nil, capacityState{}, 0)
	require.NoError(t, err)
	n.warnMainnetIndexerCapacity(
		store,
		func(string) (uint64, uint64, error) {
			return 700 << 30, 10 << 30, nil
		},
	)
	require.Contains(t, logs.String(), "mainnet IndexerDB replay may exhaust disk")
	require.Contains(t, logs.String(), "minimum_additional_bytes=55834574848")
}

func TestMainnetIndexerCapacityWarningSkipsCompletedReplay(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	n := &Node{config: Config{
		cfg: &internalconfig.Config{
			NetworkMagic: ouroboros.NetworkCardanoMainnet.NetworkMagic,
		},
		logger: slog.New(slog.NewTextHandler(&logs, nil)),
		pluginSelections: map[plugin.Capability]plugin.Selection{
			plugin.CapabilityStorageMetadata: {Provider: "sqlite"},
		},
	}}
	store, err := indexerdb.NewStore(nil, capacityState{
		indexerdb.TipSyncKey: "10:5:aa",
	}, 0)
	require.NoError(t, err)
	n.warnMainnetIndexerCapacity(
		store,
		func(string) (uint64, uint64, error) {
			return 0, 0, nil
		},
	)
	require.Empty(t, logs.String())
}
