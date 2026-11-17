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

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateIndexerLayout(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr string
	}{
		{name: "default is shared", modify: func(*Config) {}},
		{
			name:   "split in api mode",
			modify: func(c *Config) { c.StorageMode = "api"; c.Indexer.Layout = "split" },
		},
		{
			name:    "split in core mode",
			modify:  func(c *Config) { c.StorageMode = "core"; c.Indexer.Layout = "split" },
			wantErr: "indexer.layout split needs storageMode api",
		},
		{
			name: "split with history expiry",
			modify: func(c *Config) {
				c.StorageMode = "api"
				c.Indexer.Layout = "split"
				c.HistoryExpiry.Enabled = true
			},
			wantErr: "incompatible with historyExpiry.enabled",
		},
		{
			name:    "unknown layout",
			modify:  func(c *Config) { c.Indexer.Layout = "sharded" },
			wantErr: `invalid indexer.layout "sharded"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := validTestConfig()
			tt.modify(cfg)
			err := cfg.validate(cfg.RunMode, minUnprivilegedPort)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestIndexerDefaults(t *testing.T) {
	t.Parallel()
	cfg := &Config{}
	cfg.ApplyDefaults()
	require.Equal(t, IndexerLayoutShared, cfg.Indexer.Layout)
	require.Equal(
		t,
		DefaultIndexerMaxReadyLagBlocks,
		cfg.Indexer.MaxReadyLagBlocks,
	)
}

func TestConsensusStorageModeIsCoreOnTheSplitLayout(t *testing.T) {
	t.Parallel()
	cfg := &Config{StorageMode: "api"}
	require.Equal(t, "api", cfg.ConsensusStorageMode())
	require.False(t, cfg.SplitIndexer())
	cfg.Indexer.Layout = IndexerLayoutSplit
	require.True(t, cfg.SplitIndexer())
	require.Equal(t, "core", cfg.ConsensusStorageMode())
}

// The split layout runs the IndexerDB in API mode and the consensus store in
// core mode, so the core-mode requirements of the live UTxO set apply to the
// consensus store, not to the configured storageMode.
func TestValidateIndexerSplitWithLiveUtxoSet(t *testing.T) {
	t.Parallel()
	split := func(c *Config) {
		c.StorageMode = "api"
		c.Indexer.Layout = IndexerLayoutSplit
		c.LedgerUtxoBadger = true
		c.LedgerUtxoBadgerOnly = true
		c.LedgerUtxoReads = "set"
	}
	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr string
	}{
		{name: "split with badger only", modify: split},
		{
			name: "shared api with badger only",
			modify: func(c *Config) {
				split(c)
				c.Indexer.Layout = IndexerLayoutShared
			},
			wantErr: "ledgerUtxoBadgerOnly needs core storage mode",
		},
		{
			name: "split with set only",
			modify: func(c *Config) {
				split(c)
				c.LedgerStateSetOnly = true
			},
		},
		{
			name: "shared api with set only",
			modify: func(c *Config) {
				split(c)
				c.Indexer.Layout = IndexerLayoutShared
				c.LedgerStateSetOnly = true
			},
			wantErr: "ledgerStateSetOnly needs core storage mode",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := validTestConfig()
			tt.modify(cfg)
			err := cfg.validate(cfg.RunMode, minUnprivilegedPort)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}
