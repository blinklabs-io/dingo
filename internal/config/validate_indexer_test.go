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

func TestIndexerDefaults(t *testing.T) {
	t.Parallel()
	cfg := &Config{}
	cfg.ApplyDefaults()
	require.Equal(
		t,
		DefaultIndexerMaxReadyLagBlocks,
		cfg.Indexer.MaxReadyLagBlocks,
	)
}

// API storage mode always uses a separate IndexerDB, including in-memory nodes.
func TestSplitIndexerFollowsStorageMode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		mode          string
		databasePath  string
		wantSplit     bool
		wantConsensus string
	}{
		{"api on disk", "api", "/data", true, "core"},
		{"core on disk", "core", "/data", false, "core"},
		{"api in memory", "api", "", true, "core"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := &Config{StorageMode: tt.mode, DatabasePath: tt.databasePath}
			require.Equal(t, tt.wantSplit, cfg.SplitIndexer())
			require.Equal(t, tt.wantConsensus, cfg.ConsensusStorageMode())
		})
	}
}
