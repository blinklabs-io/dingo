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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMithrilEnabledDefaultFalse(t *testing.T) {
	m := newDefaultConfig().Mithril
	assert.False(t, m.Enabled)
	assert.Equal(t, 64, m.WindowChunks)
	assert.Equal(t, 8, m.BatchChunks)
	assert.Equal(t, 16, m.DownloadWorkers)
	assert.Equal(t, uint64(4), m.MinGapChunks)
	assert.Equal(t, 10*time.Minute, m.CheckInterval)
	assert.Equal(t, 30*time.Minute, m.RetryBackoff)
}

func TestMithrilEnabledRequiresCore(t *testing.T) {
	cfg := validTestConfig()
	cfg.Mithril.Enabled = true
	cfg.StorageMode = storageModeAPI
	err := cfg.validate(RunModeServe, minUnprivilegedPort)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mithril.enabled requires core storage mode")

	cfg.StorageMode = storageModeCore
	assert.NoError(t, cfg.validate(RunModeServe, minUnprivilegedPort))

	// One-shot sync is not gated by the node-managed setting.
	cfg.StorageMode = storageModeAPI
	assert.NoError(t, cfg.validate(RunModeSync, minUnprivilegedPort))
}

func TestMithrilWindowValidation(t *testing.T) {
	tests := []struct {
		name          string
		window, batch int
		wantErr       bool
	}{
		{"defaults", 64, 8, false},
		{"equal", 8, 8, false},
		{"window below batch", 4, 8, true},
		{"batch zero", 64, 0, true},
		{"batch negative", 64, -1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validTestConfig()
			cfg.Mithril.Enabled = true
			cfg.Mithril.WindowChunks = tt.window
			cfg.Mithril.BatchChunks = tt.batch
			err := cfg.validate(RunModeServe, minUnprivilegedPort)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
