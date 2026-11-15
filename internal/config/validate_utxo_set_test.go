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

func TestValidateLiveUtxoSetSettings(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr string
	}{
		{
			name: "badger set without databasePath",
			modify: func(c *Config) {
				c.Plugins.Storage.Metadata.Provider = "postgres"
				c.DatabasePath = ""
				c.LedgerUtxoBadger = true
			},
			wantErr: "ledgerUtxoBadger needs databasePath",
		},
		{
			name: "set-only without databasePath",
			modify: func(c *Config) {
				c.Plugins.Storage.Metadata.Provider = "postgres"
				c.DatabasePath = ""
				c.LedgerUtxoBadger = true
				c.LedgerUtxoBadgerOnly = true
			},
			wantErr: "ledgerUtxoBadger needs databasePath",
		},
		{
			name: "verify reads with set-only",
			modify: func(c *Config) {
				c.LedgerUtxoBadger = true
				c.LedgerUtxoBadgerOnly = true
				c.LedgerUtxoReads = "verify"
			},
			wantErr: "has no effect with ledgerUtxoBadgerOnly",
		},
		{
			name: "set reads with set-only",
			modify: func(c *Config) {
				c.LedgerUtxoBadger = true
				c.LedgerUtxoBadgerOnly = true
				c.LedgerUtxoReads = "set"
			},
		},
		{
			name: "badger set with databasePath",
			modify: func(c *Config) {
				c.LedgerUtxoBadger = true
			},
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
