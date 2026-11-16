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

func TestValidateLedgerStateSetOnly(t *testing.T) {
	t.Parallel()
	valid := func(c *Config) {
		c.LedgerUtxoBadger = true
		c.LedgerUtxoBadgerOnly = true
		c.LedgerUtxoReads = "set"
		c.LedgerStateSetOnly = true
	}
	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr string
	}{
		{name: "all prerequisites", modify: valid},
		{
			name: "with verify before drain",
			modify: func(c *Config) {
				valid(c)
				c.LedgerStateVerifyBeforeDrain = true
			},
		},
		{
			name: "explicit core storage mode",
			modify: func(c *Config) {
				valid(c)
				c.StorageMode = "core"
			},
		},
		{
			name: "without ledgerUtxoBadgerOnly",
			modify: func(c *Config) {
				valid(c)
				c.LedgerUtxoBadgerOnly = false
			},
			wantErr: "ledgerStateSetOnly needs ledgerUtxoBadgerOnly",
		},
		{
			name: "API storage mode",
			modify: func(c *Config) {
				valid(c)
				c.StorageMode = "api"
			},
			wantErr: "ledgerStateSetOnly needs core storage mode",
		},
		{
			name: "verify reads",
			modify: func(c *Config) {
				valid(c)
				c.LedgerUtxoReads = "verify"
			},
			wantErr: "cannot run with ledgerStateSetOnly",
		},
		{
			name: "default reads",
			modify: func(c *Config) {
				valid(c)
				c.LedgerUtxoReads = ""
			},
			wantErr: "ledgerStateSetOnly needs ledgerUtxoReads \"set\"",
		},
		{
			name: "shadow set",
			modify: func(c *Config) {
				valid(c)
				c.LedgerUtxoShadow = true
			},
			wantErr: "ledgerUtxoShadow cannot run with ledgerStateSetOnly",
		},
		{
			name: "verify before drain without the switch",
			modify: func(c *Config) {
				c.LedgerStateVerifyBeforeDrain = true
			},
			wantErr: "ledgerStateVerifyBeforeDrain needs ledgerStateSetOnly",
		},
		{
			name: "switch off with everything else on",
			modify: func(c *Config) {
				valid(c)
				c.LedgerStateSetOnly = false
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

func TestLedgerStateSetOnlyReportsEveryRejectedCombination(t *testing.T) {
	t.Parallel()
	cfg := validTestConfig()
	cfg.LedgerStateSetOnly = true
	cfg.LedgerStateVerifyBeforeDrain = true
	cfg.StorageMode = "api"
	cfg.LedgerUtxoReads = "verify"
	cfg.LedgerUtxoShadow = true
	err := ValidateLedgerStateSetOnly(cfg)
	require.ErrorContains(t, err, "needs ledgerUtxoBadgerOnly")
	require.ErrorContains(t, err, "needs core storage mode")
	require.ErrorContains(t, err, "\"verify\" cannot run")
	require.ErrorContains(t, err, "ledgerUtxoShadow cannot run")
}
