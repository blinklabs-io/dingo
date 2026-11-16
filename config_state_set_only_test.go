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

	"github.com/stretchr/testify/require"
)

func TestConfigValidateLedgerStateSetOnlyOptions(t *testing.T) {
	t.Parallel()
	prerequisites := []ConfigOptionFunc{
		WithLedgerUtxoBadger(true),
		WithLedgerUtxoBadgerOnly(true),
		WithLedgerUtxoReads("set"),
	}
	tests := []struct {
		name    string
		opts    []ConfigOptionFunc
		wantErr string
	}{
		{
			name: "switch without the set-only UTxO set",
			opts: []ConfigOptionFunc{
				WithLedgerUtxoBadger(true),
				WithLedgerUtxoReads("set"),
				WithLedgerStateSetOnly(true),
			},
			wantErr: "ledgerStateSetOnly needs ledgerUtxoBadgerOnly",
		},
		{
			name: "switch with API storage mode",
			opts: append([]ConfigOptionFunc{
				WithStorageMode(StorageModeAPI),
				WithLedgerStateSetOnly(true),
			}, prerequisites...),
			wantErr: "needs core storage mode",
		},
		{
			name: "switch with verify reads",
			opts: []ConfigOptionFunc{
				WithLedgerUtxoBadger(true),
				WithLedgerUtxoBadgerOnly(true),
				WithLedgerUtxoReads("verify"),
				WithLedgerStateSetOnly(true),
			},
			wantErr: "cannot run with ledgerStateSetOnly",
		},
		{
			name: "verify before drain alone",
			opts: []ConfigOptionFunc{
				WithLedgerStateVerifyBeforeDrain(true),
			},
			wantErr: "ledgerStateVerifyBeforeDrain needs ledgerStateSetOnly",
		},
		{
			name: "valid switch",
			opts: append([]ConfigOptionFunc{
				WithLedgerStateSetOnly(true),
				WithLedgerStateVerifyBeforeDrain(true),
			}, prerequisites...),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opts := append([]ConfigOptionFunc{
				WithNetworkMagic(42),
				WithListeners(ListenerConfig{
					ListenNetwork: "tcp",
					ListenAddress: "127.0.0.1:0",
				}),
			}, tt.opts...)
			cfg := NewConfig(opts...)
			node := &Node{config: cfg}
			err := node.configValidate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				require.True(t, cfg.LedgerStateSetOnly())
				require.True(t, cfg.LedgerStateVerifyBeforeDrain())
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}
