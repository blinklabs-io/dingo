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

func TestConfigValidateLiveUtxoSetOptions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		opts    []ConfigOptionFunc
		wantErr string
	}{
		{
			name:    "set-only without the Badger set",
			opts:    []ConfigOptionFunc{WithLedgerUtxoBadgerOnly(true)},
			wantErr: "ledgerUtxoBadgerOnly needs ledgerUtxoBadger",
		},
		{
			name:    "reads without the Badger set",
			opts:    []ConfigOptionFunc{WithLedgerUtxoReads("set")},
			wantErr: "needs ledgerUtxoBadger",
		},
		{
			name: "unknown reads mode",
			opts: []ConfigOptionFunc{
				WithLedgerUtxoBadger(true),
				WithLedgerUtxoReads("badger"),
			},
			wantErr: "invalid ledgerUtxoReads",
		},
		{
			name: "valid set-only",
			opts: []ConfigOptionFunc{
				WithLedgerUtxoBadger(true),
				WithLedgerUtxoBadgerOnly(true),
				WithLedgerUtxoReads("set"),
			},
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
			node := &Node{config: NewConfig(opts...)}
			err := node.configValidate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}
