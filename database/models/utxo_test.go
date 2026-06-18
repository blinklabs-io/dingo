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

package models

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUtxoLedgerToModelPaymentScript verifies that PaymentScript is set
// only when the output's payment credential is a script hash.
func TestUtxoLedgerToModelPaymentScript(t *testing.T) {
	payHash := bytes.Repeat([]byte{0xAB}, lcommon.AddressHashSize)

	tests := []struct {
		name       string
		addrType   uint8
		wantScript bool
	}{
		{
			name:       "enterprise key payment credential",
			addrType:   lcommon.AddressTypeKeyNone,
			wantScript: false,
		},
		{
			name:       "enterprise script payment credential",
			addrType:   lcommon.AddressTypeScriptNone,
			wantScript: true,
		},
		{
			name:       "key payment, script staking",
			addrType:   lcommon.AddressTypeKeyScript,
			wantScript: false,
		},
		{
			name:       "script payment, key staking",
			addrType:   lcommon.AddressTypeScriptKey,
			wantScript: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stakingHash []byte
			switch tc.addrType {
			case lcommon.AddressTypeKeyScript,
				lcommon.AddressTypeScriptKey:
				stakingHash = bytes.Repeat(
					[]byte{0xCD},
					lcommon.AddressHashSize,
				)
			}
			addr, err := lcommon.NewAddressFromParts(
				tc.addrType,
				lcommon.AddressNetworkMainnet,
				payHash,
				stakingHash,
			)
			require.NoError(t, err)

			utxo := ledger.Utxo{
				Id: shelley.NewShelleyTransactionInput(
					"0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
					0,
				),
				Output: &shelley.ShelleyTransactionOutput{
					OutputAddress: addr,
					OutputAmount:  1_000_000,
				},
			}

			model := UtxoLedgerToModel(utxo, 42)
			assert.Equal(t, tc.wantScript, model.PaymentScript)
			// Payment hash is recorded regardless of credential type.
			assert.Equal(t, payHash, model.PaymentKey)
		})
	}
}
