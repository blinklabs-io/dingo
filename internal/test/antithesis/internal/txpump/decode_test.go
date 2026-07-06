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

package txpump

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// cborTag258 is the 3-byte CBOR prefix for tag 258 (CBOR set).
var cborTag258 = []byte{0xd9, 0x01, 0x02}

func requireConwayDecode(t *testing.T, txBytes []byte) {
	t.Helper()
	var tx conway.ConwayTransaction
	_, err := cbor.Decode(txBytes, &tx)
	require.NoError(t, err)
	// Inputs must be encoded as a CBOR set (tag 258). Without this, the
	// Cardano node sees an empty input set and rejects with InputSetEmptyUTxO.
	require.True(
		t,
		bytes.Contains(txBytes, cborTag258),
		"transaction inputs must be encoded as CBOR tag-258 set (0xd90102 not found)",
	)
}

// TestInputsEncodedAsSet is a regression test: inputs must use CBOR tag 258.
// A plain array causes Cardano node to report InputSetEmptyUTxO on every tx.
func TestInputsEncodedAsSet(t *testing.T) {
	tests := []struct {
		name  string
		build func() ([]byte, error)
	}{
		{
			name: "payment",
			build: func() ([]byte, error) {
				b, _, err := BuildPayment(validParams())
				return b, err
			},
		},
		{
			name: "delegation",
			build: func() ([]byte, error) {
				return BuildDelegationTx(
					[]UTxO{{TxHash: sampleHash, Index: 0, Amount: 1_000_000}},
					make([]byte, 28), make([]byte, 28), MinFee, sampleAddr,
				)
			},
		},
		{
			name: "drep_registration",
			build: func() ([]byte, error) {
				return BuildDRepRegistrationTx(
					[]UTxO{{TxHash: sampleHash, Index: 0, Amount: 600_000_000}},
					make([]byte, 28), 500_000_000, MinFee, sampleAddr,
				)
			},
		},
		{
			name: "vote",
			build: func() ([]byte, error) {
				return BuildVoteTx(
					[]UTxO{{TxHash: sampleHash, Index: 0, Amount: 1_000_000}},
					make([]byte, 28), make([]byte, 32), 0, MinFee, sampleAddr,
				)
			},
		},
		{
			name: "plutus_lock",
			build: func() ([]byte, error) {
				return BuildPlutusLockTx(
					[]UTxO{{TxHash: sampleHash, Index: 0, Amount: 3_000_000}},
					make([]byte, 28), minSendAmount, MinFee, sampleAddr,
				)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			txBytes, err := tc.build()
			require.NoError(t, err)
			require.True(
				t,
				bytes.Contains(txBytes, cborTag258),
				"tx type %q: inputs must be CBOR tag-258 set", tc.name,
			)
		})
	}
}
