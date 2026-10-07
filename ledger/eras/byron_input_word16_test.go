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

package eras

import (
	"math"
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestByronTransactionInputIndexIsWord16 pins the Byron boundary the rest of
// dingo relies on: the reference TxIn index is a Word16, so 65535 decodes and
// constructs while 65536 and above are refused at decode and construction.
func TestByronTransactionInputIndexIsWord16(t *testing.T) {
	t.Parallel()
	txHash := strings.Repeat("ab", lcommon.Blake2b256Size)
	var txId lcommon.Blake2b256
	copy(txId[:], strings.Repeat("\xab", lcommon.Blake2b256Size))
	for _, tc := range []struct {
		index   uint32
		wantErr bool
	}{
		{index: 0},
		{index: math.MaxUint16},
		{index: math.MaxUint16 + 1, wantErr: true},
		{index: math.MaxUint32, wantErr: true},
	} {
		inner, err := cbor.Encode(&byronInputInner{TxId: txId, Index: tc.index})
		require.NoError(t, err)
		wire, err := cbor.Encode(&byronInputWire{
			Id:   0,
			Cbor: cbor.WrappedCbor(inner),
		})
		require.NoError(t, err)
		var decoded byron.ByronTransactionInput
		_, err = cbor.Decode(wire, &decoded)
		if tc.wantErr {
			require.Error(t, err, "decode index %d", tc.index)
		} else {
			require.NoError(t, err, "decode index %d", tc.index)
			require.Equal(t, tc.index, decoded.Index())
		}

		constructed, err := byron.NewByronTransactionInput(
			txHash,
			int(tc.index), // #nosec G115 -- bounded by the table above
		)
		if tc.wantErr {
			require.Error(t, err, "construct index %d", tc.index)
		} else {
			require.NoError(t, err, "construct index %d", tc.index)
			require.Equal(t, tc.index, constructed.Index())
		}
	}
}

// TestValidateTxByronRejectsInputIndexAboveWord16 builds a transaction that
// bypasses the decoder with an input at index 65536 whose UTxO resolves, so
// the UTxO lookup cannot be what rejects it.
func TestValidateTxByronRejectsInputIndexAboveWord16(t *testing.T) {
	t.Parallel()
	addr := newByronAddressForNetwork(t, nil, 0x01)
	for _, tc := range []struct {
		index   uint32
		wantErr bool
	}{
		{index: math.MaxUint16},
		{index: math.MaxUint16 + 1, wantErr: true},
		{index: math.MaxUint32, wantErr: true},
	} {
		var txId lcommon.Blake2b256
		txId[0] = 0x42
		input := byron.ByronTransactionInput{TxId: txId, OutputIndex: tc.index}
		ls := newByronRulesLedgerState()
		ls.addUtxo(input, newTestOutputWithAddress(1_000, addr))
		tx := &byron.ByronTransaction{
			Body: byron.ByronTransactionBody{
				TxInputs: []byron.ByronTransactionInput{input},
				TxOutputs: []byron.ByronTransactionOutput{
					{OutputAddress: addr, OutputAmount: 900},
				},
			},
		}
		err := ValidateTxByron(tx, 0, ls, nil)
		var tooLarge InputIndexByronError
		if tc.wantErr {
			require.ErrorAs(t, err, &tooLarge, "index %d", tc.index)
			require.Equal(t, tc.index, tooLarge.Index)
			continue
		}
		require.NotErrorAs(t, err, &tooLarge, "index %d", tc.index)
	}
}
