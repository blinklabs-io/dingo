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

package ledger

import (
	"math"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestLedgerProcessBlockByronInputIndexWord16 covers the Byron TxIn Word16
// boundary through decode and block application with a UTxO that resolves at
// each index: 65535 is applied, 65536 never decodes, and a transaction
// assembled past the decoder is refused by block application.
func TestLedgerProcessBlockByronInputIndexWord16(t *testing.T) {
	t.Parallel()

	nodeConfig := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		loadByronGenesisForTest(t, nodeConfig, strings.NewReader(`{
		"blockVersionData": {
			"slotDuration": "20000",
			"maxTxSize": "16384",
			"txFeePolicy": {"summand": "0", "multiplier": "0"}
		},
		"protocolConsts": {"k": 2160, "protocolMagic": 764824073}
	}`)),
	)
	const protocolMagic = 764824073
	key := newByronBlockTestKey(t, 0x64)
	payTo := newByronBlockTestKey(t, 0x65).address

	t.Run("index 65535 is applied", func(t *testing.T) {
		t.Parallel()
		db := newTestDB(t)
		txId := seedByronUtxoAt(
			t, db, 0x01, math.MaxUint16, key.address, 1_000,
		)
		tx := buildByronBlockTestTx(t, protocolMagic,
			[]byronBlockTestInput{{txId, math.MaxUint16}},
			[]byronBlockTestOutput{{payTo, 900}},
			nil, []byronBlockTestKey{key})
		require.NoError(
			t,
			processByronReferenceRuleBlock(t, db, nodeConfig, tx),
		)
	})

	t.Run("index 65536 does not decode", func(t *testing.T) {
		t.Parallel()
		db := newTestDB(t)
		txId := seedByronUtxoAt(
			t, db, 0x01, math.MaxUint16+1, key.address, 1_000,
		)
		txCbor, _ := encodeByronBlockTestTx(t, protocolMagic,
			[]byronBlockTestInput{{txId, math.MaxUint16 + 1}},
			[]byronBlockTestOutput{{payTo, 900}},
			nil, []byronBlockTestKey{key})
		_, err := byron.NewByronTransactionFromCbor(txCbor)
		require.ErrorContains(t, err, "out of range")
	})

	t.Run("index 65536 past the decoder is rejected", func(t *testing.T) {
		t.Parallel()
		db := newTestDB(t)
		txId := seedByronUtxoAt(
			t, db, 0x01, math.MaxUint16+1, key.address, 1_000,
		)
		var id lcommon.Blake2b256
		copy(id[:], txId)
		tx := &byron.ByronTransaction{
			Body: byron.ByronTransactionBody{
				TxInputs: []byron.ByronTransactionInput{
					{TxId: id, OutputIndex: math.MaxUint16 + 1},
				},
				TxOutputs: []byron.ByronTransactionOutput{
					{OutputAddress: payTo, OutputAmount: 900},
				},
			},
		}
		err := processByronReferenceRuleBlock(t, db, nodeConfig, tx)
		var tooLarge eras.InputIndexByronError
		require.ErrorAs(t, err, &tooLarge)
		require.EqualValues(t, math.MaxUint16+1, tooLarge.Index)
	})
}
