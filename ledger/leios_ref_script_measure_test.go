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
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// The endorser-block reference-script measure resolves through the session's
// overlay, so a script an earlier selected transaction created counts for a
// later transaction that references it, as in the reference mempool where each
// transaction is measured against the state its predecessors left.
func TestLeiosSelectionSessionMeasuresOverlayReferenceScripts(t *testing.T) {
	t.Parallel()
	const scriptSize = 123
	db := newStorageFaultTestDB(t, errInjectingMetadataStore{})
	ls := newFakeEraLedgerState(db, func(
		lcommon.Transaction,
		uint64,
		lcommon.LedgerState,
		lcommon.ProtocolParameters,
	) error {
		return nil
	}, nil)

	refInput := shelley.MustNewShelleyTransactionInput(
		"b228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee22", 0,
	)
	spendInput := shelley.MustNewShelleyTransactionInput(
		"a228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee11", 0,
	)
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone, 0, make([]byte, 28), nil,
	)
	require.NoError(t, err)
	scriptOutput := &babbage.BabbageTransactionOutput{
		OutputAddress: address,
		TxOutScriptRef: &lcommon.ScriptRef{
			Type:   lcommon.ScriptRefTypePlutusV3,
			Script: make(lcommon.PlutusV3Script, scriptSize),
		},
	}
	// The same script is referenced twice: once as a reference input and
	// once as a spent input. The measure is non-distinct, so each input
	// counts it.
	tx := &conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{spendInput},
			),
			TxReferenceInputs: cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{refInput}, false,
			),
		},
		TxIsValid: true,
	}
	created := map[utxoref.Key]lcommon.Utxo{
		utxoref.ForInput(refInput): {Id: refInput, Output: scriptOutput},
		utxoref.ForInput(spendInput): {
			Id: spendInput, Output: scriptOutput,
		},
	}

	var withScripts, withoutOverlay uint64
	var withoutOverlayErr error
	var provider forging.LeiosSelectionSessionProvider = ls
	require.NoError(t, provider.WithLeiosSelectionSession(
		t.Context(),
		nil,
		func(
			_ forging.TxValidationFunc,
			refScriptSize forging.TxRefScriptSizeFunc,
			_ func() bool,
		) error {
			var err error
			withScripts, err = refScriptSize(
				tx, map[utxoref.Key]struct{}{}, created,
			)
			require.NoError(t, err)
			withoutOverlay, withoutOverlayErr = refScriptSize(
				tx, map[utxoref.Key]struct{}{}, nil,
			)
			return nil
		},
	))
	require.Equal(t, uint64(2*scriptSize), withScripts)
	// An unresolvable reference input is an error, never a size of zero.
	require.Error(t, withoutOverlayErr)
	require.Zero(t, withoutOverlay)
}

// leiosSubTxTestTx is a transaction carrying sub-transaction bodies.
type leiosSubTxTestTx struct {
	*conway.ConwayTransaction
	subBodies []lcommon.TransactionBody
}

func (t leiosSubTxTestTx) SubTransactionBodies() []lcommon.TransactionBody {
	return t.subBodies
}

// The endorser-block measure is the reference mempool's: the non-distinct
// reference-script size of the top-level transaction's own inputs and
// reference inputs (Conway txNonDistinctRefScriptsSize). Sub-transaction
// inputs are not part of it, unlike the per-transaction ledger limit.
func TestLeiosSelectionSessionMeasuresTopLevelReferenceScriptsOnly(
	t *testing.T,
) {
	t.Parallel()
	const topScriptSize, subScriptSize = 123, 77
	db := newStorageFaultTestDB(t, errInjectingMetadataStore{})
	ls := newFakeEraLedgerState(db, func(
		lcommon.Transaction,
		uint64,
		lcommon.LedgerState,
		lcommon.ProtocolParameters,
	) error {
		return nil
	}, nil)

	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone, 0, make([]byte, 28), nil,
	)
	require.NoError(t, err)
	scriptOutput := func(size int) *babbage.BabbageTransactionOutput {
		return &babbage.BabbageTransactionOutput{
			OutputAddress: address,
			TxOutScriptRef: &lcommon.ScriptRef{
				Type:   lcommon.ScriptRefTypePlutusV3,
				Script: make(lcommon.PlutusV3Script, size),
			},
		}
	}
	topInput := shelley.MustNewShelleyTransactionInput(
		"c228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee33", 0,
	)
	subInput := shelley.MustNewShelleyTransactionInput(
		"d228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee44", 0,
	)
	tx := leiosSubTxTestTx{
		ConwayTransaction: &conway.ConwayTransaction{
			Body: conway.ConwayTransactionBody{
				TxReferenceInputs: cbor.NewSetType(
					[]shelley.ShelleyTransactionInput{topInput}, false,
				),
			},
			TxIsValid: true,
		},
		subBodies: []lcommon.TransactionBody{
			&conway.ConwayTransactionBody{
				TxReferenceInputs: cbor.NewSetType(
					[]shelley.ShelleyTransactionInput{subInput}, false,
				),
			},
		},
	}
	require.Len(t, lcommon.SubTransactionBodiesFromTransaction(tx), 1)
	created := map[utxoref.Key]lcommon.Utxo{
		utxoref.ForInput(topInput): {
			Id: topInput, Output: scriptOutput(topScriptSize),
		},
		utxoref.ForInput(subInput): {
			Id: subInput, Output: scriptOutput(subScriptSize),
		},
	}

	var size uint64
	require.NoError(t, ls.WithLeiosSelectionSession(
		t.Context(),
		nil,
		func(
			_ forging.TxValidationFunc,
			refScriptSize forging.TxRefScriptSizeFunc,
			_ func() bool,
		) error {
			var err error
			size, err = refScriptSize(tx, map[utxoref.Key]struct{}{}, created)
			return err
		},
	))
	require.Equal(t, uint64(topScriptSize), size)
}

// A reference script on a UTxO already in the database is measured from its
// stored encoding, and an input an earlier selected transaction consumed no
// longer resolves.
func TestLeiosSelectionSessionMeasuresStoredReferenceScripts(t *testing.T) {
	t.Parallel()
	const scriptSize = 321
	db := newStorageFaultTestDB(t, errInjectingMetadataStore{})
	ls := newFakeEraLedgerState(db, func(
		lcommon.Transaction,
		uint64,
		lcommon.LedgerState,
		lcommon.ProtocolParameters,
	) error {
		return nil
	}, nil)

	id := bytes.Repeat([]byte{0xe1}, 32)
	encoded, err := cbor.Encode(additionalReferenceOutput(t, scriptSize))
	require.NoError(t, err)
	require.NoError(t, db.Transaction(t.Context(), true).Do(
		func(txn *database.Txn) error {
			if err := db.CreateUtxo(
				t.Context(), txn, &models.Utxo{TxId: id},
			); err != nil {
				return err
			}
			return db.Blob().SetUtxo(txn.Blob(), id, 0, encoded)
		},
	))
	refInput := shelley.MustNewShelleyTransactionInput(
		hex.EncodeToString(id), 0,
	)
	tx := &conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxReferenceInputs: cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{refInput}, false,
			),
		},
		TxIsValid: true,
	}

	attachQueryLiveUtxoSet(t, ls, db)
	var stored, consumed uint64
	var consumedErr error
	require.NoError(t, ls.WithLeiosSelectionSession(
		t.Context(),
		nil,
		func(
			_ forging.TxValidationFunc,
			refScriptSize forging.TxRefScriptSizeFunc,
			_ func() bool,
		) error {
			var err error
			stored, err = refScriptSize(tx, map[utxoref.Key]struct{}{}, nil)
			require.NoError(t, err)
			consumed, consumedErr = refScriptSize(
				tx,
				map[utxoref.Key]struct{}{utxoref.ForInput(refInput): {}},
				nil,
			)
			return nil
		},
	))
	require.Equal(t, uint64(scriptSize), stored)
	require.Error(t, consumedErr)
	require.Zero(t, consumed)
}
