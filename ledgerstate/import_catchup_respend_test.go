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

package ledgerstate

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// minimalCertStateData builds the smallest valid CertStateData CBOR ([VState,
// PState, DState], each holding only empty maps) -- the same shape
// mithril/bootstrap_v2_test.go's minimalLedgerState builds inline for its own
// certState field. Reconcile-mode import requires non-empty CertStateData
// (validateReconcileImportConfig), even when there is nothing to import.
func minimalCertStateData(t *testing.T) cbor.RawMessage {
	t.Helper()
	emptyMap, err := cbor.Encode(map[uint64]uint64{})
	require.NoError(t, err)
	vState, err := cbor.Encode([]any{cbor.RawMessage(emptyMap)})
	require.NoError(t, err)
	pState, err := cbor.Encode([]any{cbor.RawMessage(emptyMap)})
	require.NoError(t, err)
	dStateAccounts, err := cbor.Encode([]any{
		cbor.RawMessage(emptyMap), cbor.RawMessage(emptyMap),
	})
	require.NoError(t, err)
	dState, err := cbor.Encode([]any{cbor.RawMessage(dStateAccounts)})
	require.NoError(t, err)
	certState, err := cbor.Encode([]any{
		cbor.RawMessage(vState),
		cbor.RawMessage(pState),
		cbor.RawMessage(dState),
	})
	require.NoError(t, err)
	return certState
}

// TestImportLedgerStateCatchUpRestoresPostAnchorSpentUtxo is the
// mithril-import-level regression for dingo#4770, exercised through the same
// orchestrator (ledgerstate.ImportLedgerState) mithril's catch-up/reward-repair
// path calls (mithril/sync_import.go). It reproduces the full defect shape:
//
//  1. A first import (the original bootstrap) declares an output live at the
//     anchor slot.
//  2. A real block, applied through the ordinary database.SetTransaction
//     path, spends that output at a later slot -- exactly what a node
//     synced past its bootstrap anchor does.
//  3. A second import (a catch-up or legacy reward-repair re-import of a
//     certified snapshot covering the same anchor) declares the same output
//     live again. This is where the defect lived: the conflict-tolerant
//     insert left deleted_slot/spent_at_tx_id untouched, so the output
//     stayed spent forever.
//  4. A further real block spends the (now-restored) output again. Before
//     the fix this input can never be found live, so applying it fails; the
//     ledger pipeline halts exactly as dingo#4770 reports.
func TestImportLedgerStateCatchUpRestoresPostAnchorSpentUtxo(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	addr := buildShelleyAddr(
		0, 1,
		bytes.Repeat([]byte{0x11}, 28),
		bytes.Repeat([]byte{0x22}, 28),
	)
	// inlineUTxOMap keys its single entry's tx hash as 0x40 repeated 32
	// times (see inlineUTxOMap in import_deferred_reward_live_stake_test.go).
	utxoTxID := bytes.Repeat([]byte{0x40}, 32)

	newImportConfig := func(tipSlot uint64) ImportConfig {
		nonce := make([]byte, 32)
		eraBounds := make([]EraBound, EraConway+1)
		return ImportConfig{
			Database: db,
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
			State: &RawLedgerState{
				UTxOData:            inlineUTxOMap(t, addr, []uint64{5_000_000}),
				Epoch:               100,
				EraIndex:            EraConway,
				EraBounds:           eraBounds,
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Tip: &SnapshotTip{
					Slot:      tipSlot,
					BlockHash: make([]byte, 32),
				},
			},
			EpochLength: func(uint) (uint, uint, error) {
				return 1, 1_000, nil
			},
		}
	}

	// 1. Original bootstrap import: the output is live at anchor slot 1000.
	const anchorSlot = 1_000
	require.NoError(t, ImportLedgerState(
		context.Background(), newImportConfig(anchorSlot),
	))
	liveBefore, err := db.Metadata().GetUtxo(utxoTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, liveBefore, "precondition: output live after bootstrap")

	// 2. A real post-anchor block spends the output at slot 1500, through
	// the ordinary block-apply path (not a raw SQL edit).
	require.NoError(t, applySpendingTransaction(
		t, db, 0x51, utxoTxID, 0, 1_500,
	))
	spentAfterFirstSpend, err := db.Metadata().GetUtxo(utxoTxID, 0, nil)
	require.NoError(t, err)
	require.Nil(
		t, spentAfterFirstSpend,
		"precondition: output not live after the real post-anchor spend",
	)

	// 3. Catch-up / reward-repair re-import of a certified snapshot covering
	// the same anchor: the output is declared live again. This is the
	// dingo#4770 conflict path (ON CONFLICT (tx_id, output_idx) via
	// hydrateImportedUtxo).
	require.NoError(t, ImportLedgerState(
		context.Background(), newImportConfig(anchorSlot),
	))
	liveAfterCatchUp, err := db.Metadata().GetUtxo(utxoTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(
		t, liveAfterCatchUp,
		"the catch-up import declares this output live at the anchor; "+
			"the post-anchor spend must not survive re-import",
	)

	// 4. A subsequent real block spends the restored output. Before the fix
	// this fails, because the row never actually became live again.
	require.NoError(t, applySpendingTransaction(
		t, db, 0x52, utxoTxID, 0, 2_000,
	), "applying the real block that spends the repaired output must not "+
		"fail with 'utxo not found'")

	spentAfterSecondSpend, err := db.Metadata().GetUtxo(utxoTxID, 0, nil)
	require.NoError(t, err)
	require.Nil(t, spentAfterSecondSpend, "output must be spent again")
}

// TestImportLedgerStateReconcileCatchUpRestoresPostAnchorSpentUtxo is
// TestImportLedgerStateCatchUpRestoresPostAnchorSpentUtxo's second import run
// with Reconcile: true -- this codebase's own definition of a Mithril v2
// "catch-up" (see DATABASE.md, "Mithril v2 catch-up reconcile", and
// ImportConfig.Reconcile's doc comment). Reconcile only adds a post-import
// pass that tombstones live rows *absent* from the new snapshot's live set;
// it does not change how the UTxO import phase handles a conflicting row
// still present in that live set, which is the dingo#4770 path under test.
// This proves the fix holds under the literal catch-up flag, not only under
// the legacy reward-repair shape (Reconcile: false) the first test covers.
func TestImportLedgerStateReconcileCatchUpRestoresPostAnchorSpentUtxo(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	addr := buildShelleyAddr(
		0, 1,
		bytes.Repeat([]byte{0x33}, 28),
		bytes.Repeat([]byte{0x44}, 28),
	)
	utxoTxID := bytes.Repeat([]byte{0x40}, 32)
	govStateTxHash := bytes.Repeat([]byte{0x91}, 32)

	newImportConfig := func(tipSlot uint64, reconcile bool) ImportConfig {
		nonce := make([]byte, 32)
		eraBounds := make([]EraBound, EraConway+1)
		return ImportConfig{
			Database:  db,
			Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
			Reconcile: reconcile,
			State: &RawLedgerState{
				UTxOData:            inlineUTxOMap(t, addr, []uint64{5_000_000}),
				CertStateData:       minimalCertStateData(t),
				GovStateData:        testGovStateData(t, govStateTxHash, 100),
				Epoch:               100,
				EraIndex:            EraConway,
				EraBounds:           eraBounds,
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Tip: &SnapshotTip{
					Slot:      tipSlot,
					BlockHash: make([]byte, 32),
				},
			},
			EpochLength: func(uint) (uint, uint, error) {
				return 1, 1_000, nil
			},
		}
	}

	// 1. Bootstrap (Reconcile: false -- a fresh database has nothing to
	// reconcile against).
	const anchorSlot = 1_000
	require.NoError(t, ImportLedgerState(
		context.Background(), newImportConfig(anchorSlot, false),
	))
	liveBefore, err := db.Metadata().GetUtxo(utxoTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, liveBefore, "precondition: output live after bootstrap")

	// 2. A real post-anchor block spends the output.
	require.NoError(t, applySpendingTransaction(
		t, db, 0x53, utxoTxID, 0, 1_500,
	))
	spentAfterFirstSpend, err := db.Metadata().GetUtxo(utxoTxID, 0, nil)
	require.NoError(t, err)
	require.Nil(
		t, spentAfterFirstSpend,
		"precondition: output not live after the real post-anchor spend",
	)

	// 3. A literal catch-up import: Reconcile: true, same live UTxO set
	// (nothing else exists locally for reconcile to tombstone).
	require.NoError(t, ImportLedgerState(
		context.Background(), newImportConfig(anchorSlot, true),
	))
	liveAfterCatchUp, err := db.Metadata().GetUtxo(utxoTxID, 0, nil)
	require.NoError(t, err)
	require.NotNil(
		t, liveAfterCatchUp,
		"the reconcile catch-up declares this output live at the anchor; "+
			"the post-anchor spend must not survive re-import",
	)

	// 4. A subsequent real block spends the restored output.
	require.NoError(t, applySpendingTransaction(
		t, db, 0x54, utxoTxID, 0, 2_000,
	), "applying the real block that spends the repaired output must not "+
		"fail after a reconcile catch-up")

	spentAfterSecondSpend, err := db.Metadata().GetUtxo(utxoTxID, 0, nil)
	require.NoError(t, err)
	require.Nil(t, spentAfterSecondSpend, "output must be spent again")
}

// applySpendingTransaction builds a minimal, valid transaction consuming
// (inputTxID, inputIdx) and applies it through database.Database.SetTransaction
// -- the ordinary block-apply path -- at the given slot. txSeed distinguishes
// the built transaction's own hash across calls.
func applySpendingTransaction(
	t *testing.T,
	db *database.Database,
	txSeed byte,
	inputTxID []byte,
	inputIdx uint32,
	slot uint64,
) error {
	t.Helper()

	input, err := mockledger.NewSimpleTransactionInput(inputTxID, inputIdx)
	require.NoError(t, err)

	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd").
		WithLovelace(1_000_000).
		Build()
	require.NoError(t, err)

	tx, err := mockledger.NewTransactionBuilder().
		WithId(bytes.Repeat([]byte{txSeed}, 32)).
		WithInputs(input).
		WithOutputs(output).
		WithFee(200_000).
		Build()
	require.NoError(t, err)

	var txHashArray [32]byte
	copy(txHashArray[:], tx.Hash().Bytes())
	blockHash := bytes.Repeat([]byte{txSeed}, 32)
	var blockHashArray [32]byte
	copy(blockHashArray[:], blockHash)
	offsets := &database.BlockIngestionResult{
		TxOffsets: map[[32]byte]database.CborOffset{
			txHashArray: {
				BlockSlot:  slot,
				BlockHash:  blockHashArray,
				ByteLength: 1,
			},
		},
		UtxoOffsets: map[database.UtxoRef]database.CborOffset{
			{TxId: txHashArray, OutputIdx: 0}: {
				BlockSlot:  slot,
				BlockHash:  blockHashArray,
				ByteLength: 1,
			},
		},
	}

	point := ocommon.Point{Slot: slot, Hash: blockHash}
	return db.SetTransaction(tx, point, 0, 0, nil, nil, offsets, nil)
}
