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

package sqlstore

import (
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	fxcbor "github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"
)

// payloadTestStore returns an API-mode store that resolves payload offsets
// from the returned archive.
func payloadTestStore(t *testing.T) (*Store, *testutil.PayloadArchive) {
	t.Helper()
	store := newAPIModeSQLiteStore(t, nil)
	archive := testutil.NewPayloadArchive()
	store.SetPayloadResolver(archive)
	return store, archive
}

func bindPayloads(t *testing.T, txn types.Txn, archive *testutil.PayloadArchive) {
	t.Helper()
	setter, ok := txn.(types.PayloadLocatorSetter)
	require.True(t, ok, "the store's transaction takes a payload locator")
	setter.SetPayloadLocator(archive)
}

// columnBytes reads one column of the only row a query returns.
func columnBytes(t *testing.T, store *Store, query string, args ...any) []byte {
	t.Helper()
	var value []byte
	require.NoError(
		t, store.writeDB.QueryRow(query, args...).Scan(&value),
	)
	return value
}

func requireOffsetTo(
	t *testing.T,
	archive *testutil.PayloadArchive,
	column []byte,
	payload []byte,
) {
	t.Helper()
	require.Len(t, column, types.CborOffsetSize, "the column holds an offset")
	require.NotEqual(t, payload, column, "the column holds a copy")
	got, err := archive.ResolvePayload(column)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

// A transaction's datum, metadata and label columns hold offsets into the
// block, and every reader returns the bytes the offsets name.
func TestTransactionPayloadColumnsHoldOffsets(t *testing.T) {
	t.Parallel()
	store, archive := payloadTestStore(t)
	tx, point := apiDetailTx(t, 0x31)
	datum := []byte{0x18, 0x2a}
	metadata := []byte{0xa2, 0x01, 0x61, 0x61, 0x02, 0x05}

	txn := store.Transaction(context.Background())
	defer txn.Rollback() //nolint:errcheck
	bindPayloads(t, txn, archive)
	require.NoError(
		t, store.SetTransaction(tx, point, 0, nil, true, txn, 0),
	)
	require.NoError(t, txn.Commit())

	requireOffsetTo(t, archive, columnBytes(
		t, store, "SELECT raw_datum FROM datum WHERE hash = ?",
		lcommon.Blake2b256Hash(datum).Bytes(),
	), datum)
	requireOffsetTo(t, archive, columnBytes(
		t, store,
		"SELECT metadata FROM applied_transaction WHERE slot = ?",
		point.Slot,
	), metadata)
	requireOffsetTo(t, archive, columnBytes(
		t, store,
		"SELECT cbor_value FROM transaction_metadata_label WHERE label = '2'",
	), []byte{0x05})

	gotDatum, err := store.GetDatum(lcommon.Blake2b256Hash(datum), nil)
	require.NoError(t, err)
	require.Equal(t, datum, gotDatum.RawDatum)
	gotTx, err := store.GetTransactionByHash(tx.Hash().Bytes(), nil)
	require.NoError(t, err)
	require.Equal(t, metadata, gotTx.Metadata)
	gotMetadata, err := store.GetTransactionMetadataByHash(
		tx.Hash().Bytes(), nil,
	)
	require.NoError(t, err)
	require.Equal(t, metadata, gotMetadata)
}

// A payload write with no block to locate the bytes in fails, and writes
// nothing: no column ever holds a copy.
func TestTransactionPayloadWriteRefusesACopy(t *testing.T) {
	t.Parallel()
	store, _ := payloadTestStore(t)
	tx, point := apiDetailTx(t, 0x32)

	txn := store.Transaction(context.Background())
	defer txn.Rollback() //nolint:errcheck
	err := store.SetTransaction(tx, point, 0, nil, true, txn, 0)
	require.ErrorIs(t, err, types.ErrPayloadLocatorRequired)
	require.NoError(t, txn.Rollback())

	var count int
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM datum",
	).Scan(&count))
	require.Zero(t, count)
}

// A payload the block does not hold cannot be named, so the write fails
// rather than recording an offset that reads back other bytes.
func TestPayloadWriteRefusesBytesTheBlockLacks(t *testing.T) {
	t.Parallel()
	store, _ := payloadTestStore(t)
	locator, err := types.NewBlockPayloadLocator(
		5, bytes.Repeat([]byte{0x05}, 32), []byte("a block with no datum"),
	)
	require.NoError(t, err)
	txn := store.Transaction(context.Background())
	defer txn.Rollback() //nolint:errcheck
	txn.(types.PayloadLocatorSetter).SetPayloadLocator(locator)

	err = store.SetDatum(
		lcommon.Blake2b256Hash([]byte{0x18, 0x2a}), []byte{0x18, 0x2a}, 5, txn,
	)
	require.ErrorContains(t, err, "not found in block")
}

func TestPParamUpdateColumnHoldsAnOffset(t *testing.T) {
	t.Parallel()
	store, archive := payloadTestStore(t)
	update := []byte{0xa1, 0x00, 0x1a, 0x00, 0x01, 0x00, 0x00}

	txn := store.Transaction(context.Background())
	defer txn.Rollback() //nolint:errcheck
	bindPayloads(t, txn, archive)
	require.NoError(t, store.SetPParamUpdate(
		bytes.Repeat([]byte{0x07}, 28), update, 10, 4, txn,
	))
	require.NoError(t, txn.Commit())

	requireOffsetTo(t, archive, columnBytes(
		t, store, "SELECT cbor FROM pparam_update",
	), update)
	rows, err := store.GetPParamUpdates(4, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, update, rows[0].Cbor)
	all, err := store.ListPParamUpdates(nil)
	require.NoError(t, err)
	require.Len(t, all, 1)
	require.Equal(t, update, all[0].Cbor)
}

// A Midnight row that carries an earlier row's datum records that row's
// offset, so it needs no block of its own, and every Midnight reader returns
// the datum bytes.
func TestMidnightDatumColumnsHoldOffsets(t *testing.T) {
	t.Parallel()
	store, archive := payloadTestStore(t)
	datum := []byte{0xd8, 0x79, 0x9f, 0x01, 0xff}
	candidate := []byte{0xd8, 0x79, 0x9f, 0x02, 0xff}

	registration := &models.MidnightRegistration{
		FullDatum: datum, TxHash: bytes.Repeat([]byte{0x01}, 32),
		BlockNumber: 5, BlockHash: bytes.Repeat([]byte{0x05}, 32),
	}
	governance := &models.MidnightGovernanceDatum{
		DatumType: models.MidnightGovernanceDatumTypeCouncil,
		TxHash:    bytes.Repeat([]byte{0x02}, 32), Datum: datum, BlockNumber: 5,
	}
	txn := store.Transaction(context.Background())
	defer txn.Rollback() //nolint:errcheck
	bindPayloads(t, txn, archive)
	require.NoError(t, store.CreateMidnightRegistration(txn, registration))
	require.NoError(t, store.InsertMidnightGovernanceDatum(txn, governance))
	require.NoError(t, txn.Commit())
	requireOffsetTo(t, archive, registration.FullDatumOffset, datum)

	// The deregistration is written in a later block: it carries the
	// registration's offset and no locator is bound.
	later := store.Transaction(context.Background())
	defer later.Rollback() //nolint:errcheck
	require.NoError(t, store.CreateMidnightDeregistration(
		later,
		&models.MidnightDeregistration{
			FullDatum:       datum,
			FullDatumOffset: registration.FullDatumOffset,
			TxHash:          bytes.Repeat([]byte{0x03}, 32),
			UtxoTxHash:      registration.TxHash,
			BlockNumber:     6, BlockHash: bytes.Repeat([]byte{0x06}, 32),
		},
	))
	require.NoError(t, later.Commit())
	unbound := store.Transaction(context.Background())
	err := store.CreateMidnightDeregistration(
		unbound,
		&models.MidnightDeregistration{
			FullDatum:   datum,
			TxHash:      bytes.Repeat([]byte{0x04}, 32),
			UtxoTxHash:  bytes.Repeat([]byte{0x09}, 32),
			BlockNumber: 7, BlockHash: bytes.Repeat([]byte{0x07}, 32),
		},
	)
	require.ErrorIs(t, err, types.ErrPayloadLocatorRequired)
	require.NoError(t, unbound.Rollback())

	requireOffsetTo(t, archive, columnBytes(
		t, store, "SELECT full_datum FROM midnight_registrations",
	), datum)
	requireOffsetTo(t, archive, columnBytes(
		t, store, "SELECT full_datum FROM midnight_deregistrations",
	), datum)
	requireOffsetTo(t, archive, columnBytes(
		t, store, "SELECT datum FROM midnight_governance_datums",
	), datum)
	registrations, err := store.FindMidnightRegistrationsFrom(0, 0, 10, nil)
	require.NoError(t, err)
	require.Len(t, registrations, 1)
	require.Equal(t, datum, registrations[0].FullDatum)
	deregistrations, err := store.FindMidnightDeregistrationsFrom(0, 0, 10, nil)
	require.NoError(t, err)
	require.Len(t, deregistrations, 1)
	require.Equal(t, datum, deregistrations[0].FullDatum)
	latest, err := store.GetLatestMidnightGovernanceDatum(
		models.MidnightGovernanceDatumTypeCouncil, 5, nil,
	)
	require.NoError(t, err)
	require.Equal(t, datum, latest.Datum)

	// Ariadne parameters, their rollback journal and candidate removals.
	txn = store.Transaction(context.Background())
	defer txn.Rollback() //nolint:errcheck
	bindPayloads(t, txn, archive)
	ariadne := &models.MidnightAriadneParams{Epoch: 3, Datum: datum}
	require.NoError(t, store.UpsertMidnightAriadneParams(txn, ariadne))
	require.NoError(t, store.CreateMidnightAriadneRollback(
		txn,
		&models.MidnightAriadneRollback{
			BlockNumber: 5, Epoch: 3, PreviousExists: true,
			PreviousDatum: datum,
		},
	))
	require.NoError(t, store.CreateMidnightCandidateRemoval(
		txn,
		&models.MidnightCandidateRemoval{
			BlockNumber: 5, TxHash: bytes.Repeat([]byte{0x08}, 32),
			Datum: candidate,
		},
	))
	require.NoError(t, txn.Commit())
	gotAriadne, err := store.GetMidnightAriadneParamsByEpoch(3, nil)
	require.NoError(t, err)
	require.Equal(t, datum, gotAriadne.Datum)
	require.Equal(t, ariadne.DatumOffset, gotAriadne.DatumOffset)
	rollbacks, err := store.FindMidnightAriadneRollbacksByBlock(nil, 5)
	require.NoError(t, err)
	require.Len(t, rollbacks, 1)
	require.Equal(t, datum, rollbacks[0].PreviousDatum)
	removals, err := store.FindMidnightCandidateRemovalsByBlock(nil, 5)
	require.NoError(t, err)
	require.Len(t, removals, 1)
	require.Equal(t, candidate, removals[0].Datum)
}

// A candidate snapshot is stored with the offset of each datum and read back
// in the form every reader decodes, byte for byte.
func TestMidnightEpochCandidatesStoreDatumOffsets(t *testing.T) {
	t.Parallel()
	store, archive := payloadTestStore(t)
	datum := []byte{0xd8, 0x79, 0x9f, 0x01, 0xff}
	hash := bytes.Repeat([]byte{0x0a}, 32)
	snapshot, err := fxcborMarshal([]candidateWriteEntry{
		{TxHash: hash, OutputIndex: 1, Datum: datum},
		{TxHash: hash, OutputIndex: 2},
	})
	require.NoError(t, err)

	txn := store.Transaction(context.Background())
	defer txn.Rollback() //nolint:errcheck
	bindPayloads(t, txn, archive)
	require.NoError(t, store.UpsertMidnightEpochCandidates(
		txn,
		&models.MidnightEpochCandidates{
			Epoch: 9, BlockNumber: 5, CandidatesCbor: snapshot,
		},
	))
	require.NoError(t, txn.Commit())

	stored := columnBytes(
		t, store, "SELECT candidates_cbor FROM midnight_epoch_candidates",
	)
	require.NotContains(t, string(stored), string(datum), "the datum is copied")
	got, err := store.GetMidnightEpochCandidatesByEpoch(9, nil)
	require.NoError(t, err)
	want, err := fxcborMarshal([]candidateDatumEntry{
		{TxHash: hash, OutputIndex: 1, Datum: datum},
		{TxHash: hash, OutputIndex: 2},
	})
	require.NoError(t, err)
	require.Equal(t, want, got.CandidatesCbor)
}

func fxcborMarshal(v any) ([]byte, error) {
	return fxcbor.Marshal(v)
}
