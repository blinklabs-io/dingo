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
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// migrationBlock is the one block the migration tests read every payload from.
type migrationBlock struct {
	locator  types.PayloadLocator
	block    []byte
	requests []uint64
}

func newMigrationBlock(t *testing.T, payloads ...[]byte) *migrationBlock {
	t.Helper()
	block := bytes.Join(payloads, []byte("|"))
	locator, err := types.NewBlockPayloadLocator(
		5, bytes.Repeat([]byte{0x05}, 32), block,
	)
	require.NoError(t, err)
	return &migrationBlock{locator: locator, block: block}
}

func (b *migrationBlock) LocatorAtSlot(
	_ context.Context,
	slot uint64,
) (types.PayloadLocator, error) {
	b.requests = append(b.requests, slot)
	return b.locator, nil
}

func (b *migrationBlock) ResolvePayload(encoded []byte) ([]byte, error) {
	offset, err := types.DecodeCborOffset(encoded)
	if err != nil {
		return nil, err
	}
	return bytes.Clone(
		b.block[offset.ByteOffset : offset.ByteOffset+offset.ByteLength],
	), nil
}

type migrationFixture struct {
	datum, script, metadata, label, update []byte
	registration, candidate                []byte
	snapshot                               []byte
}

// seedReleasedPayloadRows writes every payload column the way the released
// store did: the bytes themselves.
func seedReleasedPayloadRows(t *testing.T, store *Store) migrationFixture {
	t.Helper()
	fx := migrationFixture{
		datum:        []byte{0xd8, 0x79, 0x9f, 0x01, 0xff},
		script:       []byte{0x49, 0x01, 0x00, 0x00, 0x22, 0x23, 0x32, 0x00},
		metadata:     []byte{0xa1, 0x19, 0x02, 0xd1, 0x65, 'h', 'e', 'l', 'l', 'o'},
		label:        []byte{0x65, 'h', 'e', 'l', 'l', 'o'},
		update:       []byte{0xa1, 0x00, 0x1a, 0x00, 0x01, 0x00, 0x00},
		registration: []byte{0xd8, 0x79, 0x9f, 0x02, 0xff},
		candidate:    []byte{0xd8, 0x79, 0x9f, 0x03, 0xff},
	}
	txHash := bytes.Repeat([]byte{0x11}, 32)
	blockHash := bytes.Repeat([]byte{0x05}, 32)
	exec := func(query string, args ...any) {
		t.Helper()
		_, err := store.writeDB.Exec(query, args...)
		require.NoError(t, err, query)
	}
	for _, datum := range [][]byte{fx.datum, fx.registration, fx.candidate} {
		exec(
			"INSERT INTO datum (hash, raw_datum, added_slot) VALUES (?, ?, 5)",
			lcommon.Blake2b256Hash(datum).Bytes(), datum,
		)
	}
	exec(
		"INSERT INTO script (hash, content, created_slot, type) VALUES (?, ?, 5, 1)",
		bytes.Repeat([]byte{0x22}, 28), fx.script,
	)
	exec(`INSERT INTO "transaction" (id, hash) VALUES (1, ?)`, txHash)
	exec(
		`INSERT INTO applied_transaction
(tx_id, block_hash, metadata, slot, type, fee, collateral_fee, ttl, block_index, valid)
VALUES (1, ?, ?, 5, 0, '0', '0', '0', 0, 1)`,
		blockHash, fx.metadata,
	)
	exec(
		`INSERT INTO transaction_metadata_label
(transaction_id, label, slot, cbor_value, json_value) VALUES (1, '721', 5, ?, '"hello"')`,
		fx.label,
	)
	exec(
		"INSERT INTO pparam_update (genesis_hash, cbor, added_slot, epoch) VALUES (?, ?, 5, 4)",
		bytes.Repeat([]byte{0x07}, 28), fx.update,
	)
	exec(
		`INSERT INTO midnight_registrations
(full_datum, tx_hash, output_index, block_number, block_hash, tx_index, block_timestamp_ms)
VALUES (?, ?, 0, 5, ?, 0, 0)`,
		fx.registration, txHash, blockHash,
	)
	exec(
		`INSERT INTO midnight_deregistrations
(full_datum, tx_hash, utxo_tx_hash, utxo_index, block_number, block_hash, tx_index, block_timestamp_ms)
VALUES (?, ?, ?, 0, 5, ?, 0, 0)`,
		fx.registration, bytes.Repeat([]byte{0x12}, 32), txHash, blockHash,
	)
	exec(
		`INSERT INTO midnight_governance_datums
(datum_type, tx_hash, output_index, datum, block_number) VALUES ('council', ?, 0, ?, 5)`,
		txHash, fx.datum,
	)
	exec(
		"INSERT INTO midnight_ariadne_params (epoch, datum) VALUES (3, ?)",
		fx.datum,
	)
	exec(
		`INSERT INTO midnight_ariadne_rollbacks
(block_number, epoch, previous_exists, previous_datum) VALUES (5, 3, 1, ?)`,
		fx.datum,
	)
	exec(
		`INSERT INTO midnight_ariadne_rollbacks
(block_number, epoch, previous_exists, previous_datum) VALUES (5, 4, 0, NULL)`,
	)
	exec(
		`INSERT INTO midnight_candidate_removals
(block_number, tx_hash, output_index, datum) VALUES (5, ?, 0, ?)`,
		txHash, fx.candidate,
	)
	snapshot, err := fxcborMarshal([]candidateDatumEntry{
		{TxHash: txHash, OutputIndex: 0, Datum: fx.candidate},
	})
	require.NoError(t, err)
	fx.snapshot = snapshot
	exec(
		"INSERT INTO midnight_epoch_candidates (epoch, block_number, candidates_cbor) VALUES (3, 5, ?)",
		snapshot,
	)
	return fx
}

func requireMigratedReads(
	t *testing.T,
	store *Store,
	fx migrationFixture,
) {
	t.Helper()
	datum, err := store.GetDatum(lcommon.Blake2b256Hash(fx.datum), nil)
	require.NoError(t, err)
	require.Equal(t, fx.datum, datum.RawDatum)
	script, err := store.GetScript(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0x22}, 28)), nil,
	)
	require.NoError(t, err)
	require.Equal(t, fx.script, script.Content)
	metadata, err := store.GetTransactionMetadataByHash(
		bytes.Repeat([]byte{0x11}, 32), nil,
	)
	require.NoError(t, err)
	require.Equal(t, fx.metadata, metadata)
	updates, err := store.GetPParamUpdates(4, nil)
	require.NoError(t, err)
	require.Len(t, updates, 1)
	require.Equal(t, fx.update, updates[0].Cbor)
	registrations, err := store.FindMidnightRegistrationsFrom(0, 0, 10, nil)
	require.NoError(t, err)
	require.Len(t, registrations, 1)
	require.Equal(t, fx.registration, registrations[0].FullDatum)
	deregistrations, err := store.FindMidnightDeregistrationsFrom(0, 0, 10, nil)
	require.NoError(t, err)
	require.Len(t, deregistrations, 1)
	require.Equal(t, fx.registration, deregistrations[0].FullDatum)
	governance, err := store.GetLatestMidnightGovernanceDatum("council", 5, nil)
	require.NoError(t, err)
	require.Equal(t, fx.datum, governance.Datum)
	ariadne, err := store.GetMidnightAriadneParamsByEpoch(3, nil)
	require.NoError(t, err)
	require.Equal(t, fx.datum, ariadne.Datum)
	rollbacks, err := store.FindMidnightAriadneRollbacksByBlock(nil, 5)
	require.NoError(t, err)
	require.Len(t, rollbacks, 2)
	require.Equal(t, fx.datum, rollbacks[0].PreviousDatum)
	require.Empty(t, rollbacks[1].PreviousDatum)
	removals, err := store.FindMidnightCandidateRemovalsByBlock(nil, 5)
	require.NoError(t, err)
	require.Len(t, removals, 1)
	require.Equal(t, fx.candidate, removals[0].Datum)
	snapshot, err := store.GetMidnightEpochCandidatesByEpoch(3, nil)
	require.NoError(t, err)
	require.Equal(t, fx.snapshot, snapshot.CandidatesCbor)
}

// Converting a released store's rows gives every reader the bytes it read
// before, from block offsets instead of copies.
func TestMigratePayloadColumnsKeepsEveryReaderResult(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	fx := seedReleasedPayloadRows(t, store)
	block := newMigrationBlock(
		t, fx.datum, fx.script, fx.metadata, fx.label, fx.update,
		fx.registration, fx.candidate,
	)
	store.SetPayloadResolver(block)

	require.NoError(t, store.MigratePayloadColumns(t.Context(), block))
	requireMigratedReads(t, store, fx)

	for table, column := range map[string]string{
		"datum":                       "raw_datum",
		"script":                      "content",
		"applied_transaction":         "metadata",
		"transaction_metadata_label":  "cbor_value",
		"pparam_update":               "cbor",
		"midnight_registrations":      "full_datum",
		"midnight_deregistrations":    "full_datum",
		"midnight_governance_datums":  "datum",
		"midnight_ariadne_params":     "datum",
		"midnight_candidate_removals": "datum",
	} {
		rows, err := store.writeDB.Query(
			"SELECT " + column + " FROM " + table,
		)
		require.NoError(t, err, table)
		for rows.Next() {
			var value []byte
			require.NoError(t, rows.Scan(&value))
			require.Len(t, value, types.CborOffsetSize, "%s.%s", table, column)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	label := columnBytes(
		t, store, "SELECT cbor_value FROM transaction_metadata_label",
	)
	resolved, err := block.ResolvePayload(label)
	require.NoError(t, err)
	require.Equal(t, fx.label, resolved)

	// A second run finds every column converted and reads no block.
	block.requests = nil
	require.NoError(t, store.MigratePayloadColumns(t.Context(), block))
	require.Empty(t, block.requests)
	requireMigratedReads(t, store, fx)
}

// A payload its block does not hold stops the migration with the row named,
// and no row is left holding a made-up offset. Datum and script rows are the
// exception: see the orphan tests in the database package.
func TestMigratePayloadColumnsFailsWhenTheBlockLacksAPayload(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	fx := seedReleasedPayloadRows(t, store)
	block := newMigrationBlock(
		t, fx.datum, fx.script, fx.registration, fx.candidate,
	)

	err := store.MigratePayloadColumns(t.Context(), block)
	require.ErrorContains(t, err, "applied_transaction")
	require.ErrorContains(t, err, "not found in block")
	require.Equal(t, fx.metadata, columnBytes(
		t, store, "SELECT metadata FROM applied_transaction",
	), "the metadata row was rewritten")
}

type failingBlocks struct{}

func (failingBlocks) LocatorAtSlot(
	context.Context,
	uint64,
) (types.PayloadLocator, error) {
	return nil, errors.New("history expired")
}

func TestMigratePayloadColumnsFailsWhenABlockCannotBeRead(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	fx := seedReleasedPayloadRows(t, store)

	err := store.MigratePayloadColumns(t.Context(), failingBlocks{})
	require.ErrorContains(t, err, "history expired")
	require.Equal(t, fx.datum, columnBytes(
		t, store,
		"SELECT raw_datum FROM datum ORDER BY id LIMIT 1",
	))
}

type expiredBlocks struct{}

func (expiredBlocks) LocatorAtSlot(
	_ context.Context,
	slot uint64,
) (types.PayloadLocator, error) {
	return nil, &types.HistoryExpiredError{
		Slot: slot, Hash: bytes.Repeat([]byte{0xee}, 32),
	}
}

// A payload whose block expired and that no archive serves is not kept: the
// column holds a marker naming the block, and the migration completes.
func TestMigratePayloadColumnsMarksPayloadsOfExpiredBlocks(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	fx := seedReleasedPayloadRows(t, store)
	_ = fx
	require.NoError(t, store.MigratePayloadColumns(t.Context(), expiredBlocks{}))

	column := columnBytes(
		t, store, "SELECT raw_datum FROM datum ORDER BY id LIMIT 1",
	)
	expired, err := types.DecodeExpiredPayload(column)
	require.NoError(t, err)
	require.EqualValues(t, 5, expired.Slot)
	require.Equal(t, bytes.Repeat([]byte{0xee}, 32), expired.Hash)
	store.SetPayloadResolver(failingResolver{})
	_, err = store.GetDatum(lcommon.Blake2b256Hash(fx.datum), nil)
	require.Error(t, err)
}

type failingResolver struct{}

func (failingResolver) ResolvePayload([]byte) ([]byte, error) {
	return nil, errors.New("resolver reached")
}
