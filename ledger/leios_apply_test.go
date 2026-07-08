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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func newLeiosApplyTestLedger(
	t *testing.T,
) (*LedgerState, *database.Database, *gorm.DB) {
	t.Helper()
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	store, ok := db.Metadata().(*sqlite.MetadataStoreSqlite)
	require.True(t, ok, "expected sqlite metadata store")
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	return ls, db, store.DB()
}

func leiosApplyTestTx(
	t *testing.T,
	seed byte,
) (cbor.RawMessage, []byte, lcommon.Transaction) {
	t.Helper()
	bodyCbor, err := cbor.Encode(map[uint]any{
		2: 200_000 + uint64(seed),
	})
	require.NoError(t, err)
	txCbor, err := cbor.Encode([]any{
		cbor.RawMessage(bodyCbor),
		map[uint]any{},
		true,
		nil,
	})
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(
		gledger.TxTypeDijkstra,
		txCbor,
	)
	require.NoError(t, err)
	return cbor.RawMessage(txCbor), bodyCbor, tx
}

func leiosApplyTestEbHash(seed byte) []byte {
	return bytes.Repeat([]byte{seed}, lcommon.Blake2b256Size)
}

func leiosApplyTestRankingPoint(seed byte) ocommon.Point {
	return ocommon.Point{
		Slot: 10_000 + uint64(seed),
		Hash: bytes.Repeat([]byte{seed}, lcommon.Blake2b256Size),
	}
}

func requireLeiosApplyTestTxCount(
	t *testing.T,
	gdb *gorm.DB,
	want int64,
) {
	t.Helper()
	var got int64
	require.NoError(t, gdb.Model(&models.Transaction{}).Count(&got).Error)
	require.Equal(t, want, got)
}

func requireLeiosApplyTestEndorserBlob(
	t *testing.T,
	db *database.Database,
	slot uint64,
	hash []byte,
	want []byte,
) {
	t.Helper()
	txn := db.BlobTxn(false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		got, _, err := db.Blob().GetBlock(txn.Blob(), slot, hash)
		if err != nil {
			return err
		}
		require.Equal(t, want, got)
		return nil
	}))
}

func TestApplyEndorserBlockAppliesTransaction(t *testing.T) {
	ls, db, gdb := newLeiosApplyTestLedger(t)
	ls.config.LeiosApplyEndorserBlockTxs = true // CIP-conformant path
	rawTx, bodyCbor, tx := leiosApplyTestTx(t, 0x01)

	const ebSlot = uint64(200)
	ebHash := leiosApplyTestEbHash(0x22)
	applied := -1
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var err error
		applied, err = ls.applyEndorserBlock(
			txn,
			leiosApplyTestRankingPoint(0x33),
			1,
			ebSlot,
			ebHash,
			[]cbor.RawMessage{rawTx},
		)
		return err
	}))

	require.Equal(t, 1, applied)
	requireLeiosApplyTestTxCount(t, gdb, 1)
	requireLeiosApplyTestEndorserBlob(t, db, ebSlot, ebHash, bodyCbor)
	// The transaction is recorded under the ranking block's point.
	var got int64
	require.NoError(t, gdb.Model(&models.Transaction{}).
		Where("hash = ?", tx.Hash().Bytes()).
		Count(&got).Error)
	require.Equal(t, int64(1), got)
}

func TestApplyEndorserBlockAppliesMultipleTransactions(t *testing.T) {
	ls, db, gdb := newLeiosApplyTestLedger(t)
	ls.config.LeiosApplyEndorserBlockTxs = true // CIP-conformant path
	rawTx1, body1, _ := leiosApplyTestTx(t, 0x02)
	rawTx2, body2, _ := leiosApplyTestTx(t, 0x03)

	const ebSlot = uint64(300)
	ebHash := leiosApplyTestEbHash(0x44)
	applied := -1
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var err error
		applied, err = ls.applyEndorserBlock(
			txn,
			leiosApplyTestRankingPoint(0x55),
			1,
			ebSlot,
			ebHash,
			[]cbor.RawMessage{rawTx1, rawTx2},
		)
		return err
	}))

	require.Equal(t, 2, applied)
	requireLeiosApplyTestTxCount(t, gdb, 2)
	// The blob is a flat concatenation of each transaction's body CBOR (these
	// transactions produce no outputs).
	want := append(append([]byte{}, body1...), body2...)
	requireLeiosApplyTestEndorserBlob(t, db, ebSlot, ebHash, want)
}

func TestApplyEndorserBlockDeduplicatesCIPTransactions(t *testing.T) {
	ls, db, gdb := newLeiosApplyTestLedger(t)
	ls.config.LeiosApplyEndorserBlockTxs = true // CIP-conformant path
	rawTx1, body1, _ := leiosApplyTestTx(t, 0x04)
	rawTx2, _, _ := leiosApplyTestTx(t, 0x05)

	appliedFirst := -1
	appliedSameTxnDuplicate := -1
	appliedSecondUnique := -1
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var err error
		appliedFirst, err = ls.applyEndorserBlock(
			txn,
			leiosApplyTestRankingPoint(0x81),
			1,
			500,
			leiosApplyTestEbHash(0x82),
			[]cbor.RawMessage{rawTx1, rawTx1},
		)
		if err != nil {
			return err
		}
		appliedSameTxnDuplicate, err = ls.applyEndorserBlock(
			txn,
			leiosApplyTestRankingPoint(0x83),
			2,
			501,
			leiosApplyTestEbHash(0x84),
			[]cbor.RawMessage{rawTx1},
		)
		if err != nil {
			return err
		}
		appliedSecondUnique, err = ls.applyEndorserBlock(
			txn,
			leiosApplyTestRankingPoint(0x85),
			3,
			502,
			leiosApplyTestEbHash(0x86),
			[]cbor.RawMessage{rawTx2},
		)
		return err
	}))

	require.Equal(t, 1, appliedFirst)
	require.Equal(t, 0, appliedSameTxnDuplicate)
	require.Equal(t, 1, appliedSecondUnique)
	requireLeiosApplyTestTxCount(t, gdb, 2)
	requireLeiosApplyTestEndorserBlob(
		t,
		db,
		500,
		leiosApplyTestEbHash(0x82),
		body1,
	)

	appliedCommittedDuplicate := -1
	txn = db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var err error
		appliedCommittedDuplicate, err = ls.applyEndorserBlock(
			txn,
			leiosApplyTestRankingPoint(0x87),
			4,
			503,
			leiosApplyTestEbHash(0x88),
			[]cbor.RawMessage{rawTx1},
		)
		return err
	}))
	require.Equal(t, 0, appliedCommittedDuplicate)
	requireLeiosApplyTestTxCount(t, gdb, 2)
}

// On the Haskell-conformant path (Musashi prototype) the endorser block is
// stored but its transactions are not applied to the UTxO.
func TestApplyEndorserBlockHaskellPathStoresWithoutApplying(t *testing.T) {
	ls, db, gdb := newLeiosApplyTestLedger(t)
	// LeiosApplyEndorserBlockTxs defaults to false (Haskell-conformant).
	rawTx, bodyCbor, _ := leiosApplyTestTx(t, 0x06)

	const ebSlot = uint64(400)
	ebHash := leiosApplyTestEbHash(0x66)
	applied := -1
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var err error
		applied, err = ls.applyEndorserBlock(
			txn,
			leiosApplyTestRankingPoint(0x77),
			1,
			ebSlot,
			ebHash,
			[]cbor.RawMessage{rawTx},
		)
		return err
	}))

	// Nothing applied to the UTxO, but the endorser blob is still stored.
	require.Equal(t, 0, applied)
	requireLeiosApplyTestTxCount(t, gdb, 0)
	requireLeiosApplyTestEndorserBlob(t, db, ebSlot, ebHash, bodyCbor)
}

// leiosTestHash returns a distinct 32-byte hash whose bytes are all b, usable
// as both a map key (string form) and an endorser-block hash.
func leiosTestHash(b byte) []byte {
	return bytes.Repeat([]byte{b}, lcommon.Blake2b256Size)
}

// TestClassifyEndorserBlockFetches verifies the fetch policy: near the head,
// endorser blocks are fetched on announcement; in the settled backlog they are
// fetched only when a certifying ranking block certifies them (via its parent's
// announcement), and uncertified historical announcements are skipped.
func TestClassifyEndorserBlockFetches(t *testing.T) {
	var (
		hashA = leiosTestHash(0xA1) // announces ebA (historical)
		hashC = leiosTestHash(0xC1) // certifies, parent = A (historical)
		hashD = leiosTestHash(0xD1) // announces ebB (near head)
		hashE = leiosTestHash(0xE1) // announces ebE (historical, uncertified)
		ebA   = lcommon.NewBlake2b256(leiosTestHash(0x0A))
		ebB   = lcommon.NewBlake2b256(leiosTestHash(0x0B))
		ebE   = lcommon.NewBlake2b256(leiosTestHash(0x0E))
	)
	infos := []leiosBlockInfo{
		{hash: string(hashA), slot: 100, announces: true, ebHash: ebA},
		{hash: string(hashC), prevHash: string(hashA), slot: 140, certifies: true},
		{hash: string(hashD), slot: 100_000, announces: true, ebHash: ebB},
		{hash: string(hashE), slot: 200, announces: true, ebHash: ebE},
	}
	annByHash := map[string]leiosEbRef{
		string(hashA): {slot: 100, hash: ebA},
		string(hashD): {slot: 100_000, hash: ebB},
		string(hashE): {slot: 200, hash: ebE},
	}
	neverCached := func(lcommon.Blake2b256) bool { return false }

	// Haskell/cert-driven path. wallSlot 100050, waitSlots 100: slots 100/140/200
	// are settled backlog, slot 100000 is within the head window.
	backfill, tipWait := classifyEndorserBlockFetches(
		infos, annByHash, 100_050, true, 100, true, neverCached,
	)
	// Only the certified endorser block (ebA, via CertRB C's parent A) is
	// backfilled; the uncertified historical announcement (ebE) is skipped.
	require.Len(t, backfill, 1)
	require.Equal(t, ebA, backfill[0].hash)
	require.Equal(t, uint64(100), backfill[0].slot)
	// Only the near-head announcement (ebB) is fetched on announcement.
	require.Len(t, tipWait, 1)
	require.Equal(t, ebB, tipWait[0].hash)

	// A cached endorser block is not refetched.
	backfill, _ = classifyEndorserBlockFetches(
		infos, annByHash, 100_050, true, 100, true,
		func(h lcommon.Blake2b256) bool { return h == ebA },
	)
	require.Empty(t, backfill)

	// CIP path (certDrivenHistorical=false): the settled backlog is
	// announcement-driven, so every referenced historical endorser block is
	// backfilled (ebA and ebE), not just certified ones, and the near-head
	// announcement (ebB) still goes to tipWait.
	backfill, tipWait = classifyEndorserBlockFetches(
		infos, annByHash, 100_050, true, 100, false, neverCached,
	)
	require.ElementsMatch(
		t,
		[]lcommon.Blake2b256{ebA, ebE},
		[]lcommon.Blake2b256{backfill[0].hash, backfill[1].hash},
	)
	require.Len(t, tipWait, 1)
	require.Equal(t, ebB, tipWait[0].hash)

	// With an unknown wall-clock slot every block is treated as near-head, so
	// all announcements fetch on announcement and none go to backfill.
	backfill, tipWait = classifyEndorserBlockFetches(
		infos, annByHash, 0, false, 100, true, neverCached,
	)
	require.Empty(t, backfill)
	require.Len(t, tipWait, 3) // ebA, ebB, ebE (all announcements)
}
