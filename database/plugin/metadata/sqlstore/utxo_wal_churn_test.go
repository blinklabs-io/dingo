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
	"context"
	"crypto/sha256"
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// churnHash derives a uniformly distributed hash from two counters, as real
// credentials and transaction hashes are, so insert order says nothing about
// index key order.
func churnHash(kind byte, a, b uint64, size int) []byte {
	var raw [17]byte
	raw[0] = kind
	binary.BigEndian.PutUint64(raw[1:], a)
	binary.BigEndian.PutUint64(raw[9:], b)
	sum := sha256.Sum256(raw[:])
	return sum[:size]
}

// churnOutputKind selects what an output's address carries, which decides
// which utxo indexes hold an entry for it.
type churnOutputKind int

const (
	// churnEnterprise is a key-hash address with no stake part, as every
	// Byron-era output is.
	churnEnterprise churnOutputKind = iota
	// churnBase is a key-hash address with a stake credential.
	churnBase
	// churnScript is a script-hash address with a stake credential.
	churnScript
)

func churnKindFor(mix string, n uint64) churnOutputKind {
	if mix == "byron" {
		return churnEnterprise
	}
	switch n % 10 {
	case 0:
		return churnScript
	case 1, 2, 3:
		return churnEnterprise
	default:
		return churnBase
	}
}

func churnAmount(n uint64) string {
	sum := churnHash('a', n, 0, 8)
	return strconv.FormatUint(
		1_000_000+binary.BigEndian.Uint64(sum)%90_000_000_000, 10,
	)
}

// seedChurnUtxos inserts n outputs directly: every second row is spent, each
// with its own ascending deleted_slot, the rest live. Returns the live rows'
// (tx_id, output_idx) so a measured transaction can spend them.
func seedChurnUtxos(
	tb testing.TB,
	store *Store,
	mix string,
	n int,
) [][]byte {
	tb.Helper()
	tx, err := store.writeDB.Begin()
	require.NoError(tb, err)
	stmt, err := tx.Prepare(
		"INSERT INTO utxo (tx_id, output_idx, payment_key, credential_tag, " +
			"staking_key, added_slot, deleted_slot, amount, payment_script) " +
			"VALUES (?, 0, ?, ?, ?, ?, ?, ?, ?)",
	)
	require.NoError(tb, err)
	var live [][]byte
	for i := range uint64(n) {
		kind := churnKindFor(mix, i)
		txID := churnHash('t', i, 0, 32)
		var stakingKey any
		if kind != churnEnterprise {
			stakingKey = churnHash('s', i%20_000, 0, 28)
		}
		var deleted int64
		if i%2 == 0 {
			deleted = int64(1_000 + i)
		} else {
			live = append(live, txID)
		}
		_, err := stmt.Exec(
			txID, churnHash('p', i, 0, 28), int64(0), stakingKey,
			int64(i), deleted, churnAmount(i), kind == churnScript,
		)
		require.NoError(tb, err)
	}
	require.NoError(tb, stmt.Close())
	require.NoError(tb, tx.Commit())
	return live
}

func churnOutputAddress(
	tb testing.TB,
	mix string,
	n uint64,
) lcommon.Address {
	tb.Helper()
	payment := churnHash('p', n, 1, 28)
	staking := churnHash('s', n%20_000, 0, 28)
	var addrType uint8
	var stakePart []byte
	switch churnKindFor(mix, n) {
	case churnEnterprise:
		addrType = lcommon.AddressTypeKeyNone
	case churnBase:
		addrType = lcommon.AddressTypeKeyKey
		stakePart = staking
	case churnScript:
		addrType = lcommon.AddressTypeScriptKey
		stakePart = staking
	}
	addr, err := lcommon.NewAddressFromParts(
		addrType, lcommon.AddressNetworkTestnet, payment, stakePart,
	)
	require.NoError(tb, err)
	return addr
}

// newUtxoWALStore opens a file-backed WAL store that never checkpoints, so
// every committed frame stays in the -wal file where the test can read it.
func newUtxoWALStore(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := OpenDB(
		"sqlite",
		"file:"+path+"?_pragma=journal_mode(WAL)&_pragma=synchronous(OFF)"+
			"&_pragma=wal_autocheckpoint(0)",
		"sqlite",
		false,
	)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store, path
}

// walFramesByBtree counts the WAL frames written after byte offset from,
// attributed to the b-tree that owns each frame's page in the current
// database. A page allocated or freed during the window is attributed to
// whatever owns it afterwards, which is accurate enough for a comparison
// against a threshold set well clear of the measured values.
func walFramesByBtree(
	t *testing.T,
	store *Store,
	path string,
	from int64,
) map[string]int {
	t.Helper()
	var pageSize int
	require.NoError(t, store.writeDB.QueryRow("PRAGMA page_size").Scan(&pageSize))
	frameSize := int64(pageSize + 24)
	raw, err := os.ReadFile(path + "-wal")
	require.NoError(t, err)
	owner := map[uint32]string{}
	rows, err := store.writeDB.Query("SELECT pageno, name FROM dbstat WHERE aggregate = 0")
	require.NoError(t, err)
	for rows.Next() {
		var page uint32
		var name string
		require.NoError(t, rows.Scan(&page, &name))
		owner[page] = name
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	counts := map[string]int{}
	const walHeaderSize = 32
	// The file begins with a 32-byte header; from is a size measured before
	// any frames of the window were appended, so it is zero or a frame
	// boundary plus that header.
	for off := max(from, walHeaderSize); off+frameSize <= int64(len(raw)); off += frameSize {
		page := binary.BigEndian.Uint32(raw[off : off+4])
		name := owner[page]
		if name == "" {
			name = "(unattributed)"
		}
		counts[name]++
	}
	return counts
}

func walSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path + "-wal")
	require.NoError(t, err)
	return info.Size()
}

// TestSpentStakingIndexDirtiesFewPagesPerBlock applies blocks of transactions
// through SetTransaction on a populated file-backed WAL database and counts
// the WAL frames written to idx_utxo_deleted_staking_amount. An index that
// holds every live output ordered by credential and amount dirties an
// unrelated leaf for each created and each spent output; one that holds spent
// outputs only is appended to at its right edge by a spend and untouched by a
// creation.
func TestSpentStakingIndexDirtiesFewPagesPerBlock(t *testing.T) {
	t.Parallel()
	const (
		seedRows      = 60_000
		txsPerBlock   = 20
		blocks        = 10
		outputsPerTx  = 2
		inputsPerTx   = 2
		measuredIndex = "idx_utxo_deleted_staking_amount"
	)
	cases := []struct {
		mix string
		// maxFramesPerBlock bounds the index's frames per block.
		maxFramesPerBlock float64
	}{
		{mix: "byron", maxFramesPerBlock: 8},
		{mix: "shelley", maxFramesPerBlock: 8},
	}
	for _, tc := range cases {
		t.Run(tc.mix, func(t *testing.T) {
			t.Parallel()
			store, path := newUtxoWALStore(t)
			live := seedChurnUtxos(t, store, tc.mix, seedRows)
			_, err := store.writeDB.Exec("ANALYZE")
			require.NoError(t, err)
			_, err = store.writeDB.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
			require.NoError(t, err)

			next := 0
			start := walSize(t, path)
			for block := range uint64(blocks) {
				txn := store.Transaction(t.Context())
				for i := range uint64(txsPerBlock) {
					seq := uint64(seedRows) + block*txsPerBlock + i
					builder := mockledger.NewTransactionBuilder()
					for range inputsPerTx {
						// A stride coprime to len(live) visits every live
						// row once before repeating.
						ref := live[(next*7919)%len(live)]
						next++
						input, err := mockledger.NewTransactionInputBuilder().
							WithTxId(ref).WithIndex(0).Build()
						require.NoError(t, err)
						builder.WithInputs(input)
					}
					for o := range uint64(outputsPerTx) {
						output, err := mockledger.NewTransactionOutputBuilder().
							WithAddress(churnOutputAddress(t, tc.mix, seq*8+o).String()).
							WithLovelace(2_000_000 + seq*8 + o).
							Build()
						require.NoError(t, err)
						builder.WithOutputs(output)
					}
					txID := churnHash('x', seq, 0, 32)
					builder.WithId(txID)
					builder.WithValid(true)
					require.NoError(t, store.SetTransaction(
						builder,
						ocommon.Point{Slot: 10_000_000 + block, Hash: txID},
						uint32(i), nil, false, txn, 0,
					))
				}
				require.NoError(t, txn.Commit())
			}
			counts := walFramesByBtree(t, store, path, start)
			perBlock := float64(counts[measuredIndex]) / blocks
			t.Logf(
				"%s: %.1f frames per block on %s (utxo=%d); all b-trees: %v",
				tc.mix, perBlock, measuredIndex, counts["utxo"], counts,
			)
			require.LessOrEqual(t, perBlock, tc.maxFramesPerBlock,
				"%s: %s dirtied %.1f pages per %d-transaction block",
				tc.mix, measuredIndex, perBlock, txsPerBlock)
		})
	}
}
