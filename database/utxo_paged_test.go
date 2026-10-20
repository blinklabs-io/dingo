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

package database

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// pagedTestTxID makes a distinct transaction id per (batch, index).
func pagedTestTxID(batch, index uint32) []byte {
	txID := make([]byte, 32)
	binary.BigEndian.PutUint32(txID[:4], batch)
	binary.BigEndian.PutUint32(txID[4:8], index)
	return txID
}

func importPagedTestUtxos(t *testing.T, db *Database, utxos []models.Utxo) {
	t.Helper()
	require.NoError(t, db.MetadataTxn(context.Background(), true).Do(
		func(txn *Txn) error {
			return db.Metadata().ImportUtxos(utxos, txn.Metadata())
		},
	))
}

func refOf(txID []byte, idx uint32) UtxoRef {
	var ref UtxoRef
	copy(ref.TxId[:], txID)
	ref.OutputIdx = idx
	return ref
}

// TestIterateUtxoRefsAsOfPagedMatchesAsOfSetUnderConcurrentCommits pins the
// consistency argument IterateUtxoRefsAsOfPaged rests on: blocks applied
// after the read slot, which add rows and spend rows between pages, leave the
// as-of set the pages report unchanged, with no row repeated or skipped.
func TestIterateUtxoRefsAsOfPagedMatchesAsOfSetUnderConcurrentCommits(
	t *testing.T,
) {
	t.Parallel()
	const (
		atSlot   = 1_000
		seedRows = 2_000
		pageSize = 37
	)
	db := newTestDB(t)

	seed := make([]models.Utxo, 0, seedRows)
	want := make(map[UtxoRef]struct{}, seedRows)
	for i := range uint32(seedRows) {
		u := models.Utxo{
			TxId:      pagedTestTxID(0, i),
			AddedSlot: uint64(i % atSlot),
			Amount:    1,
		}
		// Every third row was spent at or before the read slot and is not in
		// the as-of set; every fifth of the rest is spent after it below.
		if i%3 == 0 {
			u.DeletedSlot = atSlot - uint64(i%atSlot)
		} else {
			want[refOf(u.TxId, 0)] = struct{}{}
		}
		seed = append(seed, u)
	}
	importPagedTestUtxos(t, db, seed)

	got := make(map[UtxoRef]struct{}, len(want))
	var (
		seen      int
		nextBatch uint32 = 1
	)
	err := db.IterateUtxoRefsAsOfPaged(
		t.Context(),
		atSlot,
		pageSize,
		func(ref UtxoRef) error {
			_, dup := got[ref]
			require.False(t, dup, "row %x#%d read twice", ref.TxId[:8], ref.OutputIdx)
			got[ref] = struct{}{}
			seen++
			if seen%pageSize != 0 {
				return nil
			}
			// Between pages, apply what later blocks do: add rows after the
			// read slot and spend live rows after it, both ahead of and
			// behind the page cursor.
			var added []models.Utxo
			for i := range uint32(25) {
				added = append(added, models.Utxo{
					TxId:      pagedTestTxID(nextBatch, i),
					AddedSlot: atSlot + uint64(nextBatch),
					Amount:    1,
				})
			}
			nextBatch++
			importPagedTestUtxos(t, db, added)
			var spend []types.UtxoKey
			for i := uint32(1); i < seedRows; i += 5 {
				if i%3 != 0 {
					spend = append(spend, types.UtxoKey{
						TxId: pagedTestTxID(0, i), OutputIdx: 0,
					})
				}
			}
			return db.MarkUtxosDeletedAtSlot(
				context.Background(),
				nil,
				spend,
				atSlot+uint64(nextBatch),
			)
		},
	)
	require.NoError(t, err)
	require.Greater(t, nextBatch, uint32(10), "commits did not interleave with pages")
	require.Equal(t, want, got)
}

// TestIterateUtxoRefsAsOfPagedReportsRollbackBelowSlot covers the one change
// that does alter an as-of set: a rollback below the read slot.
func TestIterateUtxoRefsAsOfPagedReportsRollbackBelowSlot(t *testing.T) {
	t.Parallel()
	const atSlot = 500
	for _, tc := range []struct {
		name         string
		rollbackSlot uint64
		wantErr      bool
	}{
		{name: "below the read slot", rollbackSlot: atSlot - 100, wantErr: true},
		{name: "at the read slot", rollbackSlot: atSlot, wantErr: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			var seed []models.Utxo
			for i := range uint32(200) {
				seed = append(seed, models.Utxo{
					TxId:      pagedTestTxID(0, i),
					AddedSlot: uint64(i) * 2,
					Amount:    1,
				})
			}
			importPagedTestUtxos(t, db, seed)

			rolledBack := false
			err := db.IterateUtxoRefsAsOfPaged(
				t.Context(),
				atSlot,
				50,
				func(UtxoRef) error {
					if rolledBack {
						return nil
					}
					rolledBack = true
					return db.UtxosDeleteRolledback(
						context.Background(), tc.rollbackSlot, nil,
					)
				},
			)
			if tc.wantErr {
				require.ErrorIs(t, err, ErrUtxoHistoryRolledBack)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestIterateUtxoRefsAsOfPagedBoundsWALUnderBackToBackReads is the reason the
// paged read exists. SQLite cannot restart the WAL while any reader holds a
// WAL read mark, so whole-UTxO walks run back to back, each in one read
// transaction, grow it without bound under steady commits whatever the
// checkpoint strategy; one page per short transaction leaves room between
// pages for the WAL to restart.
func TestIterateUtxoRefsAsOfPagedBoundsWALUnderBackToBackReads(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{DataDir: dataDir})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	const atSlot = 10_000
	var seed []models.Utxo
	for i := range uint32(20_000) {
		seed = append(seed, models.Utxo{
			TxId:      pagedTestTxID(0, i),
			AddedSlot: uint64(i % atSlot),
			Amount:    1,
		})
	}
	importPagedTestUtxos(t, db, seed)

	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	var readers sync.WaitGroup
	readers.Go(func() {
		// Whole-UTxO walks with no gap between them, each taking a few
		// hundred milliseconds, while block application commits beneath.
		for ctx.Err() == nil {
			var n int
			_ = db.IterateUtxoRefsAsOfPaged(
				ctx,
				atSlot,
				500,
				func(UtxoRef) error {
					n++
					if n%500 == 0 {
						time.Sleep(5 * time.Millisecond)
					}
					return ctx.Err()
				},
			)
		}
	})

	walPath := filepath.Join(dataDir, "metadata.sqlite-wal")
	payload := strings.Repeat("x", 64<<10)
	const totalBytes = 160 << 20
	var maxWAL int64
	for i := range totalBytes / len(payload) {
		require.NoError(t, db.MetadataTxn(context.Background(), true).Do(
			func(txn *Txn) error {
				return db.Metadata().SetSyncState(
					fmt.Sprintf("paged-wal-%d", i), payload, txn.Metadata(),
				)
			},
		))
		if info, err := os.Stat(walPath); err == nil {
			maxWAL = max(maxWAL, info.Size())
		}
	}
	stop()
	readers.Wait()
	t.Logf("largest WAL file: %d bytes of %d written", maxWAL, totalBytes)
	require.Less(
		t,
		maxWAL,
		int64(totalBytes/2),
		"the WAL never restarted under back-to-back whole-UTxO walks",
	)
}
