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
	"github.com/stretchr/testify/require"
)

// TestImportLedgerStateSweepsPostAnchorNonceAndNetworkRows pins the
// block_nonce, network_state, and network_donation part of
// ImportLedgerState's post-anchor sweep: rows above the anchor are removed,
// while anchor-epoch donations are replaced by the snapshot total.
func TestImportLedgerStateSweepsPostAnchorNonceAndNetworkRows(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	const (
		belowSlot  = uint64(99_500)
		anchorSlot = uint64(100_000)
		aboveSlot  = uint64(101_000)
	)
	tipHash := make([]byte, 32)
	otherHash := bytes.Repeat([]byte{0x5a}, 32)
	nonce := make([]byte, 32)
	meta := db.Metadata()

	require.NoError(
		t,
		meta.SetBlockNonce(otherHash, belowSlot, nonce, false, nil),
	)
	require.NoError(
		t,
		meta.SetBlockNonce(otherHash, anchorSlot, nonce, false, nil),
	)
	require.NoError(
		t,
		meta.SetBlockNonce(otherHash, aboveSlot, nonce, true, nil),
	)
	require.NoError(t, meta.SetNetworkState(1, 2, belowSlot, nil))
	require.NoError(t, meta.SetNetworkState(3, 4, aboveSlot, nil))
	require.NoError(t, meta.AddNetworkDonation(belowSlot, 99, 7, nil))
	require.NoError(t, meta.AddNetworkDonation(anchorSlot, 100, 8, nil))
	require.NoError(t, meta.AddNetworkDonation(aboveSlot, 101, 9, nil))

	require.NoError(t, ImportLedgerState(
		context.Background(),
		ImportConfig{
			Database: db,
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
			State: &RawLedgerState{
				Epoch:               anchorSlot / 1_000,
				EraIndex:            EraConway,
				EraBounds:           make([]EraBound, EraConway+1),
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Tip: &SnapshotTip{
					Slot:      anchorSlot,
					BlockHash: tipHash,
				},
			},
			EpochLength: func(uint) (uint, uint, error) {
				return 1, importTestEpochLength, nil
			},
		},
	))

	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	count := func(query string, args ...any) int {
		t.Helper()
		var n int
		require.NoError(t, raw.QueryRow(query, args...).Scan(&n))
		return n
	}

	require.Zero(t, count(
		"SELECT COUNT(*) FROM block_nonce WHERE slot > ?", anchorSlot,
	), "block nonces above the anchor must be swept")
	require.Zero(t, count(
		"SELECT COUNT(*) FROM block_nonce WHERE slot = ? AND hash = ?",
		anchorSlot, otherHash,
	), "a block nonce at the anchor slot on another hash must be swept")
	require.Equal(t, 1, count(
		"SELECT COUNT(*) FROM block_nonce WHERE slot = ?", belowSlot,
	), "block nonces below the anchor must be kept")

	require.Zero(t, count(
		"SELECT COUNT(*) FROM network_state WHERE slot > ?", anchorSlot,
	), "network state above the anchor must be swept")
	require.Equal(t, 1, count(
		"SELECT COUNT(*) FROM network_state WHERE slot = ?", belowSlot,
	), "network state below the anchor must be kept")

	require.Zero(t, count(
		"SELECT COUNT(*) FROM network_donation WHERE slot > ?", anchorSlot,
	), "network donations above the anchor must be swept")
	require.Equal(t, 1, count(
		"SELECT COUNT(*) FROM network_donation WHERE slot <= ?", anchorSlot,
	), "prior-epoch donations remain; the anchor epoch is replaced by the snapshot")
	require.Zero(t, count(
		"SELECT COUNT(*) FROM network_donation WHERE slot = ?", anchorSlot,
	), "zero snapshot donations replace the anchor epoch's local row")
}

func TestImportLedgerStatePostAnchorSweepIsAtomic(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	const (
		anchorSlot = uint64(100_000)
		aboveSlot  = uint64(101_000)
	)
	tipHash := make([]byte, 32)
	nonce := make([]byte, 32)
	require.NoError(t, db.SetEpoch(
		aboveSlot, 101, nonce, nonce, nonce, nonce, EraConway, 1, 1_000, nil,
	))
	require.NoError(t, db.Metadata().SetNetworkState(3, 4, aboveSlot, nil))
	require.NoError(t, db.Metadata().SetBlockNonce(
		bytes.Repeat([]byte{0x5a}, 32), aboveSlot, nonce, true, nil,
	))

	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(`DROP TABLE network_donation`)
	require.NoError(t, err)

	err = ImportLedgerState(context.Background(), ImportConfig{
		Database: db,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		State: &RawLedgerState{
			Epoch:               anchorSlot / 1_000,
			EraIndex:            EraConway,
			EraBounds:           make([]EraBound, EraConway+1),
			EpochNonce:          nonce,
			EvolvingNonce:       nonce,
			CandidateNonce:      nonce,
			LastEpochBlockNonce: nonce,
			Tip: &SnapshotTip{
				Slot:      anchorSlot,
				BlockHash: tipHash,
			},
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, importTestEpochLength, nil
		},
	})
	require.ErrorContains(t, err, "deleting post-anchor network donations")

	var count int
	require.NoError(t, raw.QueryRow(
		"SELECT COUNT(*) FROM epoch WHERE start_slot > ?", anchorSlot,
	).Scan(&count))
	require.Equal(t, 1, count, "epoch sweep must roll back with the failed delete")
	require.NoError(t, raw.QueryRow(
		"SELECT COUNT(*) FROM network_state WHERE slot > ?", anchorSlot,
	).Scan(&count))
	require.Equal(t, 1, count, "network-state sweep must roll back with the failed delete")
	require.NoError(t, raw.QueryRow(
		"SELECT COUNT(*) FROM block_nonce WHERE slot > ?", anchorSlot,
	).Scan(&count))
	require.Equal(t, 1, count, "nonce sweep must roll back with the failed delete")
}
