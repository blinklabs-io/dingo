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
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/stretchr/testify/require"
)

func TestImportRefusesADatabaseWithoutAMetadataUtxoSet(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	require.NoError(t, db.SetSyncState(database.UtxoSetOnlySyncKey, "1", nil))
	err = ImportLedgerState(t.Context(), ImportConfig{Database: db})
	require.ErrorContains(t, err, "import it with that set")
}

// TestImportIntoTheLiveUtxoSetMatchesTheMetadataStore imports one snapshot
// into a database with a metadata UTxO set and into a live UTxO set, and
// requires the set to hold the first database's outputs, the same
// reward_live_stake rows, no utxo rows in the second database, and the second
// database marked as keeping its UTxO set only in the live set.
func TestImportIntoTheLiveUtxoSetMatchesTheMetadataStore(t *testing.T) {
	t.Parallel()
	stakeHash := bytes.Repeat([]byte{0x22}, 28)
	addr := buildShelleyAddr(0, 1, bytes.Repeat([]byte{0x11}, 28), stakeHash)
	amounts := []uint64{1_000_000, 2_000_000, 5}
	nonce := make([]byte, 32)
	state := func() *RawLedgerState {
		return &RawLedgerState{
			UTxOData:            inlineUTxOMap(t, addr, amounts),
			Epoch:               1,
			EraIndex:            EraConway,
			EraBounds:           make([]EraBound, EraConway+1),
			EpochNonce:          nonce,
			EvolvingNonce:       nonce,
			CandidateNonce:      nonce,
			LastEpochBlockNonce: nonce,
			Tip: &SnapshotTip{
				Slot:      1_000,
				BlockHash: bytes.Repeat([]byte{0x77}, 32),
			},
		}
	}
	run := func(set UtxoSetLoader) *database.Database {
		db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
		require.NoError(t, err)
		t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
		require.NoError(t, ImportLedgerState(t.Context(), ImportConfig{
			Database:  db,
			Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
			State:     state(),
			ImportKey: "digest:1000",
			UtxoSet:   set,
			EpochLength: func(uint) (uint, uint, error) {
				return 1, 1_000, nil
			},
		}))
		return db
	}
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	rowsDB, onlyDB := run(nil), run(set)

	stakeRows := func(db *database.Database) []string {
		raw, err := dbtest.RawSQLiteMetadata(t, db)
		require.NoError(t, err)
		rows, err := raw.Query(`SELECT credential_tag, staking_key, utxo_stake,
reward_stake, total_stake FROM reward_live_stake ORDER BY 1, 2`)
		require.NoError(t, err)
		defer rows.Close()
		var out []string
		for rows.Next() {
			var tag int
			var key []byte
			var u, r, tot string
			require.NoError(t, rows.Scan(&tag, &key, &u, &r, &tot))
			out = append(out, fmt.Sprintf("%d/%x %s %s %s", tag, key, u, r, tot))
		}
		require.NoError(t, rows.Err())
		return out
	}
	want := stakeRows(rowsDB)
	require.Equal(t, []string{
		fmt.Sprintf("0/%x 3000005 0 3000005", stakeHash),
	}, want)
	require.Equal(t, want, stakeRows(onlyDB))

	fromRows := map[string]string{}
	require.NoError(t, rowsDB.IterateLiveUtxos(t.Context(), nil,
		func(u *models.Utxo) error {
			fromRows[fmt.Sprintf("%x#%d", u.TxId, u.OutputIdx)] = fmt.Sprintf("%x", u.Cbor)
			return nil
		}))
	require.Len(t, fromRows, len(amounts))
	fromSet := map[string]string{}
	require.NoError(t, set.EachUtxo(func(txID []byte, index uint32, cbor []byte) error {
		fromSet[fmt.Sprintf("%x#%d", txID, index)] = fmt.Sprintf("%x", cbor)
		return nil
	}))
	require.Equal(t, fromRows, fromSet)
	tip, ok, err := set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1_000), tip.Slot)
	require.Equal(t, bytes.Repeat([]byte{0x77}, 32), tip.Hash)

	n := 0
	require.NoError(t, onlyDB.IterateLiveUtxos(t.Context(), nil,
		func(*models.Utxo) error { n++; return nil }))
	require.Zero(t, n, "utxo rows in the live-set-only database")
	marked, err := onlyDB.GetSyncState(database.UtxoSetOnlySyncKey, nil)
	require.NoError(t, err)
	require.NotEmpty(t, marked)

	// A database with a metadata UTxO set cannot take a set-only import.
	err = ImportLedgerState(t.Context(), ImportConfig{
		Database: rowsDB,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		State:    state(),
		UtxoSet:  set,
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 1_000, nil
		},
	})
	require.ErrorContains(t, err, "without a metadata UTxO set")
}
