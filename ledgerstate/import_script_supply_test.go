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

// ImportLedgerState replaces the running script-locked total with one row at
// the snapshot tip slot equal to a full scan of the imported outputs, whatever
// the aggregate held before. The deferred SQLite import also credits outputs
// as it inserts them, so a stale row from before the import is what tells a
// real rebuild from that incremental maintenance.
func TestImportLedgerStateSeedsScriptLockedSupply(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(
		"INSERT INTO script_locked_supply (slot, amount) VALUES (5, 777)",
	)
	require.NoError(t, err)

	addr := buildShelleyAddr(
		1, 1,
		bytes.Repeat([]byte{0x11}, 28),
		bytes.Repeat([]byte{0x22}, 28),
	)
	nonce := make([]byte, 32)
	require.NoError(t, ImportLedgerState(
		context.Background(),
		ImportConfig{
			Database: db,
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
			State: &RawLedgerState{
				UTxOData: inlineUTxOMap(
					t,
					addr,
					[]uint64{1_000_000, 2_000_000},
				),
				Epoch:               1,
				EraIndex:            EraConway,
				EraBounds:           make([]EraBound, EraConway+1),
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Tip: &SnapshotTip{
					Slot:      1_000,
					BlockHash: make([]byte, 32),
				},
			},
			EpochLength: func(uint) (uint, uint, error) {
				return 1, 1_000, nil
			},
		},
	))

	got, err := db.Metadata().GetScriptLockedSupply(nil)
	require.NoError(t, err)
	require.EqualValues(t, 3_000_000, got)
	var slot, rows int64
	require.NoError(t, raw.QueryRow(
		"SELECT COALESCE(MIN(slot), -1), COUNT(*) FROM script_locked_supply",
	).Scan(&slot, &rows))
	require.EqualValues(t, 1_000, slot)
	require.EqualValues(t, 1, rows)
}
