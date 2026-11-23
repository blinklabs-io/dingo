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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/stretchr/testify/require"
)

// TestImportRejectingTheSnapshotLeavesTheDatabaseUnmarked requires an import
// into the live UTxO set that fails validation not to pin the database to
// the set-only mode.
func TestImportRejectingTheSnapshotLeavesTheDatabaseUnmarked(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	nonce := make([]byte, 32)

	err = importWholeState(t, t.Context(), ImportConfig{
		Database: db,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		State: &RawLedgerState{
			Epoch:               1,
			EraIndex:            EraConway,
			EraBounds:           make([]EraBound, EraConway+1),
			EpochNonce:          nonce,
			EvolvingNonce:       nonce,
			CandidateNonce:      nonce,
			LastEpochBlockNonce: nonce,
			// A tip hash of the wrong width fails validation.
			Tip: &SnapshotTip{Slot: 1_000, BlockHash: []byte{1}},
		},
		UtxoSet: set,
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 1_000, nil
		},
	})
	require.ErrorContains(t, err, "validating ledger state")
	marked, err := db.GetSyncState(database.UtxoSetOnlySyncKey, nil)
	require.NoError(t, err)
	require.Empty(t, marked)
}
