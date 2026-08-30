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
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

func newTreasuryViewTestLedger(
	t *testing.T,
	dataDir string,
) (*LedgerState, *database.Database) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: dataDir})
	require.NoError(t, err)
	return &LedgerState{db: db}, db
}

func requireTreasuryValue(
	t *testing.T,
	ls *LedgerState,
	txn *database.Txn,
	want uint64,
) {
	t.Helper()
	got, err := ls.NewView(txn).TreasuryValue()
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestLedgerViewTreasuryValueReadsValidationTransaction(t *testing.T) {
	ls, db := newTreasuryViewTestLedger(t, "")
	require.NoError(t, db.Metadata().SetNetworkState(100, 900, 10, nil))
	requireTreasuryValue(t, ls, nil, 100)

	rollbackErr := errors.New("test rollback")
	txn := db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		require.NoError(t, db.Metadata().SetNetworkState(
			250,
			750,
			20,
			txn.Metadata(),
		))
		requireTreasuryValue(t, ls, txn, 250)
		return rollbackErr
	})
	require.ErrorIs(t, err, rollbackErr)
	requireTreasuryValue(t, ls, nil, 100)
}

func TestLedgerViewTreasuryValueTracksChainRollback(t *testing.T) {
	ls, db := newTreasuryViewTestLedger(t, "")
	require.NoError(t, db.Metadata().SetNetworkState(100, 900, 10, nil))
	require.NoError(t, db.Metadata().SetNetworkState(60, 900, 20, nil))
	requireTreasuryValue(t, ls, nil, 60)

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		if err := db.DeleteNetworkStateAfterSlot(10, txn); err != nil {
			return err
		}
		value, err := ls.NewView(txn).TreasuryValue()
		if err != nil {
			return err
		}
		if value != 100 {
			return errors.New("rollback transaction exposed the wrong treasury")
		}
		return nil
	}))
	requireTreasuryValue(t, ls, nil, 100)
}

func TestLedgerViewTreasuryValueSurvivesRestart(t *testing.T) {
	dataDir := t.TempDir()
	ls, db := newTreasuryViewTestLedger(t, dataDir)
	require.NoError(t, db.Metadata().SetNetworkState(777, 223, 42, nil))
	requireTreasuryValue(t, ls, nil, 777)
	require.NoError(t, dbtest.CloseDatabase(db))

	restarted, reopened := newTreasuryViewTestLedger(t, dataDir)
	requireTreasuryValue(t, restarted, nil, 777)
	require.NoError(t, dbtest.CloseDatabase(reopened))
}

func TestLedgerViewTreasuryValueReadsMithrilBootstrapState(t *testing.T) {
	ls, db := newTreasuryViewTestLedger(t, "")
	paramsData, err := cbor.Encode(mithrilRewardConwayPParams())
	require.NoError(t, err)
	nonce := make([]byte, 32)
	const treasury = uint64(87_920_693_660_807)
	require.NoError(t, ledgerstate.ImportLedgerState(
		context.Background(),
		ledgerstate.ImportConfig{
			Database: db,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
			State: &ledgerstate.RawLedgerState{
				PParamsData:         paramsData,
				PrevPParamsData:     paramsData,
				Epoch:               12,
				EraIndex:            ledgerstate.EraConway,
				EraBounds:           make([]ledgerstate.EraBound, ledgerstate.EraConway+1),
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Treasury:            treasury,
				Reserves:            14_914_270_613_432_674,
				Tip: &ledgerstate.SnapshotTip{
					Slot:      123_456,
					BlockHash: make([]byte, 32),
				},
			},
			EpochLength: func(uint) (uint, uint, error) {
				return 1, 100, nil
			},
		},
	))
	requireTreasuryValue(t, ls, nil, treasury)
}

func TestLedgerViewTreasuryValueFailsClosedWithoutNetworkState(t *testing.T) {
	ls, _ := newTreasuryViewTestLedger(t, "")
	value, err := ls.NewView(nil).TreasuryValue()
	require.ErrorContains(t, err, "network state is unavailable")
	require.Zero(t, value)
}

func TestLedgerViewTreasuryValuePropagatesStorageErrors(t *testing.T) {
	ls, db := newTreasuryViewTestLedger(t, t.TempDir())
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(
		"INSERT INTO network_state (treasury, reserves, slot) VALUES (?, ?, ?)",
		"not-a-number",
		"0",
		1,
	)
	require.NoError(t, err)

	value, err := ls.NewView(nil).TreasuryValue()
	require.ErrorContains(t, err, "get treasury network state")
	require.ErrorContains(t, err, "invalid syntax")
	require.Zero(t, value)
}
