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

package epochengine_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/epochengine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sqlHost is a Host over a plain SQL-backed LedgerDB. It supplies only what the
// steps under test read; the embedded nil Host panics on anything else.
type sqlHost struct {
	epochengine.Host
	ldb  ledgerdb.LedgerDB
	fees uint64
}

func (h sqlHost) LedgerDB() ledgerdb.LedgerDB { return h.ldb }

func (h sqlHost) RewardEpochFees(
	ledgerdb.Txn,
	models.Epoch,
) (uint64, error) {
	return h.fees, nil
}

func newEngine(
	t *testing.T,
	fees uint64,
) (*epochengine.Engine, ledgerdb.LedgerDB) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck
	// The consensus database keeps no metadata store; the IndexerDB over it is
	// the SQL-backed store the engine reads and writes here.
	idx, err := dbtest.NewIndexerDatabase(t, db, dbtest.IndexerOptions{})
	require.NoError(t, err)
	ldb := dbadapter.New(idx)
	eng := epochengine.New(
		epochengine.Config{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		sqlHost{ldb: ldb, fees: fees},
	)
	return eng, ldb
}

func runWrite(
	t *testing.T,
	ldb ledgerdb.LedgerDB,
	fn func(txn ledgerdb.Txn) error,
) {
	t.Helper()
	txn := ldb.Begin(context.Background(), true)
	require.NoError(t, ldb.Run(txn, fn))
}

// TestEngineAppliesDonationsOverLedgerDB runs a boundary step over a LedgerDB
// with no LedgerState: the ended epoch's donations move into the treasury at
// the boundary slot and a later epoch's donations stay put.
func TestEngineAppliesDonationsOverLedgerDB(t *testing.T) {
	t.Parallel()

	eng, ldb := newEngine(t, 0)
	runWrite(t, ldb, func(txn ledgerdb.Txn) error {
		if err := ldb.SetNetworkState(1_000, 5_000, 50, txn); err != nil {
			return err
		}
		if err := ldb.AddNetworkDonation(60, 7, 100, txn); err != nil {
			return err
		}
		if err := ldb.AddNetworkDonation(70, 7, 200, txn); err != nil {
			return err
		}
		return ldb.AddNetworkDonation(600, 8, 999, txn)
	})

	runWrite(t, ldb, func(txn ledgerdb.Txn) error {
		return eng.ApplyEpochDonations(txn, 7, 80)
	})

	state, err := ldb.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(1_300), uint64(state.Treasury))
	assert.Equal(t, uint64(5_000), uint64(state.Reserves))
	assert.Equal(t, uint64(80), state.Slot)
}

// TestEngineSavesRewardAdaPotsOverLedgerDB captures the boundary's pots from
// the LedgerDB's network state and the host-supplied epoch fees.
func TestEngineSavesRewardAdaPotsOverLedgerDB(t *testing.T) {
	t.Parallel()

	eng, ldb := newEngine(t, 321)
	runWrite(t, ldb, func(txn ledgerdb.Txn) error {
		return ldb.SetNetworkState(1_000, 5_000, 50, txn)
	})

	runWrite(t, ldb, func(txn ledgerdb.Txn) error {
		return eng.SaveRewardAdaPotsForEpoch(
			txn, 3, models.Epoch{EpochId: 2, LengthInSlots: 100}, 200,
		)
	})

	pots, err := ldb.GetRewardAdaPots(3, nil)
	require.NoError(t, err)
	require.NotNil(t, pots)
	assert.Equal(t, uint64(1_000), uint64(pots.Treasury))
	assert.Equal(t, uint64(5_000), uint64(pots.Reserves))
	assert.Equal(t, uint64(321), uint64(pots.Fees))
	assert.Equal(t, uint64(200), pots.CapturedSlot)
}

// TestEngineMIRWithoutCertificatesIsNoOp leaves the network state alone when
// the ended epoch carries no MIR certificates.
func TestEngineMIRWithoutCertificatesIsNoOp(t *testing.T) {
	t.Parallel()

	eng, ldb := newEngine(t, 0)
	runWrite(t, ldb, func(txn ledgerdb.Txn) error {
		return ldb.SetNetworkState(1_000, 5_000, 50, txn)
	})

	runWrite(t, ldb, func(txn ledgerdb.Txn) error {
		return eng.ApplyMIRCerts(context.Background(), txn, 0, 100, 2)
	})

	state, err := ldb.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(1_000), uint64(state.Treasury))
	assert.Equal(t, uint64(50), state.Slot)
}
