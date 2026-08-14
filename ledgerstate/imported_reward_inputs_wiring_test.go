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
	"encoding/hex"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// testdataLedgerSnapshot is a real cardano-node ledger-state snapshot, taken
// from the DevNet conformance network at epoch 4. It is a fixture rather than
// a synthetic payload because the defect this test exists to catch was a
// difference between a real snapshot and an assumed one: current UTxO-HD
// snapshots carry pool entries inside SnapShots in the compact pool-distr
// shape, with no margin, cost, pledge, reward account or owners, and a
// derivation that reads pool parameters from there produces a basis the gate
// rejects. Synthetic input constructed from the same assumption would have
// agreed with the bug.
const testdataLedgerSnapshot = "testdata/devnet-ledger-snapshot-epoch4.cbor"

// The seeding is only worth anything if it runs. Its pure derivation is
// covered elsewhere, and separately checked against cardano-node's own stake
// snapshot, but neither exercises the wiring: reading the parsed snapshot,
// resolving pool parameters out of the imported registrations, and writing
// the rows the reward round will later read.
//
// That wiring had never executed before this test. DevNet syncs from genesis
// and never bootstraps from a snapshot, so nothing else reaches this path --
// which is exactly how the pool-distr defect above survived unit tests, a
// gate, and review.
func TestSeedImportedRewardInputsWritesRows(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err, "parsing the fixture snapshot")
	require.NotNil(t, state.SnapShotsData)
	require.NotNil(t, state.CertStateData)

	// Not tolerated: ParseSnapShots returns a non-nil error even when it
	// parses with entries skipped, so accepting that case lets a partial
	// decode through and the test then runs on incomplete data.
	snapshots, err := ParseSnapShots(state.SnapShotsData)
	require.NoError(t, err, "stake snapshots must parse completely")
	// The credential type has to survive parsing, and on the compact UTxO-HD
	// shape -- which is what current snapshots use -- it was being dropped,
	// so every credential defaulted to a key hash. A script credential can
	// share a hash with a key one, so that misdirects both the reward and the
	// share of leadership stake.
	require.Len(t, snapshots.Mark.StakeTags, len(snapshots.Mark.Stake),
		"every credential in the stake map needs its type carried alongside")

	certState, err := ParseCertState(state.CertStateData)
	require.NoError(t, err)
	require.NotEmpty(t, certState.Pools,
		"the fixture must carry pool registrations, or this test would pass "+
			"for the same reason the bug shipped")

	// Mirror the import: parameters from the cert-state registrations, stake
	// and delegations from the snapshots.
	params := make(map[string]*ParsedPool, len(certState.Pools))
	for i := range certState.Pools {
		pool := certState.Pools[i]
		params[hexPoolKey(pool.PoolKeyHash)] = &pool
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	txn := db.MetadataTxn(true)
	require.NoError(t, seedImportedRewardInputs(
		db.Metadata(),
		txn.Metadata(),
		snapshots,
		func(uint64) (map[string]*ParsedPool, error) { return params, nil },
		state.Epoch,
		state.Tip.Slot,
		logger,
	))
	require.NoError(t, txn.Commit())

	// mark, set and go cover the snapshot's epoch and the two before it --
	// the three a bootstrapped node cannot otherwise compute.
	for _, epoch := range []uint64{
		state.Epoch, state.Epoch - 1, state.Epoch - 2,
	} {
		snapshot, err := db.Metadata().GetRewardSnapshot(epoch, "mark", nil)
		require.NoError(t, err)
		require.NotNil(t, snapshot,
			"epoch %d has no reward snapshot, so its reward round would be "+
				"skipped and its rewards never credited", epoch)
		require.Positive(t, snapshot.TotalPoolCount)
		require.Positive(t, uint64(snapshot.TotalActiveStake))

		poolInputs, err := db.Metadata().GetRewardPoolInputs(epoch, nil)
		require.NoError(t, err)
		require.Len(t, poolInputs, int(snapshot.TotalPoolCount),
			"epoch %d pool inputs must match the snapshot's pool count",
			epoch)

		stakeInputs, err := db.Metadata().GetRewardStakeInputs(epoch, nil)
		require.NoError(t, err)
		require.NotEmpty(t, stakeInputs,
			"epoch %d has no per-credential stake inputs", epoch)

		// The rows have to satisfy the same reconciliation the ledger applies
		// when it reads them back; failing it there returns an error rather
		// than skipping, which fails the epoch rollover.
		var totalStake, totalDelegators uint64
		for _, pool := range poolInputs {
			require.NotEmpty(t, pool.RewardAccount,
				"a pool input without a reward account is what the "+
					"pool-distr shape produced, and the ledger rejects it")
			require.NotNil(t, pool.Margin)
			totalStake += uint64(pool.DelegatedStake)
			totalDelegators += pool.DelegatorCount
		}
		require.Equal(t, uint64(snapshot.TotalActiveStake), totalStake)
		require.Equal(t, snapshot.TotalDelegators, totalDelegators)
	}
}

// A basis derived without usable pool parameters must write nothing at all,
// rather than write rows the ledger will later refuse. This is the pool-distr
// case: it is what a snapshot's own PoolParams give on current formats.
func TestSeedImportedRewardInputsWritesNothingWithoutPoolParams(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)
	snapshots, err := ParseSnapShots(state.SnapShotsData)
	require.NoError(t, err, "stake snapshots must parse completely")

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	txn := db.MetadataTxn(true)
	// A nil resolver falls back to the snapshot's own PoolParams, which on this
	// real fixture are the compact pool-distr shape.
	require.NoError(t, seedImportedRewardInputs(
		db.Metadata(), txn.Metadata(), snapshots, nil,
		state.Epoch, state.Tip.Slot, logger,
	))
	require.NoError(t, txn.Commit())

	snapshot, err := db.Metadata().GetRewardSnapshot(state.Epoch, "mark", nil)
	require.NoError(t, err)
	require.Nil(t, snapshot,
		"an unusable basis must be dropped, not written: the ledger reads "+
			"these rows through a path that errors rather than skips, so a "+
			"bad row fails the epoch rollover instead of one reward round")
}

// hexPoolKey is a local helper so the wiring test does not depend on the
// derivation's own encoding choices.
func hexPoolKey(b []byte) string { return hex.EncodeToString(b) }
