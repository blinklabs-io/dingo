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
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// Driving importSnapShots itself, rather than the seeding function it calls.
//
// The distinction has already cost twice. The seeding's derivation was covered
// by unit tests and checked against the reference, and the seeding function was
// covered against a real snapshot -- and the glue between importSnapShots and
// the store still carried a defect that would have failed the first Mithril
// bootstrap outright (it passed the outer *database.Txn where the metadata
// store wants txn.Metadata()). Reading that path did not find it; running it
// did, immediately.
//
// So this runs the sequence the import actually performs: cert state first,
// which is what puts the pool registrations in the database, then the stake
// snapshots, whose seeding reads them back. Anything wired wrong between the
// two -- ordering, transaction plumbing, a parameter source that turns out to
// be empty -- shows up here rather than on an operator's first bootstrap.
func TestImportSnapShotsSeedsRewardInputs(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err, "parsing the fixture snapshot")
	require.NotNil(t, state.Tip)

	cfg := ImportConfig{
		Database: db,
		State:    state,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 500, nil
		},
	}
	noProgress := func(ImportProgress) {}
	ctx := context.Background()
	slot := state.Tip.Slot

	// Cert state first, exactly as the import sequences it: this is what
	// populates the pool registrations the seeding takes its parameters from.
	// If this ever stops running before the snapshots, the seeding silently
	// finds no parameters and writes nothing.
	poolsImported, err := importCertState(ctx, cfg, slot, noProgress)
	require.NoError(t, err)
	require.Positive(t, poolsImported,
		"the fixture must import pools, or the seeding below would be "+
			"vacuous for the same reason the pool-distr defect was")

	require.NoError(t, importSnapShots(ctx, cfg, slot, noProgress, false))

	// The three epochs mark, set and go cover are the ones whose reward
	// rounds a bootstrapped node cannot otherwise compute.
	for _, epoch := range []uint64{
		state.Epoch, state.Epoch - 1, state.Epoch - 2,
	} {
		snapshot, err := db.Metadata().GetRewardSnapshot(epoch, "mark", nil)
		require.NoError(t, err)
		require.NotNil(t, snapshot,
			"epoch %d has no reward snapshot after a full import, so its "+
				"reward round would be skipped and never made up", epoch)

		poolInputs, err := db.Metadata().GetRewardPoolInputs(epoch, nil)
		require.NoError(t, err)
		require.Len(t, poolInputs, int(snapshot.TotalPoolCount))
		for _, pool := range poolInputs {
			require.NotEmpty(t, pool.RewardAccount,
				"pool inputs seeded through the import path must carry a "+
					"reward account; empty is what the snapshot's own "+
					"pool-distr entries produce, and the ledger rejects it")
		}

		stakeInputs, err := db.Metadata().GetRewardStakeInputs(epoch, nil)
		require.NoError(t, err)
		require.NotEmpty(t, stakeInputs)
	}

	// The pots row is the other half of what a first reward round needs, and
	// it is seeded by a different part of the import; assert the two agree on
	// the epoch, since a mismatch leaves the round just as skipped.
	pots, err := db.Metadata().GetRewardAdaPots(state.Epoch, nil)
	require.NoError(t, err)
	if pots != nil {
		require.Equal(t, state.Epoch, pots.Epoch)
	}
}
