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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// The seeding covers three epochs at once, and pool parameters are not
// constant across them: a pool that changes its margin, cost or pledge is a
// different pool for reward purposes in the epoch before the change than in
// the epoch after. Resolving one parameter set and reusing it for all three
// seeds two of them with parameters that were not in force, which shifts how
// each pool's reward splits between operator and delegators.
//
// So the seeding asks per epoch rather than being handed a map. This pins
// that it asks for every epoch it seeds, and that what comes back is what
// gets written for that epoch specifically -- vary the cost by epoch and each
// epoch's rows must carry its own.
//
// This is the fallback path. A snapshot that carries its own parameters is
// answered from those instead, which is both per-epoch and able to describe
// retired pools; the lookup here covers snapshots in the compact shape, which
// carry only a VRF key.
func TestSeedImportedRewardInputsResolvesParamsPerEpoch(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err, "parsing the fixture snapshot")
	snapshots, err := ParseSnapShots(state.SnapShotsData)
	require.NoError(t, err, "stake snapshots must parse completely")
	// The snapshot's own parameters take precedence wherever it has them, so
	// reduce them to the compact shape: the registration fallback this test
	// is about only drives for a snapshot that cannot describe its pools.
	stripPoolParamsToVrfOnly(snapshots)
	certState, err := ParseCertState(state.CertStateData)
	require.NoError(t, err)
	require.NotEmpty(t, certState.Pools)

	base := make(map[string]*ParsedPool, len(certState.Pools))
	for i := range certState.Pools {
		pool := certState.Pools[i]
		base[hexPoolKey(pool.PoolKeyHash)] = &pool
	}

	// costForEpoch is an arbitrary but epoch-distinct marker: it rides
	// through the derivation into the persisted row, so reading it back
	// identifies which epoch's parameters were actually used.
	costForEpoch := func(epoch uint64) uint64 { return 1_000_000 + epoch }

	var asked []uint64
	resolve := func(epoch uint64) (map[string]*ParsedPool, error) {
		asked = append(asked, epoch)
		out := make(map[string]*ParsedPool, len(base))
		for key, pool := range base {
			clone := *pool
			clone.Cost = costForEpoch(epoch)
			out[key] = &clone
		}
		return out, nil
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	txn := db.MetadataTxn(true)
	require.NoError(t, seedImportedRewardInputs(
		db.Metadata(),
		txn.Metadata(),
		snapshots,
		resolve,
		nil,
		state.Epoch,
		state.Tip.Slot,
		logger,
	))
	require.NoError(t, txn.Commit())

	require.Equal(t,
		[]uint64{state.Epoch, state.Epoch - 1, state.Epoch - 2},
		asked,
		"the seeding must resolve parameters once for each epoch it seeds",
	)

	for _, epoch := range []uint64{
		state.Epoch, state.Epoch - 1, state.Epoch - 2,
	} {
		poolInputs, err := db.Metadata().GetRewardPoolInputs(epoch, nil)
		require.NoError(t, err)
		require.NotEmpty(t, poolInputs,
			"epoch %d seeded no pool inputs", epoch)
		for _, pool := range poolInputs {
			require.Equal(t, costForEpoch(epoch), uint64(pool.Cost),
				"epoch %d was seeded with another epoch's parameters",
				epoch)
		}
		failure, err := db.Metadata().GetRewardSeedFailure(epoch, "mark", nil)
		require.NoError(t, err)
		require.Empty(t, failure,
			"a successfully seeded imported basis must not retain a failure marker")
	}
}

// A parameter lookup that fails is not the same as a pool having no
// parameters. The latter is a basis that cannot be built and is dropped with
// a warning; the former means the database could not answer, and seeding the
// remaining epochs from an answer that never came would write a basis with
// no relation to what was asked for.
func TestSeedImportedRewardInputsPropagatesParamsError(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)
	snapshots, err := ParseSnapShots(state.SnapShotsData)
	require.NoError(t, err)

	wantErr := errors.New("metadata store unavailable")
	txn := db.MetadataTxn(true)
	defer txn.Release()
	err = seedImportedRewardInputs(
		db.Metadata(),
		txn.Metadata(),
		snapshots,
		func(uint64) (map[string]*ParsedPool, error) { return nil, wantErr },
		nil,
		state.Epoch,
		state.Tip.Slot,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	require.ErrorIs(t, err, wantErr)
}

// Registration history loses to the snapshot, and it should.
//
// The snapshot records what was in force during the epoch it captured. A
// registration lookup reconstructs that from certificates, and it cannot
// reconstruct a pool that has since retired at all -- which is what left
// whole epochs unseedable before. So where the two disagree the snapshot
// wins, and this pins that end to end: give the database registrations whose
// cost differs from the snapshot's, run the import, and the seeded rows must
// carry the snapshot's.
func TestImportSnapShotsPrefersSnapshotPoolParamsOverRegistrations(
	t *testing.T,
) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)
	require.NotNil(t, state.Tip)
	snapshots, err := ParseSnapShots(state.SnapShotsData)
	require.NoError(t, err)
	certState, err := ParseCertState(state.CertStateData)
	require.NoError(t, err)
	require.NotEmpty(t, certState.Pools)

	cfg := ImportConfig{
		Database: db,
		State:    state,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 500, nil
		},
	}
	ctx := context.Background()
	noProgress := func(ImportProgress) {}
	slot := state.Tip.Slot

	_, err = importCertState(ctx, cfg, slot, noProgress)
	require.NoError(t, err)

	// The pool has to be one the snapshots actually delegate to, or the
	// seeding never asks about it and the test passes vacuously.
	delegated := make(map[string]struct{}, len(snapshots.Mark.Delegations))
	for _, poolKey := range snapshots.Mark.Delegations {
		delegated[hexPoolKey(poolKey)] = struct{}{}
	}
	var target *ParsedPool
	for i := range certState.Pools {
		if _, ok := delegated[hexPoolKey(certState.Pools[i].PoolKeyHash)]; ok {
			target = &certState.Pools[i]
			break
		}
	}
	require.NotNil(t, target,
		"no cert-state pool is delegated to in the mark snapshot")
	targetKey := hexPoolKey(target.PoolKeyHash)

	// Registrations are placed before the oldest seeded epoch so the
	// effective-for-epoch lookup would select them if it were consulted.
	goStart, ok := importedEpochStartSlot(cfg, state.Epoch-2)
	require.True(t, ok)
	require.Positive(t, goStart,
		"the fixture leaves no room before the go epoch to place a "+
			"registration, so this test cannot distinguish the sources")

	const registrationCost = 111_000_000
	txn := db.MetadataTxn(true)
	require.NoError(t, db.Metadata().ImportPool(
		importTestPoolModel(target),
		importTestPoolRegistration(target, goStart-1, registrationCost),
		txn.Metadata(),
	))
	require.NoError(t, txn.Commit())

	snapshotPool, ok := snapshots.Mark.PoolParams[targetKey]
	if !ok || snapshotPool == nil {
		t.Fatalf("mark snapshot has no parameters for pool %s", targetKey)
	}
	wantCost := snapshotPool.Cost
	require.NotEqual(t, uint64(registrationCost), wantCost,
		"the two sources must disagree, or this test cannot tell which one "+
			"was used")

	require.NoError(t, importSnapShots(ctx, cfg, slot, noProgress, false))

	for _, epoch := range []uint64{
		state.Epoch, state.Epoch - 1, state.Epoch - 2,
	} {
		poolInputs, err := db.Metadata().GetRewardPoolInputs(epoch, nil)
		require.NoError(t, err)
		var found bool
		for _, pool := range poolInputs {
			if hexPoolKey(pool.PoolKeyHash) != targetKey {
				continue
			}
			found = true
			require.Equal(t, wantCost, uint64(pool.Cost),
				"epoch %d was seeded from the registration rather than "+
					"from the snapshot that recorded the epoch", epoch)
		}
		require.True(t, found,
			"epoch %d seeded no input for the target pool", epoch)
	}
}

func importTestPoolModel(pool *ParsedPool) *models.Pool {
	return &models.Pool{
		PoolKeyHash:                slices.Clone(pool.PoolKeyHash),
		VrfKeyHash:                 slices.Clone(pool.VrfKeyHash),
		RewardAccount:              slices.Clone(pool.RewardAccount),
		RewardAccountCredentialTag: pool.RewardAccountCredentialTag,
		Pledge:                     types.Uint64(pool.Pledge),
		Cost:                       types.Uint64(pool.Cost),
	}
}

func importTestPoolRegistration(
	pool *ParsedPool,
	addedSlot uint64,
	cost uint64,
) *models.PoolRegistration {
	owners := make([]models.PoolRegistrationOwner, 0, len(pool.Owners))
	for _, owner := range pool.Owners {
		owners = append(owners, models.PoolRegistrationOwner{
			KeyHash: slices.Clone(owner),
		})
	}
	den := pool.MarginDen
	if den == 0 {
		den = 1
	}
	return &models.PoolRegistration{
		PoolKeyHash:                slices.Clone(pool.PoolKeyHash),
		VrfKeyHash:                 slices.Clone(pool.VrfKeyHash),
		RewardAccount:              slices.Clone(pool.RewardAccount),
		RewardAccountCredentialTag: pool.RewardAccountCredentialTag,
		// #nosec G115 -- margin numerator and denominator are small
		Margin: &types.Rat{Rat: new(big.Rat).SetFrac64(
			int64(pool.MarginNum), int64(den),
		)},
		Pledge:    types.Uint64(pool.Pledge),
		Cost:      types.Uint64(cost),
		Owners:    owners,
		AddedSlot: addedSlot,
	}
}

// An epoch whose registration window cannot be placed is not skipped for that
// reason alone. Registrations are the fallback for pools the snapshot cannot
// describe, so a snapshot that describes every pool it delegates to seeds the
// round without them; dropping it here would lose a round that was fully
// derivable, which is the failure this seeding exists to prevent.
func TestSeedImportedRewardInputsSeedsWithoutAParamsWindow(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)
	snapshots, err := ParseSnapShots(state.SnapShotsData)
	require.NoError(t, err)

	txn := db.MetadataTxn(true)
	require.NoError(t, seedImportedRewardInputs(
		db.Metadata(),
		txn.Metadata(),
		snapshots,
		func(epoch uint64) (map[string]*ParsedPool, error) {
			return nil, fmt.Errorf(
				"%w: epoch %d", errRewardParamsWindowUnknown, epoch,
			)
		},
		nil,
		state.Epoch,
		state.Tip.Slot,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	))
	require.NoError(t, txn.Commit())

	for _, epoch := range []uint64{
		state.Epoch, state.Epoch - 1, state.Epoch - 2,
	} {
		seeded, err := db.Metadata().GetRewardSnapshot(epoch, "mark", nil)
		require.NoError(t, err)
		require.NotNil(t, seeded,
			"epoch %d is fully described by its snapshot, so an unplaceable "+
				"registration window must not cost it its reward round",
			epoch)
	}
}

// The other half: when the snapshot cannot describe its pools either, an
// unplaceable window leaves nothing to derive from and the epoch is skipped
// rather than guessed at. It is skipped by the gate, on the same
// does-not-reconcile grounds as any other underivable basis, and the epochs
// that can be derived are unaffected.
func TestSeedImportedRewardInputsSkipsEpochsWithNoParamsWindow(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)
	snapshots, err := ParseSnapShots(state.SnapShotsData)
	require.NoError(t, err)
	certState, err := ParseCertState(state.CertStateData)
	require.NoError(t, err)
	// Compact snapshots carry no usable parameters, so the registration
	// fallback is the only source and its absence is decisive.
	stripPoolParamsToVrfOnly(snapshots)

	params := make(map[string]*ParsedPool, len(certState.Pools))
	for i := range certState.Pools {
		pool := certState.Pools[i]
		params[hexPoolKey(pool.PoolKeyHash)] = &pool
	}

	unplaceable := state.Epoch - 2
	txn := db.MetadataTxn(true)
	require.NoError(t, seedImportedRewardInputs(
		db.Metadata(),
		txn.Metadata(),
		snapshots,
		func(epoch uint64) (map[string]*ParsedPool, error) {
			if epoch == unplaceable {
				return nil, fmt.Errorf(
					"%w: epoch %d", errRewardParamsWindowUnknown, epoch,
				)
			}
			return params, nil
		},
		nil,
		state.Epoch,
		state.Tip.Slot,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	))
	require.NoError(t, txn.Commit())

	skipped, err := db.Metadata().GetRewardSnapshot(unplaceable, "mark", nil)
	require.NoError(t, err)
	require.Nil(t, skipped,
		"with no snapshot parameters and no registration window there is "+
			"nothing to derive from, so the round must be left uncredited "+
			"rather than seeded from a guess")
	failure, err := db.Metadata().GetRewardSeedFailure(unplaceable, "mark", nil)
	require.NoError(t, err)
	require.Contains(t, failure, "has no reward account",
		"an underivable imported basis must leave durable provenance for the later reward skip")

	// One underivable epoch must not cost the others their rounds.
	for _, epoch := range []uint64{state.Epoch, state.Epoch - 1} {
		seeded, err := db.Metadata().GetRewardSnapshot(epoch, "mark", nil)
		require.NoError(t, err)
		require.NotNil(t, seeded,
			"epoch %d is derivable and must still be seeded", epoch)
	}
}
