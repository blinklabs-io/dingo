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
func TestSeedImportedRewardInputsResolvesParamsPerEpoch(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err, "parsing the fixture snapshot")
	snapshots, err := ParseSnapShots(state.SnapShotsData)
	require.NoError(t, err, "stake snapshots must parse completely")
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
		state.Epoch,
		state.Tip.Slot,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	require.ErrorIs(t, err, wantErr)
}

// The resolver above is only worth wiring if the lookup behind it actually
// distinguishes epochs. On a fresh bootstrap it cannot: every registration
// the import writes lands at the import slot, so all three epochs resolve to
// the same row and reading the pool rows would have given the same answer.
// The difference shows when the import runs against a database that already
// holds registration history, which is a supported path -- a re-import, or a
// resume after a partial one.
//
// So give the database that history and check each epoch picks its own
// registration: one made before the go snapshot's epoch, and a later one made
// while the set snapshot's epoch was running. mark must see the later
// parameters, set and go the earlier ones. Cost is the marker because it
// rides through the derivation into the persisted row unchanged.
func TestImportSnapShotsUsesEpochEffectivePoolParams(t *testing.T) {
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

	// Window edges come from the production helper: this test is about which
	// registration each epoch selects, not about the slot arithmetic, which
	// importedEpochStartSlot defines.
	goStart, goOK := importedEpochStartSlot(cfg, state.Epoch-2)
	setStart, setOK := importedEpochStartSlot(cfg, state.Epoch-1)
	markStart, markOK := importedEpochStartSlot(cfg, state.Epoch)
	require.True(t, goOK && setOK && markOK,
		"the fixture's era bounds must cover all three seeded epochs, or "+
			"the seeding skips them and this test proves nothing")
	require.Positive(t, goStart,
		"the fixture leaves no room before the go epoch to place a "+
			"registration, so this test cannot distinguish the epochs")
	require.Less(t, setStart, markStart)
	require.LessOrEqual(t, markStart, slot)

	const earlierCost = 111_000_000
	const laterCost = 222_000_000
	for _, reg := range []struct {
		addedSlot uint64
		cost      uint64
	}{
		{goStart - 1, earlierCost},
		{setStart, laterCost},
	} {
		txn := db.MetadataTxn(true)
		require.NoError(t, db.Metadata().ImportPool(
			importTestPoolModel(target),
			importTestPoolRegistration(target, reg.addedSlot, reg.cost),
			txn.Metadata(),
		))
		require.NoError(t, txn.Commit())
	}

	require.NoError(t, importSnapShots(ctx, cfg, slot, noProgress, false))

	for _, c := range []struct {
		epoch uint64
		want  uint64
	}{
		{state.Epoch, laterCost},
		{state.Epoch - 1, earlierCost},
		{state.Epoch - 2, earlierCost},
	} {
		poolInputs, err := db.Metadata().GetRewardPoolInputs(c.epoch, nil)
		require.NoError(t, err)
		var found bool
		for _, pool := range poolInputs {
			if hexPoolKey(pool.PoolKeyHash) != targetKey {
				continue
			}
			found = true
			require.Equal(t, c.want, uint64(pool.Cost),
				"epoch %d was seeded from a registration that was not in "+
					"force during it", c.epoch)
		}
		require.True(t, found,
			"epoch %d seeded no input for the target pool", c.epoch)
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

// An epoch whose parameter window cannot be placed is skipped, not guessed
// at and not fatal. Guessing seeds the round against parameters that were not
// in force, which credits rewards at the wrong split rather than visibly not
// crediting them; failing the import would throw away the epochs that *can*
// be placed along with it. Skipping leaves that one round uncredited and
// counted, which is the direction the rest of this seeding already takes.
func TestSeedImportedRewardInputsSkipsEpochsWithNoParamsWindow(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)
	snapshots, err := ParseSnapShots(state.SnapShotsData)
	require.NoError(t, err)
	certState, err := ParseCertState(state.CertStateData)
	require.NoError(t, err)

	params := make(map[string]*ParsedPool, len(certState.Pools))
	for i := range certState.Pools {
		pool := certState.Pools[i]
		params[hexPoolKey(pool.PoolKeyHash)] = &pool
	}

	// The oldest of the three is the one an era bound is most likely to fall
	// short of, so it stands in for the real case here.
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
		state.Epoch,
		state.Tip.Slot,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	))
	require.NoError(t, txn.Commit())

	skipped, err := db.Metadata().GetRewardSnapshot(unplaceable, "mark", nil)
	require.NoError(t, err)
	require.Nil(t, skipped,
		"an epoch with no parameter window must be skipped, not seeded from "+
			"a guessed one")

	// The epochs that can be placed are unaffected: one unplaceable epoch
	// must not cost the others their reward rounds.
	for _, epoch := range []uint64{state.Epoch, state.Epoch - 1} {
		seeded, err := db.Metadata().GetRewardSnapshot(epoch, "mark", nil)
		require.NoError(t, err)
		require.NotNil(t, seeded,
			"epoch %d has a usable window and must still be seeded", epoch)
	}
}
