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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"os"
	"slices"
	"sort"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// refPool is one pool's entry in `cardano-cli query stake-snapshot`.
type refPool struct {
	StakeMark uint64 `json:"stakeMark"`
	StakeSet  uint64 `json:"stakeSet"`
	StakeGo   uint64 `json:"stakeGo"`
}

// The gate on the derived reward basis proves it reconciles with itself. It
// cannot prove the derivation agrees with the reference implementation: a
// systematically different aggregation would satisfy every internal identity
// and still produce wrong rewards.
//
// This closes that gap by deriving from a real cardano-node ledger-state
// snapshot and comparing the per-pool result against what that same node
// reports through `cardano-cli query stake-snapshot --all-stake-pools`. The
// CLI's stakeMark/stakeSet/stakeGo are the reference's own view of the three
// snapshots this seeding reads, so agreement is agreement with cardano-node
// rather than with my reading of it.
//
// It is opt-in because it needs both artifacts from a running node:
//
//	DINGO_REF_LEDGER_SNAPSHOT  path to a cardano-node ledger snapshot file
//	                           (written on graceful shutdown, under
//	                           <db>/ledger/<slot>)
//	DINGO_REF_STAKE_SNAPSHOT   path to the JSON emitted by
//	                           `cardano-cli query stake-snapshot
//	                            --all-stake-pools --output-json`
//	DINGO_REF_EPOCH            the epoch the node was in when that query ran
//
// Running this against preview rather than DevNet is what would settle whether
// the seeding matches at scale, because DevNet has two pools and preview has several hundred. The
// artifacts can be had without a full bootstrap:
//
//  1. The ledger state lives in the Mithril *ancillary* files, which are not
//     downloaded by default since mithril-client 0.12.1. Fetch them with the
//     v2 backend, bounding the immutable range so the 15 GB preview database
//     is not pulled for a 10 KB file:
//
//     export AGGREGATOR_ENDPOINT=https://aggregator.pre-release-preview.api.mithril.network/aggregator
//     export GENESIS_VERIFICATION_KEY=$(curl -s https://raw.githubusercontent.com/IntersectMBO/mithril/main/mithril-infra/configuration/pre-release-preview/genesis.vkey)
//     export ANCILLARY_VERIFICATION_KEY=$(curl -s https://raw.githubusercontent.com/IntersectMBO/mithril/main/mithril-infra/configuration/pre-release-preview/ancillary.vkey)
//     mithril-client cardano-database download latest \
//     --include-ancillary --start <n> --end <n>
//
//     Note the network is "pre-release-preview", not "release-preview";
//     the latter does not resolve. dingo's own endpoints are in
//     mithril/client.go's defaultNetworkConfigs.
//
//  2. The reference for preview is koios rather than a local cardano-cli.
//     /pool_history?_epoch_no=E gives active_stake and delegator_cnt per pool
//     for epoch E; build the reference JSON from three queries, for the
//     snapshot's own epoch and the two before it, which is what mark, set and
//     go hold. Epoch offset still applies: koios reports the active stake for
//     epoch E, which is the distribution as of the end of E-2.
//
// A full `dingo mithril sync` is the other half of the validation, and a
// different question: it shows whether a bootstrapped node then computes
// rewards that keep its stake in line, which is visible as
// dingo_ledger_skipped_stake_reward_rounds_total staying at zero and the
// leader-threshold margin histogram showing no near-zero clustering. That
// needs the full download and several epochs of running.
//
// The epoch matters because the two artifacts are rarely from the same point:
// a ledger snapshot is written at the node's immutable tip, which lags the
// tip the CLI query sees, so the snapshot is often an epoch behind. Snapshots
// rotate at the boundary, so an offset of one means this snapshot's mark is
// the reference's set and its set is the reference's go. Getting that wrong
// makes a correct derivation look like an off-by-one bug -- it did here on
// the first run -- so the offset is stated rather than assumed.
func TestDerivedRewardInputsMatchReferenceStakeSnapshot(t *testing.T) {
	t.Parallel()

	snapshotPath := os.Getenv("DINGO_REF_LEDGER_SNAPSHOT")
	referencePath := os.Getenv("DINGO_REF_STAKE_SNAPSHOT")
	if snapshotPath == "" || referencePath == "" {
		t.Skip(
			"set DINGO_REF_LEDGER_SNAPSHOT and DINGO_REF_STAKE_SNAPSHOT to " +
				"compare the derived reward basis against cardano-node",
		)
	}

	state, err := ParseSnapshot(snapshotPath)
	require.NoError(t, err, "parsing the reference ledger snapshot")
	require.NotNil(t, state.SnapShotsData,
		"the snapshot carries no stake snapshots")

	// Not tolerated: ParseSnapShots returns a non-nil error even when it
	// parses with entries skipped, so accepting that case lets a partial
	// decode through and the comparison then runs on incomplete data.
	snapshots, err := ParseSnapShots(state.SnapShotsData)
	require.NoError(t, err, "stake snapshots must parse completely")

	// Pool parameters come from cert state, the same place the import takes
	// them: current snapshots carry only the compact pool-distr shape inside
	// SnapShots, with no margin, cost, pledge, reward account or owners.
	require.NotNil(t, state.CertStateData,
		"the snapshot carries no cert state to take pool parameters from")
	certState, err := ParseCertState(state.CertStateData)
	require.NoError(t, err, "parsing cert state")
	params := make(map[string]*ParsedPool, len(certState.Pools))
	for i := range certState.Pools {
		pool := certState.Pools[i]
		params[hex.EncodeToString(pool.PoolKeyHash)] = &pool
	}
	require.NotEmpty(t, params, "cert state carries no pool registrations")

	raw, err := os.ReadFile(referencePath)
	require.NoError(t, err, "reading the reference stake snapshot")
	var reference struct {
		Pools map[string]refPool `json:"pools"`
	}
	require.NoError(t, json.Unmarshal(raw, &reference))
	require.NotEmpty(t, reference.Pools, "reference reports no pools")

	refEpochRaw := os.Getenv("DINGO_REF_EPOCH")
	require.NotEmpty(t, refEpochRaw,
		"set DINGO_REF_EPOCH to the epoch the stake-snapshot query ran in")
	refEpoch, err := strconv.ParseUint(refEpochRaw, 10, 64)
	require.NoError(t, err, "parsing DINGO_REF_EPOCH")
	require.GreaterOrEqual(t, refEpoch, state.Epoch,
		"the reference query cannot predate the ledger snapshot")
	offset := refEpoch - state.Epoch
	require.LessOrEqual(t, offset, uint64(2),
		"a snapshot more than two epochs behind the query shares no "+
			"positions with it")
	t.Logf("ledger snapshot epoch %d, reference epoch %d (offset %d)",
		state.Epoch, refEpoch, offset)

	// Position i of this snapshot is position i+offset of the reference.
	columns := []func(v refPool) uint64{
		func(v refPool) uint64 { return v.StakeMark },
		func(v refPool) uint64 { return v.StakeSet },
		func(v refPool) uint64 { return v.StakeGo },
	}
	pick := func(i int) func(string) (uint64, bool) {
		idx := i + int(offset)
		if idx >= len(columns) {
			return nil // no counterpart in the reference at this offset
		}
		return func(p string) (uint64, bool) {
			v, ok := reference.Pools[p]
			return columns[idx](v), ok
		}
	}

	for i, c := range []struct {
		name string
		snap *ParsedSnapShot
		want func(poolHex string) (uint64, bool)
	}{
		{"mark", &snapshots.Mark, pick(0)},
		{"set", &snapshots.Set, pick(1)},
		{"go", &snapshots.Go, pick(2)},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.want == nil {
				t.Skipf(
					"the reference has no column for the %s snapshot at "+
						"offset %d", c.name, offset,
				)
			}
			bundle := deriveRewardInputs(c.snap, params, state.Epoch, 1, 0)
			require.NotNil(t, bundle)
			require.NoError(t, bundle.validate(),
				"a basis derived from a real snapshot must reconcile")

			derived := make(map[string]uint64, len(bundle.poolInputs))
			for _, pool := range bundle.poolInputs {
				derived[hex.EncodeToString(pool.PoolKeyHash)] =
					uint64(pool.DelegatedStake)
			}

			pools := make([]string, 0, len(derived))
			for pool := range derived {
				pools = append(pools, pool)
			}
			sort.Strings(pools)

			var compared int
			for _, pool := range pools {
				want, ok := c.want(pool)
				if !ok {
					// The reference lists only pools it holds stake for; a
					// pool absent there with zero derived stake is agreement.
					require.Zero(t, derived[pool],
						"pool %s has derived stake but is absent from the "+
							"reference snapshot", pool)
					continue
				}
				compared++
				require.Equal(t, want, derived[pool],
					"pool %s: derived delegated stake disagrees with "+
						"cardano-node's %s snapshot", pool, c.name)
			}
			// The sweep above only visits pools present in the derived
			// set, so a pool the reference holds stake for that the
			// derivation dropped entirely would never be examined -- and a
			// dropped pool is precisely the failure a missing or stale
			// registration produces. Sweep the other direction too.
			// Bounds first, then index: clamping the index and discarding
			// the result afterwards evaluates a column this subtest is not
			// comparing against, which reads as if it were.
			idx := i + int(offset)
			if idx >= len(columns) {
				return
			}
			var referenced int
			for pool, v := range reference.Pools {
				want := columns[idx](v)
				if want == 0 {
					continue
				}
				referenced++
				got, ok := derived[pool]
				require.True(t, ok,
					"pool %s holds %d stake in cardano-node's %s snapshot "+
						"but is absent from the derived basis", pool, want,
					c.name)
				require.Equal(t, want, got,
					"pool %s: derived stake disagrees with cardano-node's "+
						"%s snapshot", pool, c.name)
			}
			t.Logf("%s: %d pools compared derived->reference, %d "+
				"reference->derived, all equal", c.name, compared, referenced)
		})
	}
}

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
	t.Parallel()

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
		require.Empty(
			t,
			failure,
			"a successfully seeded imported basis must not retain a failure marker",
		)
	}
}

// A parameter lookup that fails is not the same as a pool having no
// parameters. The latter is a basis that cannot be built and is dropped with
// a warning; the former means the database could not answer, and seeding the
// remaining epochs from an answer that never came would write a basis with
// no relation to what was asked for.
func TestSeedImportedRewardInputsPropagatesParamsError(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	require.Contains(
		t,
		failure,
		"has no reward account",
		"an underivable imported basis must leave durable provenance for the later reward skip",
	)

	// One underivable epoch must not cost the others their rounds.
	for _, epoch := range []uint64{state.Epoch, state.Epoch - 1} {
		seeded, err := db.Metadata().GetRewardSnapshot(epoch, "mark", nil)
		require.NoError(t, err)
		require.NotNil(t, seeded,
			"epoch %d is derivable and must still be seeded", epoch)
	}
}

func TestEmptyRewardSeedFailureReasonReportsMissingParameters(t *testing.T) {
	t.Parallel()

	pools := ParsedSnapShot{
		Stake: map[string]uint64{"credential": 1},
		Delegations: map[string][]byte{
			"credential": {0x01, 0x02},
		},
	}

	reason := emptyRewardSeedFailureReason(&pools)
	require.Equal(
		t,
		"derived reward basis contains no pool inputs: pool 0102 has no parameters",
		reason,
	)
}

func TestSeedImportedRewardInputsPreservesFailureForEmptyBundle(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	txn := db.MetadataTxn(true)
	require.NoError(t, seedImportedRewardInputs(
		db.Metadata(),
		txn.Metadata(),
		&ParsedSnapShots{
			Mark: ParsedSnapShot{},
			Set:  ParsedSnapShot{},
			Go:   ParsedSnapShot{},
		},
		nil,
		nil,
		2,
		100,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	))
	require.NoError(t, txn.Commit())

	reason, err := db.Metadata().GetRewardSeedFailure(2, "mark", nil)
	require.NoError(t, err)
	require.Equal(t, "derived reward basis contains no pool inputs", reason)
}

// A pool synthesized from the current active distribution can be absent from
// mark and go while still being present, with authoritative parameters, in
// set. Registration lookup is intentionally shared across the three epochs,
// so the synthetic fallback must be scoped back to the pools delegated to in
// each target snapshot before that snapshot's complete parameters overlay it.
//
// The two-pool shape mirrors preview snapshot i27926: the affected pools were
// delegated to only in set, absent from cert state, and therefore synthesized
// with no historical reward account. Before the fix that irrelevant fallback
// made mark and go fail validation while set passed because its own complete
// parameters replaced the synthetic entry.
func TestSeedImportedRewardInputsScopesFallbackToTargetSnapshot(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	poolA := scopedRewardTestPool(0xA1, 0x11)
	poolB := scopedRewardTestPoolFromKey(
		t,
		"102e9ff50bee440b1ef337f58d1760a5475f3ce716f2aab60e6ef424",
		0x22,
	)
	poolC := scopedRewardTestPoolFromKey(
		t,
		"1fc372fdce61f42d31be7ddfc2bf8e343b08a54e4d3e6d64b2e328ff",
		0x23,
	)
	compactA := &ParsedPool{
		PoolKeyHash: poolA.PoolKeyHash,
		VrfKeyHash:  poolA.VrfKeyHash,
	}
	compactB := &ParsedPool{
		PoolKeyHash: poolB.PoolKeyHash,
		VrfKeyHash:  poolB.VrfKeyHash,
	}
	compactC := &ParsedPool{
		PoolKeyHash: poolC.PoolKeyHash,
		VrfKeyHash:  poolC.VrfKeyHash,
	}
	setCredentialB := hex28(0x32)
	setCredentialC := hex28(0x34)
	snapshots := &ParsedSnapShots{
		Mark: scopedRewardTestSnapshot(0x31, 1_000, poolA, compactA),
		Set: ParsedSnapShot{
			Stake: map[string]uint64{
				setCredentialB: 2_000,
				setCredentialC: 2_500,
			},
			Delegations: map[string][]byte{
				setCredentialB: poolB.PoolKeyHash,
				setCredentialC: poolC.PoolKeyHash,
			},
			PoolParams: map[string]*ParsedPool{
				hex.EncodeToString(poolB.PoolKeyHash): poolB,
				hex.EncodeToString(poolC.PoolKeyHash): poolC,
			},
		},
		Go: scopedRewardTestSnapshot(0x33, 3_000, poolA, compactA),
	}
	// poolB and poolC model the synthesized registrations: they identify the
	// exact two pools reported in but have none of the economics
	// needed for rewards. They are valid fallback inputs to consider for set,
	// and must not contaminate mark or go.
	registered := map[string]*ParsedPool{
		hex.EncodeToString(poolA.PoolKeyHash): poolA,
		hex.EncodeToString(poolB.PoolKeyHash): compactB,
		hex.EncodeToString(poolC.PoolKeyHash): compactC,
	}

	txn := db.MetadataTxn(true)
	require.NoError(t, seedImportedRewardInputs(
		db.Metadata(),
		txn.Metadata(),
		snapshots,
		func(uint64) (map[string]*ParsedPool, error) {
			return registered, nil
		},
		nil,
		100,
		9_999,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	))
	require.NoError(t, txn.Commit())

	want := map[uint64]struct {
		pools map[string]uint64
	}{
		100: {pools: map[string]uint64{
			hex.EncodeToString(poolA.PoolKeyHash): 1_000,
		}},
		99: {pools: map[string]uint64{
			hex.EncodeToString(poolB.PoolKeyHash): 2_000,
			hex.EncodeToString(poolC.PoolKeyHash): 2_500,
		}},
		98: {pools: map[string]uint64{
			hex.EncodeToString(poolA.PoolKeyHash): 3_000,
		}},
	}
	for epoch, expected := range want {
		var totalStake uint64
		for _, stake := range expected.pools {
			totalStake += stake
		}
		snapshot, err := db.Metadata().GetRewardSnapshot(epoch, "mark", nil)
		require.NoError(t, err)
		require.NotNil(t, snapshot,
			"epoch %d must not be dropped because another snapshot refers "+
				"to a synthesized pool", epoch)
		require.Equal(t, uint64(len(expected.pools)), snapshot.TotalPoolCount)
		require.Equal(t, totalStake, uint64(snapshot.TotalActiveStake))
		require.Equal(t, uint64(len(expected.pools)), snapshot.TotalDelegators)

		poolInputs, err := db.Metadata().GetRewardPoolInputs(epoch, nil)
		require.NoError(t, err)
		require.Len(t, poolInputs, len(expected.pools))
		for _, input := range poolInputs {
			key := hex.EncodeToString(input.PoolKeyHash)
			require.Equal(t, expected.pools[key],
				uint64(input.DelegatedStake))
			require.Equal(t, uint64(1), input.DelegatorCount)
		}

		stakeInputs, err := db.Metadata().GetRewardStakeInputs(epoch, nil)
		require.NoError(t, err)
		require.Len(t, stakeInputs, len(expected.pools))
		for _, input := range stakeInputs {
			key := hex.EncodeToString(input.PoolKeyHash)
			require.Equal(t, expected.pools[key], uint64(input.Stake))
		}
	}
}

// Scoping must never turn an actually delegated-to pool into an omission.
// If neither the target snapshot nor the registration fallback has complete
// parameters, validation must still reject the whole epoch rather than seed
// a partial basis that understates every other pool's reward share.
func TestEffectiveRewardPoolParamsKeepsIncompleteReferencedPool(t *testing.T) {
	t.Parallel()

	pool := scopedRewardTestPool(0xC3, 0x41)
	compact := &ParsedPool{
		PoolKeyHash: pool.PoolKeyHash,
		VrfKeyHash:  pool.VrfKeyHash,
	}
	snapshot := scopedRewardTestSnapshot(0x42, 4_000, pool, compact)
	key := hex.EncodeToString(pool.PoolKeyHash)

	params := effectiveRewardPoolParams(
		&snapshot,
		map[string]*ParsedPool{key: compact},
	)
	require.Contains(t, params, key,
		"a referenced pool must remain visible to validation")

	bundle := deriveRewardInputs(&snapshot, params, 100, 9_999, 0)
	require.ErrorContains(t, bundle.validate(), "has no reward account")
}

// When a target snapshot genuinely delegates stake to incomplete pools, the
// safe behavior is still to reject the epoch. The diagnostic must name every
// such pool and its delegated stake, though, so operators can see the whole
// blast radius instead of whichever map entry validation visited first.
func TestDerivedRewardInputsReportsAllIncompleteReferencedPools(t *testing.T) {
	t.Parallel()

	poolA := scopedRewardTestPoolFromKey(
		t,
		"102e9ff50bee440b1ef337f58d1760a5475f3ce716f2aab60e6ef424",
		0x51,
	)
	poolB := scopedRewardTestPoolFromKey(
		t,
		"1fc372fdce61f42d31be7ddfc2bf8e343b08a54e4d3e6d64b2e328ff",
		0x52,
	)
	credentialA := hex28(0x61)
	credentialB := hex28(0x62)
	snapshot := &ParsedSnapShot{
		Stake: map[string]uint64{
			credentialA: 393_520_844,
			credentialB: 397_411_504,
		},
		Delegations: map[string][]byte{
			credentialA: poolA.PoolKeyHash,
			credentialB: poolB.PoolKeyHash,
		},
	}
	params := map[string]*ParsedPool{
		hex.EncodeToString(poolA.PoolKeyHash): {
			PoolKeyHash: poolA.PoolKeyHash,
			VrfKeyHash:  poolA.VrfKeyHash,
		},
		hex.EncodeToString(poolB.PoolKeyHash): {
			PoolKeyHash: poolB.PoolKeyHash,
			VrfKeyHash:  poolB.VrfKeyHash,
		},
	}

	bundle := deriveRewardInputs(snapshot, params, 1_395, 120_644_200, 0)
	err := bundle.validate()
	require.Error(t, err)
	require.ErrorContains(t, err,
		"102e9ff50bee440b1ef337f58d1760a5475f3ce716f2aab60e6ef424")
	require.ErrorContains(t, err, "393520844 lovelace delegated stake")
	require.ErrorContains(t, err,
		"1fc372fdce61f42d31be7ddfc2bf8e343b08a54e4d3e6d64b2e328ff")
	require.ErrorContains(t, err, "397411504 lovelace delegated stake")
}

func TestDerivedRewardInputsBoundsIncompletePoolDiagnostic(t *testing.T) {
	t.Parallel()

	const poolCount = maxRewardSeedFailurePools + 8
	snapshot := &ParsedSnapShot{
		Stake:       make(map[string]uint64, poolCount),
		Delegations: make(map[string][]byte, poolCount),
	}
	params := make(map[string]*ParsedPool, poolCount)
	for i := range poolCount {
		credential := hash28(byte(i + 1))
		poolKey := hash28(byte(i + 100))
		credentialHex := hex.EncodeToString(credential)
		poolHex := hex.EncodeToString(poolKey)
		snapshot.Stake[credentialHex] = 1
		snapshot.Delegations[credentialHex] = poolKey
		params[poolHex] = &ParsedPool{PoolKeyHash: poolKey}
	}

	bundle := deriveRewardInputs(snapshot, params, 1, 1, 0)
	err := bundle.validate()
	require.Error(t, err)
	require.ErrorContains(t, err, "additional pools omitted")
	require.LessOrEqual(t, len(err.Error()), 4_096)
}

func scopedRewardTestPool(poolByte, rewardByte byte) *ParsedPool {
	return &ParsedPool{
		PoolKeyHash:   hash28(poolByte),
		VrfKeyHash:    make([]byte, 32),
		Pledge:        1_000,
		Cost:          75_000_000,
		MarginNum:     1,
		MarginDen:     5,
		RewardAccount: hash28(rewardByte),
		Owners:        [][]byte{hash28(rewardByte)},
	}
}

func scopedRewardTestPoolFromKey(
	t *testing.T,
	poolHex string,
	rewardByte byte,
) *ParsedPool {
	t.Helper()
	pool := scopedRewardTestPool(0, rewardByte)
	key, err := hex.DecodeString(poolHex)
	require.NoError(t, err)
	require.Len(t, key, credentialHashSize)
	pool.PoolKeyHash = key
	return pool
}

func scopedRewardTestSnapshot(
	credentialByte byte,
	stake uint64,
	pool *ParsedPool,
	params *ParsedPool,
) ParsedSnapShot {
	credential := hex28(credentialByte)
	poolKey := hex.EncodeToString(pool.PoolKeyHash)
	return ParsedSnapShot{
		Stake:       map[string]uint64{credential: stake},
		Delegations: map[string][]byte{credential: pool.PoolKeyHash},
		PoolParams:  map[string]*ParsedPool{poolKey: params},
	}
}

func hash28(b byte) []byte {
	out := make([]byte, 28)
	for i := range out {
		out[i] = b
	}
	return out
}

func hex28(b byte) string { return hex.EncodeToString(hash28(b)) }

// twoPoolSnapshot is a snapshot with two pools: one whose owner also
// delegates to it, and one with only outside delegators.
func twoPoolSnapshot() *ParsedSnapShot {
	ownerA := hex28(0x11)
	delegA := hex28(0x12)
	delegB := hex28(0x21)
	return &ParsedSnapShot{
		Stake: map[string]uint64{
			ownerA: 1_000,
			delegA: 4_000,
			delegB: 7_000,
		},
		Delegations: map[string][]byte{
			ownerA: hash28(0xAA),
			delegA: hash28(0xAA),
			delegB: hash28(0xBB),
		},
		PoolParams: map[string]*ParsedPool{
			hex28(0xAA): {
				PoolKeyHash:   hash28(0xAA),
				Pledge:        500,
				Cost:          340,
				MarginNum:     1,
				MarginDen:     50,
				RewardAccount: hash28(0x11),
				Owners:        [][]byte{hash28(0x11)},
			},
			hex28(0xBB): {
				PoolKeyHash:   hash28(0xBB),
				Pledge:        0,
				Cost:          170,
				MarginNum:     3,
				MarginDen:     100,
				RewardAccount: hash28(0x21),
			},
		},
	}
}

// The derived basis has to reconcile, because the ledger reads it back through
// a path that returns an error rather than skipping when it does not — which
// would fail the epoch rollover outright.
func TestDeriveRewardInputsReconciles(t *testing.T) {
	t.Parallel()

	bundle := deriveRewardInputs(twoPoolSnapshot(), nil, 1385, 119_750_400, 0)
	require.NotNil(t, bundle)
	require.NoError(t, bundle.validate())

	require.Equal(t, uint64(1385), bundle.snapshot.Epoch)
	require.Equal(t, uint64(2), bundle.snapshot.TotalPoolCount)
	require.Equal(t, uint64(3), bundle.snapshot.TotalDelegators)
	require.Equal(t, uint64(12_000), uint64(bundle.snapshot.TotalActiveStake))
	require.Equal(t, models.RewardStakeCalculationVersion,
		bundle.snapshot.CalculationVersion,
		"imported reward snapshots must use the current calculation version")
	require.Len(t, bundle.stakeInputs, 3)

	byPool := map[string]*struct {
		delegated, owner uint64
		delegators       uint64
	}{}
	for _, p := range bundle.poolInputs {
		byPool[hex.EncodeToString(p.PoolKeyHash)] = &struct {
			delegated, owner uint64
			delegators       uint64
		}{uint64(p.DelegatedStake), uint64(p.OwnerStake), p.DelegatorCount}
	}
	a := byPool[hex28(0xAA)]
	require.NotNil(t, a)
	require.Equal(t, uint64(5_000), a.delegated)
	// Only the credential that is a registered owner counts toward owner
	// stake; the other delegator to the same pool must not.
	require.Equal(t, uint64(1_000), a.owner)
	require.Equal(t, uint64(2), a.delegators)

	b := byPool[hex28(0xBB)]
	require.NotNil(t, b)
	require.Equal(t, uint64(7_000), b.delegated)
	require.Zero(
		t,
		b.owner,
		"a pool with no owner delegating has no owner stake",
	)
	require.Equal(t, uint64(1), b.delegators)
}

// A credential delegated to a pool the parameter map does not describe must
// fail the epoch, not be quietly dropped.
//
// Dropping it leaves a basis that still reconciles -- the totals are summed
// from whatever remained -- while understating every surviving pool's share of
// the reward pot. A pool that retired or re-registered between the snapshot's
// epoch and the import produces exactly this, as does a registration set only
// partially populated when the seeding runs, and neither is visible in the
// result.
func TestDeriveRewardInputsRejectsUnattributableStake(t *testing.T) {
	t.Parallel()

	snap := twoPoolSnapshot()
	orphan := hex28(0x31)
	snap.Stake[orphan] = 9_000
	snap.Delegations[orphan] = hash28(0xCC) // no parameters for this pool

	bundle := deriveRewardInputs(snap, nil, 1385, 1, 0)
	require.NotNil(t, bundle)
	require.ErrorContains(t, bundle.validate(), "have no parameters")
	require.ErrorContains(t, bundle.validate(), "9000",
		"the error should say how much stake could not be attributed")
}

// Zero-stake credentials are rejected by the ledger's validator, so they must
// never reach it.
func TestDeriveRewardInputsDropsZeroStake(t *testing.T) {
	t.Parallel()

	snap := twoPoolSnapshot()
	idle := hex28(0x41)
	snap.Stake[idle] = 0
	snap.Delegations[idle] = hash28(0xAA)

	bundle := deriveRewardInputs(snap, nil, 1385, 1, 0)
	require.NotNil(t, bundle)
	require.NoError(t, bundle.validate())
	require.Len(t, bundle.stakeInputs, 3)
}

// The gate exists to stop an unusable basis reaching the database. A margin
// the snapshot reports above 1 is the kind of thing it must catch: the ledger
// rejects it on read, and on that path a rejection fails the rollover.
func TestDerivedRewardInputsGateRejectsBadMargin(t *testing.T) {
	t.Parallel()

	snap := twoPoolSnapshot()
	pool := snap.PoolParams[hex28(0xAA)]
	if pool == nil {
		t.Fatal("fixture has no parameters for pool AA")
	}
	pool.MarginNum = 3
	pool.MarginDen = 2

	bundle := deriveRewardInputs(snap, nil, 1385, 1, 0)
	require.NotNil(t, bundle)
	require.ErrorContains(t, bundle.validate(), "margin outside [0,1]")
}

// A pool key of the wrong length is likewise refused rather than written.
func TestDerivedRewardInputsGateRejectsBadPoolKey(t *testing.T) {
	t.Parallel()

	snap := twoPoolSnapshot()
	pool := snap.PoolParams[hex28(0xAA)]
	if pool == nil {
		t.Fatal("fixture has no parameters for pool AA")
	}
	pool.PoolKeyHash = []byte{0x01, 0x02}

	bundle := deriveRewardInputs(snap, nil, 1385, 1, 0)
	require.NotNil(t, bundle)
	require.ErrorContains(t, bundle.validate(), "pool key hash")
}

// The gate must catch a totals mismatch, which is the failure mode a future
// edit to the derivation is most likely to introduce.
func TestDerivedRewardInputsGateRejectsTotalsMismatch(t *testing.T) {
	t.Parallel()

	bundle := deriveRewardInputs(twoPoolSnapshot(), nil, 1385, 1, 0)
	require.NotNil(t, bundle)
	require.NoError(t, bundle.validate())

	bundle.snapshot.TotalActiveStake++
	require.ErrorContains(t, bundle.validate(), "does not match snapshot")
}

// A script stake credential must reach the reward basis as a script
// credential. The stake maps are keyed by hash alone, so the type travels
// beside them; losing it attributes a script delegator's reward and its share
// of leadership stake to a key credential, or to whichever credential happens
// to share the hash.
//
// This is synthetic because it has to be: the DevNet fixture contains only
// key-hash credentials, so a real-snapshot test cannot distinguish a preserved
// tag from a hardcoded zero.
func TestDeriveRewardInputsPreservesScriptCredentialType(t *testing.T) {
	t.Parallel()

	snap := twoPoolSnapshot()
	scriptCred := hex28(0x51)
	snap.Stake[scriptCred] = 3_000
	snap.Delegations[scriptCred] = hash28(0xAA)
	// 0x12 is deliberately left out: the derivation's default branch only
	// runs for a credential absent from the map, so listing every credential
	// would leave that branch untested and the assertion below would be
	// reading back a stored zero rather than the default.
	snap.StakeTags = map[string]uint8{
		hex28(0x11): 0,
		hex28(0x21): 0,
		scriptCred:  1, // script hash
	}

	bundle := deriveRewardInputs(snap, nil, 1385, 1, 0)
	require.NotNil(t, bundle)
	require.NoError(t, bundle.validate())

	var found bool
	for _, input := range bundle.stakeInputs {
		if hex.EncodeToString(input.StakingKey) != scriptCred {
			continue
		}
		found = true
		require.Equal(t, uint8(1), input.CredentialTag,
			"a script credential must not be persisted as a key hash")
	}
	require.True(t, found, "the script credential should be in the basis")

	// And a credential with no recorded type still reads as a key hash, which
	// is the only safe default for a snapshot shape that does not encode it.
	untagged := hex28(0x12)
	require.NotContains(t, snap.StakeTags, untagged,
		"this credential must stay out of StakeTags or the default branch "+
			"below is never reached")
	found = false
	for _, input := range bundle.stakeInputs {
		if hex.EncodeToString(input.StakingKey) != untagged {
			continue
		}
		found = true
		require.Equal(t, uint8(0), input.CredentialTag)
	}
	require.True(t, found,
		"the untagged credential should be in the basis, or the default "+
			"is untested")
}

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
	t.Parallel()

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
		nil,
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

// A snapshot describes its own epoch's pool parameters, so a nil resolver is
// enough on its own: no registration lookup is needed for a pool the snapshot
// already carries. This is the case turned on -- it is also how a
// pool that has since retired gets described at all.
func TestSeedImportedRewardInputsUsesSnapshotPoolParams(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)
	snapshots, err := ParseSnapShots(state.SnapShotsData)
	require.NoError(t, err, "stake snapshots must parse completely")

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	txn := db.MetadataTxn(true)
	require.NoError(t, seedImportedRewardInputs(
		db.Metadata(), txn.Metadata(), snapshots, nil, nil,
		state.Epoch, state.Tip.Slot, logger,
	))
	require.NoError(t, txn.Commit())

	snapshot, err := db.Metadata().GetRewardSnapshot(state.Epoch, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, snapshot,
		"the snapshot carries its own pool parameters, so this epoch must "+
			"be seeded without any registration lookup at all")
	poolInputs, err := db.Metadata().GetRewardPoolInputs(state.Epoch, nil)
	require.NoError(t, err)
	require.NotEmpty(t, poolInputs)
	for _, pool := range poolInputs {
		require.NotEmpty(t, pool.RewardAccount,
			"a pool input seeded from the snapshot must carry the reward "+
				"account the snapshot records for it")
	}
}

// Parameters that are genuinely unusable must still write nothing, rather
// than rows the ledger will later refuse. A snapshot carrying only the
// compact pool-distr shape -- a VRF key and nothing else -- is what that
// looks like, and stripping the parsed parameters reproduces it without
// needing a fixture in that format.
func TestSeedImportedRewardInputsWritesNothingWithoutPoolParams(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)
	snapshots, err := ParseSnapShots(state.SnapShotsData)
	require.NoError(t, err, "stake snapshots must parse completely")
	stripPoolParamsToVrfOnly(snapshots)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	txn := db.MetadataTxn(true)
	require.NoError(t, seedImportedRewardInputs(
		db.Metadata(), txn.Metadata(), snapshots, nil, nil,
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

// stripPoolParamsToVrfOnly reduces every parsed pool entry to what the
// compact pool-distr shape carries, so a test can exercise the paths that
// exist for snapshots in that format using a fixture that is not.
func stripPoolParamsToVrfOnly(snapshots *ParsedSnapShots) {
	for _, snap := range []*ParsedSnapShot{
		&snapshots.Mark, &snapshots.Set, &snapshots.Go,
	} {
		for key, pool := range snap.PoolParams {
			snap.PoolParams[key] = &ParsedPool{
				PoolKeyHash: pool.PoolKeyHash,
				VrfKeyHash:  pool.VrfKeyHash,
			}
		}
	}
}

// hexPoolKey is a local helper so the wiring test does not depend on the
// derivation's own encoding choices.
func hexPoolKey(b []byte) string { return hex.EncodeToString(b) }
