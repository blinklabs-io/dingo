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
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestRepairRewardStakeInputBundle(t *testing.T) {
	t.Parallel()
	for _, apply := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "dry-run", true: "apply"}[apply],
			func(t *testing.T) {
				t.Parallel()
				db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
				require.NoError(t, err)
				bundle := deriveRewardInputs(
					twoPoolSnapshot(),
					nil,
					10,
					99,
					100,
				)
				require.NoError(t, bundle.validate())
				bundle.snapshot.Authoritative = true
				require.NoError(
					t,
					db.Metadata().SaveRewardSnapshot(bundle.snapshot, nil),
				)
				require.NoError(
					t,
					db.Metadata().SaveRewardPoolInputs(bundle.poolInputs, nil),
				)
				// Preserve only a non-owner row, reproducing an interrupted chunk writer.
				var subset []*models.RewardStakeInput
				for _, row := range bundle.stakeInputs {
					if !row.Owner {
						subset = append(subset, row)
						break
					}
				}
				require.Len(t, subset, 1)
				require.NoError(
					t,
					db.Metadata().SaveRewardStakeInputs(subset, nil),
				)
				require.NoError(
					t,
					db.Metadata().
						SetSyncState("dingo:stake-reward:precompute-cursor:10", "stale", nil),
				)
				require.NoError(
					t,
					db.Metadata().SetSyncState("unrelated", "preserve", nil),
				)
				tx := db.MetadataTxn(context.Background(), true)
				report, err := repairRewardStakeInputBundle(
					db.Metadata(),
					tx.Metadata(),
					bundle,
					bundle.snapshot,
					apply,
				)
				require.NoError(t, err)
				require.Equal(t, 1, report.StoredRows)
				require.Equal(t, 3, report.CanonicalRows)
				require.NoError(t, tx.Commit())
				cursor, err := db.Metadata().
					GetSyncState("dingo:stake-reward:precompute-cursor:10", nil)
				require.NoError(t, err)
				if apply {
					require.Empty(t, cursor)
				} else {
					require.Equal(t, "stale", cursor)
				}
				unrelated, err := db.Metadata().GetSyncState("unrelated", nil)
				require.NoError(t, err)
				require.Equal(t, "preserve", unrelated)
				rows, err := db.Metadata().GetRewardStakeInputs(10, nil)
				require.NoError(t, err)
				if apply {
					require.Len(t, rows, 3)
					owners := 0
					for _, row := range rows {
						if row.Owner {
							owners++
						}
					}
					require.Equal(t, 1, owners)
				} else {
					require.Len(t, rows, 1)
				}
			},
		)
	}
}

func TestRepairRewardStakeInputBundleRejectsPoolMismatch(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	bundle := deriveRewardInputs(twoPoolSnapshot(), nil, 10, 99, 100)
	bundle.snapshot.Authoritative = true
	require.NoError(t, db.Metadata().SaveRewardSnapshot(bundle.snapshot, nil))
	retained := *bundle.poolInputs[0]
	retained.OwnerStake++
	require.NoError(
		t,
		db.Metadata().
			SaveRewardPoolInputs(append([]*models.RewardPoolInput{&retained}, bundle.poolInputs[1:]...), nil),
	)
	tx := db.MetadataTxn(context.Background(), true)
	defer tx.Rollback()
	_, err = repairRewardStakeInputBundle(
		db.Metadata(),
		tx.Metadata(),
		bundle,
		bundle.snapshot,
		true,
	)
	require.ErrorContains(t, err, "disagree")
	rows, err := db.Metadata().GetRewardStakeInputs(10, tx.Metadata())
	require.NoError(t, err)
	require.Empty(t, rows)
}

func TestImportedAuthoritativeBasisCompleteness(t *testing.T) {
	t.Parallel()
	for _, pending := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "complete preserved", true: "pending reseeded"}[pending],
			func(t *testing.T) {
				t.Parallel()
				db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
				require.NoError(t, err)
				require.NoError(
					t,
					db.Metadata().
						SetEpoch(0, 9, nil, nil, nil, nil, 6, 1, 100, nil),
				)
				snapshot := twoPoolSnapshot()
				bundle := deriveRewardInputs(snapshot, nil, 10, 99, 100)
				bundle.snapshot.Authoritative = true
				require.NoError(
					t,
					db.Metadata().SaveRewardSnapshot(bundle.snapshot, nil),
				)
				require.NoError(
					t,
					db.Metadata().SaveRewardPoolInputs(bundle.poolInputs, nil),
				)
				rows := bundle.stakeInputs
				if pending {
					rows = rows[:1]
					require.NoError(
						t,
						db.Metadata().
							SetSyncState("dingo:stake-reward:stake-inputs-pending", `{"entries":[{"epoch":10,"boundary_slot":100}]}`, nil),
					)
				}
				require.NoError(
					t,
					db.Metadata().SaveRewardStakeInputs(rows, nil),
				)
				complete, err := importedAuthoritativeRewardBasisComplete(
					db.Metadata(),
					nil,
					bundle.snapshot,
				)
				require.NoError(t, err)
				require.Equal(t, !pending, complete)
				tx := db.MetadataTxn(context.Background(), true)
				require.NoError(
					t,
					seedImportedRewardInputs(
						db.Metadata(),
						tx.Metadata(),
						&ParsedSnapShots{
							Mark: *snapshot,
							Set:  *snapshot,
							Go:   *snapshot,
						},
						nil,
						nil,
						10,
						200,
						nil,
					),
				)
				require.NoError(t, tx.Commit())
				actual, err := db.Metadata().GetRewardStakeInputs(10, nil)
				require.NoError(t, err)
				require.Len(t, actual, 3)
				retained, err := db.Metadata().
					GetRewardSnapshot(10, "mark", nil)
				require.NoError(t, err)
				require.True(t, retained.Authoritative)
				require.Equal(t, uint64(99), retained.CapturedSlot)
				require.Equal(t, uint64(100), retained.BoundarySlot)
			},
		)
	}
}

func TestRepairRewardStakeInputBundleCorrectsExistingOwner(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	bundle := deriveRewardInputs(twoPoolSnapshot(), nil, 10, 99, 100)
	bundle.snapshot.Authoritative = true
	require.NoError(t, db.Metadata().SaveRewardSnapshot(bundle.snapshot, nil))
	require.NoError(
		t,
		db.Metadata().SaveRewardPoolInputs(bundle.poolInputs, nil),
	)
	for _, row := range bundle.stakeInputs {
		copy := *row
		if copy.Owner {
			copy.Owner = false
		}
		require.NoError(
			t,
			db.Metadata().
				SaveRewardStakeInputs([]*models.RewardStakeInput{&copy}, nil),
		)
	}
	before, err := db.Metadata().GetRewardStakeInputs(10, nil)
	require.NoError(t, err)
	tx := db.MetadataTxn(context.Background(), true)
	report, err := repairRewardStakeInputBundle(
		db.Metadata(),
		tx.Metadata(),
		bundle,
		bundle.snapshot,
		true,
	)
	require.NoError(t, err)
	require.Equal(t, 1, report.ChangedRows)
	require.NoError(t, tx.Commit())
	after, err := db.Metadata().GetRewardStakeInputs(10, nil)
	require.NoError(t, err)
	require.Len(t, after, len(before))
	owners := 0
	ids := map[uint]bool{}
	for _, row := range before {
		ids[row.ID] = true
	}
	for _, row := range after {
		require.True(t, ids[row.ID])
		if row.Owner {
			owners++
		}
	}
	require.Equal(t, 1, owners)
}

func TestRewardInputRepairUsesFrozenPoolFallback(t *testing.T) {
	t.Parallel()
	snapshot := twoPoolSnapshot()
	historical := map[string]*ParsedPool{
		hex28(0xAA): snapshot.PoolParams[hex28(0xAA)],
	}
	incomplete := deriveRewardInputs(snapshot, historical, 10, 99, 100)
	require.Error(t, incomplete.validate())
	complete := deriveRewardInputs(
		snapshot,
		effectiveRewardPoolParams(snapshot, historical),
		10,
		99,
		100,
	)
	require.NoError(t, complete.validate())
	require.Len(t, complete.poolInputs, 2)
	require.Len(t, complete.stakeInputs, 3)
}

func TestRepairRewardStakeInputsPreservesRetainedEconomics(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	bundle := deriveRewardInputs(twoPoolSnapshot(), nil, 10, 99, 100)
	bundle.snapshot.Authoritative = true
	require.NoError(t, db.Metadata().SaveRewardSnapshot(bundle.snapshot, nil))
	retained := *bundle.poolInputs[0]
	retained.Cost++
	retained.Pledge++
	retained.Margin = &types.Rat{Rat: big.NewRat(1, 3)}
	retained.RewardAccount = hash28(0x33)
	retained.RewardAccountCredentialTag = 1
	require.NoError(
		t,
		db.Metadata().
			SaveRewardPoolInputs(append([]*models.RewardPoolInput{&retained}, bundle.poolInputs[1:]...), nil),
	)
	before, err := db.Metadata().GetRewardPoolInputs(10, nil)
	require.NoError(t, err)
	tx := db.MetadataTxn(context.Background(), true)
	_, err = repairRewardStakeInputBundle(
		db.Metadata(),
		tx.Metadata(),
		bundle,
		bundle.snapshot,
		true,
	)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	after, err := db.Metadata().GetRewardPoolInputs(10, nil)
	require.NoError(t, err)
	require.Equal(t, before, after)
	rows, err := db.Metadata().GetRewardStakeInputs(10, nil)
	require.NoError(t, err)
	require.Len(t, rows, 3)
}

type rewardRepairRegistrationWindowSpy struct {
	metadata.MetadataStore
	start, epoch, slot uint64
}

func (s *rewardRepairRegistrationWindowSpy) GetPoolRegistrationsEffectiveForEpoch(
	keys []lcommon.PoolKeyHash,
	start, epoch, slot uint64,
	txn types.Txn,
) ([]models.PoolRegistration, error) {
	s.start = start
	s.epoch = epoch
	s.slot = slot
	return s.MetadataStore.GetPoolRegistrationsEffectiveForEpoch(
		keys,
		start,
		epoch,
		slot,
		txn,
	)
}

func TestAuthoritativeRewardRepairUsesOriginalCaptureWindow(t *testing.T) {
	t.Parallel()
	for _, dijkstra := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "Conway", true: "Dijkstra"}[dijkstra],
			func(t *testing.T) {
				t.Parallel()
				db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
				require.NoError(t, err)
				require.NoError(
					t,
					db.Metadata().
						SetEpoch(80, 9, nil, nil, nil, nil, 6, 1, 20, nil),
				)
				retained := &models.RewardSnapshot{
					Epoch:        10,
					CapturedSlot: 99,
					BoundarySlot: 100,
				}
				if dijkstra {
					retained.ProtocolVersion = lcommon.ProtocolVersionDijkstra
				}
				spy := &rewardRepairRegistrationWindowSpy{
					MetadataStore: db.Metadata(),
				}
				params, err := authoritativeRewardInputPoolParams(
					spy,
					nil,
					twoPoolSnapshot(),
					retained,
				)
				require.NoError(t, err)
				require.Len(t, params, 2)
				require.Equal(t, uint64(99), spy.slot)
				if dijkstra {
					require.Equal(t, uint64(100), spy.start)
					require.Equal(t, uint64(10), spy.epoch)
				} else {
					require.Equal(t, uint64(80), spy.start)
					require.Equal(t, uint64(9), spy.epoch)
				}
			},
		)
	}
}

func TestRetainedCaptureOwnersOverrideFrozenPoolChanges(t *testing.T) {
	t.Parallel()
	frozen := twoPoolSnapshot()
	historical := *frozen.PoolParams[hex28(0xAA)]
	frozenChanged := historical
	frozenChanged.Owners = nil
	frozenChanged.Pledge = 0
	frozen.PoolParams[hex28(0xAA)] = &frozenChanged
	params := retainedCaptureRewardPoolParams(
		frozen,
		map[string]*ParsedPool{hex28(0xAA): &historical},
	)
	bundle := deriveRewardInputs(frozen, params, 10, 99, 100)
	require.NoError(t, bundle.validate())
	require.Len(t, bundle.poolInputs, 2)
	for _, pool := range bundle.poolInputs {
		if bytes.Equal(pool.PoolKeyHash, hash28(0xAA)) {
			require.Equal(t, types.Uint64(1000), pool.OwnerStake)
			require.Equal(t, types.Uint64(500), pool.Pledge)
		}
	}
}

func TestImportedRewardInputPendingStateFailsClosed(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	bundle := deriveRewardInputs(twoPoolSnapshot(), nil, 10, 99, 100)
	bundle.snapshot.Authoritative = true
	require.NoError(
		t,
		db.Metadata().SaveRewardStakeInputs(bundle.stakeInputs, nil),
	)
	for _, raw := range []string{"null", "{}", `{"entries":{}}`, `{"entries":[],"unknown":1}`, `{"entries":[{"epoch":10,"boundary_slot":100,"unknown":1}]}`, `{"entries":[{"epoch":10}]}`} {
		require.NoError(
			t,
			db.Metadata().
				SetSyncState(models.StakeRewardSourcePrefix+"stake-inputs-pending", raw, nil),
		)
		complete, err := importedAuthoritativeRewardBasisComplete(
			db.Metadata(),
			nil,
			bundle.snapshot,
		)
		require.Error(t, err, raw)
		require.False(t, complete, raw)
	}
	for _, raw := range []string{"", `{"entries":null}`, `{"entries":[]}`} {
		require.NoError(
			t,
			db.Metadata().
				SetSyncState(models.StakeRewardSourcePrefix+"stake-inputs-pending", raw, nil),
		)
		complete, err := importedAuthoritativeRewardBasisComplete(
			db.Metadata(),
			nil,
			bundle.snapshot,
		)
		require.NoError(t, err, raw)
		require.True(t, complete, raw)
	}
}

func TestRewardInputRepairRequiresExplicitTransaction(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	bundle := deriveRewardInputs(twoPoolSnapshot(), nil, 10, 99, 100)
	bundle.snapshot.Authoritative = true
	require.NoError(t, db.Metadata().SaveRewardPoolInputs(bundle.poolInputs, nil))
	key := models.StakeRewardSourcePrefix + "precompute-cursor:10"
	require.NoError(t, db.Metadata().SetSyncState(key, "preserve", nil))
	_, err = repairRewardStakeInputBundle(db.Metadata(), nil, bundle, bundle.snapshot, true)
	require.ErrorContains(t, err, "explicit transaction")
	rows, err := db.Metadata().GetRewardStakeInputs(10, nil)
	require.NoError(t, err)
	require.Empty(t, rows)
	cursor, err := db.Metadata().GetSyncState(key, nil)
	require.NoError(t, err)
	require.Equal(t, "preserve", cursor)
}
