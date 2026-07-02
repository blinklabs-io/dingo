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
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	sqliteplugin "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

func TestSnapshotImportTargetsAlignWithRotation(t *testing.T) {
	snapshots := &ParsedSnapShots{}

	targets := snapshotImportTargets(1237, snapshots)
	if len(targets) != 3 {
		t.Fatalf("expected 3 targets, got %d", len(targets))
	}

	expected := []struct {
		name  string
		epoch uint64
	}{
		{name: "mark", epoch: 1237},
		{name: "set", epoch: 1236},
		{name: "go", epoch: 1235},
	}

	for i, target := range targets {
		if target.name != expected[i].name {
			t.Fatalf(
				"target %d: expected name %q, got %q",
				i,
				expected[i].name,
				target.name,
			)
		}
		if target.targetEpoch != expected[i].epoch {
			t.Fatalf(
				"target %d: expected epoch %d, got %d",
				i,
				expected[i].epoch,
				target.targetEpoch,
			)
		}
	}
}

func TestSnapshotImportTargetsSkipNegativeEpochs(t *testing.T) {
	snapshots := &ParsedSnapShots{}

	targets0 := snapshotImportTargets(0, snapshots)
	if len(targets0) != 1 {
		t.Fatalf("epoch 0: expected 1 target, got %d", len(targets0))
	}
	if targets0[0].name != "mark" || targets0[0].targetEpoch != 0 {
		t.Fatalf("epoch 0: unexpected target %+v", targets0[0])
	}

	targets1 := snapshotImportTargets(1, snapshots)
	if len(targets1) != 2 {
		t.Fatalf("epoch 1: expected 2 targets, got %d", len(targets1))
	}
	if targets1[0].name != "mark" || targets1[0].targetEpoch != 1 {
		t.Fatalf("epoch 1 mark: unexpected target %+v", targets1[0])
	}
	if targets1[1].name != "set" || targets1[1].targetEpoch != 0 {
		t.Fatalf("epoch 1 set: unexpected target %+v", targets1[1])
	}
}

func TestSnapshotImportTargetsNilSnapshots(t *testing.T) {
	targets := snapshotImportTargets(7, nil)
	if targets != nil {
		t.Fatalf("expected nil targets, got %+v", targets)
	}
}

func TestImportedEpochSummaryUsesCurrentEpochMetadata(t *testing.T) {
	nonce := []byte{0x01, 0x02, 0x03}

	summary := importedEpochSummary(
		nil,
		1237,
		1237,
		456789,
		nonce,
		100,
		2,
		3,
	)

	if summary.Epoch != 1237 {
		t.Fatalf("expected epoch 1237, got %d", summary.Epoch)
	}
	if summary.BoundarySlot != 456789 {
		t.Fatalf(
			"expected boundary slot 456789, got %d",
			summary.BoundarySlot,
		)
	}
	if !bytes.Equal(summary.EpochNonce, nonce) {
		t.Fatalf(
			"expected epoch nonce %x, got %x",
			nonce,
			summary.EpochNonce,
		)
	}
	if !summary.SnapshotReady {
		t.Fatal("expected snapshot summary to be marked ready")
	}
}

func TestImportedEpochSummaryLeavesHistoricalMetadataUnknown(t *testing.T) {
	summary := importedEpochSummary(
		nil,
		1237,
		1235,
		456789,
		[]byte{0x01, 0x02, 0x03},
		100,
		2,
		3,
	)

	if summary.BoundarySlot != 0 {
		t.Fatalf(
			"expected historical boundary slot to remain unknown, got %d",
			summary.BoundarySlot,
		)
	}
	if len(summary.EpochNonce) != 0 {
		t.Fatalf(
			"expected historical epoch nonce to remain unknown, got %x",
			summary.EpochNonce,
		)
	}
}

func TestImportedEpochSummaryPreservesExistingMetadata(t *testing.T) {
	existing := &models.EpochSummary{
		Epoch:        1235,
		EpochNonce:   []byte{0xaa, 0xbb, 0xcc},
		BoundarySlot: 777,
	}

	summary := importedEpochSummary(
		existing,
		1237,
		1235,
		456789,
		[]byte{0x01, 0x02, 0x03},
		100,
		2,
		3,
	)

	if summary.BoundarySlot != existing.BoundarySlot {
		t.Fatalf(
			"expected boundary slot %d, got %d",
			existing.BoundarySlot,
			summary.BoundarySlot,
		)
	}
	if !bytes.Equal(summary.EpochNonce, existing.EpochNonce) {
		t.Fatalf(
			"expected preserved epoch nonce %x, got %x",
			existing.EpochNonce,
			summary.EpochNonce,
		)
	}
	if summary.TotalPoolCount != 2 {
		t.Fatalf(
			"expected updated total pool count 2, got %d",
			summary.TotalPoolCount,
		)
	}
}

func TestImportedEpochSummaryKeepsCurrentEpochMetadataWhenExisting(
	t *testing.T,
) {
	existing := &models.EpochSummary{
		Epoch:        1237,
		EpochNonce:   []byte{0xaa, 0xbb, 0xcc},
		BoundarySlot: 777,
	}
	currentNonce := []byte{0x01, 0x02, 0x03}

	summary := importedEpochSummary(
		existing,
		1237,
		1237,
		456789,
		currentNonce,
		100,
		2,
		3,
	)

	if summary.BoundarySlot != 456789 {
		t.Fatalf(
			"expected current boundary slot 456789, got %d",
			summary.BoundarySlot,
		)
	}
	if !bytes.Equal(summary.EpochNonce, currentNonce) {
		t.Fatalf(
			"expected current epoch nonce %x, got %x",
			currentNonce,
			summary.EpochNonce,
		)
	}
}

func TestPersistImportedSnapshotClearsEpochWhenEmpty(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	store := db.Metadata()
	targetEpoch := uint64(100)
	poolKeyHash := make([]byte, 28)
	poolKeyHash[0] = 0x01

	require.NoError(t, store.SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{
			{
				Epoch:          targetEpoch,
				SnapshotType:   "mark",
				PoolKeyHash:    poolKeyHash,
				TotalStake:     10,
				DelegatorCount: 2,
				CapturedSlot:   55,
			},
		},
		nil,
	))

	existingSummary := &models.EpochSummary{
		Epoch:            targetEpoch,
		TotalActiveStake: 10,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		BoundarySlot:     777,
		EpochNonce:       []byte{0xaa, 0xbb, 0xcc},
	}
	require.NoError(t, store.SaveEpochSummary(existingSummary, nil))

	err = persistImportedSnapshot(
		ImportConfig{
			Database: db,
			State: &RawLedgerState{
				Epoch:      102,
				EpochNonce: []byte{0x01, 0x02, 0x03},
			},
		},
		999,
		snapshotImportTarget{
			name:        "set",
			targetEpoch: targetEpoch,
		},
		nil,
	)
	require.NoError(t, err)

	snapshots, err := store.GetPoolStakeSnapshotsByEpoch(
		targetEpoch,
		"mark",
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, snapshots)

	summary, err := store.GetEpochSummary(targetEpoch, nil)
	require.NoError(t, err)
	require.NotNil(t, summary)
	require.Equal(t, targetEpoch, summary.Epoch)
	require.Equal(t, uint64(0), uint64(summary.TotalActiveStake))
	require.Zero(t, summary.TotalPoolCount)
	require.Zero(t, summary.TotalDelegators)
	require.True(t, summary.SnapshotReady)
	require.Equal(t, existingSummary.BoundarySlot, summary.BoundarySlot)
	require.True(t, bytes.Equal(existingSummary.EpochNonce, summary.EpochNonce))
}

func TestPersistImportedActivePoolDistribution(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	poolKeyHash := make([]byte, 28)
	poolKeyHash[0] = 0x4a
	rows := ActivePoolDistributionSnapshots(
		[]ParsedActivePoolStake{
			{
				PoolKeyHash:      poolKeyHash,
				StakeNumerator:   3,
				StakeDenominator: 10,
				VrfKeyHash:       bytes.Repeat([]byte{0x9b}, 32),
			},
		},
		298,
		127178646,
	)

	require.NoError(t, persistImportedActivePoolDistribution(
		ImportConfig{
			Database: db,
			State:    &RawLedgerState{Epoch: 298},
		},
		rows,
	))

	stored, err := db.Metadata().GetPoolStakeSnapshot(
		298,
		models.PoolStakeSnapshotTypeActive,
		poolKeyHash,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Equal(t, uint64(3), uint64(stored.TotalStake))
	require.Equal(t, uint64(10), uint64(stored.StakeDenominator))
	require.Equal(t, uint64(127178646), stored.CapturedSlot)
}

// TestPersistImportedSnapshotResolvesAutoVoteOnlyForMark verifies the
// CIP-1694 reward-account auto-vote resolver runs against live Pool /
// Account state for the "mark" rotation (whose target epoch equals
// the import-time epoch and therefore matches the live state) but is
// SKIPPED for "set" and "go" rotations (whose target epochs are
// older than the live state). The set/go rows are still written but
// must carry RewardAccountAutoVoteResolved=false so the tally
// fallback treats them as implicit no rather than freezing today's
// delegation map into a historical boundary.
func TestPersistImportedSnapshotResolvesAutoVoteOnlyForMark(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	poolKeyHash := make([]byte, 28)
	poolKeyHash[0] = 0x42
	rewardAccount := make([]byte, 28)
	rewardAccount[0] = 0x43

	// Seed Pool + Account state so the resolver, if called, would
	// produce a non-default outcome (Abstain). Set/go must NOT pick
	// this up — that's the regression we're guarding against.
	store, ok := db.Metadata().(*sqliteplugin.MetadataStoreSqlite)
	require.True(t, ok, "test requires the sqlite metadata backend")
	require.NoError(t, store.DB().Create(&models.Pool{
		PoolKeyHash:   poolKeyHash,
		RewardAccount: rewardAccount,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: rewardAccount,
		DrepType:   models.DrepTypeAlwaysAbstain,
		AddedSlot:  1,
		Active:     true,
	}).Error)

	mkSnapshot := func(epoch uint64) []*models.PoolStakeSnapshot {
		return []*models.PoolStakeSnapshot{
			{
				Epoch:          epoch,
				SnapshotType:   "mark",
				PoolKeyHash:    poolKeyHash,
				TotalStake:     100,
				DelegatorCount: 1,
				CapturedSlot:   55,
			},
		}
	}

	cases := []struct {
		name         string
		targetEpoch  uint64
		wantResolved bool
		wantAutoVote uint8
	}{
		{
			name:         "mark",
			targetEpoch:  102,
			wantResolved: true,
			wantAutoVote: models.PoolRewardAccountAutoVoteAbstain,
		},
		{
			name:         "set",
			targetEpoch:  101,
			wantResolved: false,
			wantAutoVote: models.PoolRewardAccountAutoVoteNone,
		},
		{
			name:         "go",
			targetEpoch:  100,
			wantResolved: false,
			wantAutoVote: models.PoolRewardAccountAutoVoteNone,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := persistImportedSnapshot(
				ImportConfig{
					Database: db,
					State: &RawLedgerState{
						Epoch:      102,
						EpochNonce: []byte{0x01},
					},
				},
				999,
				snapshotImportTarget{
					name:        tc.name,
					targetEpoch: tc.targetEpoch,
				},
				mkSnapshot(tc.targetEpoch),
			)
			require.NoError(t, err)

			stored, err := db.Metadata().GetPoolStakeSnapshotsByEpoch(
				tc.targetEpoch, "mark", nil,
			)
			require.NoError(t, err)
			require.Len(t, stored, 1)
			require.Equal(
				t, tc.wantResolved, stored[0].RewardAccountAutoVoteResolved,
				"RewardAccountAutoVoteResolved mismatch for %s", tc.name,
			)
			require.Equal(
				t, tc.wantAutoVote, stored[0].RewardAccountAutoVote,
				"RewardAccountAutoVote mismatch for %s", tc.name,
			)
		})
	}
}

// TestPersistImportedSnapshotMissingPoolsNotResolved verifies that when no
// pool rows exist in the DB (e.g. the fallback pool import has not yet run),
// the current-epoch snapshot is NOT falsely marked Resolved=true. This is the
// main correctness invariant from issue #2440: a missing pool row must not
// produce an authoritative Resolved=true, AutoVote=None entry.
func TestPersistImportedSnapshotMissingPoolsNotResolved(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	poolKeyHash := make([]byte, 28)
	poolKeyHash[0] = 0x11
	// Deliberately do NOT seed any Pool rows — simulating the state
	// before fallback pool import.

	snapshots := []*models.PoolStakeSnapshot{
		{
			Epoch:          102,
			SnapshotType:   "mark",
			PoolKeyHash:    poolKeyHash,
			TotalStake:     100,
			DelegatorCount: 1,
			CapturedSlot:   55,
		},
	}

	err = persistImportedSnapshot(
		ImportConfig{
			Database: db,
			State: &RawLedgerState{
				Epoch:      102,
				EpochNonce: []byte{0x01},
			},
		},
		999,
		snapshotImportTarget{
			name:        "mark",
			targetEpoch: 102,
		},
		snapshots,
	)
	require.NoError(t, err)

	stored, err := db.Metadata().GetPoolStakeSnapshotsByEpoch(102, "mark", nil)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	require.False(t, stored[0].RewardAccountAutoVoteResolved,
		"pool row absent: must NOT be falsely resolved")
	require.Equal(t, models.PoolRewardAccountAutoVoteNone, stored[0].RewardAccountAutoVote)
}

// TestPersistImportedSnapshotPoolPresentAccountStates exercises the
// current-epoch resolver's three account outcomes for issue #2440:
//   - reward account row absent entirely → Resolved=false (data may not be
//     imported yet; must not be persisted as a false confirmed None);
//   - reward account present but inactive (deregistered) → Resolved=true,
//     AutoVote=None (CIP-1694 treats unregistered reward accounts as
//     implicit no, but it is a confirmed outcome);
//   - reward account present and active, delegated to AlwaysNoConfidence →
//     Resolved=true, AutoVote=NoConfidence.
func TestPersistImportedSnapshotPoolPresentAccountStates(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	store, ok := db.Metadata().(*sqliteplugin.MetadataStoreSqlite)
	require.True(t, ok, "test requires the sqlite metadata backend")

	poolAbsent := bytes.Repeat([]byte{0x60}, 28)   // pool present, account absent
	poolInactive := bytes.Repeat([]byte{0x61}, 28) // pool present, account inactive
	poolNoConf := bytes.Repeat([]byte{0x62}, 28)   // pool present, account active NoConf
	rewardAbsent := bytes.Repeat([]byte{0x70}, 28)
	rewardInactive := bytes.Repeat([]byte{0x71}, 28)
	rewardNoConf := bytes.Repeat([]byte{0x72}, 28)

	// All three pools exist in the DB. Only two of their reward accounts
	// have account rows; poolAbsent's reward account has none.
	for _, p := range []struct {
		pool   []byte
		reward []byte
	}{
		{poolAbsent, rewardAbsent},
		{poolInactive, rewardInactive},
		{poolNoConf, rewardNoConf},
	} {
		require.NoError(t, store.DB().Create(&models.Pool{
			PoolKeyHash:   p.pool,
			RewardAccount: p.reward,
		}).Error)
	}
	// Inactive (deregistered) account that still carries an Always* flag.
	inactive := models.Account{
		StakingKey: rewardInactive,
		DrepType:   models.DrepTypeAlwaysAbstain,
		AddedSlot:  1,
		Active:     true,
	}
	require.NoError(t, store.DB().Create(&inactive).Error)
	require.NoError(t, store.DB().Model(&inactive).Update("active", false).Error)
	// Active account delegated to AlwaysNoConfidence.
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: rewardNoConf,
		DrepType:   models.DrepTypeAlwaysNoConfidence,
		AddedSlot:  1,
		Active:     true,
	}).Error)

	cases := []struct {
		name         string
		pool         []byte
		wantResolved bool
		wantAutoVote uint8
	}{
		{"account_absent", poolAbsent, false, models.PoolRewardAccountAutoVoteNone},
		{"account_inactive", poolInactive, true, models.PoolRewardAccountAutoVoteNone},
		{"account_active_noconfidence", poolNoConf, true, models.PoolRewardAccountAutoVoteNoConfidence},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snaps := []*models.PoolStakeSnapshot{{
				Epoch:          102,
				SnapshotType:   "mark",
				PoolKeyHash:    tc.pool,
				TotalStake:     100,
				DelegatorCount: 1,
				CapturedSlot:   55,
			}}
			require.NoError(t, persistImportedSnapshot(
				ImportConfig{
					Database: db,
					State: &RawLedgerState{
						Epoch:      102,
						EpochNonce: []byte{0x01},
					},
				},
				999,
				snapshotImportTarget{name: "mark", targetEpoch: 102},
				snaps,
			))

			stored, err := db.Metadata().GetPoolStakeSnapshotsByEpoch(102, "mark", nil)
			require.NoError(t, err)
			var row *models.PoolStakeSnapshot
			for i := range stored {
				if bytes.Equal(stored[i].PoolKeyHash, tc.pool) {
					row = stored[i]
					break
				}
			}
			require.NotNil(t, row)
			require.Equal(t, tc.wantResolved, row.RewardAccountAutoVoteResolved,
				"RewardAccountAutoVoteResolved mismatch for %s", tc.name)
			require.Equal(t, tc.wantAutoVote, row.RewardAccountAutoVote,
				"RewardAccountAutoVote mismatch for %s", tc.name)
		})
	}
}

// TestPersistImportedSnapshotHistoricalLeftUnresolved verifies that historical
// N-1/N-2 imported rows (target epoch != import epoch) are left
// RewardAccountAutoVoteResolved=false even when the snapshot bundle carries
// pool params and a live reward account delegates to an Always* DRep.
//
// Faithful historical resolution would need the reward account's DRep
// delegation AS OF the historical boundary, which is not recoverable after a
// Mithril restore. Freezing live DRep state onto a historical boundary could
// persist a value that was changed after the boundary, so the row is left
// unresolved and the tally treats it as implicit no.
func TestPersistImportedSnapshotHistoricalLeftUnresolved(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	poolKeyHash := bytes.Repeat([]byte{0x20}, 28)
	rewardAccount := bytes.Repeat([]byte{0x30}, 28)

	store, ok := db.Metadata().(*sqliteplugin.MetadataStoreSqlite)
	require.True(t, ok, "test requires the sqlite metadata backend")
	// Live account delegates to AlwaysAbstain. If historical resolution
	// (incorrectly) used live state, the row would become Abstain/resolved.
	require.NoError(t, store.DB().Create(&models.Pool{
		PoolKeyHash:   poolKeyHash,
		RewardAccount: rewardAccount,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: rewardAccount,
		DrepType:   models.DrepTypeAlwaysAbstain,
		AddedSlot:  1,
		Active:     true,
	}).Error)

	targetEpoch := uint64(101) // N-1, import epoch is 102
	snaps := []*models.PoolStakeSnapshot{{
		Epoch:          targetEpoch,
		SnapshotType:   "mark",
		PoolKeyHash:    poolKeyHash,
		TotalStake:     100,
		DelegatorCount: 1,
		CapturedSlot:   55,
	}}
	err = persistImportedSnapshot(
		ImportConfig{
			Database: db,
			State: &RawLedgerState{
				Epoch:      102,
				EpochNonce: []byte{0x01},
			},
		},
		999,
		snapshotImportTarget{
			name:        "set",
			targetEpoch: targetEpoch,
			snap: &ParsedSnapShot{
				PoolParams: map[string]*ParsedPool{
					string(poolKeyHash): {
						PoolKeyHash:   poolKeyHash,
						RewardAccount: rewardAccount,
					},
				},
			},
		},
		snaps,
	)
	require.NoError(t, err)

	stored, err := db.Metadata().GetPoolStakeSnapshotsByEpoch(
		targetEpoch, "mark", nil,
	)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	require.False(t, stored[0].RewardAccountAutoVoteResolved,
		"historical N-1 row must remain unresolved (no historical DRep state)")
	require.Equal(t, models.PoolRewardAccountAutoVoteNone, stored[0].RewardAccountAutoVote)
}

// TestCollectPoolsFromSnapshotsMarkWins verifies that when a pool appears in
// more than one snapshot (Mark/Set/Go) with different reward accounts, the
// Mark-era params win. The fallback import feeds the current-epoch auto-vote
// resolver, which must read the current (Mark) reward account.
func TestCollectPoolsFromSnapshotsMarkWins(t *testing.T) {
	poolKeyHash := bytes.Repeat([]byte{0x42}, 28)
	markReward := bytes.Repeat([]byte{0x01}, 28)
	goReward := bytes.Repeat([]byte{0x02}, 28)

	snapshots := &ParsedSnapShots{
		Mark: ParsedSnapShot{
			PoolParams: map[string]*ParsedPool{
				string(poolKeyHash): {
					PoolKeyHash:   poolKeyHash,
					RewardAccount: markReward,
				},
			},
		},
		Go: ParsedSnapShot{
			PoolParams: map[string]*ParsedPool{
				string(poolKeyHash): {
					PoolKeyHash:   poolKeyHash,
					RewardAccount: goReward,
				},
			},
		},
	}

	pools := collectPoolsFromSnapshots(snapshots)
	require.Len(t, pools, 1)
	require.Equal(t, markReward, pools[0].RewardAccount,
		"Mark-era reward account must take precedence over Go-era")
}

// TestImportSnapShotsFallbackPoolsResolveCurrentEpoch verifies the end-to-end
// fix from issue #2440: when pools come from the snapshot-pool fallback path
// (importPools runs before persistImportedSnapshot), the current-epoch snapshot
// is correctly resolved rather than left with a false Resolved=true, AutoVote=None.
func TestImportSnapShotsFallbackPoolsResolveCurrentEpoch(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	poolKeyHash := bytes.Repeat([]byte{0x42}, 28)
	rewardAccount := bytes.Repeat([]byte{0x43}, 28)

	store, ok := db.Metadata().(*sqliteplugin.MetadataStoreSqlite)
	require.True(t, ok, "test requires the sqlite metadata backend")
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: rewardAccount,
		DrepType:   models.DrepTypeAlwaysAbstain,
		AddedSlot:  1,
		Active:     true,
	}).Error)

	cfg := ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
	}
	// Simulate the fallback pool import that now runs BEFORE snapshot
	// processing in importSnapShots.
	require.NoError(t, importPools(
		context.Background(),
		cfg,
		[]ParsedPool{{
			PoolKeyHash:   poolKeyHash,
			RewardAccount: rewardAccount,
			VrfKeyHash:    bytes.Repeat([]byte{0x44}, 32),
			MarginDen:     1,
		}},
		999,
	))

	// Now call persistImportedSnapshot for the current epoch,
	// which should find the pool row and correctly resolve auto-votes.
	poolSnapshots := []*models.PoolStakeSnapshot{{
		Epoch:          102,
		SnapshotType:   "mark",
		PoolKeyHash:    poolKeyHash,
		TotalStake:     5_000_000,
		DelegatorCount: 1,
		CapturedSlot:   999,
	}}
	require.NoError(t, persistImportedSnapshot(
		ImportConfig{
			Database: db,
			State: &RawLedgerState{
				Epoch:      102,
				EpochNonce: []byte{0x01},
			},
		},
		999,
		snapshotImportTarget{name: "mark", targetEpoch: 102},
		poolSnapshots,
	))

	stored, err := db.Metadata().GetPoolStakeSnapshotsByEpoch(102, "mark", nil)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	require.True(t, stored[0].RewardAccountAutoVoteResolved,
		"current-epoch snapshot must be resolved after pool fallback import")
	require.Equal(t, models.PoolRewardAccountAutoVoteAbstain, stored[0].RewardAccountAutoVote,
		"AlwaysAbstain reward account must produce Abstain auto-vote")
}

func TestImportPParamsAnchorsAddedSlotToCurrentEpochStart(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	pparamsCbor, err := cbor.Encode(testConwayPParams())
	require.NoError(t, err)

	cfg := ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
		State: &RawLedgerState{
			PParamsData:   pparamsCbor,
			Epoch:         1277,
			EraIndex:      EraConway,
			EraBoundEpoch: 1200,
			EraBoundSlot:  10_000,
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 100, nil
		},
	}

	require.NoError(t, importPParams(context.Background(), cfg))

	pparams, err := db.Metadata().GetPParams(1277, EraConway, nil)
	require.NoError(t, err)
	require.Len(t, pparams, 1)
	require.Equal(t, uint64(17_700), pparams[0].AddedSlot)
}

func TestImportAccountsPreservesCredentialTag(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	stakeKey := bytes.Repeat([]byte{0xA4}, 28)
	cfg := ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
	}

	require.NoError(t, importAccounts(
		context.Background(),
		cfg,
		[]ParsedAccount{
			{
				StakingKey: Credential{
					Type: CredentialTypeKey,
					Hash: stakeKey,
				},
				Reward: 1,
				Active: true,
			},
			{
				StakingKey: Credential{
					Type: CredentialTypeScript,
					Hash: stakeKey,
				},
				Reward: 2,
				Active: true,
			},
		},
		123,
	))

	keyAcct, err := db.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint8(0), keyAcct.CredentialTag)
	require.Equal(t, types.Uint64(1), keyAcct.Reward)

	scriptAcct, err := db.GetAccountByCredential(1, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint8(1), scriptAcct.CredentialTag)
	require.Equal(t, types.Uint64(2), scriptAcct.Reward)
}

// TestImportPoolsPreservesRewardAccountCredentialTag verifies snapshot
// pool import stores reward account tags on Pool and PoolRegistration.
func TestImportPoolsPreservesRewardAccountCredentialTag(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	poolKeyHash := bytes.Repeat([]byte{0x51}, 28)
	vrfKeyHash := bytes.Repeat([]byte{0x52}, 32)
	rewardAccount := bytes.Repeat([]byte{0x53}, 28)
	cfg := ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
	}

	require.NoError(t, importPools(
		context.Background(),
		cfg,
		[]ParsedPool{
			{
				PoolKeyHash:                poolKeyHash,
				VrfKeyHash:                 vrfKeyHash,
				RewardAccount:              rewardAccount,
				RewardAccountCredentialTag: 1,
				MarginDen:                  1,
			},
		},
		456,
	))

	store, ok := db.Metadata().(*sqliteplugin.MetadataStoreSqlite)
	require.True(t, ok, "test requires the sqlite metadata backend")

	var pool models.Pool
	require.NoError(t, store.DB().
		Where("pool_key_hash = ?", poolKeyHash).
		First(&pool).Error)
	require.Equal(t, rewardAccount, []byte(pool.RewardAccount))
	require.Equal(t, uint8(1), pool.RewardAccountCredentialTag)

	var registration models.PoolRegistration
	require.NoError(t, store.DB().
		Where("pool_key_hash = ?", poolKeyHash).
		First(&registration).Error)
	require.Equal(t, rewardAccount, []byte(registration.RewardAccount))
	require.Equal(t, uint8(1), registration.RewardAccountCredentialTag)
}

func TestImportGovStateAnchorsProposalAndConstitutionSlots(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	txHash := bytes.Repeat([]byte{0x91}, 32)
	cfg := ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
		State: &RawLedgerState{
			GovStateData:  testGovStateData(t, txHash, 1275),
			Epoch:         1277,
			EraIndex:      EraConway,
			EraBoundEpoch: 1200,
			EraBoundSlot:  10_000,
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 100, nil
		},
	}

	require.NoError(t, importGovState(
		context.Background(),
		cfg,
		func(ImportProgress) {},
	))

	proposal, err := db.Metadata().GetGovernanceProposal(
		txHash,
		0,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, proposal)
	// Proposal AddedSlot is anchored to the proposal's original epoch
	// (ProposedIn=1275 → 10_000 + (1275-1200)*100 = 17_500), not the
	// snapshot's current epoch, so older proposals retain their original
	// slot for rollback/pruning purposes.
	require.Equal(t, uint64(17_500), proposal.AddedSlot)

	constitution, err := db.Metadata().GetConstitution(nil)
	require.NoError(t, err)
	require.NotNil(t, constitution)
	require.Equal(t, uint64(17_700), constitution.AddedSlot)
}

func TestSnapshotEpochAnchorSlotUsesMatchingEraBound(t *testing.T) {
	cfg := ImportConfig{
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
		State: &RawLedgerState{
			EraBounds: []EraBound{
				{Slot: 0, Epoch: 0},
				{Slot: 1_000, Epoch: 10},
				{Slot: 2_000, Epoch: 20},
			},
			EraIndex:      EraConway,
			EraBoundEpoch: 20,
			EraBoundSlot:  2_000,
		},
		EpochLength: func(eraId uint) (uint, uint, error) {
			switch eraId {
			case 0:
				return 1, 50, nil
			case 1:
				return 1, 100, nil
			default:
				return 1, 200, nil
			}
		},
	}

	require.Equal(t, uint64(1_500), snapshotEpochAnchorSlot(cfg, 15))
}

func TestSnapshotEpochAnchorSlotWarnsOnFallback(t *testing.T) {
	var logBuf bytes.Buffer
	cfg := ImportConfig{
		Logger: slog.New(
			slog.NewTextHandler(&logBuf, nil),
		),
		State: &RawLedgerState{
			EraBounds: []EraBound{
				{Slot: 1_000, Epoch: 10},
				{Slot: 2_000, Epoch: 20},
			},
			EraIndex:      EraConway,
			EraBoundEpoch: 20,
			EraBoundSlot:  2_000,
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 100, nil
		},
	}

	require.Zero(t, snapshotEpochAnchorSlot(cfg, 5))
	require.Contains(t, logBuf.String(), "snapshotEpochAnchorSlot")
	require.Contains(t, logBuf.String(), "epoch precedes first era bound")
	require.Contains(t, logBuf.String(), "epoch=5")
	require.Contains(t, logBuf.String(), "era_bound_epoch=20")
}

func TestSnapshotEpochAnchorSlotWarnsOnMissingEpochLength(t *testing.T) {
	var logBuf bytes.Buffer
	cfg := ImportConfig{
		Logger: slog.New(
			slog.NewTextHandler(&logBuf, nil),
		),
		State: &RawLedgerState{
			EraBounds: []EraBound{
				{Slot: 1_000, Epoch: 10},
			},
			EraIndex:      EraConway,
			EraBoundEpoch: 10,
			EraBoundSlot:  1_000,
		},
	}

	require.Equal(t, uint64(1_000), snapshotEpochAnchorSlot(cfg, 12))
	require.Contains(t, logBuf.String(), "snapshotEpochAnchorSlot")
	require.Contains(t, logBuf.String(), "epoch length unavailable")
	require.Contains(t, logBuf.String(), "epoch=12")
}

func TestSnapshotEpochAnchorSlotWarnsOnEpochLengthError(t *testing.T) {
	var logBuf bytes.Buffer
	cfg := ImportConfig{
		Logger: slog.New(
			slog.NewTextHandler(&logBuf, nil),
		),
		State: &RawLedgerState{
			EraBounds: []EraBound{
				{Slot: 0, Epoch: 0},
			},
			EraIndex:      EraConway,
			EraBoundEpoch: 0,
			EraBoundSlot:  0,
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 0, 0, errors.New("boom")
		},
	}

	require.Zero(t, snapshotEpochAnchorSlot(cfg, 3))
	require.Contains(t, logBuf.String(), "snapshotEpochAnchorSlot")
	require.Contains(t, logBuf.String(), "failed to resolve epoch length")
	require.Contains(t, logBuf.String(), "boom")
}

func testGovStateData(
	t *testing.T,
	txHash []byte,
	proposedEpoch uint64,
) []byte {
	t.Helper()

	proposal := []any{
		[]any{txHash, uint64(0)},
		map[uint64]uint64{},
		map[uint64]uint64{},
		map[uint64]uint64{},
		[]any{
			uint64(100_000_000),
			bytes.Repeat([]byte{0xa1}, 29),
			[]any{uint8(2)},
			[]any{
				"https://example.com/proposal",
				bytes.Repeat([]byte{0xb2}, 32),
			},
		},
		proposedEpoch,
		proposedEpoch + 5,
	}
	govState := []any{
		[]any{[]any{}, []any{proposal}},
		[]any{},
		[]any{
			[]any{
				"https://example.com/constitution",
				bytes.Repeat([]byte{0xc3}, 32),
			},
			nil,
		},
	}
	data, err := cbor.Encode(govState)
	require.NoError(t, err)
	return data
}
