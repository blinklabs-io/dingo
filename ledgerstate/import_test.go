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

	pparams, err := db.Metadata().GetPParams(1277, nil)
	require.NoError(t, err)
	require.Len(t, pparams, 1)
	require.Equal(t, uint64(17_700), pparams[0].AddedSlot)
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
