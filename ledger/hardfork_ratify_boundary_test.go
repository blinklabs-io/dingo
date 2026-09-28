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
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// hardForkRatifyFixture reproduces the exact Preview Plomin hard-fork
// incident (dingo#4441) at a real epoch-rollover level: a HardForkInitiation
// proposal, 49 SPO votes' worth of yes/no stake collapsed into two pools
// carrying the real observed mark[740]/mark[741]/mark[742] ratios
// (0.4779/0.4757/0.6283), and a single seated CC member voting yes with a
// 1/1 quorum -- matching the live incident's committee_member/
// auth_committee_hot state. Still at protocol major 9 (bootstrap), so the
// DRep gate is bypassed entirely and only the SPO gate governs ratification,
// exactly as it did on the real network.
type hardForkRatifyFixture struct {
	ls       *LedgerState
	db       *database.Database
	proposal *models.GovernanceProposal
	pparams  *conway.ConwayProtocolParameters
}

const (
	hfrYesPool    = "hfr-yes-pool-2222222222222"
	hfrSilentPool = "hfr-silent-pool-11111111111"
)

func newHardForkRatifyFixture(t *testing.T) *hardForkRatifyFixture {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	// currentEpoch is epoch 741, already on disk; the fixture's first
	// rollover call transitions the boundary into 742.
	currentEpoch := newTestEpoch(741, 74_100, 100, eras.ConwayEraDesc.Id)
	require.NoError(t, db.SetEpoch(
		currentEpoch.StartSlot,
		currentEpoch.EpochId,
		currentEpoch.Nonce,
		currentEpoch.EvolvingNonce,
		currentEpoch.CandidateNonce,
		currentEpoch.LastEpochBlockNonce,
		currentEpoch.EraId,
		currentEpoch.SlotLength,
		currentEpoch.LengthInSlots,
		nil,
	))

	// PV9: still bootstrap, the real incident's protocol version.
	pparams := donationTestConwayPParams(9)
	pparams.MinCommitteeSize = 1

	// mark[740]/mark[741]/mark[742], the exact ratios dingo#4441 measured on
	// Preview. Yes stake is hfrYesPool's explicit Yes vote; the remainder is
	// hfrSilentPool, which casts no vote at all -- HardForkInitiation always
	// keeps a silent pool's stake in the active denominator as implicit No
	// (tallySPOVotes), matching cardano-ledger's checkDisallowedVotes-adjacent
	// SPO semantics for this action type.
	for _, row := range []struct {
		epoch    uint64
		yesStake uint64
	}{
		{740, 4_779}, // ratio 0.4779, below the 0.51 threshold
		{741, 4_757}, // ratio 0.4757, below the 0.51 threshold
		{742, 6_283}, // ratio 0.6283, clears the 0.51 threshold
	} {
		require.NoError(t, db.Metadata().SavePoolStakeSnapshot(
			&models.PoolStakeSnapshot{
				Epoch:        row.epoch,
				SnapshotType: models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:  []byte(hfrYesPool),
				TotalStake:   types.Uint64(row.yesStake),
			},
			nil,
		))
		require.NoError(t, db.Metadata().SavePoolStakeSnapshot(
			&models.PoolStakeSnapshot{
				Epoch:        row.epoch,
				SnapshotType: models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:  []byte(hfrSilentPool),
				TotalStake:   types.Uint64(10_000 - row.yesStake),
			},
			nil,
		))
	}

	// Committee: one seated member, 1/1 quorum -- matches the live
	// incident's committee_member/auth_committee_hot state exactly (issue
	// text: "committee_member holds one seated member ... auth_committee_hot
	// maps it to ... the CC yes ratio is 1/1").
	coldCredential := repeatByte(28, 0xC1)
	hotCredential := repeatByte(28, 0xC2)
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{{
		ColdCredHash: coldCredential,
		ExpiresEpoch: 1000,
		AddedSlot:    1,
	}}, nil))
	require.NoError(t, db.SetCommitteeQuorum(big.NewRat(1, 1), 1, nil))
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO auth_committee_hot (
    cold_credential, host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?)`, coldCredential, hotCredential, 1, 1)
	require.NoError(t, err)

	action := &lcommon.HardForkInitiationGovAction{Type: 1}
	action.ProtocolVersion.Major = 10
	action.ProtocolVersion.Minor = 0
	actionCbor, err := cbor.Encode(action)
	require.NoError(t, err)

	proposal := &models.GovernanceProposal{
		TxHash:        repeatByte(32, 0x49),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
		ProposedEpoch: 737,
		ExpiresEpoch:  767,
		// Deposit 0 keeps ratificationEnactmentPrecondition's return-address
		// decode (which real proposals need) out of scope here -- this test
		// is about the SPO stake epoch selection, not deposit handling.
		Deposit:       0,
		ReturnAddress: repeatByte(29, 0),
		AnchorURL:     "https://example.invalid/plomin",
		AnchorHash:    repeatByte(32, 0x4a),
		GovActionCbor: actionCbor,
		AddedSlot:     1,
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))
	loaded, err := db.GetGovernanceProposal(proposal.TxHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, loaded)

	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      loaded.ID,
		VoterType:       models.VoterTypeCC,
		VoterCredential: hotCredential,
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))
	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      loaded.ID,
		VoterType:       models.VoterTypeSPO,
		VoterCredential: []byte(hfrYesPool),
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))

	cfg := newTestEraHistoryCfg(t)
	cfg.ShelleyGenesisHash = treasuryRolloverGenesisHash
	ls := &LedgerState{
		db:             db,
		currentEra:     eras.ConwayEraDesc,
		currentEpoch:   currentEpoch,
		currentPParams: pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	return &hardForkRatifyFixture{
		ls:       ls,
		db:       db,
		proposal: loaded,
		pparams:  pparams,
	}
}

func (f *hardForkRatifyFixture) rollover(
	t *testing.T,
	currentEpoch models.Epoch,
	pparams lcommon.ProtocolParameters,
) *EpochRolloverResult {
	t.Helper()
	var result *EpochRolloverResult
	txn := f.db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		var rolloverErr error
		result, rolloverErr = f.ls.processEpochRollover(
			txn,
			currentEpoch,
			eras.ConwayEraDesc,
			pparams,
			false,
		)
		return rolloverErr
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	return result
}

func (f *hardForkRatifyFixture) reloadProposal(
	t *testing.T,
) *models.GovernanceProposal {
	t.Helper()
	loaded, err := f.db.GetGovernanceProposal(
		f.proposal.TxHash, f.proposal.ActionIndex, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	return loaded
}

// TestHardForkInitiation_RatifiesAtRealIncidentBoundary reproduces
// dingo#4441: the Preview Plomin hard fork (protocol major 9 -> 10) must
// ratify at the boundary into epoch 742, using mark[742] (ratio 0.6283),
// not mark[740] (0.4779) -- reproducing the real network's
// ratified_epoch=742. Before the stakeEpochFor fix this proposal never
// ratifies at this boundary (it would need to wait until mark[742] became
// readable as mark[newEpoch-2], i.e. two epochs later than upstream, and in
// the live incident the node halted on a downstream PV9-bootstrap
// validation rule before ever reaching that point).
func TestHardForkInitiation_RatifiesAtRealIncidentBoundary(t *testing.T) {
	t.Parallel()

	f := newHardForkRatifyFixture(t)

	before := f.reloadProposal(t)
	require.Nil(t, before.RatifiedEpoch,
		"proposal must start unratified")

	result := f.rollover(t, f.ls.currentEpoch, f.pparams)
	require.Equal(t, uint64(742), result.NewCurrentEpoch.EpochId)

	after := f.reloadProposal(t)
	require.NotNil(t, after.RatifiedEpoch,
		"HardForkInitiation must ratify at the boundary into 742 using "+
			"mark[742] (0.6283 >= 0.51); mark[740] (0.4779) alone would "+
			"never clear the threshold")
	require.Equal(t, uint64(742), *after.RatifiedEpoch)
}

// TestHardForkInitiation_EnactsOneBoundaryAfterRatification extends the
// above through the enactment boundary, reproducing the real chain's
// ratified_epoch=742 / enacted_epoch=743 pair end to end: protocol major
// must read 10 only after the boundary into 743, not before.
func TestHardForkInitiation_EnactsOneBoundaryAfterRatification(t *testing.T) {
	t.Parallel()

	f := newHardForkRatifyFixture(t)

	ratifyResult := f.rollover(t, f.ls.currentEpoch, f.pparams)
	require.Equal(t, uint64(742), ratifyResult.NewCurrentEpoch.EpochId)
	ratified := f.reloadProposal(t)
	require.NotNil(t, ratified.RatifiedEpoch)
	require.Nil(t, ratified.EnactedEpoch,
		"ratification and enactment must land on different boundaries")

	enactResult := f.rollover(
		t, ratifyResult.NewCurrentEpoch, f.pparams,
	)
	require.Equal(t, uint64(743), enactResult.NewCurrentEpoch.EpochId)
	require.NotNil(t, enactResult.NewCurrentPParams)
	conwayParams, ok := enactResult.NewCurrentPParams.(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	require.Equal(t, uint(10), conwayParams.ProtocolVersion.Major,
		"protocol major must read 10 only after the boundary into 743, "+
			"matching the real network's enacted_epoch=743")

	enacted := f.reloadProposal(t)
	require.NotNil(t, enacted.EnactedEpoch)
	require.Equal(t, uint64(743), *enacted.EnactedEpoch)
}

// hardForkRatifyLiveStakeFixture is hardForkRatifyFixture's sibling for
// proving the *plumbing* half of dingo#4441, not the epoch-offset half: it
// seeds live Pool/Account/UTxO delegation state -- never a pre-written
// pool_stake_snapshot "mark" row -- and drives the same real
// processEpochRollover path through the same epoch-boundary hooks node.go
// wires in production. That is the only way governance's RATIFY phase can
// see mark[NewEpoch] at all: that row is written only at the very end of the
// same rollover transaction, after RATIFY has already run (see
// SetCurrentBoundarySPOStakeHook's doc comment).
type hardForkRatifyLiveStakeFixture struct {
	*hardForkRatifyFixture
	snapshotMgr *snapshot.Manager
}

const (
	hfrLiveYesPool    = "hfr-live-yes-pool-3333333333"
	hfrLiveSilentPool = "hfr-live-silent-pool-4444444"
)

// newHardForkRatifyLiveStakeFixture builds the same proposal/committee/vote
// state as newHardForkRatifyFixture, but backs the SPO stake with live
// Pool/Account/UTxO rows at the decisive 0.6283 ratio instead of a
// pre-written mark[742] row, and wires the authoritative persist hook
// unconditionally (see the comment below for why not the SNAP-point
// fast-path hook too). wireCurrentBoundaryHook controls only the new
// dingo#4441 hook (SetCurrentBoundarySPOStakeHook), so a test can wire
// everything else exactly like production and isolate what that one hook
// contributes.
func newHardForkRatifyLiveStakeFixture(
	t *testing.T,
	wireCurrentBoundaryHook bool,
) *hardForkRatifyLiveStakeFixture {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	currentEpoch := newTestEpoch(741, 74_100, 100, eras.ConwayEraDesc.Id)
	require.NoError(t, db.SetEpoch(
		currentEpoch.StartSlot,
		currentEpoch.EpochId,
		currentEpoch.Nonce,
		currentEpoch.EvolvingNonce,
		currentEpoch.CandidateNonce,
		currentEpoch.LastEpochBlockNonce,
		currentEpoch.EraId,
		currentEpoch.SlotLength,
		currentEpoch.LengthInSlots,
		nil,
	))

	pparams := donationTestConwayPParams(9)
	pparams.MinCommitteeSize = 1

	// Live delegation state at the decisive ratio (0.6283, mark[742] in the
	// real incident) -- no pool_stake_snapshot row exists for epoch 742 at
	// all. ImportPool + CreateAccount + CreateUtxo is exactly how a normal
	// block-processing run builds the stake the SNAP-point calculator reads.
	seedLiveDelegatedStake(t, db, hfrLiveYesPool, 6_283)
	seedLiveDelegatedStake(t, db, hfrLiveSilentPool, 3_717)

	coldCredential := repeatByte(28, 0xD1)
	hotCredential := repeatByte(28, 0xD2)
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{{
		ColdCredHash: coldCredential,
		ExpiresEpoch: 1000,
		AddedSlot:    1,
	}}, nil))
	require.NoError(t, db.SetCommitteeQuorum(big.NewRat(1, 1), 1, nil))
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO auth_committee_hot (
    cold_credential, host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?)`, coldCredential, hotCredential, 1, 1)
	require.NoError(t, err)

	action := &lcommon.HardForkInitiationGovAction{Type: 1}
	action.ProtocolVersion.Major = 10
	action.ProtocolVersion.Minor = 0
	actionCbor, err := cbor.Encode(action)
	require.NoError(t, err)

	proposal := &models.GovernanceProposal{
		TxHash:        repeatByte(32, 0x51),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
		ProposedEpoch: 737,
		ExpiresEpoch:  767,
		Deposit:       0,
		ReturnAddress: repeatByte(29, 0),
		AnchorURL:     "https://example.invalid/plomin-live",
		AnchorHash:    repeatByte(32, 0x52),
		GovActionCbor: actionCbor,
		AddedSlot:     1,
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))
	loaded, err := db.GetGovernanceProposal(proposal.TxHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, loaded)

	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      loaded.ID,
		VoterType:       models.VoterTypeCC,
		VoterCredential: hotCredential,
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))
	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      loaded.ID,
		VoterType:       models.VoterTypeSPO,
		VoterCredential: []byte(hfrLiveYesPool),
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))

	cfg := newTestEraHistoryCfg(t)
	cfg.ShelleyGenesisHash = treasuryRolloverGenesisHash
	ls := &LedgerState{
		db:             db,
		currentEra:     eras.ConwayEraDesc,
		currentEpoch:   currentEpoch,
		currentPParams: pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Wire the authoritative persist hook, matching production, but not the
	// SNAP-point fast-path stake hook: this synthetic fixture seeds
	// Pool/Account/Utxo rows directly rather than through real block
	// processing, so the live reward aggregate ComputeEpochBoundarySnapshot's
	// fast path reads is empty here even though the historical
	// reconstruction both CaptureEpochBoundarySnapshot and
	// CurrentBoundarySPOStakeRows fall back to is not. That fallback path is
	// exactly what a real node also uses whenever the fast path is unset or
	// fails (TestCaptureEpochBoundarySnapshotStakeHookFailureDeferred), so
	// this is still a realistic, supported configuration -- not a
	// fixture-only shortcut.
	snapshotMgr := snapshot.NewManager(db, event.NewEventBus(nil, nil), nil)
	ls.SetEpochBoundarySnapshotHook(
		func(txn *database.Txn, evt event.EpochTransitionEvent) error {
			return snapshotMgr.CaptureEpochBoundarySnapshot(
				context.Background(), txn, evt,
			)
		},
	)
	if wireCurrentBoundaryHook {
		ls.SetCurrentBoundarySPOStakeHook(
			func(
				txn *database.Txn,
				evt event.EpochTransitionEvent,
			) ([]*models.PoolStakeSnapshot, error) {
				return snapshotMgr.CurrentBoundarySPOStakeRows(
					context.Background(), txn, evt,
				)
			},
		)
	}

	return &hardForkRatifyLiveStakeFixture{
		hardForkRatifyFixture: &hardForkRatifyFixture{
			ls:       ls,
			db:       db,
			proposal: loaded,
			pparams:  pparams,
		},
		snapshotMgr: snapshotMgr,
	}
}

// seedLiveDelegatedStake registers a pool with one delegator holding amount
// (in whole ADA-equivalent units matching the fixture's 10_000-unit ratio
// scale) as live Pool/Account/UTxO state, matching how a normal sync
// populates the tables the SNAP-point calculator reads.
func seedLiveDelegatedStake(
	t *testing.T,
	db *database.Database,
	poolKeyHash string,
	amount uint64,
) {
	t.Helper()
	stakingKey := append([]byte(nil), []byte(poolKeyHash)...)
	for len(stakingKey) < 28 {
		stakingKey = append(stakingKey, 0)
	}
	stakingKey = stakingKey[:28]

	require.NoError(t, db.ImportPool(nil, &models.Pool{
		PoolKeyHash: []byte(poolKeyHash),
		VrfKeyHash:  make([]byte, 32),
		Pledge:      1_000_000,
		Cost:        340_000_000,
		Margin:      &types.Rat{Rat: big.NewRat(1, 100)},
	}, &models.PoolRegistration{
		PoolKeyHash: []byte(poolKeyHash),
		AddedSlot:   74_100,
		Pledge:      1_000_000,
		Cost:        340_000_000,
		Margin:      &types.Rat{Rat: big.NewRat(1, 100)},
		VrfKeyHash:  make([]byte, 32),
	}))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: stakingKey,
		Pool:       []byte(poolKeyHash),
		AddedSlot:  74_100,
		Active:     true,
	}))
	require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
		TxId:       repeatByte(32, poolKeyHash[len(poolKeyHash)-1]),
		OutputIdx:  0,
		StakingKey: stakingKey,
		Amount:     types.Uint64(amount),
		AddedSlot:  74_100,
	}))
}

// TestHardForkInitiation_RatifiesFromLiveStakeWithHookWired proves the
// production wiring (node.go's SetCurrentBoundarySPOStakeHook alongside the
// other two epoch-boundary hooks) makes RATIFY see mark[NewEpoch] from live
// state, with no pre-written pool_stake_snapshot row involved at all.
func TestHardForkInitiation_RatifiesFromLiveStakeWithHookWired(t *testing.T) {
	t.Parallel()

	f := newHardForkRatifyLiveStakeFixture(t, true)

	result := f.rollover(t, f.ls.currentEpoch, f.pparams)
	require.Equal(t, uint64(742), result.NewCurrentEpoch.EpochId)

	after := f.reloadProposal(t)
	require.NotNil(t, after.RatifiedEpoch,
		"wiring SetCurrentBoundarySPOStakeHook must let RATIFY see "+
			"mark[742] from live state and ratify at 0.6283 >= 0.51")
	require.Equal(t, uint64(742), *after.RatifiedEpoch)
}

// TestHardForkInitiation_NeverRatifiesWithoutCurrentBoundaryHook is the
// negative control for the plumbing half of dingo#4441: even after the
// stakeEpochFor offset fix, wiring only the pre-existing SNAP-point stake
// and capture hooks (exactly as before this change) leaves governance
// reading the not-yet-written mark[742] row and permanently unable to
// ratify -- worse than the original epoch-lag bug, not better. This is the
// scenario SetCurrentBoundarySPOStakeHook's doc comment warns a production
// node must never leave unwired.
func TestHardForkInitiation_NeverRatifiesWithoutCurrentBoundaryHook(
	t *testing.T,
) {
	t.Parallel()

	f := newHardForkRatifyLiveStakeFixture(t, false)

	result := f.rollover(t, f.ls.currentEpoch, f.pparams)
	require.Equal(t, uint64(742), result.NewCurrentEpoch.EpochId)

	after := f.reloadProposal(t)
	require.Nil(t, after.RatifiedEpoch,
		"without SetCurrentBoundarySPOStakeHook wired, governance falls "+
			"back to reading the not-yet-persisted mark[742] row and must "+
			"see zero SPO stake, never ratifying")
}
