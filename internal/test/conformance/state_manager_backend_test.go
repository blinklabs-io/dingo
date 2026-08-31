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

package conformance

import (
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
	"github.com/stretchr/testify/require"
)

// testHash28 builds a deterministic, distinguishable-by-seed 28-byte hash
// value (the size of a Blake2b224 credential/pool/DRep hash) for tests that
// need a well-formed but otherwise arbitrary identity.
func testHash28(seed byte) common.Blake2b224 {
	return common.NewBlake2b224(bytes.Repeat([]byte{seed}, 28))
}

// testHash32 builds a deterministic 32-byte hash value (the size of a
// transaction id) for tests that need a well-formed but otherwise
// arbitrary transaction identity.
func testHash32(seed byte) []byte {
	return bytes.Repeat([]byte{seed}, 32)
}

// TestDingoStateManagerRestartSurvivesReopen proves the audit's "after
// restart" acceptance bullet: state committed by a DingoStateManager
// backed by a real (file-based) sqlite store is still visible after that
// manager is closed and a new one is opened against the same on-disk data
// directory -- not just visible within the process that wrote it.
func TestDingoStateManagerRestartSurvivesReopen(t *testing.T) {
	dataDir := t.TempDir()

	m1, err := newDingoStateManagerAt(dataDir)
	require.NoError(t, err)

	pp := &conway.ConwayProtocolParameters{}
	require.NoError(t, m1.LoadInitialState(
		&conformance.ParsedInitialState{CurrentEpoch: 0},
		pp,
	))

	cred := common.Credential{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xaa),
	}
	tx, err := syntheticTransaction(
		"restart-stake-registration",
		[]common.Certificate{
			&common.StakeRegistrationCertificate{
				CertType:        uint(common.CertificateTypeStakeRegistration),
				StakeCredential: cred,
			},
		},
	)
	require.NoError(t, err)
	require.NoError(t, m1.ApplyTransaction(tx, 100))

	require.NoError(t, m1.Close())

	m2, err := newDingoStateManagerAt(dataDir)
	require.NoError(t, err)
	defer func() { require.NoError(t, m2.Close()) }()

	provider := m2.GetStateProvider()
	require.True(
		t,
		provider.IsStakeCredentialRegistered(cred),
		"stake registration committed by m1 must be visible after reopening the same data directory in m2",
	)
}

// TestDingoStateManagerRollbackDiscardsWrites proves the audit's rollback
// acceptance bullet: a write made inside a real database transaction that
// is rolled back is not visible via a subsequent, fresh (independent) read
// -- not just absent from some in-memory mirror.
func TestDingoStateManagerRollbackDiscardsWrites(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	cred := testHash28(0xbb)

	txn := m.db.Transaction(true)
	defer txn.Release()
	account := &models.Account{
		StakingKey:    cred[:],
		CredentialTag: 0,
		Active:        true,
	}
	require.NoError(t, m.db.CreateAccount(txn, account))
	require.NoError(t, txn.Rollback())

	got, err := m.db.GetAccountByCredential(0, cred[:], false, nil)
	require.ErrorIs(t, err, models.ErrAccountNotFound)
	require.Nil(t, got)
}

// TestDRepDelegationReadsRealBackendNotGovStateMirror proves the audit's
// "backend bypass" finding is fixed: DRepDelegation must read the real
// account.drep column through the backend, not the govState pre-validation
// mirror, so a backend that never persists or returns account.drep
// correctly cannot hide behind a mirror that happens to agree. Following
// the reviewer's own probe, this stores one delegation only in govState
// (the mirror) and a different delegation only in the real backend, then
// asserts DRepDelegation returns the backend's value.
func TestDRepDelegationReadsRealBackendNotGovStateMirror(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	cred := common.Credential{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0x31),
	}

	// Mirror-only delegation: a script-hash DRep the real backend never
	// sees.
	mirrorDrepCredential := testHash28(0x32)
	m.govState.SetDRepDelegation(cred, common.Drep{
		Type:       common.DrepTypeScriptHash,
		Credential: mirrorDrepCredential[:],
	})

	// Backend-only delegation: always-abstain, written directly to
	// account.drep_type/account.drep, disagreeing with the mirror above.
	require.NoError(t, m.db.CreateAccount(nil, &models.Account{
		StakingKey:    cred.Credential[:],
		CredentialTag: 0,
		DrepType:      models.DrepTypeAlwaysAbstain,
		Active:        true,
	}))

	provider := NewDingoStateProvider(m)
	delegation, err := provider.DRepDelegation(cred)
	require.NoError(t, err)
	require.NotNil(t, delegation)
	require.Equal(
		t,
		int(models.DrepTypeAlwaysAbstain),
		delegation.Type,
		"DRepDelegation must return the real backend's delegation, not the govState mirror's",
	)
	require.Empty(t, delegation.Credential)
}

// TestCommitteeMemberReadsRealBackendNotGovStateMirror proves the audit's
// "backend bypass" finding class also applied to committee members:
// CommitteeMember/CommitteeMembers must never fall back to
// govState.CommitteeMembers for a member the real backend doesn't have --
// that map holds the same initial/enacted set LoadInitialState and
// enactProposal write to the real backend (see both functions' doc
// comments), so a real backend that drops or never persists a
// committee_member row correctly could still pass every vector here if
// either method fell back to the mirror for a "missing" member instead of
// reporting it absent. This stores a member only in govState -- exactly the
// shape LoadInitialState always leaves behind once the harness reads it,
// but with no corresponding real committee_member row, simulating a
// backend that dropped it -- and asserts CommitteeMember reports it absent
// and CommitteeMembers excludes it, rather than serving the mirror's copy.
func TestCommitteeMemberReadsRealBackendNotGovStateMirror(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	coldKey := testHash28(0x51)

	// Mirror-only member: the real backend never received a matching
	// committee_member row for this credential.
	m.govState.CommitteeMembers[coldKey] = &conformance.CommitteeMemberInfo{
		ColdKey:     coldKey,
		ExpiryEpoch: 999,
	}

	provider := NewDingoStateProvider(m)

	member, err := provider.CommitteeMember(coldKey)
	require.NoError(t, err)
	require.Nil(
		t,
		member,
		"CommitteeMember must not serve the govState mirror's copy of a member the real backend doesn't have",
	)

	members, err := provider.CommitteeMembers()
	require.NoError(t, err)
	require.Empty(
		t,
		members,
		"CommitteeMembers must not include the govState mirror's copy of a member the real backend doesn't have",
	)
}

// TestCommitteeMemberResignationClearsHotKey proves an authorization that is
// superseded by a later resignation is not exposed as active by either
// committee-member provider method. A still-later authorization restores the
// hot key and clears the resigned state.
func TestCommitteeMemberResignationClearsHotKey(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	coldKey := testHash28(0x52)
	hotKey := testHash28(0x53)
	require.NoError(t, m.LoadInitialState(
		&conformance.ParsedInitialState{
			CommitteeMembers: map[common.Blake2b224]uint64{coldKey: 999},
		},
		&conway.ConwayProtocolParameters{},
	))

	applyCert := func(slot uint64, seed string, cert common.Certificate) {
		tx, err := syntheticTransaction(seed, []common.Certificate{cert})
		require.NoError(t, err)
		require.NoError(t, m.ApplyTransaction(tx, slot))
	}
	authorization := func() *common.AuthCommitteeHotCertificate {
		return &common.AuthCommitteeHotCertificate{
			CertType: uint(common.CertificateTypeAuthCommitteeHot),
			ColdCredential: common.Credential{
				CredType:   common.CredentialTypeAddrKeyHash,
				Credential: coldKey,
			},
			HotCredential: common.Credential{
				CredType:   common.CredentialTypeAddrKeyHash,
				Credential: hotKey,
			},
		}
	}

	applyCert(1, "committee-authorize", authorization())
	applyCert(2, "committee-resign", &common.ResignCommitteeColdCertificate{
		CertType: uint(common.CertificateTypeResignCommitteeCold),
		ColdCredential: common.Credential{
			CredType:   common.CredentialTypeAddrKeyHash,
			Credential: coldKey,
		},
	})

	provider := NewDingoStateProvider(m)
	assertState := func(wantResigned bool, wantHotKey *common.Blake2b224) {
		member, err := provider.CommitteeMember(coldKey)
		require.NoError(t, err)
		require.NotNil(t, member)
		require.Equal(t, wantResigned, member.Resigned)
		require.Equal(t, wantHotKey, member.HotKey)

		members, err := provider.CommitteeMembers()
		require.NoError(t, err)
		require.Len(t, members, 1)
		require.Equal(t, wantResigned, members[0].Resigned)
		require.Equal(t, wantHotKey, members[0].HotKey)
	}

	assertState(true, nil)

	applyCert(3, "committee-reauthorize", authorization())
	assertState(false, &hotKey)
}

// TestPoolCurrentStatePendingRetirement proves PoolCurrentState's pending
// retirement epoch tracks the pool's latest retirement certificate by
// insertion order (AddedSlot), not the maximum epoch value across every
// retirement row on the pool: a later retirement certificate replaces the
// prior schedule even when it targets an earlier epoch, and a later pool
// registration cancels a pending retirement entirely -- matching
// ledger.LedgerView.PoolCurrentState's own fix for the same defect (see
// ledger/view_test.go's TestLedgerViewPoolCurrentStatePendingRetirement).
func TestPoolCurrentStatePendingRetirement(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	poolKeyHash := common.PoolKeyHash(testHash28(0x61))

	registrationCert := func() *common.PoolRegistrationCertificate {
		return &common.PoolRegistrationCertificate{
			CertType:   uint(common.CertificateTypePoolRegistration),
			Operator:   poolKeyHash,
			VrfKeyHash: common.VrfKeyHash(testHash32(0x62)[:32]),
		}
	}
	retirementCert := func(epoch uint64) *common.PoolRetirementCertificate {
		return &common.PoolRetirementCertificate{
			CertType:    uint(common.CertificateTypePoolRetirement),
			PoolKeyHash: poolKeyHash,
			Epoch:       epoch,
		}
	}
	applyCert := func(slot uint64, seed string, cert common.Certificate) {
		tx, err := syntheticTransaction(seed, []common.Certificate{cert})
		require.NoError(t, err)
		require.NoError(t, m.ApplyTransaction(tx, slot))
	}

	applyCert(1, "pool-register-initial", registrationCert())

	// A retirement targeting epoch 10, then a later retirement targeting an
	// EARLIER epoch (5): the later certificate must win regardless of its
	// epoch value being smaller than the one it replaces.
	applyCert(2, "pool-retire-10", retirementCert(10))
	applyCert(3, "pool-retire-5", retirementCert(5))

	provider := NewDingoStateProvider(m)
	_, pendingEpoch, err := provider.PoolCurrentState(poolKeyHash)
	require.NoError(t, err)
	require.NotNil(t, pendingEpoch)
	require.Equal(
		t,
		uint64(5),
		*pendingEpoch,
		"a later retirement certificate must replace the prior schedule even when it moves the target epoch earlier",
	)

	// A later re-registration cancels the pending retirement entirely.
	applyCert(4, "pool-reregister", registrationCert())

	_, pendingEpoch, err = provider.PoolCurrentState(poolKeyHash)
	require.NoError(t, err)
	require.Nil(
		t,
		pendingEpoch,
		"a later pool registration must cancel a pending retirement",
	)
}

// TestDRepRegistrationPropagatesBackendErrors proves DRepRegistration
// returns a real backend error instead of swallowing it as "not
// registered": only models.ErrDrepNotFound (a real "no such row" result)
// should continue the credential-tag loop and end in (nil, nil); any other
// error (a dropped connection, a query failure) must be returned, or a
// vector could pass by having every backend failure look identical to "no
// DRep."
func TestDRepRegistrationPropagatesBackendErrors(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	require.NoError(t, m.Close())

	provider := NewDingoStateProvider(m)
	_, err = provider.DRepRegistration(testHash28(0x41))
	require.Error(
		t,
		err,
		"DRepRegistration must surface a real backend error, not report it as an absent registration",
	)
}

// TestProcessEpochAgainstRealBackend drives the epoch-boundary path end to
// end against a real DingoStateManager backend: the real
// governance.ProcessEpoch orchestration (not exercised by the per-vector
// harness path -- see ProcessEpochBoundary's doc comment in
// state_manager.go for why) and the real ledger/snapshot.Manager capture
// that actually writes PoolStakeSnapshot rows (governance.ProcessEpoch
// itself does not). It asserts the resulting stake-snapshot row exists via
// a real metadata.StakeSnapshotStore read.
func TestProcessEpochAgainstRealBackend(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	// Seed a persisted epoch-0 row: both governance.ProcessEpoch's callers
	// in production and the snapshot calculator resolve "what epoch is
	// this slot in" from the epoch table, matching
	// ledger/snapshot/calculator_test.go's seedEpochs pattern.
	require.NoError(t, m.db.SetEpoch(
		0, 0, nil, nil, nil, nil,
		eras.ConwayEraDesc.Id, 1, uint(conformanceSlotsPerEpoch), nil,
	))

	poolHash := testHash28(0xcc)
	stakingKey := testHash28(0xdd)
	require.NoError(t, m.db.ImportPool(
		nil,
		&models.Pool{PoolKeyHash: poolHash[:], VrfKeyHash: make([]byte, 32)},
		&models.PoolRegistration{
			PoolKeyHash: poolHash[:],
			VrfKeyHash:  make([]byte, 32),
			AddedSlot:   0,
		},
	))
	require.NoError(t, m.db.CreateAccount(nil, &models.Account{
		StakingKey: stakingKey[:],
		Pool:       poolHash[:],
		Active:     true,
	}))
	require.NoError(t, m.db.CreateUtxo(nil, &models.Utxo{
		TxId:       testHash32(0xee),
		OutputIdx:  0,
		StakingKey: stakingKey[:],
		Amount:     types.Uint64(40_000_000),
		AddedSlot:  0,
	}))

	pp := &conway.ConwayProtocolParameters{}
	m.protocolParams = pp
	m.currentEpoch = 0
	boundarySlot := conformanceSlotsPerEpoch

	txn := m.db.Transaction(true)
	defer txn.Release()

	_, err = governance.ProcessEpoch(&governance.EpochInput{
		DB:           m.db,
		Txn:          txn,
		PrevEpoch:    0,
		NewEpoch:     1,
		BoundarySlot: boundarySlot,
		PParams:      pp,
		UpdateFn:     eras.ConwayEraDesc.PParamsUpdateFunc,
	})
	require.NoError(
		t,
		err,
		"drive the real governance epoch-boundary orchestration",
	)

	snapshotMgr := snapshot.NewManager(m.db, event.NewEventBus(nil, nil), nil)
	evt := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        1,
		BoundarySlot:    boundarySlot,
		EpochNonce:      []byte{0x01, 0x02},
		ProtocolVersion: 10,
		SnapshotSlot:    boundarySlot - 1,
	}
	require.NoError(
		t,
		snapshotMgr.ComputeEpochBoundarySnapshot(
			context.Background(),
			txn,
			evt,
		),
	)
	require.NoError(
		t,
		snapshotMgr.CaptureEpochBoundarySnapshot(
			context.Background(),
			txn,
			evt,
		),
	)
	require.NoError(t, txn.Commit())

	poolSnapshot, err := m.db.Metadata().GetPoolStakeSnapshot(
		1, models.PoolStakeSnapshotTypeMark, poolHash[:], nil,
	)
	require.NoError(t, err)
	require.NotNil(
		t,
		poolSnapshot,
		"epoch-boundary capture must persist a real PoolStakeSnapshot row",
	)
	require.Equal(t, uint64(40_000_000), uint64(poolSnapshot.TotalStake))
}
