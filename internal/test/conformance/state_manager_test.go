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
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func TestLoadInitialStatePreservesTypedDRepRegistrations(t *testing.T) {
	t.Parallel()
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	hash := testHash28(0xd1)
	key := mockledger.RewardAccountKey{
		CredType: common.CredentialTypeAddrKeyHash, Credential: hash,
	}
	script := mockledger.RewardAccountKey{
		CredType: common.CredentialTypeScriptHash, Credential: hash,
	}
	pp := &conway.ConwayProtocolParameters{DRepDeposit: 500_000_000}
	require.NoError(t, m.LoadInitialState(&conformance.ParsedInitialState{
		DRepRegistrations: []common.Blake2b224{hash},
		DRepDeposits: map[mockledger.RewardAccountKey]uint64{
			key: 400_000_000, script: 500_000_000,
		},
		DRepRegistrationsByCredential: map[mockledger.RewardAccountKey]bool{
			key: true, script: true,
		},
	}, pp))

	keyDRep, err := m.db.GetDrepByCredential(0, hash[:], false, nil)
	require.NoError(t, err)
	require.Equal(t, uint8(0), keyDRep.CredentialTag)
	scriptDRep, err := m.db.GetDrepByCredential(1, hash[:], false, nil)
	require.NoError(t, err)
	require.Equal(t, uint8(1), scriptDRep.CredentialTag)
	for _, credentialTag := range []uint8{0, 1} {
		deposit, err := m.db.GetDrepLastRegistrationDeposit(
			credentialTag,
			hash[:],
			nil,
		)
		require.NoError(t, err)
		require.NotNil(t, deposit)
		want := uint64(500_000_000)
		if credentialTag == 0 {
			want = 400_000_000
		}
		require.Equal(t, want, *deposit)
	}

	dreps, err := m.db.GetActiveDreps(nil)
	require.NoError(t, err)
	require.Len(t, dreps, 2)
}

func TestLoadInitialStateLegacyDRepUsesRecordedCredential(t *testing.T) {
	t.Parallel()
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	hash := testHash28(0xd4)
	script := mockledger.RewardAccountKey{
		CredType: common.CredentialTypeScriptHash, Credential: hash,
	}
	const recordedDeposit = uint64(400_000_000)
	pp := &conway.ConwayProtocolParameters{DRepDeposit: 500_000_000}
	require.NoError(t, m.LoadInitialState(&conformance.ParsedInitialState{
		DRepRegistrations: []common.Blake2b224{hash},
		DRepDeposits: map[mockledger.RewardAccountKey]uint64{
			script: recordedDeposit,
		},
	}, pp))

	drep, err := m.db.GetDrepByCredential(1, hash[:], false, nil)
	require.NoError(t, err)
	require.NotNil(t, drep)
	deposit, err := m.db.GetDrepLastRegistrationDeposit(1, hash[:], nil)
	require.NoError(t, err)
	require.NotNil(t, deposit)
	require.Equal(t, recordedDeposit, *deposit)
}

func TestLoadInitialStateSkipsInactiveDRepsWithoutDeposit(t *testing.T) {
	t.Parallel()
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	hash := testHash28(0xd3)
	credential := mockledger.RewardAccountKey{
		CredType: common.CredentialTypeAddrKeyHash, Credential: hash,
	}
	require.NoError(t, m.LoadInitialState(&conformance.ParsedInitialState{
		DRepRegistrationsByCredential: map[mockledger.RewardAccountKey]bool{
			credential: false,
		},
	}, &babbage.BabbageProtocolParameters{}))

	dreps, err := m.db.GetActiveDreps(nil)
	require.NoError(t, err)
	require.Empty(t, dreps)
}

func TestDRepDeregistrationPreservesOtherCredentialType(t *testing.T) {
	t.Parallel()
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	hash := testHash28(0xd2)
	key := common.Credential{
		CredType: common.CredentialTypeAddrKeyHash, Credential: hash,
	}
	script := common.Credential{
		CredType: common.CredentialTypeScriptHash, Credential: hash,
	}
	m.govState.RegisterDRepCredentialUntil(key, 10)
	m.govState.RegisterDRepCredentialUntil(script, 10)
	m.updateGovStateForCertificate(&common.DeregistrationDrepCertificate{
		CertType:       uint(common.CertificateTypeDeregistrationDrep),
		DrepCredential: script,
	})

	require.True(t, m.govState.IsDRepCredentialRegistered(key))
	require.False(t, m.govState.IsDRepCredentialRegistered(script))
}

func TestPersistRatificationStoresEpochAndBoundarySlot(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	txHash := testHash32(0xa1)
	proposal := &models.GovernanceProposal{
		TxHash:        txHash,
		ActionIndex:   0,
		ActionType:    uint8(common.GovActionTypeInfo),
		ProposedEpoch: 1,
		ExpiresEpoch:  10,
		AnchorURL:     "https://example.invalid/ratification-pair",
		AnchorHash:    testHash32(0xa2),
		ReturnAddress: bytes.Repeat([]byte{0xa3}, 29),
		GovActionCbor: []byte{0x80},
		AddedSlot:     1,
	}
	require.NoError(t, m.db.SetGovernanceProposal(proposal, nil))

	const (
		ratifiedEpoch = uint64(4)
		boundarySlot  = uint64(400)
	)
	txn := m.db.Transaction(true)
	defer txn.Release()
	require.NoError(t, m.persistRatification(
		txn,
		hex.EncodeToString(txHash)+"#0",
		ratifiedEpoch,
		boundarySlot,
	))
	require.NoError(t, txn.Commit())

	stored, err := m.db.GetGovernanceProposal(txHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, stored.RatifiedEpoch)
	require.NotNil(t, stored.RatifiedSlot)
	require.Equal(t, ratifiedEpoch, *stored.RatifiedEpoch)
	require.Equal(t, boundarySlot, *stored.RatifiedSlot)
}

func TestProposalToModelStoresRatificationPair(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	ratifiedEpoch := uint64(4)
	model := m.proposalToModel(
		hex.EncodeToString(testHash32(0xb1))+"#0",
		conformance.GovActionInfo{
			ActionType:     common.GovActionTypeInfo,
			ExpiresAfter:   10,
			RatifiedEpoch:  &ratifiedEpoch,
			SubmittedEpoch: 1,
		},
	)
	require.NotNil(t, model.RatifiedEpoch)
	require.NotNil(t, model.RatifiedSlot)
	require.Equal(
		t,
		ratifiedEpoch*conformanceSlotsPerEpoch,
		*model.RatifiedSlot,
	)
}

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

func TestCommitteeMemberReadsPendingUpdateCommitteeProposal(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	coldKey := testHash28(0x51)
	coldCredential := common.Credential{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: coldKey,
	}
	action, err := common.NewUpdateCommitteeGovAction(
		nil,
		nil,
		map[*common.Credential]uint64{&coldCredential: 999},
		cbor.Rat{Rat: big.NewRat(2, 3)},
	)
	require.NoError(t, err)
	encoded, err := cbor.Encode(action)
	require.NoError(t, err)
	m.protocolParams = &conway.ConwayProtocolParameters{}
	require.NoError(t, m.db.SetGovernanceProposal(
		&models.GovernanceProposal{
			TxHash:        testHash32(0x50),
			ActionType:    uint8(common.GovActionTypeUpdateCommittee),
			ExpiresEpoch:  1000,
			GovActionCbor: encoded,
			AnchorURL:     "https://example.invalid/pending-committee",
			AnchorHash:    testHash32(0x52),
			ReturnAddress: bytes.Repeat([]byte{0x53}, 29),
		},
		nil,
	))

	provider := NewDingoStateProvider(m)
	member, err := provider.CommitteeMember(coldKey)
	require.NoError(t, err)
	require.NotNil(t, member)
	require.Equal(t, coldKey, member.ColdKey)
	require.Equal(t, uint64(999), member.ExpiryEpoch)
	require.Nil(t, member.HotKey)
	require.False(t, member.Resigned)

	members, err := provider.CommitteeMembers()
	require.NoError(t, err)
	require.Empty(t, members, "pending members are not seated members")
}

// TestCommitteeMemberResignationClearsHotKey proves an authorization that is
// superseded by a later resignation is not exposed as active by either
// committee-member provider method. A still-later authorization cannot clear
// the permanent resignation.
func TestCommitteeMemberResignationPermanentlyClearsHotKey(t *testing.T) {
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
	assertState(true, nil)
}

func TestCommitteeHotCredentialSelectionUsesActiveMember(t *testing.T) {
	tests := []struct {
		name       string
		expiries   []uint64
		wantMember bool
	}{
		{
			name:       "shared credential with seated member",
			expiries:   []uint64{5, 6},
			wantMember: true,
		},
		{
			// Term expiry is deliberately not applied on this path, matching
			// LedgerView.CommitteeHotCredentialMember. The Conway GOV rule
			// resolves a committee voter against the authorization map, which
			// excludes only resigned members, and applies expiry later in the
			// RATIFY tally and the committeeMinSize active count. Resigned
			// exclusion is covered by
			// TestLedgerViewCommitteeHotCredentialSelection, since
			// ParsedInitialState cannot express a resignation.
			name:       "expired member still authorizes",
			expiries:   []uint64{4},
			wantMember: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m, err := NewDingoStateManager()
			require.NoError(t, err)
			defer func() { require.NoError(t, m.Close()) }()

			hot := testHash28(0x73)
			state := &conformance.ParsedInitialState{
				CurrentEpoch:     5,
				CommitteeMembers: make(map[common.Blake2b224]uint64),
				HotKeyAuthorizations: make(
					map[common.Blake2b224]common.Blake2b224,
				),
			}
			for i, expiry := range test.expiries {
				cold := testHash28(byte(0x74 + i))
				state.CommitteeMembers[cold] = expiry
				state.HotKeyAuthorizations[cold] = hot
			}
			require.NoError(t, m.LoadInitialState(
				state,
				&conway.ConwayProtocolParameters{},
			))

			member, err := NewDingoStateProvider(
				m,
			).CommitteeHotCredentialMember(
				common.Credential{
					CredType:   common.CredentialTypeAddrKeyHash,
					Credential: hot,
				},
			)
			require.NoError(t, err)
			if test.wantMember {
				require.NotNil(
					t,
					member,
					"a seated matching member must authorize the hot credential",
				)
			} else {
				require.Nil(
					t,
					member,
					"a non-authorizing member must not resolve",
				)
			}
		})
	}
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
	_, err = provider.DRepRegistration(common.Credential{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0x41),
	})
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

// unreachableThreshold and trivialThreshold isolate one side of a
// committeeActionRatified decision at a time: an unreachable committee
// threshold proves a pass could only have come from the MotionNoConfidence
// path, and a trivial (zero) pool threshold auto-approves the SPO side so a
// test can exercise the DRep side alone.
var (
	unreachableThreshold = cbor.Rat{Rat: big.NewRat(999999, 1000000)}
	trivialThreshold     = cbor.Rat{Rat: big.NewRat(0, 1)}
)

// noConfidenceCommitteeParams builds Conway protocol parameters with an
// easy-to-clear MotionNoConfidence DRep threshold and a CommitteeNormal/
// CommitteeNoConfidence DRep threshold no real stake distribution could
// ever clear, so a test can tell which threshold committeeActionRatified
// actually applied from the pass/fail outcome alone. Pool thresholds are
// all trivial so every test here isolates the DRep side.
func noConfidenceCommitteeParams() *conway.ConwayProtocolParameters {
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: common.ProtocolParametersProtocolVersion{Major: 10},
		DRepVotingThresholds: conway.DRepVotingThresholds{
			MotionNoConfidence:    cbor.Rat{Rat: big.NewRat(1, 2)},
			CommitteeNormal:       unreachableThreshold,
			CommitteeNoConfidence: unreachableThreshold,
		},
		PoolVotingThresholds: conway.PoolVotingThresholds{
			MotionNoConfidence:    trivialThreshold,
			CommitteeNormal:       trivialThreshold,
			CommitteeNoConfidence: trivialThreshold,
		},
	}
}

// TestCommitteeActionRatifiedNoConfidenceUsesMotionThresholdAndImplicitYes
// verifies that a NoConfidence proposal whose only backing is a
// DRep delegated AlwaysNoConfidence (no proposal carries an explicit vote at
// all) ratifies only if committeeActionRatified (a) judges NoConfidence
// against DRepVotingThresholds.MotionNoConfidence rather than
// CommitteeNormal/CommitteeNoConfidence, and (b) counts that
// AlwaysNoConfidence-delegated stake as a yes vote, not just a denominator
// contribution. Reverting either half of the fix flips this to false: this
// test fails against the pre-fix (fad9390e-era) code, which shared the
// unreachable committee threshold and denominator-only accounting between
// both action types.
func TestCommitteeActionRatifiedNoConfidenceUsesMotionThresholdAndImplicitYes(
	t *testing.T,
) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	m.protocolParams = noConfidenceCommitteeParams()

	credential := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xd1),
	}
	m.govState.DRepDelegationsByCredential[credential] = common.Drep{
		Type: common.DrepTypeNoConfidence,
	}
	m.govState.RewardAccountBalances[credential] = 1_000_000

	proposal := &conformance.ProposalState{
		GovActionInfo: conformance.GovActionInfo{
			ActionType: common.GovActionTypeNoConfidence,
			Votes:      map[string]uint8{},
		},
	}

	txn := m.db.Transaction(false)
	defer txn.Release()

	ratified, err := m.committeeActionRatified(txn, proposal, 5)
	require.NoError(t, err)
	require.True(
		t,
		ratified,
		"NoConfidence must ratify off MotionNoConfidence plus the "+
			"AlwaysNoConfidence implicit yes vote",
	)
}

// TestCommitteeActionRatifiedUpdateCommitteeKeepsNoConfidenceDenominatorOnly
// is the companion negative case: the identical AlwaysNoConfidence
// delegation and stake, but for an UpdateCommittee proposal, must NOT
// ratify. cardano-ledger only grants AlwaysNoConfidence an automatic yes on
// an actual NoConfidence action; on UpdateCommittee that delegated stake
// belongs in the denominator alone, so with no other voters the yes ratio
// is 0 and a reachable (1/2) CommitteeNoConfidence threshold is not met.
// This guards against a fix that stops discriminating the two action types
// in the other direction (treating every action as if it were
// NoConfidence).
func TestCommitteeActionRatifiedUpdateCommitteeKeepsNoConfidenceDenominatorOnly(
	t *testing.T,
) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	m.protocolParams = &conway.ConwayProtocolParameters{
		ProtocolVersion: common.ProtocolParametersProtocolVersion{Major: 10},
		DRepVotingThresholds: conway.DRepVotingThresholds{
			MotionNoConfidence:    trivialThreshold,
			CommitteeNormal:       cbor.Rat{Rat: big.NewRat(1, 2)},
			CommitteeNoConfidence: cbor.Rat{Rat: big.NewRat(1, 2)},
		},
		PoolVotingThresholds: conway.PoolVotingThresholds{
			MotionNoConfidence:    trivialThreshold,
			CommitteeNormal:       trivialThreshold,
			CommitteeNoConfidence: trivialThreshold,
		},
	}

	credential := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xd2),
	}
	m.govState.DRepDelegationsByCredential[credential] = common.Drep{
		Type: common.DrepTypeNoConfidence,
	}
	m.govState.RewardAccountBalances[credential] = 1_000_000

	proposal := &conformance.ProposalState{
		GovActionInfo: conformance.GovActionInfo{
			ActionType: common.GovActionTypeUpdateCommittee,
			Votes:      map[string]uint8{},
		},
	}

	txn := m.db.Transaction(false)
	defer txn.Release()

	ratified, err := m.committeeActionRatified(txn, proposal, 5)
	require.NoError(t, err)
	require.False(
		t,
		ratified,
		"UpdateCommittee must not grant AlwaysNoConfidence delegation an "+
			"implicit yes vote",
	)
}

// TestProcessEpochBoundaryRatifiesUpdateCommitteeWithoutCommitteeVote closes
// the gap a PR review found in the two tests above: both call
// committeeActionRatified directly, so neither one proves ratifyProposals
// actually routes UpdateCommittee/NoConfidence proposals to it. Reverting
// just that routing while
// keeping committeeActionRatified and both direct-call tests left the whole
// package green, including those two tests -- nothing exercised the
// decision of *which* ratification path a real proposal takes.
//
// This test drives the real entry point, ProcessEpochBoundary, the way the
// harness calls it for every vector: a DRep and an SPO each cast an
// explicit yes vote (the exact shape "CC re-election" vector
// carries) and no committee vote is ever recorded. It only ratifies if
// ProcessEpochBoundary's call into ratifyProposals actually reaches
// committeeActionRatified for this action type; the old heuristic requires
// a committee yes-vote that never exists here, so this proposal stays
// stuck pending under it.
func TestProcessEpochBoundaryRatifiesUpdateCommitteeWithoutCommitteeVote(
	t *testing.T,
) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	reachable := cbor.Rat{Rat: big.NewRat(1, 2)}
	m.protocolParams = &conway.ConwayProtocolParameters{
		ProtocolVersion: common.ProtocolParametersProtocolVersion{Major: 10},
		DRepVotingThresholds: conway.DRepVotingThresholds{
			MotionNoConfidence:    reachable,
			CommitteeNormal:       reachable,
			CommitteeNoConfidence: reachable,
		},
		PoolVotingThresholds: conway.PoolVotingThresholds{
			MotionNoConfidence:    reachable,
			CommitteeNormal:       reachable,
			CommitteeNoConfidence: reachable,
		},
	}

	// One DRep, backed by real delegated stake, votes yes.
	drepCredentialHash := testHash28(0xe1)
	drepStakeCredential := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xe2),
	}
	m.govState.DRepDelegationsByCredential[drepStakeCredential] = common.Drep{
		Type:       common.DrepTypeAddrKeyHash,
		Credential: drepCredentialHash[:],
	}
	m.govState.DRepRegistrationsByCredential[mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: drepCredentialHash,
	}] = true
	m.govState.RewardAccountBalances[drepStakeCredential] = 1_000_000

	// One pool, backed by real delegated stake, votes yes.
	poolHash := testHash28(0xe3)
	poolStakeCredential := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xe4),
	}
	m.govState.PoolRegistrations[poolHash] = true
	m.govState.PoolDelegationsByCredential[poolStakeCredential] = poolHash
	m.govState.RewardAccountBalances[poolStakeCredential] = 1_000_000

	// Vote keys match the real format committeeActionRatified reads:
	// "<voter type digit>:<hex credential hash>" (see recordVotesInGovState).
	votes := map[string]uint8{
		formatVoteKey(common.VoterTypeDRepKeyHash, drepCredentialHash): 1,
		formatVoteKey(common.VoterTypeStakingPoolKeyHash, poolHash):    1,
	}

	const govActionID = "e5e5e5e5#0"
	m.govState.Proposals[govActionID] = &conformance.ProposalState{
		GovActionInfo: conformance.GovActionInfo{
			ActionType:     common.GovActionTypeUpdateCommittee,
			SubmittedEpoch: 0,
			ExpiresAfter:   10,
			Votes:          votes,
		},
	}

	require.NoError(t, m.ProcessEpochBoundary(1))

	ratified := m.govState.Proposals[govActionID].RatifiedEpoch
	require.NotNil(
		t,
		ratified,
		"an UpdateCommittee proposal with DRep+SPO yes votes and no "+
			"committee vote must ratify through the real "+
			"ProcessEpochBoundary/ratifyProposals path",
	)
}

// TestProcessEpochBoundaryRatifiesNoConfidenceWithoutCommitteeVote is
// TestProcessEpochBoundaryRatifiesUpdateCommitteeWithoutCommitteeVote's
// NoConfidence twin. A PR review found that the UpdateCommittee test alone
// only pins that half of ratifyProposals's routing: reverting just the
// NoConfidence arm back to the hasCC-requiring heuristic (leaving
// UpdateCommittee routed through committeeActionRatified) left every test,
// including both routing tests and both direct-call tests, green.
func TestProcessEpochBoundaryRatifiesNoConfidenceWithoutCommitteeVote(
	t *testing.T,
) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	reachable := cbor.Rat{Rat: big.NewRat(1, 2)}
	m.protocolParams = &conway.ConwayProtocolParameters{
		ProtocolVersion: common.ProtocolParametersProtocolVersion{Major: 10},
		DRepVotingThresholds: conway.DRepVotingThresholds{
			MotionNoConfidence:    reachable,
			CommitteeNormal:       reachable,
			CommitteeNoConfidence: reachable,
		},
		PoolVotingThresholds: conway.PoolVotingThresholds{
			MotionNoConfidence:    reachable,
			CommitteeNormal:       reachable,
			CommitteeNoConfidence: reachable,
		},
	}

	drepCredentialHash := testHash28(0xf1)
	drepStakeCredential := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xf2),
	}
	m.govState.DRepDelegationsByCredential[drepStakeCredential] = common.Drep{
		Type:       common.DrepTypeAddrKeyHash,
		Credential: drepCredentialHash[:],
	}
	m.govState.DRepRegistrationsByCredential[mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: drepCredentialHash,
	}] = true
	m.govState.RewardAccountBalances[drepStakeCredential] = 1_000_000

	poolHash := testHash28(0xf3)
	poolStakeCredential := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xf4),
	}
	m.govState.PoolRegistrations[poolHash] = true
	m.govState.PoolDelegationsByCredential[poolStakeCredential] = poolHash
	m.govState.RewardAccountBalances[poolStakeCredential] = 1_000_000

	votes := map[string]uint8{
		formatVoteKey(common.VoterTypeDRepKeyHash, drepCredentialHash): 1,
		formatVoteKey(common.VoterTypeStakingPoolKeyHash, poolHash):    1,
	}

	const govActionID = "f5f5f5f5#0"
	m.govState.Proposals[govActionID] = &conformance.ProposalState{
		GovActionInfo: conformance.GovActionInfo{
			ActionType:     common.GovActionTypeNoConfidence,
			SubmittedEpoch: 0,
			ExpiresAfter:   10,
			Votes:          votes,
		},
	}

	require.NoError(t, m.ProcessEpochBoundary(1))

	ratified := m.govState.Proposals[govActionID].RatifiedEpoch
	require.NotNil(
		t,
		ratified,
		"a NoConfidence proposal with DRep+SPO yes votes and no committee "+
			"vote must ratify through the real ProcessEpochBoundary/"+
			"ratifyProposals path",
	)
}

// TestProcessEpochBoundaryRatifiesNoConfidenceWithNoExplicitVotes pins a
// blocker a PR review found: ratifyProposals returned early on
// `len(proposal.Votes) == 0` before ever reaching the NoConfidence/
// UpdateCommittee branch, so a proposal backed only by an implicit
// AlwaysNoConfidence delegation -- no proposal.Votes entry at all, exactly
// TestCommitteeActionRatifiedNoConfidenceUsesMotionThresholdAndImplicitYes's
// state -- was silently skipped every epoch boundary and never ratified,
// even though committeeActionRatified alone (called directly) correctly
// says yes. Driving that same state through the real ProcessEpochBoundary
// entry point is what exposes the gap a direct call cannot.
func TestProcessEpochBoundaryRatifiesNoConfidenceWithNoExplicitVotes(
	t *testing.T,
) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	m.protocolParams = noConfidenceCommitteeParams()

	credential := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xfa),
	}
	m.govState.DRepDelegationsByCredential[credential] = common.Drep{
		Type: common.DrepTypeNoConfidence,
	}
	m.govState.RewardAccountBalances[credential] = 1_000_000

	const govActionID = "fbfbfbfb#0"
	m.govState.Proposals[govActionID] = &conformance.ProposalState{
		GovActionInfo: conformance.GovActionInfo{
			ActionType:     common.GovActionTypeNoConfidence,
			SubmittedEpoch: 0,
			ExpiresAfter:   10,
			Votes:          map[string]uint8{},
		},
	}

	require.NoError(t, m.ProcessEpochBoundary(1))

	require.NotNil(
		t,
		m.govState.Proposals[govActionID].RatifiedEpoch,
		"a NoConfidence proposal backed only by an implicit "+
			"AlwaysNoConfidence delegation, with no entries in "+
			"proposal.Votes at all, must still ratify through "+
			"ProcessEpochBoundary",
	)
}

// TestCommitteeActionRatifiedRefusesDuringConwayBootstrap pins the Conway
// bootstrap gate directly: the exact same DRep/SPO-backed NoConfidence
// setup that TestProcessEpochBoundaryRatifiesNoConfidenceWithoutCommitteeVote
// proves ratifies at protocol major 10 must NOT ratify at major 9, since
// ledger/governance's ShouldRatify refuses NoConfidence and UpdateCommittee
// outright during bootstrap regardless of votes. A PR review found this
// gate was added without any test pinning it: deleting it left every
// existing test green.
func TestCommitteeActionRatifiedRefusesDuringConwayBootstrap(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	pparamsAt := func(major uint) *conway.ConwayProtocolParameters {
		return &conway.ConwayProtocolParameters{
			ProtocolVersion: common.ProtocolParametersProtocolVersion{
				Major: major,
			},
			DRepVotingThresholds: conway.DRepVotingThresholds{
				MotionNoConfidence: cbor.Rat{Rat: big.NewRat(1, 2)},
			},
			// Trivial (zero): this test isolates the DRep side and the
			// bootstrap gate, not SPO stake -- no pool is set up below.
			PoolVotingThresholds: conway.PoolVotingThresholds{
				MotionNoConfidence: trivialThreshold,
			},
		}
	}

	credential := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xf6),
	}
	m.govState.DRepDelegationsByCredential[credential] = common.Drep{
		Type: common.DrepTypeNoConfidence,
	}
	m.govState.RewardAccountBalances[credential] = 1_000_000

	proposal := &conformance.ProposalState{
		GovActionInfo: conformance.GovActionInfo{
			ActionType: common.GovActionTypeNoConfidence,
			Votes:      map[string]uint8{},
		},
	}

	txn := m.db.Transaction(false)
	defer txn.Release()

	m.protocolParams = pparamsAt(9)
	ratifiedAtBootstrap, err := m.committeeActionRatified(txn, proposal, 5)
	require.NoError(t, err)
	require.False(
		t,
		ratifiedAtBootstrap,
		"protocol major 9 (Conway bootstrap) must refuse NoConfidence "+
			"ratification regardless of votes",
	)

	m.protocolParams = pparamsAt(10)
	ratifiedAfterBootstrap, err := m.committeeActionRatified(txn, proposal, 5)
	require.NoError(t, err)
	require.True(
		t,
		ratifiedAfterBootstrap,
		"the same state must ratify once past bootstrap (major 10), "+
			"proving major 9 alone caused the refusal above",
	)
}

// TestRatifyProposalsGatesTreasuryWithdrawalDuringConwayBootstrap pins the
// bootstrap gate a PR review asked for on ratifyProposals's vote-shape
// heuristic path: unlike UpdateCommittee/NoConfidence, TreasuryWithdrawal
// and NewConstitution have no stake tally to hand to ShouldRatify, so
// ratifyProposals must check inConwayBootstrap directly for them.
func TestRatifyProposalsGatesTreasuryWithdrawalDuringConwayBootstrap(
	t *testing.T,
) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	m.protocolParams = &conway.ConwayProtocolParameters{
		ProtocolVersion: common.ProtocolParametersProtocolVersion{Major: 9},
	}

	const govActionID = "f7f7f7f7#0"
	m.govState.Proposals[govActionID] = &conformance.ProposalState{
		GovActionInfo: conformance.GovActionInfo{
			ActionType:     common.GovActionTypeTreasuryWithdrawal,
			SubmittedEpoch: 0,
			ExpiresAfter:   10,
			Votes: map[string]uint8{
				formatVoteKey(
					common.VoterTypeConstitutionalCommitteeHotKeyHash,
					testHash28(0xf8),
				): 1,
				formatVoteKey(common.VoterTypeDRepKeyHash, testHash28(0xf9)): 1,
			},
		},
	}

	require.NoError(t, m.ProcessEpochBoundary(1))
	require.Nil(
		t,
		m.govState.Proposals[govActionID].RatifiedEpoch,
		"TreasuryWithdrawal must not ratify during Conway bootstrap "+
			"regardless of votes",
	)

	m.protocolParams = &conway.ConwayProtocolParameters{
		ProtocolVersion: common.ProtocolParametersProtocolVersion{Major: 10},
	}
	require.NoError(t, m.ProcessEpochBoundary(2))
	require.NotNil(
		t,
		m.govState.Proposals[govActionID].RatifiedEpoch,
		"the same votes must ratify once past bootstrap (major 10), "+
			"proving major 9 alone caused the refusal above",
	)
}

// TestCommitteeActionRatifiedRefusesUpdateCommitteeOverTermLimit pins
// committeeTermsWithinLimit end to end: a PR review found that
// proposal.ProposedMembersByCredential is empty at ratification in every
// vector the corpus and this file's other tests exercise, so
// syntheticUpdateCommitteeGovAction's loop over it never runs anywhere --
// the term-limit check ShouldRatify performs off that synthetic action has
// no coverage proving it can actually refuse. This constructs a proposed
// member whose expiry is far beyond CommitteeTermLimit and gives the
// proposal trivial (always-met) DRep/SPO thresholds, so the only thing that
// can block ratification is the term-limit check.
func TestCommitteeActionRatifiedRefusesUpdateCommitteeOverTermLimit(
	t *testing.T,
) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	m.protocolParams = &conway.ConwayProtocolParameters{
		ProtocolVersion:    common.ProtocolParametersProtocolVersion{Major: 10},
		CommitteeTermLimit: 5,
		DRepVotingThresholds: conway.DRepVotingThresholds{
			CommitteeNormal:       trivialThreshold,
			CommitteeNoConfidence: trivialThreshold,
		},
		PoolVotingThresholds: conway.PoolVotingThresholds{
			CommitteeNormal:       trivialThreshold,
			CommitteeNoConfidence: trivialThreshold,
		},
	}

	const currentEpoch = 5
	proposal := &conformance.ProposalState{
		GovActionInfo: conformance.GovActionInfo{
			ActionType: common.GovActionTypeUpdateCommittee,
			Votes:      map[string]uint8{},
			ProposedMembersByCredential: map[mockledger.RewardAccountKey]uint64{
				{
					CredType:   common.CredentialTypeAddrKeyHash,
					Credential: testHash28(0xfc),
				}: currentEpoch + 100, // 100 > CommitteeTermLimit (5)
			},
		},
	}

	txn := m.db.Transaction(false)
	defer txn.Release()

	ratified, err := m.committeeActionRatified(txn, proposal, currentEpoch)
	require.NoError(t, err)
	require.False(
		t,
		ratified,
		"an UpdateCommittee proposal whose member term exceeds "+
			"CommitteeTermLimit must not ratify even when DRep/SPO "+
			"thresholds are trivially met",
	)
}

// TestCommitteeActionRatifiedUsesPassedEpochNotManagerField pins the fix for
// an epoch-source inconsistency a PR review found unpinned: reverting
// drepStakeForCommitteeAction's IsDRepCredentialActive call back to
// m.currentEpoch left the whole package green, because every other test
// either sets m.currentEpoch to match the currentEpoch argument or never
// exercises a DRep whose active window depends on which of the two is used.
// This sets m.currentEpoch to 0 and passes a different currentEpoch (5) to
// committeeActionRatified, with a credential-backed DRep active only
// through epoch 3: using currentEpoch correctly excludes it (expired), so
// the proposal's only vote is gone and ratification must fail; using
// m.currentEpoch would wrongly count it as still active and ratify.
func TestCommitteeActionRatifiedUsesPassedEpochNotManagerField(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	m.protocolParams = &conway.ConwayProtocolParameters{
		ProtocolVersion: common.ProtocolParametersProtocolVersion{Major: 10},
		DRepVotingThresholds: conway.DRepVotingThresholds{
			MotionNoConfidence: cbor.Rat{Rat: big.NewRat(1, 2)},
		},
		PoolVotingThresholds: conway.PoolVotingThresholds{
			MotionNoConfidence: trivialThreshold,
		},
	}
	// m.currentEpoch is left at its zero value deliberately -- the real
	// path (ProcessEpochBoundary) always sets it to match the currentEpoch
	// argument, but a direct call (as every test in this file makes) can
	// exercise the two diverging, which is exactly what this test needs.

	drepCredentialHash := testHash28(0xfd)
	drepStakeCredential := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xfe),
	}
	m.govState.DRepDelegationsByCredential[drepStakeCredential] = common.Drep{
		Type:       common.DrepTypeAddrKeyHash,
		Credential: drepCredentialHash[:],
	}
	drepKey := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: drepCredentialHash,
	}
	m.govState.DRepRegistrationsByCredential[drepKey] = true
	m.govState.DRepExpiries[drepKey] = 3 // active only through epoch 3
	m.govState.RewardAccountBalances[drepStakeCredential] = 1_000_000

	proposal := &conformance.ProposalState{
		GovActionInfo: conformance.GovActionInfo{
			ActionType: common.GovActionTypeNoConfidence,
			Votes: map[string]uint8{
				formatVoteKey(common.VoterTypeDRepKeyHash, drepCredentialHash): 1,
			},
		},
	}

	txn := m.db.Transaction(false)
	defer txn.Release()

	ratified, err := m.committeeActionRatified(txn, proposal, 5)
	require.NoError(t, err)
	require.False(
		t,
		ratified,
		"a DRep whose registration expired at epoch 3 must be excluded "+
			"when committeeActionRatified is called with currentEpoch 5, "+
			"leaving no yes stake to ratify with",
	)
}

// formatVoteKey builds a GovActionInfo.Votes key exactly as
// recordVotesInGovState does: "<voter type digit>:<hex credential hash>".
func formatVoteKey(voterType uint8, credential common.Blake2b224) string {
	return string(rune('0'+voterType)) + ":" + hex.EncodeToString(credential[:])
}

// TestCommitteeActionRatifiedExcludesProposalDepositFromSPOStake pins a PR
// review finding: an active proposal deposit raises the return
// account's DRep voting power, but it is not delegated stake behind a pool
// and must not enter the SPO tally. Production reads SPO stake straight from
// the stake distribution snapshot (tallySPOVotes over LoadSPOVotingState's
// Dist), which carries no deposit adjustment.
//
// The stake is arranged so the deposit decides the outcome. The yes pool
// holds 2,000,000 and the silent (implicit no) pool holds 1,000,000, so the
// SPO ratio is 2/3 against a 1/2 threshold and the proposal ratifies. Route
// a 3,000,000 deposit to the silent pool's delegator and, if the SPO tally
// counted it, that pool would hold 4,000,000, dropping the ratio to 1/3 and
// refusing the proposal. Passing the deposit map back into
// spoStakeForCommitteeAction therefore fails this test.
func TestCommitteeActionRatifiedExcludesProposalDepositFromSPOStake(
	t *testing.T,
) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	reachable := cbor.Rat{Rat: big.NewRat(1, 2)}
	m.protocolParams = &conway.ConwayProtocolParameters{
		ProtocolVersion: common.ProtocolParametersProtocolVersion{Major: 10},
		DRepVotingThresholds: conway.DRepVotingThresholds{
			MotionNoConfidence:    reachable,
			CommitteeNormal:       reachable,
			CommitteeNoConfidence: reachable,
		},
		PoolVotingThresholds: conway.PoolVotingThresholds{
			MotionNoConfidence:    reachable,
			CommitteeNormal:       reachable,
			CommitteeNoConfidence: reachable,
		},
	}

	// DRep side: an AlwaysNoConfidence delegator is an implicit yes on a
	// NoConfidence action, so the DRep ratio is 1 and the decision turns on
	// the SPO side alone. This credential delegates to no pool, so it stays
	// out of the SPO tally entirely.
	drepStakeCredential := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xa1),
	}
	m.govState.DRepDelegationsByCredential[drepStakeCredential] = common.Drep{
		Type: common.DrepTypeNoConfidence,
	}
	m.govState.RewardAccountBalances[drepStakeCredential] = 1_000_000

	// Yes pool: 2,000,000 of delegated stake, voting yes explicitly.
	yesPoolHash := testHash28(0xa2)
	yesPoolDelegator := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xa3),
	}
	m.govState.PoolRegistrations[yesPoolHash] = true
	m.govState.PoolDelegationsByCredential[yesPoolDelegator] = yesPoolHash
	m.govState.RewardAccountBalances[yesPoolDelegator] = 2_000_000

	// Silent pool: 1,000,000 of delegated stake and no reward-account DRep
	// delegation, so it is an implicit no and contributes to the denominator.
	silentPoolHash := testHash28(0xa4)
	silentPoolDelegator := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xa5),
	}
	m.govState.PoolRegistrations[silentPoolHash] = true
	m.govState.PoolDelegationsByCredential[silentPoolDelegator] = silentPoolHash
	m.govState.RewardAccountBalances[silentPoolDelegator] = 1_000_000

	// An unrelated active proposal whose deposit is returned to the silent
	// pool's delegator. Large enough to invert the SPO ratio if counted.
	depositReturnAccount := silentPoolDelegator
	m.govState.Proposals["dddddddd#0"] = &conformance.ProposalState{
		GovActionInfo: conformance.GovActionInfo{
			ActionType:     common.GovActionTypeInfo,
			SubmittedEpoch: 0,
			ExpiresAfter:   10,
			Votes:          map[string]uint8{},
			Deposit:        3_000_000,
			ReturnAccount:  &depositReturnAccount,
		},
	}

	proposal := &conformance.ProposalState{
		GovActionInfo: conformance.GovActionInfo{
			ActionType:   common.GovActionTypeNoConfidence,
			ExpiresAfter: 10,
			Votes: map[string]uint8{
				formatVoteKey(
					common.VoterTypeStakingPoolKeyHash,
					yesPoolHash,
				): 1,
			},
		},
	}

	txn := m.db.Transaction(false)
	defer txn.Release()

	// The assertion deliberately goes through committeeActionRatified rather
	// than calling spoStakeForCommitteeAction directly: a direct call would
	// bind this test to that helper's signature, so restoring the deposit
	// argument would break the build instead of failing the assertion. Going
	// through the decision keeps the revert behavioural.
	ratified, err := m.committeeActionRatified(txn, proposal, 5)
	require.NoError(t, err)
	require.True(
		t,
		ratified,
		"SPO ratio is 2/3 against a 1/2 threshold once the proposal "+
			"deposit is excluded from pool stake",
	)
}

// gOuroboros common.CommitteeVotingState: CommitteeHotCredentialColdCredentials
// returns every cold credential currently authorizing the hot credential,
// seated or not, and omits only resigned ones.
func TestCommitteeHotCredentialColdCredentialsIncludesUnseatedAuthorization(
	t *testing.T,
) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	hot := testHash28(0x81)
	seatedCold := testHash28(0x82)
	require.NoError(t, m.LoadInitialState(
		&conformance.ParsedInitialState{
			CurrentEpoch:     5,
			CommitteeMembers: map[common.Blake2b224]uint64{seatedCold: 999},
			HotKeyAuthorizations: map[common.Blake2b224]common.Blake2b224{
				seatedCold: hot,
			},
		},
		&conway.ConwayProtocolParameters{},
	))
	keyCredential := func(hash common.Blake2b224) common.Credential {
		return common.Credential{
			CredType:   common.CredentialTypeAddrKeyHash,
			Credential: hash,
		}
	}
	pendingCold := testHash28(0x83)
	resignedCold := testHash28(0x84)
	persist := func(seed string, slot uint64, certs ...common.Certificate) {
		tx, err := syntheticTransaction(seed, certs)
		require.NoError(t, err)
		require.NoError(t, m.db.SetTransactionMetadataOnly(
			tx,
			ocommon.Point{Slot: slot, Hash: syntheticBlockHash(slot)},
			0,
			map[int]uint64{},
			nil,
		))
	}
	persist("pending-auth", 10,
		&common.AuthCommitteeHotCertificate{
			CertType:       uint(common.CertificateTypeAuthCommitteeHot),
			ColdCredential: keyCredential(pendingCold),
			HotCredential:  keyCredential(hot),
		},
		&common.AuthCommitteeHotCertificate{
			CertType:       uint(common.CertificateTypeAuthCommitteeHot),
			ColdCredential: keyCredential(resignedCold),
			HotCredential:  keyCredential(hot),
		},
	)
	persist("pending-resign", 11,
		&common.ResignCommitteeColdCertificate{
			CertType:       uint(common.CertificateTypeResignCommitteeCold),
			ColdCredential: keyCredential(resignedCold),
		},
	)

	provider := NewDingoStateProvider(m)
	coldCredentials, err := provider.CommitteeHotCredentialColdCredentials(
		keyCredential(hot),
	)
	require.NoError(t, err)
	require.ElementsMatch(
		t,
		[]common.Credential{
			keyCredential(seatedCold),
			keyCredential(pendingCold),
		},
		coldCredentials,
	)
	elected, err := provider.CommitteeCredentialIsElected(
		keyCredential(pendingCold),
	)
	require.NoError(t, err)
	require.False(t, elected)
	coldCredentials, err = provider.CommitteeHotCredentialColdCredentials(
		common.Credential{
			CredType:   common.CredentialTypeScriptHash,
			Credential: hot,
		},
	)
	require.NoError(t, err)
	require.Empty(t, coldCredentials)
}

// The harness mirrors LedgerView's committee windows: an unseated
// credential's authorization lasts until the next epoch boundary, as
// cardano-ledger's EPOCH updateCommitteeState drops it there.
func TestCommitteeHotCredentialMemberDropsUnseatedAuthorizationAtBoundary(
	t *testing.T,
) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	seatedCold := testHash28(0xa1)
	require.NoError(t, m.LoadInitialState(
		&conformance.ParsedInitialState{
			CurrentEpoch:     5,
			CommitteeMembers: map[common.Blake2b224]uint64{seatedCold: 999},
		},
		&conway.ConwayProtocolParameters{},
	))
	pending := common.Credential{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xa2),
	}
	hot := common.Credential{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xa3),
	}
	action, err := common.NewUpdateCommitteeGovAction(
		nil,
		nil,
		map[*common.Credential]uint64{&pending: 999},
		cbor.Rat{Rat: big.NewRat(2, 3)},
	)
	require.NoError(t, err)
	encoded, err := cbor.Encode(action)
	require.NoError(t, err)
	require.NoError(t, m.db.SetGovernanceProposal(
		&models.GovernanceProposal{
			TxHash:        testHash32(0xa4),
			ActionType:    uint8(common.GovActionTypeUpdateCommittee),
			ExpiresEpoch:  1000,
			GovActionCbor: encoded,
			AnchorHash:    testHash32(0xa5),
			ReturnAddress: bytes.Repeat([]byte{0xa6}, 29),
		},
		nil,
	))
	tx, err := syntheticTransaction(
		"pending-authorization",
		[]common.Certificate{&common.AuthCommitteeHotCertificate{
			CertType:       uint(common.CertificateTypeAuthCommitteeHot),
			ColdCredential: pending,
			HotCredential:  hot,
		}},
	)
	require.NoError(t, err)
	require.NoError(t, m.ApplyTransaction(tx, 10))

	provider := NewDingoStateProvider(m)
	member, err := provider.CommitteeHotCredentialMember(hot)
	require.NoError(t, err)
	require.NotNil(t, member, "the authorization holds for its own epoch")

	require.NoError(t, m.ProcessEpochBoundary(6))
	member, err = provider.CommitteeHotCredentialMember(hot)
	require.NoError(t, err)
	require.Nil(t, member, "the boundary drops an unseated authorization")
	coldMember, err := provider.CommitteeCredentialMember(pending)
	require.NoError(t, err)
	require.NotNil(t, coldMember, "the credential is still a potential member")
	require.Nil(t, coldMember.HotKey)
}

// The wrapper must keep satisfying everything the real provider satisfies,
// including the optional capability Dingo's Conway and Dijkstra validation
// type-asserts for. Losing it here would change what the entry point
// validates while still reporting as "routed".
var (
	_ common.LedgerState            = (*observedLedgerState)(nil)
	_ common.EpochState             = (*observedLedgerState)(nil)
	_ eras.CommitteeCredentialState = (*observedLedgerState)(nil)
)

// UtxoById records the input reference before delegating. The lookup is
// recorded even when it fails: an entry point that asked for an input it
// could not resolve still executed, which is what is being observed.
func (o *observedLedgerState) UtxoById(
	id common.TransactionInput,
) (common.Utxo, error) {
	o.reads++
	if id != nil {
		o.utxoLookups[utxoLookupKey(id)] = struct{}{}
	}
	return o.DingoStateProvider.UtxoById(id)
}

// NetworkId records the read performed by the network-id rules.
func (o *observedLedgerState) NetworkId() uint {
	o.reads++
	return o.DingoStateProvider.NetworkId()
}

// CostModels records the read performed by the script rules.
func (o *observedLedgerState) CostModels() map[common.PlutusLanguage]common.CostModel {
	o.reads++
	return o.DingoStateProvider.CostModels()
}

// entryPointCorpusDecodeEra is the era decodeVectorTransaction decodes as.
const entryPointCorpusDecodeEra = conway.EraNameConway

func TestConformanceApplyTransactionResetsDormancyBeforeCertificates(t *testing.T) {
	t.Parallel()
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	pparams := &conway.ConwayProtocolParameters{
		ProtocolVersion:         common.ProtocolParametersProtocolVersion{Major: 9},
		GovActionValidityPeriod: 20,
		DRepInactivityPeriod:    20,
	}
	m.protocolParams = pparams
	m.currentEpoch = 100
	drepCredential := testHash28(0xd7)
	require.NoError(t, m.db.SetImportedDormantDRepEpochs(3, nil))

	rewardHash := testHash28(0xd8)
	rewardAddress, err := common.NewAddressFromBytes(
		append([]byte{0xE1}, rewardHash[:]...),
	)
	require.NoError(t, err)
	var anchorHash [32]byte
	copy(anchorHash[:], testHash32(0xd9))
	proposal := conway.ConwayProposalProcedure{
		PPDeposit:       1,
		PPRewardAccount: rewardAddress,
		PPGovAction: conway.ConwayGovAction{
			Type:   uint(common.GovActionTypeInfo),
			Action: &common.InfoGovAction{Type: uint(common.GovActionTypeInfo)},
		},
		PPAnchor: common.GovAnchor{Url: "https://example.com/new", DataHash: anchorHash},
	}
	transaction := mockledger.NewTransactionBuilder()
	transaction.WithId(testHash32(0xda))
	transaction.WithType(int(conway.EraIdConway))
	transaction.WithValid(true)
	transaction.WithCertificates(&common.RegistrationDrepCertificate{
		CertType: uint(common.CertificateTypeRegistrationDrep),
		DrepCredential: common.Credential{
			CredType:   common.CredentialTypeAddrKeyHash,
			Credential: common.CredentialHash(drepCredential),
		},
		Amount: 500,
	})
	transaction.WithProposalProcedures(proposal)

	require.NoError(t, m.ApplyTransaction(transaction, 100))
	drep, err := m.db.GetDrepByCredential(0, drepCredential[:], true, nil)
	require.NoError(t, err)
	require.NotNil(t, drep)
	require.Equal(t, uint64(100), drep.LastActivityEpoch)
	require.Equal(t, uint64(120), drep.ExpiryEpoch)
	dormantEpochs, err := m.db.GetDormantDRepEpochs(nil)
	require.NoError(t, err)
	require.Zero(t, dormantEpochs)
}

// TestDingoStateManagerRollbackDiscardsWrites proves the audit's rollback
// acceptance bullet: a write made inside a real database transaction that
// is rolled back is not visible via a subsequent, fresh (independent) read
// -- not just absent from some in-memory mirror.
