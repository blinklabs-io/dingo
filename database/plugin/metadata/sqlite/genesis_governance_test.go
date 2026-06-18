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

package sqlite

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeCred(b byte) *lcommon.Credential {
	var h [28]byte
	for i := range h {
		h[i] = b
	}
	return &lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.CredentialHash(h),
	}
}

func makePoolId(b byte) lcommon.PoolId {
	var p [28]byte
	for i := range p {
		p[i] = b
	}
	return lcommon.PoolId(p)
}

func TestSqliteSetGenesisGovernance(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	drepCred := makeCred(0x11)
	anchor := &lcommon.GovAnchor{
		Url:      "https://example.com/drep.json",
		DataHash: [32]byte{0xab, 0xcd},
	}
	initialDReps := conway.ConwayGenesisInitialDReps{
		drepCred: conway.ConwayGenesisDRepState{
			Expiry:  100,
			Deposit: 500_000_000,
			Anchor:  anchor,
		},
	}

	stakeCred := makeCred(0x22)
	voteCred := makeCred(0x33)
	stakeVoteCred := makeCred(0x44)
	poolA := makePoolId(0xaa)
	poolB := makePoolId(0xbb)

	delegs := conway.ConwayGenesisDelegs{
		stakeCred: conway.ConwayGenesisDelegatee{
			Type:   conway.ConwayGenesisDelegateeTypeStake,
			PoolId: poolA,
		},
		voteCred: conway.ConwayGenesisDelegatee{
			Type: conway.ConwayGenesisDelegateeTypeVote,
			DRep: lcommon.Drep{
				Type:       lcommon.DrepTypeAddrKeyHash,
				Credential: drepCred.Credential[:],
			},
		},
		stakeVoteCred: conway.ConwayGenesisDelegatee{
			Type:   conway.ConwayGenesisDelegateeTypeStakeVote,
			PoolId: poolB,
			DRep: lcommon.Drep{
				Type: lcommon.DrepTypeAbstain,
			},
		},
	}

	require.NoError(t, store.SetGenesisGovernance(
		initialDReps,
		delegs,
		[]byte("genesis-hash"),
		nil,
	))

	// DRep row exists at slot 0 with anchor and expiry.
	var drep models.Drep
	require.NoError(
		t,
		store.DB().Where("credential = ?", drepCred.Credential[:]).
			First(&drep).Error,
	)
	assert.Equal(t, uint64(0), drep.AddedSlot)
	assert.Equal(t, uint64(100), drep.ExpiryEpoch)
	assert.Equal(t, "https://example.com/drep.json", drep.AnchorURL)
	assert.True(t, drep.Active)

	// Registration row recorded the deposit.
	var reg models.RegistrationDrep
	require.NoError(
		t,
		store.DB().Where("drep_credential = ?", drepCred.Credential[:]).
			First(&reg).Error,
	)
	assert.Equal(t, uint64(0), reg.AddedSlot)
	assert.Equal(t, types.Uint64(500_000_000), reg.DepositAmount)

	// Every genesis delegation account has a synthetic Registration
	// row at slot 0 so the rollback path treats it as registered.
	for _, cred := range [][]byte{
		stakeCred.Credential[:],
		voteCred.Credential[:],
		stakeVoteCred.Credential[:],
	} {
		var genReg models.Registration
		require.NoError(
			t,
			store.DB().Where(
				"staking_key = ? AND added_slot = ?", cred, uint64(0),
			).First(&genReg).Error,
			"missing genesis Registration for %x", cred,
		)
		assert.Equal(t, uint64(0), genReg.AddedSlot)
	}

	// Stake-only delegation: account.pool set, no DRep.
	var stakeAcct models.Account
	require.NoError(
		t,
		store.DB().Where("staking_key = ?", stakeCred.Credential[:]).
			First(&stakeAcct).Error,
	)
	assert.Equal(t, poolA[:], stakeAcct.Pool)
	assert.Nil(t, stakeAcct.Drep)
	var stakeRow models.StakeDelegation
	require.NoError(
		t,
		store.DB().Where("staking_key = ?", stakeCred.Credential[:]).
			First(&stakeRow).Error,
	)
	assert.Equal(t, poolA[:], stakeRow.PoolKeyHash)
	assert.Equal(t, uint64(0), stakeRow.AddedSlot)

	// Vote-only delegation: account.drep set, no pool.
	var voteAcct models.Account
	require.NoError(
		t,
		store.DB().Where("staking_key = ?", voteCred.Credential[:]).
			First(&voteAcct).Error,
	)
	assert.Nil(t, voteAcct.Pool)
	assert.Equal(t, drepCred.Credential[:], voteAcct.Drep)
	assert.Equal(t, models.DrepTypeAddrKeyHash, voteAcct.DrepType)
	var voteRow models.VoteDelegation
	require.NoError(
		t,
		store.DB().Where("staking_key = ?", voteCred.Credential[:]).
			First(&voteRow).Error,
	)
	assert.Equal(t, drepCred.Credential[:], voteRow.Drep)
	assert.Equal(t, uint64(0), voteRow.AddedSlot)

	// Combined stake+vote delegation, with an AlwaysAbstain predefined DRep
	// (no credential persisted).
	var stakeVoteAcct models.Account
	require.NoError(
		t,
		store.DB().Where("staking_key = ?", stakeVoteCred.Credential[:]).
			First(&stakeVoteAcct).Error,
	)
	assert.Equal(t, poolB[:], stakeVoteAcct.Pool)
	assert.Nil(t, stakeVoteAcct.Drep)
	assert.Equal(t, models.DrepTypeAlwaysAbstain, stakeVoteAcct.DrepType)
	var stakeVoteRow models.StakeVoteDelegation
	require.NoError(
		t,
		store.DB().Where("staking_key = ?", stakeVoteCred.Credential[:]).
			First(&stakeVoteRow).Error,
	)
	assert.Equal(t, poolB[:], stakeVoteRow.PoolKeyHash)
	assert.Nil(t, stakeVoteRow.Drep)
	assert.Equal(t, models.DrepTypeAlwaysAbstain, stakeVoteRow.DrepType)
}

func TestSqliteSetGenesisGovernanceIdempotent(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	drepCred := makeCred(0x55)
	stakeCred := makeCred(0x66)
	poolA := makePoolId(0x77)

	initialDReps := conway.ConwayGenesisInitialDReps{
		drepCred: conway.ConwayGenesisDRepState{
			Expiry:  100,
			Deposit: 500_000_000,
		},
	}
	delegs := conway.ConwayGenesisDelegs{
		stakeCred: conway.ConwayGenesisDelegatee{
			Type:   conway.ConwayGenesisDelegateeTypeStake,
			PoolId: poolA,
		},
	}

	// Run twice — simulates a retry after partial genesis bootstrap.
	require.NoError(t,
		store.SetGenesisGovernance(initialDReps, delegs, nil, nil),
	)
	require.NoError(t,
		store.SetGenesisGovernance(initialDReps, delegs, nil, nil),
	)

	var drepCount, regDrepCount, accountCount, delegCount, regCount int64
	require.NoError(t,
		store.DB().Model(&models.Drep{}).Count(&drepCount).Error,
	)
	require.NoError(t,
		store.DB().Model(&models.RegistrationDrep{}).Count(&regDrepCount).Error,
	)
	require.NoError(t,
		store.DB().Model(&models.Account{}).Count(&accountCount).Error,
	)
	require.NoError(t,
		store.DB().Model(&models.StakeDelegation{}).Count(&delegCount).Error,
	)
	require.NoError(t,
		store.DB().Model(&models.Registration{}).Count(&regCount).Error,
	)
	assert.Equal(t, int64(1), drepCount)
	assert.Equal(t, int64(1), regDrepCount)
	assert.Equal(t, int64(1), accountCount)
	assert.Equal(t, int64(1), delegCount)
	assert.Equal(t, int64(1), regCount)
}

func TestSqliteSetGenesisGovernanceEmpty(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	require.NoError(t, store.SetGenesisGovernance(
		conway.ConwayGenesisInitialDReps{},
		conway.ConwayGenesisDelegs{},
		nil,
		nil,
	))

	var count int64
	require.NoError(t, store.DB().Model(&models.Drep{}).Count(&count).Error)
	assert.Equal(t, int64(0), count)
	require.NoError(t, store.DB().Model(&models.Account{}).Count(&count).Error)
	assert.Equal(t, int64(0), count)
}

// TestSqliteRollbackPreservesGenesisDrep verifies that rolling back past
// an on-chain DRep update does not error out on a DRep that was created
// from Conway genesis. Before the fix, batchFetchDrepCerts INNER-JOINed
// the genesis registration_drep row (certificate_id=0) out of the cache,
// causing "DRep ... has no registration cert at or before slot ..." in
// Phase 2 of RestoreDrepStateAtSlot.
func TestSqliteRollbackPreservesGenesisDrep(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	drepCred := makeCred(0x81)

	require.NoError(t, store.SetGenesisGovernance(
		conway.ConwayGenesisInitialDReps{
			drepCred: conway.ConwayGenesisDRepState{
				Expiry:  500,
				Deposit: 500_000_000,
			},
		},
		conway.ConwayGenesisDelegs{},
		nil,
		nil,
	))

	// On-chain UpdateDrep at slot 300 mutates Drep.added_slot, and
	// simulated voting activity bumps last_activity_epoch.
	require.NoError(t, createTestTransaction(store.DB(), 100, 300))
	updateCert := models.Certificate{
		TransactionID: 100,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeUpdateDrep),
		Slot:          300,
	}
	require.NoError(t, store.DB().Create(&updateCert).Error)
	require.NoError(t, store.DB().Create(&models.UpdateDrep{
		Credential:    drepCred.Credential[:],
		AnchorURL:     "https://example.com/updated",
		AnchorHash:    bytes.Repeat([]byte{0x99}, 32),
		AddedSlot:     300,
		CertificateID: updateCert.ID,
	}).Error)
	require.NoError(t,
		store.DB().Model(&models.Drep{}).
			Where("credential = ?", drepCred.Credential[:]).
			Updates(map[string]any{
				"anchor_url":          "https://example.com/updated",
				"anchor_hash":         bytes.Repeat([]byte{0x99}, 32),
				"added_slot":          uint64(300),
				"last_activity_epoch": uint64(10),
			}).Error,
	)

	// Rollback to slot 200 must NOT error, and must keep the DRep
	// (with its genesis anchor restored, since the update is undone).
	require.NoError(t, store.RestoreDrepStateAtSlot(200, nil))

	restored, err := store.GetDrep(drepCred.Credential[:], true, nil)
	require.NoError(t, err)
	require.NotNil(t, restored, "genesis DRep must survive rollback")
	assert.True(t, restored.Active)
	// ExpiryEpoch came from the Conway genesis config and must NOT be
	// reset to 0 by rollback. drepActiveAtEpoch treats expiry_epoch=0
	// as "never expires" — zeroing the genesis value would silently
	// turn the DRep into an immortal one and inflate governance
	// tallies after recovery.
	assert.Equal(t, uint64(500), restored.ExpiryEpoch,
		"genesis-configured ExpiryEpoch must be preserved on rollback")
	assert.Equal(t, uint64(10), restored.LastActivityEpoch,
		"genesis-rooted LastActivityEpoch must be preserved on rollback")
}

// TestSqliteRollbackResetsExpiryForOnChainDrep verifies that the
// rollback path still resets ExpiryEpoch to 0 for purely on-chain
// DReps (no genesis row), preserving the existing behavior where
// expiry/activity is re-seeded by subsequent governance events. Only
// genesis-rooted DReps (slot-0 registration) get their values
// preserved.
func TestSqliteRollbackResetsExpiryForOnChainDrep(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	drepCred := bytes.Repeat([]byte{0xa1}, 28)

	require.NoError(t, store.DB().Create(&models.Drep{
		Credential:        drepCred,
		AddedSlot:         300,
		ExpiryEpoch:       100,
		LastActivityEpoch: 50,
		Active:            true,
	}).Error)

	// On-chain registration at slot 100.
	require.NoError(t, createTestTransaction(store.DB(), 300, 100))
	regCert := models.Certificate{
		TransactionID: 300,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeRegistrationDrep),
		Slot:          100,
	}
	require.NoError(t, store.DB().Create(&regCert).Error)
	require.NoError(t, store.DB().Create(&models.RegistrationDrep{
		DrepCredential: drepCred,
		AddedSlot:      100,
		CertificateID:  regCert.ID,
	}).Error)

	require.NoError(t, store.RestoreDrepStateAtSlot(200, nil))

	restored, err := store.GetDrep(drepCred, true, nil)
	require.NoError(t, err)
	require.NotNil(t, restored)
	// Non-genesis DRep: expiry/activity reset as documented.
	assert.Equal(t, uint64(0), restored.ExpiryEpoch)
	assert.Equal(t, uint64(0), restored.LastActivityEpoch)
}

// TestSqliteRollbackDrepCrossTypeBlockIndex verifies that the
// cross-type comparison inside RestoreDrepStateAtSlot uses the full
// (added_slot, block_index, cert_index) ordering. Same-slot
// registration and deregistration certs in different txs both have
// cert_index=0; only block_index disambiguates which wins. Before the
// fix, the cross-type comparison used (added_slot, cert_index) only:
// a same-slot deregistration with higher block_index would tie on
// cert_index, fail the strict-greater check, and lose to the
// registration (which the loop starts with as the default winner) —
// silently keeping the DRep active when it should have been
// deregistered.
func TestSqliteRollbackDrepCrossTypeBlockIndex(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	drepCred := bytes.Repeat([]byte{0xb1}, 28)

	// Current DRep state (post-rollback the rollback path will rewrite
	// this).
	require.NoError(t, store.DB().Create(&models.Drep{
		Credential: drepCred,
		AnchorURL:  "to-be-replaced",
		AddedSlot:  300,
		Active:     true,
	}).Error)

	// Two txs in the same block at slot 100. tx 401 has block_index=0,
	// tx 402 has block_index=1.
	require.NoError(t,
		store.DB().Exec(
			`INSERT INTO "transaction" (id, hash, slot, valid, type, block_index)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			401, []byte("dtx401"), 100, true, 0, 0,
		).Error,
	)
	require.NoError(t,
		store.DB().Exec(
			`INSERT INTO "transaction" (id, hash, slot, valid, type, block_index)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			402, []byte("dtx402"), 100, true, 0, 1,
		).Error,
	)
	// Registration in tx 401 (block_index=0) — comes BEFORE the
	// deregistration in block order. Deregistration in tx 402
	// (block_index=1) is the correct winner. The buggy cross-type
	// comparison ignores block_index and keeps the registration as
	// "latest" because both certs have cert_index=0 and the comparator
	// requires a STRICT > on cert_index to swap.
	regCert := models.Certificate{
		TransactionID: 401,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeRegistrationDrep),
		Slot:          100,
	}
	require.NoError(t, store.DB().Create(&regCert).Error)
	require.NoError(t, store.DB().Create(&models.RegistrationDrep{
		DrepCredential: drepCred,
		AnchorURL:      "https://example.com/reg",
		AddedSlot:      100,
		CertificateID:  regCert.ID,
	}).Error)
	deregCert := models.Certificate{
		TransactionID: 402,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeDeregistrationDrep),
		Slot:          100,
	}
	require.NoError(t, store.DB().Create(&deregCert).Error)
	require.NoError(t, store.DB().Create(&models.DeregistrationDrep{
		DrepCredential: drepCred,
		AddedSlot:      100,
		CertificateID:  deregCert.ID,
	}).Error)

	require.NoError(t, store.RestoreDrepStateAtSlot(200, nil))

	restored, err := store.GetDrep(drepCred, true, nil)
	require.NoError(t, err)
	require.NotNil(t, restored)
	// Deregistration is in the tx with higher block_index, so it wins.
	// DRep must be inactive with cleared anchor, and AddedSlot must
	// reflect the winning cert's slot (pins down that latest.addedSlot
	// is what gets persisted out of the cross-type comparison).
	assert.False(t, restored.Active,
		"deregistration in higher-block_index tx must beat registration in lower-block_index tx at same slot")
	assert.Equal(t, "", restored.AnchorURL,
		"anchor must be cleared because deregistration wins")
	assert.Equal(t, uint64(100), restored.AddedSlot,
		"AddedSlot must be set to the winning cert's slot")
}

// TestSqliteRollbackPreservesGenesisVoteOnlyAccount verifies that an
// account created from a Conway-genesis vote delegation is NOT deleted
// when we roll back past a later on-chain VoteDelegation. Before the
// fix, batchFetchCerts INNER-JOINed the synthetic Registration row out,
// hasReg was false, and the account was deleted by Phase 1 of
// RestoreAccountStateAtSlot.
func TestSqliteRollbackPreservesGenesisVoteOnlyAccount(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	drepCred := makeCred(0x82)
	stakeCred := makeCred(0x83)

	require.NoError(t, store.SetGenesisGovernance(
		conway.ConwayGenesisInitialDReps{
			drepCred: conway.ConwayGenesisDRepState{Expiry: 500},
		},
		conway.ConwayGenesisDelegs{
			stakeCred: conway.ConwayGenesisDelegatee{
				Type: conway.ConwayGenesisDelegateeTypeVote,
				DRep: lcommon.Drep{
					Type:       lcommon.DrepTypeAddrKeyHash,
					Credential: drepCred.Credential[:],
				},
			},
		},
		nil,
		nil,
	))

	// On-chain VoteDelegation at slot 300 changes the account's DRep
	// to AlwaysAbstain and bumps Account.added_slot.
	require.NoError(t, createTestTransaction(store.DB(), 101, 300))
	delegCert := models.Certificate{
		TransactionID: 101,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeVoteDelegation),
		Slot:          300,
	}
	require.NoError(t, store.DB().Create(&delegCert).Error)
	require.NoError(t, store.DB().Create(&models.VoteDelegation{
		StakingKey:    stakeCred.Credential[:],
		Drep:          nil,
		DrepType:      models.DrepTypeAlwaysAbstain,
		AddedSlot:     300,
		CertificateID: delegCert.ID,
	}).Error)
	require.NoError(t,
		store.DB().Model(&models.Account{}).
			Where("staking_key = ?", stakeCred.Credential[:]).
			Updates(map[string]any{
				"drep":       nil,
				"drep_type":  models.DrepTypeAlwaysAbstain,
				"added_slot": uint64(300),
			}).Error,
	)

	// Rollback to slot 200: the genesis-rooted account must remain.
	require.NoError(t, store.RestoreAccountStateAtSlot(200, nil))

	restored, err := store.GetAccount(stakeCred.Credential[:], true, nil)
	require.NoError(t, err)
	require.NotNil(t,
		restored,
		"genesis-rooted vote account must not be deleted by rollback",
	)
	assert.Equal(t, drepCred.Credential[:], restored.Drep,
		"DRep must be restored to genesis state")
	assert.Equal(t, models.DrepTypeAddrKeyHash, restored.DrepType)
}

// TestSqliteRollbackPreservesGenesisStakeVoteAccount verifies that an
// account created from a Conway-genesis stake+vote delegation is NOT
// deleted when we roll back past a later on-chain StakeDelegation.
func TestSqliteRollbackPreservesGenesisStakeVoteAccount(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	drepCred := makeCred(0x84)
	stakeCred := makeCred(0x85)
	poolGenesis := makePoolId(0x86)
	poolLater := makePoolId(0x87)

	require.NoError(t, store.SetGenesisGovernance(
		conway.ConwayGenesisInitialDReps{
			drepCred: conway.ConwayGenesisDRepState{Expiry: 500},
		},
		conway.ConwayGenesisDelegs{
			stakeCred: conway.ConwayGenesisDelegatee{
				Type:   conway.ConwayGenesisDelegateeTypeStakeVote,
				PoolId: poolGenesis,
				DRep: lcommon.Drep{
					Type:       lcommon.DrepTypeAddrKeyHash,
					Credential: drepCred.Credential[:],
				},
			},
		},
		nil,
		nil,
	))

	// On-chain StakeDelegation at slot 300 changes the account's pool.
	require.NoError(t, createTestTransaction(store.DB(), 102, 300))
	delegCert := models.Certificate{
		TransactionID: 102,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeStakeDelegation),
		Slot:          300,
	}
	require.NoError(t, store.DB().Create(&delegCert).Error)
	require.NoError(t, store.DB().Create(&models.StakeDelegation{
		StakingKey:    stakeCred.Credential[:],
		PoolKeyHash:   poolLater[:],
		AddedSlot:     300,
		CertificateID: delegCert.ID,
	}).Error)
	require.NoError(t,
		store.DB().Model(&models.Account{}).
			Where("staking_key = ?", stakeCred.Credential[:]).
			Updates(map[string]any{
				"pool":       poolLater[:],
				"added_slot": uint64(300),
			}).Error,
	)

	// Rollback to slot 200: the genesis-rooted account must remain,
	// with both pool and DRep restored to their genesis values.
	require.NoError(t, store.RestoreAccountStateAtSlot(200, nil))

	restored, err := store.GetAccount(stakeCred.Credential[:], true, nil)
	require.NoError(t, err)
	require.NotNil(t,
		restored,
		"genesis-rooted stake/vote account must not be deleted",
	)
	assert.Equal(t, poolGenesis[:], restored.Pool,
		"pool must be restored to genesis value")
	assert.Equal(t, drepCred.Credential[:], restored.Drep,
		"DRep must be restored to genesis value")
	assert.Equal(t, models.DrepTypeAddrKeyHash, restored.DrepType)
}

// TestCertRecordIsMoreRecentBlockIndex verifies that the in-memory
// recency comparator on certRecord and drepCertRecord breaks same-slot
// ties by block_index BEFORE cert_index. This matters because
// cert_index resets per transaction: at the same slot, two certs from
// different txs can both have cert_index=0; only block_index
// distinguishes them. (See CLAUDE.md: "Order certificates using
// added_slot DESC, block_index DESC, cert_index DESC".)
func TestCertRecordIsMoreRecentBlockIndex(t *testing.T) {
	t.Parallel()
	// Same slot, different block_index, same cert_index — the higher
	// block_index must win.
	earlier := certRecord{addedSlot: 100, blockIndex: 0, certIndex: 0}
	later := certRecord{addedSlot: 100, blockIndex: 1, certIndex: 0}
	assert.True(t, later.isMoreRecent(earlier),
		"higher block_index must beat lower at same slot")
	assert.False(t, earlier.isMoreRecent(later),
		"lower block_index must lose to higher at same slot")

	// block_index ranks above cert_index: a row with lower block_index
	// but higher cert_index is still older than one with higher
	// block_index. This is the precise failure case the reviewer
	// flagged.
	loBlkHiCert := certRecord{addedSlot: 100, blockIndex: 0, certIndex: 99}
	hiBlkLoCert := certRecord{addedSlot: 100, blockIndex: 1, certIndex: 0}
	assert.True(t, hiBlkLoCert.isMoreRecent(loBlkHiCert),
		"block_index outranks cert_index when slots match")

	// Same slot AND same block_index: cert_index breaks the tie.
	c0 := certRecord{addedSlot: 100, blockIndex: 5, certIndex: 0}
	c1 := certRecord{addedSlot: 100, blockIndex: 5, certIndex: 1}
	assert.True(t, c1.isMoreRecent(c0))

	// Same exact key: not more recent than self.
	assert.False(t, c0.isMoreRecent(c0))

	// drepCertRecord uses the same ordering.
	drepEarlier := drepCertRecord{addedSlot: 100, blockIndex: 0, certIndex: 0}
	drepLater := drepCertRecord{addedSlot: 100, blockIndex: 1, certIndex: 0}
	assert.True(t, drepLater.isMoreRecent(drepEarlier))
	assert.False(t, drepEarlier.isMoreRecent(drepLater))
}

// TestRollbackPicksLatestByBlockIndexAtSameSlot is an end-to-end check
// that RestoreAccountStateAtSlot picks the correct VoteDelegation when
// two of them land at the same slot in different transactions. The
// later transaction (higher block_index) must win; with the previous
// ordering (slot, cert_index) both certs tied on cert_index=0 and the
// outcome was non-deterministic.
func TestRollbackPicksLatestByBlockIndexAtSameSlot(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	stakeKey := bytes.Repeat([]byte{0x91}, 28)
	drepEarly := bytes.Repeat([]byte{0x92}, 28)
	drepLate := bytes.Repeat([]byte{0x93}, 28)
	drepCurrent := bytes.Repeat([]byte{0x94}, 28)

	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeKey,
		Drep:       drepCurrent,
		DrepType:   models.DrepTypeAddrKeyHash,
		AddedSlot:  300,
		Active:     true,
	}).Error)

	// Registration so the account survives Phase 1 of restore.
	require.NoError(t, createTestTransaction(store.DB(), 200, 50))
	regCert := models.Certificate{
		TransactionID: 200,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeStakeRegistration),
		Slot:          50,
	}
	require.NoError(t, store.DB().Create(&regCert).Error)
	require.NoError(t, store.DB().Create(&models.StakeRegistration{
		StakingKey:    stakeKey,
		AddedSlot:     50,
		CertificateID: regCert.ID,
	}).Error)

	// Two VoteDelegation certs at the SAME slot 100, different txs.
	// tx 201 has block_index=0; tx 202 has block_index=1. Both certs
	// are the first in their respective tx (cert_index=0). The cert
	// from tx 202 must win because of the higher block_index. We
	// insert tx 202's cert first (lower certs.id) so a buggy query
	// that doesn't tiebreak by block_index would deterministically
	// pick the wrong one (tx 201, inserted later, higher certs.id).
	require.NoError(t,
		store.DB().Exec(
			`INSERT INTO "transaction" (id, hash, slot, valid, type, block_index)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			201, []byte("tx201"), 100, true, 0, 0,
		).Error,
	)
	require.NoError(t,
		store.DB().Exec(
			`INSERT INTO "transaction" (id, hash, slot, valid, type, block_index)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			202, []byte("tx202"), 100, true, 0, 1,
		).Error,
	)
	// tx 202's cert (later block_index, correct answer) — insert FIRST
	// so it gets the LOWER certs.id, while the WRONG answer gets the
	// higher certs.id (typical SQL "last wins" tiebreak).
	certLate := models.Certificate{
		TransactionID: 202,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeVoteDelegation),
		Slot:          100,
	}
	require.NoError(t, store.DB().Create(&certLate).Error)
	require.NoError(t, store.DB().Create(&models.VoteDelegation{
		StakingKey:    stakeKey,
		Drep:          drepLate,
		DrepType:      models.DrepTypeAddrKeyHash,
		AddedSlot:     100,
		CertificateID: certLate.ID,
	}).Error)
	certEarly := models.Certificate{
		TransactionID: 201,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeVoteDelegation),
		Slot:          100,
	}
	require.NoError(t, store.DB().Create(&certEarly).Error)
	require.NoError(t, store.DB().Create(&models.VoteDelegation{
		StakingKey:    stakeKey,
		Drep:          drepEarly,
		DrepType:      models.DrepTypeAddrKeyHash,
		AddedSlot:     100,
		CertificateID: certEarly.ID,
	}).Error)

	// Roll back to slot 200 — both slot-100 certs are valid history,
	// the slot-300 state is undone. Expected DRep: drepLate (higher
	// block_index).
	require.NoError(t, store.RestoreAccountStateAtSlot(200, nil))

	restored, err := store.GetAccount(stakeKey, true, nil)
	require.NoError(t, err)
	require.NotNil(t, restored)
	assert.Equal(t, drepLate, restored.Drep,
		"latest VoteDelegation at slot 100 must be the one in the tx "+
			"with higher block_index (tx 202), not tx 201")
}
