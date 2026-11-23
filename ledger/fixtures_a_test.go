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
	"bytes"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// installTestLedgerSources gives ls's database the reads NewLedgerState
// installs, for a ledger a fixture built as a literal: the database's own
// rollback, truncate and pool reads go through ls's LedgerDB set.
func installTestLedgerSources(t testing.TB, ls *LedgerState) {
	t.Helper()
	require.NotNil(t, ls.config.UtxoSet, "the fixture's set must be open")
	require.NoError(t, ls.db.SetLiveUtxoSource(ls))
	ls.installNonceSource()
	ls.installEpochSource()
	ls.installPParamsSource()
	ls.installPoolSource()
}

// attachTestLedgerDB is attachTestLiveSet for a fixture whose database reads
// ledger state itself, as it does on a rollback.
func attachTestLedgerDB(t testing.TB, ls *LedgerState) {
	t.Helper()
	attachTestLiveSet(t, ls)
	installTestLedgerSources(t, ls)
}

// committeeSeedAuthorization records cold's authorization of hot at slot in
// db's open LedgerDB set, as the certificate applied in a block does.
func committeeSeedAuthorization(
	t testing.TB,
	db *database.Database,
	cold, hot lcommon.Credential,
	certificateID uint,
	slot uint64,
) {
	t.Helper()
	ls := seedLedgerOf(db)
	require.NotNil(t, ls, "the fixture's set must be open")
	ls.stageLedgerChanges(nil, LedgerChanges{Committee: []CommitteeChange{{
		Slot: slot,
		Auth: &models.AuthCommitteeHot{
			ColdCredentialTag: uint8(cold.CredType),
			ColdCredential:    bytes.Clone(cold.Credential[:]),
			HotCredentialTag:  uint8(hot.CredType),
			HotCredential:     bytes.Clone(hot.Credential[:]),
			CertificateID:     certificateID,
			AddedSlot:         slot,
		},
	}}})
}

// committeeSeedResignation records cold's resignation at slot in db's open
// LedgerDB set.
func committeeSeedResignation(
	t testing.TB,
	db *database.Database,
	cold lcommon.Credential,
	certificateID uint,
	slot uint64,
) {
	t.Helper()
	ls := seedLedgerOf(db)
	require.NotNil(t, ls, "the fixture's set must be open")
	ls.stageLedgerChanges(nil, LedgerChanges{Committee: []CommitteeChange{{
		Slot: slot,
		Resign: &models.ResignCommitteeCold{
			ColdCredentialTag: uint8(cold.CredType),
			ColdCredential:    bytes.Clone(cold.Credential[:]),
			CertificateID:     certificateID,
			AddedSlot:         slot,
		},
	}}})
}

// committeeStoreUpdateProposal stores an active UpdateCommittee proposal
// that adds credential with the given term expiry.
func committeeStoreUpdateProposal(
	t testing.TB,
	db *database.Database,
	seed byte,
	credential lcommon.Credential,
	expiry uint64,
) {
	t.Helper()
	action, err := lcommon.NewUpdateCommitteeGovAction(
		nil,
		nil,
		map[*lcommon.Credential]uint64{&credential: expiry},
		cbor.Rat{Rat: big.NewRat(2, 3)},
	)
	require.NoError(t, err)
	encoded, err := cbor.Encode(action)
	require.NoError(t, err)
	require.NoError(t, seedDB(t, db).SetGovernanceProposal(
		t.Context(),
		&models.GovernanceProposal{
			TxHash:        governanceTestHash(seed),
			ActionType:    uint8(lcommon.GovActionTypeUpdateCommittee),
			ProposedEpoch: 0,
			ExpiresEpoch:  100,
			AnchorHash:    make([]byte, 32),
			ReturnAddress: make([]byte, 29),
			GovActionCbor: encoded,
		},
		nil,
	))
}

// drepSeedRegistered puts a DRep at slot in db's open LedgerDB set. A
// non-nil deposit is the one its registration recorded; nil leaves the DRep
// with no recorded registration, as a vote replayed during recovery does.
func drepSeedRegistered(
	t testing.TB,
	db *database.Database,
	cred lcommon.Credential,
	deposit *uint64,
	slot uint64,
	active bool,
) {
	t.Helper()
	ls := seedLedgerOf(db)
	require.NotNil(t, ls, "the fixture's set must be open")
	tag, err := models.CredentialTagFromUint(cred.CredType)
	require.NoError(t, err)
	hash := bytes.Clone(cred.Credential[:])
	changes := LedgerChanges{DReps: []DRepChange{{
		Slot: slot,
		Drep: models.Drep{
			CredentialTag: tag,
			Credential:    hash,
			AddedSlot:     slot,
			Active:        active,
		},
	}}}
	if deposit != nil {
		changes.DRepDeposits = []DRepDepositChange{{
			Slot: slot, Tag: tag, Credential: hash, Deposit: deposit,
		}}
	}
	ls.stageLedgerChanges(nil, changes)
}

// stakeSeedRegistration registers cred's account at slot in db's open
// LedgerDB set, by a legacy stake registration certificate of the
// transaction bytes.Repeat(seed, 32). A nil deposit is a registration whose
// deposit was never recorded.
func stakeSeedRegistration(
	t testing.TB,
	db *database.Database,
	cred lcommon.Credential,
	deposit *uint64,
	slot uint64,
	seed byte,
) {
	t.Helper()
	ls := seedLedgerOf(db)
	require.NotNil(t, ls, "the fixture's set must be open")
	tag, err := models.CredentialTagFromUint(cred.CredType)
	require.NoError(t, err)
	key := bytes.Clone(cred.Credential[:])
	ls.stageLedgerChanges(nil, LedgerChanges{
		Accounts: []AccountChange{{
			Slot: slot,
			Account: models.Account{
				StakingKey:    key,
				CredentialTag: tag,
				Active:        true,
				AddedSlot:     slot,
			},
		}},
		StakeEvents: []StakeEventChange{{
			Ref: models.NewStakeCredentialRef(tag, key),
			Event: StakeEvent{
				Slot:       slot,
				Registered: true,
				Deposit:    deposit,
				TxHash:     bytes.Repeat([]byte{seed}, 32),
				BlockHash:  bytes.Repeat([]byte{seed + 2}, 32),
				TxSlot:     slot,
				Class:      StakeClassLegacy,
			},
		}},
	})
}

// poolSeedRegistered puts pool in db's open LedgerDB set with reg as the
// registration in force and as its registration certificate at
// reg.AddedSlot, the two records a pool registered in a block leaves.
func poolSeedRegistered(
	t testing.TB,
	db *database.Database,
	pool []byte,
	reg *models.PoolRegistration,
) {
	t.Helper()
	ls := seedLedgerOf(db)
	require.NotNil(t, ls, "the fixture's set must be open")
	ls.stageLedgerChanges(nil, LedgerChanges{
		Pools: []PoolChange{{
			Slot:  reg.AddedSlot,
			State: PoolState{Pool: pool, Params: CleanPoolRegistration(reg)},
		}},
		PoolEvents: []PoolEventChange{{
			Pool:  pool,
			Event: PoolEvent{Slot: reg.AddedSlot, Registration: reg},
		}},
	})
}

// pparamSeedUpdate records a genesis delegate's protocol parameter update
// proposal made at slot for epoch: in db's LedgerDB set once it is open, in
// the metadata store an earlier release left it in before that.
func pparamSeedUpdate(
	t testing.TB,
	db *database.Database,
	genesisHash, updateCbor []byte,
	slot, epoch uint64,
) {
	t.Helper()
	ls := seedLedgerOf(db)
	if ls == nil {
		require.NoError(t, db.SetPParamUpdate(
			genesisHash, updateCbor, slot, epoch, nil,
		))
		return
	}
	ls.stageLedgerChanges(nil, LedgerChanges{
		PParamUpdates: []PParamUpdateChange{{
			Update: models.PParamUpdate{
				GenesisHash: bytes.Clone(genesisHash),
				Cbor:        bytes.Clone(updateCbor),
				AddedSlot:   slot,
				Epoch:       epoch,
			},
		}},
	})
}
