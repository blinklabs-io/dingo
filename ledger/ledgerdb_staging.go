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
	"slices"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/types"
)

// stagingLedgerDB is the LedgerDB the ledger and the snapshot manager write
// through when the live UTxO set keeps epoch state. Each epoch, snapshot,
// network state and donation write goes to the metadata store and is staged
// on its transaction's batch, so the set writes the same rows in the same
// commit.
type stagingLedgerDB struct {
	ledgerdb.LedgerDB
	ls *LedgerState
}

func (s stagingLedgerDB) SavePoolStakeSnapshots(
	snapshots []*models.PoolStakeSnapshot,
	txn ledgerdb.Txn,
) error {
	if err := s.LedgerDB.SavePoolStakeSnapshots(snapshots, txn); err != nil {
		return err
	}
	changes := make([]SnapshotChange, 0, len(snapshots))
	for _, sn := range snapshots {
		c := *sn
		changes = append(changes, SnapshotChange{
			Kind: SnapshotSave, Snapshot: &c,
		})
	}
	s.ls.stageLedgerChanges(txn, LedgerChanges{Snapshots: changes})
	return nil
}

func (s stagingLedgerDB) DeletePoolStakeSnapshotsForEpoch(
	epoch uint64,
	snapshotType string,
	txn ledgerdb.Txn,
) error {
	err := s.LedgerDB.DeletePoolStakeSnapshotsForEpoch(epoch, snapshotType, txn)
	if err != nil {
		return err
	}
	s.ls.stageLedgerChanges(txn, LedgerChanges{Snapshots: []SnapshotChange{{
		Kind: SnapshotDeleteForEpoch, Epoch: epoch, Type: snapshotType,
	}}})
	return nil
}

func (s stagingLedgerDB) DeletePoolStakeSnapshotsBeforeEpoch(
	epoch uint64,
	txn ledgerdb.Txn,
) error {
	if err := s.LedgerDB.DeletePoolStakeSnapshotsBeforeEpoch(epoch, txn); err != nil {
		return err
	}
	s.ls.stageLedgerChanges(txn, LedgerChanges{Snapshots: []SnapshotChange{{
		Kind: SnapshotDeleteBeforeEpoch, Epoch: epoch,
	}}})
	return nil
}

func (s stagingLedgerDB) SetMidEpochEraSwitch(
	sw database.MidEpochEraSwitch,
	txn ledgerdb.Txn,
) error {
	if !s.ls.familyDrained(nodesettings.FamilyEraSwitch) {
		if err := s.LedgerDB.SetMidEpochEraSwitch(sw, txn); err != nil {
			return err
		}
	}
	s.ls.stageLedgerChanges(txn, LedgerChanges{EraSwitch: &sw})
	return nil
}

func (s stagingLedgerDB) SetNetworkState(
	treasury, reserves, slot uint64,
	txn ledgerdb.Txn,
) error {
	if !s.ls.familyDrained(nodesettings.FamilyNetworkDonations) {
		if err := s.LedgerDB.SetNetworkState(treasury, reserves, slot, txn); err != nil {
			return err
		}
	}
	s.ls.stageLedgerChanges(txn, LedgerChanges{NetworkStates: []models.NetworkState{{
		Treasury: types.Uint64(treasury),
		Reserves: types.Uint64(reserves),
		Slot:     slot,
	}}})
	return nil
}

func (s stagingLedgerDB) AddNetworkDonation(
	slot, epoch, amount uint64,
	txn ledgerdb.Txn,
) error {
	if !s.ls.familyDrained(nodesettings.FamilyNetworkDonations) {
		if err := s.LedgerDB.AddNetworkDonation(slot, epoch, amount, txn); err != nil {
			return err
		}
	}
	s.ls.stageLedgerChanges(txn, LedgerChanges{Donations: []models.NetworkDonation{{
		Slot: slot, Epoch: epoch, Amount: amount,
	}}})
	return nil
}

func (s stagingLedgerDB) SetPParams(
	params []byte,
	slot, epoch uint64,
	era uint,
	txn ledgerdb.Txn,
) error {
	if !s.pparamsDrained() {
		if err := s.LedgerDB.SetPParams(params, slot, epoch, era, txn); err != nil {
			return err
		}
	}
	s.ls.stageLedgerChanges(txn, LedgerChanges{PParams: []models.PParams{{
		Cbor: slices.Clone(params), AddedSlot: slot, Epoch: epoch, EraId: era,
	}}})
	return nil
}

// stageLedgerChanges stages a commit's epoch, snapshot, era switch, network
// state, donation and protocol parameter writes for the set, or applies them at once without a
// transaction.
func (ls *LedgerState) stageLedgerChanges(txn ledgerdb.Txn, lc LedgerChanges) {
	stage := func(b *utxoSetBatch) {
		b.epochs = append(b.epochs, lc.Epochs...)
		b.snapshots = append(b.snapshots, lc.Snapshots...)
		b.summaries = append(b.summaries, lc.Summaries...)
		if lc.EraSwitch != nil {
			b.eraSwitch = lc.EraSwitch
		}
		b.networkStates = append(b.networkStates, lc.NetworkStates...)
		b.donations = append(b.donations, lc.Donations...)
		b.pparams = append(b.pparams, lc.PParams...)
		b.accounts = append(b.accounts, lc.Accounts...)
		b.rewardRecords = append(b.rewardRecords, lc.RewardRecords...)
		b.dreps = append(b.dreps, lc.DReps...)
		b.drepLinks = append(b.drepLinks, lc.DRepLinks...)
		b.dormancy = append(b.dormancy, lc.Dormancy...)
		b.pools = append(b.pools, lc.Pools...)
		b.poolEvents = append(b.poolEvents, lc.PoolEvents...)
		b.stakeEvents = append(b.stakeEvents, lc.StakeEvents...)
		b.delegations = append(b.delegations, lc.DelegationEvents...)
		b.drepDeposits = append(b.drepDeposits, lc.DRepDeposits...)
		b.genesisDelegs = append(b.genesisDelegs, lc.GenesisDelegations...)
		b.mirCerts = append(b.mirCerts, lc.MIRCerts...)
		b.rewardOutputs = append(b.rewardOutputs, lc.RewardOutputs...)
		b.rewardRounds = append(b.rewardRounds, lc.RewardRounds...)
		b.postSnapshot = append(b.postSnapshot, lc.PostSnapshotCredits...)
		b.opCerts = append(b.opCerts, lc.OpCerts...)
		b.rewardBlocks = append(b.rewardBlocks, lc.RewardBlocks...)
		b.rewardInputs = append(b.rewardInputs, lc.RewardInputs...)
		b.rewardDerived = append(b.rewardDerived, lc.RewardDerived...)
		b.committee = append(b.committee, lc.Committee...)
		b.proposals = append(b.proposals, lc.Proposals...)
		b.votes = append(b.votes, lc.Votes...)
		b.pparamUpdates = append(b.pparamUpdates, lc.PParamUpdates...)
		b.syncMarkers = append(b.syncMarkers, lc.SyncMarkers...)
		b.importBaselines = append(b.importBaselines, lc.ImportBaselines...)
		b.witnesses = append(b.witnesses, lc.Witnesses...)
	}
	if txn == nil {
		b := &utxoSetBatch{}
		stage(b)
		ls.applyUtxoSet(b)
		return
	}
	ls.utxoSetBatchForTxn(txn, stage)
}

// LedgerDB returns the LedgerDB the ledger writes through, for a component
// such as the snapshot manager whose writes must reach the live UTxO set too.
func (ls *LedgerState) LedgerDB() ledgerdb.LedgerDB {
	return ls.ledgerDB()
}

// keepsEpochState reports whether the live UTxO set keeps epoch state.
func (ls *LedgerState) keepsEpochState() bool {
	_, ok := ls.config.UtxoSet.(LedgerChangesWriter)
	return ok
}
