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
	"context"
	"reflect"
	"slices"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// stageRewardInput stages changes for the set, the only copy of the reward
// inputs.
func (s stagingLedgerDB) stageRewardInput(
	txn ledgerdb.Txn,
	changes ...RewardInputChange,
) {
	s.ls.stageLedgerChanges(
		txn,
		LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
			RewardInputs: changes,
		},
	)
}

func (s stagingLedgerDB) SaveRewardAdaPots(
	pots *models.RewardAdaPots,
	txn ledgerdb.Txn,
) error {
	cp := *pots
	s.stageRewardInput(txn, RewardInputChange{
		Kind: RewardInputPots, Epoch: pots.Epoch, Pots: &cp,
	})
	return nil
}

func (s stagingLedgerDB) SaveRewardSnapshot(
	snapshot *models.RewardSnapshot,
	txn ledgerdb.Txn,
) error {
	cp := *snapshot
	s.stageRewardInput(txn, RewardInputChange{
		Kind: RewardInputSnapshot, Epoch: snapshot.Epoch, Snapshot: &cp,
	})
	return nil
}

func (s stagingLedgerDB) SaveRewardPoolInputs(
	inputs []*models.RewardPoolInput,
	txn ledgerdb.Txn,
) error {
	byEpoch := map[uint64][]*models.RewardPoolInput{}
	var order []uint64
	for _, in := range inputs {
		cp := *in
		if _, ok := byEpoch[in.Epoch]; !ok {
			order = append(order, in.Epoch)
		}
		byEpoch[in.Epoch] = append(byEpoch[in.Epoch], &cp)
	}
	for _, epoch := range order {
		s.stageRewardInput(txn, RewardInputChange{
			Kind: RewardInputPools, Epoch: epoch, Pools: byEpoch[epoch],
		})
	}
	return nil
}

// SaveRewardStakeInputs stages the rows without copying them: a caller does
// not change a row it has saved.
func (s stagingLedgerDB) SaveRewardStakeInputs(
	inputs []*models.RewardStakeInput,
	txn ledgerdb.Txn,
) error {
	s.stageStakeInputs(txn, inputs)
	return nil
}

func (s stagingLedgerDB) stageStakeInputs(
	txn ledgerdb.Txn,
	inputs []*models.RewardStakeInput,
) {
	if len(inputs) == 0 {
		return
	}
	epoch := inputs[0].Epoch
	same := true
	for _, in := range inputs {
		same = same && in.Epoch == epoch
	}
	if same {
		s.stageRewardInput(
			txn,
			RewardInputChange{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
				Kind:   RewardInputStakes,
				Epoch:  epoch,
				Stakes: inputs,
			},
		)
		return
	}
	byEpoch := map[uint64][]*models.RewardStakeInput{}
	var order []uint64
	for _, in := range inputs {
		if _, ok := byEpoch[in.Epoch]; !ok {
			order = append(order, in.Epoch)
		}
		byEpoch[in.Epoch] = append(byEpoch[in.Epoch], in)
	}
	for _, e := range order {
		s.stageRewardInput(
			txn,
			RewardInputChange{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
				Kind:   RewardInputStakes,
				Epoch:  e,
				Stakes: byEpoch[e],
			},
		)
	}
}

func (s stagingLedgerDB) DeleteRewardInputsForEpoch(
	epoch uint64,
	txn ledgerdb.Txn,
) error {
	s.stageRewardInput(txn, RewardInputChange{
		Kind: RewardInputReplace, Epoch: epoch,
	})
	return nil
}

// DeleteRewardStateBeforeEpoch prunes the reward account outputs too, and the
// derived reward rows of the set.
func (s stagingLedgerDB) DeleteRewardStateBeforeEpoch(
	ctx context.Context,
	epoch uint64,
	txn ledgerdb.Txn,
) error {
	s.stageRewardInput( //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		txn,
		RewardInputChange{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
			Kind:       RewardInputPrune,
			PruneBelow: epoch,
		},
	)
	//nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
	s.pruneDerived(txn, epoch)
	return nil
}

// rewardInputsServed returns the view reads are answered from. Without a
// serving set the read has no answer to give.
func (s stagingLedgerDB) rewardInputsServed(
	txn ledgerdb.Txn,
) (*rewardInputsView, error) {
	if r := s.ls.rewardInputsServing(txn); r != nil {
		v := s.ls.newRewardInputsView(r, txn)
		return &v, nil
	}
	return nil, s.ls.setFamilyUnserved(nodesettings.FamilyRewardInputs)
}

func (s stagingLedgerDB) GetRewardAdaPots(
	epoch uint64,
	txn ledgerdb.Txn,
) (*models.RewardAdaPots, error) {
	v, err := s.rewardInputsServed(txn)
	if err != nil {
		return nil, err
	}
	return v.pots(epoch)
}

func samePots(a, b *models.RewardAdaPots) bool {
	if a == nil || b == nil {
		return a == b
	}
	x, y := *a, *b
	x.ID, y.ID = 0, 0
	if (x.ImportedEpochFees == nil) != (y.ImportedEpochFees == nil) ||
		(x.ImportedEpochFees != nil && *x.ImportedEpochFees != *y.ImportedEpochFees) {
		return false
	}
	x.ImportedEpochFees, y.ImportedEpochFees = nil, nil
	return reflect.DeepEqual(x, y)
}

func (s stagingLedgerDB) GetRewardSnapshot(
	epoch uint64,
	snapshotType string,
	txn ledgerdb.Txn,
) (*models.RewardSnapshot, error) {
	v, err := s.rewardInputsServed(txn)
	if err != nil {
		return nil, err
	}
	return v.snapshot(epoch, snapshotType)
}

func sameSnapshot(a, b *models.RewardSnapshot) bool {
	if a == nil || b == nil {
		return a == b
	}
	x, y := *a, *b
	x.ID, y.ID = 0, 0
	if (x.ExcludedActiveStake == nil) != (y.ExcludedActiveStake == nil) ||
		(x.ExcludedActiveStake != nil && *x.ExcludedActiveStake != *y.ExcludedActiveStake) ||
		(x.EpochNonce == nil) != (y.EpochNonce == nil) ||
		!bytes.Equal(x.EpochNonce, y.EpochNonce) {
		return false
	}
	x.ExcludedActiveStake, y.ExcludedActiveStake = nil, nil
	x.EpochNonce, y.EpochNonce = nil, nil
	return reflect.DeepEqual(x, y)
}

func (s stagingLedgerDB) GetRewardSeedFailure(
	epoch uint64,
	snapshotType string,
	txn ledgerdb.Txn,
) (string, error) {
	v, err := s.rewardInputsServed(txn)
	if err != nil {
		return "", err
	}
	return v.failure(epoch, snapshotType)
}

func (s stagingLedgerDB) GetRewardPoolInputs(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardPoolInput, error) {
	v, err := s.rewardInputsServed(txn)
	if err != nil {
		return nil, err
	}
	return v.pools(epoch)
}

func samePoolInputs(a, b []*models.RewardPoolInput) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !samePoolInput(a[i], b[i]) {
			return false
		}
	}
	return true
}

func samePoolInput(a, b *models.RewardPoolInput) bool {
	x, y := *a, *b
	x.ID, y.ID = 0, 0
	if (x.Margin == nil || x.Margin.Rat == nil) != (y.Margin == nil || y.Margin.Rat == nil) ||
		(x.Margin != nil && x.Margin.Rat != nil && x.Margin.Cmp(y.Margin.Rat) != 0) ||
		(x.BlocksProduced == nil) != (y.BlocksProduced == nil) ||
		(x.BlocksProduced != nil && *x.BlocksProduced != *y.BlocksProduced) ||
		(x.TotalBlocksInEpoch == nil) != (y.TotalBlocksInEpoch == nil) ||
		(x.TotalBlocksInEpoch != nil && *x.TotalBlocksInEpoch != *y.TotalBlocksInEpoch) ||
		(x.RewardAccount == nil) != (y.RewardAccount == nil) ||
		!bytes.Equal(x.RewardAccount, y.RewardAccount) ||
		!bytes.Equal(x.PoolKeyHash, y.PoolKeyHash) {
		return false
	}
	x.Margin, y.Margin = nil, nil
	x.BlocksProduced, y.BlocksProduced = nil, nil
	x.TotalBlocksInEpoch, y.TotalBlocksInEpoch = nil, nil
	x.RewardAccount, y.RewardAccount = nil, nil
	x.PoolKeyHash, y.PoolKeyHash = nil, nil
	return reflect.DeepEqual(x, y)
}

func (s stagingLedgerDB) GetRewardStakeInputs(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardStakeInput, error) {
	return s.stakeInputs(epoch, nil, nil, txn)
}

func (s stagingLedgerDB) GetRewardStakeInputsInPoolKeyHashRange(
	epoch uint64,
	poolKeyHashLo []byte,
	poolKeyHashHi []byte,
	txn ledgerdb.Txn,
) ([]*models.RewardStakeInput, error) {
	return s.stakeInputs(epoch, poolKeyHashLo, poolKeyHashHi, txn)
}

func (s stagingLedgerDB) stakeInputs(
	epoch uint64,
	lo, hi []byte,
	txn ledgerdb.Txn,
) ([]*models.RewardStakeInput, error) {
	v, err := s.rewardInputsServed(txn)
	if err != nil {
		return nil, err
	}
	return v.stakes(epoch, lo, hi)
}

func sameStakeInputs(a, b []*models.RewardStakeInput) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := *a[i], *b[i]
		x.ID, y.ID = 0, 0
		if !bytes.Equal(x.PoolKeyHash, y.PoolKeyHash) ||
			!bytes.Equal(x.StakingKey, y.StakingKey) {
			return false
		}
		x.PoolKeyHash, y.PoolKeyHash = nil, nil
		x.StakingKey, y.StakingKey = nil, nil
		if !reflect.DeepEqual(x, y) {
			return false
		}
	}
	return true
}

// ResolvePoolRewardAccountAutoVotes resolves from the pools and accounts the
// ledger reads, which the set serves, rather than from the metadata store's
// tables.
func (s stagingLedgerDB) ResolvePoolRewardAccountAutoVotes(
	ctx context.Context,
	snapshots []*models.PoolStakeSnapshot,
	txn ledgerdb.Txn,
) error {
	return database.ResolveRewardAccountAutoVotes(
		ctx,
		snapshots,
		func(pkhs []lcommon.PoolKeyHash) ([]models.Pool, error) {
			return s.GetPools(ctx, pkhs, txn)
		},
		func(
			refs []models.StakeCredentialRef,
		) (map[string]*models.Account, error) {
			return s.GetAccountsByCredential(ctx, refs, true, txn)
		},
	)
}

// getPendingStakeInputs reads the pending sync_state record from the set.
func (s stagingLedgerDB) getPendingStakeInputs(
	txn ledgerdb.Txn,
) (string, error) {
	v, err := s.rewardInputsServed(txn)
	if err != nil {
		return "", err
	}
	return v.pendingRaw()
}

// setPendingStakeInputs stages the pending record for the set.
func (s stagingLedgerDB) setPendingStakeInputs(
	value string,
	txn ledgerdb.Txn,
) error {
	entries, err := ParseRewardStakeInputsPending(value)
	if err != nil {
		return err
	}
	s.stageRewardInput(txn, RewardInputChange{
		Kind: RewardInputPending, Pending: slices.Clone(entries),
	})
	return nil
}
