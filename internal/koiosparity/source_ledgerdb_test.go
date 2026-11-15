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

package koiosparity

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/require"
)

var errLedgerDBRead = errors.New("ledger state read refused by the test")

// refusingLedgerDB refuses every ledger state read the source makes, noting
// its name: an answer the source still produces came from somewhere else.
type refusingLedgerDB struct {
	ledgerdb.LedgerDB
	mu    sync.Mutex
	calls []string
}

func (r *refusingLedgerDB) refuse(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, name)
	return errLedgerDBRead
}

func (r *refusingLedgerDB) taken() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := slices.Clone(r.calls)
	r.calls = nil
	return out
}

func (r *refusingLedgerDB) GetTip(ledgerdb.Txn) (ochainsync.Tip, error) {
	return ochainsync.Tip{}, r.refuse("GetTip")
}

func (r *refusingLedgerDB) GetEpoch(
	uint64, ledgerdb.Txn,
) (*models.Epoch, error) {
	return nil, r.refuse("GetEpoch")
}

func (r *refusingLedgerDB) GetEpochBySlot(
	uint64, ledgerdb.Txn,
) (*models.Epoch, error) {
	return nil, r.refuse("GetEpochBySlot")
}

func (r *refusingLedgerDB) GetEpochSummary(
	uint64, ledgerdb.Txn,
) (*models.EpochSummary, error) {
	return nil, r.refuse("GetEpochSummary")
}

func (r *refusingLedgerDB) GetLatestEpochSummary(
	ledgerdb.Txn,
) (*models.EpochSummary, error) {
	return nil, r.refuse("GetLatestEpochSummary")
}

func (r *refusingLedgerDB) GetPoolStakeSnapshotsByEpoch(
	uint64, string, ledgerdb.Txn,
) ([]*models.PoolStakeSnapshot, error) {
	return nil, r.refuse("GetPoolStakeSnapshotsByEpoch")
}

func (r *refusingLedgerDB) GetPoolKeyHashesRetiredByEpoch(
	uint64, uint64, ledgerdb.Txn,
) ([][]byte, error) {
	return nil, r.refuse("GetPoolKeyHashesRetiredByEpoch")
}

func (r *refusingLedgerDB) GetRewardSnapshot(
	uint64, string, ledgerdb.Txn,
) (*models.RewardSnapshot, error) {
	return nil, r.refuse("GetRewardSnapshot")
}

func (r *refusingLedgerDB) GetRewardAdaPots(
	uint64, ledgerdb.Txn,
) (*models.RewardAdaPots, error) {
	return nil, r.refuse("GetRewardAdaPots")
}

func (r *refusingLedgerDB) GetRewardAccountOutputs(
	uint64, ledgerdb.Txn,
) ([]*models.RewardAccountOutput, error) {
	return nil, r.refuse("GetRewardAccountOutputs")
}

func (r *refusingLedgerDB) GetRewardPoolInputs(
	uint64, ledgerdb.Txn,
) ([]*models.RewardPoolInput, error) {
	return nil, r.refuse("GetRewardPoolInputs")
}

func (r *refusingLedgerDB) GetPParamsRows(
	uint64, uint, ledgerdb.Txn,
) ([]models.PParams, error) {
	return nil, r.refuse("GetPParamsRows")
}

func (r *refusingLedgerDB) GetGovernanceProposalsRefundedAt(
	uint64, ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	return nil, r.refuse("GetGovernanceProposalsRefundedAt")
}

// TestDatabaseSourceReadsLedgerStateThroughLedgerDB requires every read a
// DatabaseSource makes of epochs, summaries, snapshots, rewards, protocol
// parameters and refunds to go through its LedgerDB, so the decorator, and
// in set mode the live UTxO set, answers it.
func TestDatabaseSourceReadsLedgerStateThroughLedgerDB(t *testing.T) {
	t.Parallel()
	db := newTestDatabaseSourceDB(t)
	refusing := &refusingLedgerDB{LedgerDB: dbadapter.New(db)}
	src, err := NewDatabaseSourceWithLedgerDB(db, refusing)
	require.NoError(t, err)
	ctx := context.Background()

	cases := []struct {
		name string
		read func() error
		want string
	}{
		{"latest epoch", func() error {
			_, err := src.GetLatestEpoch(ctx)
			return err
		}, "GetLatestEpochSummary"},
		{"epoch data", func() error {
			_, err := src.GetEpochData(ctx, 5)
			return err
		}, "GetEpochSummary"},
		{"snapshot members", func() error {
			_, err := src.GetPoolStakeSnapshotMembers(ctx, 5)
			return err
		}, "GetPoolStakeSnapshotsByEpoch"},
		{"retired pools", func() error {
			_, err := src.GetPoolsRetiredByEpoch(ctx, 5, 100)
			return err
		}, "GetPoolKeyHashesRetiredByEpoch"},
		{"reward snapshot", func() error {
			_, err := src.GetRewardSnapshot(ctx, 5)
			return err
		}, "GetRewardSnapshot"},
		{"protocol parameters", func() error {
			_, err := src.GetProtocolParams(ctx, 5)
			return err
		}, "GetEpoch"},
		{"reward account outputs", func() error {
			_, err := src.GetRewardAccountOutputs(ctx, 5)
			return err
		}, "GetRewardAccountOutputs"},
		{"proposal refunds", func() error {
			_, err := src.GetProposalDepositRefunds(ctx, 5)
			return err
		}, "GetTip"},
		{"pool epoch data", func() error {
			_, err := src.GetPoolEpochDataMap(ctx, 4, 4)
			return err
		}, "GetTip"},
	}
	for _, tc := range cases {
		refusing.taken()
		err := tc.read()
		require.ErrorIs(t, err, errLedgerDBRead, tc.name)
		calls := refusing.taken()
		require.NotEmpty(t, calls, tc.name)
		require.Equal(t, tc.want, calls[0], tc.name)
	}
}

func TestDatabaseSourceNeedsALedgerDB(t *testing.T) {
	t.Parallel()
	db := newTestDatabaseSourceDB(t)
	_, err := NewDatabaseSourceWithLedgerDB(db, nil)
	require.Error(t, err)
	_, err = NewDatabaseSourceWithLedgerDB(nil, dbadapter.New(db))
	require.Error(t, err)
}
