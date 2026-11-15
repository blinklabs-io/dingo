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
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// readCountingLedgerDB counts the reads the tests below require to go
// through the LedgerDB the ledger reads with.
type readCountingLedgerDB struct {
	ledgerdb.LedgerDB
	totalStake atomic.Int64
	proposals  atomic.Int64
}

func (r *readCountingLedgerDB) GetTotalActiveStake(
	epoch uint64,
	snapshotType string,
	txn ledgerdb.Txn,
) (uint64, error) {
	r.totalStake.Add(1)
	return r.LedgerDB.GetTotalActiveStake(epoch, snapshotType, txn)
}

func (r *readCountingLedgerDB) GetActiveGovernanceProposals(
	ctx context.Context,
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	r.proposals.Add(1)
	return r.LedgerDB.GetActiveGovernanceProposals(ctx, epoch, txn)
}

// TestPoolAbsentFromPopulatedSnapshotReadsThroughTheLedgerDB requires the
// total stake the check reads to come from the LedgerDB the ledger reads
// with, not the metadata store beneath it.
func TestPoolAbsentFromPopulatedSnapshotReadsThroughTheLedgerDB(t *testing.T) {
	t.Parallel()
	ls, db := newDiskTestLedger(t)
	reads := &readCountingLedgerDB{LedgerDB: dbadapter.New(db)}
	ls.ldb = reads
	ls.poolAbsentFromPopulatedSnapshot(electingSnapshot{
		epoch: 5, kind: models.PoolStakeSnapshotTypeMark,
	})
	require.Equal(t, int64(1), reads.totalStake.Load())
}

// TestHardForkStabilityScanReadsThroughTheLedgerDB requires the scan that
// runs in the background to read the governance state from the LedgerDB the
// ledger reads with, which is the live UTxO set's verifying one when the set
// keeps epoch state.
func TestHardForkStabilityScanReadsThroughTheLedgerDB(t *testing.T) {
	t.Parallel()
	ls, db := stabilityFixtureLedgerState(t, 9)
	reads := &readCountingLedgerDB{LedgerDB: dbadapter.New(db)}
	ls.ldb = reads
	seedRatifiableBootstrapHardForkInitiation(t, db, stabilityFixtureEpochID, 7)
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(stabilityFixtureVotingDeadline, []byte("tip")),
	}
	ls.evaluateHardForkInitiationStability(context.Background())
	awaitHFIEvalIdle(t, ls)
	require.Positive(t, reads.proposals.Load())
}
