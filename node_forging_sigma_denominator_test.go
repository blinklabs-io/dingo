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

package dingo

import (
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/leader"
)

const (
	sigmaDenomEpoch = uint64(7)
	// The mark rows sum to 4_000_000 ...
	sigmaDenomPoolAStake = uint64(3_000_000)
	sigmaDenomPoolBStake = uint64(1_000_000)
	sigmaDenomRowSum     = sigmaDenomPoolAStake + sigmaDenomPoolBStake
	// ... while epoch_summary.total_active_stake carries a different value.
	//
	// Rotation normally writes both from one calculation, so they match. This
	// fixture drives them apart on purpose, because "they agree by
	// construction" is a property of the WRITER: it makes the two readers
	// indistinguishable in every ordinary fixture and so hides which one a
	// given code path actually consults. Separating them is the only way to
	// observe that choice, and #3814 is precisely the report that the forge
	// and verify paths made it differently.
	sigmaDenomSummaryTotal = uint64(5_000_000)
)

// newSigmaDenominatorLedger builds a real LedgerState over a real database,
// so the assertions below run against the production forging adapter rather
// than a reimplementation of it.
func newSigmaDenominatorLedger(
	t *testing.T,
) (*ledger.LedgerState, *database.Database) {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })
	chainManager, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	ledgerState, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:     db,
		ChainManager: chainManager,
		Logger:       logger,
	})
	require.NoError(t, err)
	return ledgerState, db
}

func sigmaDenomPoolKeyHash(fill byte) []byte {
	hash := make([]byte, 28)
	for i := range hash {
		hash[i] = fill
	}
	return hash
}

// seedSigmaDenominatorSnapshot writes the mark rows and the epoch summary with
// deliberately different totals.
func seedSigmaDenominatorSnapshot(
	t *testing.T,
	db *database.Database,
	poolA, poolB []byte,
) {
	t.Helper()
	require.NoError(t, db.Metadata().SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{
			{
				Epoch:          sigmaDenomEpoch,
				SnapshotType:   models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:    poolA,
				TotalStake:     dbtypes.Uint64(sigmaDenomPoolAStake),
				DelegatorCount: 1,
				CapturedSlot:   1,
			},
			{
				Epoch:          sigmaDenomEpoch,
				SnapshotType:   models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:    poolB,
				TotalStake:     dbtypes.Uint64(sigmaDenomPoolBStake),
				DelegatorCount: 1,
				CapturedSlot:   1,
			},
		},
		nil,
	))
	require.NoError(t, db.Metadata().SaveEpochSummary(
		&models.EpochSummary{
			Epoch:            sigmaDenomEpoch,
			TotalActiveStake: dbtypes.Uint64(sigmaDenomSummaryTotal),
			TotalPoolCount:   2,
			TotalDelegators:  2,
			BoundarySlot:     1,
			// Required for GetTotalActiveStake to prefer the summary; this is
			// what rotation sets, so it is the state a synced node is in.
			SnapshotReady: true,
		},
		nil,
	))
}

// TestStakeDistributionAdapterResolvesDenominatorThroughVerifyAccessor is the
// regression test for dingo #3814.
//
// The forging adapter used to return ledger.StakeDistribution.TotalStake,
// which LedgerView.GetStakeDistribution accumulates by summing the mark rows
// itself. Header verification instead reads
// epoch_summary.total_active_stake through Metadata().GetTotalActiveStake.
// Two derivations of one consensus quantity: a node whose forge denominator
// differs from its verify denominator can forge a block it would itself
// reject, or decline a slot it is genuinely eligible for.
//
// The fixture makes the summary and the row sum differ, then asserts the
// adapter reports the value VERIFICATION would use. Before the fix the
// adapter returns sigmaDenomRowSum (4_000_000) and both assertions fail.
func TestStakeDistributionAdapterResolvesDenominatorThroughVerifyAccessor(
	t *testing.T,
) {
	ledgerState, db := newSigmaDenominatorLedger(t)
	poolA := sigmaDenomPoolKeyHash(0x41)
	poolB := sigmaDenomPoolKeyHash(0x42)
	seedSigmaDenominatorSnapshot(t, db, poolA, poolB)

	// The denominator header verification resolves, read through the accessor
	// verify_header.go uses. Captured from the database rather than restated
	// as a literal, so the test compares the two paths instead of comparing
	// one path to a number this test chose.
	verifyTotal, err := db.Metadata().GetTotalActiveStake(
		sigmaDenomEpoch,
		models.PoolStakeSnapshotTypeMark,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, sigmaDenomSummaryTotal, verifyTotal,
		"fixture precondition: the verify accessor must serve the summary")

	adapter := &stakeDistributionAdapter{ledgerState: ledgerState}
	poolStake, forgeTotal, err := adapter.GetPoolAndTotalActiveStake(
		sigmaDenomEpoch,
		poolA,
	)
	require.NoError(t, err)

	assert.Equal(t, verifyTotal, forgeTotal,
		"forge and verify must resolve one denominator through one accessor "+
			"(dingo #3814)")
	assert.NotEqual(t, sigmaDenomRowSum, forgeTotal,
		"the forge denominator must not be re-derived by summing the mark "+
			"rows; that is the second derivation #3814 removes")

	// The numerator is unchanged by this fix and must still come from the
	// pool's own mark row.
	assert.Equal(t, sigmaDenomPoolAStake, poolStake,
		"the numerator must remain the pool's mark-snapshot stake")
}

// TestStakeDistributionAdapterSigmaPairSurvivesRecapture checks that each
// adapter read yields a self-consistent sigma across a snapshot re-capture.
//
// Scope, stated plainly: this drives a re-capture between two SEPARATE
// adapter calls, not between the two halves of a single call. It therefore
// does NOT by itself prove the dingo #3815 atomicity property -- a write
// landing inside one call is not reachable from outside the adapter without
// a seam that does not exist. What it does prove is that both halves of a
// given read move together to the new generation rather than one of them
// lagging, and it would catch a fix that made only one half transactional.
//
// The atomicity property itself is pinned two ways instead:
// TestStakeDistributionProviderForbidsTornSigmaRead below makes the split
// read unexpressible in the provider interface, and
// TestComputeScheduleReadsSigmaPairInOneProviderCall in ledger/leader
// asserts the real schedule computation performs exactly one paired read.
//
// The two generations are chosen with DIFFERENT absolute values but the SAME
// sigma, so a torn pair is detectable as a sigma matching neither.
func TestStakeDistributionAdapterSigmaPairSurvivesRecapture(t *testing.T) {
	ledgerState, db := newSigmaDenominatorLedger(t)
	poolA := sigmaDenomPoolKeyHash(0x41)
	poolB := sigmaDenomPoolKeyHash(0x42)

	// Generation one: sigma = 3_000_000 / 5_000_000.
	seedSigmaDenominatorSnapshot(t, db, poolA, poolB)

	adapter := &stakeDistributionAdapter{ledgerState: ledgerState}
	poolStake, total, err := adapter.GetPoolAndTotalActiveStake(
		sigmaDenomEpoch,
		poolA,
	)
	require.NoError(t, err)

	// Generation two: every value doubled, so sigma is identical while both
	// halves differ. Written AFTER the read above, then read again below.
	require.NoError(t, db.Metadata().SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{
			{
				Epoch:          sigmaDenomEpoch,
				SnapshotType:   models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:    poolA,
				TotalStake:     dbtypes.Uint64(sigmaDenomPoolAStake * 2),
				DelegatorCount: 1,
				CapturedSlot:   2,
			},
		},
		nil,
	))
	require.NoError(t, db.Metadata().SaveEpochSummary(
		&models.EpochSummary{
			Epoch:            sigmaDenomEpoch,
			TotalActiveStake: dbtypes.Uint64(sigmaDenomSummaryTotal * 2),
			TotalPoolCount:   2,
			TotalDelegators:  2,
			BoundarySlot:     2,
			SnapshotReady:    true,
		},
		nil,
	))

	poolStake2, total2, err := adapter.GetPoolAndTotalActiveStake(
		sigmaDenomEpoch,
		poolA,
	)
	require.NoError(t, err)

	// Each read must be self-consistent: numerator*otherDenominator equals
	// denominator*otherNumerator only when both pairs carry the same sigma.
	// Cross-multiplied to keep this exact rather than float.
	assert.Equal(t,
		poolStake*sigmaDenomSummaryTotal,
		total*sigmaDenomPoolAStake,
		"the first read's sigma must come from a single snapshot generation",
	)
	assert.Equal(t,
		poolStake2*sigmaDenomSummaryTotal,
		total2*sigmaDenomPoolAStake,
		"the second read's sigma must come from a single snapshot generation",
	)
	// And the second read must actually have observed the re-capture, or the
	// assertions above would be vacuous.
	assert.Equal(t, sigmaDenomPoolAStake*2, poolStake2,
		"the second read must observe the re-captured snapshot")
	assert.Equal(t, sigmaDenomSummaryTotal*2, total2,
		"the second read must observe the re-captured summary")
}

// TestStakeDistributionProviderForbidsTornSigmaRead pins the interface shape
// that makes the dingo #3815 defect unexpressible.
//
// The fix is not only that the adapter now reads both halves in one
// transaction; it is that StakeDistributionProvider no longer offers a way to
// read them separately. A future adapter cannot reintroduce the torn read
// without changing the interface, which this test makes a visible decision
// rather than an accident.
func TestStakeDistributionProviderForbidsTornSigmaRead(t *testing.T) {
	var adapter any = &stakeDistributionAdapter{}

	if _, ok := adapter.(leader.StakeDistributionProvider); !ok {
		t.Fatal(
			"stakeDistributionAdapter must satisfy " +
				"leader.StakeDistributionProvider",
		)
	}

	// The separate accessors must be gone. Either one surviving means a
	// caller can still take the numerator and the denominator from different
	// transactions.
	type poolStakeReader interface {
		GetPoolStake(uint64, []byte) (uint64, error)
	}
	type totalStakeReader interface {
		GetTotalActiveStake(uint64) (uint64, error)
	}
	if _, ok := adapter.(poolStakeReader); ok {
		t.Error(
			"stakeDistributionAdapter must not expose a standalone " +
				"GetPoolStake; the sigma pair is read together (dingo #3815)",
		)
	}
	if _, ok := adapter.(totalStakeReader); ok {
		t.Error(
			"stakeDistributionAdapter must not expose a standalone " +
				"GetTotalActiveStake; the sigma pair is read together " +
				"(dingo #3815)",
		)
	}
}

func replaceSigmaSnapshotAtomically(
	t *testing.T,
	db *database.Database,
	poolKeyHash []byte,
	poolStake, totalStake uint64,
	capturedSlot uint64,
) {
	t.Helper()
	txn := db.Transaction(true)
	defer func() { require.NoError(t, txn.Rollback()) }()

	require.NoError(t, db.Metadata().SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{{
			Epoch:          sigmaDenomEpoch,
			SnapshotType:   models.PoolStakeSnapshotTypeMark,
			PoolKeyHash:    poolKeyHash,
			TotalStake:     dbtypes.Uint64(poolStake),
			DelegatorCount: 1,
			CapturedSlot:   capturedSlot,
		}},
		txn.Metadata(),
	))
	require.NoError(t, db.Metadata().SaveEpochSummary(
		&models.EpochSummary{
			Epoch:            sigmaDenomEpoch,
			TotalActiveStake: dbtypes.Uint64(totalStake),
			TotalPoolCount:   2,
			TotalDelegators:  2,
			BoundarySlot:     capturedSlot,
			SnapshotReady:    true,
		},
		txn.Metadata(),
	))
	require.NoError(t, txn.Commit())
}

// TestStakeDistributionAdapterKeepsSigmaConsistentAcrossRecapture proves the
// reader's transaction is the consistency boundary for the sigma pair.
//
// The hook releases an atomic recapture after the numerator query has fixed the
// read transaction's snapshot but before the denominator query. A reader that
// opens one transaction per half can combine generation one and generation two;
// the paired accessor must return generation one in full.
func TestStakeDistributionAdapterKeepsSigmaConsistentAcrossRecapture(
	t *testing.T,
) {
	ledgerState, db := newSigmaDenominatorLedger(t)
	poolA := sigmaDenomPoolKeyHash(0x41)
	poolB := sigmaDenomPoolKeyHash(0x42)
	replaceSigmaSnapshotAtomically(
		t, db, poolA,
		sigmaDenomPoolAStake, sigmaDenomSummaryTotal, 1,
	)
	require.NoError(t, db.Metadata().SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{{
			Epoch:          sigmaDenomEpoch,
			SnapshotType:   models.PoolStakeSnapshotTypeMark,
			PoolKeyHash:    poolB,
			TotalStake:     dbtypes.Uint64(sigmaDenomPoolBStake),
			DelegatorCount: 1,
			CapturedSlot:   1,
		}},
		nil,
	))

	readStarted := make(chan struct{})
	releaseRead := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseRead) }) }
	defer release()
	result := make(chan struct {
		poolStake  uint64
		totalStake uint64
		err        error
	}, 1)
	adapter := &stakeDistributionAdapter{
		ledgerState: ledgerState,
		afterPoolStakeReadFn: func() {
			close(readStarted)
			<-releaseRead
		},
	}
	go func() {
		poolStake, totalStake, err := adapter.GetPoolAndTotalActiveStake(
			sigmaDenomEpoch,
			poolA,
		)
		result <- struct {
			poolStake  uint64
			totalStake uint64
			err        error
		}{poolStake, totalStake, err}
	}()

	select {
	case <-readStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("sigma reader did not reach the coordinated recapture point")
	}
	// Generation two changes both halves while the reader is paused between
	// its two SQL statements. The write is one transaction, matching the
	// snapshot publication path in ledger/snapshot/rotation.go.
	replaceSigmaSnapshotAtomically(
		t, db, poolA,
		sigmaDenomPoolAStake*2, sigmaDenomSummaryTotal*2, 2,
	)
	release()

	var got struct {
		poolStake  uint64
		totalStake uint64
		err        error
	}
	select {
	case got = <-result:
	case <-time.After(5 * time.Second):
		t.Fatal("sigma reader did not finish after recapture")
	}
	require.NoError(t, got.err)
	require.Equal(t, sigmaDenomPoolAStake, got.poolStake,
		"the numerator must remain from the reader's snapshot generation")
	require.Equal(t, sigmaDenomSummaryTotal, got.totalStake,
		"the denominator must not come from the recaptured generation")
	require.Equal(t,
		got.poolStake*sigmaDenomSummaryTotal,
		got.totalStake*sigmaDenomPoolAStake,
		"a sigma read must use one committed snapshot generation",
	)
}
