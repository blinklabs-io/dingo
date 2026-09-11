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
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/require"
)

// tipWriteFailingMetadataStore fails the final step of
// database.TruncateAfterSlot -- writing the rolled-back tip -- with an error
// the caller would otherwise read as "the rollback was refused and nothing
// changed". Everything else, including the blob-side block deletion the chain
// truncation performs, keeps working, so the injected failure lands exactly
// where the ledger rollback runs: after chain.RollbackDeferred has already
// removed the abandoned blocks.
type tipWriteFailingMetadataStore struct {
	metadata.MetadataStore
	err error
}

func (s tipWriteFailingMetadataStore) SetTip(
	_ ochainsync.Tip,
	_ dbtypes.Txn,
) error {
	return s.err
}

// failLedgerRollbackAfterChainTruncation points ls at a database whose ledger
// truncation fails with err while leaving the chain manager's own database
// untouched, so ls.chain.RollbackDeferred still succeeds.
func failLedgerRollbackAfterChainTruncation(
	t *testing.T,
	ls *LedgerState,
	err error,
) {
	t.Helper()
	base := ls.db
	failing, newErr := database.New(
		base.Config(),
		database.Stores{
			Blob: base.Blob(),
			Metadata: tipWriteFailingMetadataStore{
				MetadataStore: base.Metadata(),
				err:           err,
			},
		},
	)
	require.NoError(t, newErr)
	t.Cleanup(func() { require.NoError(t, failing.Close()) })
	ls.db = failing
}

// TestRollbackChainAndStateDeferredReportsChainTruncationOnLedgerFailure pins
// the ordering hazard in the issue: rollbackChainAndStateDeferred truncates the
// primary chain first and rolls the ledger back second, so a ledger failure
// leaves durably deleted chain blocks under a ledger tip that still names one
// of them.
//
// That state cannot be undone -- the blocks are gone -- so the failure has to
// be reported as one, not as an appliable-rollback refusal. Every "the rollback
// was rejected, nothing happened" identity the chainsync callers branch on
// (models.ErrBlockNotFound, chain.ErrRollbackExceedsSecurityParam,
// ErrRollbackExceedsMithrilBoundary) is reachable from ls.rollback *after* the
// truncation, and each one drives a plain re-intersect that resumes from the
// stale ledger tip.
func TestRollbackChainAndStateDeferredReportsChainTruncationOnLedgerFailure(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	failLedgerRollbackAfterChainTruncation(t, ls, models.ErrBlockNotFound)
	// Arm the audit window so the discard below has something to discard.
	ls.armContinuationAudit(fixture.currentTip.Point, "test arming")
	require.NotNil(t, ls.continuationAudit.Load())

	err := ls.rollbackChainAndStateDeferred(fixture.ancestorTip.Point, nil)
	require.Error(t, err)

	// State the failure left behind: the chain truncated, the ledger did not.
	require.Equal(t, fixture.ancestorTip, ls.chain.Tip())
	require.Equal(t, fixture.currentTip, ls.currentTip)

	require.False(
		t,
		errors.Is(err, models.ErrBlockNotFound),
		"a ledger rollback that failed after the chain was truncated must "+
			"not be reported with an identity that means the rollback was "+
			"refused and nothing changed; got %v",
		err,
	)
	require.ErrorIs(t, err, ErrChainTruncatedLedgerRollbackFailed)
	require.Contains(
		t,
		err.Error(),
		models.ErrBlockNotFound.Error(),
		"the cause must stay in the message even though it is kept out of "+
			"the errors.Is chain",
	)
	require.Nil(
		t,
		ls.continuationAudit.Load(),
		"the continuation-audit window is armed at a tip the chain no "+
			"longer holds and must be discarded",
	)
}

// TestHandleEventChainsyncRollbackFailsHardAfterChainTruncation is the caller
// half. On the not-found identity handleEventChainsyncRollback logs "rollback
// point not found locally", requests a re-intersect and returns nil -- the node
// carries on with a ledger tip whose block the chain deleted. The failure has
// to surface instead, so the node's fatal-error path can act on it.
func TestHandleEventChainsyncRollbackFailsHardAfterChainTruncation(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	failLedgerRollbackAfterChainTruncation(t, ls, models.ErrBlockNotFound)

	err := ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        fixture.ancestorTip.Point,
		},
		nil,
	)

	require.Equal(t, fixture.ancestorTip, ls.chain.Tip())
	require.Equal(t, fixture.currentTip, ls.currentTip)
	require.Error(
		t,
		err,
		"the chain was truncated and the ledger was not; that must not be "+
			"swallowed as a recoverable re-intersect",
	)
}

// floorLookupFailingMetadataStore fails the durable-applied-floor lookup that
// enforceDurableTipFloor runs. ls.rollback calls enforceDurableTipFloor from
// two places -- the no-op branch it takes when the ledger tip already equals
// the rollback point, and the tail of the full rollback, after the truncation
// has committed and ls.currentTip has been published -- so this is how
// ls.rollback fails with the ledger already sitting on the rollback point.
type floorLookupFailingMetadataStore struct {
	metadata.MetadataStore
	err error
}

func (s floorLookupFailingMetadataStore) GetLatestBlockNonce(
	_ dbtypes.Txn,
) (models.BlockNonce, bool, error) {
	return models.BlockNonce{}, false, s.err
}

// TestRollbackChainAndStateDeferredKeepsOrdinaryErrorWhenLedgerReachedPoint is
// the negative half of the sentinel's contract. ErrChainTruncatedLedgerRollbackFailed
// means the chain truncated and the ledger did not, and the callers escalate on
// it. A ls.rollback failure raised once the ledger tip is already at the
// rollback point is not that state -- both halves agree -- so it has to keep
// the ordinary wrapped error and the recovery it has always had.
func TestRollbackChainAndStateDeferredKeepsOrdinaryErrorWhenLedgerReachedPoint(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	// Put the ledger on the rollback point while the chain is still ahead,
	// so the chain truncation below is real and ls.rollback is the no-op
	// that returns enforceDurableTipFloor's error directly.
	ls.currentTip = fixture.ancestorTip
	ls.publishSnapshotsLocked()

	floorErr := models.ErrBlockNotFound
	base := ls.db
	failing, err := database.New(
		base.Config(),
		database.Stores{
			Blob: base.Blob(),
			Metadata: floorLookupFailingMetadataStore{
				MetadataStore: base.Metadata(),
				err:           floorErr,
			},
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, failing.Close()) })
	ls.db = failing

	rbErr := ls.rollbackChainAndStateDeferred(fixture.ancestorTip.Point, nil)
	require.Error(t, rbErr)

	// Both halves reached the rollback point: the chain truncated to it and
	// the ledger was already there.
	require.Equal(t, fixture.ancestorTip, ls.chain.Tip())
	require.Equal(t, fixture.ancestorTip, ls.currentTip)

	require.False(
		t,
		errors.Is(rbErr, ErrChainTruncatedLedgerRollbackFailed),
		"the ledger tip is on the rollback point, so the two halves are "+
			"not split and the divergence sentinel must not be "+
			"reported; got %v",
		rbErr,
	)
	require.ErrorIs(
		t,
		rbErr,
		floorErr,
		"a failure that left the halves agreeing keeps the cause in the "+
			"errors.Is chain, as it did before the sentinel existed",
	)
}
