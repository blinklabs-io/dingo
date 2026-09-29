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
	"encoding/binary"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// holdRatificationApply stops the scenario's ratification jobs between
// deciding and writing until the returned release runs.
func holdRatificationApply(
	t *testing.T,
	s *govDiffScenario,
) (held <-chan uint64, release func()) {
	t.Helper()
	ch := make(chan uint64, 8)
	gate := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	s.ls.ratificationApplyHook = func(epoch uint64) {
		ch <- epoch
		<-gate
	}
	return ch, release
}

func requireHeld(t *testing.T, held <-chan uint64, epoch uint64) {
	t.Helper()
	select {
	case got := <-held:
		require.Equal(t, epoch, got)
	case <-time.After(10 * time.Second):
		t.Fatalf("ratification job for epoch %d never decided", epoch)
	}
}

func (s *govDiffScenario) proposalByMarker(
	t *testing.T,
	marker byte,
) *models.GovernanceProposal {
	t.Helper()
	p, err := s.db.GetGovernanceProposal(repeatByte(32, marker), 0, nil)
	require.NoError(t, err)
	return p
}

// A job that has decided but not written by the next boundary must not lose
// the decision: that boundary writes it before ENACT reads it, and the job's
// own write then finds nothing to do.
func TestNextBoundaryWritesUndeliveredRatification(t *testing.T) {
	t.Parallel()

	s := newGovDiffScenario(t)
	held, release := holdRatificationApply(t, s)
	s.run(t, 1, func(*LedgerState) {})
	requireHeld(t, held, 742)
	require.Nil(t, s.proposalByMarker(t, 0x71).RatifiedEpoch)

	s.run(t, 1, func(*LedgerState) {})
	update := s.proposalByMarker(t, 0x71)
	require.NotNil(t, update.EnactedEpoch,
		"the boundary after an undelivered decision did not enact it")
	require.Equal(t, uint64(743), *update.EnactedEpoch)
	require.Equal(t, uint64(74_200), *update.RatifiedSlot)

	release()
	require.NoError(t, s.ls.WaitEpochBoundaryJob(t.Context()))
	rec, err := loadPendingRatification(s.db, nil)
	require.NoError(t, err)
	require.Nil(t, rec)
	require.Equal(t, uint64(743), *s.proposalByMarker(t, 0x71).EnactedEpoch)
}

// Readers of the boundary's marks wait for them to be durable.
func TestWaitEpochBoundaryJobBlocksUntilDurable(t *testing.T) {
	t.Parallel()

	s := newGovDiffScenario(t)
	held, release := holdRatificationApply(t, s)
	s.run(t, 1, func(*LedgerState) {})
	requireHeld(t, held, 742)

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(
		t, s.ls.WaitEpochBoundaryJob(cancelled), context.Canceled,
		"a reader went ahead before the decision was written",
	)

	release()
	require.NoError(t, s.ls.WaitEpochBoundaryJob(t.Context()))
	require.NotNil(t, s.proposalByMarker(t, 0x71).RatifiedEpoch)
}

func queryProposals(
	t *testing.T,
	ls *LedgerState,
) olocalstatequery.ProposalsResult {
	t.Helper()
	result, err := ls.queryShelleyGetProposals(nil)
	require.NoError(t, err)
	wrapped, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, wrapped, 1)
	proposals, ok := wrapped[0].(olocalstatequery.ProposalsResult)
	require.True(t, ok)
	return proposals
}

// LSQ GetProposals returns the Conway proposals set: an action RATIFY just
// classified expired, and its child, stay in it until the boundary that
// drops them.
func TestGetProposalsReturnsTheProposalsSet(t *testing.T) {
	t.Parallel()

	s := newGovDiffScenario(t)
	s.run(t, 1, func(ls *LedgerState) {
		require.NoError(t, ls.WaitEpochBoundaryJob(t.Context()))
	})
	require.NotNil(t, s.proposalByMarker(t, 0x77).ExpiredEpoch)

	proposals := queryProposals(t, s.ls)
	require.Len(t, proposals, 5, "expired members missing from GetProposals")

	s.run(t, 1, func(ls *LedgerState) {
		require.NoError(t, ls.WaitEpochBoundaryJob(t.Context()))
	})
	proposals = queryProposals(t, s.ls)
	// The committee update was enacted; the lapsed action and its child
	// were dropped.
	require.Len(t, proposals, 2)
}

// A rollback below the pending boundary discards its decision; one that keeps
// the boundary keeps it.
func TestRollbackDiscardsRatificationOfRemovedBoundary(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		rollbackTo uint64
		keeps      bool
	}{
		{"below the boundary", 74_199, false},
		{"within the epoch", 74_200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newGovDiffScenario(t)
			held, release := holdRatificationApply(t, s)
			s.run(t, 1, func(*LedgerState) {})
			requireHeld(t, held, 742)

			txn := s.db.Transaction(true)
			require.NoError(t, txn.Do(func(txn *database.Txn) error {
				return s.ls.discardPendingRatificationAfterSlot(
					txn, tc.rollbackTo,
				)
			}))
			release()
			if tc.keeps {
				require.NoError(
					t, s.ls.WaitEpochBoundaryJob(t.Context()),
				)
				require.NotNil(t, s.proposalByMarker(t, 0x71).RatifiedEpoch)
				return
			}
			require.NoError(t, s.ls.WaitEpochBoundaryJob(t.Context()))
			// The job's own write runs after release; let it finish.
			s.ls.ratificationWG.Wait()
			rec, err := loadPendingRatification(s.db, nil)
			require.NoError(t, err)
			require.Nil(t, rec)
			require.Nil(t, s.proposalByMarker(t, 0x71).RatifiedEpoch,
				"a rolled-back boundary's decision was written")
		})
	}
}

func seedResumeBlocks(
	t *testing.T,
	db *database.Database,
	firstSlot uint64,
	count int,
) {
	t.Helper()
	txn := db.BlobTxn(true)
	var prev []byte
	for i := range count {
		hash := make([]byte, 32)
		binary.BigEndian.PutUint64(hash, uint64(i)+1)
		require.NoError(t, db.BlockCreate(models.Block{
			Slot:     firstSlot + uint64(i),
			Number:   uint64(i) + 1,
			Hash:     hash,
			PrevHash: prev,
			Cbor:     []byte{0x80},
		}, txn))
		prev = hash
	}
	require.NoError(t, txn.Commit())
}

// A restart with an undecided boundary records a rewind below it; one too far
// back for the rollback intent fails start-up instead.
func TestResumePendingRatificationRewindsOrFails(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		blocks int
		fails  bool
	}{
		{"within the intent limit", 4, false},
		{"beyond the intent limit", maxRollbackIntentBlocks + 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			const boundary = 1_000
			seedResumeBlocks(t, db, boundary-1, tc.blocks)
			raw, err := json.Marshal(pendingRatificationRecord{
				Epoch: 10, BoundarySlot: boundary, ID: 1,
			})
			require.NoError(t, err)
			require.NoError(t, db.SetSyncState(
				pendingRatificationSyncKey, string(raw), nil,
			))
			ls := &LedgerState{db: db, config: LedgerStateConfig{
				Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			}}
			err = ls.resumePendingRatificationIntent()
			if tc.fails {
				require.ErrorIs(t, err, errRollbackIntentTooLarge)
				return
			}
			require.NoError(t, err)
			point, blocks, pending, err := loadRollbackIntent(db)
			require.NoError(t, err)
			require.True(t, pending)
			require.Equal(t, uint64(boundary-1), point.Slot)
			require.Len(t, blocks, tc.blocks-1)
		})
	}
}

// The ledger rollback transaction discards a pending ratification whose
// boundary it removes, and keeps one it does not.
func TestLedgerRollbackDiscardsPendingRatification(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		above uint64
		keeps bool
	}{
		{"boundary above the rollback point", 1, false},
		{"boundary at the rollback point", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			ls := fixture.ls
			rec := pendingRatificationRecord{
				Epoch:        1,
				BoundarySlot: fixture.ancestorTip.Point.Slot + tc.above,
				ID:           7,
			}
			raw, err := json.Marshal(rec)
			require.NoError(t, err)
			require.NoError(t, ls.db.SetSyncState(
				pendingRatificationSyncKey, string(raw), nil,
			))
			job := &ratificationJob{
				record:  rec,
				decided: make(chan struct{}),
				settled: make(chan struct{}),
			}
			ls.ratificationMu.Lock()
			ls.ratificationJob = job
			ls.ratificationMu.Unlock()

			require.NoError(t, ls.rollback(fixture.ancestorTip.Point))

			stored, err := loadPendingRatification(ls.db, nil)
			require.NoError(t, err)
			if tc.keeps {
				require.Equal(t, &rec, stored)
				return
			}
			require.Nil(t, stored, "rollback kept a removed boundary's record")
			select {
			case <-job.settled:
			default:
				t.Fatal("rollback left readers waiting on a discarded decision")
			}
		})
	}
}
