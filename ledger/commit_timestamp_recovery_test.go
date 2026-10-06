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
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// commitTimestampRecoveryFixture leaves an on-disk database the way an unclean
// stop between a blob commit and its metadata commit does: five chain blocks in
// the blob store, a metadata tip at the third, and a blob commit timestamp the
// metadata store never received. It returns the blocks and the reopened
// database, whose open reported the timestamp mismatch.
func commitTimestampRecoveryFixture(
	t *testing.T,
) ([]chain.RawBlock, *database.Database) {
	return commitTimestampRecoveryFixtureAtTip(t, 2)
}

func commitTimestampRecoveryFixtureAtTip(
	t *testing.T,
	tipIndex int,
) ([]chain.RawBlock, *database.Database) {
	t.Helper()
	dataDir := t.TempDir()
	cfg := &database.Config{
		DataDir: dataDir,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	db, err := dbtest.NewDatabase(t, cfg)
	require.NoError(t, err)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	raw := make([]chain.RawBlock, 0, 5)
	var prev []byte
	for i := 1; i <= 5; i++ {
		h := testHashBytes(fmt.Sprintf("commit-ts-recovery-block-%d", i))
		raw = append(raw, chain.RawBlock{
			Slot:        uint64(i * 10),
			Hash:        h,
			BlockNumber: uint64(i),
			Type:        1,
			PrevHash:    prev,
			Cbor:        []byte{0x80},
		})
		prev = h
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(raw))
	ledgerTip := rawBlockTip(raw[tipIndex])
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return db.SetTip(ledgerTip, txn)
	}))
	blobTxn := db.Blob().NewTransaction(true)
	defer blobTxn.Rollback() //nolint:errcheck
	require.NoError(t, db.Blob().SetCommitTimestamp(1, blobTxn))
	require.NoError(t, blobTxn.Commit())
	require.NoError(t, dbtest.CloseDatabase(db))

	reopened, err := dbtest.NewDatabase(t, cfg)
	require.NotNil(t, reopened)
	_, isTimestampErr := errors.AsType[database.CommitTimestampError](err)
	require.True(
		t,
		isTimestampErr,
		"reopen must report the commit timestamp mismatch, got %v",
		err,
	)
	return raw, reopened
}

type nthDeleteFailingBlobStore struct {
	blob.BlobStore
	failAt  int32
	deletes atomic.Int32
	err     error
}

func (s *nthDeleteFailingBlobStore) DeleteBlock(
	txn dbtypes.Txn,
	slot uint64,
	hash []byte,
	id uint64,
) error {
	if s.deletes.Add(1) == s.failAt {
		return s.err
	}
	return s.BlobStore.DeleteBlock(txn, slot, hash, id)
}

func rawBlockTip(b chain.RawBlock) ochainsync.Tip {
	return ochainsync.Tip{
		Point:       ocommon.NewPoint(b.Slot, b.Hash),
		BlockNumber: b.BlockNumber,
	}
}

// TestRecoverCommitTimestampConflictRewindsChainManagerTip reproduces the
// startup order in node.go and node_lifecycle.go: the chain manager loads its
// tip from the newest stored block, then recovery trims the blocks above the
// metadata tip. The chain manager must follow the trim, or it keeps naming a
// deleted block and no block can be added after it again.
func TestRecoverCommitTimestampConflictRewindsChainManagerTip(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		securityParam int // 0: SetLedger not called yet
	}{
		{name: "after SetLedger", securityParam: 2},
		{name: "before SetLedger", securityParam: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw, db := commitTimestampRecoveryFixture(t)
			cm, err := chain.NewManager(db, nil)
			require.NoError(t, err)
			if tc.securityParam > 0 {
				require.NoError(t, cm.SetLedger(
					testSecurityParamLedger{securityParam: tc.securityParam},
				))
			}
			require.Equal(t, rawBlockTip(raw[4]), cm.PrimaryChain().Tip())
			ls := &LedgerState{
				db:    db,
				chain: cm.PrimaryChain(),
				config: LedgerStateConfig{
					ChainManager: cm,
					Logger: slog.New(
						slog.NewTextHandler(io.Discard, nil),
					),
				},
			}

			require.NoError(t, ls.RecoverCommitTimestampConflict())

			require.Equal(t, rawBlockTip(raw[2]), ls.chain.Tip())
			_, err = ls.chain.BlockByPoint(ls.chain.Tip().Point, nil)
			require.NoError(t, err, "chain tip block must be stored")
			for _, b := range raw[3:] {
				_, err := database.BlockByPoint(
					db,
					ocommon.NewPoint(b.Slot, b.Hash),
				)
				require.Error(
					t,
					err,
					"block at slot %d must be trimmed",
					b.Slot,
				)
			}
			// The node refetches the trimmed blocks; they must extend the
			// chain again.
			require.NoError(t, ls.chain.AddRawBlocks(raw[3:]))
			require.Equal(t, rawBlockTip(raw[4]), ls.chain.Tip())
		})
	}
}

// TestRecoverCommitTimestampConflictKeepsChainWhenRewindRefused covers a gap
// the chain manager refuses to rewind (deeper than K). Recovery must then leave
// the blocks above the metadata tip in place, a forward extension the ledger
// replays, rather than delete them underneath the chain manager's tip.
func TestRecoverCommitTimestampConflictKeepsChainWhenRewindRefused(
	t *testing.T,
) {
	t.Parallel()

	raw, db := commitTimestampRecoveryFixture(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 1}))
	ls := &LedgerState{
		db:    db,
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			ChainManager: cm,
			Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}

	require.NoError(t, ls.RecoverCommitTimestampConflict())

	require.Equal(t, rawBlockTip(raw[4]), ls.chain.Tip())
	for _, b := range raw {
		_, err := ls.chain.BlockByPoint(ocommon.NewPoint(b.Slot, b.Hash), nil)
		require.NoError(t, err, "block at slot %d must remain", b.Slot)
	}
}

func TestRecoverCommitTimestampConflictFailsClosedOnLaterDeleteError(
	t *testing.T,
) {
	t.Parallel()

	raw, db := commitTimestampRecoveryFixture(t)
	injectedErr := errors.New("injected second block delete failure")
	failing := &nthDeleteFailingBlobStore{
		BlobStore: db.Blob(),
		failAt:    2,
		err:       injectedErr,
	}
	db.SetBlobStore(failing)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))
	ls := &LedgerState{
		db:    db,
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			ChainManager: cm,
			Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.armContinuationAudit(rawBlockTip(raw[1]).Point, "test rollback")

	err = ls.RecoverCommitTimestampConflict()

	require.ErrorIs(t, err, injectedErr)
	require.EqualValues(t, 2, failing.deletes.Load())
	require.Nil(t, ls.continuationAudit.Load())
	_, err = database.BlockByPoint(
		db,
		ocommon.NewPoint(raw[3].Slot, raw[3].Hash),
	)
	require.NoError(t, err, "recovery must stop before cleanup deletes more blocks")
}

func TestRecoverCommitTimestampConflictSerializesTipDecisionWithCleanup(
	t *testing.T,
) {
	t.Parallel()

	raw, db := commitTimestampRecoveryFixtureAtTip(t, 4)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))
	ls := &LedgerState{
		db:    db,
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			ChainManager: cm,
			Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	nextHash := testHashBytes("commit-ts-recovery-concurrent-add")
	next := chain.RawBlock{
		Slot:        raw[4].Slot + 10,
		Hash:        nextHash,
		BlockNumber: raw[4].BlockNumber + 1,
		Type:        1,
		PrevHash:    raw[4].Hash,
		Cbor:        []byte{0x80},
	}
	ls.beforeCommitRecoveryMutationBarrier = func() {
		require.NoError(t, ls.chain.AddRawBlocks([]chain.RawBlock{next}))
	}

	require.NoError(t, ls.RecoverCommitTimestampConflict())

	require.Equal(t, rawBlockTip(raw[4]), ls.chain.Tip())
	_, err = database.BlockByPoint(
		db,
		ocommon.NewPoint(next.Slot, next.Hash),
	)
	require.Error(t, err)
}

func TestRecoverCommitTimestampConflictSettlesContinuationAudit(
	t *testing.T,
) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		forkIndex int
		wantAudit bool
	}{
		{name: "fork point survives rewind", forkIndex: 1, wantAudit: true},
		{name: "fork point is truncated", forkIndex: 3, wantAudit: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw, db := commitTimestampRecoveryFixture(t)
			cm, err := chain.NewManager(db, nil)
			require.NoError(t, err)
			require.NoError(t, cm.SetLedger(
				testSecurityParamLedger{securityParam: 2},
			))
			ls := &LedgerState{
				db:    db,
				chain: cm.PrimaryChain(),
				config: LedgerStateConfig{
					ChainManager: cm,
					Logger: slog.New(
						slog.NewTextHandler(io.Discard, nil),
					),
				},
			}
			forkPoint := rawBlockTip(raw[tc.forkIndex]).Point
			ls.armContinuationAudit(forkPoint, "test rollback")

			require.NoError(t, ls.RecoverCommitTimestampConflict())

			window := ls.continuationAudit.Load()
			if tc.wantAudit {
				require.NotNil(t, window)
				require.True(t, pointMatches(
					window.forkPoint,
					rawBlockTip(raw[2]).Point,
				))
			} else {
				require.Nil(t, window)
			}
		})
	}
}
