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
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/require"
)

// staleForkLedgerDB counts the transactions a candidate's validation opens
// and runs onSetTip when validation stages a block.
type staleForkLedgerDB struct {
	ledgerdb.LedgerDB
	begins   atomic.Int32
	onSetTip func()
}

func (d *staleForkLedgerDB) Begin(
	ctx context.Context,
	readWrite bool,
) ledgerdb.Txn {
	d.begins.Add(1)
	return d.LedgerDB.Begin(ctx, readWrite)
}

func (d *staleForkLedgerDB) SetTip(
	tip ochainsync.Tip,
	txn ledgerdb.Txn,
) error {
	err := d.LedgerDB.SetTip(tip, txn)
	if err == nil && d.onSetTip != nil {
		d.onSetTip()
	}
	return err
}

type syncLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

const switchUnavailableLog = "validated fork candidate became unavailable"

type staleForkFixture struct {
	*chainsyncRollbackFixture
	db        *staleForkLedgerDB
	logs      *syncLogBuffer
	candidate *forkCandidate
}

// newStaleForkFixture builds a candidate that validates against the fixture's
// ancestor, so a test decides what changes around its validation.
func newStaleForkFixture(t *testing.T) *staleForkFixture {
	t.Helper()
	fixture := newForkCandidateFixture(t, 432000)
	blocks, err := fixtures.GenerateAlonzoChain(
		fixture.ancestorTip.BlockNumber+1,
		common.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		fixture.currentTip.Point.Slot+9,
		1,
		2,
	)
	require.NoError(t, err)
	block := blocks[0]
	point := ocommon.NewPoint(block.SlotNumber(), block.Hash().Bytes())
	logs := &syncLogBuffer{}
	fixture.ls.config.Logger = slog.New(slog.NewJSONHandler(logs, nil))
	db := &staleForkLedgerDB{LedgerDB: fixture.ls.ledgerDB()}
	fixture.ls.ldb = db
	return &staleForkFixture{
		chainsyncRollbackFixture: fixture,
		db:                       db,
		logs:                     logs,
		candidate: &forkCandidate{
			connection: fixture.connId,
			ancestor:   fixture.ancestorTip.Point,
			localTip:   fixture.currentTip,
			generation: fixture.ls.CurrentHeaderCryptoVerificationGeneration(),
			path: []ChainsyncEvent{{
				ConnectionId: fixture.connId,
				Point:        point,
				BlockHeader:  block.Header(),
				Type:         uint(block.Type()),
			}},
			blocks: []BlockfetchEvent{{
				ConnectionId: fixture.connId, Point: point, Block: block,
			}},
		},
	}
}

func (f *staleForkFixture) bumpGeneration() {
	f.ls.Lock()
	defer f.ls.Unlock()
	f.ls.publishSnapshotsLocked()
}

// extendPrimaryChain appends a block to the selected chain and returns its
// point, moving the primary tip off the candidate's local tip.
func (f *staleForkFixture) extendPrimaryChain(t *testing.T) ocommon.Point {
	t.Helper()
	hash := testHashBytes("stale-fork-extension")
	require.NoError(t, f.ls.chain.AddRawBlocks(
		context.Background(), []chain.RawBlock{{
			Slot:        f.currentTip.Point.Slot + 1,
			Hash:        hash,
			BlockNumber: f.currentTip.BlockNumber + 1,
			Type:        1,
			PrevHash:    f.currentTip.Point.Hash,
			Cbor:        chainsyncFixtureBlockCbor,
		}},
	))
	return ocommon.NewPoint(f.currentTip.Point.Slot+1, hash)
}

// requireUnchanged asserts the candidate changed no tip and installed no
// fork headers.
func (f *staleForkFixture) requireUnchanged(
	t *testing.T,
	chainTip ocommon.Point,
) {
	t.Helper()
	require.Equal(t, chainTip, f.ls.chain.Tip().Point)
	f.ls.RLock()
	ledgerTip := f.ls.currentTip
	f.ls.RUnlock()
	require.Equal(t, f.currentTip, ledgerTip)
	dbTip, err := f.ls.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, f.currentTip.Point, dbTip.Point)
	require.Zero(t, f.ls.chain.HeaderCount())
}

// A candidate whose header generation moved before it is validated is dropped
// without opening a validation transaction.
func TestFinishForkCandidateSkipsValidationAfterGenerationChange(t *testing.T) {
	t.Parallel()
	f := newStaleForkFixture(t)
	f.bumpGeneration()

	f.ls.finishForkCandidate(f.candidate)

	require.Zero(t, f.db.begins.Load(), "a stale candidate must not be validated")
	f.requireUnchanged(t, f.currentTip.Point)
	require.NotContains(t, f.logs.String(), switchUnavailableLog)
}

// A candidate whose selected chain moved during the fetch is dropped without
// opening a validation transaction.
func TestFinishForkCandidateSkipsValidationAfterTipMove(t *testing.T) {
	t.Parallel()
	f := newStaleForkFixture(t)
	moved := f.extendPrimaryChain(t)

	f.ls.finishForkCandidate(f.candidate)

	require.Zero(t, f.db.begins.Load(), "a stale candidate must not be validated")
	f.requireUnchanged(t, moved)
	require.NotContains(t, f.logs.String(), switchUnavailableLog)
}

// The generation can move while the candidate validates. The switch is then
// skipped before it reaches the rollback.
func TestFinishForkCandidateSkipsSwitchAfterGenerationChangeDuringValidation(
	t *testing.T,
) {
	t.Parallel()
	f := newStaleForkFixture(t)
	f.db.onSetTip = f.bumpGeneration

	f.ls.finishForkCandidate(f.candidate)

	require.Positive(t, f.db.begins.Load(), "the candidate must have validated")
	f.requireUnchanged(t, f.currentTip.Point)
	require.NotContains(t, f.logs.String(), switchUnavailableLog)
}

// The primary tip can move after the candidate validates and before the
// switch takes the chain lock. The switch is then skipped before it reaches
// the rollback.
func TestFinishForkCandidateSkipsSwitchAfterTipMoveDuringValidation(
	t *testing.T,
) {
	t.Parallel()
	f := newStaleForkFixture(t)
	validated := make(chan struct{})
	f.db.onSetTip = func() { close(validated) }

	f.ls.chainsyncMutex.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.ls.finishForkCandidate(f.candidate)
	}()
	<-validated
	moved := f.extendPrimaryChain(t)
	f.ls.chainsyncMutex.Unlock()
	<-done

	require.Positive(t, f.db.begins.Load(), "the candidate must have validated")
	f.requireUnchanged(t, moved)
	require.NotContains(t, f.logs.String(), switchUnavailableLog)
}
