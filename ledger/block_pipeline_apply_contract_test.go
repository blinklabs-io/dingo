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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/pipeline"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// The tests in this file pin the properties of
// github.com/blinklabs-io/gouroboros/pipeline.ApplyStage that
// ARCHITECTURE.md's "Why dingo's ledger apply is not wired into
// pipeline.ApplyFunc" decision (issue #3227) rests on. They deliberately
// assert upstream behavior rather than dingo behavior: the decision is only
// sound while ApplyFunc keeps this contract, so a gouroboros bump that
// changes it must fail here rather than silently invalidate a recorded
// architecture decision. If one of these fails, revisit the decision, not
// the test.

// applyContractRecorder records every ApplyFunc invocation, in call order,
// and detects any concurrent overlap between invocations.
type applyContractRecorder struct {
	mu        sync.Mutex
	sequences []uint64
	inFlight  atomic.Int32
	overlaps  atomic.Int32
	// failAt makes ApplyFunc return an error for that one sequence number,
	// but only when failSet is true.
	failAt   uint64
	failSet  bool
	applyErr error
	// blockFor makes each apply take measurable time, so a drain barrier
	// can be observed waiting for it.
	blockFor  time.Duration
	callCount atomic.Int32
}

var errApplyContractInjected = errors.New("injected apply failure")

func (r *applyContractRecorder) applyFunc(item *pipeline.BlockItem) error {
	if r.inFlight.Add(1) > 1 {
		r.overlaps.Add(1)
	}
	defer r.inFlight.Add(-1)
	r.callCount.Add(1)
	r.mu.Lock()
	r.sequences = append(r.sequences, item.SequenceNumber())
	r.mu.Unlock()
	if r.blockFor > 0 {
		time.Sleep(r.blockFor)
	}
	if r.failSet && item.SequenceNumber() == r.failAt {
		return r.applyErr
	}
	return nil
}

func (r *applyContractRecorder) appliedSequences() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint64(nil), r.sequences...)
}

// startApplyContractPipeline builds a decode-only pipeline (matching dingo's
// production shape minus the validate workers, which are irrelevant here)
// wired to the supplied recorder, and drains Results() and Errors() for its
// lifetime so neither bounded channel can wedge a stage.
func startApplyContractPipeline(
	t *testing.T,
	rec *applyContractRecorder,
	workers int,
) (*pipeline.BlockPipeline, <-chan *pipeline.BlockItem) {
	t.Helper()
	p := pipeline.NewBlockPipeline(
		pipeline.WithDecodeWorkers(workers),
		pipeline.WithApplyFunc(rec.applyFunc),
	)
	require.NoError(t, p.Start(t.Context()))
	drained := make(chan *pipeline.BlockItem, 256)
	errsDone := make(chan struct{})
	go func() {
		defer close(errsDone)
		//nolint:revive // draining is the point; the values are unused
		for range p.Errors() {
		}
	}()
	resultsDone := make(chan struct{})
	go func() {
		defer close(resultsDone)
		for item := range p.Results() {
			drained <- item
		}
		close(drained)
	}()
	t.Cleanup(func() {
		require.NoError(t, p.Stop())
		testutil.RequireReceive(
			t, errsDone, 5*time.Second, "errors drain did not exit",
		)
		testutil.RequireReceive(
			t, resultsDone, 5*time.Second, "results drain did not exit",
		)
	})
	return p, drained
}

func submitContractBlock(
	t *testing.T,
	p *pipeline.BlockPipeline,
	slot uint64,
	cbor []byte,
) {
	t.Helper()
	require.NoError(t, p.Submit(
		context.Background(),
		gledger.BlockTypeConway,
		cbor,
		ocommon.Tip{
			Point:       ocommon.NewPoint(slot, []byte{byte(slot)}),
			BlockNumber: slot + 1,
		},
	))
}

// TestPipelineApplyFuncIsSerialAndInSubmissionOrder pins the one property that
// makes ApplyFunc a candidate for ledger apply at all: it runs on a single
// goroutine, strictly in submission-sequence order, regardless of how many
// decode workers finished out of order ahead of it.
func TestPipelineApplyFuncIsSerialAndInSubmissionOrder(t *testing.T) {
	const numBlocks = 32
	rec := &applyContractRecorder{}
	p, drained := startApplyContractPipeline(t, rec, 4)
	for i := range numBlocks {
		slot := uint64(i) //nolint:gosec // small test index
		submitContractBlock(
			t, p, slot,
			testutil.BuildDecodableConwayBlockBytes(t, slot, slot+1),
		)
	}
	require.NoError(t, p.Fence(t.Context()))

	seqs := rec.appliedSequences()
	require.Len(t, seqs, numBlocks, "every submitted block must be applied")
	for i, seq := range seqs {
		require.Equal(
			t, uint64(i), seq, //nolint:gosec // small test index
			"ApplyFunc must be called in strict submission order",
		)
	}
	require.Zero(
		t,
		rec.overlaps.Load(),
		"ApplyFunc invocations must never overlap",
	)
	// Results still arrive in the same order after apply.
	for i := range numBlocks {
		item := testutil.RequireReceive(
			t, drained, 5*time.Second, "results drain",
		)
		require.Equal(t, uint64(i), item.SequenceNumber()) //nolint:gosec
		require.True(t, item.IsApplied())
	}
}

// TestPipelineApplyFuncErrorDoesNotStopLaterBlocks is the decisive contract
// fact behind #3227's decision. ApplyStage records a failing item's error on
// the item and pushes it onto the (log-only) errors channel, then goes right
// on to apply every following block. A ledger cannot do that: block N+1's
// state depends on block N, so a failure at N must stop the batch. There is no
// option on the stage to make it stop, and the error never reaches the
// submitter synchronously, so dingo's errRestartLedgerPipeline /
// errStaleChainIterator retry contract cannot be expressed through ApplyFunc.
func TestPipelineApplyFuncErrorDoesNotStopLaterBlocks(t *testing.T) {
	const numBlocks = 8
	const failSeq = 2
	rec := &applyContractRecorder{
		failAt:   failSeq,
		failSet:  true,
		applyErr: errApplyContractInjected,
	}
	p, drained := startApplyContractPipeline(t, rec, 2)
	for i := range numBlocks {
		slot := uint64(i) //nolint:gosec // small test index
		submitContractBlock(
			t, p, slot,
			testutil.BuildDecodableConwayBlockBytes(t, slot, slot+1),
		)
	}
	require.NoError(t, p.Fence(t.Context()))

	seqs := rec.appliedSequences()
	require.Len(
		t,
		seqs,
		numBlocks,
		"ApplyStage keeps applying after a failed block; if this ever "+
			"stops at the failure, ARCHITECTURE.md's #3227 decision "+
			"must be revisited",
	)
	var failedItem *pipeline.BlockItem
	for range numBlocks {
		item := testutil.RequireReceive(
			t, drained, 5*time.Second, "results drain",
		)
		if item.SequenceNumber() == failSeq {
			failedItem = item
			continue
		}
		require.True(
			t,
			item.IsApplied(),
			"block after a failed apply is still applied (seq %d)",
			item.SequenceNumber(),
		)
	}
	require.NotNil(t, failedItem)
	require.False(t, failedItem.IsApplied())
	require.ErrorIs(t, failedItem.ApplyError(), errApplyContractInjected)
}

// TestPipelineApplyFuncSkipsUndecodableBlockButAppliesTheNext pins the second
// blocking contract fact: an item that failed decode is skipped by
// maybeApply, but its sequence slot is consumed and the *following* block is
// applied normally. Dingo's decodeReadChainBatchWithError instead discards the
// whole batch on any decode failure, so it never hands a partially-decoded
// batch to apply. Wiring apply into the stage would replace batch-discard with
// apply-the-rest, which is a chain-continuity violation.
func TestPipelineApplyFuncSkipsUndecodableBlockButAppliesTheNext(t *testing.T) {
	rec := &applyContractRecorder{}
	p, drained := startApplyContractPipeline(t, rec, 1)
	submitContractBlock(t, p, 0, testutil.BuildDecodableConwayBlockBytes(t, 0, 1))
	submitContractBlock(t, p, 1, []byte{0xff, 0xff, 0xff, 0xff})
	submitContractBlock(t, p, 2, testutil.BuildDecodableConwayBlockBytes(t, 2, 3))
	require.NoError(t, p.Fence(t.Context()))

	require.Equal(
		t,
		[]uint64{0, 2},
		rec.appliedSequences(),
		"the undecodable block is skipped but the one after it is applied",
	)
	var sawDecodeError bool
	for range 3 {
		item := testutil.RequireReceive(
			t, drained, 5*time.Second, "results drain",
		)
		if item.SequenceNumber() == 1 {
			require.Error(t, item.DecodeError())
			require.False(t, item.IsApplied())
			sawDecodeError = true
		}
	}
	require.True(t, sawDecodeError)
}

// TestPipelineWaitForDrainCoversApplyFunc pins the third contract fact, the
// one that ties #3227 to #3840. drainBlockPipelineBeforeRollback bounds its
// WaitForDrain at BlockPipelineRollbackDrainTimeout and, on timeout, logs and
// *proceeds with the rollback anyway*. That is safe today because a nil
// ApplyFunc means the only work the barrier covers is decode and
// re-sequencing, and the real ledger apply stays behind the reader
// goroutine's in-order readChainResult handshake. If ApplyFunc did the ledger
// apply, WaitForDrain would be covering database work, so a timeout would let
// a rollback run while blocks were still being applied to the ledger --
// exactly the "mutation survives a rollback" class of #3771/#3840.
func TestPipelineWaitForDrainCoversApplyFunc(t *testing.T) {
	rec := &applyContractRecorder{blockFor: 50 * time.Millisecond}
	p, _ := startApplyContractPipeline(t, rec, 2)
	const numBlocks = 4
	for i := range numBlocks {
		slot := uint64(i) //nolint:gosec // small test index
		submitContractBlock(
			t, p, slot,
			testutil.BuildDecodableConwayBlockBytes(t, slot, slot+1),
		)
	}
	// A drain barrier shorter than the apply work it covers times out.
	shortCtx, shortCancel := context.WithTimeout(
		t.Context(), 10*time.Millisecond,
	)
	defer shortCancel()
	require.Error(
		t,
		p.WaitForDrain(shortCtx),
		"WaitForDrain must wait for ApplyFunc, so a barrier shorter than "+
			"the apply work times out",
	)
	require.Less(
		t,
		int(rec.callCount.Load()),
		numBlocks,
		"apply was still in progress when the short barrier expired",
	)
	// The same barrier with enough time covers every apply.
	require.NoError(t, p.WaitForDrain(t.Context()))
	require.Equal(t, numBlocks, int(rec.callCount.Load()))
}
