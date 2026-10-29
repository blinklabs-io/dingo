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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/require"
)

func TestLedgerReadChainIteratorFlushesDrainedNetworkFrontier(t *testing.T) {
	// Not t.Parallel: changes the shared gather coalescing interval.
	// A regressed reader waits minutes; the receive deadline is only a failure
	// bound, not a scheduler-sensitive performance assertion.
	shrinkGatherCoalesceRetryInterval(t, 10*time.Minute, 1)
	block, point := buildDecodableTestBlock(t, 10, 1)
	cm, err := chain.NewManager(t.Context(), newTestDB(t), nil)
	require.NoError(t, err)
	pc := cm.PrimaryChain()
	decoded, err := block.Decode()
	require.NoError(t, err)
	require.NoError(t, pc.AddBlockHeader(t.Context(), decoded.Header()))
	require.Equal(t, 1, pc.HeaderCount())
	ls := &LedgerState{chain: pc}
	probe := &gatherSpanLockProbe{}
	ls.config = LedgerStateConfig{
		Logger: testLogger(), GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
			probe.record(ls, true)
			return nil
		},
	}
	ctx, cancel := context.WithCancel(t.Context())
	iter := &pausingGapLedgerReadIterator{
		ctx:        ctx,
		first:      &chain.ChainIteratorResult{Point: point, Block: block},
		gapEntered: make(chan struct{}),
		resume:     make(chan struct{}),
	}
	results := make(chan readChainResult, 1)
	done := make(chan struct{})
	go func() { defer close(done); ls.ledgerReadChainIterator(ctx, iter, results) }()
	t.Cleanup(
		func() { cancel(); testutil.RequireReceive(t, done, testutil.AsyncWait, "reader did not stop") },
	)
	testutil.RequireReceive(
		t,
		iter.gapEntered,
		testutil.AsyncWait,
		"reader never reached gap",
	)
	require.False(
		t,
		tryLockGatherMutex(ls),
		"rollback must remain excluded while a body is gathered",
	)
	// The body has exhausted its admitted network frontier. The next header may
	// depend on applying this body and publishing its validation state.
	pc.ClearHeaders()
	close(iter.resume)
	result := testutil.RequireReceive(
		t,
		results,
		10*time.Second,
		"empty network frontier waited for an impossible next body",
	)
	require.NoError(t, result.err)
	require.Len(t, result.blocks, 1)
	close(result.done)
	activeCalls, _, insideSpan := probe.counts()
	require.Positive(t, activeCalls)
	require.Zero(t, insideSpan, "peer selection must stay outside the gather lock")
}

func TestLedgerReadChainIteratorKeepsAuthorizedNetworkCoalescing(t *testing.T) {
	// Not t.Parallel: changes the shared gather coalescing interval.
	shrinkGatherCoalesceRetryInterval(t, time.Millisecond, 2)
	block1, point1 := buildDecodableTestBlock(t, 10, 1)
	block2, point2 := buildDecodableTestBlock(t, 20, 2)
	cm, err := chain.NewManager(t.Context(), newTestDB(t), nil)
	require.NoError(t, err)
	pc := cm.PrimaryChain()
	decoded, err := block2.Decode()
	require.NoError(t, err)
	require.NoError(t, pc.AddBlockHeader(t.Context(), decoded.Header()))
	ls := &LedgerState{chain: pc}
	probe := &gatherSpanLockProbe{}
	ls.config = LedgerStateConfig{
		Logger: testLogger(), GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
			probe.record(ls, true)
			return nil
		},
	}
	ctx, cancel := context.WithCancel(t.Context())
	iter := &scriptedGapLedgerReadIterator{
		ctx: ctx,
		script: []*chain.ChainIteratorResult{
			{Point: point1, Block: block1},
			nil,
			{Point: point2, Block: block2},
		},
	}
	results := make(chan readChainResult, 1)
	done := make(chan struct{})
	go func() { defer close(done); ls.ledgerReadChainIterator(ctx, iter, results) }()
	t.Cleanup(
		func() { cancel(); testutil.RequireReceive(t, done, testutil.AsyncWait, "reader did not stop") },
	)
	result := testutil.RequireReceive(
		t,
		results,
		testutil.AsyncWait,
		"authorized bodies did not coalesce",
	)
	require.NoError(t, result.err)
	require.Len(
		t,
		result.blocks,
		2,
		"an admitted header still justifies waiting across a brief body gap",
	)
	close(result.done)
	activeCalls, _, insideSpan := probe.counts()
	require.Positive(t, activeCalls)
	require.Zero(t, insideSpan, "peer selection must stay outside the gather lock")
}
