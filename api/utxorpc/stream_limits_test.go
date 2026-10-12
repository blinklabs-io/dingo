// Copyright 2025 Blink Labs Software
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

package utxorpc

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit/submitconnect"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync/syncconnect"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch/watchconnect"
)

func TestStreamLimiterEnforcesTotalAndPerClientCaps(t *testing.T) {
	t.Parallel()

	l := newStreamLimiter(3, 2)
	relA1, err := l.acquire("10.0.0.1:1000")
	require.NoError(t, err)
	_, err = l.acquire("10.0.0.1:2000")
	require.NoError(t, err)

	// Same host, different port: the per-client cap applies.
	_, err = l.acquire("10.0.0.1:3000")
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))

	relB, err := l.acquire("10.0.0.2:1000")
	require.NoError(t, err)

	// Total cap reached for a client under its own cap.
	_, err = l.acquire("10.0.0.3:1000")
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))

	relA1()
	relA1() // releasing twice must not free a second slot
	_, err = l.acquire("10.0.0.3:1000")
	require.NoError(t, err)
	_, err = l.acquire("10.0.0.4:1000")
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
	relB()
}

func TestConnect_StreamsRefusedAtAdmissionLimit(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{
		numBlocks: 25,
		tune: func(cfg *UtxorpcConfig) {
			cfg.MaxStreams = 1
			cfg.ServerTimeout = time.Second
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	release, err := h.U.streams.acquire("127.0.0.1:1")
	require.NoError(t, err)

	syncClient := syncconnect.NewSyncServiceClient(
		h.Client, h.Server.URL, connect.WithGRPC(),
	)
	follow, err := syncClient.FollowTip(
		ctx, connect.NewRequest(&sync.FollowTipRequest{}),
	)
	require.NoError(t, err)
	require.False(t, follow.Receive())
	assert.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(follow.Err()))

	watchClient := watchconnect.NewWatchServiceClient(
		h.Client, h.Server.URL, connect.WithGRPC(),
	)
	watchTx, err := watchClient.WatchTx(
		ctx, connect.NewRequest(&watch.WatchTxRequest{}),
	)
	require.NoError(t, err)
	require.False(t, watchTx.Receive())
	assert.Equal(
		t,
		connect.CodeResourceExhausted,
		connect.CodeOf(watchTx.Err()),
	)

	submitClient := submitconnect.NewSubmitServiceClient(
		h.Client, h.Server.URL, connect.WithGRPC(),
	)
	watchMempool, err := submitClient.WatchMempool(
		ctx, connect.NewRequest(&submit.WatchMempoolRequest{}),
	)
	require.NoError(t, err)
	require.False(t, watchMempool.Receive())
	assert.Equal(
		t, connect.CodeResourceExhausted, connect.CodeOf(watchMempool.Err()),
	)
	waitForTx, err := submitClient.WaitForTx(
		ctx,
		connect.NewRequest(
			&submit.WaitForTxRequest{Ref: [][]byte{make([]byte, 32)}},
		),
	)
	require.NoError(t, err)
	require.False(t, waitForTx.Receive())
	assert.Equal(
		t,
		connect.CodeResourceExhausted,
		connect.CodeOf(waitForTx.Err()),
	)

	// With the slot free, a stream is admitted again.
	release()
	blocks := loadTestChainBlocks(t, 25)
	stream := startWatchTxAt(t, ctx, h, blocks[len(blocks)-3])
	require.True(t, stream.Receive(), "stream refused: %v", stream.Err())
}

func TestConnect_WatchPredicateNodeBudget(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{
		numBlocks: 25,
		tune:      func(cfg *UtxorpcConfig) { cfg.MaxPredicateNodes = 5 },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	blocks := loadTestChainBlocks(t, 25)
	wideWatch := func(children int) *watch.TxPredicate {
		p := &watch.TxPredicate{}
		for range children {
			p.AllOf = append(p.AllOf, &watch.TxPredicate{})
		}
		return p
	}
	watchClient := watchconnect.NewWatchServiceClient(
		h.Client, h.Server.URL, connect.WithGRPC(),
	)
	watchWith := func(children int) *connect.ServerStreamForClient[watch.WatchTxResponse] {
		last := blocks[len(blocks)-3]
		stream, err := watchClient.WatchTx(ctx, connect.NewRequest(
			&watch.WatchTxRequest{
				Predicate: wideWatch(children),
				Intersect: []*watch.BlockRef{{
					Slot: last.Slot, Hash: last.Hash, Height: last.Number,
				}},
			},
		))
		require.NoError(t, err)
		return stream
	}

	// Root plus four children is exactly five nodes.
	atLimit := watchWith(4)
	require.True(t, atLimit.Receive(), "stream refused: %v", atLimit.Err())

	over := watchWith(5)
	require.False(t, over.Receive())
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(over.Err()))

	submitClient := submitconnect.NewSubmitServiceClient(
		h.Client, h.Server.URL, connect.WithGRPC(),
	)
	sp := &submit.TxPredicate{}
	for range 5 {
		sp.AllOf = append(sp.AllOf, &submit.TxPredicate{})
	}
	mempoolStream, err := submitClient.WatchMempool(ctx, connect.NewRequest(
		&submit.WatchMempoolRequest{Predicate: sp},
	))
	require.NoError(t, err)
	require.False(t, mempoolStream.Receive())
	assert.Equal(
		t, connect.CodeInvalidArgument, connect.CodeOf(mempoolStream.Err()),
	)
}

func TestConnect_WatchTxReplayBoundedByBlocks(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{
		numBlocks: 25,
		tune:      func(cfg *UtxorpcConfig) { cfg.MaxReplayBlocks = 3 },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	blocks := loadTestChainBlocks(t, 25)
	tip := len(blocks) - 1

	atLimit := startWatchTxAt(t, ctx, h, blocks[tip-3])
	require.True(t, atLimit.Receive(), "stream refused: %v", atLimit.Err())

	tooOld := startWatchTxAt(t, ctx, h, blocks[tip-4])
	require.False(t, tooOld.Receive())
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(tooOld.Err()))

	watchClient := watchconnect.NewWatchServiceClient(
		h.Client, h.Server.URL, connect.WithGRPC(),
	)
	origin, err := watchClient.WatchTx(
		ctx,
		connect.NewRequest(&watch.WatchTxRequest{
			Intersect: []*watch.BlockRef{{}},
		}),
	)
	require.NoError(t, err)
	require.False(t, origin.Receive())
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(origin.Err()))
}

func TestMempoolStreamQueueRefusesSlowConsumer(t *testing.T) {
	t.Parallel()

	q := newMempoolStreamQueue(2)
	require.NoError(t, q.offer(&submit.WatchMempoolResponse{}))
	require.NoError(t, q.offer(&submit.WatchMempoolResponse{}))
	err := q.offer(&submit.WatchMempoolResponse{})
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))

	<-q.responses
	require.NoError(t, q.offer(&submit.WatchMempoolResponse{}))
}
