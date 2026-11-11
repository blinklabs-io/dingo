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
	"errors"
	"fmt"
	"math"
	"sync"

	"connectrpc.com/connect"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit"
)

// streamLimiter admits long-lived streams (FollowTip, WatchTx, WatchMempool,
// WaitForTx) under a process-wide and a per-client cap. A cap of zero or less
// is not applied. A client is the remote IPv4
// address or IPv6 /64 prefix (see clientKey), so clients behind one proxy
// share a budget.
type streamLimiter struct {
	mu           sync.Mutex
	total        int
	perClient    map[string]int
	maxTotal     int
	maxPerClient int
	resource     string
}

func newStreamLimiter(maxTotal, maxPerClient int) *streamLimiter {
	return &streamLimiter{
		perClient:    make(map[string]int),
		maxTotal:     maxTotal,
		maxPerClient: maxPerClient,
		resource:     "stream",
	}
}

func newRequestLimiter(
	maxTotal, maxPerClient int,
	resource string,
) *streamLimiter {
	return &streamLimiter{
		perClient:    make(map[string]int),
		maxTotal:     maxTotal,
		maxPerClient: maxPerClient,
		resource:     resource,
	}
}

// acquire reserves a stream slot for the client at peerAddr and returns the
// function that releases it. A refused stream gets a ResourceExhausted error.
func (l *streamLimiter) acquire(peerAddr string) (func(), error) {
	client := clientKey(peerAddr)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.maxTotal > 0 && l.total >= l.maxTotal {
		return nil, connect.NewError(
			connect.CodeResourceExhausted,
			fmt.Errorf(
				"server %s limit of %d reached",
				l.resource,
				l.maxTotal,
			),
		)
	}
	if l.maxPerClient > 0 && l.perClient[client] >= l.maxPerClient {
		return nil, connect.NewError(
			connect.CodeResourceExhausted,
			fmt.Errorf(
				"per-client %s limit of %d reached",
				l.resource,
				l.maxPerClient,
			),
		)
	}
	l.total++
	l.perClient[client]++
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			l.total--
			if l.perClient[client]--; l.perClient[client] == 0 {
				delete(l.perClient, client)
			}
		})
	}, nil
}

// mempoolStreamQueue hands WatchMempool responses from the event bus
// callback to the request goroutine, the only stream sender. offer never
// blocks, so a slow client cannot hold up event delivery. A positive limit
// cuts off a client that lets that many responses queue; zero is unbounded.
type mempoolStreamQueue struct {
	mu      sync.Mutex
	pending []*submit.WatchMempoolResponse
	limit   int
	notify  chan struct{}
}

func newMempoolStreamQueue(limit int) *mempoolStreamQueue {
	return &mempoolStreamQueue{
		limit:  limit,
		notify: make(chan struct{}, 1),
	}
}

func (q *mempoolStreamQueue) offer(resp *submit.WatchMempoolResponse) error {
	q.mu.Lock()
	if q.limit > 0 && len(q.pending) >= q.limit {
		q.mu.Unlock()
		return connect.NewError(
			connect.CodeResourceExhausted,
			errors.New("client is too slow to receive mempool transactions"),
		)
	}
	q.pending = append(q.pending, resp)
	q.mu.Unlock()
	select {
	case q.notify <- struct{}{}:
	default:
	}
	return nil
}

// drain returns every queued response in arrival order.
func (q *mempoolStreamQueue) drain() []*submit.WatchMempoolResponse {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := q.pending
	q.pending = nil
	return out
}

// admitStream reserves a stream slot for the calling client; the returned
// function releases it when the stream ends.
func (u *Utxorpc) admitStream(peer connect.Peer) (func(), error) {
	return u.streams.acquire(peer.Addr)
}

func (u *Utxorpc) admitEvalRequest(peer connect.Peer) (func(), error) {
	return u.evalRequests.acquire(peer.Addr)
}

func predicateBudgetError(limit int) error {
	return connect.NewError(
		connect.CodeInvalidArgument,
		fmt.Errorf("predicate exceeds the maximum of %d nodes", limit),
	)
}

// checkReplayDistance refuses a WatchTx start point (when MaxReplayBlocks is set) more than MaxReplayBlocks
// behind the tip, bounding the history evaluated before the stream goes live.
func (u *Utxorpc) checkReplayDistance(point ocommon.Point) error {
	if u.config.MaxReplayBlocks <= 0 {
		return nil
	}
	var pointHeight uint64
	if len(point.Hash) > 0 {
		block, err := u.config.LedgerState.GetBlock(point)
		if err != nil {
			return err
		}
		pointHeight = block.Number
	}
	tipHeight := u.config.LedgerState.Tip().BlockNumber
	// #nosec G115 -- MaxReplayBlocks is positive here
	if tipHeight > pointHeight &&
		tipHeight-pointHeight > uint64(u.config.MaxReplayBlocks) {
		return connect.NewError(
			connect.CodeInvalidArgument,
			fmt.Errorf(
				"intersect is %d blocks behind the tip, exceeding the replay limit of %d",
				tipHeight-pointHeight,
				u.config.MaxReplayBlocks,
			),
		)
	}
	return nil
}

// predicateNodeBudget is the node count a watch predicate may have; an unset
// MaxPredicateNodes allows any size.
func (u *Utxorpc) predicateNodeBudget() int {
	if u.config.MaxPredicateNodes <= 0 {
		return math.MaxInt
	}
	return u.config.MaxPredicateNodes
}
