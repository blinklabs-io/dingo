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
	"net"
	"sync"

	"connectrpc.com/connect"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit"
)

// Defaults for the long-lived stream bounds on UtxorpcConfig.
const (
	DefaultMaxStreams          = 1000
	DefaultMaxStreamsPerClient = 20
	DefaultMaxPredicateNodes   = 1000
	DefaultMaxReplayBlocks     = 10000
)

// streamLimiter admits long-lived streams (FollowTip, WatchTx, WatchMempool)
// under a process-wide and a per-client cap. A client is the remote host, so
// clients behind one proxy share a budget.
type streamLimiter struct {
	mu           sync.Mutex
	total        int
	perClient    map[string]int
	maxTotal     int
	maxPerClient int
}

func newStreamLimiter(maxTotal, maxPerClient int) *streamLimiter {
	return &streamLimiter{
		perClient:    make(map[string]int),
		maxTotal:     maxTotal,
		maxPerClient: maxPerClient,
	}
}

// acquire reserves a stream slot for the client at peerAddr and returns the
// function that releases it. A refused stream gets a ResourceExhausted error.
func (l *streamLimiter) acquire(peerAddr string) (func(), error) {
	client := peerAddr
	if host, _, err := net.SplitHostPort(peerAddr); err == nil {
		client = host
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.total >= l.maxTotal {
		return nil, connect.NewError(
			connect.CodeResourceExhausted,
			fmt.Errorf("server stream limit of %d reached", l.maxTotal),
		)
	}
	if l.perClient[client] >= l.maxPerClient {
		return nil, connect.NewError(
			connect.CodeResourceExhausted,
			fmt.Errorf(
				"per-client stream limit of %d reached",
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

// txPredicateNodeCount returns the number of nodes in the predicate tree.
func txPredicateNodeCount(n *txPredicateNode) int {
	if n == nil {
		return 0
	}
	count := 1
	for _, group := range [][]*txPredicateNode{n.not, n.allOf, n.anyOf} {
		for _, child := range group {
			count += txPredicateNodeCount(child)
		}
	}
	return count
}

// mempoolStreamQueue hands WatchMempool responses from the event bus
// callback to the request goroutine, the only stream sender. offer never
// blocks, so a slow client cannot hold up event delivery; a client that lets
// the queue fill is cut off instead.
type mempoolStreamQueue struct {
	responses chan *submit.WatchMempoolResponse
}

func newMempoolStreamQueue(size int) *mempoolStreamQueue {
	return &mempoolStreamQueue{
		responses: make(chan *submit.WatchMempoolResponse, size),
	}
}

func (q *mempoolStreamQueue) offer(resp *submit.WatchMempoolResponse) error {
	select {
	case q.responses <- resp:
		return nil
	default:
		return connect.NewError(
			connect.CodeResourceExhausted,
			errors.New("client is too slow to receive mempool transactions"),
		)
	}
}

// admitStream reserves a stream slot for the calling client; the returned
// function releases it when the stream ends.
func (u *Utxorpc) admitStream(peer connect.Peer) (func(), error) {
	return u.streams.acquire(peer.Addr)
}

// checkPredicateBudget refuses a predicate whose total node count exceeds
// MaxPredicateNodes.
func (u *Utxorpc) checkPredicateBudget(n *txPredicateNode) error {
	if count := txPredicateNodeCount(n); count > u.config.MaxPredicateNodes {
		return connect.NewError(
			connect.CodeInvalidArgument,
			fmt.Errorf(
				"predicate has %d nodes, exceeding the maximum of %d",
				count,
				u.config.MaxPredicateNodes,
			),
		)
	}
	return nil
}

// checkReplayDistance refuses a WatchTx start point more than MaxReplayBlocks
// behind the tip, bounding the history evaluated before the stream goes live.
func (u *Utxorpc) checkReplayDistance(point ocommon.Point) error {
	var pointHeight uint64
	if len(point.Hash) > 0 {
		block, err := u.config.LedgerState.GetBlock(point)
		if err != nil {
			return err
		}
		pointHeight = block.Number
	}
	tipHeight := u.config.LedgerState.Tip().BlockNumber
	// #nosec G115 -- MaxReplayBlocks is positive after NewUtxorpc defaults it
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
