//go:build linux && devnet

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

package devnet

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	// observerDialTimeout bounds a single connection attempt.
	observerDialTimeout = 10 * time.Second
	// observerMinBackoff and observerMaxBackoff bound the reconnect
	// delay. The scenario deliberately stops containers, so a node being
	// unreachable is an expected state to sit in, not a failure — but
	// recovery has to be picked up quickly enough that the recovery
	// budget measures the node rather than the observer.
	observerMinBackoff = 250 * time.Millisecond
	observerMaxBackoff = 2 * time.Second
)

// ChainObservers holds one persistent ChainSync session per node.
//
// This replaces opening a fresh Node-to-Node connection per tip query.
// A short-lived connection can only ever report the tip captured when it
// intersected, which is why the polling harness had to reconnect on every
// check; a session that stays up and follows the chain instead receives
// RollForward and RollBackward as the node produces them. Scenario
// assertions then wait on observed protocol events with a bounded
// context rather than sampling on an interval.
type ChainObservers struct {
	group     *ChainGroup
	endpoints map[string]NodeEndpoint
	magic     uint32
	logf      func(format string, args ...any)

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// StartObservers opens a ChainSync session to every endpoint and follows
// each node's chain until Stop is called or ctx is cancelled. Sessions
// reconnect on their own, so a node that is stopped and started again
// resumes without the caller doing anything.
func StartObservers(
	ctx context.Context,
	endpoints []NodeEndpoint,
	magic uint32,
	logf func(format string, args ...any),
) *ChainObservers {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	names := make([]string, 0, len(endpoints))
	byName := make(map[string]NodeEndpoint, len(endpoints))
	for _, ep := range endpoints {
		names = append(names, ep.Name)
		byName[ep.Name] = ep
	}
	runCtx, cancel := context.WithCancel(ctx)
	o := &ChainObservers{
		group:     NewChainGroup(names...),
		endpoints: byName,
		magic:     magic,
		logf:      logf,
		cancel:    cancel,
	}
	for _, ep := range endpoints {
		o.wg.Add(1)
		go func(ep NodeEndpoint) {
			defer o.wg.Done()
			o.follow(runCtx, ep)
		}(ep)
	}
	return o
}

// Group returns the observed chains, for waiting on conditions.
func (o *ChainObservers) Group() *ChainGroup { return o.group }

// Chain returns one node's observed chain.
func (o *ChainObservers) Chain(node string) *ObservedChain {
	return o.group.Chain(node)
}

// Endpoint returns the endpoint definition for a node.
func (o *ChainObservers) Endpoint(node string) (NodeEndpoint, bool) {
	ep, ok := o.endpoints[node]
	return ep, ok
}

// Stop tears every session down and waits for the goroutines to exit.
func (o *ChainObservers) Stop() {
	o.cancel()
	o.wg.Wait()
}

// follow keeps one node's session alive for the life of the scenario.
func (o *ChainObservers) follow(ctx context.Context, ep NodeEndpoint) {
	chain := o.group.Chain(ep.Name)
	if chain == nil {
		return
	}
	backoff := observerMinBackoff
	for ctx.Err() == nil {
		established, err := o.session(ctx, ep, chain)
		if ctx.Err() != nil {
			return
		}
		chain.Disconnected(err)
		o.logf(
			"observer %s: session ended (%v); reconnecting in %s",
			ep.Name, err, backoff,
		)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		// Back off only against a node that will not talk to us at all.
		// A session that got as far as following the chain and then
		// dropped — which is exactly what a restarted node looks like —
		// starts over at the shortest delay, so the recovery budget
		// measures the node rather than the observer's patience.
		if established {
			backoff = observerMinBackoff
		} else {
			backoff = min(2*backoff, observerMaxBackoff)
		}
	}
}

// session runs one connection from dial to teardown. It reports whether
// the session got as far as following the chain, and the reason it ended;
// a cancelled context returns a nil error.
func (o *ChainObservers) session(
	ctx context.Context,
	ep NodeEndpoint,
	chain *ObservedChain,
) (established bool, err error) {
	dialer := &net.Dialer{Timeout: observerDialTimeout}
	rawConn, err := dialer.DialContext(ctx, "tcp", ep.Address)
	if err != nil {
		return false, fmt.Errorf("dial %s: %w", ep.Address, err)
	}

	rollForward := func(
		_ chainsync.CallbackContext,
		_ uint,
		headerAny any,
		tip chainsync.Tip,
	) error {
		header, ok := headerAny.(lcommon.BlockHeader)
		if !ok {
			return fmt.Errorf(
				"observer %s: unexpected header type %T", ep.Name, headerAny,
			)
		}
		hash := header.Hash()
		chain.RollForward(
			ObservedHeader{
				Slot:        header.SlotNumber(),
				BlockNumber: header.BlockNumber(),
				Hash:        hash.Bytes(),
				BodySize:    header.BlockBodySize(),
			},
			tipFrom(tip),
		)
		return nil
	}
	rollBackward := func(
		_ chainsync.CallbackContext,
		point pcommon.Point,
		tip chainsync.Tip,
	) error {
		chain.RollBackward(
			ChainPoint{Slot: point.Slot, Hash: point.Hash},
			tipFrom(tip),
		)
		return nil
	}

	conn, err := ouroboros.NewConnection(
		ouroboros.WithConnection(rawConn),
		ouroboros.WithNetworkMagic(o.magic),
		ouroboros.WithNodeToNode(true),
		// A session outlives many blocks, so it needs keep-alive to stay
		// up through the quiet stretches the scenario creates when it
		// stops a producer.
		ouroboros.WithKeepAlive(true),
		ouroboros.WithChainSyncConfig(chainsync.NewConfig(
			chainsync.WithRollForwardFunc(rollForward),
			chainsync.WithRollBackwardFunc(rollBackward),
		)),
	)
	if err != nil {
		_ = rawConn.Close()
		return false, fmt.Errorf(
			"ouroboros handshake with %s: %w", ep.Name, err,
		)
	}
	defer conn.Close() //nolint:errcheck

	cs := conn.ChainSync()
	if cs == nil || cs.Client == nil {
		return false, fmt.Errorf(
			"observer %s: no chain-sync client", ep.Name,
		)
	}
	if err := cs.Client.Sync(o.intersectPoints(chain)); err != nil {
		return false, fmt.Errorf("observer %s: sync: %w", ep.Name, err)
	}
	chain.Connected()
	o.logf("observer %s: following chain from %s", ep.Name, ep.Address)

	select {
	case <-ctx.Done():
		return true, nil
	case err, ok := <-conn.ErrorChan():
		if !ok {
			return true, errors.New("connection closed")
		}
		return true, err
	}
}

// intersectPoints builds the ladder of points offered to FindIntersect.
// Recent points first so a reconnect resumes where the previous session
// stopped, thinning out with depth, and always ending at origin so the
// intersect succeeds even when the node has rolled back past everything
// we retained or has been rebuilt from scratch.
func (o *ChainObservers) intersectPoints(
	chain *ObservedChain,
) []pcommon.Point {
	headers := chain.Snapshot().Headers
	points := make([]pcommon.Point, 0, 12)
	for step := 1; step <= len(headers); step *= 2 {
		h := headers[len(headers)-step]
		points = append(points, pcommon.NewPoint(h.Slot, h.Hash))
	}
	return append(points, pcommon.NewPointOrigin())
}

// tipFrom converts the peer's reported tip into the harness type.
func tipFrom(t chainsync.Tip) ChainTip {
	return ChainTip{
		SlotNumber:  t.Point.Slot,
		BlockNumber: t.BlockNumber,
		Hash:        t.Point.Hash,
	}
}
