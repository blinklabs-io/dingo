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

package chainsync

import (
	"bytes"
	"time"

	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// DefaultPatienceCapacity is the default Genesis Limit on Patience bucket
// capacity, in tokens. With DefaultPatienceRate it gives a peer 200 seconds
// of undelivered progress, the same budget as the reference node.
const DefaultPatienceCapacity = 1000

// DefaultPatienceRate is the default Genesis Limit on Patience leak rate, in
// tokens per second: the sustained header rate a peer must deliver while it
// advertises progress.
//
// The reference node leaks 500 tokens per second, which assumes its deep
// ChainSync pipeline. Dingo pipelines 10 requests, so one honest peer
// delivers about 10/RTT headers per second; this rate keeps a peer with up to
// two seconds of round-trip time from losing patience.
const DefaultPatienceRate = 5

// PatienceConfig configures the Genesis Limit on Patience (LoP): a per-peer
// leaky token bucket that bounds how slowly a ChainSync peer may deliver the
// progress it advertises while Genesis selection is active.
type PatienceConfig struct {
	// Enabled turns the Limit on Patience on. A disabled bucket never leaks.
	Enabled bool
	// Capacity is the initial and maximum bucket level, in tokens. Zero
	// selects DefaultPatienceCapacity.
	Capacity uint64
	// Rate is the leak rate, in tokens per second. Zero selects
	// DefaultPatienceRate.
	Rate uint64
}

// DefaultPatienceConfig returns the default Limit on Patience configuration.
func DefaultPatienceConfig() PatienceConfig {
	return PatienceConfig{
		Enabled:  true,
		Capacity: DefaultPatienceCapacity,
		Rate:     DefaultPatienceRate,
	}
}

func (c PatienceConfig) withDefaults() PatienceConfig {
	if c.Capacity == 0 {
		c.Capacity = DefaultPatienceCapacity
	}
	if c.Rate == 0 {
		c.Rate = DefaultPatienceRate
	}
	return c
}

// PatienceState is a tracked client's Limit on Patience bucket.
//
// The bucket starts full and, once running, leaks while the peer owes us a
// message. A header
// earns one token when it passes verification and raises the highest block
// number the peer has delivered, so re-delivering the same chain after a
// rollback earns nothing. The leak pauses while Dingo itself is processing a
// message and while the peer has delivered its own advertised tip, because
// neither is time the peer is withholding progress. Once the level reaches
// zero the bucket latches exhausted; later headers cannot refill it.
type PatienceState struct {
	// Tokens is the bucket level as of UpdatedAt.
	Tokens float64
	// BestBlockNumber is the highest block number delivered by the peer.
	BestBlockNumber uint64
	// Paused reports that the bucket is not leaking.
	Paused bool
	// Exhausted reports that the bucket emptied.
	Exhausted bool
	// UpdatedAt is when Tokens was last brought up to date.
	UpdatedAt time.Time
	// reported records that the exhaustion has been published, so each
	// exhausted client is reported exactly once.
	reported bool
}

// newPatienceState returns a full bucket that starts paused. Tracked clients
// are registered from inside the peer's first ChainSync callback, so a running
// bucket would charge the peer for this node's processing of that first
// header; the leak starts once a header is accepted or a rollback arrives. A
// peer that never sends one is left to the stall watchdog.
func newPatienceState(capacity uint64, now time.Time) PatienceState {
	return PatienceState{
		Tokens:    float64(capacity),
		Paused:    true,
		UpdatedAt: now,
	}
}

// patienceActive reports whether the Limit on Patience currently applies.
// Like the reference node, it applies only while Genesis selection is
// syncing; outside it the bucket is held full.
func (s *State) patienceActive() bool {
	if !s.config.Patience.Enabled {
		return false
	}
	return s.config.PatienceActiveFunc != nil && s.config.PatienceActiveFunc()
}

// leakPatienceLocked brings tc's bucket up to date at now.
// Caller must hold clientConnIdMutex.
func (s *State) leakPatienceLocked(
	tc *TrackedClient,
	now time.Time,
	active bool,
) {
	p := &tc.Patience
	defer func() {
		if now.After(p.UpdatedAt) {
			p.UpdatedAt = now
		}
	}()
	if p.Exhausted || tc.ObservabilityOnly {
		return
	}
	if !active {
		p.Tokens = float64(s.config.Patience.Capacity)
		return
	}
	if p.Paused || !now.After(p.UpdatedAt) {
		return
	}
	p.Tokens -= now.Sub(p.UpdatedAt).Seconds() *
		float64(s.config.Patience.Rate)
	if p.Tokens <= 0 {
		p.Tokens = 0
		p.Exhausted = true
	}
}

// PatienceMessageArrived charges a client's bucket for the time up to
// arrival, when a ChainSync message from the peer reached the network
// callback, and pauses it while Dingo processes the message. It reports
// whether the client is tracked.
func (s *State) PatienceMessageArrived(
	connId ouroboros.ConnectionId,
	arrival time.Time,
) bool {
	active := s.patienceActive()
	s.clientConnIdMutex.Lock()
	defer s.clientConnIdMutex.Unlock()
	tc, exists := s.trackedClients[connId]
	if !exists {
		return false
	}
	s.leakPatienceLocked(tc, arrival, active)
	tc.Patience.Paused = true
	return true
}

// PatienceHeaderAccepted records a verified header from the peer and resumes
// the leak unless the peer has now delivered its advertised tip. A header
// earns a token only when blockNumber exceeds the peer's best delivered block
// number.
func (s *State) PatienceHeaderAccepted(
	connId ouroboros.ConnectionId,
	blockNumber uint64,
	atTip bool,
) {
	active := s.patienceActive()
	s.clientConnIdMutex.Lock()
	defer s.clientConnIdMutex.Unlock()
	tc, exists := s.trackedClients[connId]
	if !exists {
		return
	}
	s.leakPatienceLocked(tc, s.now(), active)
	p := &tc.Patience
	if blockNumber > p.BestBlockNumber {
		p.BestBlockNumber = blockNumber
		if !p.Exhausted {
			p.Tokens = min(p.Tokens+1, float64(s.config.Patience.Capacity))
		}
	}
	p.Paused = atTip
}

// PatiencePause stops a client's bucket from leaking until the peer's next
// message, for local work such as a ChainSync restart that the peer is not
// responsible for.
func (s *State) PatiencePause(connId ouroboros.ConnectionId) {
	active := s.patienceActive()
	s.clientConnIdMutex.Lock()
	defer s.clientConnIdMutex.Unlock()
	tc, exists := s.trackedClients[connId]
	if !exists {
		return
	}
	s.leakPatienceLocked(tc, s.now(), active)
	tc.Patience.Paused = true
}

// resumePatienceAfterRollbackLocked restarts the leak after a rollback: the
// peer has rolled back to point and owes the headers up to its tip.
// Caller must hold clientConnIdMutex.
func (s *State) resumePatienceAfterRollbackLocked(
	tc *TrackedClient,
	point ocommon.Point,
	tip ochainsync.Tip,
	active bool,
) {
	s.leakPatienceLocked(tc, s.now(), active)
	tc.Patience.Paused = point.Slot == tip.Point.Slot &&
		bytes.Equal(point.Hash, tip.Point.Hash)
}

// CheckPatienceExhausted brings every running bucket up to date and returns
// the clients whose Genesis Limit on Patience is newly exhausted, publishing a
// ClientPatienceExhaustedEvent for each. A client is returned once; the caller
// is expected to disconnect it.
func (s *State) CheckPatienceExhausted() []ouroboros.ConnectionId {
	active := s.patienceActive()
	s.clientConnIdMutex.Lock()
	now := s.now()
	var exhausted []ouroboros.ConnectionId
	var events []ClientPatienceExhaustedEvent
	for id, tc := range s.trackedClients {
		s.leakPatienceLocked(tc, now, active)
		p := &tc.Patience
		if !p.Exhausted || p.reported {
			continue
		}
		p.reported = true
		exhausted = append(exhausted, id)
		events = append(events, ClientPatienceExhaustedEvent{
			ConnId:           id,
			BestBlockNumber:  p.BestBlockNumber,
			TipBlockNumber:   tc.Tip.BlockNumber,
			TipSlot:          tc.Tip.Point.Slot,
			HeadersDelivered: tc.HeadersRecv,
		})
	}
	s.clientConnIdMutex.Unlock()
	for _, evt := range events {
		s.patienceExhaustedCounter.Inc()
		if s.eventBus != nil {
			s.eventBus.PublishAsync(
				ClientPatienceExhaustedEventType,
				event.NewEvent(ClientPatienceExhaustedEventType, evt),
			)
		}
	}
	return exhausted
}
