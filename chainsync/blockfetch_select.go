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
	"encoding/binary"
	"hash/fnv"
	"math"
	"net"
	"slices"
	"sync"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	// blockfetchModelBytes is the batch size the delivery model prices. Peers
	// are compared on how long this many bytes take to arrive, so the one
	// number that matters is that every candidate is priced on the same one.
	blockfetchModelBytes = 256 << 10
	// blockfetchBand is the relative margin within which two delivery
	// estimates count as equal and the per-node salt decides.
	blockfetchBand = 0.05
	// blockfetchExploreEvery spaces the ranges that go to a peer with no
	// delivery sample, so a new peer can earn one without taking a large
	// share of the traffic.
	blockfetchExploreEvery = 8
)

type blockfetchSelectionState struct {
	sync.Mutex
	last     ouroboros.ConnectionId
	haveLast bool
}

// RecordBlockfetchThroughput folds one batch's delivery rate into the
// connection's per-byte cost estimate. Batches that moved no bytes or took no
// measurable time carry no rate and are ignored.
func (s *State) RecordBlockfetchThroughput(
	connId ouroboros.ConnectionId,
	bytes uint64,
	elapsed time.Duration,
) {
	if bytes == 0 || elapsed <= 0 {
		return
	}
	sample := elapsed.Seconds() / float64(bytes)
	s.clientConnIdMutex.Lock()
	defer s.clientConnIdMutex.Unlock()
	tc, ok := s.trackedClients[connId]
	if !ok {
		return
	}
	tc.blockfetchThroughputSamples++
	if tc.blockfetchThroughputSamples == 1 {
		tc.blockfetchSecondsPerByte = sample
		return
	}
	tc.blockfetchSecondsPerByte = sample*blockfetchLatencyAlpha +
		tc.blockfetchSecondsPerByte*(1-blockfetchLatencyAlpha)
}

type blockfetchCandidate struct {
	connId  ouroboros.ConnectionId
	sampled bool
	// estimate is the expected seconds to deliver blockfetchModelBytes.
	estimate float64
	tie      uint64
}

// SelectBlockfetchPeer picks the connection to fetch the range ending at point
// from. The candidates are origin, which delivered the header, and every other
// eligible peer that announced point; headers chain by hash, so such a peer
// holds the whole range. Peers are ranked by the time their measured one-way
// latency and per-byte delivery cost predict for a typical batch; estimates
// within blockfetchBand of the best are tied and settled by a per-node salt,
// so near-equal peers neither flap nor attract every node alike. A peer with
// no latency sample is tried for one range in blockfetchExploreEvery, chosen
// by the salted range hash, so asking about the same range again gives the
// same answer while the measurements stand. When nothing has been measured,
// or no other peer holds point, origin is used.
func (s *State) SelectBlockfetchPeer(
	origin ouroboros.ConnectionId,
	point ocommon.Point,
) ouroboros.ConnectionId {
	holders := s.PeersWithBlock(origin, point)
	candidates := s.blockfetchCandidates(origin, holders)
	chosen, decision := s.chooseBlockfetchCandidate(candidates, point)

	s.blockfetchSelectionsCounter.WithLabelValues(decision).Inc()
	sel := &s.blockfetchSelection
	sel.Lock()
	if sel.haveLast && sel.last != chosen {
		s.blockfetchHandoffsCounter.Inc()
	}
	sel.last, sel.haveLast = chosen, true
	sel.Unlock()
	return chosen
}

// blockfetchCandidates prices origin and the eligible holders.
func (s *State) blockfetchCandidates(
	origin ouroboros.ConnectionId,
	holders []ouroboros.ConnectionId,
) []blockfetchCandidate {
	s.clientConnIdMutex.RLock()
	defer s.clientConnIdMutex.RUnlock()
	candidates := make([]blockfetchCandidate, 0, 1+len(holders))
	var perByte []float64
	add := func(connId ouroboros.ConnectionId) {
		c := blockfetchCandidate{connId: connId, tie: s.blockfetchTie(connId)}
		if tc, ok := s.trackedClients[connId]; ok &&
			tc.blockfetchSampleCount > 0 {
			c.sampled = true
			c.estimate = tc.BlockfetchLatencyEWMA.Seconds()
			if tc.blockfetchThroughputSamples > 0 {
				c.estimate += tc.blockfetchSecondsPerByte *
					blockfetchModelBytes
				perByte = append(perByte, tc.blockfetchSecondsPerByte)
			}
		}
		candidates = append(candidates, c)
	}
	add(origin)
	for _, connId := range holders {
		if tc, ok := s.trackedClients[connId]; ok && !tc.ObservabilityOnly {
			add(connId)
		}
	}
	// A peer with latency but no rate yet is priced at the median rate of
	// the candidates that have one, so it is neither favored nor penalized
	// for the missing sample.
	if len(perByte) > 0 {
		slices.Sort(perByte)
		median := perByte[len(perByte)/2]
		for i := range candidates {
			tc := s.trackedClients[candidates[i].connId]
			if tc != nil && candidates[i].sampled &&
				tc.blockfetchThroughputSamples == 0 {
				candidates[i].estimate += median * blockfetchModelBytes
			}
		}
	}
	return candidates
}

func (s *State) chooseBlockfetchCandidate(
	candidates []blockfetchCandidate,
	point ocommon.Point,
) (ouroboros.ConnectionId, string) {
	origin := candidates[0].connId
	if len(candidates) == 1 {
		return origin, "only_holder"
	}
	exploring := s.saltedHash(point.Hash)%blockfetchExploreEvery == 0

	var unsampled, sampled []blockfetchCandidate
	for _, c := range candidates {
		if c.sampled {
			sampled = append(sampled, c)
		} else {
			unsampled = append(unsampled, c)
		}
	}
	if exploring {
		if c, ok := lowestTie(unsampled, math.Inf(1)); ok {
			return c.connId, "explore"
		}
	}
	best := math.Inf(1)
	for _, c := range sampled {
		best = min(best, c.estimate)
	}
	if c, ok := lowestTie(sampled, best*(1+blockfetchBand)); ok {
		return c.connId, "model"
	}
	return origin, "unmeasured"
}

// lowestTie returns the candidate with the lowest tie rank among those whose
// estimate is at most limit, and false when there is none.
func lowestTie(
	candidates []blockfetchCandidate,
	limit float64,
) (blockfetchCandidate, bool) {
	var best blockfetchCandidate
	found := false
	for _, c := range candidates {
		if c.estimate <= limit && (!found || c.tie < best.tie) {
			best, found = c, true
		}
	}
	return best, found
}

// blockfetchTie ranks connId among tied peers: stable for this node, but
// different on nodes with different salts.
func (s *State) blockfetchTie(connId ouroboros.ConnectionId) uint64 {
	key := appendBlockfetchAddressKey(nil, connId.LocalAddr)
	key = appendBlockfetchAddressKey(key, connId.RemoteAddr)
	return s.saltedHash(key)
}

func appendBlockfetchAddressKey(key []byte, addr net.Addr) []byte {
	if addr == nil {
		return append(key, 0)
	}
	key = append(key, 1)
	for _, part := range [...]string{addr.Network(), addr.String()} {
		var length [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(length[:], uint64(len(part)))
		key = append(key, length[:n]...)
		key = append(key, part...)
	}
	return key
}

// saltedHash hashes data with this node's blockfetch salt.
func (s *State) saltedHash(data []byte) uint64 {
	h := fnv.New64a()
	var salt [8]byte
	binary.LittleEndian.PutUint64(salt[:], s.blockfetchSalt)
	_, _ = h.Write(salt[:])
	_, _ = h.Write(data)
	return h.Sum64()
}
