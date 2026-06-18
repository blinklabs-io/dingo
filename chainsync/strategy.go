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
	"fmt"
	"slices"
	"strings"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// HeaderSyncStrategy selects how headers from multiple eligible ChainSync
// peers drive ledger ingress. Cross-peer deduplication and fork detection
// apply under every strategy; the strategy only decides which eligible peer is
// permitted to publish a header into the ledger ingress queue.
type HeaderSyncStrategy int

const (
	// HeaderSyncStrategyPrimary drives ledger ingress from a single active
	// peer, failing over to another eligible peer when it stalls or
	// disconnects. A new header from any eligible peer is published, and the
	// active peer replays a header first observed from another peer so it
	// stays the contiguous ingress driver. This is the default and preserves
	// the behavior from before the strategy gate existed.
	HeaderSyncStrategyPrimary HeaderSyncStrategy = iota
	// HeaderSyncStrategyParallel lets every eligible peer drive ledger
	// ingress concurrently. The first peer to report a header drives it;
	// duplicates from other peers are deduplicated before ledger ingress, so
	// a header never enters ledger processing twice.
	HeaderSyncStrategyParallel
	// HeaderSyncStrategyRoundRobin rotates a single ingress-driving peer
	// across the eligible peers. The rotation advances via
	// AdvanceHeaderSyncRotation.
	HeaderSyncStrategyRoundRobin
)

// String returns the canonical name for the strategy.
func (h HeaderSyncStrategy) String() string {
	switch h {
	case HeaderSyncStrategyPrimary:
		return "primary"
	case HeaderSyncStrategyParallel:
		return "parallel"
	case HeaderSyncStrategyRoundRobin:
		return "round-robin"
	default:
		return "unknown"
	}
}

// ParseHeaderSyncStrategy parses a header-sync strategy name. An empty string
// returns the default (primary). Names are case-insensitive and ignore
// surrounding whitespace; "round-robin", "roundrobin", and "round_robin" are
// all accepted.
func ParseHeaderSyncStrategy(s string) (HeaderSyncStrategy, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "primary":
		return HeaderSyncStrategyPrimary, nil
	case "parallel":
		return HeaderSyncStrategyParallel, nil
	case "round-robin", "roundrobin", "round_robin":
		return HeaderSyncStrategyRoundRobin, nil
	default:
		return HeaderSyncStrategyPrimary, fmt.Errorf(
			"invalid header sync strategy %q (want primary, parallel, or round-robin)",
			s,
		)
	}
}

// HeaderSyncStrategy returns the configured header-sync strategy.
func (s *State) HeaderSyncStrategy() HeaderSyncStrategy {
	return s.config.HeaderSyncStrategy
}

// ShouldPublishHeader reports whether a header from connId — already past peer
// eligibility and cross-peer deduplication — should be published into the
// ledger ingress queue under the configured header-sync strategy. isNew is the
// result returned by UpdateClientTip for this header; point is required to
// evaluate duplicate replay for the single-driver strategies.
//
// The caller is responsible for the prior ingress-eligibility gate; this
// method only applies the strategy-specific driver policy.
func (s *State) ShouldPublishHeader(
	connId ouroboros.ConnectionId,
	point ocommon.Point,
	isNew bool,
) bool {
	switch s.config.HeaderSyncStrategy {
	case HeaderSyncStrategyParallel:
		// The first peer to report a header drives it. Duplicates are never
		// replayed, so a header enters ledger processing exactly once even
		// when several eligible peers offer it.
		return isNew
	case HeaderSyncStrategyRoundRobin:
		if !s.isRoundRobinDriver(connId) {
			return false
		}
		if isNew {
			return true
		}
		// The current driver replays a duplicate first observed from another
		// connection so its ledger stream stays contiguous.
		return s.HeaderPreviouslySeenFromOtherConn(connId, point)
	case HeaderSyncStrategyPrimary:
		// Handled by the primary policy below.
	}
	// Primary (default): a new header from any eligible peer publishes, and
	// the active peer replays a duplicate first observed from another
	// connection so it stays the contiguous ingress driver.
	if isNew {
		return true
	}
	active := s.GetClientConnId()
	return active != nil &&
		trackedConnIdsEqual(*active, connId) &&
		s.HeaderPreviouslySeenFromOtherConn(connId, point)
}

// AdvanceHeaderSyncRotation advances the round-robin ingress driver to the
// next eligible peer. It is a no-op unless the round-robin strategy is in use,
// so it is safe to call unconditionally (e.g. on every stall-check tick).
func (s *State) AdvanceHeaderSyncRotation() {
	if s.config.HeaderSyncStrategy != HeaderSyncStrategyRoundRobin {
		return
	}
	s.clientConnIdMutex.Lock()
	defer s.clientConnIdMutex.Unlock()
	s.roundRobinIndex++
}

// isRoundRobinDriver reports whether connId is the eligible peer currently
// selected by the round-robin rotation.
func (s *State) isRoundRobinDriver(connId ouroboros.ConnectionId) bool {
	s.clientConnIdMutex.RLock()
	defer s.clientConnIdMutex.RUnlock()
	driver, ok := s.roundRobinDriverLocked()
	return ok && trackedConnIdsEqual(driver, connId)
}

// roundRobinDriverLocked returns the eligible peer at the current rotation
// index. Eligible peers are sorted by connection id so the rotation is stable
// regardless of map iteration order. Caller must hold clientConnIdMutex.
func (s *State) roundRobinDriverLocked() (ouroboros.ConnectionId, bool) {
	eligible := s.sortedEligibleConnIdsLocked()
	if len(eligible) == 0 {
		return ouroboros.ConnectionId{}, false
	}
	// len(eligible) > 0, so the conversion is safe and the modulo result is
	// always a valid index into eligible.
	idx := s.roundRobinIndex % uint64(len(eligible)) //nolint:gosec // G115: len is non-negative
	return eligible[idx], true
}

// sortedEligibleConnIdsLocked returns the connection ids of all eligible
// (tracked, non-observability, non-failed, non-stalled) clients sorted by id
// string. Stalled peers are excluded so the round-robin rotation does not
// select a peer that has stopped delivering headers and strand ingestion until
// the next rotation advance. Caller must hold clientConnIdMutex.
func (s *State) sortedEligibleConnIdsLocked() []ouroboros.ConnectionId {
	eligible := make([]ouroboros.ConnectionId, 0, len(s.trackedClients))
	for id, tc := range s.trackedClients {
		if tc.ObservabilityOnly ||
			tc.Status == ClientStatusFailed ||
			tc.Status == ClientStatusStalled {
			continue
		}
		eligible = append(eligible, id)
	}
	slices.SortFunc(
		eligible,
		func(a, b ouroboros.ConnectionId) int {
			return strings.Compare(a.String(), b.String())
		},
	)
	return eligible
}
