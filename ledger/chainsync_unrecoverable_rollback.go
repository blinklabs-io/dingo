// Copyright 2025 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ledger

import (
	"encoding/hex"
	"fmt"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	// unrecoverableRollbackThreshold is how many times the same rollback
	// point must fail to apply locally (within the detection window)
	// before we treat the node as stuck on a diverged local chain. It is
	// higher than rollbackLoopThreshold because this counter, unlike the
	// per-connection loop detector, survives reset+reconnect and so must
	// tolerate a couple of legitimate transient re-intersects.
	unrecoverableRollbackThreshold = 3

	// unrecoverableRollbackWindow bounds how far back failed rollback
	// points are remembered. A point that has not recurred within the
	// window ages out, so a node that recovers naturally stops reporting.
	unrecoverableRollbackWindow = 10 * time.Minute

	// unrecoverableRollbackLogInterval throttles the operator-facing error
	// so a stuck node emits an actionable message roughly once a minute
	// instead of joining the ~2/sec WARN loop.
	unrecoverableRollbackLogInterval = 60 * time.Second
)

// unrecoverableRollbackRecord counts how often a single rollback point has
// failed to apply locally within the detection window.
type unrecoverableRollbackRecord struct {
	count     int
	firstSeen time.Time
	lastSeen  time.Time
}

func unrecoverableRollbackKey(point ocommon.Point) string {
	return fmt.Sprintf("%d:%x", point.Slot, point.Hash)
}

// noteUnrecoverableRollback records that we could not apply a rollback to
// point (block missing locally, rollback exceeds K, or point below the
// Mithril boundary) and reports the running count plus whether that count
// has crossed unrecoverableRollbackThreshold within the window.
//
// Unlike the rollbackHistory loop detector this tracker is keyed on the
// point alone and is NOT wiped by resetChainsyncResyncState/
// requestChainsyncResync, so it accumulates across the reset+reconnect
// cycle that a diverged node would otherwise loop through forever
// (see issue #2728).
//
// Callers must hold chainsyncMutex.
func (ls *LedgerState) noteUnrecoverableRollback(
	point ocommon.Point,
) (int, bool) {
	now := time.Now()
	if ls.unrecoverableRollbacks == nil {
		ls.unrecoverableRollbacks = make(
			map[string]unrecoverableRollbackRecord,
		)
	}
	// Prune points that have not recurred within the window.
	cutoff := now.Add(-unrecoverableRollbackWindow)
	for key, rec := range ls.unrecoverableRollbacks {
		if rec.lastSeen.Before(cutoff) {
			delete(ls.unrecoverableRollbacks, key)
		}
	}
	key := unrecoverableRollbackKey(point)
	rec := ls.unrecoverableRollbacks[key]
	if rec.count == 0 {
		rec.firstSeen = now
	}
	rec.count++
	rec.lastSeen = now
	ls.unrecoverableRollbacks[key] = rec
	return rec.count, rec.count >= unrecoverableRollbackThreshold
}

// clearUnrecoverableRollbacks resets the tracker after the node makes
// forward progress (a rollback applied successfully), so a later,
// unrelated divergence starts counting from zero.
//
// Callers must hold chainsyncMutex.
func (ls *LedgerState) clearUnrecoverableRollbacks() {
	ls.unrecoverableRollbacks = nil
}

// reportUnrecoverableRollbackIfStuck records a failed rollback point and,
// once the same point has failed enough times across reset+reconnect
// cycles, surfaces the stuck-divergence condition as a throttled
// operator-facing error plus a metric, rather than only the per-attempt
// WARN that otherwise repeats indefinitely with zero forward progress.
//
// Callers must hold chainsyncMutex.
func (ls *LedgerState) reportUnrecoverableRollbackIfStuck(
	point ocommon.Point,
	reason string,
	connId ouroboros.ConnectionId,
) {
	count, stuck := ls.noteUnrecoverableRollback(point)
	if !stuck {
		return
	}
	if ls.metrics.unrecoverableRollbacks != nil {
		ls.metrics.unrecoverableRollbacks.Inc()
	}
	now := time.Now()
	if now.Sub(ls.lastUnrecoverableRollbackLog) < unrecoverableRollbackLogInterval {
		return
	}
	ls.lastUnrecoverableRollbackLog = now
	if ls.config.Logger != nil {
		ls.config.Logger.Error(
			"chainsync stuck: repeatedly cannot cross to peer rollback point; "+
				"local chain has diverged from the network and cannot self-recover. "+
				"Operator intervention required (e.g. re-bootstrap from a Mithril snapshot).",
			"component", "ledger",
			"slot", point.Slot,
			"hash", hex.EncodeToString(point.Hash),
			"reason", reason,
			"failed_attempts", count,
			"window", unrecoverableRollbackWindow,
			"connection_id", connId.String(),
		)
	}
}
