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

package chainselection

import (
	"time"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// caughtUpJudgement is the caught-up state of the Genesis state machine for a
// selector that does not run it as a selection mode. Guarded by mutex.
type caughtUpJudgement struct {
	caughtUp bool
	// minStayUntil, bonus, tip, tipSet and deadline play the roles of
	// praosMinStayUntil, praosBonus, reentryTip, reentryTipSet and
	// reentryDeadline.
	minStayUntil time.Time
	bonus        time.Duration
	tip          ocommon.Point
	tipSet       bool
	deadline     time.Time
}

// CaughtUp reports the Genesis state machine's judgement, YoungEnough in the
// reference's terms: the node follows the tip of the chain.
//
// A selector that runs the machine as its selection mode answers from that
// mode. Any other selector, one that selects by Praos rules, runs the same
// transitions for the judgement alone, evaluated when asked: it never
// changes the selection mode, publishes no mode event and disconnects no
// peer, because those belong to Genesis selection.
//
// Syncing becomes caught up when at least one connection is known, every
// connection is idle and no candidate is better than the selection, the
// reference's blockUntilCaughtUp. Caught up becomes syncing when the selection
// tip is older than the maximum caught-up age, plus a random bonus drawn once
// per caught-up period, and not before that age has passed since the node
// became caught up, the reference's blockWhileCaughtUp and
// gsmMinCaughtUpDuration.
//
// The judgement starts as syncing, as the reference does without its
// caught-up marker, and there is none here: a restarted node is caught up
// again as soon as its peers are idle. Two parts of the reference are not
// mirrored. Syncing waits on the Honest Availability Assumption, the
// diffusion layer's trust state, which Dingo does not track; the Genesis
// selection mode ignores it as well. Without Genesis selection the points of
// a peer's chain are not recorded, so a peer that is idle at a point off the
// node's chain counts as a better candidate where the reference compares the
// chains; the judgement then stays syncing, never caught up too early.
func (cs *ChainSelector) CaughtUp() bool {
	if cs.genesisEnabled {
		active, _ := cs.GenesisSelectionState()
		return !active
	}
	if cs.config.SlotTime == nil {
		return false
	}
	cs.mutex.RLock()
	caught := cs.judgement.caughtUp
	cs.mutex.RUnlock()
	if caught {
		return cs.judgementStaysCaughtUp()
	}
	return cs.judgementBecomesCaughtUp()
}

func (cs *ChainSelector) judgementBecomesCaughtUp() bool {
	cs.mutex.RLock()
	snap := cs.gsmSnapshotLocked()
	cs.mutex.RUnlock()
	if !cs.caughtUp(snap) {
		return false
	}
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	if cs.judgement.caughtUp {
		return true
	}
	if !snap.sameAs(cs.gsmSnapshotLocked()) {
		return false
	}
	cs.judgement = caughtUpJudgement{
		caughtUp:     true,
		minStayUntil: cs.now().Add(cs.maxCaughtUpAge()),
		bonus:        cs.drawBonus(),
	}
	cs.config.Logger.Info(
		"caught up",
		"local_slot", snap.localTip.Point.Slot,
		"peers", len(snap.peers),
	)
	return true
}

// judgementStaysCaughtUp reports whether a caught-up node still is. The age of
// the selection is measured afresh on the first look after the minimum stay
// and whenever the selection changes; a tip that cannot be placed in time, or
// that is already past the age limit, is too old at once without the bonus.
func (cs *ChainSelector) judgementStaysCaughtUp() bool {
	cs.mutex.RLock()
	now := cs.now()
	tip, known := cs.appliedLocalTipLocked()
	state := cs.judgement
	cs.mutex.RUnlock()

	if !state.caughtUp {
		return false
	}
	if !known || now.Before(state.minStayUntil) {
		return true
	}
	deadline := state.deadline
	if !state.tipSet || !pointsEqual(state.tip, tip.Point) {
		deadline = now
		origin := tip.Point.Slot == 0 && len(tip.Point.Hash) == 0
		if slotTime, err := cs.config.SlotTime(tip.Point.Slot); err == nil &&
			!origin {
			limit := slotTime.Add(cs.maxCaughtUpAge())
			if limit.After(now) {
				deadline = limit.Add(state.bonus)
			}
		}
	}
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	if !cs.judgement.caughtUp ||
		!cs.judgement.minStayUntil.Equal(state.minStayUntil) {
		return cs.judgement.caughtUp
	}
	live, liveKnown := cs.appliedLocalTipLocked()
	if !liveKnown || !pointsEqual(live.Point, tip.Point) {
		return true
	}
	cs.judgement.tip = clonePoint(tip.Point)
	cs.judgement.tipSet = true
	cs.judgement.deadline = deadline
	if now.Before(deadline) {
		return true
	}
	cs.judgement.caughtUp = false
	cs.config.Logger.Info(
		"no longer caught up",
		"local_slot", tip.Point.Slot,
		"deadline", deadline,
	)
	return false
}
