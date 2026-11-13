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

package dingo

import (
	"context"
	"log/slog"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/consensus/praos"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/vrf"
)

// arbiterDecisionCacheSlots is how many recent slots the arbiter keeps
// decisions and outcomes for. Every pool of a node asks about the same slot
// within a moment of the others, and a pool waits on a slot no longer than
// the slot lasts, so a short history is enough.
const arbiterDecisionCacheSlots = 64

// arbiterNonceTimeout bounds the epoch nonce read behind a decision. The
// nonce of the current epoch is already stable, so the read is a lookup.
const arbiterNonceTimeout = time.Second

// arbiterLeader is the part of a pool's leader election the arbiter reads.
// NextLeaderSlot, unlike ShouldProduceBlock, counts nothing in the election's
// metrics, so the arbiter's own reads leave them as the forger left them.
type arbiterLeader interface {
	NextLeaderSlot(fromSlot uint64) (uint64, bool)
}

// arbiterProver proves a VRF input with a pool's VRF key; a pool's
// forging.PoolCredentials is one.
type arbiterProver interface {
	VRFProve(alpha []byte) (proof []byte, output []byte, err error)
}

// arbiterMember is one pool the arbiter decides between.
type arbiterMember struct {
	poolID string
	creds  arbiterProver
	leader arbiterLeader
	praos  bool
}

// arbiterChainView is what a decision needs from the ledger: the slot's epoch
// and era, and that epoch's nonce.
type arbiterChainView interface {
	EpochForSlot(slot uint64) (uint64, error)
	EraIDForEpoch(epoch uint64) (uint, error)
	EpochNonce(ctx context.Context, epoch uint64) []byte
}

// ledgerArbiterView reads arbiterChainView from the ledger state.
type ledgerArbiterView struct {
	ls *ledger.LedgerState
}

func (v ledgerArbiterView) EpochForSlot(slot uint64) (uint64, error) {
	epoch, err := v.ls.SlotToEpoch(slot)
	if err != nil {
		return 0, err
	}
	return epoch.EpochId, nil
}

func (v ledgerArbiterView) EraIDForEpoch(epoch uint64) (uint, error) {
	return v.ls.EraIDForEpoch(epoch)
}

func (v ledgerArbiterView) EpochNonce(
	ctx context.Context,
	epoch uint64,
) []byte {
	return v.ls.EpochNonce(ctx, epoch)
}

// poolSlotArbiter decides which of a node's pools forges a slot that more than
// one of them leads. Only one block per slot can stay on chain, so a second
// block of the node's own only competes with the first. The pool kept is the
// one whose leader VRF output is lowest: the output chain selection compares
// for two blocks in one slot (praos.GetVRFOutput), where the lower wins. Any
// doubt -- a non-Praos schedule, no nonce, a VRF failure -- decides nothing,
// and every pool forges, since a lost slot costs more than an extra block.
//
// The ranking knows nothing of the winner's later refusals or a failed build,
// so a yielded pool waits for the slot's outcome (AwaitWinner) instead of
// giving the slot up. Every pool reports each forge check (ReportSlot). An
// adopted block ends the wait with a yield. A winner that adopts none
// releases the slot: it is decided again among the leaders that have not
// released it, so the next best pool forges and any pool behind that one
// keeps waiting. A wait ends with a forge when its deadline passes, its
// context ends or the members or view change.
type poolSlotArbiter struct {
	logger *slog.Logger

	mu      sync.Mutex
	view    arbiterChainView
	members map[string]arbiterMember
	// slots holds each recent slot's decision and outcome; order is its
	// insertion order, for eviction.
	slots map[uint64]*arbiterSlot
	order []uint64
}

// arbiterSlot is one slot's decision and what its pools reported.
type arbiterSlot struct {
	// decided is set once winner holds a final decision; winner is "" when
	// the decision is that every leading pool forges.
	decided bool
	winner  string
	// released holds the leading pools that reported a forge check without
	// an adopted block; they are left out of any later decision.
	released map[string]bool
	// adopted is set once any pool reported an adopted block.
	adopted bool
	// changed is closed, and replaced, on every change a waiter acts on.
	changed chan struct{}
}

func newPoolSlotArbiter(logger *slog.Logger) *poolSlotArbiter {
	return &poolSlotArbiter{
		logger:  logger,
		members: make(map[string]arbiterMember),
		slots:   make(map[uint64]*arbiterSlot),
	}
}

// setView points the arbiter at the ledger it reads nonces and eras from.
func (a *poolSlotArbiter) setView(view arbiterChainView) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.view = view
	a.resetLocked()
}

// add records a pool the arbiter decides between, replacing any earlier
// entry for it.
func (a *poolSlotArbiter) add(m arbiterMember) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.members[m.poolID] = m
	a.resetLocked()
}

// remove forgets a pool, so slots it led are no longer weighed against it.
func (a *poolSlotArbiter) remove(poolID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.members, poolID)
	a.resetLocked()
}

// resetLocked drops cached decisions, which a change of members or view can
// invalidate, and wakes every waiting pool, which then forges: its winner
// may be gone. Reported outcomes are facts and stay. Callers hold a.mu.
func (a *poolSlotArbiter) resetLocked() {
	for _, st := range a.slots {
		st.decided, st.winner = false, ""
		st.notify()
	}
}

// notify wakes the pools waiting on the slot.
func (st *arbiterSlot) notify() {
	close(st.changed)
	st.changed = make(chan struct{})
}

// slotLocked returns slot's state, creating it, and evicting the oldest slot
// past arbiterDecisionCacheSlots. A pool still waiting on an evicted slot
// forges. Callers hold a.mu.
func (a *poolSlotArbiter) slotLocked(slot uint64) *arbiterSlot {
	if st, ok := a.slots[slot]; ok {
		return st
	}
	st := &arbiterSlot{
		released: make(map[string]bool),
		changed:  make(chan struct{}),
	}
	a.slots[slot] = st
	a.order = append(a.order, slot)
	if len(a.order) > arbiterDecisionCacheSlots {
		evict := len(a.order) - arbiterDecisionCacheSlots
		for _, old := range a.order[:evict] {
			if gone, ok := a.slots[old]; ok {
				delete(a.slots, old)
				gone.notify()
			}
		}
		a.order = append(a.order[:0], a.order[evict:]...)
	}
	return st
}

// forPool returns the SlotArbiter the forger of poolID consults.
func (a *poolSlotArbiter) forPool(poolID string) forging.SlotArbiter {
	return poolArbiterView{arbiter: a, poolID: poolID}
}

// winner returns the pool that forges slot, or "" when no pool yields it.
func (a *poolSlotArbiter) winner(slot uint64) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	st := a.slotLocked(slot)
	if !st.decided {
		w, final := a.decideLocked(slot, st.released)
		if !final {
			return w
		}
		st.decided, st.winner = true, w
	}
	return st.winner
}

// report records poolID's forge check outcome for slot. A report from a pool
// that does not lead slot changes nothing.
func (a *poolSlotArbiter) report(poolID string, slot uint64, adopted bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	m, ok := a.members[poolID]
	if !ok {
		return
	}
	if next, leads := m.leader.NextLeaderSlot(slot); !leads || next != slot {
		return
	}
	st := a.slotLocked(slot)
	if adopted {
		if !st.adopted {
			st.adopted = true
			st.notify()
		}
		return
	}
	if st.released[poolID] {
		return
	}
	st.released[poolID] = true
	if st.decided && st.winner == poolID {
		// Decide again without the pools that released. Fewer than two
		// leaders left is no decision: the one left forges.
		w, final := a.decideLocked(slot, st.released)
		st.decided, st.winner = final, w
		st.notify()
	}
}

// await waits until slot's outcome settles poolID's yield, and returns true
// only when a pool adopted a block for it. Every other end -- the slot passed
// to poolID or left to every pool, the slot reset or evicted, ctx done, until
// passed -- returns false, and poolID forges.
func (a *poolSlotArbiter) await(
	ctx context.Context,
	poolID string,
	slot uint64,
	until time.Time,
) bool {
	timer := time.NewTimer(time.Until(until))
	defer timer.Stop()
	for {
		a.mu.Lock()
		st, ok := a.slots[slot]
		if !ok {
			a.mu.Unlock()
			return false
		}
		if st.adopted {
			a.mu.Unlock()
			return true
		}
		if !st.decided || st.winner == "" || st.winner == poolID {
			a.mu.Unlock()
			return false
		}
		changed := st.changed
		a.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return false
		case <-timer.C:
			return false
		}
	}
}

// decideLocked ranks the pools that lead slot by their leader VRF output,
// leaving out the released ones. It holds a.mu while it proves, so a pool
// asking while another's decision is being made waits for that decision
// rather than making its own. final is false when fewer than two pools lead
// the slot: a pool whose schedule is not ready yet reads as a non-leader, so
// that answer is not cached, and a pool asking once its schedule arrives sees
// every leader.
func (a *poolSlotArbiter) decideLocked(
	slot uint64,
	released map[string]bool,
) (winner string, final bool) {
	leaders := make([]arbiterMember, 0, len(a.members))
	for _, m := range a.members {
		if released[m.poolID] {
			continue
		}
		if next, ok := m.leader.NextLeaderSlot(slot); ok && next == slot {
			leaders = append(leaders, m)
		}
	}
	if len(leaders) < 2 {
		return "", false
	}
	// Map order is random; equal outputs, which a VRF all but rules out,
	// still need one answer for every pool that asks.
	slices.SortFunc(leaders, func(x, y arbiterMember) int {
		switch {
		case x.poolID < y.poolID:
			return -1
		case x.poolID > y.poolID:
			return 1
		default:
			return 0
		}
	})
	for _, m := range leaders {
		if !m.praos {
			return a.noDecision(
				slot,
				"a leading pool has no Praos VRF schedule",
			), true
		}
	}
	input, reason := a.leaderVRFInputLocked(slot)
	if reason != "" {
		return a.noDecision(slot, reason), true
	}
	best := ""
	var bestOutput []byte
	for _, m := range leaders {
		_, output, err := m.creds.VRFProve(input)
		if err != nil {
			return a.noDecision(slot, "VRF prove failed: "+err.Error()), true
		}
		if len(output) != praos.VRFOutputSize {
			return a.noDecision(slot, "VRF output has the wrong length"), true
		}
		if best == "" ||
			praos.CompareVRFOutputs(output, bestOutput) == praos.ChainABetter {
			best, bestOutput = m.poolID, output
		}
	}
	return best, true
}

// leaderVRFInputLocked builds the VRF input a block for slot proves its
// leadership with: MkInputVrf from Babbage on, the SeedL TPraos seed before.
// It returns why it could not, or "".
func (a *poolSlotArbiter) leaderVRFInputLocked(slot uint64) ([]byte, string) {
	if a.view == nil {
		return nil, "no ledger view"
	}
	if slot > math.MaxInt64 {
		return nil, "slot exceeds int64"
	}
	epoch, err := a.view.EpochForSlot(slot)
	if err != nil {
		return nil, "epoch for slot: " + err.Error()
	}
	era, err := a.view.EraIDForEpoch(epoch)
	if err != nil {
		return nil, "era for epoch: " + err.Error()
	}
	ctx, cancel := context.WithTimeout(
		context.Background(),
		arbiterNonceTimeout,
	)
	defer cancel()
	nonce := a.view.EpochNonce(ctx, epoch)
	if len(nonce) == 0 {
		return nil, "epoch nonce not available"
	}
	var input []byte
	if era < babbage.EraIdBabbage {
		input, err = vrf.MkSeedTPraos(
			int64(slot), // #nosec G115 -- bounded above
			nonce,
			vrf.SeedL(),
		)
	} else {
		input, err = vrf.MkInputVrf(
			int64(slot), // #nosec G115 -- bounded above
			nonce,
		)
	}
	if err != nil {
		return nil, "VRF input: " + err.Error()
	}
	return input, ""
}

// noDecision logs why slot is left to every pool that leads it, and returns
// the empty winner.
func (a *poolSlotArbiter) noDecision(slot uint64, reason string) string {
	if a.logger != nil {
		a.logger.Warn(
			"slot arbiter: several pools of this node lead the slot and each will forge",
			"component",
			"node",
			"slot",
			slot,
			"reason",
			reason,
		)
	}
	return ""
}

// poolArbiterView is one pool's view of the shared arbiter.
type poolArbiterView struct {
	arbiter *poolSlotArbiter
	poolID  string
}

func (v poolArbiterView) YieldSlot(slot uint64) (bool, string) {
	w := v.arbiter.winner(slot)
	if w == "" || w == v.poolID {
		return false, ""
	}
	return true, w
}

func (v poolArbiterView) AwaitWinner(
	ctx context.Context,
	slot uint64,
	until time.Time,
) bool {
	return v.arbiter.await(ctx, v.poolID, slot, until)
}

func (v poolArbiterView) ReportSlot(slot uint64, adopted bool) {
	v.arbiter.report(v.poolID, slot, adopted)
}
