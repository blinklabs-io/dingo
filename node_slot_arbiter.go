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

// arbiterDecisionCacheSlots is how many recent slot decisions the arbiter
// keeps. Every pool of a node asks about the same slot within a moment of the
// others, so a short history is enough for all of them to read the decision
// the first one made.
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
type poolSlotArbiter struct {
	logger *slog.Logger

	mu      sync.Mutex
	view    arbiterChainView
	members map[string]arbiterMember
	// decided maps a slot to the pool that forges it, "" for no decision;
	// order is its insertion order, for eviction.
	decided map[uint64]string
	order   []uint64
}

func newPoolSlotArbiter(logger *slog.Logger) *poolSlotArbiter {
	return &poolSlotArbiter{
		logger:  logger,
		members: make(map[string]arbiterMember),
		decided: make(map[uint64]string),
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
// invalidate. Callers hold a.mu.
func (a *poolSlotArbiter) resetLocked() {
	clear(a.decided)
	a.order = a.order[:0]
}

// forPool returns the SlotArbiter the forger of poolID consults.
func (a *poolSlotArbiter) forPool(poolID string) forging.SlotArbiter {
	return poolArbiterView{arbiter: a, poolID: poolID}
}

// winner returns the pool that forges slot, or "" when no pool yields it.
func (a *poolSlotArbiter) winner(slot uint64) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if w, ok := a.decided[slot]; ok {
		return w
	}
	w := a.decideLocked(slot)
	a.decided[slot] = w
	a.order = append(a.order, slot)
	if len(a.order) > arbiterDecisionCacheSlots {
		evict := len(a.order) - arbiterDecisionCacheSlots
		for _, old := range a.order[:evict] {
			delete(a.decided, old)
		}
		a.order = append(a.order[:0], a.order[evict:]...)
	}
	return w
}

// decideLocked ranks the pools that lead slot by their leader VRF output. It
// holds a.mu while it proves, so a pool asking while another's decision is
// being made waits for that decision rather than making its own.
func (a *poolSlotArbiter) decideLocked(slot uint64) string {
	leaders := make([]arbiterMember, 0, len(a.members))
	for _, m := range a.members {
		if next, ok := m.leader.NextLeaderSlot(slot); ok && next == slot {
			leaders = append(leaders, m)
		}
	}
	if len(leaders) < 2 {
		return ""
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
			)
		}
	}
	input, reason := a.leaderVRFInputLocked(slot)
	if reason != "" {
		return a.noDecision(slot, reason)
	}
	best := ""
	var bestOutput []byte
	for _, m := range leaders {
		_, output, err := m.creds.VRFProve(input)
		if err != nil {
			return a.noDecision(slot, "VRF prove failed: "+err.Error())
		}
		if len(output) != praos.VRFOutputSize {
			return a.noDecision(slot, "VRF output has the wrong length")
		}
		if best == "" ||
			praos.CompareVRFOutputs(output, bestOutput) == praos.ChainABetter {
			best, bestOutput = m.poolID, output
		}
	}
	return best
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
