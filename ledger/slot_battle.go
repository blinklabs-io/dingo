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

package ledger

import (
	"encoding/hex"
	"sync"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/forging"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// maxPendingSlotBattles bounds the battles awaiting settlement. A battle
// settles within a security parameter of blocks, so the bound is only reached
// by a node whose chain stops advancing; the oldest entry is dropped then.
const maxPendingSlotBattles = 64

// slotBattle is a detected battle whose winner is not yet known.
type slotBattle struct {
	slot       uint64
	localHash  []byte
	remoteHash []byte
	// settleAt is the tip block number from which the outcome is final: a
	// security parameter of blocks past the tip seen at detection.
	settleAt uint64
}

// slotBattleBook holds the battles awaiting settlement. The zero value is
// ready to use.
type slotBattleBook struct {
	mu      sync.Mutex
	pending []slotBattle
}

func (b *slotBattleBook) add(battle slotBattle) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.pending) >= maxPendingSlotBattles {
		b.pending = b.pending[1:]
	}
	b.pending = append(b.pending, battle)
}

// settled removes and returns the battles final at tipBlockNumber.
func (b *slotBattleBook) settled(tipBlockNumber uint64) []slotBattle {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []slotBattle
	kept := make([]slotBattle, 0, len(b.pending))
	for _, battle := range b.pending {
		if tipBlockNumber >= battle.settleAt {
			out = append(out, battle)
		} else {
			kept = append(kept, battle)
		}
	}
	b.pending = kept
	return out
}

// slotBattleSettleDepth is the number of blocks after which a battle's winner
// can no longer change. The era comes from the published snapshot, so no
// ledger lock is needed.
func (ls *LedgerState) slotBattleSettleDepth() uint64 {
	var eraId uint
	if snap := ls.loadConsensusSnapshot(); snap != nil {
		eraId = snap.currentEra.Id
	}
	return uint64(max(ls.securityParamForEraOrDefault(eraId), 1)) // #nosec G115 -- clamped positive
}

// resolveSlotBattles reports the winner of every pending battle that has
// settled: the local block wins when the chain holds it. A battle whose chain
// lookup fails stays pending, so a transient error does not lose it.
func (ls *LedgerState) resolveSlotBattles() {
	if ls.chain == nil {
		return
	}
	for _, battle := range ls.slotBattles.settled(ls.chain.Tip().BlockNumber) {
		_, _, localWon, err := ls.chain.TipRelation(
			ocommon.NewPoint(battle.slot, battle.localHash),
		)
		if err != nil {
			ls.config.Logger.Warn(
				"slot battle winner could not be determined; will retry",
				"component", "ledger",
				"slot", battle.slot,
				"error", err,
			)
			ls.slotBattles.add(battle)
			continue
		}
		ls.config.Logger.Warn(
			"slot battle settled",
			"component", "ledger",
			"slot", battle.slot,
			"local_block_hash", hex.EncodeToString(battle.localHash),
			"remote_block_hash", hex.EncodeToString(battle.remoteHash),
			"local_won", localWon,
		)
		if ls.config.EventBus != nil {
			ls.config.EventBus.PublishAsync(
				forging.SlotBattleEventType,
				event.NewEvent(
					forging.SlotBattleEventType,
					forging.SlotBattleEvent{
						Slot:            battle.slot,
						LocalBlockHash:  battle.localHash,
						RemoteBlockHash: battle.remoteHash,
						Won:             localWon,
					},
				),
			)
		}
	}
}
