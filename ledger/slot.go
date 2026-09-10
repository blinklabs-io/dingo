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

package ledger

import (
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

// timeConv returns this LedgerState's SlotTimeConverter, building and caching
// it on first access via timeConverterOnce. The converter's dependencies
// close back over ls to read era history and genesis fresh on every call, so
// this lazy accessor exists only to give bare-constructed LedgerStates (as
// used throughout this package's tests) a converter without requiring
// NewLedgerState; production callers get the converter built eagerly by
// NewLedgerState, so this is a formality for them.
//
// sync.Once makes the lazy build itself race-free regardless of caller
// discipline, rather than relying on test code never racing the first call.
func (ls *LedgerState) timeConv() *SlotTimeConverter {
	ls.timeConverterOnce.Do(func() {
		if ls.timeConverter == nil {
			ls.timeConverter = ls.newTimeConverter()
		}
	})
	return ls.timeConverter
}

// newTimeConverter builds a SlotTimeConverter wired to this LedgerState's
// era history (via HardForkSummary) and genesis config.
func (ls *LedgerState) newTimeConverter() *SlotTimeConverter {
	return NewSlotTimeConverter(SlotTimeConverterDeps{
		HardForkSummary: ls.hardForkSummaryAnchoredAt,
		ShelleyGenesis: func() *shelley.ShelleyGenesis {
			if ls.config.CardanoNodeConfig == nil {
				return nil
			}
			return ls.config.CardanoNodeConfig.ShelleyGenesis()
		},
		EpochCache: func() []models.Epoch {
			return ls.loadConsensusSnapshot().epochCache
		},
	})
}

// SlotToTime returns the wall-clock start time of the given slot. See
// SlotTimeConverter.SlotToTime for details.
func (ls *LedgerState) SlotToTime(slot uint64) (time.Time, error) {
	return ls.timeConv().SlotToTime(slot)
}

// SlotToTimeWithHorizonFrom returns the wall-clock start time of the given
// slot with the forecast horizon measured from horizonAnchorSlot. See
// SlotTimeConverter.SlotToTimeWithHorizonFrom.
func (ls *LedgerState) SlotToTimeWithHorizonFrom(
	horizonAnchorSlot uint64,
	slot uint64,
) (time.Time, error) {
	return ls.timeConv().SlotToTimeWithHorizonFrom(horizonAnchorSlot, slot)
}

// TimeToSlot returns the slot containing the given wall-clock time. See
// SlotTimeConverter.TimeToSlot for details.
func (ls *LedgerState) TimeToSlot(t time.Time) (uint64, error) {
	return ls.timeConv().TimeToSlot(t)
}

// SlotToEpoch returns the epoch containing the given slot. See
// SlotTimeConverter.SlotToEpoch for details.
func (ls *LedgerState) SlotToEpoch(slot uint64) (models.Epoch, error) {
	return ls.timeConv().SlotToEpoch(slot)
}

// EpochInfo returns boundary information for the given epoch. See
// SlotTimeConverter.EpochInfo for details.
func (ls *LedgerState) EpochInfo(epoch uint64) (models.Epoch, error) {
	return ls.timeConv().EpochInfo(epoch)
}

// shelleySlotLength returns the Shelley-era slot length as a duration, or 0 if
// the genesis is unavailable. See SlotTimeConverter.shelleySlotLength.
func (ls *LedgerState) shelleySlotLength() time.Duration {
	return ls.timeConv().shelleySlotLength()
}

// EndorserBlockWaitDuration returns the wall-clock window to wait for a
// referenced/certified endorser block's transaction closure to become
// available, derived from the Leios pipeline timing (EndorserBlockWaitSlots,
// the certify-by deadline) and the Shelley slot length. It returns 0 when the
// wait is disabled or the slot length is unknown. This is the same window
// ledger application uses to gate a ranking block on its endorser block (see
// ensureReferencedEndorserBlocks), so NtC serving and ledger application wait
// for the same healthy closure-delivery window.
func (ls *LedgerState) EndorserBlockWaitDuration() time.Duration {
	return ls.timeConv().
		EndorserBlockWaitDuration(ls.config.EndorserBlockWaitSlots)
}
