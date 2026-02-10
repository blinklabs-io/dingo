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
	"errors"
	"math"
	"math/big"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
)

// SlotToTime returns the current time for a given slot based on known epochs
func (ls *LedgerState) SlotToTime(slot uint64) (time.Time, error) {
	if slot > math.MaxInt64 {
		return time.Time{}, errors.New("slot is larger than time.Duration")
	}
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil {
		return time.Time{}, errors.New("could not get genesis config")
	}
	slotTime := shelleyGenesis.SystemStart
	// Special case for chain genesis
	if slot == 0 {
		return slotTime, nil
	}

	// Take a deep copy of epochCache while holding the read lock
	ls.RLock()
	epochCacheCopy := make([]models.Epoch, len(ls.epochCache))
	copy(epochCacheCopy, ls.epochCache)
	ls.RUnlock()

	foundSlot := false
	for _, epoch := range epochCacheCopy {
		if epoch.StartSlot > math.MaxInt64 ||
			epoch.LengthInSlots > math.MaxInt64 ||
			epoch.SlotLength > math.MaxInt64 {
			return time.Time{}, errors.New(
				"epoch slot values are larger than time.Duration",
			)
		}
		if slot < epoch.StartSlot+uint64(epoch.LengthInSlots) {
			slotTime = slotTime.Add(
				time.Duration(
					int64(slot)-int64(epoch.StartSlot),
				) * (time.Duration(epoch.SlotLength) * time.Millisecond),
			)
			foundSlot = true
			break
		}
		slotTime = slotTime.Add(
			time.Duration(
				epoch.LengthInSlots,
			) * (time.Duration(epoch.SlotLength) * time.Millisecond),
		)
	}
	if !foundSlot {
		// Project the current slot length forward to calculate future slots
		lastEpoch := epochCacheCopy[len(epochCacheCopy)-1]
		leftoverSlots := slot - (lastEpoch.StartSlot + uint64(lastEpoch.LengthInSlots))
		slotTime = slotTime.Add(
			// nolint:gosec
			time.Duration(
				leftoverSlots,
			) * (time.Duration(lastEpoch.SlotLength) * time.Millisecond),
		)
	}
	return slotTime, nil
}

// TimeToSlot returns the slot number for a given time based on known epochs
func (ls *LedgerState) TimeToSlot(t time.Time) (uint64, error) {
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil {
		return 0, errors.New("could not get genesis config")
	}

	// Take a deep copy of epochCache while holding the read lock
	ls.RLock()
	epochCacheCopy := make([]models.Epoch, len(ls.epochCache))
	copy(epochCacheCopy, ls.epochCache)
	ls.RUnlock()

	epochStartTime := shelleyGenesis.SystemStart
	var timeSlot uint64
	foundTime := false
	for _, epoch := range epochCacheCopy {
		if epoch.LengthInSlots > math.MaxInt64 ||
			epoch.SlotLength > math.MaxInt64 {
			return 0, errors.New(
				"epoch slot values are larger than time.Duration",
			)
		}
		slotDuration := time.Duration(epoch.SlotLength) * time.Millisecond
		if slotDuration < 0 {
			return 0, errors.New("slot duration is negative")
		}
		epochEndTime := epochStartTime.Add(
			time.Duration(epoch.LengthInSlots) * slotDuration,
		)
		if (t.Equal(epochStartTime) || t.After(epochStartTime)) &&
			t.Before(epochEndTime) {
			// Figure out how far into the epoch the specified time is
			timeDiff := t.Sub(epochStartTime)
			//nolint:gosec
			// This will never overflow using 2 positive int64 values, but gosec seems determined
			// to complain about it
			timeSlot += uint64(timeDiff / slotDuration)
			foundTime = true
			break
		}
		epochStartTime = epochEndTime
		timeSlot += uint64(epoch.LengthInSlots)
	}
	if !foundTime {
		// Special case for current time
		// This is mostly useful at chain genesis
		if time.Since(t) < (5 * time.Second) {
			sinceStart := time.Since(
				shelleyGenesis.SystemStart,
			) / time.Millisecond
			slotLength := uint(
				new(big.Int).Div(
					new(big.Int).Mul(
						big.NewInt(1000),
						shelleyGenesis.SlotLength.Num(),
					),
					shelleyGenesis.SlotLength.Denom(),
				).Uint64(),
			)
			// nolint:gosec
			// Slot length is small enough to not overflow int64
			timeSlot := uint64(sinceStart / time.Duration(slotLength))
			return timeSlot, nil
		}
		return timeSlot, errors.New("time not found in known epochs")
	}
	return timeSlot, nil
}

// SlotToEpoch returns an epoch by slot number.
// For slots within known epochs, returns the exact epoch.
// For slots beyond known epochs, projects forward using the last known epoch's
// parameters (epoch length, slot length) to calculate the future epoch.
func (ls *LedgerState) SlotToEpoch(slot uint64) (models.Epoch, error) {
	// Take a deep copy of epochCache while holding the read lock
	ls.RLock()
	epochCacheCopy := make([]models.Epoch, len(ls.epochCache))
	copy(epochCacheCopy, ls.epochCache)
	ls.RUnlock()

	if len(epochCacheCopy) == 0 {
		return models.Epoch{}, errors.New("no epochs in cache")
	}

	// Guard: reject slots before the first known epoch
	firstEpoch := epochCacheCopy[0]
	if slot < firstEpoch.StartSlot {
		return models.Epoch{}, errors.New(
			"slot is before the first known epoch",
		)
	}

	for _, epoch := range epochCacheCopy {
		if slot < epoch.StartSlot {
			continue
		}
		if slot < epoch.StartSlot+uint64(epoch.LengthInSlots) {
			return epoch, nil
		}
	}

	// Project forward for future slots using the last known epoch's parameters
	lastEpoch := epochCacheCopy[len(epochCacheCopy)-1]
	lastEpochEndSlot := lastEpoch.StartSlot + uint64(lastEpoch.LengthInSlots)

	// Calculate how many epochs past the last known epoch this slot is
	slotsPastLastEpoch := slot - lastEpochEndSlot
	epochsPastLast := slotsPastLastEpoch / uint64(lastEpoch.LengthInSlots)
	slotInProjectedEpoch := slotsPastLastEpoch % uint64(lastEpoch.LengthInSlots)

	// Build projected epoch info
	projectedEpochId := lastEpoch.EpochId + 1 + epochsPastLast
	projectedStartSlot := lastEpochEndSlot + (epochsPastLast * uint64(lastEpoch.LengthInSlots))

	// Verify our calculation is correct (slot should be within the projected epoch)
	if slot < projectedStartSlot ||
		slot >= projectedStartSlot+uint64(lastEpoch.LengthInSlots) {
		// This shouldn't happen, but return error if our math is wrong
		return models.Epoch{}, errors.New(
			"internal error: projected epoch calculation mismatch",
		)
	}
	_ = slotInProjectedEpoch // Used only for verification

	return models.Epoch{
		EpochId:       projectedEpochId,
		StartSlot:     projectedStartSlot,
		EraId:         lastEpoch.EraId,
		SlotLength:    lastEpoch.SlotLength,
		LengthInSlots: lastEpoch.LengthInSlots,
		// Nonce is unknown for future epochs
		Nonce: nil,
	}, nil
}
