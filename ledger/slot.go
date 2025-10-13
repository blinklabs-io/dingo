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
	foundSlot := false
	for _, epoch := range ls.epochCache {
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
		lastEpoch := ls.epochCache[len(ls.epochCache)-1]
		leftoverSlots := slot - (lastEpoch.StartSlot + uint64(lastEpoch.LengthInSlots))
		slotTime = slotTime.Add(
			// nolint:gosec
			time.Duration(leftoverSlots) * (time.Duration(lastEpoch.SlotLength) * time.Millisecond),
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
	epochStartTime := shelleyGenesis.SystemStart
	var timeSlot uint64
	foundTime := false
	for _, epoch := range ls.epochCache {
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

// SlotToEpoch returns a known epoch by slot number
func (ls *LedgerState) SlotToEpoch(slot uint64) (models.Epoch, error) {
	for _, epoch := range ls.epochCache {
		if slot < epoch.StartSlot {
			continue
		}
		if slot < epoch.StartSlot+uint64(epoch.LengthInSlots) {
			return epoch, nil
		}
	}
	return models.Epoch{}, errors.New("slot not found in known epochs")
}
