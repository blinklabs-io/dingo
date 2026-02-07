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

package keystore

import "time"

// KES (Key Evolving Signature) constants for Cardano.
// These values are defined in the Shelley genesis configuration and
// determine how often KES keys must be evolved.
// Default values are for the preview network (Dingo's default network).
const (
	// DefaultSlotsPerKESPeriod is the number of slots in a KES period on preview.
	// After this many slots, the KES key must be evolved to the next period.
	// Preview value: 129600 slots = 1.5 days (at 1 second per slot)
	DefaultSlotsPerKESPeriod = 129600

	// DefaultMaxKESEvolutions is the maximum number of times a KES key can evolve.
	// For Cardano's KES depth of 6, this is 2^6 - 2 = 62 evolutions.
	// After this many evolutions, a new operational certificate must be issued.
	DefaultMaxKESEvolutions = 62

	// DefaultSlotDuration is the default slot duration on preview (1 second).
	DefaultSlotDuration = time.Second
)

// KESPeriodDuration returns the approximate wall-clock duration of a KES period.
// This is useful for calculating when the next evolution will occur.
func KESPeriodDuration(slotsPerPeriod uint64, slotDuration time.Duration) time.Duration {
	//nolint:gosec // G115: slotsPerPeriod is bounded by protocol params, will not overflow
	return time.Duration(slotsPerPeriod) * slotDuration
}

// DefaultKESPeriodDuration returns the KES period duration using preview defaults.
// This is approximately 1.5 days.
func DefaultKESPeriodDuration() time.Duration {
	return KESPeriodDuration(DefaultSlotsPerKESPeriod, DefaultSlotDuration)
}

// OpCertLifetimeSlots returns the total number of slots an OpCert is valid for.
func OpCertLifetimeSlots(slotsPerPeriod, maxEvolutions uint64) uint64 {
	return slotsPerPeriod * maxEvolutions
}

// DefaultOpCertLifetimeSlots returns the OpCert lifetime using preview defaults.
// This is approximately 93 days (62 periods * 1.5 days per period).
func DefaultOpCertLifetimeSlots() uint64 {
	return OpCertLifetimeSlots(DefaultSlotsPerKESPeriod, DefaultMaxKESEvolutions)
}

// OpCertLifetimeDuration returns the approximate wall-clock duration an OpCert is valid.
func OpCertLifetimeDuration(
	slotsPerPeriod, maxEvolutions uint64,
	slotDuration time.Duration,
) time.Duration {
	//nolint:gosec // G115: these values are bounded by protocol params, will not overflow
	return time.Duration(slotsPerPeriod*maxEvolutions) * slotDuration
}

// DefaultOpCertLifetimeDuration returns the OpCert lifetime using preview defaults.
func DefaultOpCertLifetimeDuration() time.Duration {
	return OpCertLifetimeDuration(
		DefaultSlotsPerKESPeriod,
		DefaultMaxKESEvolutions,
		DefaultSlotDuration,
	)
}

// SlotToKESPeriod converts a slot number to a KES period.
// Returns 0 if slotsPerPeriod is 0 to avoid division by zero.
func SlotToKESPeriod(slot, slotsPerPeriod uint64) uint64 {
	if slotsPerPeriod == 0 {
		return 0
	}
	return slot / slotsPerPeriod
}

// KESPeriodStartSlot returns the first slot of the given KES period.
func KESPeriodStartSlot(period, slotsPerPeriod uint64) uint64 {
	return period * slotsPerPeriod
}

// KESPeriodEndSlot returns the last slot of the given KES period.
// Returns 0 if slotsPerPeriod is 0 to avoid underflow.
func KESPeriodEndSlot(period, slotsPerPeriod uint64) uint64 {
	if slotsPerPeriod == 0 {
		return 0
	}
	return (period+1)*slotsPerPeriod - 1
}

// ExpiryWarningThresholds defines warning thresholds for OpCert expiry.
// These are the number of KES periods remaining before expiry.
type ExpiryWarningThresholds struct {
	// Critical: OpCert is about to expire, cannot produce blocks soon
	Critical uint64
	// Warning: OpCert should be renewed soon
	Warning uint64
	// Info: OpCert expiry is approaching
	Info uint64
}

// DefaultExpiryWarningThresholds returns the default warning thresholds.
func DefaultExpiryWarningThresholds() ExpiryWarningThresholds {
	return ExpiryWarningThresholds{
		Critical: 3,  // ~4.5 days remaining
		Warning:  7,  // ~10.5 days remaining
		Info:     14, // ~21 days remaining
	}
}
