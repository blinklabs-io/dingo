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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package event

// HardForkEventType is the event type for hard fork
// (era transition) events
const HardForkEventType = EventType("hardfork.transition")

// HardForkEvent is emitted when a hard fork (era transition)
// occurs at an epoch boundary due to a protocol version change
type HardForkEvent struct {
	// Slot is the slot at which the hard fork takes effect
	Slot uint64
	// EpochNo is the epoch number where the hard fork occurs
	EpochNo uint64
	// FromEra is the era ID before the hard fork
	FromEra uint
	// ToEra is the era ID after the hard fork
	ToEra uint
	// OldMajorVersion is the protocol major version before
	OldMajorVersion uint
	// OldMinorVersion is the protocol minor version before
	OldMinorVersion uint
	// NewMajorVersion is the protocol major version after
	NewMajorVersion uint
	// NewMinorVersion is the protocol minor version after
	NewMinorVersion uint
}
