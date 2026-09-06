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

package forging

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestEBSelectionDeadlineBoundsALateSlot rejects the earlier "an expired
// budget drops the bound" behaviour. A slot whose window has already
// closed must not trigger a full mempool re-validation pass: the pass
// would finish even later, for a block nobody is waiting for. It is
// bounded by a minimal fixed budget instead.
func TestEBSelectionDeadlineBoundsALateSlot(t *testing.T) {
	now := time.Now()
	forger := &BlockForger{
		now:                     func() time.Time { return now },
		forgeEBSelectionReserve: 300 * time.Millisecond,
		slotClock: &ebTestSlotClock{
			currentSlot: 10,
			// The slot ended a second ago.
			slotEnd: now.Add(-time.Second),
		},
	}

	deadline, ok := forger.ebSelectionDeadline(10)
	require.True(t, ok, "a late slot still gets a bound")
	require.Equal(t, now.Add(300*time.Millisecond), deadline)
}

// TestEBSelectionDeadlineBoundsWithoutASlotClock closes the other
// unbounded path for the same reason.
func TestEBSelectionDeadlineBoundsWithoutASlotClock(t *testing.T) {
	now := time.Now()
	forger := &BlockForger{
		now:                     func() time.Time { return now },
		forgeEBSelectionReserve: 300 * time.Millisecond,
	}

	_, ok := forger.ebSelectionDeadline(10)
	require.False(t, ok, "no clock means no slot-derived deadline")
}

// TestEBSelectionDeadlineShrinksReserveOnShortSlots covers fast-slot
// networks. With a 100ms slot a fixed 300ms reserve puts the deadline
// before the slot even began, which would leave selection with no budget
// at all -- or, before this, unbounded. The reserve never takes more than
// half of what is left.
func TestEBSelectionDeadlineShrinksReserveOnShortSlots(t *testing.T) {
	now := time.Now()
	slotEnd := now.Add(100 * time.Millisecond)
	forger := &BlockForger{
		now:                     func() time.Time { return now },
		forgeEBSelectionReserve: 300 * time.Millisecond,
		slotClock: &ebTestSlotClock{
			currentSlot: 10,
			slotEnd:     slotEnd,
		},
	}

	deadline, ok := forger.ebSelectionDeadline(10)
	require.True(t, ok)
	require.Equal(
		t,
		slotEnd.Add(-50*time.Millisecond),
		deadline,
		"the reserve is capped at half the remaining slot",
	)
	require.True(t, deadline.After(now), "selection must still get budget")
}

// TestEBSelectionDeadlineUsesFullReserveOnNormalSlots is the negative
// case: on a one-second slot the configured reserve applies unchanged.
func TestEBSelectionDeadlineUsesFullReserveOnNormalSlots(t *testing.T) {
	now := time.Now()
	slotEnd := now.Add(time.Second)
	forger := &BlockForger{
		now:                     func() time.Time { return now },
		forgeEBSelectionReserve: 300 * time.Millisecond,
		slotClock: &ebTestSlotClock{
			currentSlot: 10,
			slotEnd:     slotEnd,
		},
	}

	deadline, ok := forger.ebSelectionDeadline(10)
	require.True(t, ok)
	require.Equal(t, slotEnd.Add(-300*time.Millisecond), deadline)
}
