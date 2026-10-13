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

package ratewindow

import (
	"testing"
	"time"
)

func TestFixedWindowAdmissionLimitsAndReset(t *testing.T) {
	start := time.Date(2026, time.September, 27, 12, 0, 0, 0, time.UTC)
	window := NewFixedWindow(time.Minute, 2, 3, 2)
	for range 2 {
		if got := window.Admit("peer-a", start); got != Admitted {
			t.Fatalf("Admit(peer-a) = %v, want Admitted", got)
		}
	}
	if got := window.Admit("peer-a", start); got != PeerBudgetExceeded {
		t.Fatalf("Admit(peer-a) = %v, want PeerBudgetExceeded", got)
	}
	if got := window.Admit("peer-b", start); got != Admitted {
		t.Fatalf("Admit(peer-b) = %v, want Admitted", got)
	}
	if got := window.Admit("peer-c", start); got != PeerTrackingCapacityExceeded {
		t.Fatalf("Admit(peer-c) = %v, want PeerTrackingCapacityExceeded", got)
	}
	if got := window.Admit("peer-b", start); got != ProcessBudgetExceeded {
		t.Fatalf("Admit(peer-b) = %v, want ProcessBudgetExceeded", got)
	}
	if got := window.Admit("peer-a", start.Add(time.Minute)); got != Admitted {
		t.Fatalf("Admit after window = %v, want Admitted", got)
	}
}

func TestFixedWindowRecordBoundsKeysUntilWindowExpires(t *testing.T) {
	start := time.Date(2026, time.September, 27, 12, 0, 0, 0, time.UTC)
	window := NewFixedWindow(time.Minute, 0, 0, 1)
	if count, ok := window.Record("peer-a", start); !ok || count != 1 {
		t.Fatalf("Record(peer-a) = (%d, %t), want (1, true)", count, ok)
	}
	if count, ok := window.Record("peer-b", start); ok || count != 0 {
		t.Fatalf("Record(peer-b) = (%d, %t), want (0, false)", count, ok)
	}
	if count, ok := window.Record("peer-b", start.Add(time.Minute)); !ok || count != 1 {
		t.Fatalf("Record after window = (%d, %t), want (1, true)", count, ok)
	}
}
