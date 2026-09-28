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

package ouroboros

import (
	"testing"
	"time"
)

// TestLeiosFetchGuardCooldown verifies the per-connection backfill cooldown that
// FetchEndorserBlockByPoint uses to prefer healthy leios-fetch connections over
// ones that recently failed or timed out.
func TestLeiosFetchGuardCooldown(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_780_000_000, 0)
	g := &leiosFetchGuard{}

	// A fresh guard is never in cooldown.
	if g.inCooldown(now) {
		t.Fatal("fresh guard should not be in cooldown")
	}

	// After a failed fetch, the connection cools down for the configured window.
	g.markFetchFailed(now, leiosBackfillConnCooldown)
	if !g.inCooldown(now) {
		t.Fatal("guard should be in cooldown immediately after a failed fetch")
	}
	// Still cooling down just before the deadline.
	if !g.inCooldown(now.Add(leiosBackfillConnCooldown - time.Nanosecond)) {
		t.Fatal("guard should still be in cooldown before the deadline")
	}
	// No longer cooling down at/after the deadline.
	if g.inCooldown(now.Add(leiosBackfillConnCooldown)) {
		t.Fatal("guard should not be in cooldown at the deadline")
	}
	if g.inCooldown(now.Add(leiosBackfillConnCooldown + time.Second)) {
		t.Fatal("guard should not be in cooldown after the deadline")
	}

	// A successful fetch clears the cooldown immediately, even mid-window.
	g.markFetchFailed(now, leiosBackfillConnCooldown)
	g.markFetchOK()
	if g.inCooldown(now) {
		t.Fatal("markFetchOK should clear the cooldown")
	}
}
