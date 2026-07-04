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

	"github.com/stretchr/testify/require"
)

// assertCooldownWindow asserts the guard is in cooldown right up to, but not at,
// now+want.
func assertCooldownWindow(
	t *testing.T,
	g *leiosFetchGuard,
	now time.Time,
	want time.Duration,
) {
	t.Helper()
	require.Truef(
		t,
		g.inCooldown(now.Add(want-time.Nanosecond)),
		"expected in cooldown just before %s", want,
	)
	require.Falsef(
		t,
		g.inCooldown(now.Add(want)),
		"expected not in cooldown at %s", want,
	)
}

// TestLeiosFetchGuardCooldownEscalates verifies that consecutive backfill
// failures on the same connection escalate the cooldown exponentially from the
// base, so a connection that repeatedly returns wrong (hash-mismatching) or
// unservable/stalling responses is deprioritized for progressively longer.
func TestLeiosFetchGuardCooldownEscalates(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_780_000_000, 0)
	base := leiosBackfillConnCooldown
	g := &leiosFetchGuard{}

	// 1st failure = base, then doubling on each consecutive failure.
	g.markFetchFailed(now, base)
	assertCooldownWindow(t, g, now, base)

	g.markFetchFailed(now, base)
	assertCooldownWindow(t, g, now, 2*base)

	g.markFetchFailed(now, base)
	assertCooldownWindow(t, g, now, 4*base)

	g.markFetchFailed(now, base)
	assertCooldownWindow(t, g, now, 8*base)
}

// TestLeiosFetchGuardCooldownCaps verifies the escalating cooldown never exceeds
// leiosBackfillConnCooldownMax no matter how many consecutive failures occur.
func TestLeiosFetchGuardCooldownCaps(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_780_000_000, 0)
	base := leiosBackfillConnCooldown
	g := &leiosFetchGuard{}

	for range 50 {
		g.markFetchFailed(now, base)
	}
	// At the cap: still cooling just before the max deadline, clear at/after it.
	assertCooldownWindow(t, g, now, leiosBackfillConnCooldownMax)
}

// TestLeiosFetchGuardCooldownResetsOnSuccess verifies a successful fetch clears
// the cooldown AND resets the escalation, so the next failure starts again at
// the base cooldown rather than the escalated one.
func TestLeiosFetchGuardCooldownResetsOnSuccess(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_780_000_000, 0)
	base := leiosBackfillConnCooldown
	g := &leiosFetchGuard{}

	// Escalate a few times.
	g.markFetchFailed(now, base)
	g.markFetchFailed(now, base)
	g.markFetchFailed(now, base)
	assertCooldownWindow(t, g, now, 4*base)

	// A success clears the cooldown immediately and resets escalation.
	g.markFetchOK()
	require.False(t, g.inCooldown(now), "success should clear the cooldown")

	// The next failure starts again at the base window, not the escalated one.
	g.markFetchFailed(now, base)
	assertCooldownWindow(t, g, now, base)
}
