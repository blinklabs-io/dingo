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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A restart that made progress is not backed off at all, and is never stuck.
func TestLedgerPipelineBackoffProgressResets(t *testing.T) {
	for _, consecutive := range []int{0, -1} {
		backoff, stuck := ledgerPipelineBackoff(consecutive)
		require.Zero(t, backoff)
		require.False(t, stuck)
	}
}

// Transient failures back off gently and are not reported as stuck: the
// pipeline restarting a handful of times is normal (a rollback racing the
// iterator, a peer dropping mid-batch) and must not raise an operator alarm.
func TestLedgerPipelineBackoffTransientFailuresAreNotStuck(t *testing.T) {
	prev := time.Duration(0)
	for consecutive := 1; consecutive < noProgressStuckThreshold; consecutive++ {
		backoff, stuck := ledgerPipelineBackoff(consecutive)
		require.False(t, stuck,
			"%d consecutive restarts should still be transient", consecutive)
		require.LessOrEqual(t, backoff, noProgressBackoffMax,
			"transient backoff must stay under the normal ceiling")
		require.GreaterOrEqual(t, backoff, prev,
			"backoff must be monotonic")
		prev = backoff
	}
	require.Equal(t, noProgressBackoffMax, prev,
		"backoff should reach the normal ceiling before the stuck threshold")
}

// A deterministic failure -- a canonical block the node rejects every time --
// never stops repeating. Capping at the transient ceiling means retrying it
// forever at that rate, which is what turned a single rejected block into a
// node that spun every two seconds indefinitely. Past the threshold the
// pipeline is declared stuck and the wait escalates well beyond the transient
// ceiling.
func TestLedgerPipelineBackoffDeterministicFailureEscalates(t *testing.T) {
	_, stuck := ledgerPipelineBackoff(noProgressStuckThreshold)
	require.True(t, stuck, "the stuck threshold should report stuck")

	// Escalates past the transient ceiling rather than sitting on it.
	longRun, stuck := ledgerPipelineBackoff(noProgressStuckThreshold + 20)
	require.True(t, stuck)
	require.Greater(t, longRun, noProgressBackoffMax,
		"a stuck pipeline must back off further than a transient one")
	require.LessOrEqual(t, longRun, noProgressStuckBackoffMax,
		"backoff must stay bounded by the stuck ceiling")

	// And is bounded no matter how long it stays stuck.
	forever, stuck := ledgerPipelineBackoff(1_000_000)
	require.True(t, stuck)
	require.Equal(t, noProgressStuckBackoffMax, forever)
}

// Monotonic across the transient/stuck boundary: the escalation must not dip.
func TestLedgerPipelineBackoffIsMonotonic(t *testing.T) {
	prev := time.Duration(0)
	for consecutive := range noProgressStuckThreshold + 64 {
		backoff, _ := ledgerPipelineBackoff(consecutive)
		require.GreaterOrEqual(t, backoff, prev,
			"backoff dipped at %d consecutive restarts", consecutive)
		prev = backoff
	}
}
