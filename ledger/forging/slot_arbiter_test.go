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
	"bytes"
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type fixedSlotArbiter struct {
	yield  bool
	winner string
	panics bool
	asked  []uint64
}

func (a *fixedSlotArbiter) YieldSlot(slot uint64) (bool, string) {
	a.asked = append(a.asked, slot)
	if a.panics {
		panic("arbiter fault")
	}
	return a.yield, a.winner
}

func TestForgerYieldsALeaderSlotToTheNodesWinningPool(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	forger, builder, _ := newStaleTipTestForger(t, 200, 199, 199, &logs)
	arbiter := &fixedSlotArbiter{yield: true, winner: "pool-b"}
	forger.slotArbiter = arbiter

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, []uint64{200}, arbiter.asked)
	require.Zero(t, builder.calls, "a yielded slot must not be built")
	require.Equal(t, float64(1),
		testutil.ToFloat64(forger.metrics.forgeYieldedToOwnPool))
	require.Zero(t,
		testutil.ToFloat64(forger.metrics.forgeMissedLeaderSlots),
		"yielding to the node's own pool is not a missed slot")
	require.Contains(t, logs.String(), "pool-b")
}

func TestForgerForgesWhenTheArbiterKeepsTheSlot(t *testing.T) {
	t.Parallel()
	for _, arbiter := range []*fixedSlotArbiter{
		{yield: false},
		{panics: true}, // a faulty arbiter fails open
	} {
		var logs bytes.Buffer
		forger, builder, _ := newStaleTipTestForger(t, 200, 199, 199, &logs)
		forger.slotArbiter = arbiter

		require.NoError(t, forger.checkAndForgeProduction(context.Background()))

		require.Equal(t, 1, builder.calls)
		require.Zero(t, testutil.ToFloat64(forger.metrics.forgeYieldedToOwnPool))
	}
}

func TestForgerAsksTheArbiterOnlyForLeaderSlots(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	forger, _, _ := newStaleTipTestForger(t, 200, 199, 199, &logs)
	forger.leaderChecker = forgeStaleTipTestNonLeader{}
	arbiter := &fixedSlotArbiter{yield: true}
	forger.slotArbiter = arbiter

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Empty(t, arbiter.asked)
}
