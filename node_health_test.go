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

package dingo

import (
	"context"
	"testing"
	"time"

	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTipGapUnknownBeforeFirstSlotTick pins the state the readiness probe
// must refuse on: a node that has opened its database but has not yet
// followed the chain reports no tip gap at all, rather than a zero gap that
// would read as "perfectly caught up".
func TestTipGapUnknownBeforeFirstSlotTick(t *testing.T) {
	n := &Node{}

	gap, ok := n.TipGapSlots()
	assert.False(t, ok, "a node with no slot tick must report no tip gap")
	assert.Zero(t, gap)
}

// TestLedgerStateConfigReportsTipGap pins the production wiring, not the
// setter: ledgerStateConfig is the single construction site both Run and a
// live Restore/Truncate rebuild use, so the readiness probe only sees a tip
// gap if that config actually carries ReportTipGapFunc. Dropping the field
// leaves everything compiling and every probe permanently unready.
func TestLedgerStateConfigReportsTipGap(t *testing.T) {
	n := &Node{
		config: Config{cfg: &internalconfig.Config{}},
	}

	cfg := n.ledgerStateConfig()
	require.NotNil(
		t,
		cfg.ReportTipGapFunc,
		"ledger config must carry the health tip-gap reporter",
	)

	cfg.ReportTipGapFunc(1234)
	gap, ok := n.TipGapSlots()
	require.True(t, ok)
	assert.Equal(t, uint64(1234), gap)

	// Later ticks replace the value rather than accumulating.
	cfg.ReportTipGapFunc(7)
	gap, ok = n.TipGapSlots()
	require.True(t, ok)
	assert.Equal(t, uint64(7), gap)

	// A zero gap is a real reading (caught up), distinct from "unknown".
	cfg.ReportTipGapFunc(0)
	gap, ok = n.TipGapSlots()
	require.True(t, ok)
	assert.Zero(t, gap)
}

// A rebuilt ledger must keep reporting: ledgerStateConfig closes over the
// Node, so the config a live Restore/Truncate builds feeds the same probe
// state as the one Run built at startup.
func TestLedgerStateConfigReportsTipGapAfterRebuild(t *testing.T) {
	n := &Node{
		config: Config{cfg: &internalconfig.Config{}},
	}

	initial := n.ledgerStateConfig()
	require.NotNil(t, initial.ReportTipGapFunc)
	initial.ReportTipGapFunc(11)

	rebuilt := n.ledgerStateConfig()
	require.NotNil(t, rebuilt.ReportTipGapFunc)
	rebuilt.ReportTipGapFunc(22)

	gap, ok := n.TipGapSlots()
	require.True(t, ok)
	assert.Equal(t, uint64(22), gap)
}

// TestLiveLifecycleTeardownForgetsTipGap covers the window a live database
// Restore or Truncate opens: closeStorageForLiveLifecycleOp stops the ledger
// that feeds the probe, and the rebuilt one does not tick until it has
// caught up again. Without clearing the gap, TipGapSlots would keep
// returning the pre-restore reading, and /readyz would answer 200 for the
// whole rebuild -- the opposite of what a readiness probe is for.
func TestLiveLifecycleTeardownForgetsTipGap(t *testing.T) {
	n := &Node{
		config: Config{cfg: &internalconfig.Config{}},
	}
	initial := n.ledgerStateConfig()
	initial.ReportTipGapFunc(3)
	gap, ok := n.TipGapSlots()
	require.True(t, ok)
	require.Equal(t, uint64(3), gap)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, n.closeStorageForLiveLifecycleOp(ctx))

	gap, ok = n.TipGapSlots()
	assert.False(
		t,
		ok,
		"a torn-down ledger must leave the tip gap unknown, not stale",
	)
	assert.Zero(t, gap)

	// A tick already dequeued by the old ledger can arrive after Close returns.
	// It must not restore the stale reading into the new lifecycle generation.
	initial.ReportTipGapFunc(9)
	gap, ok = n.TipGapSlots()
	assert.False(t, ok)
	assert.Zero(t, gap)

	// The rebuilt ledger reports into the same state and restores it.
	n.ledgerStateConfig().ReportTipGapFunc(9)
	gap, ok = n.TipGapSlots()
	require.True(t, ok)
	assert.Equal(t, uint64(9), gap)
}
