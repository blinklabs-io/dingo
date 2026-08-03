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
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCloseStopsForgingScheduler verifies that Close stops ls.Scheduler,
// not just ls.slotClock/ls.dbWorkerPool. initForge registers the
// dev-mode block-forging task (ls.forgeBlock) on this scheduler as a
// fixed-interval task that writes directly to ls.chain/the database in
// its own transaction, entirely bypassing ls.dbWorkerPool -- so shutting
// down dbWorkerPool alone does not stop it. Left running past Close, a
// live restore/truncate's quiesce (which only stops the production
// BlockForger, node_lifecycle.go) would leave this scheduler free to
// keep firing forgeBlock against a LedgerState being closed and replaced
// out from under it, racing the live operation's own storage mutations
// and the subsequently-constructed LedgerState's own new Scheduler. A
// stray block landing in that window can leave the persistent block-ID
// index with a gap whose far side doesn't chain from the post-operation
// tip -- surfacing later as a "persistent chain index gap" error from
// the chain iterator (chain/chain.go) and a permanently stalled tip.
//
// This registers a plain counting task directly on ls.Scheduler rather
// than exercising the real forgeBlock (which needs a full genesis/
// mempool/VRF setup to run without erroring) -- the bug is specifically
// about Close's own resource-shutdown discipline forgetting this
// scheduler, not about forgeBlock's own logic, so a minimal stand-in
// task exercises the exact same missing-Stop-call gap.
func TestCloseStopsForgingScheduler(t *testing.T) {
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.Scheduler = NewScheduler(time.Millisecond)
	ls.Scheduler.Start()

	var ticks atomic.Int64
	ls.Scheduler.Register(1, func() { ticks.Add(1) }, nil)

	// Confirm the scheduler is actually running before Close -- otherwise
	// the require.Never check below would pass vacuously against a
	// scheduler that was never ticking in the first place.
	require.Eventually(
		t, func() bool { return ticks.Load() > 0 },
		time.Second, time.Millisecond,
		"scheduler must be ticking before Close",
	)

	require.NoError(t, ls.Close())

	afterClose := ticks.Load()
	require.Never(
		t, func() bool { return ticks.Load() != afterClose },
		100*time.Millisecond, 5*time.Millisecond,
		"Close must stop the scheduler: no further ticks may fire afterward",
	)
}
