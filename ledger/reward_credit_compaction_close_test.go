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
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// The reward credit compaction job reads the LedgerDB set from a goroutine of
// its own, and the set's store panics on a read after it is closed. The
// ledger's Close waits for the job, but a set can be closed under a ledger
// that was not: closing the set must then wait for the job's read, and a job
// that starts afterwards must not read at all.
func TestClosingTheSetWaitsForARewardCreditCompactionInFlight(t *testing.T) {
	t.Parallel()
	f := newRollbackCrashFixture(t)
	ls := f.ls

	entered := make(chan struct{})
	proceed := make(chan struct{})
	abandon := make(chan struct{})
	var enteredOnce, proceedOnce, abandonOnce sync.Once
	ls.rewardCreditCompactionHook = func() {
		enteredOnce.Do(func() { close(entered) })
		select {
		case <-proceed:
		case <-abandon:
			// The test failed with the set already closed: end the job
			// without the read that would panic the whole test binary.
			runtime.Goexit()
		}
	}
	// Registered after the fixture's cleanups, so a failing test ends the
	// parked job before the ledger's Close waits for it.
	t.Cleanup(func() { abandonOnce.Do(func() { close(abandon) }) })

	ls.queueRewardCreditCompaction(context.Background())
	testutil.RequireReceive(t, entered, 5*time.Second,
		"the compaction job did not start")

	closed := make(chan error, 1)
	go func() { closed <- f.closeSet() }()
	testutil.RequireNoReceive(t, closed, 200*time.Millisecond,
		"the set closed under a compaction job that had yet to read it")

	proceedOnce.Do(func() { close(proceed) })
	require.NoError(t, testutil.RequireReceive(t, closed, 10*time.Second,
		"the set did not close once the compaction job had read it"))

	// The ledger is still open, so a job can be queued against the closed
	// set. It must end without reading it.
	ls.queueRewardCreditCompaction(context.Background())
	done := make(chan struct{})
	go func() {
		ls.rewardCreditCompactionWG.Wait()
		close(done)
	}()
	testutil.RequireReceive(t, done, 10*time.Second,
		"a compaction job queued after the set closed did not end")
}
