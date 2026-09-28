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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/require"
)

func TestLeiosDispatchConcurrentAdmissionBound(t *testing.T) {
	t.Parallel()
	for range 100 {
		o := &Ouroboros{}
		connID := gouroboros.ConnectionId{}
		guard := o.leiosFetchGuardFor(connID)
		guard.mu.Lock()
		var unlockOnce sync.Once
		unlock := func() { unlockOnce.Do(guard.mu.Unlock) }
		t.Cleanup(unlock)
		start := make(chan struct{})
		var callers sync.WaitGroup
		var accepted, executed atomic.Int32
		for range 64 {
			callers.Go(func() {
				<-start
				if o.dispatchLeiosFetch(connID, func() { executed.Add(1) }) {
					accepted.Add(1)
				}
			})
		}
		close(start)
		callers.Wait()
		admitted := accepted.Load()
		inflight := guard.inflight.Load()
		unlock()
		testutil.WaitForCondition(t, func() bool {
			return guard.inflight.Load() == 0
		}, 5*time.Second, "admitted dispatch workers must finish")
		require.Equal(t, int32(leiosFetchMaxInflightPerConn), admitted,
			"concurrent admission must respect the per-connection limit")
		require.Equal(t, admitted, inflight)
		require.Equal(t, admitted, executed.Load())
		// Completion returns capacity to the same connection.
		done := make(chan struct{})
		require.True(t, o.dispatchLeiosFetch(connID, func() { close(done) }))
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("released dispatch capacity was not reusable")
		}
		testutil.WaitForCondition(t, func() bool {
			return guard.inflight.Load() == 0
		}, 5*time.Second, "reused dispatch worker must finish")
	}
}
