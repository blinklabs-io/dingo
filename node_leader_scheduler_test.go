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
	"sync"
	"sync/atomic"
	"testing"

	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/stretchr/testify/require"
)

type restoreCountingScheduler struct {
	restored atomic.Int64
}

func (s *restoreCountingScheduler) Start(context.Context) error    { return nil }
func (s *restoreCountingScheduler) Stop() error                    { return nil }
func (s *restoreCountingScheduler) ConsensusStateRestored()        { s.restored.Add(1) }
func (s *restoreCountingScheduler) ShouldProduceBlock(uint64) bool { return false }
func (s *restoreCountingScheduler) NextLeaderSlot(uint64) (uint64, bool) {
	return 0, false
}

// The ledger's rollback callback runs on the ledger goroutine while block
// producer startup publishes the scheduler, so the two must not share an
// unsynchronized field. Run under -race.
func TestConsensusStateRestoredReadsPublishedScheduler(t *testing.T) {
	t.Parallel()

	n := &Node{config: Config{cfg: &internalconfig.Config{}}}
	restored := n.ledgerStateConfig().ConsensusStateRestoredFunc
	require.NotNil(t, restored)
	scheduler := &restoreCountingScheduler{}

	var wg sync.WaitGroup
	wg.Go(func() {
		for range 200 {
			n.setLeaderElection(scheduler)
		}
	})
	for range 200 {
		restored()
	}
	wg.Wait()
	before := scheduler.restored.Load()
	restored()
	require.Equal(t, before+1, scheduler.restored.Load())
}
