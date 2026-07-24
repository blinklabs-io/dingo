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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// blockingRegisterer wraps a real prometheus.Registerer, but its Register
// method signals startedCh once (closing it) and then blocks on proceedCh
// before delegating — used to pause a rebuildableRegisterer.Register call
// at a controlled point, so a test can deterministically race it against
// a concurrent unregisterAll call.
type blockingRegisterer struct {
	real      prometheus.Registerer
	startedCh chan struct{}
	proceedCh chan struct{}
}

func (b *blockingRegisterer) Register(c prometheus.Collector) error {
	close(b.startedCh)
	<-b.proceedCh
	return b.real.Register(c)
}

func (b *blockingRegisterer) MustRegister(cs ...prometheus.Collector) {
	for _, c := range cs {
		if err := b.Register(c); err != nil {
			panic(err)
		}
	}
}

func (b *blockingRegisterer) Unregister(c prometheus.Collector) bool {
	return b.real.Unregister(c)
}

var _ prometheus.Registerer = &blockingRegisterer{}

// TestRebuildableRegistererRegisterIsAtomicWithUnregisterAll guards against
// comment-23's original bug: Register only held r.mu around appending c to
// r.collectors, not around the call into the underlying registerer. A
// concurrent unregisterAll snapshotting r.collectors while a Register call
// had already succeeded against the real registry but not yet appended
// would miss that collector entirely -- it "escapes" the cleanup pass
// even though it is genuinely registered, so the next rebuild cycle's
// attempt to register a fresh collector under the same metric name hits a
// duplicate-registration error most callers (chain.NewManager,
// ledger.NewLedgerState, mempool.New, ...) don't handle gracefully.
//
// This pauses a Register call inside the underlying registerer (before
// the fix's widened lock would have been acquired for the append) and
// confirms a concurrent unregisterAll cannot proceed past its own
// snapshot-and-clear until Register fully completes -- proving the two
// are now atomic with respect to each other, not just the append.
func TestRebuildableRegistererRegisterIsAtomicWithUnregisterAll(t *testing.T) {
	blocking := &blockingRegisterer{
		real:      prometheus.NewRegistry(),
		startedCh: make(chan struct{}),
		proceedCh: make(chan struct{}),
	}
	r := newRebuildableRegisterer(blocking)

	registerErr := make(chan error, 1)
	go func() {
		registerErr <- r.Register(prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "test_rebuildable_registerer_gauge",
		}))
	}()
	testutil.RequireReceive(
		t, blocking.startedCh, time.Second,
		"Register must reach the underlying registerer",
	)

	unregisterAllDone := make(chan struct{})
	go func() {
		r.unregisterAll()
		close(unregisterAllDone)
	}()

	testutil.RequireNoReceive(
		t, unregisterAllDone, 150*time.Millisecond,
		"unregisterAll must not complete while a concurrent Register call "+
			"is still in flight inside the underlying registerer",
	)

	close(blocking.proceedCh)

	require.NoError(t, <-registerErr)
	testutil.RequireReceive(
		t, unregisterAllDone, time.Second,
		"unregisterAll must complete once the concurrent Register call finishes",
	)
}
