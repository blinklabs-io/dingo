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
	"testing"

	"github.com/stretchr/testify/require"
)

// A panic between the reservation and the payload copy (the allocation-failure
// window) must give the reservation back. A leaked one is permanent queue
// capacity lost to nothing.
func TestLeiosPersistEnqueueUnwindReleasesReservation(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithPausedLeiosPersistWriter(t)
	point, blockRaw, data, size := leiosPersistTestEntry(t, 70, 4, 512)
	o.leiosPersistAfterReserve = func() {
		_, bytes, reserved := leiosPersistQueueState(o)
		require.Equal(t, size, bytes, "reservation must precede the copy")
		require.Equal(t, 1, reserved)
		panic("simulated allocation failure")
	}

	require.Panics(t, func() {
		o.enqueueLeiosPersist(point, blockRaw, data)
	})

	queued, bytes, reserved := leiosPersistQueueState(o)
	require.Zero(t, queued)
	require.Zero(t, bytes, "an unwound enqueue must release its bytes")
	require.Zero(t, reserved, "an unwound enqueue must release its slot")
}

// A complete job that lands while a manifest-only job for the same hash is
// being copied wins, and the manifest-only job's reservation is released
// exactly once rather than replacing or double-charging the complete one.
func TestLeiosPersistManifestOnlyLosesRaceToCompleteJob(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithPausedLeiosPersistWriter(t)
	point, blockRaw, data, completeSize := leiosPersistTestEntry(
		t, 71, 4, 512,
	)
	manifestSize := leiosPersistJobSize(point.Hash, blockRaw, nil)
	key := leiosBlockKey(point.Slot, point.Hash)

	admitted, _ := o.reserveLeiosPersistBytes(key, manifestSize, false)
	require.True(t, admitted)

	// The complete job is admitted and installed inside that window.
	o.enqueueLeiosPersist(point, blockRaw, data)
	queued, bytes, reserved := leiosPersistQueueState(o)
	require.Equal(t, 1, queued)
	require.Equal(t, completeSize+manifestSize, bytes)
	require.Equal(t, 1, reserved)

	installed := o.installLeiosPersistJob(key, &leiosPersistJob{
		slot:        point.Slot,
		hash:        point.Hash,
		manifestRaw: blockRaw,
		size:        manifestSize,
	})
	require.False(t, installed)

	queued, bytes, reserved = leiosPersistQueueState(o)
	require.Equal(t, 1, queued)
	require.Equal(t, completeSize, bytes)
	require.Zero(t, reserved)
	require.NotNil(t, o.leiosPersistPending[key].txsRaw)
}

// Concurrent admission of distinct entries against a budget that holds exactly
// K of them admits exactly K, whatever the interleaving, and a drain returns
// the queue to zero.
// Not t.Parallel: withLowerLeiosPersistQueueBudget swaps the package-level
// leiosPersistMaxQueueBytes.
func TestLeiosPersistQueueConcurrentAdmissionHonorsByteBudget(t *testing.T) {
	const (
		enqueuers = 24
		fits      = 5
	)
	o := newTestOuroborosWithPausedLeiosPersistWriter(t)
	_, _, _, size := leiosPersistTestEntry(t, 200, 4, 512)
	withLowerLeiosPersistQueueBudget(t, fits*size)

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range enqueuers {
		point, blockRaw, data, s := leiosPersistTestEntry(t, 200+i, 4, 512)
		require.Equal(t, size, s, "entries must be equal-sized")
		wg.Go(func() {
			<-start
			o.enqueueLeiosPersist(point, blockRaw, data)
		})
	}
	close(start)
	wg.Wait()

	queued, bytes, reserved := leiosPersistQueueState(o)
	require.Equal(t, fits, queued)
	require.Equal(t, fits*size, bytes)
	require.Zero(t, reserved)
	require.Equal(t, uint64(enqueuers-fits), o.leiosPersistDropped.Load())

	o.drainLeiosPersist()
	queued, bytes, reserved = leiosPersistQueueState(o)
	require.Zero(t, queued)
	require.Zero(t, bytes)
	require.Zero(t, reserved)
}
