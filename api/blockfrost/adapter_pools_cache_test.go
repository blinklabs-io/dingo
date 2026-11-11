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

package blockfrost

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func TestActivePoolCacheLoadsOncePerTip(t *testing.T) {
	t.Parallel()
	var cache activePoolCache
	var loads atomic.Int32
	load := func() ([][]byte, bool, error) {
		loads.Add(1)
		return [][]byte{{1}, {2}}, true, nil
	}
	tipA := activePoolCacheKey{slot: 10, hash: "a"}

	first, err := cache.get(context.Background(), tipA, load)
	require.NoError(t, err)
	// A caller reorders its copy; the cached list must not follow.
	first[0], first[1] = first[1], first[0]
	second, err := cache.get(context.Background(), tipA, load)
	require.NoError(t, err)
	require.Equal(t, [][]byte{{1}, {2}}, second)
	require.Equal(t, int32(1), loads.Load())

	// A rollback to the same slot on another block is another tip.
	_, err = cache.get(
		context.Background(), activePoolCacheKey{slot: 10, hash: "b"}, load,
	)
	require.NoError(t, err)
	require.Equal(t, int32(2), loads.Load())
}

func TestActivePoolCacheDoesNotCacheFailure(t *testing.T) {
	t.Parallel()
	var cache activePoolCache
	key := activePoolCacheKey{slot: 1, hash: "a"}
	boom := errors.New("boom")
	_, err := cache.get(context.Background(), key, func() ([][]byte, bool, error) {
		return nil, false, boom
	})
	require.ErrorIs(t, err, boom)
	got, err := cache.get(context.Background(), key, func() ([][]byte, bool, error) {
		return [][]byte{{9}}, true, nil
	})
	require.NoError(t, err)
	require.Equal(t, [][]byte{{9}}, got)
}

// TestActivePoolCacheSharesConcurrentLoad covers requests arriving at one tip
// while the list is being computed: they wait for that computation rather
// than start their own, and a waiter whose context ends stops waiting.
func TestActivePoolCacheSharesConcurrentLoad(t *testing.T) {
	t.Parallel()
	var cache activePoolCache
	key := activePoolCacheKey{slot: 1, hash: "a"}
	var loads atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	load := func() ([][]byte, bool, error) {
		loads.Add(1)
		close(entered)
		<-release
		return [][]byte{{1}}, true, nil
	}

	var wg sync.WaitGroup
	results := make(chan [][]byte, 2)
	wg.Go(func() {
		got, err := cache.get(context.Background(), key, load)
		require.NoError(t, err)
		results <- got
	})
	testutil.RequireReceive(t, entered, 5*time.Second, "load started")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := cache.get(ctx, key, load)
	require.ErrorIs(t, err, context.Canceled)

	wg.Go(func() {
		got, err := cache.get(context.Background(), key, load)
		require.NoError(t, err)
		results <- got
	})
	// Two goroutines are inside get once the waiter is parked: the loader
	// and the waiter.
	require.Eventually(t, func() bool {
		buf := make([]byte, 1<<20)
		buf = buf[:runtime.Stack(buf, true)]
		return strings.Count(string(buf), "(*activePoolCache).get(") >= 2
	}, 5*time.Second, 5*time.Millisecond)
	close(release)
	wg.Wait()
	require.Equal(t, int32(1), loads.Load())
	require.Equal(t, [][]byte{{1}}, <-results)
	require.Equal(t, [][]byte{{1}}, <-results)
}

func TestActivePoolCacheSkipsUncacheableLoad(t *testing.T) {
	t.Parallel()
	var cache activePoolCache
	key := activePoolCacheKey{slot: 1, hash: "a"}
	var loads atomic.Int32
	got, err := cache.get(context.Background(), key,
		func() ([][]byte, bool, error) {
			loads.Add(1)
			return [][]byte{{1}}, false, nil
		})
	require.NoError(t, err)
	require.Equal(t, [][]byte{{1}}, got)
	_, err = cache.get(context.Background(), key,
		func() ([][]byte, bool, error) {
			loads.Add(1)
			return [][]byte{{2}}, true, nil
		})
	require.NoError(t, err)
	require.Equal(t, int32(2), loads.Load())
}

func TestActivePoolCachePanicReleasesWaiters(t *testing.T) {
	t.Parallel()
	var cache activePoolCache
	key := activePoolCacheKey{slot: 1, hash: "a"}
	require.Panics(t, func() {
		_, _ = cache.get(context.Background(), key,
			func() ([][]byte, bool, error) { panic("boom") })
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := cache.get(ctx, key, func() ([][]byte, bool, error) {
		return [][]byte{{3}}, true, nil
	})
	require.NoError(t, err)
	require.Equal(t, [][]byte{{3}}, got)
}
