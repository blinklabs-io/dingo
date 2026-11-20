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
	"slices"
	"sync"
)

// activePoolCacheKey identifies the chain state an active pool list was
// computed at. The active set only changes when a block is applied or rolled
// back, and either moves the tip, so a list is valid for exactly one tip
// point.
type activePoolCacheKey struct {
	slot uint64
	hash string
}

// activePoolCache holds the ordered active pool key hashes for the latest tip
// it served. Computing them ranks every pool registration and retirement
// certificate ever recorded, so concurrent requests at one tip share a single
// computation and later ones reuse its result. The zero value is ready to use.
type activePoolCache struct {
	mu      sync.Mutex
	valid   bool
	key     activePoolCacheKey
	hashes  [][]byte
	loading chan struct{}
}

// get returns the active pool key hashes for key, calling load when no list
// for key is cached. load must compute the list from the state key names; see
// PoolsList. A caller that finds a load in flight waits for it, or for ctx,
// and then rechecks the cache; a failed or panicking load is not cached and
// releases the waiters. The returned slice is the caller's to reorder.
func (c *activePoolCache) get(
	ctx context.Context,
	key activePoolCacheKey,
	load func() ([][]byte, error),
) ([][]byte, error) {
	for {
		c.mu.Lock()
		if c.valid && c.key == key {
			hashes := slices.Clone(c.hashes)
			c.mu.Unlock()
			return hashes, nil
		}
		if wait := c.loading; wait != nil {
			c.mu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		done := make(chan struct{})
		c.loading = done
		c.mu.Unlock()

		return c.run(key, done, load)
	}
}

// run executes load and releases the in-flight slot in a defer, so a panic in
// load cannot leave later callers waiting on a channel nothing will close.
func (c *activePoolCache) run(
	key activePoolCacheKey,
	done chan struct{},
	load func() ([][]byte, error),
) ([][]byte, error) {
	var (
		hashes   [][]byte
		err      error
		returned bool
	)
	defer func() {
		c.mu.Lock()
		if returned && err == nil {
			c.valid, c.key, c.hashes = true, key, slices.Clone(hashes)
		}
		c.loading = nil
		c.mu.Unlock()
		close(done)
	}()
	hashes, err = load()
	returned = true
	return hashes, err
}
