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
	"fmt"
	"slices"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// PoolsList returns the paginated list of currently registered
// (active, non-retired) stake pool IDs, along with the total number of
// matching results before pagination.
//
// Ordering: the store query (GetActivePoolKeyHashesOrdered) returns every
// active pool key hash already sorted oldest-first by each pool's FIRST
// on-chain registration certificate -- not its most recent one -- which is
// the chain-derived order the pool_list schema's "oldest first, newest
// last" wording calls for (see poolorder.GetActivePoolKeyHashesOrdered's
// doc comment for the full rationale, including why this is a deliberate,
// reversible semantic choice rather than one the schema pins). asc uses
// that order as-is; desc reverses the same slice, so the two are exact
// reverses of each other by construction rather than by two
// independently-sorted queries agreeing. This ordering and the
// active/retired determination are verified identical across sqlite,
// postgres, and mysql against the same 8-pool fixture: see
// TestNodeAdapterPoolsListOrderingAndActiveSet (pools_list_test.go) and its
// direct-store postgres/mysql counterparts
// (database/plugin/metadata/{postgres,mysql}/pool_active_ordered_test.go).
//
// Query cost: the store query ranks every pool registration and retirement
// certificate ever recorded, because the order is derived from a per-pool
// window ranking over the full history, so it cannot be paged in SQL. The
// result is one row per active pool (~3,000 on mainnet) and depends only on
// the chain state, so it is cached per tip point (activePoolCache): requests
// at one tip share one computation, and the page is sliced from the cached
// list in memory. The tip is the one stored in the snapshot the list is read
// from.
func (a *NodeAdapter) PoolsList(ctx context.Context,
	params PaginationParams,
) ([]string, int, error) {
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	// The cache key is the tip stored in the same read snapshot the list is
	// computed from, so a list is filed under the state it was read at. The
	// in-memory ledger tip cannot serve as the key: a block applied and then
	// rolled back between reading it and reading the snapshot restores the
	// same slot and hash, and the list of the discarded fork would be filed
	// under the restored tip.
	tip, err := db.GetTip(txn)
	if err != nil {
		return nil, 0, fmt.Errorf("get tip: %w", err)
	}
	poolKeyHashes, err := a.activePools.get(
		ctx,
		activePoolCacheKey{slot: tip.Point.Slot, hash: string(tip.Point.Hash)},
		func() ([][]byte, error) {
			return db.GetActivePoolKeyHashesOrdered(ctx, txn)
		},
	)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"get active pool key hashes ordered: %w",
			err,
		)
	}

	if params.Order == PaginationOrderDesc {
		slices.Reverse(poolKeyHashes)
	}
	total := len(poolKeyHashes)

	start := (params.Page - 1) * params.Count
	if start >= total {
		return []string{}, total, nil
	}
	end := min(start+params.Count, total)

	ret := make([]string, 0, end-start)
	for _, pkh := range poolKeyHashes[start:end] {
		poolKeyHash, err := lcommon.NewBlake2b224Checked(pkh)
		if err != nil {
			return nil, 0, fmt.Errorf("active pool key hash: %w", err)
		}
		ret = append(ret, lcommon.PoolId(poolKeyHash).String())
	}
	return ret, total, nil
}
