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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package blockfrost

import (
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
// Query cost: the result is one row per active pool (~3,000 on mainnet),
// the same result-set shape PoolsExtended already reads via
// GetActivePoolKeyHashes/GetActivePoolKeyHashesAtSlot; no per-page query is
// added. The underlying scan cost is bounded by the total historical
// pool_registration row count instead (verified via EXPLAIN on all three
// backends, see DATABASE.md), since the added_slot filter can't use the
// (pool_id, added_slot) index on any backend -- the same bound
// GetActivePoolKeyHashesAtSlot already pays for PoolsExtended. Pagination
// is applied in memory (slice), matching PoolsRetiring (GetRetiringPools +
// PoolsRetiring) rather than pushing LIMIT/OFFSET into SQL: the ORDER BY
// here is derived from a per-pool window-function ranking computed across
// the full registration/retirement history, so the whole active set must
// be ranked before any page boundary can be determined -- a SQL-side
// LIMIT/OFFSET would trim rows only after that ranking work is done, and
// would not reduce it. The response is bare pool ID strings, so slicing
// the resulting hash slice in memory before conversion is cheap relative
// to that query.
func (a *NodeAdapter) PoolsList(
	params PaginationParams,
) ([]string, int, error) {
	db := a.ledgerState.Database()
	txn := db.Transaction(false)
	defer txn.Release()

	poolKeyHashes, err := db.Metadata().GetActivePoolKeyHashesOrdered(
		txn.Metadata(),
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
		poolID := lcommon.PoolId(lcommon.NewBlake2b224(pkh))
		ret = append(ret, poolID.String())
	}
	return ret, total, nil
}
