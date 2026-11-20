// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlstore

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

type rollbackQueryRecorder struct {
	queryer
	query string
}

func (q *rollbackQueryRecorder) QueryContext(
	ctx context.Context,
	query string,
	args ...any,
) (*sql.Rows, error) {
	q.query = query
	return q.queryer.QueryContext(ctx, query, args...)
}

func TestRollbackOpCertLookupUsesSlotRange(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	poolA := make([]byte, 28)
	poolA[0] = 0xAA
	poolB := make([]byte, 28)
	poolB[0] = 0xBB
	seedPoolOpCertSequence(t, store, poolA, 2_000)
	seedPoolOpCertSequence(t, store, poolB, 2_000)

	const rollbackSlot = int64(39_940)
	db := &rollbackQueryRecorder{queryer: store.writeDB}
	hashes, err := queryPoolKeyHashesWithOpCertAfterSlot(
		context.Background(),
		db,
		uint64(rollbackSlot),
	)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]byte{poolA, poolB}, hashes)
	require.Len(t, hashes, 2, "each affected pool must be returned once")

	plan := queryPlan(t, store.writeDB, db.query, rollbackSlot)
	require.Contains(
		t,
		plan,
		"SEARCH pool_opcert_sequence USING INDEX "+
			"idx_pool_opcert_sequence_slot (slot>?)",
		"depth-1 rollback must range over the discarded slot window: %s",
		plan,
	)
	require.NotContains(
		t,
		plan,
		"SCAN pool_opcert_sequence",
		"rollback must not scan retained op-cert history: %s",
		plan,
	)
}
