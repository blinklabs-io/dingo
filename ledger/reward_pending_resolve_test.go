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
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/stretchr/testify/require"
)

// A pending epoch is rebuilt from the metadata store's history before the
// move into the live UTxO set deletes it, and its marker is cleared in the
// store, so nothing later needs history the set does not keep.
func TestResolvePendingRewardStakeInputsRebuildsFromMetadataHistory(
	t *testing.T,
) {
	t.Parallel()

	ls, db, snapshotEpoch, _ := captureOwnerWindowRewardInputs(t)
	meta := db.Metadata()
	captured, err := meta.GetRewardStakeInputs(snapshotEpoch, nil)
	require.NoError(t, err)
	require.NotEmpty(t, captured)
	snap, err := meta.GetRewardSnapshot(snapshotEpoch, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, snap)
	// The deferred write stopped after a single chunk.
	require.NoError(t, meta.DeleteRewardStakeInputBeforeEpoch(
		context.Background(), snapshotEpoch+1, nil,
	))
	require.NoError(t, meta.SaveRewardStakeInputs(captured[:1], nil))
	require.NoError(t, markRewardStakeInputsPending(
		dbadapter.New(db), nil, snapshotEpoch, snap.BoundarySlot,
	))

	require.NoError(t, ls.ResolvePendingRewardStakeInputs(
		t.Context(), dbadapter.New(db),
	))

	rows, err := meta.GetRewardStakeInputs(snapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, rows, len(captured))
	incomplete, err := ls.rewardStakeInputsIncomplete(
		nil, snapshotEpoch, snapshotEpoch+1,
	)
	require.NoError(t, err)
	require.False(t, incomplete)
}
