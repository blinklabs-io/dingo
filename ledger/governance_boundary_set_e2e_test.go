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

package ledger_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// A Conway boundary writes its snapshot to the live UTxO set, and a local
// state query acquired at a point of the epoch it opens reads that
// snapshot, with the RATIFY outcome the boundary reached. A point of the
// previous epoch, whose boundary the set never saw, reads as not retained
// rather than as another epoch's snapshot.
func TestGovernanceBoundarySnapshotServedFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	ls, set, boundary, proposal := ledger.GovernanceBoundaryOverSetForTest(t)

	stored, ok, err := set.GovernanceBoundarySnapshotAt(742, nil)
	require.NoError(t, err)
	require.True(t, ok, "the boundary wrote no row to the set")
	require.Equal(t, boundary, stored.BoundarySlot)

	got, err := ls.GovernanceBoundarySnapshotAtSlot(boundary + 5)
	require.NoError(t, err)
	require.Equal(t, uint64(742), got.Epoch)
	require.Equal(t, boundary, got.BoundarySlot)
	require.Equal(t, []models.GovernanceActionRef{{
		TxHash: proposal.TxHash, ActionIndex: proposal.ActionIndex,
	}}, got.Ratified)

	_, err = ls.GovernanceBoundarySnapshotAtSlot(boundary - 1)
	require.ErrorIs(t, err, models.ErrGovernanceBoundarySnapshotNotRetained)
}
