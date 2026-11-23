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

package utxoset_test

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestReconcileNeverRebuildsASetServingTheLedger requires a set that cannot be
// resumed to stop with an error naming the journal it lacks a base in, and
// never to rebuild it from the store.
func TestReconcileNeverRebuildsASetServingTheLedger(t *testing.T) {
	t.Parallel()
	const applied = 4
	for _, tc := range []struct {
		name      string
		ledgerTip ocommon.Point
		resumes   bool
		wantErr   bool
		wantCount uint64
	}{
		{"behind, not resumable", slotPoint(6), false, true, 1},
		{"behind, resumable", slotPoint(6), true, false, applied},
		{"ahead of the journal", slotPoint(2), false, true, applied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			set := openRecoverySet(t, utxoset.Config{})
			require.NoError(t, set.StartEmptyForTest())
			applySlots(t, set, applied)
			if tc.ledgerTip.Slot < applied {
				require.NoError(t, set.PruneHistory(applied))
			}
			db := ledgerAt(t, tc.ledgerTip)
			resume := func(context.Context, ocommon.Point) (bool, error) {
				return tc.resumes, nil
			}
			err := set.Reconcile(t.Context(), dbadapter.New(db), resume)
			if tc.wantErr {
				require.ErrorContains(t, err, "rollback journal reaches back only to")
				require.NotContains(t, err.Error(), "resync")
			} else {
				require.NoError(t, err)
			}
			n, err := set.Count()
			require.NoError(t, err)
			require.Equal(t, tc.wantCount, n)
		})
	}
}

// TestReconcileRefusesToRebuildAfterAFailedRewindOfASetServingTheLedger requires a
// failure mark the journal cannot rewind to stop the node with an error naming
// the missing state rather than rebuild the set from the metadata store.
func TestReconcileRefusesToRebuildAfterAFailedRewindOfASetServingTheLedger(
	t *testing.T,
) {
	t.Parallel()
	{
		set := openRecoverySet(t, utxoset.Config{})
		require.NoError(t, set.StartEmptyForTest())
		applySlots(t, set, 6)
		require.NoError(t, set.PruneHistory(5))
		require.NoError(t, set.MarkUtxoSetFailed(2))
		db := ledgerAt(t, slotPoint(6))
		err := set.Reconcile(t.Context(), dbadapter.New(db), neverResume)
		n, countErr := set.Count()
		require.NoError(t, countErr)
		require.ErrorContains(t, err, "no copy of the ledger state")
		require.NotContains(t, err.Error(), "resync")
		_, marked, err := set.FailedAt()
		require.NoError(t, err)
		require.True(t, marked, "the failure mark must stay")
		require.Equal(t, uint64(6), n, "the set must not be rebuilt")
	}
}
