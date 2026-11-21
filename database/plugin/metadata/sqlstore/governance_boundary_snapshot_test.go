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

package sqlstore

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

func TestGovernanceBoundarySnapshotAsOfSlotAndRollback(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	first := &models.GovernanceBoundarySnapshot{
		Epoch:        10,
		BoundarySlot: 100,
		DRepDistribution: []models.DRepStakeDistributionEntry{{
			DRepType: uint8(models.DrepTypeAlwaysAbstain),
			Stake:    40,
		}},
	}
	second := &models.GovernanceBoundarySnapshot{
		Epoch:        11,
		BoundarySlot: 200,
		Ratified: []models.GovernanceActionRef{{
			TxHash:      []byte{1, 2, 3},
			ActionIndex: 4,
		}},
	}
	require.NoError(t, store.SaveGovernanceBoundarySnapshot(first, nil))
	require.NoError(t, store.SaveGovernanceBoundarySnapshot(second, nil))

	got, err := store.GetGovernanceBoundarySnapshotAtSlot(250, nil)
	require.NoError(t, err)
	require.Equal(t, second, got)
	require.NoError(t, store.DeleteGovernanceBoundarySnapshotsAfterSlot(150, nil))
	got, err = store.GetGovernanceBoundarySnapshotAtSlot(250, nil)
	require.NoError(t, err)
	require.Equal(t, first, got)
}
