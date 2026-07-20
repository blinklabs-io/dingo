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

//go:build dingo_extra_plugins

package postgres

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

func TestSaveEpochSummaryUpsertSnapshotReadyPostgres(t *testing.T) {
	store := newTestPostgresStore(t)

	const epoch = uint64(424242)
	t.Cleanup(func() {
		_ = store.DB().Where("epoch = ?", epoch).Delete(&models.EpochSummary{}).Error
		_ = store.Close()
	})
	require.NoError(t, store.DB().Where("epoch = ?", epoch).Delete(&models.EpochSummary{}).Error)

	initial := &models.EpochSummary{
		Epoch:            epoch,
		TotalActiveStake: types.Uint64(1000),
		TotalPoolCount:   10,
		TotalDelegators:  100,
		BoundarySlot:     1,
		SnapshotReady:    false,
	}
	require.NoError(t, store.SaveEpochSummary(initial, nil))

	updated := &models.EpochSummary{
		Epoch:            epoch,
		TotalActiveStake: types.Uint64(2000),
		TotalPoolCount:   20,
		TotalDelegators:  200,
		BoundarySlot:     2,
		SnapshotReady:    true,
	}
	require.NoError(t, store.SaveEpochSummary(updated, nil))

	again := &models.EpochSummary{
		Epoch:            epoch,
		TotalActiveStake: types.Uint64(3000),
		TotalPoolCount:   30,
		TotalDelegators:  300,
		BoundarySlot:     3,
		SnapshotReady:    false,
	}
	require.NoError(t, store.SaveEpochSummary(again, nil))

	retrieved, err := store.GetEpochSummary(epoch, nil)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, types.Uint64(3000), retrieved.TotalActiveStake)
	require.True(t, retrieved.SnapshotReady)
}
