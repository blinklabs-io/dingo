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

package leader

import (
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/types"
)

type mockSyncStateStore struct {
	values map[string]string
	getErr error
	setErr error
}

func newMockSyncStateStore() *mockSyncStateStore {
	return &mockSyncStateStore{
		values: make(map[string]string),
	}
}

func (m *mockSyncStateStore) GetSyncState(
	key string,
	txn types.Txn,
) (string, error) {
	if m.getErr != nil {
		return "", m.getErr
	}
	return m.values[key], nil
}

func (m *mockSyncStateStore) SetSyncState(
	key string,
	value string,
	txn types.Txn,
) error {
	if m.setErr != nil {
		return m.setErr
	}
	m.values[key] = value
	return nil
}

func TestSyncStateScheduleStoreRoundTrip(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	copy(poolId[:], []byte("testpool1234567890123"))

	backingStore := newMockSyncStateStore()
	store := NewSyncStateScheduleStore(backingStore)

	schedule := NewSchedule(42, poolId, 1000, 10000, electionTestNonce)
	schedule.AddLeaderSlot(420)
	schedule.AddLeaderSlot(424)

	err := store.SaveSchedule(schedule)
	require.NoError(t, err)

	loaded, err := store.LoadSchedule(42, poolId)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	assert.Equal(t, schedule.Epoch, loaded.Epoch)
	assert.Equal(t, schedule.PoolId, loaded.PoolId)
	assert.Equal(t, schedule.PoolStake, loaded.PoolStake)
	assert.Equal(t, schedule.TotalStake, loaded.TotalStake)
	assert.Equal(t, schedule.EpochNonce, loaded.EpochNonce)
	assert.Equal(
		t,
		schedule.LeaderSlotsSnapshot(),
		loaded.LeaderSlotsSnapshot(),
	)
}
