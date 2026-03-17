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
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

const syncStateSchedulePrefix = "leader_schedule"

type syncStateStore interface {
	GetSyncState(key string, txn types.Txn) (string, error)
	SetSyncState(key, value string, txn types.Txn) error
}

type syncStateScheduleRecord struct {
	Epoch       uint64   `json:"epoch"`
	PoolID      string   `json:"pool_id"`
	PoolStake   uint64   `json:"pool_stake"`
	TotalStake  uint64   `json:"total_stake"`
	EpochNonce  string   `json:"epoch_nonce"`
	LeaderSlots []uint64 `json:"leader_slots"`
}

// syncStateScheduleStore persists schedules in metadata sync state.
type syncStateScheduleStore struct {
	store syncStateStore
}

// NewSyncStateScheduleStore creates a ScheduleStore backed by sync state.
func NewSyncStateScheduleStore(store syncStateStore) ScheduleStore {
	if store == nil {
		return nil
	}
	return &syncStateScheduleStore{store: store}
}

// LoadSchedule returns a previously persisted leader schedule, if present.
func (s *syncStateScheduleStore) LoadSchedule(
	epoch uint64,
	poolId lcommon.PoolKeyHash,
) (*Schedule, error) {
	if s == nil || s.store == nil {
		return nil, nil
	}
	key := syncStateScheduleKey(epoch, poolId)
	raw, err := s.store.GetSyncState(key, nil)
	if err != nil {
		return nil, fmt.Errorf("load schedule %q: %w", key, err)
	}
	if raw == "" {
		return nil, nil
	}

	var record syncStateScheduleRecord
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return nil, fmt.Errorf("decode schedule %q: %w", key, err)
	}
	if record.Epoch != epoch {
		return nil, fmt.Errorf(
			"schedule %q epoch mismatch: got %d want %d",
			key,
			record.Epoch,
			epoch,
		)
	}
	expectedPoolID := hex.EncodeToString(poolId[:])
	if record.PoolID != expectedPoolID {
		return nil, fmt.Errorf(
			"schedule %q pool mismatch: got %s want %s",
			key,
			record.PoolID,
			expectedPoolID,
		)
	}
	epochNonce, err := hex.DecodeString(record.EpochNonce)
	if err != nil {
		return nil, fmt.Errorf("decode epoch nonce for %q: %w", key, err)
	}
	schedule := NewSchedule(
		record.Epoch,
		poolId,
		record.PoolStake,
		record.TotalStake,
		epochNonce,
	)
	for _, slot := range record.LeaderSlots {
		schedule.AddLeaderSlot(slot)
	}
	return schedule, nil
}

// SaveSchedule persists a computed leader schedule.
func (s *syncStateScheduleStore) SaveSchedule(schedule *Schedule) error {
	if s == nil || s.store == nil || schedule == nil {
		return nil
	}
	record := syncStateScheduleRecord{
		Epoch:       schedule.Epoch,
		PoolID:      hex.EncodeToString(schedule.PoolId[:]),
		PoolStake:   schedule.PoolStake,
		TotalStake:  schedule.TotalStake,
		EpochNonce:  hex.EncodeToString(schedule.EpochNonce),
		LeaderSlots: schedule.LeaderSlotsSnapshot(),
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf(
			"encode leader schedule for epoch %d: %w",
			schedule.Epoch,
			err,
		)
	}
	key := syncStateScheduleKey(schedule.Epoch, schedule.PoolId)
	if err := s.store.SetSyncState(key, string(payload), nil); err != nil {
		return fmt.Errorf("save schedule %q: %w", key, err)
	}
	return nil
}

func syncStateScheduleKey(
	epoch uint64,
	poolId lcommon.PoolKeyHash,
) string {
	return fmt.Sprintf(
		"%s:%s:%d",
		syncStateSchedulePrefix,
		hex.EncodeToString(poolId[:]),
		epoch,
	)
}
