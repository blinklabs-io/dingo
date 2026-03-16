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

package dingo

import (
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/leader"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

const leaderScheduleSyncStatePrefix = "leader_schedule"

type leaderScheduleRecord struct {
	Epoch       uint64   `json:"epoch"`
	PoolID      string   `json:"pool_id"`
	PoolStake   uint64   `json:"pool_stake"`
	TotalStake  uint64   `json:"total_stake"`
	EpochNonce  string   `json:"epoch_nonce"`
	LeaderSlots []uint64 `json:"leader_slots"`
}

type leaderScheduleStore struct {
	db *database.Database
}

func newLeaderScheduleStore(db *database.Database) *leaderScheduleStore {
	if db == nil {
		return nil
	}
	return &leaderScheduleStore{db: db}
}

func (s *leaderScheduleStore) LoadSchedule(
	epoch uint64,
	poolId lcommon.PoolKeyHash,
) (*leader.Schedule, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	key := leaderScheduleStoreKey(epoch, poolId)
	raw, err := s.db.GetSyncState(key, nil)
	if err != nil {
		return nil, fmt.Errorf("load schedule %q: %w", key, err)
	}
	if raw == "" {
		return nil, nil
	}

	var record leaderScheduleRecord
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
	schedule := leader.NewSchedule(
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

func (s *leaderScheduleStore) SaveSchedule(
	schedule *leader.Schedule,
) error {
	if s == nil || s.db == nil || schedule == nil {
		return nil
	}
	record := leaderScheduleRecord{
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
	key := leaderScheduleStoreKey(schedule.Epoch, schedule.PoolId)
	if err := s.db.SetSyncState(key, string(payload), nil); err != nil {
		return fmt.Errorf("save schedule %q: %w", key, err)
	}
	return nil
}

func leaderScheduleStoreKey(
	epoch uint64,
	poolId lcommon.PoolKeyHash,
) string {
	return fmt.Sprintf(
		"%s:%s:%d",
		leaderScheduleSyncStatePrefix,
		hex.EncodeToString(poolId[:]),
		epoch,
	)
}
