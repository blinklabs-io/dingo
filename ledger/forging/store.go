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

package forging

import (
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

const (
	syncStateForgeFencePrefix = "forge_fence"

	// forgeFenceFormatVersion guards the on-disk record shape. A record
	// written by a build with an incompatible layout must fail loudly
	// rather than decode into a weaker fence.
	forgeFenceFormatVersion = 1
)

type syncStateStore interface {
	GetSyncState(key string, txn types.Txn) (string, error)
	SetSyncState(key, value string, txn types.Txn) error
}

type syncStateForgeFenceRecord struct {
	FormatVersion  int    `json:"format_version"`
	PoolID         string `json:"pool_id"`
	LastForgedSlot uint64 `json:"last_forged_slot"`
}

// syncStateForgeFenceStore persists the last-forged-slot fence in
// metadata sync state, alongside the leader schedule.
type syncStateForgeFenceStore struct {
	store  syncStateStore
	poolID lcommon.PoolKeyHash
	key    string
}

// NewSyncStateForgeFenceStore creates a ForgeFenceStore backed by sync
// state. The fence is namespaced by pool id so a node re-keyed to
// different credentials is not gated by a fence it never signed under.
func NewSyncStateForgeFenceStore(
	store syncStateStore,
	poolID lcommon.PoolKeyHash,
) ForgeFenceStore {
	if store == nil {
		return nil
	}
	return &syncStateForgeFenceStore{
		store:  store,
		poolID: poolID,
		key:    syncStateForgeFenceKey(poolID),
	}
}

// LoadLastForgedSlot returns the persisted fence, if any. An unreadable
// or foreign record is an error rather than "no fence": reporting no
// fence would let the forger sign a slot it may already have used.
func (s *syncStateForgeFenceStore) LoadLastForgedSlot() (
	uint64,
	bool,
	error,
) {
	raw, err := s.store.GetSyncState(s.key, nil)
	if err != nil {
		return 0, false, fmt.Errorf("load forge fence %q: %w", s.key, err)
	}
	if raw == "" {
		return 0, false, nil
	}
	var record syncStateForgeFenceRecord
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return 0, false, fmt.Errorf(
			"decode forge fence %q: %w",
			s.key,
			err,
		)
	}
	if record.FormatVersion != forgeFenceFormatVersion {
		return 0, false, fmt.Errorf(
			"forge fence %q format version mismatch: got %d want %d",
			s.key,
			record.FormatVersion,
			forgeFenceFormatVersion,
		)
	}
	expectedPoolID := hex.EncodeToString(s.poolID[:])
	if record.PoolID != expectedPoolID {
		return 0, false, fmt.Errorf(
			"forge fence %q pool mismatch: got %s want %s",
			s.key,
			record.PoolID,
			expectedPoolID,
		)
	}
	return record.LastForgedSlot, true, nil
}

// StoreLastForgedSlot records slot as used. The fence only ever moves
// forward: a lower slot leaves the stronger recorded value in place.
func (s *syncStateForgeFenceStore) StoreLastForgedSlot(slot uint64) error {
	current, ok, err := s.LoadLastForgedSlot()
	if err != nil {
		return err
	}
	if ok && current >= slot {
		return nil
	}
	payload, err := json.Marshal(syncStateForgeFenceRecord{
		FormatVersion:  forgeFenceFormatVersion,
		PoolID:         hex.EncodeToString(s.poolID[:]),
		LastForgedSlot: slot,
	})
	if err != nil {
		return fmt.Errorf("encode forge fence for slot %d: %w", slot, err)
	}
	if err := s.store.SetSyncState(s.key, string(payload), nil); err != nil {
		return fmt.Errorf("save forge fence %q: %w", s.key, err)
	}
	return nil
}

func syncStateForgeFenceKey(poolID lcommon.PoolKeyHash) string {
	return fmt.Sprintf(
		"%s:%s",
		syncStateForgeFencePrefix,
		hex.EncodeToString(poolID[:]),
	)
}
