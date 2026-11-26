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
	"sync"

	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

const (
	// forgeFenceFormatVersion guards the on-disk record shape. A record
	// written by a build with an incompatible layout must fail loudly
	// rather than decode into a weaker fence.
	forgeFenceFormatVersion = 1
)

// chainStateStore is the block store's chain state. A write returns only
// once it is flushed: the fence is recorded before the header is signed, and
// a fence lost to an unclean shutdown would let that slot be signed again.
type chainStateStore interface {
	ChainStateValue(name string) (string, error)
	SetChainStateSynced(name, value string) error
}

// chainStateForgeFenceRecord is the persisted fence. LastForgedSlot is a
// pointer so an absent field is distinguishable from an explicit zero: a
// record missing it would otherwise decode to a zero fence, which permits
// every positive slot and silently disables the protection.
type chainStateForgeFenceRecord struct {
	FormatVersion  int     `json:"format_version"`
	PoolID         string  `json:"pool_id"`
	LastForgedSlot *uint64 `json:"last_forged_slot"`
}

// chainStateForgeFenceStore persists the last-forged-slot fence in the block
// store's chain state.
//
// StoreLastForgedSlot is a read-modify-write, so mu makes the
// forward-only guarantee a property of this type rather than of its
// caller. The forger drives it from a single goroutine today, but that
// is not something an exported store should depend on. mu does not span
// processes; two nodes sharing a data directory would corrupt the chain
// database long before the fence mattered.
type chainStateForgeFenceStore struct {
	mu     sync.Mutex
	store  chainStateStore
	poolID lcommon.PoolKeyHash
	key    string
}

// NewChainStateForgeFenceStore creates a ForgeFenceStore backed by the block
// store's chain state. The fence is namespaced by pool id so a node re-keyed to
// different credentials is not gated by a fence it never signed under.
func NewChainStateForgeFenceStore(
	store chainStateStore,
	poolID lcommon.PoolKeyHash,
) ForgeFenceStore {
	if store == nil {
		return nil
	}
	return &chainStateForgeFenceStore{
		store:  store,
		poolID: poolID,
		key:    chainStateForgeFenceKey(poolID),
	}
}

// LoadLastForgedSlot returns the persisted fence, if any. An unreadable
// or foreign record is an error rather than "no fence": reporting no
// fence would let the forger sign a slot it may already have used.
func (s *chainStateForgeFenceStore) LoadLastForgedSlot() (
	uint64,
	bool,
	error,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadLocked()
}

// loadLocked is LoadLastForgedSlot's body, for callers already holding
// mu.
func (s *chainStateForgeFenceStore) loadLocked() (uint64, bool, error) {
	raw, err := s.store.ChainStateValue(s.key)
	if err != nil {
		return 0, false, fmt.Errorf("load forge fence %q: %w", s.key, err)
	}
	if raw == "" {
		return 0, false, nil
	}
	var record chainStateForgeFenceRecord
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
	if record.LastForgedSlot == nil {
		return 0, false, fmt.Errorf(
			"forge fence %q missing last_forged_slot",
			s.key,
		)
	}
	return *record.LastForgedSlot, true, nil
}

// StoreLastForgedSlot records slot as used. The fence only ever moves
// forward: a lower slot leaves the stronger recorded value in place.
func (s *chainStateForgeFenceStore) StoreLastForgedSlot(slot uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok, err := s.loadLocked()
	if err != nil {
		return err
	}
	if ok && current >= slot {
		return nil
	}
	payload, err := json.Marshal(chainStateForgeFenceRecord{
		FormatVersion:  forgeFenceFormatVersion,
		PoolID:         hex.EncodeToString(s.poolID[:]),
		LastForgedSlot: &slot,
	})
	if err != nil {
		return fmt.Errorf("encode forge fence for slot %d: %w", slot, err)
	}
	if err := s.store.SetChainStateSynced(s.key, string(payload)); err != nil {
		return fmt.Errorf("save forge fence %q: %w", s.key, err)
	}
	return nil
}

func chainStateForgeFenceKey(poolID lcommon.PoolKeyHash) string {
	return types.ForgeFenceChainStatePrefix + hex.EncodeToString(poolID[:])
}
