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
	"bytes"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// committedUtxoSet is a live UTxO set holding only committed outputs.
type committedUtxoSet map[string][]byte

func (s committedUtxoSet) ApplyUtxoChanges(*ocommon.Point, []UtxoEvent) error {
	return nil
}

func (s committedUtxoSet) Sum(key []byte) (uint64, error) {
	var n uint64
	for _, c := range s {
		if k, l, ok := UtxoStakeOf(c); ok && bytes.Equal(k, key) {
			n += l
		}
	}
	return n, nil
}

func (s committedUtxoSet) Get(txID []byte, index uint32) ([]byte, bool, error) {
	c, ok := s[string(txID)+string(rune(index))]
	return c, ok, nil
}

// TestUtxoSetReadsSeeTheirTransactionsStagedChanges stages changes in an open
// write transaction, as a multi-block commit does before validating its next
// block, and requires reads in that transaction to see them over the set's
// committed state, and reads elsewhere and after a rollback not to.
func TestUtxoSetReadsSeeTheirTransactionsStagedChanges(t *testing.T) {
	t.Parallel()
	spentID := bytes.Repeat([]byte{1}, 32)
	newID := bytes.Repeat([]byte{2}, 32)
	set := committedUtxoSet{string(spentID) + string(rune(0)): {0x80}}
	ls := &LedgerState{
		db: newTestDB(t),
		config: LedgerStateConfig{
			UtxoSet:      set,
			UtxoSetReads: UtxoSetReadsSet,
			Logger:       testLogger(),
		},
	}
	txn := ls.ledgerDB().Begin(t.Context(), true)
	ls.stageUtxoSet(txn, nil, UtxoEvent{
		Removed: []models.UtxoId{{Hash: spentID}},
		Added:   []UtxoOutput{{Ref: models.UtxoId{Hash: newID}, Cbor: []byte{0x81}}},
	})

	_, ok, err := ls.utxoSetGet(set, txn, spentID, 0)
	require.NoError(t, err)
	require.False(t, ok, "spent in this transaction")
	c, ok, err := ls.utxoSetGet(set, txn, newID, 0)
	require.NoError(t, err)
	require.True(t, ok, "created in this transaction")
	require.Equal(t, []byte{0x81}, c)

	_, ok, err = ls.utxoSetGet(set, nil, spentID, 0)
	require.NoError(t, err)
	require.True(t, ok, "outside the transaction the spend is not visible")

	require.NoError(t, txn.Rollback())
	_, ok, err = ls.utxoSetGet(set, txn, newID, 0)
	require.NoError(t, err)
	require.False(t, ok, "a rolled-back transaction's changes are gone")
	ls.utxoSetMu.Lock()
	require.Empty(t, ls.utxoSetBatches)
	ls.utxoSetMu.Unlock()
}

func stakedOutput(t *testing.T, stake byte, lovelace uint64) []byte {
	t.Helper()
	addr, err := lcommon.NewAddressFromBytes(rowBatchingBaseAddress(t, 0x11, stake))
	require.NoError(t, err)
	raw, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
		OutputAddress: addr,
		OutputAmount:  lovelace,
	})
	require.NoError(t, err)
	return raw
}

// TestUtxoStakeSeenByIncludesTheTransactionsStagedChanges spends a committed
// output, creates two to the same credential and spends one of them again in
// a later change of the same transaction, and requires the credential's stake
// as that transaction sees it to net all four, while other readers and the
// transaction after a rollback see only the committed sum.
func TestUtxoStakeSeenByIncludesTheTransactionsStagedChanges(t *testing.T) {
	t.Parallel()
	spent := bytes.Repeat([]byte{1}, 32)
	created := bytes.Repeat([]byte{2}, 32)
	set := committedUtxoSet{string(spent) + string(rune(0)): stakedOutput(t, 0x22, 5_000_000)}
	key := CredentialStakeKey(0, rowBatchingKey(0x22))
	ls := &LedgerState{
		db:     newTestDB(t),
		config: LedgerStateConfig{UtxoSet: set, Logger: testLogger()},
	}
	txn := ls.ledgerDB().Begin(t.Context(), true)
	ls.stageUtxoSet(txn, nil,
		UtxoEvent{
			Removed: []models.UtxoId{{Hash: spent}},
			Added: []UtxoOutput{
				{Ref: models.UtxoId{Hash: created, Idx: 0}, Cbor: stakedOutput(t, 0x22, 3_000_000)},
				{Ref: models.UtxoId{Hash: created, Idx: 1}, Cbor: stakedOutput(t, 0x22, 1_000_000)},
			},
		},
		UtxoEvent{Removed: []models.UtxoId{{Hash: created, Idx: 1}}},
	)
	got, err := ls.utxoStakeSeenBy(set, txn, key)
	require.NoError(t, err)
	require.Equal(t, uint64(3_000_000), got)
	got, err = ls.utxoStakeSeenBy(set, nil, key)
	require.NoError(t, err)
	require.Equal(t, uint64(5_000_000), got)
	require.NoError(t, txn.Rollback())
	got, err = ls.utxoStakeSeenBy(set, txn, key)
	require.NoError(t, err)
	require.Equal(t, uint64(5_000_000), got)
}

// applyingUtxoSet is a live UTxO set that applies each commit's changes.
type applyingUtxoSet struct {
	mu      sync.Mutex
	outputs committedUtxoSet
}

func (s *applyingUtxoSet) ApplyUtxoChanges(_ *ocommon.Point, changes []UtxoEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range changes {
		for _, ref := range c.Removed {
			delete(s.outputs, string(ref.Hash)+string(rune(ref.Idx)))
		}
		for _, o := range c.Added {
			s.outputs[string(o.Ref.Hash)+string(rune(o.Ref.Idx))] = o.Cbor
		}
	}
	return nil
}

func (s *applyingUtxoSet) Sum(key []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.outputs.Sum(key)
}

func (s *applyingUtxoSet) Get(txID []byte, index uint32) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.outputs.Get(txID, index)
}

func (s *applyingUtxoSet) EachSum(fn func(key []byte, lovelace uint64) error) error {
	s.mu.Lock()
	sums := make(map[string]uint64)
	for _, c := range s.outputs {
		if k, l, ok := UtxoStakeOf(c); ok {
			sums[string(k)] += l
		}
	}
	s.mu.Unlock()
	for k, n := range sums {
		if err := fn([]byte(k), n); err != nil {
			return err
		}
	}
	return nil
}

// TestUtxoSetReadsSeeACommittedBatchBeforeItIsApplied reads the set from
// another transaction after a write transaction commits and before its batch
// reaches the set, as a write that starts in that gap does, and requires the
// committed change: a stake re-sum that missed it would store the
// credential's stake from before the commit until its next re-sum.
func TestUtxoSetReadsSeeACommittedBatchBeforeItIsApplied(t *testing.T) {
	t.Parallel()
	spent := bytes.Repeat([]byte{1}, 32)
	created := bytes.Repeat([]byte{2}, 32)
	set := &applyingUtxoSet{outputs: committedUtxoSet{
		string(spent) + string(rune(0)): stakedOutput(t, 0x22, 5_000_000),
	}}
	key := CredentialStakeKey(0, rowBatchingKey(0x22))
	ls := &LedgerState{
		db: newTestDB(t),
		config: LedgerStateConfig{
			UtxoSet:      set,
			UtxoSetReads: UtxoSetReadsSet,
			Logger:       testLogger(),
		},
	}
	type seen struct {
		stake, each        uint64
		spentLive, newLive bool
	}
	read := func() seen {
		var s seen
		var err error
		s.stake, err = ls.utxoStakeSeenBy(set, nil, key)
		require.NoError(t, err)
		require.NoError(t, ls.EachLiveUtxoStake(nil,
			func(tag uint8, hash []byte, n uint64) error {
				if bytes.Equal(CredentialStakeKey(tag, hash), key) {
					s.each = n
				}
				return nil
			}))
		_, s.spentLive, err = ls.utxoSetGet(set, nil, spent, 0)
		require.NoError(t, err)
		_, s.newLive, err = ls.utxoSetGet(set, nil, created, 0)
		require.NoError(t, err)
		return s
	}
	want := seen{stake: 3_000_000, each: 3_000_000, newLive: true}

	txn := ls.ledgerDB().Begin(t.Context(), true)
	var inGap seen
	// Registered before the batch's own callback, so it runs after the
	// commit and before the batch is applied.
	txn.AfterCommit(func() { inGap = read() })
	ls.stageUtxoSet(txn, nil, UtxoEvent{
		Removed: []models.UtxoId{{Hash: spent}},
		Added: []UtxoOutput{{
			Ref:  models.UtxoId{Hash: created},
			Cbor: stakedOutput(t, 0x22, 3_000_000),
		}},
	})
	require.Equal(t, seen{stake: 5_000_000, each: 5_000_000, spentLive: true}, read(),
		"before the commit other readers see only the committed set")
	require.NoError(t, txn.Commit())
	require.Equal(t, want, inGap, "a committed batch not yet applied")
	require.Equal(t, want, read(), "after the batch is applied, counted once")
	ls.utxoSetMu.Lock()
	require.Empty(t, ls.utxoSetBatches)
	require.Empty(t, ls.utxoSetByMeta)
	ls.utxoSetMu.Unlock()
}
