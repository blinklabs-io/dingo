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
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// seedBlockAtSlot writes a minimal block index entry for slot/hash, enough
// for database.BlockBySlot to find it -- Query's verifyPointOnChain doesn't
// decode the block, only compares Hash, so no real CBOR content is needed.
func seedBlockAtSlot(t *testing.T, ls *LedgerState, slot uint64, hash []byte) {
	t.Helper()
	require.NoError(t, ls.db.BlockCreate(models.Block{
		ID:   slot,
		Slot: slot,
		Hash: hash,
	}, nil))
}

// TestQuery_PinnedPointOnChain_Succeeds covers the common case: a pinned
// point naming a block this node's current chain actually has at that slot
// must be accepted, dispatching through to the query as normal
// (blinklabs-io/dingo#382, #5 in the follow-up review).
func TestQuery_PinnedPointOnChain_Succeeds(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	hash := bytes.Repeat([]byte{0xAB}, 32)
	seedBlockAtSlot(t, ls, 100, hash)

	_, err := ls.Query(utxoWholeQuery(), QueryPoint{Slot: 100, Hash: hash})
	require.NoError(t, err)
}

// TestQuery_PinnedPointWrongHash_Rejected covers a rollback/fork scenario:
// the caller acquired a point at slot 100 naming hash A, but this node's
// current chain now has a different block (hash B) at slot 100 -- e.g. a
// rollback happened between Acquire and Query. Before verifyPointOnChain
// existed, a purely slot-keyed reconstruction would have silently answered
// against the new fork's data; it must instead fail with ErrPointNotOnChain.
func TestQuery_PinnedPointWrongHash_Rejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	actualHash := bytes.Repeat([]byte{0xAB}, 32)
	acquiredHash := bytes.Repeat([]byte{0xCD}, 32)
	seedBlockAtSlot(t, ls, 100, actualHash)

	_, err := ls.Query(
		utxoWholeQuery(),
		QueryPoint{Slot: 100, Hash: acquiredHash},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPointNotOnChain)
}

// TestQuery_PinnedPointNoBlockAtSlot_Rejected covers a point naming a slot
// this node has no block for at all (never seen it, or it was never a real
// chain point) -- must fail with ErrPointNotOnChain rather than silently
// treating "no block" as equivalent to "empty state as of that slot".
func TestQuery_PinnedPointNoBlockAtSlot_Rejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	_, err := ls.Query(
		utxoWholeQuery(),
		QueryPoint{Slot: 999, Hash: bytes.Repeat([]byte{0xEE}, 32)},
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrPointNotOnChain))
}

// TestQuery_UnpinnedSkipsPointValidation covers the live path: a zero-value
// QueryPoint must not trigger verifyPointOnChain at all (no block need
// exist at slot 0), preserving every existing unpinned query's behavior.
func TestQuery_UnpinnedSkipsPointValidation(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	_, err := ls.Query(utxoWholeQuery(), QueryPoint{})
	require.NoError(t, err)
}
