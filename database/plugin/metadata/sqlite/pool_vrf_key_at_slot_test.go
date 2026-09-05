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

package sqlite

import (
	"bytes"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedPoolRegistration inserts one pool_registration row for the pool,
// creating the pool row on first use.
func seedPoolRegistration(
	t *testing.T,
	raw *sql.DB,
	poolKeyHash, vrfKeyHash []byte,
	addedSlot uint64,
) {
	t.Helper()
	var poolID int64
	err := raw.QueryRow(
		`SELECT id FROM pool WHERE pool_key_hash = ?`, poolKeyHash,
	).Scan(&poolID)
	if err != nil {
		res, ierr := raw.Exec(
			`INSERT INTO pool (pool_key_hash, vrf_key_hash) VALUES (?, ?)`,
			poolKeyHash, vrfKeyHash,
		)
		require.NoError(t, ierr)
		poolID, ierr = res.LastInsertId()
		require.NoError(t, ierr)
	} else {
		_, uerr := raw.Exec(
			`UPDATE pool SET vrf_key_hash = ? WHERE id = ?`,
			vrfKeyHash, poolID,
		)
		require.NoError(t, uerr)
	}
	_, err = raw.Exec(`
INSERT INTO pool_registration (pool_id, pool_key_hash, vrf_key_hash, added_slot)
VALUES (?, ?, ?, ?)`,
		poolID, poolKeyHash, vrfKeyHash, addedSlot,
	)
	require.NoError(t, err)
}

// TestGetPoolVrfKeyHashAtSlotFollowsRotation is the dingo #3842 regression,
// built from the rotation that wedged a Preview replay at epoch 38.
//
// The pool ran on one VRF key from slot 1014930, rotated to a second at slot
// 3279920, and produced a block at slot 3362555. The snapshot that elected that
// block was captured at slot 3196799 — before the rotation — so the header
// legitimately carries the old key. Reading the pool's current registration
// yields the new key and rejects a canonical block.
func TestGetPoolVrfKeyHashAtSlotFollowsRotation(t *testing.T) {
	t.Parallel()
	store, raw := newSharedSQLStore(t)

	pool := bytes.Repeat([]byte{0x11}, 28)
	oldKey := bytes.Repeat([]byte{0xB5}, 32)
	newKey := bytes.Repeat([]byte{0xFA}, 32)

	seedPoolRegistration(t, raw, pool, oldKey, 1_014_930)
	seedPoolRegistration(t, raw, pool, oldKey, 2_479_516)
	seedPoolRegistration(t, raw, pool, newKey, 3_279_920)

	// At the electing snapshot's capture, the old key was in force.
	got, ok, err := store.GetPoolVrfKeyHashAtSlot(pool, 3_196_799, nil)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, oldKey, got,
		"a rotation after the capture must not change the electing key")

	// At and after the rotation, the new key is in force.
	got, ok, err = store.GetPoolVrfKeyHashAtSlot(pool, 3_279_920, nil)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, newKey, got)

	got, ok, err = store.GetPoolVrfKeyHashAtSlot(pool, 3_362_555, nil)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, newKey, got)
}

// TestGetPoolVrfKeyHashAtSlotBeforeFirstRegistration pins the absent case:
// before the pool ever registered there is no key, which is a different answer
// from a registration carrying no key.
func TestGetPoolVrfKeyHashAtSlotBeforeFirstRegistration(t *testing.T) {
	t.Parallel()
	store, raw := newSharedSQLStore(t)

	pool := bytes.Repeat([]byte{0x22}, 28)
	seedPoolRegistration(t, raw, pool, bytes.Repeat([]byte{0xAA}, 32), 5_000)

	got, ok, err := store.GetPoolVrfKeyHashAtSlot(pool, 4_999, nil)
	require.NoError(t, err)
	assert.False(t, ok, "no registration exists at or before this slot")
	assert.Nil(t, got)

	// An unknown pool is likewise absent rather than an error.
	got, ok, err = store.GetPoolVrfKeyHashAtSlot(
		bytes.Repeat([]byte{0x33}, 28), 10_000, nil,
	)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, got)
}

// Two registrations for one pool at the same slot are not representable:
// pool_registration is UNIQUE on (pool_id, added_slot). The query still orders
// by block and certificate index after added_slot so it cannot disagree with
// GetActivePoolKeyHashesAtSlot, but that tie-break is unreachable here and so
// is not covered by a test that would have to violate the constraint to exist.
