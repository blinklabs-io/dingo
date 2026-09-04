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

func seedPoolRegistrationAtSlot(
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
		res, insertErr := raw.Exec(
			`INSERT INTO pool (pool_key_hash, vrf_key_hash) VALUES (?, ?)`,
			poolKeyHash,
			vrfKeyHash,
		)
		require.NoError(t, insertErr)
		poolID, insertErr = res.LastInsertId()
		require.NoError(t, insertErr)
	} else {
		_, err = raw.Exec(
			`UPDATE pool SET vrf_key_hash = ? WHERE id = ?`,
			vrfKeyHash,
			poolID,
		)
		require.NoError(t, err)
	}
	_, err = raw.Exec(`
INSERT INTO pool_registration (pool_id, pool_key_hash, vrf_key_hash, added_slot)
VALUES (?, ?, ?, ?)`, poolID, poolKeyHash, vrfKeyHash, addedSlot)
	require.NoError(t, err)
}

func TestGetPoolVrfKeyHashAtSlotFollowsRotation(t *testing.T) {
	t.Parallel()
	store, raw := newSharedSQLStore(t)

	pool := bytes.Repeat([]byte{0x11}, 28)
	oldKey := bytes.Repeat([]byte{0xB5}, 32)
	newKey := bytes.Repeat([]byte{0xFA}, 32)
	seedPoolRegistrationAtSlot(t, raw, pool, oldKey, 1_014_930)
	seedPoolRegistrationAtSlot(t, raw, pool, oldKey, 2_479_516)
	seedPoolRegistrationAtSlot(t, raw, pool, newKey, 3_279_920)

	got, ok, err := store.GetPoolVrfKeyHashAtSlot(pool, 3_196_799, nil)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, oldKey, got)

	got, ok, err = store.GetPoolVrfKeyHashAtSlot(pool, 3_279_920, nil)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, newKey, got)
}

func TestGetPoolVrfKeyHashAtSlotDistinguishesMissingRegistration(t *testing.T) {
	t.Parallel()
	store, raw := newSharedSQLStore(t)

	pool := bytes.Repeat([]byte{0x22}, 28)
	seedPoolRegistrationAtSlot(t, raw, pool, bytes.Repeat([]byte{0xAA}, 32), 5_000)

	got, ok, err := store.GetPoolVrfKeyHashAtSlot(pool, 4_999, nil)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, got)

	got, ok, err = store.GetPoolVrfKeyHashAtSlot(
		bytes.Repeat([]byte{0x33}, 28), 10_000, nil,
	)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, got)
}
