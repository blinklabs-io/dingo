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

package sqlstore

import (
	"math"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetTipRejectsNegativeStoredSlot covers the regression this issue was
// filed for: SQLite's INTEGER columns are signed, so a tip row corrupted
// outside normal writes (SetTip already rejects out-of-range values via
// checkedInt64) can surface a negative slot. Converting that directly to
// uint64 would silently produce a near-MaxUint64 chain point instead of
// failing.
func TestGetTipRejectsNegativeStoredSlot(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	_, err := store.writeDB.Exec(
		"INSERT INTO tip (id, hash, slot, block_number) VALUES (1, ?, ?, ?)",
		[]byte{0x01},
		-1,
		5,
	)
	require.NoError(t, err)
	_, err = store.GetTip(nil)
	require.Error(t, err)
}

// TestGetTipRejectsNegativeStoredBlockNumber covers the same regression as
// TestGetTipRejectsNegativeStoredSlot for the tip's block_number column
// instead of its slot column.
func TestGetTipRejectsNegativeStoredBlockNumber(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	_, err := store.writeDB.Exec(
		"INSERT INTO tip (id, hash, slot, block_number) VALUES (1, ?, ?, ?)",
		[]byte{0x01},
		5,
		-1,
	)
	require.NoError(t, err)
	_, err = store.GetTip(nil)
	require.Error(t, err)
}

// TestGetTipRoundTripsMaxInt64Slot covers the maximum in-range value:
// checkedInt64/checkedUint64 must not reject anything that legitimately
// fits in a signed 64-bit SQL column.
func TestGetTipRoundTripsMaxInt64Slot(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	tip := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: math.MaxInt64,
			Hash: []byte{0x02},
		},
		BlockNumber: math.MaxInt64,
	}
	require.NoError(t, store.SetTip(tip, nil))
	got, err := store.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(math.MaxInt64), got.Point.Slot)
	assert.Equal(t, uint64(math.MaxInt64), got.BlockNumber)
}

// TestCheckedUint64 unit-tests the helper directly across the boundary
// cases the GetTip tests exercise indirectly through SQLite: zero, a
// typical positive value, the maximum signed 64-bit value, and negative.
func TestCheckedUint64(t *testing.T) {
	t.Parallel()
	v, err := checkedUint64(42)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), v)

	v, err = checkedUint64(0)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), v)

	v, err = checkedUint64(math.MaxInt64)
	require.NoError(t, err)
	assert.Equal(t, uint64(math.MaxInt64), v)

	_, err = checkedUint64(-1)
	require.Error(t, err)
}

// TestGetScriptRejectsOutOfRangeStoredType covers script.type: an
// unconstrained SQLite INTEGER, so a row corrupted outside normal writes
// can hold a value outside uint8's [0, 255] range. Converting that
// directly would silently wrap instead of failing.
func TestGetScriptRejectsOutOfRangeStoredType(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	hash := lcommon.NewBlake2b224([]byte{0x03})
	_, err := store.writeDB.Exec(
		"INSERT INTO script (hash, content, created_slot, type) VALUES (?, ?, ?, ?)",
		hash[:],
		[]byte{0x80},
		1,
		256,
	)
	require.NoError(t, err)

	_, err = store.GetScript(hash, nil)
	require.Error(t, err)
}

// TestGetScriptRejectsNegativeStoredType covers the same regression as
// TestGetScriptRejectsOutOfRangeStoredType for a negative script.type.
func TestGetScriptRejectsNegativeStoredType(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	hash := lcommon.NewBlake2b224([]byte{0x04})
	_, err := store.writeDB.Exec(
		"INSERT INTO script (hash, content, created_slot, type) VALUES (?, ?, ?, ?)",
		hash[:],
		[]byte{0x80},
		1,
		-1,
	)
	require.NoError(t, err)

	_, err = store.GetScript(hash, nil)
	require.Error(t, err)
}

// TestCheckedUint8 unit-tests the helper directly: zero, the maximum
// in-range value, and both out-of-range directions (negative and > 255).
func TestCheckedUint8(t *testing.T) {
	t.Parallel()
	v, err := checkedUint8(0)
	require.NoError(t, err)
	assert.Equal(t, uint8(0), v)

	v, err = checkedUint8(255)
	require.NoError(t, err)
	assert.Equal(t, uint8(255), v)

	_, err = checkedUint8(256)
	require.Error(t, err)

	_, err = checkedUint8(-1)
	require.Error(t, err)
}
