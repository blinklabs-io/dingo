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

package database_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

func TestBlockCreateImmutableRequiresImmutableDB(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	err = db.BlockCreateImmutable(
		models.Block{ID: 1, Slot: 1, Hash: bytes.Repeat([]byte{1}, 32)},
		types.ImmutableMarker{Chunk: 0, Offset: 1, Size: 2, CRC: 3},
		nil,
	)
	require.ErrorIs(t, err, database.ErrImmutableNotEnabled)
	require.ErrorIs(t,
		db.AdoptImmutableTip(1, bytes.Repeat([]byte{1}, 32), 1),
		database.ErrImmutableNotEnabled)
}

func TestAdoptImmutableTipOnlyMovesForward(t *testing.T) {
	t.Parallel()
	db, _ := newImmutableDB(t, 10)
	hashA := bytes.Repeat([]byte{0xA}, 32)
	hashB := bytes.Repeat([]byte{0xB}, 32)

	require.NoError(t, db.AdoptImmutableTip(100, hashA, 10))
	st, _ := db.ImmutableState()
	require.True(t, st.HasTip)
	require.Equal(t, uint64(10), st.TipIndex)
	require.Equal(t, hashA, st.TipHash)
	require.NoError(t, db.AdoptImmutableTip(100, hashA, 10),
		"repeating the recorded tip is a no-op")

	require.Error(t, db.AdoptImmutableTip(90, hashB, 9), "lower index")
	require.Error(t, db.AdoptImmutableTip(100, hashB, 11), "same slot")
	require.Error(t, db.AdoptImmutableTip(200, nil, 11), "empty hash")
	require.NoError(t, db.AdoptImmutableTip(200, hashB, 11))
	st, _ = db.ImmutableState()
	require.Equal(t, uint64(11), st.TipIndex)
	require.Equal(t, uint64(200), st.TipSlot)
}
