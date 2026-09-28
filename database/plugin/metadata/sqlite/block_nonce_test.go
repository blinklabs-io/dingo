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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

func TestGetLatestBlockNonce(t *testing.T) {
	store, _ := newSharedSQLStore(t)

	row, ok, err := store.GetLatestBlockNonce(nil)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, models.BlockNonce{}, row)

	require.NoError(t, store.SetBlockNonce(
		[]byte{0x01}, 10, []byte{0x0a}, false, nil,
	))
	require.NoError(t, store.SetBlockNonce(
		[]byte{0xff}, 20, []byte{0x14}, false, nil,
	))

	row, ok, err = store.GetLatestBlockNonce(nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(20), row.Slot)
	require.Equal(t, []byte{0xff}, row.Hash)
}

func TestGetLatestBlockNonceUsesApplicationOrderForSameSlot(t *testing.T) {
	store, _ := newSharedSQLStore(t)

	const slot = uint64(20)
	firstHash := []byte{0xff}
	secondHash := []byte{0x01}
	firstNonce := []byte("nonce-first")
	secondNonce := []byte("nonce-second")

	// The later application has the lower hash. Hash ordering must not make the
	// earlier row look like the durable floor after a same-slot fork race.
	require.NoError(t, store.SetBlockNonce(
		firstHash, slot, firstNonce, false, nil,
	))
	require.NoError(t, store.SetBlockNonce(
		secondHash, slot, secondNonce, false, nil,
	))

	row, ok, err := store.GetLatestBlockNonce(nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, secondHash, row.Hash)
	require.Equal(t, secondNonce, row.Nonce)
}
