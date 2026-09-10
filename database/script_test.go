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

package database

import (
	"bytes"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestGetScript(t *testing.T) {
	db := openTestDB(t)
	raw := rawSQLiteMetadataFixture(t, db)
	hash := lcommon.NewBlake2b224(bytes.Repeat([]byte{0x45}, 28))
	content := []byte{0x82, 0x01, 0x02}
	_, err := raw.Exec(
		"INSERT INTO script (hash, content, created_slot, type) VALUES (?, ?, ?, ?)",
		hash[:],
		content,
		42,
		2,
	)
	require.NoError(t, err)

	got, err := db.GetScript(hash[:], nil)
	require.NoError(t, err)
	require.Equal(t, hash[:], got.Hash)
	require.Equal(t, content, got.Content)
	require.Equal(t, uint64(42), got.CreatedSlot)
	require.Equal(t, uint8(2), got.Type)

	_, err = db.GetScript(nil, nil)
	require.ErrorIs(t, err, ErrScriptNotFound)
	missing := lcommon.NewBlake2b224(bytes.Repeat([]byte{0x46}, 28))
	_, err = db.GetScript(missing[:], nil)
	require.ErrorIs(t, err, ErrScriptNotFound)
}
