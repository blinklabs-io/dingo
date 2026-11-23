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

package badgermem_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/internal/badgermem"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func closedStore(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("k"), []byte("v"))
	}))
	require.NoError(t, db.Close())
	return dir
}

// TestOpenRetriesEmptyMemtable checks an empty memtable file left by an
// interrupted shutdown no longer stops the store opening, and data survives.
func TestOpenRetriesEmptyMemtable(t *testing.T) {
	t.Parallel()
	dir := closedStore(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "00099.mem"), nil, 0o600))

	db, retried, err := badgermem.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.True(t, retried)
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("k"))
		if err != nil {
			return err
		}
		v, err := item.ValueCopy(nil)
		require.Equal(t, []byte("v"), v)
		return err
	}))
}

// TestOpenLeavesAHeldDirectoryAlone checks a directory another handle holds
// is neither retried nor modified: badger's lock check fails before it reaches
// the memtable files.
func TestOpenLeavesAHeldDirectoryAlone(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	holder, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, holder.Close()) })
	empty := filepath.Join(dir, "00099.mem")
	require.NoError(t, os.WriteFile(empty, nil, 0o600))

	db, retried, err := badgermem.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.Error(t, err)
	require.Nil(t, db)
	require.False(t, retried)
	info, err := os.Stat(empty)
	require.NoError(t, err)
	require.Zero(t, info.Size())
}
