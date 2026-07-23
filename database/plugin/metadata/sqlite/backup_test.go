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
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBackupToRestoreFromRoundTrip verifies that a backed-up store
// restores into a fresh directory with matching account data.
func TestBackupToRestoreFromRoundTrip(t *testing.T) {
	srcDir := t.TempDir()
	src, err := New(srcDir, nil, nil)
	require.NoError(t, err)
	require.NoError(t, src.Start())
	defer src.Close() //nolint:errcheck

	require.NoError(t, src.SetAccount(
		0,
		make([]byte, 28),
		nil,
		nil,
		100,
		true,
		nil,
	))

	backupPath := filepath.Join(t.TempDir(), "backup.sqlite")
	require.NoError(t, src.BackupTo(context.Background(), backupPath))

	dstDir := t.TempDir()
	dst, err := New(dstDir, nil, nil)
	require.NoError(t, err)
	require.NoError(t, dst.RestoreFrom(context.Background(), backupPath))
	require.NoError(t, dst.Start())
	defer dst.Close() //nolint:errcheck

	account, err := dst.GetAccountByCredential(0, make([]byte, 28), false, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(100), account.AddedSlot)
}

// TestBackupToInMemoryErrors verifies that BackupTo errors when the
// store has no on-disk data directory to back up.
func TestBackupToInMemoryErrors(t *testing.T) {
	db, err := New("", nil, nil)
	require.NoError(t, err)
	require.NoError(t, db.Start())
	defer db.Close() //nolint:errcheck

	err = db.BackupTo(context.Background(), filepath.Join(t.TempDir(), "out.sqlite"))
	require.Error(t, err)
}

// TestBackupToExistingDestinationErrors verifies that a second BackupTo
// call to the same path fails rather than overwriting it.
func TestBackupToExistingDestinationErrors(t *testing.T) {
	srcDir := t.TempDir()
	src, err := New(srcDir, nil, nil)
	require.NoError(t, err)
	require.NoError(t, src.Start())
	defer src.Close() //nolint:errcheck

	backupPath := filepath.Join(t.TempDir(), "backup.sqlite")
	require.NoError(t, src.BackupTo(context.Background(), backupPath))
	// Second backup to the same path must fail rather than clobber it.
	err = src.BackupTo(context.Background(), backupPath)
	require.Error(t, err)
}

// TestRestoreFromExistingDestinationErrors verifies that RestoreFrom
// refuses to clobber a destination that already has a metadata database.
func TestRestoreFromExistingDestinationErrors(t *testing.T) {
	srcDir := t.TempDir()
	src, err := New(srcDir, nil, nil)
	require.NoError(t, err)
	require.NoError(t, src.Start())
	defer src.Close() //nolint:errcheck

	backupPath := filepath.Join(t.TempDir(), "backup.sqlite")
	require.NoError(t, src.BackupTo(context.Background(), backupPath))

	dstDir := t.TempDir()
	dst, err := New(dstDir, nil, nil)
	require.NoError(t, err)
	require.NoError(t, dst.Start())
	defer dst.Close() //nolint:errcheck

	// dst already has a metadata.sqlite from Start(); restoring must refuse
	// to clobber it.
	err = dst.RestoreFrom(context.Background(), backupPath)
	require.Error(t, err)
}
