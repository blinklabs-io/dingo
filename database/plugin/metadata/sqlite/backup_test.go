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
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/stretchr/testify/require"
)

func newBackupStore(t *testing.T, dataDir string) interface {
	Start(context.Context) error
	Close() error
	BackupTo(context.Context, string) error
	RestoreFrom(context.Context, string) error
} {
	t.Helper()
	store, err := NewSQLStore(
		Config{DataDir: dataDir},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	return store
}

func TestBackupToRestoreFromRoundTrip(t *testing.T) {
	src := newBackupStore(t, t.TempDir())
	require.NoError(t, src.Start(context.Background()))
	defer src.Close() //nolint:errcheck

	backupPath := filepath.Join(t.TempDir(), "backup.sqlite")
	require.NoError(t, src.BackupTo(context.Background(), backupPath))

	dstDir := filepath.Join(t.TempDir(), "nested", "restore-target")
	dst := newBackupStore(t, dstDir)
	require.NoError(t, dst.RestoreFrom(context.Background(), backupPath))
	require.FileExists(t, filepath.Join(dstDir, "metadata.sqlite"))
	require.NoError(t, dst.Start(context.Background()))
	defer dst.Close() //nolint:errcheck
}

func TestBackupRejectsInMemoryAndExistingDestination(t *testing.T) {
	store := newBackupStore(t, "")
	require.NoError(t, store.Start(context.Background()))
	defer store.Close() //nolint:errcheck

	path := filepath.Join(t.TempDir(), "backup.sqlite")
	require.Error(t, store.BackupTo(context.Background(), path))

	onDisk := newBackupStore(t, t.TempDir())
	require.NoError(t, onDisk.Start(context.Background()))
	defer onDisk.Close() //nolint:errcheck
	require.NoError(t, onDisk.BackupTo(context.Background(), path))
	require.Error(t, onDisk.BackupTo(context.Background(), path))
}

func TestBackupRequiresStartedStore(t *testing.T) {
	store := newBackupStore(t, t.TempDir())
	require.Error(t, store.BackupTo(
		context.Background(), filepath.Join(t.TempDir(), "backup.sqlite"),
	))
	require.NoError(t, store.Close())
}

func TestBackupAfterCloseReturnsError(t *testing.T) {
	store := newBackupStore(t, t.TempDir())
	require.NoError(t, store.Start(context.Background()))
	require.NoError(t, store.Close())
	require.Error(t, store.BackupTo(
		context.Background(), filepath.Join(t.TempDir(), "backup.sqlite"),
	))
}

func TestRestoreRejectsExistingDestination(t *testing.T) {
	src := newBackupStore(t, t.TempDir())
	require.NoError(t, src.Start(context.Background()))
	defer src.Close() //nolint:errcheck
	backupPath := filepath.Join(t.TempDir(), "backup.sqlite")
	require.NoError(t, src.BackupTo(context.Background(), backupPath))

	dst := newBackupStore(t, t.TempDir())
	require.NoError(t, dst.Start(context.Background()))
	defer dst.Close() //nolint:errcheck
	require.Error(t, dst.RestoreFrom(context.Background(), backupPath))
}

func TestContextReaderStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader := &contextReader{
		ctx: ctx,
		r:   bytes.NewReader(bytes.Repeat([]byte("x"), 32)),
	}
	buf := make([]byte, 8)
	_, err := reader.Read(buf)
	require.NoError(t, err)
	cancel()
	_, err = reader.Read(buf)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCopyFile(t *testing.T) {
	src := filepath.Join(t.TempDir(), "src.bin")
	require.NoError(t, os.WriteFile(src, []byte("hello"), 0o644))
	dstDir := filepath.Join(t.TempDir(), "a", "b")
	dst := filepath.Join(dstDir, "dst.bin")
	require.NoError(t, os.MkdirAll(dstDir, 0o755))
	require.NoError(t, copyFile(context.Background(), src, dst))
	data, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), data)
}

func TestCopyReaderToFileRemovesPartialDestinationOnCancellation(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "dst.bin")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(
		t,
		copyReaderToFile(ctx, bytes.NewReader([]byte("hello")), dst),
	)
	_, err := os.Stat(dst)
	require.True(t, os.IsNotExist(err))
}

func TestBackupFailureDoesNotTouchDestination(t *testing.T) {
	store := newBackupStore(t, t.TempDir())
	require.NoError(t, store.Start(context.Background()))
	defer store.Close() //nolint:errcheck

	dst := filepath.Join(t.TempDir(), "backup.sqlite")
	original := runVacuumInto
	t.Cleanup(func() { runVacuumInto = original })
	runVacuumInto = func(_ context.Context, _ *sql.DB, staged string) error {
		require.NoError(t, os.WriteFile(staged, []byte("partial"), 0o600))
		require.NoError(t, os.WriteFile(dst, []byte("concurrent"), 0o600))
		return errors.New("simulated vacuum failure")
	}
	require.Error(t, store.BackupTo(context.Background(), dst))
	data, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, []byte("concurrent"), data)
}

var _ io.Reader = &contextReader{}
