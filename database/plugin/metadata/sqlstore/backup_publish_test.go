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
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPublishBackupFileRoundTrip validates the basic happy path: the
// write callback's output ends up at dstPath, including creating any
// missing parent directories.
func TestPublishBackupFileRoundTrip(t *testing.T) {
	dstDir := filepath.Join(t.TempDir(), "nested")
	dst := filepath.Join(dstDir, "backup.bin")
	err := PublishBackupFile(dst, func(stagedPath string) error {
		return os.WriteFile(stagedPath, []byte("hello"), 0o600)
	})
	require.NoError(t, err)
	data, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), data)
}

// TestPublishBackupFileRejectsExistingDestination validates that
// PublishBackupFile refuses to run the write callback at all when
// dstPath already exists, leaving the existing file untouched.
func TestPublishBackupFileRejectsExistingDestination(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "backup.bin")
	require.NoError(t, os.WriteFile(dst, []byte("existing"), 0o600))
	err := PublishBackupFile(dst, func(stagedPath string) error {
		return os.WriteFile(stagedPath, []byte("new"), 0o600)
	})
	require.Error(t, err)
	data, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, []byte("existing"), data)
}

// TestPublishBackupFileFailureDoesNotClobberConcurrentDestination guards the
// TOCTOU property PublishBackupFile exists for: a failed write must not
// touch dstPath even if something else created it concurrently, in the
// window between the initial existence check and the failure.
func TestPublishBackupFileFailureDoesNotClobberConcurrentDestination(
	t *testing.T,
) {
	dst := filepath.Join(t.TempDir(), "backup.bin")
	err := PublishBackupFile(dst, func(stagedPath string) error {
		require.NoError(t, os.WriteFile(stagedPath, []byte("partial"), 0o600))
		require.NoError(t, os.WriteFile(dst, []byte("concurrent"), 0o600))
		return errors.New("simulated failure")
	})
	require.Error(t, err)
	data, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, []byte("concurrent"), data)
}
