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
	"github.com/stretchr/testify/require"
)

// TestRemoveEmptyKeepsEverythingElse checks only zero-length *.mem files are
// removed, and a missing directory is not an error.
func TestRemoveEmptyKeepsEverythingElse(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	write := func(name string, data []byte) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0o600))
	}
	write("00001.mem", nil)
	write("00002.mem", []byte("wal"))
	write("000001.vlog", nil)
	write("MANIFEST", []byte("m"))

	removed, err := badgermem.RemoveEmpty(dir)
	require.NoError(t, err)
	require.Equal(t, []string{"00001.mem"}, removed)
	for _, name := range []string{"00002.mem", "000001.vlog", "MANIFEST"} {
		require.FileExists(t, filepath.Join(dir, name))
	}
	require.NoFileExists(t, filepath.Join(dir, "00001.mem"))

	removed, err = badgermem.RemoveEmpty(filepath.Join(dir, "missing"))
	require.NoError(t, err)
	require.Empty(t, removed)
}
