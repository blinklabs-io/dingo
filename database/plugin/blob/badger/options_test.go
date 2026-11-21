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

package badger

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewRejectsOnDiskCompressionWithoutBlockCache(t *testing.T) {
	t.Parallel()
	store, err := New(
		WithDataDir(t.TempDir()),
		WithBlockCacheSize(0),
		WithCompressionEnabled(true),
		WithDeferOpen(),
	)
	require.Nil(t, store)
	require.EqualError(
		t,
		err,
		"badger block cache size must be greater than zero when compression is enabled",
	)
}

func TestWithSubdirSelectsStoreDirectory(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for _, tc := range []struct {
		subdir string
		want   string
	}{
		{subdir: "", want: "blob"},
		{subdir: "leios", want: "leios"},
	} {
		store, err := New(
			WithDataDir(dir),
			WithSubdir(tc.subdir),
			WithBlockCacheSize(1<<20),
			WithValueLogFileSize(16<<20),
			WithMemTableSize(8<<20),
			WithGc(false),
		)
		require.NoError(t, err)
		require.NoError(t, store.Close())
		require.DirExists(t, filepath.Join(dir, tc.want))
	}
}
