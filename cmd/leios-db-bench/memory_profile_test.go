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

package main

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemoryProfileWritesCompressedProfile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "heap.pprof")
	stop := startMemoryProfile(path)
	require.NoError(t, stop())
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close() //nolint:errcheck
	reader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer reader.Close() //nolint:errcheck
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NotEmpty(t, data)
}

func TestMemoryProfileReportsCreateError(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "absent", "heap.pprof")
	stop := startMemoryProfile(path)
	require.ErrorIs(t, stop(), os.ErrNotExist)
}
