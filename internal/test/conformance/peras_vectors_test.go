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

package conformance

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/ouroboros-mock/conformance"
	"github.com/stretchr/testify/require"
)

// perasVectorDir is the corpus subdirectory, beside eras/ and synthetic/,
// that holds Peras vectors. The embedded ouroboros-mock corpus does not ship
// it yet.
const perasVectorDir = "peras"

// loadPerasVectors returns the vector files under root, collected by the same
// rules as the rest of the corpus. A missing root yields no vectors rather
// than an error so the Peras subset skips cleanly.
func loadPerasVectors(root string) ([]string, error) {
	vectors, err := conformance.CollectVectorFiles(root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	return vectors, err
}

func TestLoadPerasVectors(t *testing.T) {
	t.Parallel()

	t.Run("missing root is not an error", func(t *testing.T) {
		t.Parallel()
		vectors, err := loadPerasVectors(filepath.Join(t.TempDir(), "absent"))
		require.NoError(t, err)
		require.Empty(t, vectors)
	})

	t.Run("uses the corpus vector convention", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		for _, name := range []string{
			"b",
			"a/nested",
			"a/README.md",
			"pparams-by-hash/0011",
		} {
			path := filepath.Join(root, name)
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
			require.NoError(t, os.WriteFile(path, []byte{0x80}, 0o644))
		}
		vectors, err := loadPerasVectors(root)
		require.NoError(t, err)
		require.Equal(t, []string{
			filepath.Join(root, "a", "nested"),
			filepath.Join(root, "b"),
		}, vectors)
	})
}

// TestPerasConformanceVectors is the Peras discovery hook. It skips until the
// corpus carries a peras/ directory; there is no Peras validation yet, so each
// discovered vector is only checked to be non-empty.
func TestPerasConformanceVectors(t *testing.T) {
	t.Parallel()

	root, err := corpusTestdataRoot()
	require.NoError(t, err)
	vectors, err := loadPerasVectors(filepath.Join(root, perasVectorDir))
	require.NoError(t, err)
	if len(vectors) == 0 {
		t.Skipf("no Peras vectors under %s in the corpus", perasVectorDir)
	}
	for _, path := range vectors {
		rel, err := filepath.Rel(root, path)
		require.NoError(t, err)
		t.Run(filepath.ToSlash(rel), func(t *testing.T) {
			t.Parallel()
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			require.NotEmpty(t, data)
		})
	}
}
