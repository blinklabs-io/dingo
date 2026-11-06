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
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/ouroboros-mock/conformance"
	"github.com/stretchr/testify/require"
)

// TestCorpusShardsPartitionTheCorpus proves the shard roots hold every corpus
// vector exactly once between them, so a sharded replay cannot skip or repeat
// a vector.
func TestCorpusShardsPartitionTheCorpus(t *testing.T) {
	root, err := corpusTestdataRoot()
	require.NoError(t, err)
	vectors, err := collectEntryPointVectors(root)
	require.NoError(t, err)
	const workers = 5
	vectorShard := make(map[string]int, len(vectors))
	for i, path := range vectors {
		vectorShard[path] = i % workers
	}
	scratch := t.TempDir()
	seen := make(map[string]int, len(vectors))
	for shard := range workers {
		dst := filepath.Join(scratch, "shard")
		dst = filepath.Join(dst, string(rune('a'+shard)))
		require.NoError(t, shardCorpusRoot(root, dst, vectorShard, shard))
		found := make([]string, 0)
		for _, sub := range []string{"eras", "synthetic"} {
			paths, err := conformance.CollectVectorFiles(
				filepath.Join(dst, sub),
			)
			if os.IsNotExist(err) {
				continue
			}
			require.NoError(t, err)
			found = append(found, paths...)
		}
		for _, path := range found {
			rel, err := filepath.Rel(dst, path)
			require.NoError(t, err)
			seen[filepath.Join(root, rel)]++
		}
	}
	require.Len(t, seen, len(vectors))
	for path, count := range seen {
		require.Equal(t, 1, count, "vector %s is in %d shards", path, count)
	}
}

// TestShardedReplayAttributesFailuresToTheirVector proves a failing vector is
// still reported under its own path in the corpus root, whichever shard ran
// it.
func TestShardedReplayAttributesFailuresToTheirVector(t *testing.T) {
	root := t.TempDir()
	eras := filepath.Join(root, "eras")
	require.NoError(t, os.MkdirAll(eras, 0o755))
	var bad []string
	for _, name := range []string{"a", "b", "c"} {
		path := filepath.Join(eras, name)
		require.NoError(t, os.WriteFile(path, []byte{0xff}, 0o600))
		bad = append(bad, path)
	}
	run := replayCorpusSharded(root, 2, NewDingoStateManager)
	require.NoError(t, run.err)
	require.Len(t, run.results, len(bad))
	for i, result := range run.results {
		require.Equal(t, bad[i], result.Path)
		require.False(t, result.Success)
		require.Error(t, result.Error)
	}
}
