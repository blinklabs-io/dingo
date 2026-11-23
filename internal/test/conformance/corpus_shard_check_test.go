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

	"github.com/stretchr/testify/require"
)

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
