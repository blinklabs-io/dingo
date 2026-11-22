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

package indexer

import (
	"bytes"
	rtrace "runtime/trace"
	"testing"

	"github.com/blinklabs-io/dingo/internal/tracing"
	"github.com/stretchr/testify/require"
)

// Not t.Parallel: only one execution trace can run per process.
func TestWriterStepRecordsTraceRegions(t *testing.T) {
	store := newTestStore(t)
	chain := newFakeChain(4, "a")
	w := NewWriter(store, chain, &syncRowIndexer{}, nil, 3)

	var buf bytes.Buffer
	require.NoError(t, rtrace.Start(&buf))
	err := w.CatchUp(t.Context())
	rtrace.Stop()
	require.NoError(t, err)

	for _, name := range []string{
		tracing.RegionIndexerReadBlocks,
		tracing.RegionIndexerBatch,
		tracing.RegionIndexerIndexBlocks,
		tracing.RegionSQLCommit,
		tracing.RegionBlobCommit,
	} {
		require.Truef(
			t,
			bytes.Contains(buf.Bytes(), []byte(name)),
			"execution trace has no region %q",
			name,
		)
	}
}
