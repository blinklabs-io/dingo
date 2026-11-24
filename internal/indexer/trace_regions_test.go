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
	"testing"

	"github.com/blinklabs-io/dingo/internal/tracing"
	"github.com/blinklabs-io/dingo/internal/tracing/tracetest"
	"github.com/stretchr/testify/require"
)

// Not t.Parallel: only one execution trace can run per process.
func TestWriterStepRecordsTraceRegions(t *testing.T) {
	store := newTestStore(t)
	chain := newFakeChain(4, "a")
	w := NewWriter(store, chain, &syncRowIndexer{}, nil, 3)

	var err error
	trace := tracetest.Capture(t, func() {
		err = w.CatchUp(t.Context())
	})
	require.NoError(t, err)

	tracetest.RequireRegions(
		t,
		trace,
		tracing.RegionIndexerReadBlocks,
		tracing.RegionIndexerBatch,
		tracing.RegionIndexerIndexBlocks,
		tracing.RegionSQLCommit,
		tracing.RegionBlobCommit,
	)
}
