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

package database

import (
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/tracetest"
	"github.com/blinklabs-io/dingo/internal/tracing"
	"github.com/stretchr/testify/require"
)

// Not t.Parallel: only one execution trace can run per process.
func TestBlobOnlyCommitsRecordBlobCommitRegion(t *testing.T) {
	db := newTestDB(t)

	regions := tracetest.Record(t, func() {
		withCtx, err := db.BlockBatchTxnContext(t.Context())
		require.NoError(t, err)
		require.NoError(t, withCtx.Commit())
	})

	require.Equal(t, 1, regions.Begins[tracing.RegionBlobCommit])
	require.Equal(t, 1, regions.Ends[tracing.RegionBlobCommit])
	require.Empty(t, regions.Misuse)
}
