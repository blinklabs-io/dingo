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

package ledger

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBlockPipelineWorkerCount covers the CPU-scaled worker count that
// replaced the hardcoded blockPipelineDecodeWorkers/blockPipelineValidateWorkers
// constant of 2. A from-genesis sync profile showed a single core saturated
// while the rest of a 16-core host sat idle running the block pipeline at
// that fixed count; this proves the replacement actually tracks GOMAXPROCS
// within its floor and cap rather than silently staying pinned at 2.
//
// GOMAXPROCS is process-global, so these subtests cannot run in parallel
// with each other or with anything else that reads it; t.Cleanup restores
// the value this test observed on entry.
func TestBlockPipelineWorkerCount(t *testing.T) {
	original := runtime.GOMAXPROCS(0)
	t.Cleanup(func() { runtime.GOMAXPROCS(original) })

	cases := []struct {
		gomaxprocs int
		want       int
	}{
		// Below the floor: a constrained host (a single-core container)
		// must get exactly what it did before this change, never fewer.
		{gomaxprocs: 1, want: blockPipelineMinWorkers},
		// At and within the floor/cap range: tracks GOMAXPROCS exactly.
		{gomaxprocs: blockPipelineMinWorkers, want: blockPipelineMinWorkers},
		{gomaxprocs: 4, want: 4},
		{gomaxprocs: blockPipelineMaxWorkers, want: blockPipelineMaxWorkers},
		// Above the cap: bounded rather than left to scale unboundedly.
		{gomaxprocs: blockPipelineMaxWorkers + 1, want: blockPipelineMaxWorkers},
		{gomaxprocs: 128, want: blockPipelineMaxWorkers},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("GOMAXPROCS=%d", tc.gomaxprocs), func(t *testing.T) {
			runtime.GOMAXPROCS(tc.gomaxprocs)
			require.Equal(t, tc.want, blockPipelineWorkerCount())
		})
	}
}
