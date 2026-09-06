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

package chain

import (
	"testing"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// TestBatchRestoreIsSafeLocked pins which chain states a failed add batch may
// write its pre-batch snapshot back over.
//
// addRawBlocks guards its in-memory update and transaction commit with both
// chain locks, so a rollback cannot observe a tip whose blocks are not durable.
// The generation guard still protects the Commit-failure restore if a future
// caller changes that locking boundary. Before the guard, an unconditional
// restore could raise tipBlockIndex above blocks a concurrent rollback had
// deleted, leaving the chain claiming a tip it did not store (issue #3889).
func TestBatchRestoreIsSafeLocked(t *testing.T) {
	applied := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("applied")},
		BlockNumber: 10,
	}
	const appliedIndex = uint64(10)
	const appliedGeneration = uint64(1)

	for _, tc := range []struct {
		name  string
		chain *Chain
		want  bool
	}{
		{
			name: "unchanged since the batch",
			chain: &Chain{
				tipBlockIndex:      appliedIndex,
				mutationGeneration: appliedGeneration,
				currentTip:         applied,
			},
			want: true,
		},
		{
			name: "concurrent rollback lowered the tip",
			chain: &Chain{
				tipBlockIndex: appliedIndex - 4,
				currentTip: ochainsync.Tip{
					Point: ocommon.Point{
						Slot: 60,
						Hash: []byte("rolled-back"),
					},
					BlockNumber: 6,
				},
			},
			want: false,
		},
		{
			name: "concurrent append raised the tip",
			chain: &Chain{
				tipBlockIndex: appliedIndex + 1,
				currentTip: ochainsync.Tip{
					Point: ocommon.Point{
						Slot: 110,
						Hash: []byte("appended"),
					},
					BlockNumber: 11,
				},
			},
			want: false,
		},
		{
			name: "same index, different block",
			chain: &Chain{
				tipBlockIndex: appliedIndex,
				currentTip: ochainsync.Tip{
					Point: ocommon.Point{
						Slot: 100,
						Hash: []byte("other-fork"),
					},
					BlockNumber: 10,
				},
			},
			want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.chain.batchRestoreIsSafeLocked(
				applied, appliedIndex, appliedGeneration,
			)
			if got != tc.want {
				t.Fatalf(
					"batchRestoreIsSafeLocked() = %v, want %v",
					got,
					tc.want,
				)
			}
		})
	}
}
