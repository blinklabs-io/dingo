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
// addRawBlocks releases both chain locks when its transaction closure returns
// and only then does txn.Do commit, so the Commit-failure restore runs with
// the chain open to everyone else. Rolling the primary chain back while
// blockfetch appends to it is not a corner case -- it is what a ledger
// recovery rewind does on every deterministic rejection -- and the restore
// used to write its snapshot back unconditionally. That raised tipBlockIndex
// to a value above the blocks the concurrent rollback had already deleted, so
// the chain claimed a tip it did not store: the ledger's windowed rewind then
// asked for the point a security parameter behind that tip and was told the
// block did not exist (issue #3889).
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
