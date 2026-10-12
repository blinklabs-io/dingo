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

package dingo

import (
	"context"
	"strings"
	"testing"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// TestLeiosPipelineAdapterParentAnnouncementRejectsWrongLengthTipHash covers
// the ranking-block hash announced as an endorser block's parent. Padded, a
// 31-byte tip hash would name a ranking block that is not on the chain.
func TestLeiosPipelineAdapterParentAnnouncementRejectsWrongLengthTipHash(
	t *testing.T,
) {
	t.Parallel()
	parent := leiosParentBlock(t, testLeiosHash(0x41), 8192)
	adapter := &leiosPipelineAdapter{
		chain: testLeiosParentChain{
			tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: parent.Slot,
					Hash: parent.Hash[:31],
				},
				BlockNumber: parent.Number,
			},
			block: parent,
		},
	}

	_, _, ok, err := adapter.ParentLeiosAnnouncement(context.Background())
	if err == nil {
		t.Fatal("expected an error for a 31-byte tip hash")
	}
	if !strings.Contains(err.Error(), "invalid blake2b-256 hash") {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("a rejected tip must not report a parent announcement")
	}
}
