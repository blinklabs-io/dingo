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

	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

// TestBlockNumberContiguous covers the block-number continuity rule that binds a
// header's self-reported block number to its parent, preventing a forged
// (inflated) number from entering the chain and winning chain selection.
func TestBlockNumberContiguous(t *testing.T) {
	const parent = uint64(100)
	tests := []struct {
		name   string
		eraId  uint8
		number uint64
		wantOK bool
	}{
		{"shelley parent+1 ok", shelley.EraIdShelley, parent + 1, true},
		{"shelley same as parent rejected", shelley.EraIdShelley, parent, false},
		{"shelley inflated rejected", shelley.EraIdShelley, parent + 1_000_000, false},
		{"shelley below parent rejected", shelley.EraIdShelley, parent - 1, false},
		{"shelley zero rejected", shelley.EraIdShelley, 0, false},
		{"byron parent+1 ok (normal block)", byron.EraIdByron, parent + 1, true},
		{"byron same as parent ok (EBB)", byron.EraIdByron, parent, true},
		{"byron inflated rejected", byron.EraIdByron, parent + 1_000_000, false},
		{"byron below parent rejected", byron.EraIdByron, parent - 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := blockNumberContiguous(tt.eraId, tt.number, parent)
			if got != tt.wantOK {
				t.Fatalf(
					"blockNumberContiguous(era=%d, number=%d, parent=%d) = %v, want %v",
					tt.eraId, tt.number, parent, got, tt.wantOK,
				)
			}
		})
	}
}
