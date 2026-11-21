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
	"testing"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// The delta of a Leios closure applied after its context slot gives its UTxO
// events that context slot as their effective slot; any other delta, and a
// closure applied at its context slot, gives none.
func TestLedgerDeltaClosureEffectiveSlot(t *testing.T) {
	t.Parallel()
	before, same := uint64(90), uint64(100)
	for _, tc := range []struct {
		name  string
		delta *LedgerDelta
		want  uint64
	}{
		{"ordinary block", &LedgerDelta{Point: ocommon.Point{Slot: 100}}, 0},
		{"closure after its context", &LedgerDelta{
			Point: ocommon.Point{Slot: 100}, closureContextSlot: &before,
		}, 90},
		{"closure at its context", &LedgerDelta{
			Point: ocommon.Point{Slot: 100}, closureContextSlot: &same,
		}, 0},
	} {
		require.Equal(t, tc.want, tc.delta.closureEffectiveSlot(), tc.name)
	}
}
