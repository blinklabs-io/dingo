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
	"context"
	"testing"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVerifyDeferredBlockHeaderStateVerifyReplayHeaders pins the apply-time
// gate VerifyReplayHeaders opens: an unmarked block is verified when the flag
// is set, skipped when it is not, and skipped when Mithril covers its slot.
func TestVerifyDeferredBlockHeaderStateVerifyReplayHeaders(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		seed         byte
		tamper       tamperOption
		verify       bool
		mithrilAtTip bool
		wantReject   bool
	}{
		{
			name:   "flag off leaves an unmarked block unverified",
			seed:   70,
			tamper: tamperKESSig,
		},
		{
			name:       "flag on rejects an unmarked block with a bad KES signature",
			seed:       71,
			tamper:     tamperKESSig,
			verify:     true,
			wantReject: true,
		},
		{
			name:       "flag on rejects an unmarked block with a bad VRF proof",
			seed:       72,
			tamper:     tamperVRFProof,
			verify:     true,
			wantReject: true,
		},
		{
			name:         "flag on skips a Mithril-covered slot",
			seed:         73,
			tamper:       tamperKESSig,
			verify:       true,
			mithrilAtTip: true,
		},
		{
			name:   "flag on accepts a valid unmarked block",
			seed:   74,
			tamper: tamperNone,
			verify: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tb := createTestBlock(t, [32]byte{tc.seed}, 0, tc.tamper)
			ls, db := newEligibilityTestLedger(t, tb.epochNonce)
			poolKeyHash := tb.block.IssuerVkey().Hash()
			seedBlockPoolRegistration(t, db, tb.block)
			seedPoolStakeSnapshot(t, db, 4, poolKeyHash[:], 1_000_000_000)
			ls.config.VerifyReplayHeaders = tc.verify
			if tc.mithrilAtTip {
				ls.mithrilLedgerSlot = tb.block.SlotNumber()
			}
			point := ocommon.NewPoint(
				tb.block.SlotNumber(),
				tb.block.Hash().Bytes(),
			)

			err := ls.verifyDeferredBlockHeaderState(
				context.Background(), nil, point, tb.block,
			)
			if !tc.wantReject {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			var hve *headerValidationError
			require.ErrorAs(t, err, &hve)
			assert.Equal(t, point, hve.BlockPoint)
		})
	}
}
