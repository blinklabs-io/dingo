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
	"errors"
	"testing"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// TestCheckEphemeralBufferSpan covers rollbackLocked's buffer precondition.
//
// No public call path reaches the error: AddBlock and reconcile keep a fork's
// tip index and its in-memory buffer in step, and the behavioral rollback
// tests exercise only consistent chains. The check still has to be correct,
// because the deletion loop indexes that buffer per rolled-back block — a
// short buffer detected part-way through would leave the rollback half
// applied. Drive it directly rather than leaving the branch unexecuted.
func TestCheckEphemeralBufferSpan(t *testing.T) {
	points := func(n int) []ocommon.Point {
		out := make([]ocommon.Point, n)
		for i := range out {
			out[i] = ocommon.Point{Slot: uint64(i) * 20} //nolint:gosec
		}
		return out
	}

	for _, tc := range []struct {
		name       string
		persistent bool
		tipIndex   uint64
		lastCommon uint64
		bufferLen  int
		wantErr    bool
	}{
		{
			name:       "buffer exactly spans the fork",
			tipIndex:   6,
			lastCommon: 3,
			bufferLen:  3,
		},
		{
			name:       "buffer longer than the fork",
			tipIndex:   6,
			lastCommon: 3,
			bufferLen:  4,
		},
		{
			name:       "buffer one short",
			tipIndex:   6,
			lastCommon: 3,
			bufferLen:  2,
			wantErr:    true,
		},
		{
			name:       "buffer empty with blocks above the fork",
			tipIndex:   6,
			lastCommon: 3,
			bufferLen:  0,
			wantErr:    true,
		},
		{
			name:       "tip at the fork point needs no buffer",
			tipIndex:   3,
			lastCommon: 3,
			bufferLen:  0,
		},
		{
			name:       "tip below the fork point needs no buffer",
			tipIndex:   2,
			lastCommon: 3,
			bufferLen:  0,
		},
		{
			name:       "persistent chains keep no buffer",
			persistent: true,
			tipIndex:   6,
			lastCommon: 0,
			bufferLen:  0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Chain{
				persistent:           tc.persistent,
				tipBlockIndex:        tc.tipIndex,
				lastCommonBlockIndex: tc.lastCommon,
				blocks:               points(tc.bufferLen),
			}
			err := c.checkEphemeralBufferSpan()
			if tc.wantErr {
				if !errors.Is(err, ErrRollbackBeyondEphemeralChain) {
					t.Fatalf(
						"want ErrRollbackBeyondEphemeralChain, got %v",
						err,
					)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	}
}
