// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chainselection

import (
	"sync/atomic"
	"testing"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestPeerTipAdmissionDefersExpiredGeneration(t *testing.T) {
	t.Parallel()
	for _, underLock := range []bool{false, true} {
		name := "before lock"
		if underLock {
			name = "under lock"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var checks atomic.Uint64
			cs := NewChainSelector(ChainSelectorConfig{
				HeaderCryptoVerificationGenerationCurrent: func(uint64) bool { return underLock && checks.Add(1) == 1 },
			})
			tip := ochainsync.Tip{
				Point:       ocommon.NewPoint(20, make([]byte, 32)),
				BlockNumber: 1,
			}
			update := PeerTipUpdateEvent{
				ConnectionId:                       newTestConnectionId(1),
				AdmissionID:                        1,
				ObservedTip:                        tip,
				Tip:                                tip,
				HeaderCryptoVerificationGeneration: 217,
			}
			require.Equal(
				t,
				PeerTipUpdateDeferred,
				cs.PreparePeerTipAdmissionDisposition(update),
				"local expiry must be distinguishable from peer rejection",
			)
			require.Empty(
				t,
				cs.pendingPeerTips,
				"an expired verdict must not stage a candidate",
			)
			require.False(
				t,
				cs.PreparePeerTipAdmission(update),
				"the boolean API must not report deferred admission as success",
			)
			require.Nil(t, cs.GetPeerTip(update.ConnectionId))
		})
	}
}
