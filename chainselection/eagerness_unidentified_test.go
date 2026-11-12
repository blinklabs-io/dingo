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

package chainselection

import (
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A connection without a corroboration identity can never witness or be
// selected while Genesis corroboration is active, so its fragment must not
// bound the Limit on Eagerness: here a disjoint unidentified fragment would
// leave the identified candidates with no common point.
func TestLimitOnEagernessIgnoresUnidentifiedCandidates(t *testing.T) {
	t.Parallel()
	a := newTestConnectionId(1)
	b := newTestConnectionId(2)
	u := newTestConnectionId(3)
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         loeTestK,
		MinCorroboratingPeers: 1,
		PeerIdentity: func(connId ouroboros.ConnectionId) string {
			switch connId {
			case a:
				return "pool-a"
			case b:
				return "pool-b"
			default:
				return ""
			}
		},
	})
	feedLoEChain(cs, a, "c", 1, 14)
	feedLoEChain(cs, b, "c", 1, 14)
	feedLoEChain(cs, u, "u", 1, 40)
	cs.SetLocalTip(loeTip("c", 10))

	limit := cs.EagernessLimit()
	require.True(t, limit.Active)
	require.True(t, limit.Intersected,
		"identified candidates share a point; the unidentified one must not break it")
	assert.Equal(t, "c14", string(limit.Point.Hash))

	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	assert.True(t, cs.eagernessVouchesLocked(a, cs.peerTips[a], cs.localTip, true),
		"the identified prefix vouches for a frontier within the limit")
}
