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
	"context"
	"testing"

	sdkchain "github.com/blinklabs-io/dingo/sdk/chain"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type testConsensusRole struct {
	a, b   sdkchain.Tip
	result sdkchain.Comparison
}

func (r *testConsensusRole) Compare(a, b sdkchain.Tip) sdkchain.Comparison {
	r.a, r.b = a, b
	return r.result
}

func (*testConsensusRole) ShouldProduceBlock(uint64) bool { return false }
func (*testConsensusRole) NextLeaderSlot(uint64) (uint64, bool) {
	return 0, false
}

func (*testConsensusRole) ValidateForgedBlock(
	context.Context,
	sdkchain.Block,
) error {
	return nil
}

func TestConsensusRoleReceivesNeutralTips(t *testing.T) {
	t.Parallel()

	role := &testConsensusRole{result: sdkchain.ComparisonBBetter}
	selector := NewChainSelector(ChainSelectorConfig{
		ConsensusRole:             role,
		DisableEventSubscriptions: true,
	})
	a := &PeerChainTip{Tip: ochainsync.Tip{
		Point:       ocommon.Point{Slot: 10, Hash: []byte("a")},
		BlockNumber: 8,
	}}
	b := &PeerChainTip{Tip: ochainsync.Tip{
		Point:       ocommon.Point{Slot: 11, Hash: []byte("b")},
		BlockNumber: 9,
	}}

	require.Equal(t, ChainBBetter, selector.compareConsensusTips(a, b))
	require.Equal(t, sdkchain.Tip{
		Point:       sdkchain.Point{Slot: 10, Hash: []byte("a")},
		BlockNumber: 8,
	}, role.a)
	require.Equal(t, sdkchain.Tip{
		Point:       sdkchain.Point{Slot: 11, Hash: []byte("b")},
		BlockNumber: 9,
	}, role.b)
}

func TestNeutralCallbacksTakePrecedence(t *testing.T) {
	t.Parallel()

	selector := NewChainSelector(ChainSelectorConfig{
		DisableEventSubscriptions: true,
		AppliedTip: func() (ochainsync.Tip, bool) {
			t.Fatal("legacy applied-tip callback called")
			return ochainsync.Tip{}, false
		},
		AppliedChainTip: func() (sdkchain.Tip, bool) {
			return sdkchain.Tip{
				Point:       sdkchain.Point{Slot: 20, Hash: []byte("tip")},
				BlockNumber: 19,
			}, true
		},
		ChainPointOnLocalChain: func(point sdkchain.Point) bool {
			return point.Slot == 20
		},
	})

	tip, ok := selector.configuredAppliedTip()
	require.True(t, ok)
	require.Equal(t, uint64(19), tip.BlockNumber)
	require.True(
		t,
		selector.onChain(ocommon.Point{Slot: 20, Hash: []byte("tip")}),
	)
}
