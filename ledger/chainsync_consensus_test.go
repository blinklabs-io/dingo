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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/consensus"
	"github.com/blinklabs-io/dingo/consensus/praos"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// fixedComparisonProvider answers every tip comparison with one result, so a
// test can tell a decision made by the provider from one made by the ledger.
type fixedComparisonProvider struct {
	consensus.Provider
	result praos.ChainComparisonResult
}

type recordingComparisonProvider struct {
	consensus.Provider
	incoming []ochainsync.Tip
}

func (p fixedComparisonProvider) CompareTips(
	ochainsync.Tip,
	ochainsync.Tip,
	praos.PraosTiebreakerView,
	praos.PraosTiebreakerView,
) praos.ChainComparisonResult {
	return p.result
}

func (p *recordingComparisonProvider) CompareTips(
	incoming ochainsync.Tip,
	_ ochainsync.Tip,
	_ praos.PraosTiebreakerView,
	_ praos.PraosTiebreakerView,
) praos.ChainComparisonResult {
	p.incoming = append(p.incoming, incoming)
	return praos.ChainEqual
}

func newProviderComparisonLedger(
	result praos.ChainComparisonResult,
) *LedgerState {
	return &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			Consensus: fixedComparisonProvider{
				Provider: consensus.NewPraosProvider(),
				result:   result,
			},
		},
	}
}

func chainsyncEventAtTip(slot, blockNumber uint64) ChainsyncEvent {
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(slot, []byte("incoming")),
		BlockNumber: blockNumber,
	}
	return ChainsyncEvent{Point: tip.Point, Tip: tip}
}

func TestEarlierHeaderCanBeatLocalTip_DefersHeightDecisionToProvider(
	t *testing.T,
) {
	t.Parallel()

	localTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(200, []byte("local")),
		BlockNumber: 10,
	}
	higher := chainsyncEventAtTip(100, 11)
	lower := chainsyncEventAtTip(100, 9)

	rejecting := newProviderComparisonLedger(praos.ChainBBetter)
	require.False(t, rejecting.earlierHeaderCanBeatLocalTip(higher, localTip),
		"a higher block number must not override the provider's rejection")

	preferring := newProviderComparisonLedger(praos.ChainABetter)
	require.True(t, preferring.earlierHeaderCanBeatLocalTip(lower, localTip),
		"a lower block number must not override the provider's preference")
}

func TestCompareIncomingHeaderToLocalTip_IgnoresAdvertisedTip(
	t *testing.T,
) {
	t.Parallel()

	localTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(200, []byte("local")),
		BlockNumber: 10,
	}
	event := chainsyncEventAtTip(300, 10)
	event.BlockHeader = &babbage.BabbageBlockHeader{
		Body: babbage.BabbageBlockHeaderBody{BlockNumber: 10, Slot: 300},
	}
	event.Tip = ochainsync.Tip{
		Point:       ocommon.NewPoint(400, []byte("advertised")),
		BlockNumber: 12,
	}

	provider := &recordingComparisonProvider{
		Provider: consensus.NewPraosProvider(),
	}
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
			Consensus: provider,
		},
	}
	require.Equal(t, praos.ChainEqual,
		ls.compareIncomingHeaderToLocalTip(event, localTip))
	require.Equal(t, []ochainsync.Tip{observedHeaderTip(event)}, provider.incoming)
}
