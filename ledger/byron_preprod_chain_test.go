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

package ledger

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// loadPreprodByronChain returns every block of the preprod Byron era: the
// genesis EBB at slot 0 and the 45 main blocks through slot 84242, as served
// by a preprod relay. The fixture is a CBOR array of [block type, block CBOR].
func loadPreprodByronChain(t *testing.T) []gledger.Block {
	t.Helper()
	raw, err := os.ReadFile(
		filepath.Join("testdata", "preprod-byron-chain.cbor"),
	)
	require.NoError(t, err)
	var entries []struct {
		cbor.StructAsArray
		Type  uint
		Block []byte
	}
	_, err = cbor.Decode(raw, &entries)
	require.NoError(t, err)
	require.Len(t, entries, 46)
	blocks := make([]gledger.Block, 0, len(entries))
	for index, entry := range entries {
		block, err := gledger.NewBlockFromCbor(entry.Type, entry.Block)
		require.NoError(t, err, "decode block %d", index)
		if index > 0 {
			require.Equal(
				t,
				blocks[index-1].Hash(),
				block.PrevHash(),
				"block %d does not extend its predecessor",
				index,
			)
		}
		blocks = append(blocks, block)
	}
	return blocks
}

// TestPreprodByronChainIsValid replays the complete preprod Byron chain, which
// cardano-node accepted, onto an empty primary chain through the inbound
// envelope (placement, ordering, body proofs, EBB and main-block size limits)
// and the PBFT transition (genesis anchor, genesis issuer, proxy certificate,
// header signature, delegation, issuer window). No preprod update proposal
// changes a parameter, so the genesis limits are the adopted ones throughout.
func TestPreprodByronChainIsValid(t *testing.T) {
	t.Parallel()

	blocks := loadPreprodByronChain(t)
	nodeConfig, err := cardano.NewCardanoNodeConfigFromEmbedFS(
		cardano.EmbeddedConfigFS,
		"preprod/config.json",
	)
	require.NoError(t, err)
	cm, err := chain.NewManager(newTestDB(t), nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2160}))
	primary := cm.PrimaryChain()
	ls := &LedgerState{
		chain:  primary,
		config: LedgerStateConfig{CardanoNodeConfig: nodeConfig},
	}
	ls.slotClock = NewSlotClock(
		newMockSlotTimeProvider(time.Unix(0, 0), time.Second, 100),
		DefaultSlotClockConfig(),
	)
	state, err := ls.byronPBFTStateAtTip(context.Background(), ocommon.Tip{})
	require.NoError(t, err)

	parent := envelopeParent{origin: true}
	for _, block := range blocks {
		require.NoError(
			t,
			validateInboundBlockEnvelope(block, nil, nodeConfig, parent),
			"envelope of preprod Byron block at slot %d",
			block.SlotNumber(),
		)
		state, err = ls.advanceByronPBFTState(state, block, true)
		require.NoError(
			t,
			err,
			"apply preprod Byron block at slot %d",
			block.SlotNumber(),
		)
		require.NoError(t, primary.AddBlock(block, nil))
		parent = envelopeParentFromBlock(block)
	}
	require.Equal(t, uint64(84_242), parent.slot)
	require.Equal(t, uint64(45), parent.blockNumber)
}
