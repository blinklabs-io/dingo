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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
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

// TestByronUpdateStateReplaysPreprodByronChain replays the complete preprod
// Byron chain through the update, delegation and PBFT transitions. Preprod
// leaves Byron by TriggerAtVersion, so the chain fixes when the reference
// adopted each version: the proposal for 2.0.0 at slot 43211 is only
// registrable once 1.0.0 is adopted, Byron blocks still exist in epoch 3,
// and Shelley starts at epoch 4. Counting an endorsement made before its
// proposal is 2k-stable, or confirming with the wrong threshold, moves an
// adoption to a different epoch or rejects a block of this chain.
func TestByronUpdateStateReplaysPreprodByronChain(t *testing.T) {
	t.Parallel()

	blocks := loadPreprodByronChain(t)
	nodeConfig, err := cardano.NewCardanoNodeConfigFromEmbedFS(
		cardano.EmbeddedConfigFS,
		"preprod/config.json",
	)
	require.NoError(t, err)
	ls := &LedgerState{config: LedgerStateConfig{
		CardanoNodeConfig: nodeConfig,
	}}
	ls.slotClock = NewSlotClock(
		newMockSlotTimeProvider(time.Unix(0, 0), time.Second, 100),
		DefaultSlotClockConfig(),
	)
	config, err := ls.byronPBFTConfig()
	require.NoError(t, err)
	state, err := newByronPBFTState(
		config,
		nodeConfig.ByronGenesis().BlockVersionData,
	)
	require.NoError(t, err)

	adoptedAtEpoch := make(map[uint64]byron.ByronBlockVersion)
	for _, block := range blocks {
		state, err = ls.advanceByronPBFTState(state, block, true)
		require.NoError(t, err, "apply preprod Byron block at slot %d", block.SlotNumber())
		epoch := block.SlotNumber() / config.SlotsPerEpoch
		if _, seen := adoptedAtEpoch[epoch]; !seen {
			adoptedAtEpoch[epoch] = state.updateState.protocolVersion
		}
	}
	require.Equal(t, map[uint64]byron.ByronBlockVersion{
		0: {Major: 0},
		1: {Major: 0},
		2: {Major: 1},
		3: {Major: 1},
	}, adoptedAtEpoch)

	shelley, err := state.updateState.advanceEpoch(
		4,
		4*config.SlotsPerEpoch,
		config.SecurityParam,
	)
	require.NoError(t, err)
	require.Equal(t, byron.ByronBlockVersion{Major: 2}, shelley.protocolVersion)
}

// TestAdvanceByronPBFTStateEBBDoesNotTickUpdateState pins that only main
// blocks run the update epoch transition: the reference's EBB rule leaves
// cvsLastSlot unchanged, so the first main block of a later epoch, not the
// EBB, decides which candidate is stable.
func TestAdvanceByronPBFTStateEBBDoesNotTickUpdateState(t *testing.T) {
	t.Parallel()

	blocks := loadPreprodByronChain(t)
	nodeConfig, err := cardano.NewCardanoNodeConfigFromEmbedFS(
		cardano.EmbeddedConfigFS,
		"preprod/config.json",
	)
	require.NoError(t, err)
	ls := &LedgerState{config: LedgerStateConfig{
		CardanoNodeConfig: nodeConfig,
	}}
	config, err := ls.byronPBFTConfig()
	require.NoError(t, err)
	state, err := newByronPBFTState(
		config,
		nodeConfig.ByronGenesis().BlockVersionData,
	)
	require.NoError(t, err)
	state.updateState.candidates = []byronProtocolAdoption{{
		slot:    0,
		version: byron.ByronBlockVersion{Major: 1},
		params:  state.updateState.params,
	}}

	ebb, ok := blocks[0].(*byron.ByronEpochBoundaryBlock)
	require.True(t, ok)
	laterEbb := *ebb
	header := *ebb.BlockHeader
	header.ConsensusData.Epoch = 3
	laterEbb.BlockHeader = &header
	next, err := ls.advanceByronPBFTState(state, &laterEbb, false)
	require.NoError(t, err)
	require.Equal(t, byron.ByronBlockVersion{}, next.updateState.protocolVersion)
	require.Equal(t, uint64(0), next.updateState.lastEpoch)
	require.Len(t, next.updateState.candidates, 1)
}
