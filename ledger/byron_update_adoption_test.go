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
	"time"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestByronAdoptedUpdateChangesSizeLimitsAtAdoptionPoint covers the
// criterion that an adopted update's ppMaxBlockSize and ppMaxHeaderSize
// govern inbound regular-block validation from the adoption point. A real
// proposal is registered, voted, endorsed and stable through the update
// state; with k = 10 the epoch is 100 slots, so it is adopted by the first
// block of epoch 1. The block in the last slot of epoch 0 is measured against
// the limits before adoption and the first block of epoch 1 against the
// adopted ones, using the parameters block application takes from the state.
func TestByronAdoptedUpdateChangesSizeLimitsAtAdoptionPoint(t *testing.T) {
	t.Parallel()
	const (
		protocolMagic = uint32(44)
		securityParam = 10
		wide          = uint64(2_000_000)
	)
	issuer := newByronPBFTTestKey(0x91)
	delegate := newByronPBFTTestKey(0x92)
	certificate := newSignedByronPBFTDelegationCertificate(
		t, protocolMagic, 0, issuer, delegate,
	)
	template := loadRealByronMainBlock(t)
	newBlock := func(
		epoch, slot, number uint64,
		payload []byte,
		version byron.ByronBlockVersion,
	) *byron.ByronMainBlock {
		return newSignedByronPBFTBlockWithBody(
			t, template, protocolMagic, epoch, slot, number,
			lcommon.Blake2b256{}, issuer, delegate, certificate, nil,
			&byronPBFTBodyOverride{
				emptyTransactions: true,
				updatePayload:     payload,
				blockVersion:      &version,
			},
		)
	}
	current := byron.ByronBlockVersion{}
	adoptedVersion := byron.ByronBlockVersion{Minor: 1}
	lastBefore := newBlock(0, 99, 4, byronUpdatePayload(nil), current)
	firstAfter := newBlock(1, 0, 4, byronUpdatePayload(nil), current)
	blockSizes := [2]uint64{
		uint64(len(lastBefore.Cbor())), uint64(len(firstAfter.Cbor())),
	}
	headerSizes := [2]uint64{
		uint64(len(lastBefore.Header().Cbor())),
		uint64(len(firstAfter.Header().Cbor())),
	}
	lowest := func(sizes [2]uint64) uint64 { return min(sizes[0], sizes[1]) }
	highest := func(sizes [2]uint64) uint64 { return max(sizes[0], sizes[1]) }
	ptr := func(v uint64) *uint64 { return &v }

	tests := []struct {
		name string
		// genesis and adopted are {maxBlockSize, maxHeaderSize}.
		genesis, adopted [2]uint64
		// beforeErr and afterErr are the substrings the last block of
		// epoch 0 and the first block of epoch 1 must be rejected with, or
		// empty when they must be accepted.
		beforeErr, afterErr string
	}{
		{
			"lower the block limit below the block",
			[2]uint64{wide, wide},
			[2]uint64{blockSizes[1] - 1, wide},
			"", "exceeds maxBlockSize",
		},
		{
			"lower the block limit to exactly the block",
			[2]uint64{wide, wide},
			[2]uint64{blockSizes[1], wide},
			"", "",
		},
		{
			"lower the header limit below the header",
			[2]uint64{wide, wide},
			[2]uint64{wide, headerSizes[1] - 1},
			"", "exceeds maxHeaderSize",
		},
		{
			"lower the header limit to exactly the header",
			[2]uint64{wide, wide},
			[2]uint64{wide, headerSizes[1]},
			"", "",
		},
		{
			"raise the block limit to admit the block",
			[2]uint64{lowest(blockSizes) - 1, wide},
			[2]uint64{highest(blockSizes), wide},
			"exceeds maxBlockSize", "",
		},
		{
			"raise the header limit to admit the header",
			[2]uint64{wide, lowest(headerSizes) - 1},
			[2]uint64{wide, highest(headerSizes)},
			"exceeds maxHeaderSize", "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			nodeConfig := newGeneratedByronPBFTTestNodeConfig(
				t, protocolMagic, securityParam, issuer, delegate, certificate,
			)
			limits := &nodeConfig.ByronGenesis().BlockVersionData
			limits.MaxBlockSize = int(test.genesis[0])
			limits.MaxHeaderSize = int(test.genesis[1])
			// A transaction and a proposal must fit the smallest limits.
			limits.MaxTxSize = 100
			limits.MaxProposalSize = 700
			ls := &LedgerState{
				config: LedgerStateConfig{CardanoNodeConfig: nodeConfig},
			}
			ls.slotClock = NewSlotClock(
				newMockSlotTimeProvider(time.Unix(0, 0), time.Second, 100),
				DefaultSlotClockConfig(),
			)
			config, err := ls.byronPBFTConfig()
			require.NoError(t, err)
			genesisParams, err := ls.byronGenesisProtocolParameters()
			require.NoError(t, err)
			state, err := newByronPBFTState(config, genesisParams)
			require.NoError(t, err)
			state.update = state.update.Advance(0, 0)

			proposal, proposalId := newByronUpdateProposal(
				t, protocolMagic, delegate, [3]uint64{0, 1, 0},
				ptr(test.adopted[0]), ptr(test.adopted[1]),
			)
			vote := newByronUpdateVote(
				t, protocolMagic, delegate, proposalId, false,
			)
			// Header validation is off: one genesis key signs every block
			// here and would exceed the PBFT signature window. A rejected
			// update payload is then only logged, so the adopted version
			// asserted below is what shows the proposal registered.
			// Registered and confirmed in epoch 0, endorsed once confirmation
			// is 2k slots old, and stable for 4k slots by the next epoch.
			for _, block := range []*byron.ByronMainBlock{
				newBlock(0, 6, 1, byronUpdatePayload(proposal), current),
				newBlock(0, 7, 2, byronUpdatePayload(nil, vote), current),
				newBlock(0, 30, 3, byronUpdatePayload(nil), adoptedVersion),
			} {
				state, err = ls.advanceByronPBFTState(state, block, false)
				require.NoError(t, err)
			}
			require.Zero(t, state.update.AdoptedVersion().Minor)

			check := func(
				block *byron.ByronMainBlock,
				wantVersion uint16,
				wantErr string,
			) {
				t.Helper()
				next, err := ls.advanceByronPBFTState(state, block, false)
				require.NoError(t, err)
				require.Equal(t, wantVersion, next.update.AdoptedVersion().Minor)
				params, ok := byronBlockPParams(block, next, nil).(*eras.ByronProtocolParameters)
				require.True(t, ok)
				err = validateByronBlockSizes(block, params, nodeConfig)
				if wantErr == "" {
					require.NoError(t, err)
					return
				}
				require.ErrorContains(t, err, wantErr)
			}
			check(lastBefore, 0, test.beforeErr)
			check(firstAfter, 1, test.afterErr)
		})
	}
}
