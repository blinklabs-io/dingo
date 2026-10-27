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
	"bytes"
	"context"
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestPeerHeaderForkPathTrimsOnlyAtExactAppliedPoint(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		appliedIndex int
		exact        bool
	}{
		{"exact applied point", 0, true},
		{"applied cached head", 2, true},
		{"same slot different hash", 0, false},
	} {
		samePoint := tc.exact
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newForkPathFreshnessFixture(t, 0)
			ls := f.ls
			conn := ouroboros.ConnectionId{}
			anchor := ls.currentTip
			nonce := bytes.Repeat([]byte{0x31}, 32)
			previous := anchor.Point.Hash
			var blocks []*testBlockResult
			var headers []gledger.BlockHeader
			for index := range 4 {
				number := anchor.BlockNumber + uint64(index) + 1
				block := createTestBlockWithNonceAtSlotsAndBlockNumber(
					t,
					[32]byte{byte(91 + index)},
					nonce,
					uint64(
						100+index*100,
					),
					uint64(101+index*100),
					previous,
					&number,
					tamperNone,
				)
				blocks = append(blocks, block)
				headers = append(headers, block.block.Header())
				previous = block.block.Hash().Bytes()
			}
			stamped := ls.CurrentHeaderCryptoVerificationGeneration()
			records := make(map[string]gledger.BlockHeader)
			for _, header := range headers {
				records[string(header.Hash().Bytes())] = header
			}
			ls.config.PeerHeaderLookupFunc = func(
				_ ouroboros.ConnectionId,
				hash []byte,
			) (ChainsyncEvent, []byte, bool) {
				header, ok := records[string(hash)]
				if !ok {
					return ChainsyncEvent{}, nil, false
				}
				return ChainsyncEvent{
						Point: ocommon.NewPoint(
							header.SlotNumber(),
							hash,
						),
						BlockHeader:                        header,
						HeaderCryptoVerificationGeneration: stamped,
					}, header.PrevHash().
						Bytes(),
					true
			}
			_, path, found, err := ls.peerHeaderForkPath(conn, headers[2])
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, path, 3)
			applied := blocks[tc.appliedIndex]
			if !samePoint {
				number := anchor.BlockNumber + 1
				applied = createTestBlockWithNonceAtSlotsAndBlockNumber(
					t,
					[32]byte{99},
					nonce,
					headers[0].SlotNumber(),
					headers[0].SlotNumber()+1,
					anchor.Point.Hash,
					&number,
					tamperNone,
				)
				require.Equal(
					t,
					headers[0].SlotNumber(),
					applied.block.SlotNumber(),
				)
			}
			if samePoint {
				for _, block := range blocks[:tc.appliedIndex+1] {
					require.NoError(t, ls.chain.AddLocalBlock(t.Context(), block.block))
				}
			} else {
				require.NoError(t, ls.chain.AddLocalBlock(t.Context(), applied.block))
			}
			ls.currentTip = ochainsync.Tip{
				Point: ocommon.NewPoint(
					applied.block.SlotNumber(),
					applied.block.Hash().Bytes(),
				),
				BlockNumber: applied.block.BlockNumber(),
			}
			ls.publishSnapshotsLocked()
			if samePoint {
				for _, header := range headers[:tc.appliedIndex+1] {
					delete(records, string(header.Hash().Bytes()))
				}
			}
			intersection, path, found, err := ls.peerHeaderForkPath(
				conn,
				headers[3],
			)
			require.NoError(t, err)
			require.True(t, found)
			if samePoint {
				require.Equal(
					t,
					ls.currentTip.Point,
					intersection,
					"the cached anchor must advance to the exact applied point",
				)
				require.Equal(
					t,
					headers[tc.appliedIndex+1:],
					path,
					"applied prefix must no longer depend on evicted peer records",
				)
				require.NoError(
					t,
					ls.verifyPeerForkTargetEpochAncestors(
						conn,
						path,
						ls.currentEpoch,
						nonce,
						ls.loadConsensusSnapshot(),
					),
				)
				gotNonce, epoch, fork, err := ls.peerChainSelectionEpochNonce(
					conn,
					headers[3],
					ls.loadConsensusSnapshot(),
				)
				require.NoError(t, err)
				require.True(t, fork)
				require.Equal(t, ls.currentEpoch.EpochId, epoch.EpochId)
				require.Equal(t, nonce, gotNonce)

			} else {
				require.Equal(
					t,
					anchor.Point,
					intersection,
					"a matching slot on another fork must not change the anchor",
				)
				require.Equal(t, headers, path)
			}
		})
	}
}

func TestTrimPeerForkPathPreservesCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	ls := &LedgerState{}
	_, err := ls.trimPeerForkPathAtAppliedTip(
		ctx,
		"peer",
		peerForkPathPrefix{},
		1,
	)
	require.ErrorIs(t, err, context.Canceled)
}
