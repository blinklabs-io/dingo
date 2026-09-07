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

package chain_test

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestStandaloneBlockAddsDrainQueuedHeaderEvents is the regression test for the
// standalone block-add paths leaving the chain-level sequencer undrained.
//
// AddBlockHeader enqueues an announcing header's event on that sequencer
// rather than publishing it inline, and addBlockLocked enqueues the
// invalidation for the queued headers a new block discards. AddLocalBlock
// drains before publishing its block; AddBlock and AddBlockWithPoint did not,
// and the local forge path reaches the chain through AddBlock
// (ledger.forgeBlock). So a forged block could discard a queued announcement
// and publish nothing to say so, leaving the vote manager holding a vote armed
// for a ranking block that had just left the chain -- the invalidation is keyed
// by announcing ranking block, so nothing else retracts it.
func TestStandaloneBlockAddsDrainQueuedHeaderEvents(t *testing.T) {
	for _, tc := range []struct {
		name string
		add  func(t *testing.T, c *chain.Chain, block ledger.Block)
	}{
		{
			name: "AddBlock",
			add: func(t *testing.T, c *chain.Chain, b ledger.Block) {
				require.NoError(t, c.AddBlock(b, nil))
			},
		},
		{
			name: "AddBlockWithPoint",
			add: func(t *testing.T, c *chain.Chain, b ledger.Block) {
				require.NoError(t, c.AddBlockWithPoint(
					b,
					ocommon.Point{
						Slot: b.SlotNumber(),
						Hash: b.Hash().Bytes(),
					},
					nil,
				))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, bus := newHeaderStreamChain(t)

			blocks, err := testfixtures.GenerateConwayChain(1)
			require.NoError(t, err)
			require.Len(t, blocks, 1)

			// A queued header announcing an endorser block. Its event is
			// enqueued on the sequencer, not published.
			// The header is the block's own: the chain requires an added
			// block to match the first pending header, and this is the
			// sequence the finding describes -- an announcing header
			// admitted, then applied.
			require.NoError(t, c.AddBlockHeader(announcingStreamHeader{
				headerStreamHeader: headerStreamHeader{
					hash:        blocks[0].Hash(),
					prevHash:    blocks[0].PrevHash(),
					blockNumber: blocks[0].BlockNumber(),
					slot:        blocks[0].SlotNumber(),
				},
				ebHash:    lcommon.NewBlake2b256([]byte("announced-eb")),
				ebSize:    4096,
				announces: true,
			}))

			// Subscribing after the enqueue is deliberate: a deferred event
			// is delivered on the drain, so a subscriber attached now must
			// still receive it.
			subId, headerCh := bus.Subscribe(chain.ChainHeaderEventType)
			defer bus.Unsubscribe(chain.ChainHeaderEventType, subId)

			tc.add(t, c, blocks[0])

			select {
			case <-headerCh:
			case <-time.After(5 * time.Second):
				t.Fatal(
					"the block add left the queued header event on the " +
						"sequencer; the vote manager never sees it",
				)
			}
		})
	}
}

// TestHeaderAnnouncementAppliedDistinguishesLocalBlocksFromHeaderArrivals pins
// the source indicator on the announcement.
//
// The event documents itself as a header-arrival signal whose block "has not
// been fetched, validated or applied". That holds for a peer header entering
// the queue, but a locally forged block never passes through the queue, so its
// announcement is emitted from the block add -- by which point the block is
// validated and on the chain. Both arrive on one event type, so without a
// discriminator a consumer cannot tell an applied local block from a
// provisional peer header and would have to treat the stronger case as the
// weaker one.
func TestHeaderAnnouncementAppliedDistinguishesLocalBlocksFromHeaderArrivals(
	t *testing.T,
) {
	t.Run("peer header arrival is provisional", func(t *testing.T) {
		c, bus := newHeaderStreamChain(t)
		subId, headerCh := bus.Subscribe(chain.ChainHeaderEventType)
		defer bus.Unsubscribe(chain.ChainHeaderEventType, subId)

		require.NoError(t, c.AddBlockHeader(announcingStreamHeader{
			headerStreamHeader: headerStreamHeader{
				hash:        lcommon.NewBlake2b256([]byte("peer-hdr")),
				prevHash:    lcommon.NewBlake2b256(nil),
				blockNumber: 1,
				slot:        10,
			},
			ebHash:    lcommon.NewBlake2b256([]byte("peer-eb")),
			ebSize:    4096,
			announces: true,
		}))
		c.PublishPendingChainUpdates()

		announcement := nextAnnouncement(t, headerCh)
		require.False(
			t,
			announcement.Applied,
			"a queued peer header names a block that has not been applied",
		)
	})

	t.Run("locally forged block is applied", func(t *testing.T) {
		c, bus := newHeaderStreamChain(t)
		subId, headerCh := bus.Subscribe(chain.ChainHeaderEventType)
		defer bus.Unsubscribe(chain.ChainHeaderEventType, subId)

		const localHash = "00000000000000000000000000000000" +
			"000000000000000000000000000000ff"
		require.NoError(t, c.AddLocalBlock(announcingStreamBlock{
			MockBlock: &MockBlock{
				MockBlockNumber: 1,
				MockSlot:        11,
				MockHash:        localHash,
			},
			header: announcingStreamHeader{
				headerStreamHeader: headerStreamHeader{
					hash:        lcommon.NewBlake2b256([]byte("local-blk")),
					prevHash:    lcommon.NewBlake2b256(nil),
					blockNumber: 1,
					slot:        11,
				},
				ebHash:    lcommon.NewBlake2b256([]byte("local-eb")),
				ebSize:    2048,
				announces: true,
			},
		}))

		announcement := nextAnnouncement(t, headerCh)
		require.True(
			t,
			announcement.Applied,
			"a locally forged block is on the chain when its announcement publishes",
		)
	})
}

func nextAnnouncement(
	t *testing.T,
	ch <-chan event.Event,
) chain.ChainHeaderAnnouncementEvent {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case evt := <-ch:
			if a, ok := evt.Data.(chain.ChainHeaderAnnouncementEvent); ok {
				return a
			}
		case <-deadline:
			t.Fatal("expected a header announcement event")
		}
	}
}

// announcingStreamBlock is a MockBlock whose header announces a Leios endorser
// block, so AddLocalBlock exercises the local-block announcement path.
type announcingStreamBlock struct {
	*MockBlock
	header announcingStreamHeader
}

func (b announcingStreamBlock) Header() ledger.BlockHeader {
	return b.header
}
