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
			require.NoError(t, c.AddVerifiedBlockHeader(announcingStreamHeader{
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

// TestHeaderAnnouncementRequiresCryptoVerifiedHeader is the regression test
// for arming a Leios vote from a header nobody authenticated.
//
// chainsync admits a roll-forward header through AddBlockHeader, not
// AddVerifiedBlockHeader, whenever chainsyncHeaderCryptoPolicy declines to
// verify it now: the epoch nonce for its slot is not cached, so VRF/KES
// verification is deferred to blockfetch. Publishing an announcement for such
// a header lets any chainsync peer make this node sign and publish a BLS vote
// for a ranking block it never checked. That vote then occupies the
// (slot, voterId) pair, so the honest block's vote for the same slot is
// refused as a duplicate -- the same "seated member does not vote" outcome the
// header stream exists to fix, handed to a peer.
//
// The verified header must still announce: that is the path this PR adds, and
// gating it away would restore the missed votes.
func TestHeaderAnnouncementRequiresCryptoVerifiedHeader(t *testing.T) {
	announcing := func(tag string, slot uint64) announcingStreamHeader {
		return announcingStreamHeader{
			headerStreamHeader: headerStreamHeader{
				hash:        lcommon.NewBlake2b256([]byte(tag)),
				prevHash:    lcommon.NewBlake2b256(nil),
				blockNumber: 1,
				slot:        slot,
			},
			ebHash:    lcommon.NewBlake2b256([]byte(tag + "-eb")),
			ebSize:    4096,
			announces: true,
		}
	}

	t.Run("unverified queued header does not announce", func(t *testing.T) {
		c, bus := newHeaderStreamChain(t)
		subId, headerCh := bus.Subscribe(chain.ChainHeaderEventType)
		defer bus.Unsubscribe(chain.ChainHeaderEventType, subId)

		require.NoError(t, c.AddBlockHeader(announcing("unverified", 10)))
		c.PublishPendingChainUpdates()

		requireNoAnnouncement(t, headerCh)
	})

	t.Run("verified header announces", func(t *testing.T) {
		c, bus := newHeaderStreamChain(t)
		subId, headerCh := bus.Subscribe(chain.ChainHeaderEventType)
		defer bus.Unsubscribe(chain.ChainHeaderEventType, subId)

		header := announcing("verified", 11)
		require.NoError(t, c.AddVerifiedBlockHeader(header))
		c.PublishPendingChainUpdates()

		announcement := nextAnnouncement(t, headerCh)
		require.Equal(t, uint64(11), announcement.Slot)
		require.Equal(t, header.Hash(), announcement.RbHash)
		require.Equal(t, header.ebHash, announcement.EbHash)
		require.NotZero(t, announcement.Seq)
	})

	// A locally forged block never passes through the header queue, so the
	// gate must not reach it: its announcement is emitted from the block add,
	// by which point the block is validated and on our chain.
	t.Run("locally forged block still announces", func(t *testing.T) {
		c, bus := newHeaderStreamChain(t)
		subId, headerCh := bus.Subscribe(chain.ChainHeaderEventType)
		defer bus.Unsubscribe(chain.ChainHeaderEventType, subId)

		const localHash = "00000000000000000000000000000000" +
			"000000000000000000000000000000ff"
		header := announcing("local-blk", 12)
		require.NoError(t, c.AddLocalBlock(announcingStreamBlock{
			MockBlock: &MockBlock{
				MockBlockNumber: 1,
				MockSlot:        12,
				MockHash:        localHash,
			},
			header: header,
		}))

		announcement := nextAnnouncement(t, headerCh)
		require.Equal(t, uint64(12), announcement.Slot)
		require.Equal(t, header.ebHash, announcement.EbHash)
	})
}

// requireNoAnnouncement fails if any announcement reaches the stream. It
// drains other header-stream events (invalidations) rather than accepting the
// first event as proof, so it cannot pass for the wrong reason.
func requireNoAnnouncement(t *testing.T, ch <-chan event.Event) {
	t.Helper()
	deadline := time.After(500 * time.Millisecond)
	for {
		select {
		case evt := <-ch:
			if a, ok := evt.Data.(chain.ChainHeaderAnnouncementEvent); ok {
				t.Fatalf(
					"unverified header announced eb %s at slot %d; a peer can arm a vote for a block we never authenticated",
					a.EbHash.String(),
					a.Slot,
				)
			}
		case <-deadline:
			return
		}
	}
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
