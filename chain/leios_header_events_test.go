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
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	utxorpc "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

// headerStreamHeader is a minimal ranking-block header that does not implement
// the Leios announcement interface at all, which is how a pre-Leios era header
// behaves. announcingStreamHeader embeds it to add the interface.
type headerStreamHeader struct {
	hash        lcommon.Blake2b256
	prevHash    lcommon.Blake2b256
	blockNumber uint64
	slot        uint64
}

func (h headerStreamHeader) Hash() lcommon.Blake2b256          { return h.hash }
func (h headerStreamHeader) PrevHash() lcommon.Blake2b256      { return h.prevHash }
func (h headerStreamHeader) BlockNumber() uint64               { return h.blockNumber }
func (h headerStreamHeader) SlotNumber() uint64                { return h.slot }
func (h headerStreamHeader) IssuerVkey() lcommon.IssuerVkey    { return lcommon.IssuerVkey{} }
func (h headerStreamHeader) BlockBodySize() uint64             { return 0 }
func (h headerStreamHeader) Era() lcommon.Era                  { return conway.EraConway }
func (h headerStreamHeader) Cbor() []byte                      { return nil }
func (h headerStreamHeader) BlockBodyHash() lcommon.Blake2b256 { return lcommon.Blake2b256{} }

type announcingStreamHeader struct {
	headerStreamHeader
	ebHash    lcommon.Blake2b256
	ebSize    uint64
	announces bool
}

func (h announcingStreamHeader) LeiosAnnouncement() (
	lcommon.Blake2b256,
	uint64,
	bool,
) {
	return h.ebHash, h.ebSize, h.announces
}

func newHeaderStreamChain(t *testing.T) (*chain.Chain, *event.EventBus) {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	cm, err := chain.NewManager(nil, bus)
	require.NoError(t, err)
	c := cm.PrimaryChain()
	require.NotNil(t, c)
	return c, bus
}

// TestAddBlockHeaderQueuesLeiosAnnouncement pins the header-arrival signal at
// its source: the announcement is surfaced when the header enters the queue,
// not when the block it belongs to applies. Applying an EB-announcing ranking
// block waits on fetching that same endorser block, which lands after the
// Leios vote window has closed.
func TestAddBlockHeaderQueuesLeiosAnnouncement(t *testing.T) {
	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	base := headerStreamHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-1")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        577,
	}
	for _, tc := range []struct {
		name        string
		header      interface{ Hash() lcommon.Blake2b256 }
		wantPublish bool
	}{
		{
			name: "announcing header",
			header: announcingStreamHeader{
				headerStreamHeader: base,
				ebHash:             ebHash,
				ebSize:             4096,
				announces:          true,
			},
			wantPublish: true,
		},
		{
			name: "announcement-capable header announcing nothing",
			header: announcingStreamHeader{
				headerStreamHeader: base,
				announces:          false,
			},
		},
		{
			name:   "pre-leios header",
			header: base,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, bus := newHeaderStreamChain(t)
			subId, ch := bus.Subscribe(chain.ChainHeaderEventType)
			defer bus.Unsubscribe(chain.ChainHeaderEventType, subId)

			switch h := tc.header.(type) {
			case announcingStreamHeader:
				require.NoError(t, c.AddBlockHeader(h))
			case headerStreamHeader:
				require.NoError(t, c.AddBlockHeader(h))
			default:
				t.Fatalf("unexpected header type %T", h)
			}
			c.PublishPendingChainUpdates()

			if !tc.wantPublish {
				testutil.RequireNoReceive(
					t,
					ch,
					300*time.Millisecond,
					"no header announcement expected",
				)
				return
			}
			evt := testutil.RequireReceive(
				t, ch, 2*time.Second, "header announcement",
			)
			data, ok := evt.Data.(chain.ChainHeaderAnnouncementEvent)
			require.True(t, ok)
			assert.Equal(t, uint64(577), data.Slot)
			assert.Equal(t, base.hash, data.RbHash)
			assert.Equal(t, ebHash, data.EbHash)
			assert.Equal(t, uint64(4096), data.EbSize)
			assert.NotZero(t, data.Seq)
		})
	}
}

// TestClearHeadersQueuesHeaderInvalidation covers the case where an admitted
// announcing header is discarded without ever becoming a block: blockfetch
// startup fails, the queue is cleared, and no rollback is published because no
// block was ever added. Without the invalidation, the announcement would
// outlive the header and a vote could be cast for a ranking block that is not
// on our chain.
func TestClearHeadersQueuesHeaderInvalidation(t *testing.T) {
	c, bus := newHeaderStreamChain(t)
	subId, ch := bus.Subscribe(chain.ChainHeaderEventType)
	defer bus.Unsubscribe(chain.ChainHeaderEventType, subId)

	require.NoError(t, c.AddBlockHeader(announcingStreamHeader{
		headerStreamHeader: headerStreamHeader{
			hash:        lcommon.NewBlake2b256([]byte("hdr-1")),
			prevHash:    lcommon.NewBlake2b256(nil),
			blockNumber: 1,
			slot:        577,
		},
		ebHash:    lcommon.NewBlake2b256([]byte("announced-eb")),
		ebSize:    4096,
		announces: true,
	}))
	c.ClearHeaders()
	c.PublishPendingChainUpdates()

	announcement := testutil.RequireReceive(
		t, ch, 2*time.Second, "header announcement",
	)
	announced, ok := announcement.Data.(chain.ChainHeaderAnnouncementEvent)
	require.True(t, ok)

	invalidation := testutil.RequireReceive(
		t, ch, 2*time.Second, "header invalidation",
	)
	invalid, ok := invalidation.Data.(chain.ChainHeaderInvalidationEvent)
	require.True(t, ok)
	assert.Equal(t, chain.HeaderInvalidationQueueCleared, invalid.Reason)
	// Everything above the block tip was queued headers and is now gone.
	assert.Equal(t, c.Tip().Point.Slot, invalid.Point.Slot)
	assert.Greater(
		t,
		invalid.Seq,
		announced.Seq,
		"the invalidation must be sequenced after the announcement it voids",
	)

	// A cleared-but-empty queue publishes nothing.
	c.ClearHeaders()
	c.PublishPendingChainUpdates()
	testutil.RequireNoReceive(
		t,
		ch,
		300*time.Millisecond,
		"clearing an empty header queue publishes nothing",
	)
}

// TestRollbackToQueuedHeaderInvalidatesLaterHeaders covers the rollback branch
// that resolves its point inside the header queue. It drops the later queued
// headers and returns before any block-level work, so it produces no
// ChainRollbackEvent at all -- nothing else would ever void the announcements
// those headers carried.
func TestRollbackToQueuedHeaderInvalidatesLaterHeaders(t *testing.T) {
	c, bus := newHeaderStreamChain(t)
	subId, ch := bus.Subscribe(chain.ChainHeaderEventType)
	defer bus.Unsubscribe(chain.ChainHeaderEventType, subId)

	first := announcingStreamHeader{
		headerStreamHeader: headerStreamHeader{
			hash:        lcommon.NewBlake2b256([]byte("hdr-1")),
			prevHash:    lcommon.NewBlake2b256(nil),
			blockNumber: 1,
			slot:        577,
		},
		ebHash:    lcommon.NewBlake2b256([]byte("eb-1")),
		ebSize:    4096,
		announces: true,
	}
	second := announcingStreamHeader{
		headerStreamHeader: headerStreamHeader{
			hash:        lcommon.NewBlake2b256([]byte("hdr-2")),
			prevHash:    first.hash,
			blockNumber: 2,
			slot:        578,
		},
		ebHash:    lcommon.NewBlake2b256([]byte("eb-2")),
		ebSize:    4096,
		announces: true,
	}
	require.NoError(t, c.AddBlockHeader(first))
	require.NoError(t, c.AddBlockHeader(second))
	c.PublishPendingChainUpdates()
	for range 2 {
		testutil.RequireReceive(t, ch, 2*time.Second, "announcement")
	}

	// Roll back to the first queued header. The second is dropped; no block
	// was ever added, so no chain.update is produced.
	require.NoError(t, c.Rollback(ocommon.NewPoint(
		first.slot,
		first.hash.Bytes(),
	)))
	require.Equal(t, 1, c.HeaderCount())

	evt := testutil.RequireReceive(
		t, ch, 2*time.Second, "invalidation for the dropped queued headers",
	)
	invalid, ok := evt.Data.(chain.ChainHeaderInvalidationEvent)
	require.True(t, ok)
	assert.Equal(t, chain.HeaderInvalidationRollback, invalid.Reason)
	assert.Equal(t, first.slot, invalid.Point.Slot)
	assert.NotZero(t, invalid.Seq)

	// Rolling back to the queue tip drops nothing and publishes nothing.
	require.NoError(t, c.Rollback(ocommon.NewPoint(
		first.slot,
		first.hash.Bytes(),
	)))
	testutil.RequireNoReceive(
		t,
		ch,
		300*time.Millisecond,
		"a rollback that drops no header publishes no invalidation",
	)
}

// TestRollbackInvalidationRoutedThroughSequencer pins the routing, not just
// the eventual delivery. The invalidation must go on the chain-level sequencer
// in both rollback modes and must never be handed back to the caller for
// inline publication: inline, it bypasses the sequencer and can be published
// ahead of an announcement that was queued before it, which is exactly the
// inversion the single ordered stream exists to prevent.
func TestRollbackInvalidationRoutedThroughSequencer(t *testing.T) {
	c, bus := newHeaderStreamChain(t)
	subId, ch := bus.Subscribe(chain.ChainHeaderEventType)
	defer bus.Unsubscribe(chain.ChainHeaderEventType, subId)

	first := announcingStreamHeader{
		headerStreamHeader: headerStreamHeader{
			hash:        lcommon.NewBlake2b256([]byte("hdr-1")),
			prevHash:    lcommon.NewBlake2b256(nil),
			blockNumber: 1,
			slot:        577,
		},
		ebHash:    lcommon.NewBlake2b256([]byte("eb-1")),
		ebSize:    4096,
		announces: true,
	}
	second := announcingStreamHeader{
		headerStreamHeader: headerStreamHeader{
			hash:        lcommon.NewBlake2b256([]byte("hdr-2")),
			prevHash:    first.hash,
			blockNumber: 2,
			slot:        578,
		},
		ebHash:    lcommon.NewBlake2b256([]byte("eb-2")),
		ebSize:    4096,
		announces: true,
	}

	// Two real blocks, so the rollback below takes the block-removal path
	// (the one that builds the caller-published event slice) rather than
	// the queued-header early return.
	blocks, err := testfixtures.GenerateConwayChain(2)
	require.NoError(t, err)
	require.Len(t, blocks, 2)
	for i := range blocks {
		_, addErr := c.AddBlockWithPointDeferred(blocks[i], ocommon.Point{
			Slot: blocks[i].SlotNumber(),
			Hash: blocks[i].Hash().Bytes(),
		}, nil)
		require.NoError(t, addErr)
	}
	c.PublishPendingChainUpdates()
	require.Equal(t, blocks[1].SlotNumber(), c.Tip().Point.Slot)

	// Chain the announcing headers onto the block tip, and leave them
	// undrained on the sequencer.
	first.prevHash = blocks[1].Hash()
	first.blockNumber = blocks[1].BlockNumber() + 1
	first.slot = blocks[1].SlotNumber() + 1
	second.prevHash = first.hash
	second.blockNumber = first.blockNumber + 1
	second.slot = first.slot + 1
	require.NoError(t, c.AddBlockHeader(first))
	require.NoError(t, c.AddBlockHeader(second))

	evts, err := c.RollbackDeferred(ocommon.Point{
		Slot: blocks[0].SlotNumber(),
		Hash: blocks[0].Hash().Bytes(),
	})
	require.NoError(t, err)
	require.NotEmpty(
		t,
		evts,
		"the rollback must remove a block, so it produces caller-published events",
	)
	for _, evt := range evts {
		require.NotEqual(
			t,
			event.EventType(chain.ChainHeaderEventType),
			evt.Type,
			"the invalidation must not be handed back for inline publication",
		)
	}

	// Queued after the rollback. If the invalidation were published inline
	// rather than sequenced, it could land either side of this.
	third := announcingStreamHeader{
		headerStreamHeader: headerStreamHeader{
			hash:        lcommon.NewBlake2b256([]byte("hdr-3")),
			prevHash:    blocks[0].Hash(),
			blockNumber: blocks[0].BlockNumber() + 1,
			slot:        blocks[0].SlotNumber() + 1,
		},
		ebHash:    lcommon.NewBlake2b256([]byte("eb-3")),
		ebSize:    4096,
		announces: true,
	}
	require.NoError(t, c.AddBlockHeader(third))

	c.PublishPendingChainUpdates()

	// Exactly: announcement, announcement, invalidation, announcement --
	// in chain-mutation order.
	wantAnnouncements := []lcommon.Blake2b256{
		first.hash,
		second.hash,
	}
	for _, want := range wantAnnouncements {
		evt := testutil.RequireReceive(
			t, ch, 2*time.Second, "announcement before the rollback",
		)
		data, ok := evt.Data.(chain.ChainHeaderAnnouncementEvent)
		require.True(t, ok, "got %T, want an announcement", evt.Data)
		assert.Equal(t, want, data.RbHash)
	}
	evt := testutil.RequireReceive(
		t, ch, 2*time.Second, "invalidation",
	)
	invalid, ok := evt.Data.(chain.ChainHeaderInvalidationEvent)
	require.True(
		t,
		ok,
		"the invalidation must follow the announcements it voids, got %T",
		evt.Data,
	)
	assert.Equal(t, chain.HeaderInvalidationRollback, invalid.Reason)

	evt = testutil.RequireReceive(
		t, ch, 2*time.Second, "announcement after the rollback",
	)
	after, ok := evt.Data.(chain.ChainHeaderAnnouncementEvent)
	require.True(t, ok, "got %T, want an announcement", evt.Data)
	assert.Equal(t, third.hash, after.RbHash)
	assert.Greater(
		t,
		after.Seq,
		invalid.Seq,
		"a header admitted after the rollback is sequenced after it",
	)
}

// TestNonDeferredRollbackPublishesInvalidation covers the same routing through
// the non-deferred entry point, which drains the sequencer itself.
func TestNonDeferredRollbackPublishesInvalidation(t *testing.T) {
	c, bus := newHeaderStreamChain(t)
	subId, ch := bus.Subscribe(chain.ChainHeaderEventType)
	defer bus.Unsubscribe(chain.ChainHeaderEventType, subId)

	first := announcingStreamHeader{
		headerStreamHeader: headerStreamHeader{
			hash:        lcommon.NewBlake2b256([]byte("hdr-1")),
			prevHash:    lcommon.NewBlake2b256(nil),
			blockNumber: 1,
			slot:        577,
		},
		ebHash:    lcommon.NewBlake2b256([]byte("eb-1")),
		ebSize:    4096,
		announces: true,
	}
	second := announcingStreamHeader{
		headerStreamHeader: headerStreamHeader{
			hash:        lcommon.NewBlake2b256([]byte("hdr-2")),
			prevHash:    first.hash,
			blockNumber: 2,
			slot:        578,
		},
		ebHash:    lcommon.NewBlake2b256([]byte("eb-2")),
		ebSize:    4096,
		announces: true,
	}
	require.NoError(t, c.AddBlockHeader(first))
	require.NoError(t, c.AddBlockHeader(second))
	require.NoError(t, c.Rollback(ocommon.NewPoint(
		first.slot,
		first.hash.Bytes(),
	)))

	var seqs []uint64
	for range 3 {
		evt := testutil.RequireReceive(
			t, ch, 2*time.Second, "ordered header event",
		)
		switch data := evt.Data.(type) {
		case chain.ChainHeaderAnnouncementEvent:
			require.Less(
				t,
				len(seqs),
				2,
				"both announcements precede the invalidation",
			)
			seqs = append(seqs, data.Seq)
		case chain.ChainHeaderInvalidationEvent:
			require.Len(
				t,
				seqs,
				2,
				"the invalidation must not overtake the announcements it voids",
			)
			assert.Greater(t, data.Seq, seqs[1])
		default:
			t.Fatalf("unexpected payload %T", data)
		}
	}
}

// localForgedBlock is the minimum a locally forged block needs to reach
// AddLocalBlock's header-discard path. Embedding announcingStreamHeader lets a
// test forge a block that itself announces an endorser block.
type localForgedBlock struct {
	announcingStreamHeader
}

func (b localForgedBlock) Header() lcommon.BlockHeader { return b.announcingStreamHeader }
func (b localForgedBlock) Type() int                   { return 6 }
func (b localForgedBlock) Transactions() []lcommon.Transaction {
	return nil
}
func (b localForgedBlock) Utxorpc() (*utxorpc.Block, error) {
	return nil, errors.New("not implemented")
}

// TestAddLocalBlockInvalidatesDiscardedPeerHeaders covers the one header-queue
// discard that neither a rollback nor ClearHeaders produces. A locally forged
// block on the same parent as the queued peer headers discards them, and the
// chain.update it publishes is a block add, not a rollback -- so without an
// explicit invalidation nothing voids the announcements those headers carried,
// and the producer would vote for a ranking block that is not on its chain.
//
// The chain grew rather than shrank, so the discarded headers can sit at or
// below the new tip: the invalidation names them explicitly rather than
// relying on the point.
func TestAddLocalBlockInvalidatesDiscardedPeerHeaders(t *testing.T) {
	c, bus := newHeaderStreamChain(t)
	subId, ch := bus.Subscribe(chain.ChainHeaderEventType)
	defer bus.Unsubscribe(chain.ChainHeaderEventType, subId)

	blocks, err := testfixtures.GenerateConwayChain(2)
	require.NoError(t, err)
	for i := range blocks {
		_, addErr := c.AddBlockWithPointDeferred(blocks[i], ocommon.Point{
			Slot: blocks[i].SlotNumber(),
			Hash: blocks[i].Hash().Bytes(),
		}, nil)
		require.NoError(t, addErr)
	}
	c.PublishPendingChainUpdates()

	// A peer header announcing an endorser block, queued on the tip.
	peerHeader := announcingStreamHeader{
		headerStreamHeader: headerStreamHeader{
			hash:        lcommon.NewBlake2b256([]byte("peer-hdr")),
			prevHash:    blocks[1].Hash(),
			blockNumber: blocks[1].BlockNumber() + 1,
			slot:        blocks[1].SlotNumber() + 1,
		},
		ebHash:    lcommon.NewBlake2b256([]byte("peer-eb")),
		ebSize:    4096,
		announces: true,
	}
	require.NoError(t, c.AddBlockHeader(peerHeader))
	c.PublishPendingChainUpdates()
	announcement := testutil.RequireReceive(
		t, ch, 2*time.Second, "peer header announcement",
	)
	announced, ok := announcement.Data.(chain.ChainHeaderAnnouncementEvent)
	require.True(t, ok)
	assert.Equal(t, peerHeader.hash, announced.RbHash)

	// Forge on the same parent. The peer header is discarded, and the new
	// tip is deliberately at a HIGHER slot than it, so a point-based rule
	// alone would not name it.
	local := localForgedBlock{announcingStreamHeader: announcingStreamHeader{
		headerStreamHeader: headerStreamHeader{
			hash:        lcommon.NewBlake2b256([]byte("local-hdr")),
			prevHash:    blocks[1].Hash(),
			blockNumber: blocks[1].BlockNumber() + 1,
			slot:        peerHeader.slot + 10,
		},
	}}
	require.NoError(t, c.AddLocalBlock(local))
	require.Zero(t, c.HeaderCount(), "the local block discards peer headers")

	// AddLocalBlock drains the sequencer itself; no other event is needed.
	evt := testutil.RequireReceive(
		t, ch, 2*time.Second, "invalidation for the discarded peer header",
	)
	invalid, ok := evt.Data.(chain.ChainHeaderInvalidationEvent)
	require.True(t, ok, "got %T", evt.Data)
	assert.Equal(t, chain.HeaderInvalidationLocalBlock, invalid.Reason)
	assert.Contains(
		t,
		invalid.RbHashes,
		peerHeader.hash,
		"the discarded header must be named; it sits below the new tip",
	)
	assert.Greater(t, invalid.Seq, announced.Seq)
	assert.Equal(t, c.Tip().Point.Slot, invalid.Point.Slot)
	assert.NotContains(
		t,
		invalid.RbHashes,
		local.hash,
		"the forged block itself is on the chain and must not be invalidated",
	)
}

// TestAddLocalBlockNonAnnouncingPublishesNoAnnouncement covers what a forged
// block that announces nothing must and must not put on the header stream. The
// two cases are kept together because they share a setup and differ only in
// whether there are queued peer headers to discard; separating them produced
// two tests that were indistinguishable in practice.
func TestAddLocalBlockNonAnnouncingPublishesNoAnnouncement(t *testing.T) {
	newFixture := func(t *testing.T) (
		*chain.Chain,
		<-chan event.Event,
		[]lcommon.Block,
	) {
		t.Helper()
		c, bus := newHeaderStreamChain(t)
		subId, ch := bus.Subscribe(chain.ChainHeaderEventType)
		t.Cleanup(func() {
			bus.Unsubscribe(chain.ChainHeaderEventType, subId)
		})
		blocks, err := testfixtures.GenerateConwayChain(1)
		require.NoError(t, err)
		_, err = c.AddBlockWithPointDeferred(blocks[0], ocommon.Point{
			Slot: blocks[0].SlotNumber(),
			Hash: blocks[0].Hash().Bytes(),
		}, nil)
		require.NoError(t, err)
		c.PublishPendingChainUpdates()
		return c, ch, blocks
	}
	nonAnnouncingBlock := func(parent lcommon.Block) localForgedBlock {
		return localForgedBlock{
			announcingStreamHeader: announcingStreamHeader{
				headerStreamHeader: headerStreamHeader{
					hash:        lcommon.NewBlake2b256([]byte("local-hdr")),
					prevHash:    parent.Hash(),
					blockNumber: parent.BlockNumber() + 1,
					slot:        parent.SlotNumber() + 1,
				},
				announces: false,
			},
		}
	}

	t.Run("nothing to discard publishes nothing at all", func(t *testing.T) {
		c, ch, blocks := newFixture(t)
		require.NoError(t, c.AddLocalBlock(nonAnnouncingBlock(blocks[0])))
		testutil.RequireNoReceive(
			t,
			ch,
			300*time.Millisecond,
			"forging with an empty header queue and no announcement publishes nothing",
		)
	})

	t.Run("discarding headers publishes only the invalidation", func(t *testing.T) {
		c, ch, blocks := newFixture(t)
		peerHeader := announcingStreamHeader{
			headerStreamHeader: headerStreamHeader{
				hash:        lcommon.NewBlake2b256([]byte("peer-hdr")),
				prevHash:    blocks[0].Hash(),
				blockNumber: blocks[0].BlockNumber() + 1,
				slot:        blocks[0].SlotNumber() + 1,
			},
			ebHash:    lcommon.NewBlake2b256([]byte("peer-eb")),
			ebSize:    4096,
			announces: true,
		}
		require.NoError(t, c.AddBlockHeader(peerHeader))
		c.PublishPendingChainUpdates()
		testutil.RequireReceive(
			t, ch, 2*time.Second, "peer announcement",
		)

		require.NoError(t, c.AddLocalBlock(nonAnnouncingBlock(blocks[0])))
		evt := testutil.RequireReceive(
			t, ch, 2*time.Second, "invalidation for the discarded header",
		)
		invalid, ok := evt.Data.(chain.ChainHeaderInvalidationEvent)
		require.True(t, ok, "got %T", evt.Data)
		assert.Contains(t, invalid.RbHashes, peerHeader.hash)
		testutil.RequireNoReceive(
			t,
			ch,
			300*time.Millisecond,
			"a forged block that announces nothing adds no announcement",
		)
	})
}

// TestAddLocalBlockAnnouncesItselfAfterInvalidation pins the ordering that
// makes a same-slot forge safe. A locally forged block never passes through
// AddBlockHeader, so its own announcement would otherwise reach the consumer
// only on the block-update topic, which is selected independently of this one.
// When the block competes with a discarded peer header at the same slot, the
// peer's vote holds the (slot, voter) vote id: arming the local announcement
// before the invalidation frees that id gets the local vote rejected as a
// duplicate, and nothing retries it.
//
// Announcing the forged block on this stream, immediately behind the
// invalidation, makes the id free before the consumer ever arms it.
func TestAddLocalBlockAnnouncesItselfAfterInvalidation(t *testing.T) {
	c, bus := newHeaderStreamChain(t)
	subId, ch := bus.Subscribe(chain.ChainHeaderEventType)
	defer bus.Unsubscribe(chain.ChainHeaderEventType, subId)

	blocks, err := testfixtures.GenerateConwayChain(2)
	require.NoError(t, err)
	for i := range blocks {
		_, addErr := c.AddBlockWithPointDeferred(blocks[i], ocommon.Point{
			Slot: blocks[i].SlotNumber(),
			Hash: blocks[i].Hash().Bytes(),
		}, nil)
		require.NoError(t, addErr)
	}
	c.PublishPendingChainUpdates()

	// A competing peer header at the slot the local block will occupy.
	sameSlot := blocks[1].SlotNumber() + 1
	peerHeader := announcingStreamHeader{
		headerStreamHeader: headerStreamHeader{
			hash:        lcommon.NewBlake2b256([]byte("peer-hdr")),
			prevHash:    blocks[1].Hash(),
			blockNumber: blocks[1].BlockNumber() + 1,
			slot:        sameSlot,
		},
		ebHash:    lcommon.NewBlake2b256([]byte("peer-eb")),
		ebSize:    4096,
		announces: true,
	}
	require.NoError(t, c.AddBlockHeader(peerHeader))
	c.PublishPendingChainUpdates()
	testutil.RequireReceive(t, ch, 2*time.Second, "peer announcement")

	localEb := lcommon.NewBlake2b256([]byte("local-eb"))
	local := localForgedBlock{announcingStreamHeader: announcingStreamHeader{
		headerStreamHeader: headerStreamHeader{
			hash:        lcommon.NewBlake2b256([]byte("local-hdr")),
			prevHash:    blocks[1].Hash(),
			blockNumber: blocks[1].BlockNumber() + 1,
			slot:        sameSlot,
		},
		ebHash:    localEb,
		ebSize:    4096,
		announces: true,
	}}
	require.NoError(t, c.AddLocalBlock(local))

	// Invalidation first, then the forged block's own announcement.
	invalidation := testutil.RequireReceive(
		t, ch, 2*time.Second, "invalidation for the discarded peer header",
	)
	invalid, ok := invalidation.Data.(chain.ChainHeaderInvalidationEvent)
	require.True(t, ok, "the invalidation must come first, got %T", invalidation.Data)
	assert.Contains(t, invalid.RbHashes, peerHeader.hash)

	announcement := testutil.RequireReceive(
		t, ch, 2*time.Second, "the forged block announces itself",
	)
	announced, ok := announcement.Data.(chain.ChainHeaderAnnouncementEvent)
	require.True(t, ok, "got %T", announcement.Data)
	assert.Equal(t, local.hash, announced.RbHash)
	assert.Equal(t, localEb, announced.EbHash)
	assert.Equal(t, sameSlot, announced.Slot)
	assert.Greater(
		t,
		announced.Seq,
		invalid.Seq,
		"the forged block is announced behind the invalidation that frees the vote id",
	)
}
