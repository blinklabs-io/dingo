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
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// headerStreamHeader is a minimal ranking-block header. announces controls
// whether it carries a Leios endorser-block announcement; leiosCapable
// controls whether it implements the announcement interface at all, which is
// how a pre-Leios era header behaves.
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
