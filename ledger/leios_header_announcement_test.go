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
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// announcingMockHeader is a ranking-block header that carries a Leios
// endorser-block announcement, as a Dijkstra-era header does.
type announcingMockHeader struct {
	mockHeader
	ebHash    lcommon.Blake2b256
	ebSize    uint64
	announces bool
}

func (m announcingMockHeader) LeiosAnnouncement() (
	lcommon.Blake2b256,
	uint64,
	bool,
) {
	return m.ebHash, m.ebSize, m.announces
}

// TestPublishLeiosHeaderAnnouncement asserts the announcement is surfaced from
// the roll-forward header. The Leios vote window is measured from the
// announcing ranking block's slot, and applying an EB-announcing ranking block
// waits on fetching that same endorser block, so an apply-driven signal cannot
// arrive while the window is open.
func TestPublishLeiosHeaderAnnouncement(t *testing.T) {
	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	rbHeaderHash := lcommon.NewBlake2b256([]byte("announcing-rb"))
	for _, tc := range []struct {
		name        string
		header      lcommon.BlockHeader
		wantPublish bool
	}{
		{
			name: "announcing header publishes",
			header: announcingMockHeader{
				mockHeader: mockHeader{
					hash: rbHeaderHash,
					slot: 577,
				},
				ebHash:    ebHash,
				ebSize:    4096,
				announces: true,
			},
			wantPublish: true,
		},
		{
			name: "header with no announcement publishes nothing",
			header: announcingMockHeader{
				mockHeader: mockHeader{
					hash: rbHeaderHash,
					slot: 577,
				},
				announces: false,
			},
		},
		{
			name: "pre-leios header publishes nothing",
			header: mockHeader{
				hash: rbHeaderHash,
				slot: 577,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bus := event.NewEventBus(nil, nil)
			subId, ch := bus.Subscribe(
				chain.ChainHeaderAnnouncementEventType,
			)
			defer bus.Unsubscribe(
				chain.ChainHeaderAnnouncementEventType,
				subId,
			)
			ls := &LedgerState{
				config: LedgerStateConfig{EventBus: bus},
			}
			var pending pendingPublishes
			ls.publishLeiosHeaderAnnouncement(
				ChainsyncEvent{BlockHeader: tc.header},
				&pending,
			)
			pending.flush()
			if !tc.wantPublish {
				testutil.RequireNoReceive(
					t,
					ch,
					300*time.Millisecond,
					"no announcement event expected",
				)
				return
			}
			evt := testutil.RequireReceive(
				t,
				ch,
				2*time.Second,
				"header announcement published",
			)
			data, ok := evt.Data.(chain.ChainHeaderAnnouncementEvent)
			require.True(t, ok)
			assert.Equal(t, uint64(577), data.Slot)
			assert.Equal(t, rbHeaderHash, data.RbHash)
			assert.Equal(t, ebHash, data.EbHash)
			assert.Equal(t, uint64(4096), data.EbSize)
		})
	}
}

// TestPublishLeiosHeaderAnnouncementWithoutEventBus covers run modes with no
// event bus wired: the header path must stay silent rather than panic.
func TestPublishLeiosHeaderAnnouncementWithoutEventBus(t *testing.T) {
	ls := &LedgerState{}
	var pending pendingPublishes
	assert.NotPanics(t, func() {
		ls.publishLeiosHeaderAnnouncement(
			ChainsyncEvent{BlockHeader: announcingMockHeader{
				mockHeader: mockHeader{slot: 577},
				announces:  true,
			}},
			&pending,
		)
		ls.publishLeiosHeaderAnnouncement(ChainsyncEvent{}, &pending)
		pending.flush()
	})
}

// TestChainsyncHeaderAdmissionPublishesLeiosAnnouncement pins the call site:
// admitting a roll-forward header that announces an endorser block must
// surface the announcement, without waiting for the block body.
func TestChainsyncHeaderAdmissionPublishesLeiosAnnouncement(t *testing.T) {
	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	header := announcingMockHeader{
		mockHeader: mockHeader{
			hash:        lcommon.NewBlake2b256([]byte("hdr-1")),
			prevHash:    lcommon.NewBlake2b256(nil),
			blockNumber: 1,
			slot:        577,
		},
		ebHash:    ebHash,
		ebSize:    4096,
		announces: true,
	}
	bus := event.NewEventBus(nil, nil)
	subId, ch := bus.Subscribe(chain.ChainHeaderAnnouncementEventType)
	defer bus.Unsubscribe(chain.ChainHeaderAnnouncementEventType, subId)
	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			EventBus: bus,
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	require.NoError(t, ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId,
		BlockHeader:  header,
		Point: ocommon.NewPoint(
			header.slot,
			header.hash.Bytes(),
		),
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(60001, []byte("tip-1")),
			BlockNumber: 60001,
		},
	}))
	require.Equal(t, 1, ls.chain.HeaderCount())

	evt := testutil.RequireReceive(
		t,
		ch,
		2*time.Second,
		"announcement published from header admission",
	)
	data, ok := evt.Data.(chain.ChainHeaderAnnouncementEvent)
	require.True(t, ok)
	assert.Equal(t, uint64(577), data.Slot)
	assert.Equal(t, header.hash, data.RbHash)
	assert.Equal(t, ebHash, data.EbHash)
}
