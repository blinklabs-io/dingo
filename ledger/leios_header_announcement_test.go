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
	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// announcingMockHeader is a ranking-block header that carries a Leios
// endorser-block announcement, as a Dijkstra-era header does.
type announcingMockHeader struct {
	mockHeader
	ebHash lcommon.Blake2b256
	ebSize uint64
}

func (m announcingMockHeader) LeiosAnnouncement() (
	lcommon.Blake2b256,
	uint64,
	bool,
) {
	return m.ebHash, m.ebSize, true
}

// headerStreamFixture is a LedgerState whose chain publishes onto a real event
// bus, so tests can observe the ordered chain.header stream the Leios vote
// manager consumes.
type headerStreamFixture struct {
	ls     *LedgerState
	bus    *event.EventBus
	connId ouroboros.ConnectionId
	ch     <-chan event.Event
}

func newHeaderStreamLedger(t *testing.T) *headerStreamFixture {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	cm, err := chain.NewManager(nil, bus)
	require.NoError(t, err)
	subId, ch := bus.Subscribe(chain.ChainHeaderEventType)
	t.Cleanup(func() { bus.Unsubscribe(chain.ChainHeaderEventType, subId) })
	ls := &LedgerState{
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			EventBus: bus,
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	return &headerStreamFixture{
		ls:  ls,
		bus: bus,
		connId: ouroboros.ConnectionId{
			LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
			RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
		},
		ch: ch,
	}
}

func announcingHeader(
	slot uint64,
	name string,
	prevHash lcommon.Blake2b256,
	blockNumber uint64,
	ebHash lcommon.Blake2b256,
) announcingMockHeader {
	return announcingMockHeader{
		mockHeader: mockHeader{
			hash:        lcommon.NewBlake2b256([]byte(name)),
			prevHash:    prevHash,
			blockNumber: blockNumber,
			slot:        slot,
		},
		ebHash: ebHash,
		ebSize: 4096,
	}
}

// TestChainsyncHeaderAdmissionAnnouncesOnlyWhenCryptoVerified pins the ledger
// half of the crypto gate on the header stream.
//
// chainsyncHeaderCryptoPolicy admits a roll-forward header without verifying
// its VRF/KES on two paths: a slot covered by an imported Mithril snapshot and
// no cached epoch nonce for the slot (verification deferred to blockfetch).
// Both reach
// chain.AddBlockHeader, not AddVerifiedBlockHeader. Announcing such a header
// would let a chainsync peer make this node sign and publish a BLS vote for a
// ranking block it never authenticated, taking the (slot, voterId) pair the
// honest block's own vote needs.
func TestChainsyncHeaderAdmissionAnnouncesOnlyWhenCryptoVerified(
	t *testing.T,
) {
	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	header := announcingHeader(
		577, "hdr-1", lcommon.NewBlake2b256(nil), 1, ebHash,
	)
	point := ocommon.NewPoint(header.slot, header.hash.Bytes())

	// The fixture has no cached epoch nonce, so the policy defers verification
	// and the handler takes the AddBlockHeader branch -- the real end-to-end
	// unverified admission.
	t.Run("unverified admission is queued, not announced", func(t *testing.T) {
		fixture := newHeaderStreamLedger(t)
		verifyNow, trusted := fixture.ls.chainsyncHeaderCryptoPolicy(
			header.slot,
		)
		require.False(t, verifyNow, "fixture must exercise the unverified path")
		require.False(t, trusted)

		require.NoError(
			t,
			fixture.ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
				ConnectionId: fixture.connId,
				BlockHeader:  header,
				Point:        point,
				Tip: ochainsync.Tip{
					Point:       ocommon.NewPoint(60001, []byte("tip-1")),
					BlockNumber: 60001,
				},
			}),
		)
		require.Equal(t, 1, fixture.ls.chain.HeaderCount())

		testutil.RequireNoReceive(
			t,
			fixture.ch,
			500*time.Millisecond,
			"an unverified header must not arm a vote",
		)
	})

	// The verified branch of the same handler is one call:
	// ls.chain.AddVerifiedBlockHeader(e.BlockHeader). It is driven directly
	// here because a header that both announces a Leios endorser block and
	// passes real VRF/KES cannot be synthesized in this suite: gouroboros
	// VerifyBlock dispatches on the concrete era header type, so wrapping a
	// valid Babbage header to add LeiosAnnouncement fails verification with
	// "unsupported block type for VRF verification". The gate itself is
	// covered from both sides in chain:
	// TestHeaderAnnouncementRequiresCryptoVerifiedHeader.
	t.Run("verified admission announces", func(t *testing.T) {
		fixture := newHeaderStreamLedger(t)
		require.NoError(t, fixture.ls.chain.AddVerifiedBlockHeader(header))
		fixture.ls.chain.PublishPendingChainUpdates()

		evt := testutil.RequireReceive(
			t,
			fixture.ch,
			2*time.Second,
			"announcement published from verified header admission",
		)
		data, ok := evt.Data.(chain.ChainHeaderAnnouncementEvent)
		require.True(t, ok)
		assert.Equal(t, uint64(577), data.Slot)
		assert.Equal(t, header.hash, data.RbHash)
		assert.Equal(t, ebHash, data.EbHash)
		assert.NotZero(t, data.Seq)
	})
}

// TestChainsyncHeaderQueueClearedInvalidatesAnnouncement covers the case where
// header admission succeeds but blockfetch startup then fails: the queue is
// discarded and no rollback is published, because no block was ever added.
// Without the invalidation on the same stream, the announcement would outlive
// the header and the vote manager could vote for a ranking block that is not
// on our chain.
//
// The announcing header is admitted verified (the only kind that announces);
// the header that drives the handler into its failing blockfetch start chains
// onto it and announces nothing of its own, so the single announcement under
// test is unambiguous.
func TestChainsyncHeaderQueueClearedInvalidatesAnnouncement(t *testing.T) {
	fixture := newHeaderStreamLedger(t)
	// No BlockfetchRequestRangeFunc is wired, so every blockfetch start
	// attempt fails and the handler exhausts its fallbacks.
	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	announcing := announcingHeader(
		577, "hdr-1", lcommon.NewBlake2b256(nil), 1, ebHash,
	)
	require.NoError(t, fixture.ls.chain.AddVerifiedBlockHeader(announcing))

	follower := mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-2")),
		prevHash:    announcing.hash,
		blockNumber: 2,
		slot:        578,
	}
	point := ocommon.NewPoint(follower.slot, follower.hash.Bytes())

	require.NoError(
		t,
		fixture.ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
			ConnectionId: fixture.connId,
			BlockHeader:  follower,
			Point:        point,
			// Tip equal to the header keeps the handler out of the
			// header-accumulation branches so it reaches blockfetch.
			Tip: ochainsync.Tip{Point: point, BlockNumber: 2},
		}),
	)
	assert.Zero(
		t,
		fixture.ls.chain.HeaderCount(),
		"failed blockfetch start discards the queued headers",
	)

	announcement := testutil.RequireReceive(
		t, fixture.ch, 2*time.Second, "announcement",
	)
	announced, ok := announcement.Data.(chain.ChainHeaderAnnouncementEvent)
	require.True(t, ok, "got %T", announcement.Data)
	assert.Equal(t, announcing.hash, announced.RbHash)

	invalidation := testutil.RequireReceive(
		t, fixture.ch, 2*time.Second, "invalidation for the discarded header",
	)
	invalid, ok := invalidation.Data.(chain.ChainHeaderInvalidationEvent)
	require.True(t, ok, "got %T", invalidation.Data)
	assert.Equal(t, chain.HeaderInvalidationQueueCleared, invalid.Reason)
	assert.Contains(t, invalid.RbHashes, announcing.hash)
	assert.Greater(t, invalid.Seq, announced.Seq)
}

// TestForkResolutionAnnouncesOnlyTheVerifiedIncomingHeader covers the second
// way an announcing header reaches the header queue. A header that does not
// fit the current tip is queued by tryResolveFork rather than by the direct
// admission path, and that branch returns before the caller's ordinary
// bookkeeping runs. Emitting from the chain's own header-queue mutation is
// what keeps this path covered.
//
// tryResolveFork re-queues a whole fork path: the header this event delivered,
// plus earlier headers replayed from recorded peer history. Only the delivered
// one carries a crypto verdict, so only it may be admitted verified, and only
// a verified admission announces (see addForkPathHeader). Both halves are
// asserted here at that composition site.
func TestForkResolutionAnnouncesOnlyTheVerifiedIncomingHeader(t *testing.T) {
	for _, tc := range []struct {
		name           string
		cryptoVerified bool
		wantAnnounce   bool
	}{
		{
			name:           "verified incoming header announces",
			cryptoVerified: true,
			wantAnnounce:   true,
		},
		{
			name:           "unverified incoming header does not announce",
			cryptoVerified: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bus := event.NewEventBus(nil, nil)
			t.Cleanup(bus.Stop)
			fixture := newChainsyncRollbackFixtureWithBus(t, bus)
			subId, ch := bus.Subscribe(chain.ChainHeaderEventType)
			defer bus.Unsubscribe(chain.ChainHeaderEventType, subId)
			// Keep the test at header admission; no blockfetch worker is
			// needed.
			fixture.ls.chainsyncBlockfetchReadyChan = make(chan struct{})

			ebHash := lcommon.NewBlake2b256([]byte("fork-announced-eb"))
			header := announcingHeader(
				fixture.currentTip.Point.Slot+10,
				"fork-announcing-header",
				lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
				fixture.ancestorTip.BlockNumber+1,
				ebHash,
			)
			// The header does not fit the current tip; that failure is the
			// condition tryResolveFork exists to handle.
			var notFitErr chain.BlockNotFitChainTipError
			require.ErrorAs(
				t,
				fixture.ls.chain.AddBlockHeader(header),
				&notFitErr,
			)
			advertisedSlot := ^uint64(0)

			resolved, err := fixture.ls.tryResolveFork(
				ChainsyncEvent{
					ConnectionId: fixture.connId,
					Point: ocommon.NewPoint(
						header.slot,
						header.hash.Bytes(),
					),
					BlockHeader: header,
					Tip: ochainsync.Tip{
						Point: ocommon.NewPoint(
							advertisedSlot,
							[]byte("unbound-fork-tip"),
						),
						BlockNumber: advertisedSlot,
					},
				},
				notFitErr,
				nil,
				tc.cryptoVerified,
			)
			require.NoError(t, err)
			require.True(t, resolved)
			// The header was queued through fork resolution, not direct
			// admission.
			require.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
			require.Equal(t, 1, fixture.ls.chain.HeaderCount())
			fixture.ls.chain.PublishPendingChainUpdates()

			// The rollback's invalidation precedes anything the fork
			// resolution queued after it.
			invalidation := testutil.RequireReceive(
				t, ch, 2*time.Second, "rollback invalidation",
			)
			invalid, ok := invalidation.Data.(chain.ChainHeaderInvalidationEvent)
			require.True(t, ok, "got %T", invalidation.Data)
			assert.Equal(t, chain.HeaderInvalidationRollback, invalid.Reason)

			if !tc.wantAnnounce {
				testutil.RequireNoReceive(
					t,
					ch,
					500*time.Millisecond,
					"an unverified fork header must not arm a vote",
				)
				return
			}

			announcement := testutil.RequireReceive(
				t, ch, 2*time.Second, "announcement from fork resolution",
			)
			announced, ok := announcement.Data.(chain.ChainHeaderAnnouncementEvent)
			require.True(t, ok, "got %T", announcement.Data)
			assert.Equal(t, header.hash, announced.RbHash)
			assert.Equal(t, ebHash, announced.EbHash)
			assert.Greater(
				t,
				announced.Seq,
				invalid.Seq,
				"the fork header is admitted after the rollback that made room for it",
			)
			testutil.RequireNoReceive(
				t,
				ch,
				300*time.Millisecond,
				"the incoming fork header must be announced exactly once",
			)
		})
	}
}

// newChainsyncRollbackFixtureWithBus mirrors newChainsyncRollbackFixture but
// gives the chain a real event bus so its deferred header/rollback events can
// be observed.
func newChainsyncRollbackFixtureWithBus(
	t *testing.T,
	bus *event.EventBus,
) *chainsyncRollbackFixture {
	t.Helper()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, bus)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 2}),
	)

	ancestorHash := testHashBytes("ancestor-block")
	currentHash := testHashBytes("current-block")
	ancestorBlock := chain.RawBlock{
		Slot:        10,
		Hash:        ancestorHash,
		BlockNumber: 1,
		Type:        1,
		Cbor:        []byte{0x80},
	}
	currentBlock := chain.RawBlock{
		Slot:        20,
		Hash:        currentHash,
		BlockNumber: 2,
		Type:        1,
		PrevHash:    ancestorHash,
		Cbor:        []byte{0x80},
	}
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks([]chain.RawBlock{
			ancestorBlock,
			currentBlock,
		}),
	)

	ls, err := NewLedgerState(
		LedgerStateConfig{
			Database:          db,
			ChainManager:      cm,
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	)
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	// Attached after construction so NewLedgerState does not register the
	// node-level subscribers this focused test does not want.
	ls.config.EventBus = bus

	ancestorTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(ancestorBlock.Slot, ancestorBlock.Hash),
		BlockNumber: ancestorBlock.BlockNumber,
	}
	currentTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(currentBlock.Slot, currentBlock.Hash),
		BlockNumber: currentBlock.BlockNumber,
	}
	ancestorNonce := []byte("nonce-ancestor")
	currentNonce := []byte("nonce-current")
	require.NoError(t, db.SetBlockNonce(
		ancestorTip.Point.Hash,
		ancestorTip.Point.Slot,
		ancestorNonce,
		true,
		nil,
	))
	require.NoError(t, db.SetBlockNonce(
		currentTip.Point.Hash, currentTip.Point.Slot, currentNonce, false, nil,
	))
	require.NoError(t, db.SetTip(currentTip, nil))

	ls.currentTip = currentTip
	ls.currentTipBlockNonce = append([]byte(nil), currentNonce...)
	ls.chainsyncState = SyncingChainsyncState
	ls.publishSnapshotsLocked()

	return &chainsyncRollbackFixture{
		ls:          ls,
		ancestorTip: ancestorTip,
		currentTip:  currentTip,
		connId: ouroboros.ConnectionId{
			LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
			RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
		},
		ancestorNonce: ancestorNonce,
	}
}

// TestConnectionClosedPublishesHeaderInvalidation covers a header-queue
// discard on a peer-stall path. When the connection that owned the header
// pipeline closes, the queue is discarded and Chain.ClearHeaders enqueues the
// invalidation on the chain-level sequencer -- but this handler previously
// registered no drain, so it sat there until some unrelated handler ran. A
// dead peer is exactly the case where no further event is guaranteed, so the
// announcement would stay armed well past the ten-slot vote window.
func TestConnectionClosedPublishesHeaderInvalidation(t *testing.T) {
	fixture := newHeaderStreamLedger(t)
	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	header := announcingHeader(
		577, "hdr-1", lcommon.NewBlake2b256(nil), 1, ebHash,
	)
	require.NoError(t, fixture.ls.chain.AddVerifiedBlockHeader(header))
	fixture.ls.headerPipelineConnId = fixture.connId
	require.Equal(t, 1, fixture.ls.chain.HeaderCount())

	// The announcement is still undrained on the sequencer; the closed
	// connection must publish it and the invalidation that voids it.
	fixture.ls.handleConnectionClosedEvent(event.NewEvent(
		ConnectionClosedEventType,
		ConnectionClosedEvent{ConnectionId: fixture.connId},
	))
	assert.Zero(t, fixture.ls.chain.HeaderCount())

	announcement := testutil.RequireReceive(
		t, fixture.ch, 2*time.Second, "announcement",
	)
	announced, ok := announcement.Data.(chain.ChainHeaderAnnouncementEvent)
	require.True(t, ok, "got %T", announcement.Data)
	assert.Equal(t, header.hash, announced.RbHash)

	invalidation := testutil.RequireReceive(
		t,
		fixture.ch,
		2*time.Second,
		"invalidation published without any later event",
	)
	invalid, ok := invalidation.Data.(chain.ChainHeaderInvalidationEvent)
	require.True(t, ok, "got %T", invalidation.Data)
	assert.Equal(t, chain.HeaderInvalidationQueueCleared, invalid.Reason)
	assert.Contains(t, invalid.RbHashes, header.hash)
	assert.Greater(t, invalid.Seq, announced.Seq)
}

// TestBlockfetchTimeoutDrainsHeaderSequencer covers the other peer-stall path.
// The timeout handler tears the batch down and clears the header queue, and it
// is the last thing that runs for a peer that stopped sending, so it has to
// drain the sequencer rather than leave header events queued behind it.
func TestBlockfetchTimeoutDrainsHeaderSequencer(t *testing.T) {
	fixture := newHeaderStreamLedger(t)
	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	header := announcingHeader(
		577, "hdr-1", lcommon.NewBlake2b256(nil), 1, ebHash,
	)
	require.NoError(t, fixture.ls.chain.AddVerifiedBlockHeader(header))

	var pending pendingPublishes
	func() {
		defer pending.flush()
		fixture.ls.chainsyncBlockfetchMutex.Lock()
		defer fixture.ls.chainsyncBlockfetchMutex.Unlock()
		fixture.ls.handleBlockfetchTimeoutLocked(fixture.connId, &pending)
	}()

	evt := testutil.RequireReceive(
		t,
		fixture.ch,
		2*time.Second,
		"header events published without any later event",
	)
	announced, ok := evt.Data.(chain.ChainHeaderAnnouncementEvent)
	require.True(t, ok, "got %T", evt.Data)
	assert.Equal(t, header.hash, announced.RbHash)
}
