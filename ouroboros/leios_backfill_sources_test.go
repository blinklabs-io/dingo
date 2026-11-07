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

package ouroboros

import (
	"bytes"
	"fmt"
	"net"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosnotify "github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
	"github.com/stretchr/testify/require"
)

func TestLeiosBackfillSourceIsPointAndOwnerScoped(t *testing.T) {
	t.Parallel()
	o := newOuroboros(OuroborosConfig{})
	connId := namedConnId("announcer")
	owner := &oleiosnotify.Client{}
	point := ocommon.Point{Slot: 100, Hash: []byte{0x01, 0x02}}
	o.recordLeiosBackfillSource(
		point,
		connId,
		owner,
	)

	require.True(t, o.leiosRecordedBackfillSource(point, connId, owner))
	require.False(
		t,
		o.leiosRecordedBackfillSource(
			ocommon.Point{Slot: point.Slot + 1, Hash: point.Hash},
			connId,
			owner,
		),
	)
	require.False(
		t,
		o.leiosRecordedBackfillSource(
			ocommon.Point{Slot: point.Slot, Hash: []byte{0x02, 0x01}},
			connId,
			owner,
		),
	)
	require.False(
		t,
		o.leiosRecordedBackfillSource(point, connId, &oleiosnotify.Client{}),
	)
}

func TestLeiosBackfillSourceRefreshDoesNotExtendOtherPeer(t *testing.T) {
	t.Parallel()
	o := newOuroboros(OuroborosConfig{})
	point := ocommon.Point{Slot: 100, Hash: []byte{0x01, 0x02}}
	oldConn := namedConnId("old-announcer")
	freshConn := namedConnId("fresh-announcer")
	oldOwner := &oleiosnotify.Client{}
	freshOwner := &oleiosnotify.Client{}
	o.recordLeiosBackfillSource(point, oldConn, oldOwner)
	o.recordLeiosBackfillSource(point, freshConn, freshOwner)

	key := leiosBlockKey(point.Slot, point.Hash)
	o.leiosBackfillSourcesMu.Lock()
	source := o.leiosBackfillSources[key].announcers[oldConn]
	source.recordedAt = time.Now().Add(-leiosEndorserBlockCacheTTL - time.Second)
	o.leiosBackfillSources[key].announcers[oldConn] = source
	o.leiosBackfillSourcesMu.Unlock()
	// Refreshing one source prunes the expired owner independently.
	o.recordLeiosBackfillSource(point, freshConn, freshOwner)

	require.False(t, o.leiosRecordedBackfillSource(point, oldConn, oldOwner))
	require.True(t, o.leiosRecordedBackfillSource(point, freshConn, freshOwner))
}

func TestLeiosBackfillSourcesBoundReconnectChurnPerPoint(t *testing.T) {
	t.Parallel()
	o := newOuroboros(OuroborosConfig{})
	owner := &oleiosnotify.Client{}
	point := ocommon.Point{Slot: 100, Hash: []byte{0x01}}
	for i := 0; i <= leiosBackfillMaxSourcesPerPoint; i++ {
		o.recordLeiosBackfillSource(
			point,
			namedConnId(fmt.Sprintf("announcer-%d", i)),
			owner,
		)
	}

	o.leiosBackfillSourcesMu.Lock()
	defer o.leiosBackfillSourcesMu.Unlock()
	sources := o.leiosBackfillSources[leiosBlockKey(point.Slot, point.Hash)]
	require.Len(t, sources.announcers, leiosBackfillMaxSourcesPerPoint)
	_, oldestRetained := sources.announcers[namedConnId("announcer-0")]
	_, newestRetained := sources.announcers[namedConnId(fmt.Sprintf(
		"announcer-%d",
		leiosBackfillMaxSourcesPerPoint,
	))]
	require.False(t, oldestRetained)
	require.True(t, newestRetained)
}

func TestLeiosBackfillSourcePointsShareCacheEntryBound(t *testing.T) {
	t.Parallel()
	o := newOuroboros(OuroborosConfig{})
	owner := &oleiosnotify.Client{}
	// One connection may hold only leiosBackfillMaxPointsPerConn points, so
	// spread the points across enough connections to reach the global bound.
	for slot := uint64(0); slot <= leiosEndorserBlockCacheMaxEntries; slot++ {
		o.recordLeiosBackfillSource(
			ocommon.Point{Slot: slot, Hash: []byte{0x01}},
			namedConnId(fmt.Sprintf(
				"announcer-%d", slot/leiosBackfillMaxPointsPerConn,
			)),
			owner,
		)
	}

	o.leiosBackfillSourcesMu.Lock()
	defer o.leiosBackfillSourcesMu.Unlock()
	require.Len(t, o.leiosBackfillSources, leiosEndorserBlockCacheMaxEntries)
	_, oldestRetained := o.leiosBackfillSources[leiosBlockKey(0, []byte{0x01})]
	_, newestRetained := o.leiosBackfillSources[leiosBlockKey(
		leiosEndorserBlockCacheMaxEntries,
		[]byte{0x01},
	)]
	require.False(t, oldestRetained)
	require.True(t, newestRetained)
}

// TestLeiosOffersForImplausiblePointsDoNotTouchSharedState proves a peer
// cannot write the endorser-block cache or the backfill-source table with a
// point that fails validation, so it cannot churn them or evict sources
// recorded for real points.
func TestLeiosOffersForImplausiblePointsDoNotTouchSharedState(t *testing.T) {
	t.Parallel()
	cm, peers := newLeiosBackfillSelectorPeers(t, "peer")
	conn := peers[0]
	o := newOuroboros(OuroborosConfig{ConnManager: cm})
	now := time.Now()
	o.leiosAnnouncementLedger = &fakeLeiosAnnouncementLedger{
		slotTimeFunc: func(slot uint64) time.Time {
			switch slot {
			case 1:
				return now.Add(-time.Hour) // too old
			case 2:
				return now.Add(time.Hour) // in the future
			default:
				return now
			}
		},
	}
	cb := oleiosnotify.CallbackContext{
		ConnectionId: conn.Id(),
		Client:       conn.LeiosNotify().Client,
	}
	hash := bytes.Repeat([]byte{0xab}, 32)
	bad := []ocommon.Point{
		{Slot: 1, Hash: hash},
		{Slot: 2, Hash: hash},
		{Slot: 3, Hash: hash[:31]},
	}
	for _, point := range bad {
		require.NoError(t, o.leiosnotifyClientNotification(
			cb, oleiosnotify.NewMsgBlockTxsOffer(point),
		))
		require.NoError(t, o.leiosnotifyClientNotification(
			cb, oleiosnotify.NewMsgBlockOffer(point, 1),
		))
	}
	o.leiosMu.Lock()
	cached := len(o.leiosEndorserBlocks)
	o.leiosMu.Unlock()
	require.Zero(t, cached, "implausible offers must not enter the EB cache")
	o.leiosBackfillSourcesMu.Lock()
	sources := len(o.leiosBackfillSources)
	o.leiosBackfillSourcesMu.Unlock()
	require.Zero(t, sources, "implausible offers must not record sources")

	// A plausible offer is still recorded.
	good := ocommon.Point{Slot: 4, Hash: hash}
	require.NoError(t, o.leiosnotifyClientNotification(
		cb, oleiosnotify.NewMsgBlockTxsOffer(good),
	))
	require.True(t, o.leiosRecordedBackfillSource(
		good, conn.Id(), conn.LeiosNotify().Client,
	))
}

// TestLeiosBackfillSourceOverflowOnlyEvictsOwnConnection proves one
// connection recording many points cannot displace another connection's
// sources: past its per-connection cap it replaces its own oldest point.
func TestLeiosBackfillSourceOverflowOnlyEvictsOwnConnection(t *testing.T) {
	t.Parallel()
	o := newOuroboros(OuroborosConfig{})
	honest := namedConnId("honest")
	noisy := namedConnId("noisy")
	honestOwner := &oleiosnotify.Client{}
	noisyOwner := &oleiosnotify.Client{}
	realPoint := ocommon.Point{Slot: 1, Hash: bytes.Repeat([]byte{0x01}, 32)}
	o.recordLeiosBackfillSource(realPoint, honest, honestOwner)

	total := 3 * leiosBackfillMaxPointsPerConn
	for i := range total {
		o.recordLeiosBackfillSource(
			ocommon.Point{
				Slot: uint64(100 + i),
				Hash: bytes.Repeat([]byte{byte(i + 2)}, 32),
			},
			noisy,
			noisyOwner,
		)
	}

	require.True(
		t,
		o.leiosRecordedBackfillSource(realPoint, honest, honestOwner),
		"another connection's source must survive",
	)
	held := 0
	o.leiosBackfillSourcesMu.Lock()
	for _, sources := range o.leiosBackfillSources {
		if _, ok := sources.announcers[noisy]; ok {
			held++
		}
	}
	o.leiosBackfillSourcesMu.Unlock()
	require.Equal(t, leiosBackfillMaxPointsPerConn, held)
	require.False(t, o.leiosRecordedBackfillSource(
		ocommon.Point{Slot: 100, Hash: bytes.Repeat([]byte{2}, 32)},
		noisy,
		noisyOwner,
	), "the noisy connection's oldest point must be the one evicted")
	// Newest points are kept, the oldest replaced.
	require.True(t, o.leiosRecordedBackfillSource(
		ocommon.Point{
			Slot: uint64(100 + total - 1),
			Hash: bytes.Repeat([]byte{byte(total + 1)}, 32),
		},
		noisy,
		noisyOwner,
	))
}

// TestLeiosIntentOnlyOffersOverflowOnlyEvictsOwnConnection is the endorser-
// block cache counterpart.
func TestLeiosIntentOnlyOffersOverflowOnlyEvictsOwnConnection(t *testing.T) {
	t.Parallel()
	o := newOuroboros(OuroborosConfig{})
	realPoint := ocommon.Point{Slot: 1, Hash: bytes.Repeat([]byte{0x01}, 32)}
	o.markLeiosEndorserBlockRelayOfferFrom(realPoint, "honest")
	for i := range 3 * leiosMaxIntentOnlyPerConn {
		o.markLeiosEndorserBlockRelayOfferFrom(
			ocommon.Point{
				Slot: uint64(100 + i),
				Hash: bytes.Repeat([]byte{byte(i + 2)}, 32),
			},
			"noisy",
		)
	}
	o.leiosMu.Lock()
	defer o.leiosMu.Unlock()
	require.Contains(
		t,
		o.leiosEndorserBlocks,
		leiosBlockKey(realPoint.Slot, realPoint.Hash),
		"another connection's intent entry must survive",
	)
	noisy := 0
	for _, data := range o.leiosEndorserBlocks {
		if data.relayOfferSource == "noisy" {
			noisy++
		}
	}
	require.Equal(t, leiosMaxIntentOnlyPerConn, noisy)
	total := 3 * leiosMaxIntentOnlyPerConn
	require.NotContains(
		t,
		o.leiosEndorserBlocks,
		leiosBlockKey(100, bytes.Repeat([]byte{2}, 32)),
		"the noisy connection's oldest entry must be the one evicted",
	)
	require.Contains(
		t,
		o.leiosEndorserBlocks,
		leiosBlockKey(
			uint64(100+total-1),
			bytes.Repeat([]byte{byte(total + 1)}, 32),
		),
		"the noisy connection's newest entry must be kept",
	)
}

// TestLeiosIntentOnlyReofferDoesNotTransferOwnership proves a connection
// re-offering another connection's intent-only point neither takes over its
// eviction ownership nor keeps the cap from applying to its own entries,
// while relayOfferSource still tracks the latest offerer.
func TestLeiosIntentOnlyReofferDoesNotTransferOwnership(t *testing.T) {
	t.Parallel()
	o := newOuroboros(OuroborosConfig{})
	shared := ocommon.Point{Slot: 1, Hash: bytes.Repeat([]byte{0x01}, 32)}
	o.markLeiosEndorserBlockRelayOfferFrom(shared, "owner")
	o.markLeiosEndorserBlockRelayOfferFrom(shared, "other")
	for i := range 3 * leiosMaxIntentOnlyPerConn {
		o.markLeiosEndorserBlockRelayOfferFrom(
			ocommon.Point{
				Slot: uint64(100 + i),
				Hash: bytes.Repeat([]byte{byte(i + 2)}, 32),
			},
			"other",
		)
	}
	o.leiosMu.Lock()
	defer o.leiosMu.Unlock()
	data := o.leiosEndorserBlocks[leiosBlockKey(shared.Slot, shared.Hash)]
	require.NotNil(t, data, "the owner's entry must survive the other peer's overflow")
	require.Equal(t, "owner", data.intentOwner)
	require.Equal(t, "other", data.relayOfferSource)
}

// TestLeiosOfferBudgetIsPerHost proves a host past its offer
// budget is dropped before touching shared state while others still work.
func TestLeiosOfferBudgetIsPerHost(t *testing.T) {
	t.Parallel()
	o := newOuroboros(OuroborosConfig{})
	point := ocommon.Point{Slot: 1, Hash: bytes.Repeat([]byte{0x01}, 32)}
	for range leiosOfferMaxPerPeer {
		require.True(t, o.leiosOfferedPointPlausible(point, "noisy"))
	}
	require.False(t, o.leiosOfferedPointPlausible(point, "noisy"))
	require.True(t, o.leiosOfferedPointPlausible(point, "honest"))
}

// TestLeiosOfferBudgetSurvivesReconnect proves connections from one remote
// host share a budget, so reconnecting does not refresh it.
func TestLeiosOfferBudgetSurvivesReconnect(t *testing.T) {
	t.Parallel()
	local := &net.TCPAddr{IP: net.ParseIP("10.0.0.1"), Port: 3001}
	connA := ouroboros.ConnectionId{
		LocalAddr:  local,
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("192.0.2.7"), Port: 40001},
	}
	connB := ouroboros.ConnectionId{
		LocalAddr:  local,
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("192.0.2.7"), Port: 40002},
	}
	other := ouroboros.ConnectionId{
		LocalAddr:  local,
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("192.0.2.8"), Port: 40001},
	}
	require.Equal(t, leiosOfferBudgetKey(connA), leiosOfferBudgetKey(connB))
	require.NotEqual(t, leiosOfferBudgetKey(connA), leiosOfferBudgetKey(other))
	o := newOuroboros(OuroborosConfig{})
	point := ocommon.Point{Slot: 1, Hash: bytes.Repeat([]byte{0x01}, 32)}
	for range leiosOfferMaxPerPeer {
		require.True(t, o.leiosOfferedPointPlausible(point, leiosOfferBudgetKey(connA)))
	}
	require.False(t, o.leiosOfferedPointPlausible(point, leiosOfferBudgetKey(connB)))
	require.True(t, o.leiosOfferedPointPlausible(point, leiosOfferBudgetKey(other)))
}

// TestLeiosOfferBudgetTrackingIsBounded proves hosts beyond the tracking cap
// share one overflow budget instead of growing the map, and tracked hosts
// are unaffected.
func TestLeiosOfferBudgetTrackingIsBounded(t *testing.T) {
	t.Parallel()
	o := newOuroboros(OuroborosConfig{})
	point := ocommon.Point{Slot: 1, Hash: bytes.Repeat([]byte{0x01}, 32)}
	for i := range leiosOfferMaxHosts {
		require.True(t, o.leiosOfferedPointPlausible(point, fmt.Sprintf("host-%d", i)))
	}
	for range leiosOfferMaxPerPeer {
		require.True(t, o.leiosOfferedPointPlausible(point, "new-host-a"))
	}
	require.False(t, o.leiosOfferedPointPlausible(point, "new-host-b"),
		"untracked hosts must share the overflow budget")
	require.True(t, o.leiosOfferedPointPlausible(point, "host-0"),
		"tracked hosts keep their own budget")
}
