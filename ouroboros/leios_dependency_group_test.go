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
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	gouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosnotify "github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
	"github.com/stretchr/testify/require"
)

func TestLeiosDependencyHeaderSurvivesTransientEviction(t *testing.T) {
	t.Parallel()
	log := newLeiosForgedEBLog()
	log.registerConn("slow", nil, nil)
	hash := lcommon.NewBlake2b256([]byte("eb"))
	point := ocommon.NewPoint(42, hash.Bytes())
	header := testDijkstraAnnouncementHeaderRawFor(t, point.Slot, hash, 42)
	log.append(
		leiosForgedEBEntry{announcement: header},
		leiosForgedEBEntry{point: &point},
		leiosForgedEBEntry{txOffer: &point},
	)
	for range leiosEBLogMaxTransientEntries {
		log.append(leiosForgedEBEntry{announcement: []byte("later")})
	}
	entry, _ := log.next("slow")
	require.NotNil(t, entry)
	require.Equal(
		t,
		header,
		entry.announcement,
		"retained dependent offer must not outlive its announcing header",
	)
}

func TestLeiosDependencyReconnectReplaysHeader(t *testing.T) {
	t.Parallel()
	log := newLeiosForgedEBLog()
	log.registerConn("old", nil, nil)
	hash := lcommon.NewBlake2b256([]byte("eb"))
	point := ocommon.NewPoint(42, hash.Bytes())
	header := testDijkstraAnnouncementHeaderRawFor(t, point.Slot, hash, 42)
	log.append(
		leiosForgedEBEntry{announcement: header},
		leiosForgedEBEntry{point: &point},
		leiosForgedEBEntry{txOffer: &point},
	)
	entry, _ := log.next("old")
	require.Equal(t, header, entry.announcement)
	log.complete("old", nil, true)
	entry, _ = log.next("old")
	require.NotNil(t, entry.point)
	log.complete("old", nil, false)
	log.removeConn("old")
	log.registerConn("new", nil, nil)
	entry, _ = log.next("new")
	require.NotNil(t, entry)
	require.Equal(
		t,
		header,
		entry.announcement,
		"new bearer must receive the header before retrying a dependent offer",
	)
}

func TestLeiosDependencyHeaderFailedDeliveryAndOwnerReplacement(t *testing.T) {
	t.Parallel()
	log := newLeiosForgedEBLog()
	first, second := new(oleiosnotify.Server), new(oleiosnotify.Server)
	hash := lcommon.NewBlake2b256([]byte("eb"))
	point := ocommon.NewPoint(42, hash.Bytes())
	header := testDijkstraAnnouncementHeaderRawFor(t, 42, hash, 42)
	log.registerConn("holder", nil, nil)
	log.registerConn("same-address", first, nil)
	log.append(leiosForgedEBEntry{point: &point, dependencyHeader: header})
	entry, _ := log.nextWhileConnected("same-address", first, nil)
	require.Equal(t, header, entry.announcement)
	log.complete("same-address", first, false)
	log.registerConn("same-address", second, nil)
	entry, _ = log.nextWhileConnected("same-address", second, nil)
	require.Equal(t, header, entry.announcement)
	log.complete("same-address", first, true)
	require.Empty(
		t,
		log.announced["same-address"],
		"old owner cannot acknowledge new owner header",
	)
	log.complete("same-address", second, true)
	entry, _ = log.nextWhileConnected("same-address", second, nil)
	require.NotNil(t, entry.point)
	require.True(
		t,
		log.reservations["same-address"].retry,
		"header delivery must not discharge offer retry",
	)
	log.complete("same-address", second, true)
	require.Empty(t, log.retries)
	log.removeConn("same-address")
	log.removeConn("holder")
	require.Empty(t, log.announced)
}

func TestLeiosDependencyDoesNotBindUnrelatedOffer(t *testing.T) {
	t.Parallel()
	log := newLeiosForgedEBLog()
	log.registerConn("peer", nil, nil)
	hash := lcommon.NewBlake2b256([]byte("eb"))
	header := testDijkstraAnnouncementHeaderRawFor(t, 42, hash, 42)
	other := ocommon.NewPoint(43, hash.Bytes())
	log.append(
		leiosForgedEBEntry{announcement: header},
		leiosForgedEBEntry{point: &other},
	)
	first, _ := log.next("peer")
	require.Equal(t, header, first.announcement)
	log.complete("peer", nil, true)
	offer, _ := log.next("peer")
	require.NotNil(t, offer.point)
	require.Empty(t, offer.dependencyHeader)
}

func TestLeiosDependencyExpiryDropsHeaderAndOffers(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		log := newLeiosForgedEBLog()
		log.registerConn("peer", nil, nil)
		hash := lcommon.NewBlake2b256([]byte("eb"))
		point := ocommon.NewPoint(42, hash.Bytes())
		header := testDijkstraAnnouncementHeaderRawFor(t, 42, hash, 42)
		log.append(
			leiosForgedEBEntry{
				announcement: header,
				expiresAt:    time.Now().Add(time.Minute),
			},
			leiosForgedEBEntry{point: &point},
			leiosForgedEBEntry{txOffer: &point},
		)
		time.Sleep(time.Minute)
		entry, _ := log.next("peer")
		require.Nil(t, entry)
		require.Empty(t, log.items)
		require.Empty(t, log.retries)
	})
}

func TestLeiosOversizedPersistedAnnouncementFlushesAfterParking(t *testing.T) {
	// Not t.Parallel: changes the process-wide cache byte budget.
	point, manifest, txs := leiosPersistedTestEntry(t, 33, 0, 20)
	withLowerLeiosEndorserBlockCacheBudgets(t, 200, 1<<20)
	o := newTestOuroborosWithLeiosDB(t)
	o.leiosAnnouncementLedger = &fakeLeiosAnnouncementLedger{
		slotTime: time.Now(),
	}
	require.NoError(
		t,
		o.leiosDatabase().SetLeiosEB(point.Slot, point.Hash, manifest, txs),
	)
	o.leiosEBLog.registerConn("peer", nil, nil)
	header := testDijkstraAnnouncementHeaderRawFor(
		t,
		point.Slot,
		lcommon.NewBlake2b256(point.Hash),
		uint64(len(manifest)),
	)
	require.NoError(t, recordRelayedHeader(t, o, point, len(manifest), header))
	next, _ := o.leiosEBLog.next("peer")
	require.NotNil(t, next)
	require.Equal(t, header, next.announcement)
	require.Empty(t, o.leiosRelayPendingAnnouncements)
	require.NotContains(
		t,
		o.leiosEndorserBlocks,
		leiosBlockKey(point.Slot, point.Hash),
	)
}

func TestLeiosDependencyInterleavingNeverRepeatsHeader(t *testing.T) {
	t.Parallel()
	log := newLeiosForgedEBLog()
	log.registerConn("peer", nil, nil)
	hash := lcommon.NewBlake2b256([]byte("eb"))
	a := ocommon.NewPoint(42, hash.Bytes())
	b := ocommon.NewPoint(43, hash.Bytes())
	ha := testDijkstraAnnouncementHeaderRawFor(t, a.Slot, hash, 42)
	hb := testDijkstraAnnouncementHeaderRawFor(t, b.Slot, hash, 42)
	log.append(
		leiosForgedEBEntry{announcement: ha},
		leiosForgedEBEntry{point: &a},
	)
	log.append(
		leiosForgedEBEntry{announcement: hb},
		leiosForgedEBEntry{point: &b},
	)
	log.append(
		leiosForgedEBEntry{announcement: ha},
		leiosForgedEBEntry{txOffer: &a},
	)
	headers := map[string]int{}
	offers := 0
	for {
		entry, _ := log.next("peer")
		if entry == nil {
			break
		}
		if len(entry.announcement) > 0 {
			headers[string(entry.announcement)]++
		} else {
			offers++
		}
		log.complete("peer", nil, true)
	}
	require.Equal(
		t,
		map[string]int{string(ha): 1, string(hb): 1},
		headers,
		"reference peers disconnect on a repeated announcement",
	)
	require.Equal(t, 3, offers)
}

func TestLeiosDependencyHistoryBoundRecyclesWithoutRepeating(t *testing.T) {
	t.Parallel()
	log := newLeiosForgedEBLog()
	log.registerConn("peer", nil, nil)
	for i := range leiosEBLogMaxEntries {
		log.append(
			leiosForgedEBEntry{
				announcement: []byte(fmt.Sprintf("header-%d", i)),
			},
		)
		entry, _ := log.next("peer")
		require.NotNil(t, entry)
		log.complete("peer", nil, true)
	}
	header := []byte("beyond-bound")
	log.append(leiosForgedEBEntry{announcement: header})
	entry, _ := log.next("peer")
	require.ErrorContains(t, entry.deliveryError, "history limit")
	require.Len(t, log.announced["peer"], leiosEBLogMaxEntries)
	log.removeConn("peer")
	log.registerConn("replacement", nil, nil)
	entry, _ = log.next("replacement")
	require.Equal(t, header, entry.announcement)
	log.complete("replacement", nil, true)
	log.removeConn("replacement")
	require.Empty(t, log.retries)
	require.Empty(t, log.announced)
}

func TestLeiosDependencyUnservableDiscardClearsAllRetries(t *testing.T) {
	t.Parallel()
	log := newLeiosForgedEBLog()
	hash := lcommon.NewBlake2b256([]byte("eb"))
	point := ocommon.NewPoint(42, hash.Bytes())
	header := testDijkstraAnnouncementHeaderRawFor(t, 42, hash, 42)
	for _, peer := range []string{"one", "two"} {
		log.registerConn(peer, nil, nil)
	}
	log.append(leiosForgedEBEntry{point: &point, dependencyHeader: header})
	for _, peer := range []string{"one", "two"} {
		entry, _ := log.next(peer)
		require.Equal(t, header, entry.announcement)
		log.complete(peer, nil, false)
	}
	require.Equal(t, 2, log.retries[0])
	for _, peer := range []string{"one", "two"} {
		entry, _ := log.next(peer)
		require.NotNil(t, entry)
		log.discardReservation(peer, nil)
		log.removeConn(peer)
	}
	require.Empty(t, log.retries)
	require.Empty(t, log.retryCursors)
	require.Empty(t, log.reservations)
	require.Empty(t, log.items)
}

func TestLeiosDependencySlowLookupCannotSendAfterOwnerReplacement(
	t *testing.T,
) {
	t.Parallel()
	o := newTestOuroborosWithLeiosDB(t)
	db := o.leiosDatabase()
	base := db.Blob()
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	store := &blockingLeiosManifestGetStore{
		BlobStore: base,
		started:   make(chan struct{}),
		release:   release,
	}
	db.SetBlobStore(store)
	t.Cleanup(func() { unblock(); _, drain := db.SetBlobStore(base); drain() })
	first := oleiosnotify.NewServer(protocol.ProtocolOptions{}, nil)
	second := oleiosnotify.NewServer(protocol.ProtocolOptions{}, nil)
	t.Cleanup(first.Stop)
	t.Cleanup(second.Stop)
	conn := gouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3002},
	}
	key := leiosConnectionIdString(conn)
	done := make(chan any)
	point := ocommon.NewPoint(42, bytes.Repeat([]byte{1}, 32))
	o.leiosEBLog.registerConn("holder", nil, nil)
	o.leiosEBLog.registerConn(key, first, nil)
	o.leiosEBLog.append(
		leiosForgedEBEntry{
			announcement:    []byte("header"),
			dependencyPoint: &point,
		},
	)
	type outcome struct {
		msg protocol.Message
		err error
	}
	finished := make(chan outcome, 1)
	go func() {
		msg, err := o.leiosnotifyServerRequestNext(
			oleiosnotify.CallbackContext{
				Server:             first,
				ConnectionId:       conn,
				ConnectionDoneChan: done,
			},
		)
		finished <- outcome{msg, err}
	}()
	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		t.Fatal("lookup did not enter blob store")
	}
	close(done)
	o.leiosEBLog.registerConn(key, second, nil)
	entry, _ := o.leiosEBLog.nextWhileConnected(key, second, nil)
	require.NotNil(t, entry)
	unblock()
	select {
	case result := <-finished:
		require.Nil(t, result.msg)
	case <-time.After(5 * time.Second):
		t.Fatal("old callback did not finish")
	}
	o.leiosEBLog.mu.Lock()
	require.Same(t, second, o.leiosEBLog.owners[key])
	require.Contains(t, o.leiosEBLog.reservations, key)
	o.leiosEBLog.mu.Unlock()
	o.leiosEBLog.removeConn(key)
	o.leiosEBLog.removeConn("holder")
}

type relayTimingFailureLedger struct {
	*fakeLeiosAnnouncementLedger
	fail atomic.Bool
}

func (l *relayTimingFailureLedger) SlotToTime(slot uint64) (time.Time, error) {
	if l.fail.Load() {
		return time.Time{}, errors.New("slot timing temporarily unavailable")
	}
	return l.fakeLeiosAnnouncementLedger.SlotToTime(slot)
}

func TestLeiosDeferredHeaderKeepsAdmittedExpiryWhenTimingFails(t *testing.T) {
	t.Parallel()
	for _, expired := range []bool{false, true} {
		t.Run(fmt.Sprintf("expired_%t", expired), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				base := &fakeLeiosAnnouncementLedger{slotTime: time.Now()}
				ledger := &relayTimingFailureLedger{
					fakeLeiosAnnouncementLedger: base,
				}
				o, point, manifest, tx, header := newDeferredRelayFixture(
					t,
					base,
				)
				o.leiosAnnouncementLedger = ledger
				o.markLeiosEndorserBlockRelayOffer(point)
				require.NoError(
					t,
					recordRelayedHeader(t, o, point, len(manifest), header),
				)
				require.NoError(
					t,
					o.storeLeiosEndorserBlock(
						point,
						manifest,
						nil,
						leiosStorePeerOffered,
					),
				)
				before, _ := o.leiosEBLog.next("downstream")
				require.Nil(t, before)
				require.Len(t, o.leiosRelayPendingAnnouncements, 1)
				ledger.fail.Store(true)
				if expired {
					time.Sleep(leiosNotifyRelayAnnouncementAge + time.Second)
				}
				require.NoError(
					t,
					o.storeLeiosEndorserBlock(
						point,
						manifest,
						[]cbor.RawMessage{tx},
						leiosStorePeerOffered,
					),
				)
				entry, _ := o.leiosEBLog.next("downstream")
				if expired {
					require.Nil(t, entry)
				} else {
					require.NotNil(t, entry)
					require.Equal(t, header, entry.announcement)
					o.leiosEBLog.complete("downstream", nil, true)
					offer, _ := o.leiosEBLog.next("downstream")
					require.Nil(t, offer, "new offers need a valid expiry")
				}
				require.Empty(
					t,
					o.leiosRelayPendingAnnouncements,
					"admitted headers must not remain parked",
				)
			})
		})
	}
}
