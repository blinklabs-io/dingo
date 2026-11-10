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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	databaseTypes "github.com/blinklabs-io/dingo/database/types"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// blockingLeiosManifestGetStore parks the first Leios manifest read until
// released, simulating a slow remote blob backend.
type blockingLeiosManifestGetStore struct {
	blob.BlobStore
	started chan struct{}
	release <-chan struct{}
	claimed atomic.Bool
}

func (s *blockingLeiosManifestGetStore) Get(
	txn databaseTypes.Txn,
	key []byte,
) ([]byte, error) {
	if bytes.HasPrefix(key, []byte(databaseTypes.LeiosEBManifestKeyPrefix)) {
		if s.claimed.CompareAndSwap(false, true) {
			close(s.started)
			<-s.release
		}
	}
	return s.BlobStore.Get(txn, key)
}

// A slow blob read made while deciding whether a relayed announcement must
// wait for its manifest must not stall other announcements.
func TestLeiosAnnouncementBlobReadDoesNotBlockOtherAnnouncements(
	t *testing.T,
) {
	t.Parallel()
	o := newTestOuroborosWithLeiosDB(t)
	o.leiosAnnouncementLedger = &fakeLeiosAnnouncementLedger{
		currentSlot: 10,
		slotTime:    time.Now().Add(-time.Minute),
	}
	db := o.leiosDatabase()
	require.NotNil(t, db)
	baseStore := db.Blob()
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	store := &blockingLeiosManifestGetStore{
		BlobStore: baseStore,
		started:   make(chan struct{}),
		release:   release,
	}
	db.SetBlobStore(store)
	t.Cleanup(func() {
		unblock()
		_, drain := db.SetBlobStore(baseStore)
		drain()
	})

	record := func(slot uint64, hashByte byte, relay bool) error {
		point := ocommon.NewPoint(slot, bytes.Repeat([]byte{hashByte}, 32))
		headerRaw := testDijkstraAnnouncementHeaderRawFor(
			t, slot, testEbHash(point), 100,
		)
		header, err := gdijkstra.NewDijkstraBlockHeaderFromCbor(headerRaw)
		require.NoError(t, err)
		return o.recordLeiosAnnouncement(
			headerRaw, header.Cbor(), testEbHash(point), 100, header,
			"upstream", relay,
		)
	}

	slow := make(chan error, 1)
	go func() { slow <- record(42, 0xa1, true) }()
	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		t.Fatal("announcement never reached the blob store")
	}

	fast := make(chan error, 1)
	go func() { fast <- record(43, 0xb2, false) }()
	select {
	case err := <-fast:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("announcement admission stalled behind a blob read")
	}

	unblock()
	require.NoError(t, <-slow)
}
