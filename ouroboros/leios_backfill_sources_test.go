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
	"fmt"
	"testing"
	"time"

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
	connId := namedConnId("announcer")
	owner := &oleiosnotify.Client{}
	for slot := uint64(0); slot <= leiosEndorserBlockCacheMaxEntries; slot++ {
		o.recordLeiosBackfillSource(
			ocommon.Point{Slot: slot, Hash: []byte{0x01}},
			connId,
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
