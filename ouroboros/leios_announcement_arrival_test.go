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
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func (f *fakeLeiosVoteHandler) arrivalsSnapshot() []handledArrival {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]handledArrival(nil), f.arrivals...)
}

// TestLeiosNotifyAnnouncementReportsVerifiedArrival covers
// blinklabs-io/dingo#6462: an accepted LeiosNotify announcement reports when
// its ranking-block header reached this node, so the vote manager can time
// L_hdr from it rather than from the later chainsync admission. A header that
// failed or deferred verification, or carries a stale op-cert counter, must
// not report: the arrival time moves the vote gate.
func TestLeiosNotifyAnnouncementReportsVerifiedArrival(t *testing.T) {
	t.Parallel()
	raw := testDijkstraAnnouncementHeaderRaw(t)
	header, err := decodeLeiosAnnouncementHeader(raw)
	require.NoError(t, err)
	wantEb, _, ok := header.LeiosAnnouncement()
	require.True(t, ok)
	wantRb := lcommon.NewBlake2b256(header.Hash().Bytes())

	newAcceptor := func(
		t *testing.T,
		announcementLedger *fakeLeiosAnnouncementLedger,
	) (*Ouroboros, *fakeLeiosVoteHandler) {
		t.Helper()
		o := newOuroboros(OuroborosConfig{
			EnableLeios:             true,
			LeiosAnnouncementLedger: announcementLedger,
		})
		votes := &fakeLeiosVoteHandler{}
		require.NoError(t, o.SetLeiosVotes(votes))
		return o, votes
	}
	freshLedger := func() *fakeLeiosAnnouncementLedger {
		return &fakeLeiosAnnouncementLedger{
			currentSlot: 10,
			slotTime:    time.Now().Add(-time.Minute),
			staleness:   ledger.LeiosAnnouncementFreshOCIN,
		}
	}

	t.Run("reports the receipt time of a verified header", func(t *testing.T) {
		t.Parallel()
		announcementLedger := freshLedger()
		o, votes := newAcceptor(t, announcementLedger)
		before := time.Now()
		require.NoError(t, o.acceptLeiosAnnouncement(raw, "peer-a"))
		after := time.Now()
		// A second peer's copy of the same header is already known and
		// reports nothing new.
		require.NoError(t, o.acceptLeiosAnnouncement(raw, "peer-b"))

		arrivals := votes.arrivalsSnapshot()
		require.Len(t, arrivals, 1)
		require.Equal(t, header.SlotNumber(), arrivals[0].slot)
		require.Equal(t, wantRb, arrivals[0].rbHash)
		require.Equal(t, wantEb, arrivals[0].ebHash)
		require.False(t, arrivals[0].arrivedAt.Before(before))
		require.False(t, arrivals[0].arrivedAt.After(after))
		require.Equal(t, 1, announcementLedger.validated)
	})

	t.Run("does not report a header that failed verification", func(t *testing.T) {
		t.Parallel()
		announcementLedger := freshLedger()
		announcementLedger.err = errors.New("invalid VRF proof")
		o, votes := newAcceptor(t, announcementLedger)
		require.Error(t, o.acceptLeiosAnnouncement(raw, "peer-a"))
		require.Empty(t, votes.arrivalsSnapshot())
	})

	t.Run("does not report a header whose verification was deferred", func(t *testing.T) {
		t.Parallel()
		probe := newTestOuroborosWithLeiosDB(t)
		deferredErr := probe.ledgerState.ValidateChainSelectionHeaderCrypto(header)
		require.True(t, ledger.IsHeaderVerificationDeferred(deferredErr))
		announcementLedger := freshLedger()
		announcementLedger.err = deferredErr
		o, votes := newAcceptor(t, announcementLedger)
		require.Error(t, o.acceptLeiosAnnouncement(raw, "peer-a"))
		require.Empty(t, votes.arrivalsSnapshot())
	})

	t.Run("does not report a stale op-cert counter", func(t *testing.T) {
		t.Parallel()
		announcementLedger := freshLedger()
		announcementLedger.staleness = ledger.LeiosAnnouncementStaleOCIN
		o, votes := newAcceptor(t, announcementLedger)
		require.NoError(t, o.acceptLeiosAnnouncement(raw, "peer-a"))
		require.Empty(t, votes.arrivalsSnapshot())
	})
}
