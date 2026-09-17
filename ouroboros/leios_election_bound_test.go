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
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func electionAnnouncement(
	t *testing.T,
	slot, blockNo uint64,
	issuer byte,
) []byte {
	t.Helper()
	raw := testDijkstraAnnouncementHeaderRawFor(
		t,
		slot,
		lcommon.Blake2b256{0xaa},
		1234,
	)
	var top, body []cbor.RawMessage
	_, err := cbor.Decode(raw, &top)
	require.NoError(t, err)
	if len(top) == 0 {
		t.Fatal("missing announcement header body")
		return nil
	}
	_, err = cbor.Decode(top[0], &body)
	require.NoError(t, err)
	if len(body) < 4 {
		t.Fatal("incomplete announcement header body")
		return nil
	}
	body[0] = mustCbor(t, blockNo)
	body[3] = mustCbor(t, bytes.Repeat([]byte{issuer}, 32))
	top[0] = mustCbor(t, body)
	return mustCbor(t, top)
}

func TestLeiosAnnouncementElectionBoundAcrossSources(t *testing.T) {
	// Header cryptography is supplied by the existing ledger fixture; this
	// exercises the real decode, time-window, pruning, recording and relay path.
	ledger := &fakeLeiosAnnouncementLedger{
		currentSlot: 11,
		slotTime:    time.Now().Add(-time.Minute),
	}
	o := newOuroboros(
		OuroborosConfig{EnableLeios: true, LeiosAnnouncementLedger: ledger},
	)
	o.leiosEBLog.registerConn("observer")
	first := electionAnnouncement(t, 10, 1, 1)
	require.NoError(t, o.acceptLeiosAnnouncement(first, "connection-a"))
	require.NoError(
		t,
		o.acceptLeiosAnnouncement(
			electionAnnouncement(t, 10, 2, 1),
			"connection-b",
		),
	)
	// Relaying an already-known header from a new source must not consume
	// another slot or fail after the election's two distinct headers are seen.
	require.NoError(t, o.acceptLeiosAnnouncement(first, "connection-c"))
	err := o.acceptLeiosAnnouncement(
		electionAnnouncement(t, 10, 3, 1),
		"connection-c",
	)
	require.ErrorContains(t, err, "third distinct")
	require.Len(t, o.leiosAnnouncements, 2)
	require.Len(
		t,
		o.leiosEBLog.items,
		2,
		"rejected announcement must not be relayed",
	)

	// Both parts of election identity matter: another issuer at the same
	// slot and the same issuer at another slot each get their own budget.
	for _, election := range []struct {
		slot   uint64
		issuer byte
	}{{10, 2}, {11, 1}} {
		require.NoError(
			t,
			o.acceptLeiosAnnouncement(
				electionAnnouncement(t, election.slot, 1, election.issuer),
				"connection-c",
			),
		)
		require.NoError(
			t,
			o.acceptLeiosAnnouncement(
				electionAnnouncement(t, election.slot, 2, election.issuer),
				"connection-c",
			),
		)
	}
	require.Len(t, o.leiosAnnouncements, 6)
}
