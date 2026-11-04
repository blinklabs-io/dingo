// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chainsync_test

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/chainsync"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestAuthenticatedDeliveryHistory(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"raw isolation and copies", "replaced session", "evicted header", "withdrawn header"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s := newTestState(t, newTestEventBus(t), chainsync.DefaultConfig())
			conn := newTestConnId(440)
			require.True(t, s.AddClientConnId(conn))
			session := s.ClientSession(conn)
			header := func(number uint64) chainsync.ObservedHeader {
				hash := sha256.Sum256([]byte(fmt.Sprint(number)))
				point := ocommon.NewPoint(number, hash[:])
				return chainsync.ObservedHeader{
					ConnectionId: conn, Point: point, BlockNumber: number,
					Tip: ochainsync.Tip{Point: point, BlockNumber: number},
					BlockHeader: testBlockHeader{
						hash: lcommon.NewBlake2b256(
							hash[:],
						), prevHash: lcommon.NewBlake2b256([]byte("parent")),
						blockNumber: number, slot: number,
					},
				}
			}
			h := header(1)
			s.RecordObservedHeaderForSession(session, h)
			_, ok := s.LatestAuthenticatedDeliveryForSession(session, conn)
			require.False(
				t,
				ok,
				"raw ancestry must not become an authenticated delivery",
			)
			h.HeaderCryptoVerificationGeneration = 217
			s.RecordAuthenticatedDeliveryForSession(session, h)
			raw, _, ok := s.LookupObservedHeader(conn, h.Point.Hash)
			require.True(t, ok)
			require.Zero(
				t,
				raw.HeaderCryptoVerificationGeneration,
				"delivery authentication must not authorize raw ancestry for ledger admission",
			)
			got, ok := s.LatestAuthenticatedDeliveryForSession(session, conn)
			require.True(t, ok)
			require.EqualValues(t, 217, got.HeaderCryptoVerificationGeneration)
			switch mode {
			case "raw isolation and copies":
				got.Point.Hash[0] ^= 0xff
				got.Tip.Point.Hash[1] ^= 0xff
				next := header(2)
				next.HeaderCryptoVerificationGeneration = 99
				s.RecordObservedHeaderForSession(session, next)
				got, ok = s.LatestAuthenticatedDeliveryForSession(session, conn)
				require.True(t, ok)
				require.Equal(
					t,
					h.Point,
					got.Point,
					"a raw child and caller mutation must not replace trusted delivery",
				)
				require.Equal(t, h.Tip, got.Tip)
			case "replaced session":
				s.RemoveClientConnId(conn)
				require.True(t, s.AddClientConnId(conn))
				s.RecordAuthenticatedDeliveryForSession(session, h)
				_, ok = s.LatestAuthenticatedDeliveryForSession(session, conn)
				require.False(t, ok)
				_, ok = s.LatestAuthenticatedDeliveryForSession(
					s.ClientSession(conn),
					conn,
				)
				require.False(
					t,
					ok,
					"late authentication must not populate a replacement bearer",
				)
				newSession := s.ClientSession(conn)
				h.HeaderCryptoVerificationGeneration = 237
				s.RecordAuthenticatedDeliveryForSession(newSession, h)
				got, ok = s.LatestAuthenticatedDeliveryForSession(
					newSession,
					conn,
				)
				require.True(t, ok)
				require.EqualValues(
					t,
					237,
					got.HeaderCryptoVerificationGeneration,
				)
				_, ok = s.LatestAuthenticatedDeliveryForSession(session, conn)
				require.False(
					t,
					ok,
					"an old session must not read a new bearer's delivery",
				)
			case "evicted header":
				for number := uint64(2); number < 300; number++ {
					s.RecordObservedHeaderForSession(session, header(number))
				}
				_, _, retained := s.LookupObservedHeader(conn, h.Point.Hash)
				require.False(
					t,
					retained,
					"the control must really evict the old bounded record",
				)
				_, ok = s.LatestAuthenticatedDeliveryForSession(session, conn)
				require.False(
					t,
					ok,
					"authentication metadata must not retain a header outside the byte/count budget",
				)
			case "withdrawn header":
				s.ClearObservedHeaderHistoryFrom(conn, h.Point)
				_, ok = s.LatestAuthenticatedDeliveryForSession(session, conn)
				require.False(
					t,
					ok,
					"a withdrawn header must not be used by a parked callback",
				)
			}
		})
	}
}

func TestAuthenticatedDeliveryHistoryRetainsAuthenticatedPrefixAfterTruncation(t *testing.T) {
	t.Parallel()
	s := newTestState(t, newTestEventBus(t), chainsync.DefaultConfig())
	conn := newTestConnId(441)
	require.True(t, s.AddClientConnId(conn))
	session := s.ClientSession(conn)
	var headers []chainsync.ObservedHeader
	for number := uint64(1); number <= 4; number++ {
		hash := sha256.Sum256([]byte(fmt.Sprint(number)))
		h := chainsync.ObservedHeader{ConnectionId: conn, Point: ocommon.NewPoint(number, hash[:]), BlockNumber: number,
			BlockHeader:                        testBlockHeader{hash: lcommon.NewBlake2b256(hash[:]), prevHash: lcommon.NewBlake2b256([]byte("parent")), blockNumber: number, slot: number},
			HeaderCryptoVerificationGeneration: 217}
		headers = append(headers, h)
		if number == 1 || number == 3 {
			s.RecordAuthenticatedDeliveryForSession(session, h)
		} else {
			s.RecordObservedHeaderForSession(session, h)
		}
	}
	s.ClearObservedHeaderHistoryFrom(conn, headers[2].Point)
	got, ok := s.LatestAuthenticatedDeliveryForSession(session, conn)
	require.True(t, ok, "truncation must preserve the authenticated predecessor")
	require.Equal(t, headers[0].Point, got.Point, "the retained raw child must not become authenticated")
	require.EqualValues(t, 217, got.HeaderCryptoVerificationGeneration)
	s.ClearObservedHeaderHistoryFrom(conn, headers[0].Point)
	_, ok = s.LatestAuthenticatedDeliveryForSession(session, conn)
	require.False(t, ok, "withdrawing the entire prefix must clear authentication")
}
