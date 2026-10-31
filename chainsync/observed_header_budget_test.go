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

package chainsync

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// sizedObservedHeader is a header whose CBOR has a chosen length, so a test
// can drive the observed history's byte accounting without real headers.
type sizedObservedHeader struct {
	hash     lcommon.Blake2b256
	prevHash lcommon.Blake2b256
	slot     uint64
	cbor     []byte
}

func (h sizedObservedHeader) Hash() lcommon.Blake2b256     { return h.hash }
func (h sizedObservedHeader) PrevHash() lcommon.Blake2b256 { return h.prevHash }
func (h sizedObservedHeader) BlockNumber() uint64          { return h.slot }
func (h sizedObservedHeader) SlotNumber() uint64           { return h.slot }
func (h sizedObservedHeader) IssuerVkey() lcommon.IssuerVkey {
	return lcommon.IssuerVkey{}
}
func (h sizedObservedHeader) BlockBodySize() uint64 { return 0 }
func (h sizedObservedHeader) Era() lcommon.Era      { return babbage.EraBabbage }
func (h sizedObservedHeader) Cbor() []byte          { return h.cbor }
func (h sizedObservedHeader) BlockBodyHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

func observedBudgetConnId(port int) ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port},
	}
}

// newObservedBudgetState returns a state whose observed-header count limit is
// the mainnet Genesis candidate-fragment bound (129600 window slots plus
// k=2160), so only the byte budgets can stop a dense peer.
func newObservedBudgetState(t *testing.T) *State {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	cfg := DefaultConfig()
	cfg.PromRegistry = prometheus.NewRegistry()
	cfg.MaxClients = 16
	cfg.ObservedHeaderLimitFunc = func() int { return 129600 + 2160 }
	return NewStateWithConfig(bus, nil, cfg)
}

// recordDenseObservedHeaders records count linked headers of cborLen bytes
// each for conn and returns their hashes in order.
func recordDenseObservedHeaders(
	t *testing.T,
	s *State,
	conn ouroboros.ConnectionId,
	count int,
	cborLen int,
) [][]byte {
	t.Helper()
	cbor := make([]byte, cborLen)
	hashes := make([][]byte, 0, count)
	var prev lcommon.Blake2b256
	seed := make([]byte, 16)
	binary.BigEndian.PutUint64(seed, uint64(conn.RemoteAddr.(*net.TCPAddr).Port)) //nolint:forcetypeassert,gosec
	for i := range count {
		binary.BigEndian.PutUint64(seed[8:], uint64(i)) //nolint:gosec
		hash := lcommon.NewBlake2b256(append([]byte(nil), seed...))
		s.RecordObservedHeader(ObservedHeader{
			ConnectionId: conn,
			BlockHeader: sizedObservedHeader{
				hash:     hash,
				prevHash: prev,
				slot:     uint64(i + 1), //nolint:gosec
				cbor:     cbor,
			},
			Point: ocommon.NewPoint(uint64(i+1), hash.Bytes()), //nolint:gosec
			Type:  1,
		})
		hashes = append(hashes, hash.Bytes())
		prev = hash
	}
	return hashes
}

// retainedObservedHeaderBytes charges every retained record its CBOR plus the
// record overhead, independently of the history's own running totals.
func retainedObservedHeaderBytes(
	s *State,
	conn ouroboros.ConnectionId,
) (int, int) {
	s.observedHeadersMutex.RLock()
	defer s.observedHeadersMutex.RUnlock()
	chainHistory := s.observedHeaders[conn]
	if chainHistory == nil {
		return 0, 0
	}
	total := 0
	for _, record := range chainHistory.byHash {
		total += observedHeaderRecordOverhead +
			len(record.header.BlockHeader.Cbor())
	}
	return total, len(chainHistory.byHash)
}

func TestObservedHeaderHistoryBoundedByBytesPerConnection(t *testing.T) {
	s := newObservedBudgetState(t)
	conn := observedBudgetConnId(4001)
	require.True(t, s.AddClientConnId(conn))

	const cborLen = 4096
	perRecord := observedHeaderRecordOverhead + cborLen
	count := maxObservedHeaderBytesPerConn/perRecord + 2000
	hashes := recordDenseObservedHeaders(t, s, conn, count, cborLen)

	retained, records := retainedObservedHeaderBytes(s, conn)
	require.LessOrEqual(
		t,
		retained,
		maxObservedHeaderBytesPerConn,
		"a dense peer within the Genesis header count must not retain more than the per-connection byte budget",
	)
	require.Equal(t, maxObservedHeaderBytesPerConn/perRecord, records)
	_, _, ok := s.LookupObservedHeader(conn, hashes[0])
	require.False(t, ok, "the oldest header is evicted first")
	_, _, ok = s.LookupObservedHeader(conn, hashes[len(hashes)-1])
	require.True(t, ok, "the newest header is retained")
}

func TestObservedHeaderHistoryBoundedByBytesAcrossConnections(t *testing.T) {
	s := newObservedBudgetState(t)
	const cborLen = 4096
	perRecord := observedHeaderRecordOverhead + cborLen
	perConn := maxObservedHeaderBytesPerConn / perRecord

	small := observedBudgetConnId(5000)
	require.True(t, s.AddClientConnId(small))
	smallHashes := recordDenseObservedHeaders(t, s, small, 256, cborLen)

	// ConnectionId holds address pointers, so each identifier is built once
	// and reused for every lookup.
	conns := make(
		[]ouroboros.ConnectionId,
		maxObservedHeaderBytesTotal/maxObservedHeaderBytesPerConn+2,
	)
	for i := range conns {
		conns[i] = observedBudgetConnId(5001 + i)
		require.True(t, s.AddClientConnId(conns[i]))
		recordDenseObservedHeaders(t, s, conns[i], perConn, cborLen)
	}

	total := 0
	for _, conn := range conns {
		retained, records := retainedObservedHeaderBytes(s, conn)
		require.NotZero(t, records)
		total += retained
	}
	smallRetained, smallRecords := retainedObservedHeaderBytes(s, small)
	total += smallRetained
	require.LessOrEqual(
		t,
		total,
		maxObservedHeaderBytesTotal,
		"dense peers together must not retain more than the total byte budget",
	)
	require.Equal(
		t,
		len(smallHashes),
		smallRecords,
		"total pressure trims the largest histories before a small one",
	)
}

func TestObservedHeaderHistoryKeepsDenseCandidateWithinBudget(t *testing.T) {
	s := newObservedBudgetState(t)
	conn := observedBudgetConnId(6001)
	require.True(t, s.AddClientConnId(conn))

	// A mainnet-sized header, and more linked headers than 4k on mainnet,
	// still fit the per-connection budget, so the intersection survives.
	const cborLen = 859
	const count = 4*2160 + 1
	hashes := recordDenseObservedHeaders(t, s, conn, count, cborLen)
	_, records := retainedObservedHeaderBytes(s, conn)
	require.Equal(t, count, records)
	_, _, ok := s.LookupObservedHeader(conn, hashes[0])
	require.True(t, ok)
}

func TestObservedHeaderHistoryReleasesBytesOnClear(t *testing.T) {
	s := newObservedBudgetState(t)
	conn := observedBudgetConnId(7001)
	require.True(t, s.AddClientConnId(conn))
	hashes := recordDenseObservedHeaders(t, s, conn, 10, 100)

	s.ClearObservedHeaderHistoryFrom(conn, ocommon.NewPoint(6, hashes[5]))
	retained, records := retainedObservedHeaderBytes(s, conn)
	require.Equal(t, 5, records)
	s.observedHeadersMutex.RLock()
	require.Equal(t, retained, s.observedHeaderBytes)
	s.observedHeadersMutex.RUnlock()

	s.ClearObservedHeaderHistory(conn)
	s.observedHeadersMutex.RLock()
	require.Zero(t, s.observedHeaderBytes)
	s.observedHeadersMutex.RUnlock()
}
