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

package chainsync_test

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainsync"
	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fetchPeers is a State with peers that all announced the same range start.
type fetchPeers struct {
	state *chainsync.State
	start ocommon.Point
	conns []ouroboros.ConnectionId
}

func newFetchPeers(t *testing.T, peers int) *fetchPeers {
	t.Helper()
	cfg := chainsync.DefaultConfig()
	cfg.MaxClients = peers
	cfg.PromRegistry = prometheus.NewRegistry()
	f := &fetchPeers{
		state: chainsync.NewStateWithConfig(nil, nil, cfg),
		start: ocommon.NewPoint(500, []byte("range-start")),
	}
	for i := range peers {
		connId := newTestConnId(uint(i + 1))
		require.True(t, f.state.AddClientConnId(connId))
		f.state.RecordObservedHeader(chainsync.ObservedHeader{
			ConnectionId: connId,
			Point:        f.start,
			Tip:          ochainsync.Tip{Point: f.start, BlockNumber: 5},
			BlockHeader: testBlockHeader{
				hash:        lcommon.NewBlake2b256(f.start.Hash),
				prevHash:    lcommon.NewBlake2b256([]byte("prev")),
				blockNumber: 5,
				slot:        f.start.Slot,
			},
		})
		f.conns = append(f.conns, connId)
	}
	return f
}

// sample records a delivery model for peer i: first-block latency and the
// time it took to move one megabyte.
func (f *fetchPeers) sample(
	i int,
	latency time.Duration,
	perMegabyte time.Duration,
) {
	for range 50 {
		f.state.RecordBlockfetchLatency(f.conns[i], latency)
		f.state.RecordBlockfetchThroughput(f.conns[i], 1<<20, perMegabyte)
	}
}

// selectFrom returns the index of the peer chosen when peer origin sent the
// header, or -1 for a connection the test did not create.
func (f *fetchPeers) selectFrom(origin int) int {
	return f.indexOf(f.state.SelectBlockfetchPeer(f.conns[origin], f.start))
}

// indexOf maps a connection back to its peer index: a ConnectionId compares by
// address pointer, so failures would otherwise print as pointers.
func (f *fetchPeers) indexOf(connId ouroboros.ConnectionId) int {
	for i, c := range f.conns {
		if c == connId {
			return i
		}
	}
	return -1
}

func TestSelectBlockfetchPeerPrefersFasterPeer(t *testing.T) {
	t.Parallel()
	f := newFetchPeers(t, 2)
	f.sample(0, 300*time.Millisecond, 50*time.Millisecond)
	f.sample(1, 40*time.Millisecond, 50*time.Millisecond)

	assert.Equal(
		t,
		1,
		f.selectFrom(0),
		"the fast peer must be fetched from although the slow one sent the header",
	)
}

// Latency alone would pick peer 0; the per-byte cost makes peer 1 deliver a
// real batch sooner.
func TestSelectBlockfetchPeerWeighsThroughput(t *testing.T) {
	t.Parallel()
	f := newFetchPeers(t, 2)
	f.sample(0, 10*time.Millisecond, 4*time.Second)
	f.sample(1, 80*time.Millisecond, 20*time.Millisecond)

	assert.Equal(t, 1, f.selectFrom(0))
}

func TestSelectBlockfetchPeerKeepsHeaderPeerWhenOnlyHolder(t *testing.T) {
	t.Parallel()
	f := newFetchPeers(t, 2)
	f.sample(0, 500*time.Millisecond, 2*time.Second)
	f.sample(1, 10*time.Millisecond, 10*time.Millisecond)
	elsewhere := ocommon.NewPoint(900, []byte("other-range"))

	assert.Equal(
		t,
		0,
		f.indexOf(f.state.SelectBlockfetchPeer(f.conns[0], elsewhere)),
		"a peer that does not hold the range start is not a candidate",
	)
}

func TestSelectBlockfetchPeerUnsampledOnlyHolderIsUsed(t *testing.T) {
	t.Parallel()
	f := newFetchPeers(t, 1)

	assert.Equal(t, 0, f.selectFrom(0))
}

// announce records point as announced by every peer.
func (f *fetchPeers) announce(point ocommon.Point) {
	for _, connId := range f.conns {
		f.state.RecordObservedHeader(chainsync.ObservedHeader{
			ConnectionId: connId,
			Point:        point,
			Tip:          ochainsync.Tip{Point: point, BlockNumber: 5},
			BlockHeader: testBlockHeader{
				hash:        lcommon.NewBlake2b256(point.Hash),
				prevHash:    lcommon.NewBlake2b256([]byte("prev")),
				blockNumber: 5,
				slot:        point.Slot,
			},
		})
	}
}

// rangePoints returns n distinct points, each announced by every peer.
func (f *fetchPeers) rangePoints(n int) []ocommon.Point {
	points := make([]ocommon.Point, n)
	for i := range points {
		points[i] = ocommon.NewPoint(
			uint64(1000+i),
			[]byte{'r', byte(i), byte(i >> 8)},
		)
		f.announce(points[i])
	}
	return points
}

// An unsampled peer is not shut out for lacking a sample: it is selected for
// some ranges, and only for a small share of them, so it can earn one.
func TestSelectBlockfetchPeerExploresUnsampledPeerSparingly(t *testing.T) {
	t.Parallel()
	f := newFetchPeers(t, 2)
	f.sample(0, 40*time.Millisecond, 50*time.Millisecond)

	points := f.rangePoints(64)
	explored := 0
	for _, point := range points {
		if f.indexOf(f.state.SelectBlockfetchPeer(f.conns[0], point)) == 1 {
			explored++
		}
	}

	assert.Positive(t, explored, "the unsampled peer must eventually be tried")
	assert.LessOrEqual(
		t,
		explored,
		len(points)/4,
		"exploration must stay a small share of selections",
	)
}

// The ledger can ask about one range more than once (when a deep catch-up
// pipeline defers to the policy and again when the next batch starts), so the
// answer for a range must not depend on how many times it was asked.
func TestSelectBlockfetchPeerAnswersEachRangeConsistently(t *testing.T) {
	t.Parallel()
	f := newFetchPeers(t, 2)
	f.sample(0, 40*time.Millisecond, 50*time.Millisecond)

	for _, point := range f.rangePoints(32) {
		first := f.state.SelectBlockfetchPeer(f.conns[0], point)
		again := f.state.SelectBlockfetchPeer(f.conns[0], point)
		assert.Equal(
			t,
			f.indexOf(first),
			f.indexOf(again),
			"range at slot %d answered differently when asked again",
			point.Slot,
		)
	}
}

// Peers whose measurements differ by less than the band are equals: the
// choice between them must not follow the noise.
func TestSelectBlockfetchPeerDoesNotFlapBetweenNearEqualPeers(t *testing.T) {
	t.Parallel()
	f := newFetchPeers(t, 2)
	f.sample(0, 100*time.Millisecond, 50*time.Millisecond)
	f.sample(1, 103*time.Millisecond, 50*time.Millisecond)
	first := f.selectFrom(0)

	for round := range 40 {
		// Swap which peer measures marginally faster.
		fast, slow := 0, 1
		if round%2 == 1 {
			fast, slow = 1, 0
		}
		f.sample(fast, 100*time.Millisecond, 50*time.Millisecond)
		f.sample(slow, 103*time.Millisecond, 50*time.Millisecond)
		assert.Equal(
			t,
			first,
			f.selectFrom(round%2),
			"round %d: selection moved between near-equal peers",
			round,
		)
	}
}

func TestSelectBlockfetchPeerCountsDecisionsAndHandoffs(t *testing.T) {
	t.Parallel()
	cfg := chainsync.DefaultConfig()
	cfg.MaxClients = 2
	reg := prometheus.NewRegistry()
	cfg.PromRegistry = reg
	f := newFetchPeers(t, 2)
	// newFetchPeers installs its own registry; build the measured State on
	// this one instead so the metric can be read back.
	f.state = chainsync.NewStateWithConfig(nil, nil, cfg)
	for i := range f.conns {
		f.conns[i] = newTestConnId(uint(i + 1))
		require.True(t, f.state.AddClientConnId(f.conns[i]))
		f.state.RecordObservedHeader(chainsync.ObservedHeader{
			ConnectionId: f.conns[i],
			Point:        f.start,
			Tip:          ochainsync.Tip{Point: f.start, BlockNumber: 5},
			BlockHeader: testBlockHeader{
				hash:        lcommon.NewBlake2b256(f.start.Hash),
				prevHash:    lcommon.NewBlake2b256([]byte("prev")),
				blockNumber: 5,
				slot:        f.start.Slot,
			},
		})
	}
	f.sample(0, 300*time.Millisecond, 50*time.Millisecond)
	f.sample(1, 40*time.Millisecond, 50*time.Millisecond)

	require.Equal(t, 1, f.selectFrom(0))
	require.Equal(t, 1, f.selectFrom(0))
	// Peer 0 becomes the fast one: the next selection hands off to it.
	f.sample(0, 5*time.Millisecond, 5*time.Millisecond)
	require.Equal(t, 0, f.selectFrom(1))

	families, err := reg.Gather()
	require.NoError(t, err)
	var decisions, handoffs float64
	for _, family := range families {
		switch family.GetName() {
		case "dingo_blockfetch_peer_selections_total":
			for _, m := range family.GetMetric() {
				decisions += m.GetCounter().GetValue()
			}
		case "dingo_blockfetch_peer_handoffs_total":
			handoffs += family.GetMetric()[0].GetCounter().GetValue()
		}
	}
	assert.Equal(t, float64(3), decisions)
	assert.Equal(
		t,
		float64(1),
		handoffs,
		"only the change of peer between selections is a handoff",
	)
}
