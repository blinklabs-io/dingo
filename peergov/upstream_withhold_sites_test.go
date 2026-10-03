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

package peergov

import (
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/assert"
)

const withholdSiteAddr = "10.2.0.1:3001"

// newWithholdSiteGov returns a governor holding one peer whose open client
// connection carries the withhold flag. With denied set the denial is live;
// otherwise it has expired and no reconcile has cleared the flag yet, which
// is the state where only the live-denial read restores eligibility.
func newWithholdSiteGov(
	source PeerSource,
	state PeerState,
	score float64,
	denied bool,
) (*PeerGovernor, ouroboros.ConnectionId) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MinHotPeers:           1,
		MinLedgerPeersForExit: 1,
		MinScoreThreshold:     0.5,
	})
	connId := ouroboros.ConnectionId{
		LocalAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.9"), Port: 3001},
		RemoteAddr: &net.TCPAddr{
			IP:   net.ParseIP("10.2.0.1"),
			Port: 3001,
		},
	}
	expiry := time.Now().Add(-time.Second)
	if denied {
		expiry = time.Now().Add(time.Minute)
	}
	pg.mu.Lock()
	pg.peers = []*Peer{{
		Address:           withholdSiteAddr,
		NormalizedAddress: withholdSiteAddr,
		Source:            source,
		State:             state,
		PerformanceScore:  score,
		FirstSeen:         time.Now(),
		Connection: &PeerConnection{
			Id:               connId,
			IsClient:         true,
			UpstreamWithheld: true,
		},
	}}
	pg.denyList[withholdSiteAddr] = expiry
	pg.mu.Unlock()
	return pg, connId
}

// addColdBootstrapPeer gives the governor a bootstrap source so bootstrap
// exit and recovery evaluate their peer counts.
func addColdBootstrapPeer(pg *PeerGovernor) {
	pg.peers = append(pg.peers, &Peer{
		Address:           "45.0.0.1:3001",
		NormalizedAddress: "45.0.0.1:3001",
		Source:            PeerSourceTopologyBootstrapPeer,
		State:             PeerStateCold,
	})
}

// Every governor decision that counts or selects upstreams must exclude a
// withheld connection while its denial is live, and include it again as soon
// as the denial expires, before reconcile clears the stored flag.
func TestWithheldConnectionCallSites(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		source PeerSource
		state  PeerState
		score  float64
		// usable reports whether the governor treated the peer as a usable
		// upstream.
		usable func(*PeerGovernor, ouroboros.ConnectionId) bool
	}{
		{
			name:   "countEligibleUpstreams",
			source: PeerSourceP2PGossip,
			state:  PeerStateHot,
			usable: func(pg *PeerGovernor, _ ouroboros.ConnectionId) bool {
				pg.mu.Lock()
				defer pg.mu.Unlock()
				return pg.countEligibleUpstreamsLocked() == 1
			},
		},
		{
			name:   "IsConfiguredRootConnection",
			source: PeerSourceTopologyLocalRoot,
			state:  PeerStateWarm,
			usable: func(pg *PeerGovernor, id ouroboros.ConnectionId) bool {
				return pg.IsConfiguredRootConnection(id)
			},
		},
		{
			name:   "bootstrapExitSuccessorCount",
			source: PeerSourceP2PGossip,
			state:  PeerStateWarm,
			usable: func(pg *PeerGovernor, _ ouroboros.ConnectionId) bool {
				pg.mu.Lock()
				defer pg.mu.Unlock()
				return pg.bootstrapExitSuccessorCountLocked() == 1
			},
		},
		{
			name:   "shouldExitBootstrap ledger peer count",
			source: PeerSourceP2PLedger,
			state:  PeerStateWarm,
			usable: func(pg *PeerGovernor, _ ouroboros.ConnectionId) bool {
				pg.mu.Lock()
				defer pg.mu.Unlock()
				addColdBootstrapPeer(pg)
				exit, _ := pg.shouldExitBootstrap()
				return exit
			},
		},
		{
			name:   "bootstrap recovery warm candidates",
			source: PeerSourceP2PGossip,
			state:  PeerStateWarm,
			usable: func(pg *PeerGovernor, _ ouroboros.ConnectionId) bool {
				pg.mu.Lock()
				defer pg.mu.Unlock()
				addColdBootstrapPeer(pg)
				pg.bootstrapExited = true
				pg.checkBootstrapRecoveryLocked()
				// A usable warm candidate keeps bootstrap exited.
				return pg.bootstrapExited
			},
		},
		{
			name:   "isPromotableWarmNonRootPeer",
			source: PeerSourceP2PGossip,
			state:  PeerStateWarm,
			score:  1,
			usable: func(pg *PeerGovernor, _ ouroboros.ConnectionId) bool {
				pg.mu.Lock()
				defer pg.mu.Unlock()
				return pg.isPromotableWarmNonRootPeerLocked(pg.peers[0])
			},
		},
		{
			// A low-scoring hot peer is churned out unless it is the last
			// eligible upstream.
			name:   "gossipChurn last eligible upstream",
			source: PeerSourceP2PGossip,
			state:  PeerStateHot,
			score:  0.1,
			usable: func(pg *PeerGovernor, _ ouroboros.ConnectionId) bool {
				pg.gossipChurn()
				pg.mu.Lock()
				defer pg.mu.Unlock()
				return pg.peers[0].State == PeerStateHot
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pg, id := newWithholdSiteGov(tc.source, tc.state, tc.score, true)
			assert.False(t, tc.usable(pg, id), "withheld while denied")
			pg, id = newWithholdSiteGov(tc.source, tc.state, tc.score, false)
			assert.True(t, tc.usable(pg, id), "usable once the denial expires")
		})
	}
}
