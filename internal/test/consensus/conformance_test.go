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

// Package consensus_test drives the upstream
// consensus-conformance corpus (vectors + replay harness embedded in
// ouroboros-mock) against dingo's chain-selection subsystem. All
// recording infrastructure (capture sidecar, docker scenarios,
// scripts) lives upstream; this file is the dingo-side adapter +
// test entrypoint.
package consensus_test

import (
	"fmt"
	"net"
	"testing"

	"github.com/blinklabs-io/dingo/chainselection"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/blinklabs-io/ouroboros-mock/consensus"
)

// TestConsensusConformanceVectors runs the upstream
// consensus-conformance corpus through dingo's ChainSelector. Each
// embedded vector becomes a subtest, with a fresh selector so the
// previous vector's peer tips don't leak across.
func TestConsensusConformanceVectors(t *testing.T) {
	consensus.RunAllCapturedVectors(t, func() consensus.ChainSelector {
		return newSelectorAdapter()
	})
}

// selectorAdapter wraps dingo's chainselection.ChainSelector and
// satisfies the upstream consensus.ChainSelector interface. The
// harness identifies peers by the vector's peer_id (a uint64);
// chainselection identifies peers by ouroboros.ConnectionId, so the
// adapter synthesizes a stable ConnectionId per peer_id.
type selectorAdapter struct {
	cs    *chainselection.ChainSelector
	conns map[uint64]ouroboros.ConnectionId
}

func newSelectorAdapter() *selectorAdapter {
	return &selectorAdapter{
		cs: chainselection.NewChainSelector(
			chainselection.ChainSelectorConfig{},
		),
		conns: make(map[uint64]ouroboros.ConnectionId),
	}
}

func (a *selectorAdapter) UpdatePeerTip(
	peerID uint64,
	tip chainsync.Tip,
	vrfOutput []byte,
) bool {
	return a.cs.UpdatePeerTip(a.connFor(peerID), tip, vrfOutput)
}

func (a *selectorAdapter) EvaluateAndSwitch() {
	a.cs.EvaluateAndSwitch()
}

func (a *selectorAdapter) BestPeerTip() (chainsync.Tip, bool) {
	best := a.cs.GetBestPeer()
	if best == nil {
		return chainsync.Tip{}, false
	}
	pt := a.cs.GetPeerTip(*best)
	if pt == nil {
		return chainsync.Tip{}, false
	}
	return pt.Tip, true
}

// connFor returns a stable, deterministic ConnectionId for peerID.
// Distinct peerIDs yield distinct conn ids; same peerID always
// yields the same conn id within a single test run.
//
// peerID is expected to be small (committed scenarios use 0..N where
// N is the peer count). A peerID above ~55000 would overflow the
// 16-bit TCP port range used below.
func (a *selectorAdapter) connFor(peerID uint64) ouroboros.ConnectionId {
	if id, ok := a.conns[peerID]; ok {
		return id
	}
	local, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:3001")
	remote, _ := net.ResolveTCPAddr(
		"tcp",
		fmt.Sprintf("127.0.0.1:%d", 10000+peerID),
	)
	id := ouroboros.ConnectionId{LocalAddr: local, RemoteAddr: remote}
	a.conns[peerID] = id
	return id
}
