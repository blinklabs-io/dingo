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
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/topology"
	"github.com/stretchr/testify/require"
)

func TestTopologyHotValencyReachesGovernor(t *testing.T) {
	t.Parallel()
	cfg, err := topology.NewTopologyConfigFromReader(strings.NewReader(`{
		"localRoots": [{"accessPoints": [{"address":"127.0.0.1","port":3001}], "hotValency":1}]
	}`))
	require.NoError(t, err)
	p := NewPeerGovernor(PeerGovernorConfig{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	p.LoadTopologyConfig(cfg)
	require.Len(t, p.peers, 1)
	require.Equal(t, uint(1), p.peers[0].Valency)
	require.Equal(t, uint(1), p.peers[0].WarmValency)
}

func TestPeerSnapshotBootstrapRecovery(t *testing.T) {
	t.Parallel()
	eb := newMockEventBus()
	defer eb.Close()
	p := NewPeerGovernor(PeerGovernorConfig{
		Logger:                    slog.New(slog.NewTextHandler(io.Discard, nil)),
		EventBus:                  eb,
		MinHotPeers:               1,
		BootstrapRecoveryCooldown: time.Minute,
	})
	p.LoadTopologyConfig(&topology.TopologyConfig{
		BootstrapPeers: []topology.TopologyConfigP2PBootstrapPeer{{Address: "127.0.0.1", Port: 3001}},
	})
	p.PreferPeerSnapshot()
	defer p.wg.Wait()
	p.mu.Lock()
	defer p.mu.Unlock()
	require.True(t, p.bootstrapExited)
	require.Len(t, p.peers, 1)
	p.stopCh = make(chan struct{})
	close(p.stopCh)
	p.spawnOutboundConnectionLocked(p.peers[0])
	require.False(t, p.peers[0].Reconnecting, "dormant bootstrap peers must not reserve outbound dials")
	require.Empty(t, p.checkBootstrapRecoveryLocked(), "cooldown gives snapshot peers time to connect")
	p.lastBootstrapExit = time.Now().Add(-2 * time.Minute)
	p.peers = append(p.peers, &Peer{
		Address: "127.0.0.2:3001", Source: PeerSourceP2PLedger,
		State: PeerStateHot, Connection: &PeerConnection{IsClient: true},
	})
	require.Empty(t, p.checkBootstrapRecoveryLocked(), "healthy snapshot connection keeps bootstrap dormant")
	require.True(t, p.bootstrapExited)
	p.peers = p.peers[:1]
	require.NotEmpty(t, p.checkBootstrapRecoveryLocked())
	require.False(t, p.bootstrapExited, "failed snapshot peers must not strand bootstrap recovery")
}
