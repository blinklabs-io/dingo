package peergov

import (
	"io"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
)

func BenchmarkReconcile(b *testing.B) {
	for _, peerCount := range []int{100, 500, 1000} {
		b.Run(strconv.Itoa(peerCount), func(b *testing.B) {
			pg := NewPeerGovernor(PeerGovernorConfig{
				Logger:                         slog.New(slog.NewJSONHandler(io.Discard, nil)),
				MinHotPeers:                    20,
				TargetNumberOfActivePeers:      20,
				TargetNumberOfEstablishedPeers: 50,
				TargetNumberOfKnownPeers:       150,
			})
			basePeers := benchmarkPeers(peerCount)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				pg.peers = cloneBenchmarkPeers(basePeers)
				pg.denyList = make(map[string]time.Time)
				pg.bootstrapExited = false
				pg.lastBootstrapExit = time.Time{}
				b.StartTimer()
				pg.reconcile(b.Context())
			}
		})
	}
}

func benchmarkPeers(count int) []*Peer {
	now := time.Now()
	peers := make([]*Peer, 0, count)
	for i := range count {
		peer := &Peer{
			Address:           net.JoinHostPort("198.51.100."+strconv.Itoa(i%250+1), strconv.Itoa(3000+i)),
			NormalizedAddress: net.JoinHostPort("198.51.100."+strconv.Itoa(i%250+1), strconv.Itoa(3000+i)),
			FirstSeen:         now.Add(-2 * time.Hour),
			LastActivity:      now.Add(-1 * time.Minute),
			GroupID:           "group-" + strconv.Itoa(i%16),
			Valency:           2,
			WarmValency:       4,
		}
		switch i % 6 {
		case 0:
			peer.Source = PeerSourceTopologyLocalRoot
		case 1:
			peer.Source = PeerSourceTopologyPublicRoot
		case 2:
			peer.Source = PeerSourceP2PGossip
		case 3:
			peer.Source = PeerSourceP2PLedger
		case 4:
			peer.Source = PeerSourceInboundConn
		default:
			peer.Source = PeerSourceUnknown
		}
		switch {
		case i < 20:
			peer.State = PeerStateHot
			peer.Connection = benchmarkPeerConnection(i)
			if i%3 == 0 {
				peer.LastActivity = now.Add(-30 * time.Minute)
			}
		case i < count/2:
			peer.State = PeerStateWarm
			peer.Connection = benchmarkPeerConnection(i)
			peer.BlockFetchLatencyMs = 120 + float64(i%40)
			peer.BlockFetchLatencyInit = true
			peer.BlockFetchSuccessRate = 0.95
			peer.BlockFetchSuccessInit = true
			peer.ConnectionStability = 0.90
			peer.ConnectionStabilityInit = true
			peer.HeaderArrivalRate = 4 + float64(i%5)
			peer.HeaderArrivalRateInit = true
			peer.TipSlotDelta = int64(-(i % 10))
			peer.TipSlotDeltaInit = true
			if peer.Source == PeerSourceInboundConn {
				peer.FirstSeen = now.Add(-30 * time.Minute)
			}
			peer.UpdatePeerScore()
		default:
			peer.State = PeerStateCold
			if i%5 == 0 {
				peer.Connection = benchmarkPeerConnection(i)
			}
			if i%9 == 0 {
				peer.ReconnectCount = 5
			}
		}
		peers = append(peers, peer)
	}
	return peers
}

func cloneBenchmarkPeers(peers []*Peer) []*Peer {
	cloned := make([]*Peer, 0, len(peers))
	for _, peer := range peers {
		if peer == nil {
			cloned = append(cloned, nil)
			continue
		}
		peerCopy := *peer
		if peer.Connection != nil {
			connCopy := *peer.Connection
			peerCopy.Connection = &connCopy
		}
		cloned = append(cloned, &peerCopy)
	}
	return cloned
}

func benchmarkPeerConnection(i int) *PeerConnection {
	return &PeerConnection{
		Id: ouroboros.ConnectionId{
			LocalAddr: &net.TCPAddr{
				IP:   net.IPv4(127, 0, 0, 1),
				Port: 3001 + i,
			},
			RemoteAddr: &net.TCPAddr{
				IP:   net.IPv4(198, 51, 100, byte(i%250+1)),
				Port: 4001 + i,
			},
		},
		IsClient: true,
	}
}
