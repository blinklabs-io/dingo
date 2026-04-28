package connmanager

import (
	"net"
	"strconv"
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/prometheus/client_golang/prometheus"
)

func BenchmarkUpdateConnectionMetrics(b *testing.B) {
	for _, connections := range []int{10, 100, 500, 1000} {
		b.Run(strconv.Itoa(connections), func(b *testing.B) {
			cm := NewConnectionManager(ConnectionManagerConfig{
				PromRegistry: prometheus.NewRegistry(),
			})
			for i := range connections {
				addr := net.JoinHostPort("198.51.100."+strconv.Itoa(i%250+1), strconv.Itoa(3000+i))
				cm.connections[connectionIDForBench(i)] = &connectionInfo{
					peerAddr:  addr,
					isInbound: i%2 == 0,
				}
				if i%2 == 0 {
					cm.inboundCount++
				}
			}
			b.ReportAllocs()
			cm.updateConnectionMetrics()
			b.ResetTimer()
			for range b.N {
				cm.updateConnectionMetrics()
			}
		})
	}
}

func BenchmarkTryReserveInboundSlotParallel(b *testing.B) {
	for _, connections := range []int{10, 100, 500} {
		b.Run(strconv.Itoa(connections), func(b *testing.B) {
			cm := NewConnectionManager(ConnectionManagerConfig{
				MaxInboundConns: connections * 2,
			})
			for i := range connections {
				addr := net.JoinHostPort("198.51.100."+strconv.Itoa(i%250+1), strconv.Itoa(3000+i))
				cm.connections[connectionIDForBench(i)] = &connectionInfo{
					peerAddr:  addr,
					isInbound: true,
				}
			}
			cm.inboundCount = connections
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if cm.tryReserveInboundSlot() {
						cm.releaseInboundSlot()
					}
				}
			})
		})
	}
}

func BenchmarkHasInboundPeerAddress(b *testing.B) {
	for _, connections := range []int{10, 100, 500, 1000} {
		b.Run(strconv.Itoa(connections), func(b *testing.B) {
			cm := NewConnectionManager(ConnectionManagerConfig{
				OutboundSourcePort: 3001,
			})
			for i := range connections {
				addr := net.JoinHostPort(
					"198.51.100."+strconv.Itoa(i%250+1),
					strconv.Itoa(3000+i),
				)
				cm.connections[connectionIDForBench(i)] = &connectionInfo{
					peerAddr:  addr,
					isInbound: true,
					conn:      &ouroboros.Connection{},
				}
				cm.inboundCount++
				peerKey := NormalizePeerAddr(addr)
				cm.inboundPeerAddrs[peerKey]++
			}
			targetAddr := net.JoinHostPort("203.0.113.10", "4000")
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if cm.HasInboundPeerAddress(targetAddr) {
					b.Fatal("unexpected inbound peer-address match")
				}
			}
		})
	}
}

func connectionIDForBench(i int) ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr: &net.TCPAddr{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: 3001 + i,
		},
		RemoteAddr: &net.TCPAddr{
			IP:   net.IPv4(198, 51, 100, byte(i%250+1)),
			Port: 4001 + i,
		},
	}
}
