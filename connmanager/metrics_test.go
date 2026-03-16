package connmanager

import (
	"net"
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestUpdateConnectionMetricsRebuildsState(t *testing.T) {
	cm := NewConnectionManager(ConnectionManagerConfig{
		PromRegistry: prometheus.NewRegistry(),
	})
	cm.connections[connectionIDForMetricsTest(1)] = &connectionInfo{
		peerAddr:  "198.51.100.10:3001",
		isInbound: true,
	}
	cm.connections[connectionIDForMetricsTest(2)] = &connectionInfo{
		peerAddr:  "198.51.100.10:3001",
		isInbound: true,
	}
	cm.connections[connectionIDForMetricsTest(3)] = &connectionInfo{
		peerAddr:  "198.51.100.10:3001",
		isInbound: false,
	}
	cm.connections[connectionIDForMetricsTest(4)] = &connectionInfo{
		peerAddr:  "198.51.100.11:4001",
		isInbound: false,
	}

	cm.updateConnectionMetrics()

	if got := testutil.ToFloat64(cm.metrics.incomingConns); got != 2 {
		t.Fatalf("incomingConns = %v, want 2", got)
	}
	if got := testutil.ToFloat64(cm.metrics.outgoingConns); got != 2 {
		t.Fatalf("outgoingConns = %v, want 2", got)
	}
	if got := testutil.ToFloat64(cm.metrics.duplexConns); got != 1 {
		t.Fatalf("duplexConns = %v, want 1", got)
	}
	if got := testutil.ToFloat64(cm.metrics.unidirectionalConns); got != 1 {
		t.Fatalf("unidirectionalConns = %v, want 1", got)
	}
	if got := testutil.ToFloat64(cm.metrics.prunableConns); got != 0 {
		t.Fatalf("prunableConns = %v, want 0", got)
	}
}

func TestUpdatePeerConnectivityLockedNormalizesPeerAddress(t *testing.T) {
	cm := NewConnectionManager(ConnectionManagerConfig{})

	cm.updatePeerConnectivityLocked("Relay.Example.Com:3001", true, true)
	cm.updatePeerConnectivityLocked("relay.example.com:3001", false, true)

	if got := cm.duplexPeers; got != 1 {
		t.Fatalf("duplexPeers = %d, want 1", got)
	}
	if got := cm.unidirectional; got != 0 {
		t.Fatalf("unidirectional = %d, want 0", got)
	}
	if _, ok := cm.peerConnectivity["Relay.Example.Com:3001"]; ok {
		t.Fatal("expected raw peer address key to be normalized")
	}
	if _, ok := cm.peerConnectivity["relay.example.com:3001"]; !ok {
		t.Fatal("expected normalized peer address key to be present")
	}
}

func connectionIDForMetricsTest(i int) ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr: &net.TCPAddr{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: 3000 + i,
		},
		RemoteAddr: &net.TCPAddr{
			IP:   net.IPv4(198, 51, 100, byte(i+1)),
			Port: 4000 + i,
		},
	}
}
