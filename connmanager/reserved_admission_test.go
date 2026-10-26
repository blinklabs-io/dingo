package connmanager

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

func newReservedAdmissionManager(maxInbound int) *ConnectionManager {
	return NewConnectionManager(ConnectionManagerConfig{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MaxInboundConns: maxInbound,
	})
}

func inboundAccounting(cm *ConnectionManager) (reserved, count int) {
	cm.connectionsMutex.Lock()
	defer cm.connectionsMutex.Unlock()
	return cm.inboundReserved, cm.inboundCount
}

func TestAddReservedInboundConnectionConvertsReservation(t *testing.T) {
	t.Parallel()
	cm := newReservedAdmissionManager(1)
	require.True(t, cm.tryReserveInboundSlot())
	conn := newUnstartedConnection(t)
	require.True(
		t,
		cm.addReservedInboundConnection(conn, "1.2.3.4:3001", ""),
	)
	reserved, count := inboundAccounting(cm)
	require.Equal(t, 0, reserved)
	require.Equal(t, 1, count)
	// The cap holds: the admitted connection occupies the only slot.
	require.False(t, cm.tryReserveInboundSlot())
	conn.ErrorChan() <- nil
	waitForConnectionManagerWatchers(t, cm)
}

func TestAddReservedInboundConnectionCollisionReleasesReservation(
	t *testing.T,
) {
	t.Parallel()
	cm := newReservedAdmissionManager(2)
	outbound := newUnstartedConnection(t)
	require.True(t, cm.AddConnection(outbound, false, "1.2.3.4:3001"))
	require.True(t, cm.tryReserveInboundSlot())
	inbound := newUnstartedConnection(t)
	require.False(
		t,
		cm.addReservedInboundConnection(inbound, "1.2.3.4:3001", ""),
	)
	reserved, count := inboundAccounting(cm)
	require.Equal(t, 0, reserved)
	require.Equal(t, 0, count)
	outbound.ErrorChan() <- nil
	waitForConnectionManagerWatchers(t, cm)
}

func TestAddReservedInboundConnectionShutdownReleasesReservation(
	t *testing.T,
) {
	t.Parallel()
	cm := newReservedAdmissionManager(1)
	cm.listenersMutex.Lock()
	cm.closing = true
	cm.listenersMutex.Unlock()
	require.True(t, cm.tryReserveInboundSlot())
	conn := newUnstartedConnection(t)
	require.False(
		t,
		cm.addReservedInboundConnection(conn, "1.2.3.4:3001", ""),
	)
	reserved, count := inboundAccounting(cm)
	require.Equal(t, 0, reserved)
	require.Equal(t, 0, count)
}
