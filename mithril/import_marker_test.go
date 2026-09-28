package mithril

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWasBootstrapped pins the exported detector used by the serve-time
// CIP-0163 guard: false on a fresh database, true once the durable
// immutable-import marker a completed Mithril sync leaves behind is present.
func TestWasBootstrapped(t *testing.T) {
	t.Parallel()

	db := newSyncModeTestDB(t)

	got, err := WasBootstrapped(db)
	require.NoError(t, err)
	require.False(t, got, "fresh database is not Mithril-bootstrapped")

	require.NoError(t, setImmutableImportMarker(db, 12345))
	got, err = WasBootstrapped(db)
	require.NoError(t, err)
	require.True(
		t,
		got,
		"database with the immutable-import marker is bootstrapped",
	)
}
