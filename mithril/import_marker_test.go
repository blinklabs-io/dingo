package mithril

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestImmutableImportMarkerRoundTrip pins the persisted marker that records the
// highest immutable file number a Mithril sync imported, so a later catch-up can
// bound its download and anchor the chain-intersection check.
func TestImmutableImportMarkerRoundTrip(t *testing.T) {
	db := newSyncModeTestDB(t)

	_, ok, err := getImmutableImportMarker(db)
	require.NoError(t, err)
	require.False(t, ok, "fresh database should have no marker")

	require.NoError(t, setImmutableImportMarker(db, 26887))
	num, ok, err := getImmutableImportMarker(db)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(26887), num)

	// A later, higher import overwrites the marker.
	require.NoError(t, setImmutableImportMarker(db, 26900))
	num, ok, err = getImmutableImportMarker(db)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(26900), num)
}
