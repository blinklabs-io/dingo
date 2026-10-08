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

package database_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

func TestDeferredHeaderMarkerRoundTrip(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck

	has, err := db.HasDeferredHeaderMarker("10:aa")
	require.NoError(t, err)
	require.False(t, has)
	value, found, err := db.GetDeferredHeaderMarkerValue("10:aa")
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, value)

	require.NoError(t, db.SetDeferredHeaderMarker("10:aa"))
	require.NoError(t, db.SetDeferredHeaderMarker("20:bb"))
	has, err = db.HasDeferredHeaderMarker("10:aa")
	require.NoError(t, err)
	require.True(t, has)
	value, found, err = db.GetDeferredHeaderMarkerValue("10:aa")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte{1}, value)

	require.NoError(t, db.SetDeferredHeaderMarkerWithValue(
		"20:bb", []byte("true@127.0.0.1:3001"),
	))
	value, found, err = db.GetDeferredHeaderMarkerValue("20:bb")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("true@127.0.0.1:3001"), value)
	markers, err := db.ListDeferredHeaderMarkerValues()
	require.NoError(t, err)
	markerValues := make(map[string][]byte, len(markers))
	for _, marker := range markers {
		markerValues[marker.Key] = marker.Value
	}
	require.Equal(t, map[string][]byte{
		"10:aa": []byte{1},
		"20:bb": []byte("true@127.0.0.1:3001"),
	}, markerValues)

	keys, err := db.ListDeferredHeaderMarkers()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"10:aa", "20:bb"}, keys)

	require.NoError(t, db.DeleteDeferredHeaderMarker("10:aa"))
	require.NoError(t, db.DeleteDeferredHeaderMarker("10:aa"))
	has, err = db.HasDeferredHeaderMarker("10:aa")
	require.NoError(t, err)
	require.False(t, has)
	keys, err = db.ListDeferredHeaderMarkers()
	require.NoError(t, err)
	require.Equal(t, []string{"20:bb"}, keys)
}
