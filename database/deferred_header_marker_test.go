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

	require.NoError(t, db.SetDeferredHeaderMarker("10:aa"))
	require.NoError(t, db.SetDeferredHeaderMarker("20:bb"))
	has, err = db.HasDeferredHeaderMarker("10:aa")
	require.NoError(t, err)
	require.True(t, has)

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
