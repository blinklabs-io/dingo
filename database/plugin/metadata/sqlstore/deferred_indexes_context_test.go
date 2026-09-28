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

package sqlstore

import (
	"context"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/stretchr/testify/require"
)

// TestBuildDeferredIndexesContextHonoursCancellation proves the store's
// context-aware rebuild actually reaches the database driver with the
// caller's context, rather than accepting one and building on
// context.Background() anyway. Restore relies on this: an uninterruptible
// full-manifest rebuild outlives a cancelled restore for as long as its
// largest index takes.
func TestBuildDeferredIndexesContextHonoursCancellation(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	require.NoError(t, store.DropDeferredIndexes())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(
		t,
		store.BuildDeferredIndexesContext(ctx),
		context.Canceled,
	)

	// The cancelled attempt must leave the recovery marker in place so the
	// next run still knows a rebuild is outstanding.
	pending, err := store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.True(t, pending)

	require.NoError(t, store.BuildDeferredIndexesContext(context.Background()))
	pending, err = store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.False(t, pending)
	missing, err := store.MissingDeferredIndexes()
	require.NoError(t, err)
	require.Empty(t, missing)
	require.NotEmpty(t, deferred.Manifest)
}

// TestMissingDeferredIndexesNamesDroppedEntries covers the full-manifest
// lister the repair paths use to announce a rebuild before it starts.
func TestMissingDeferredIndexesNamesDroppedEntries(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	missing, err := store.MissingDeferredIndexes()
	require.NoError(t, err)
	require.Empty(t, missing)

	require.NoError(t, store.DropDeferredIndexes())
	missing, err = store.MissingDeferredIndexes()
	require.NoError(t, err)
	require.NotEmpty(t, missing)
	critical, err := store.MissingCriticalDeferredIndexes()
	require.NoError(t, err)
	require.Subset(t, missing, critical)
}

func TestBuildDeferredIndexesContextWithProgressReportsOnlyMissingIndexes(
	t *testing.T,
) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	var before, after []string
	require.NoError(t, store.BuildDeferredIndexesContextWithProgress(
		t.Context(),
		func(name string) { before = append(before, name) },
		func(name string, _ time.Duration) { after = append(after, name) },
	))
	require.Empty(t, before, "present indexes need no build progress events")
	require.Empty(t, after, "present indexes need no build progress events")

	missing := deferred.Manifest[0]
	_, err := store.writeDB.ExecContext(
		t.Context(),
		store.dialect.DropIndexSQL(missing.Name, missing.Table),
	)
	require.NoError(t, err)

	before = nil
	after = nil
	require.NoError(t, store.BuildDeferredIndexesContextWithProgress(
		t.Context(),
		func(name string) { before = append(before, name) },
		func(name string, _ time.Duration) {
			after = append(after, name)
		},
	))
	require.Equal(t, []string{missing.Name}, before)
	require.Equal(t, []string{missing.Name}, after)
	exists, err := store.deferredIndexExists(
		t.Context(),
		store.readDB,
		missing,
	)
	require.NoError(t, err)
	require.True(t, exists, "the after callback follows the CREATE INDEX")
}
