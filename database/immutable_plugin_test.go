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
	"context"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	immplugin "github.com/blinklabs-io/dingo/database/plugin/immutable"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// spyImmutableDB records how the database uses the injected provider.
type spyImmutableDB struct {
	immplugin.ImmutableDB
	mu     sync.Mutex
	opens  []immutable.Geometry
	reads  int
	listed int
}

func (s *spyImmutableDB) Open(
	geom immutable.Geometry,
) (immplugin.Store, error) {
	s.mu.Lock()
	s.opens = append(s.opens, geom)
	s.mu.Unlock()
	return s.ImmutableDB.Open(geom)
}

func (s *spyImmutableDB) ReadBlockAt(
	loc immutable.Location,
) (uint, []byte, error) {
	s.mu.Lock()
	s.reads++
	s.mu.Unlock()
	return s.ImmutableDB.ReadBlockAt(loc)
}

func (s *spyImmutableDB) ListFiles() ([]immutable.File, error) {
	s.mu.Lock()
	s.listed++
	s.mu.Unlock()
	return s.ImmutableDB.ListFiles()
}

func TestDatabaseUsesInjectedImmutableDB(t *testing.T) {
	t.Parallel()
	blocks := fixtureModels(t, 20)
	spy := &spyImmutableDB{}
	dir := t.TempDir()
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config: &database.Config{
			DataDir: dir,
			Immutable: database.ImmutableConfig{
				Enabled: true, ChunkSlots: 4320, CanContainEBB: true,
			},
		},
		WrapImmutable: func(
			svc immplugin.ImmutableDB,
		) immplugin.ImmutableDB {
			spy.ImmutableDB = svc
			return spy
		},
	})
	require.NoError(t, err)
	seedBlocks(t, db, blocks)

	_, err = db.PromoteThrough(context.Background(), 20)
	require.NoError(t, err)
	spy.mu.Lock()
	require.Equal(
		t,
		[]immutable.Geometry{{ChunkSlots: 4320, CanContainEBB: true}},
		spy.opens,
		"the chunk writer is opened through the provider, once",
	)
	spy.mu.Unlock()
	require.Equal(t, spy.Dir(), db.ImmutableDir())

	got, err := db.BlockByIndex(blocks[5].ID, nil)
	require.NoError(t, err)
	require.Equal(t, blocks[5].Cbor, got.Cbor)
	spy.mu.Lock()
	require.Positive(t, spy.reads, "marker reads go through the provider")
	spy.mu.Unlock()

	layout, ok, err := db.ImmutableLayout()
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, layout.Files)
	spy.mu.Lock()
	require.Positive(t, spy.listed, "snapshot placement lists through the provider")
	spy.mu.Unlock()
}

func TestDatabaseRequiresImmutableProviderWhenEnabled(t *testing.T) {
	t.Parallel()
	_, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config: &database.Config{
			DataDir: t.TempDir(),
			Immutable: database.ImmutableConfig{
				Enabled: true, ChunkSlots: 4320, CanContainEBB: true,
			},
		},
		WrapImmutable: func(immplugin.ImmutableDB) immplugin.ImmutableDB {
			return nil
		},
	})
	require.ErrorContains(t, err, "storage.immutable provider")
}

func TestDatabaseWithoutImmutableDBNeedsNoProvider(t *testing.T) {
	t.Parallel()
	_, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config: &database.Config{DataDir: t.TempDir()},
		WrapImmutable: func(immplugin.ImmutableDB) immplugin.ImmutableDB {
			return nil
		},
	})
	require.NoError(t, err)
}
