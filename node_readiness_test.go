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

package dingo

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/stretchr/testify/require"
)

func newReadinessTestDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	return db
}

func TestDatabaseReadyReportsTheDatabaseState(t *testing.T) {
	t.Parallel()

	t.Run("open database is ready", func(t *testing.T) {
		t.Parallel()
		n := &Node{db: newReadinessTestDB(t)}
		require.NoError(t, n.DatabaseReady())
	})

	t.Run("database not opened yet", func(t *testing.T) {
		t.Parallel()
		require.ErrorContains(t, (&Node{}).DatabaseReady(), "not open")
	})

	t.Run("database that fails reads", func(t *testing.T) {
		t.Parallel()
		db := newReadinessTestDB(t)
		require.NoError(t, dbtest.CloseDatabase(db))
		n := &Node{db: db}
		require.ErrorContains(t, n.DatabaseReady(), "database read failed")
	})

	t.Run("startup in progress", func(t *testing.T) {
		t.Parallel()
		n := &Node{db: newReadinessTestDB(t)}
		n.startupLifecycleMu.Lock()
		defer n.startupLifecycleMu.Unlock()
		require.ErrorContains(t, n.DatabaseReady(), "starting")
	})

	t.Run("live restore or truncate in progress", func(t *testing.T) {
		t.Parallel()
		n := &Node{db: newReadinessTestDB(t)}
		n.liveLifecycleMu.Lock()
		defer n.liveLifecycleMu.Unlock()
		require.ErrorContains(t, n.DatabaseReady(), "restore or truncate")
	})
}

type fakeForgerReadiness struct {
	running bool
	err     error
}

func (f fakeForgerReadiness) IsRunning() bool { return f.running }

func (f fakeForgerReadiness) CredentialsUsable() error { return f.err }

func TestBlockProducerReadinessNamesWhyForgingCannotProceed(t *testing.T) {
	t.Parallel()

	require.NoError(
		t,
		blockProducerReadiness(fakeForgerReadiness{running: true}),
	)
	require.ErrorContains(
		t,
		blockProducerReadiness(fakeForgerReadiness{}),
		"not running",
	)
	err := blockProducerReadiness(fakeForgerReadiness{
		running: true,
		err:     errors.New("operational certificate expired"),
	})
	require.ErrorContains(t, err, "credentials unusable")
	require.ErrorContains(t, err, "expired")
}

func TestBlockProducerReadyOnlyConstrainsBlockProducers(t *testing.T) {
	t.Parallel()

	t.Run("relay is not gated on forging", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, (&Node{}).BlockProducerReady())
	})

	t.Run("block producer without a forger", func(t *testing.T) {
		t.Parallel()
		n := &Node{config: Config{blockProducer: true}}
		require.ErrorContains(t, n.BlockProducerReady(), "not initialized")
	})

	t.Run("block producer whose forger is not running", func(t *testing.T) {
		t.Parallel()
		n := &Node{
			config:      Config{blockProducer: true},
			blockForger: &forging.BlockForger{},
		}
		require.ErrorContains(t, n.BlockProducerReady(), "not running")
	})

	t.Run("block producer during a live restore", func(t *testing.T) {
		t.Parallel()
		n := &Node{
			config:      Config{blockProducer: true},
			blockForger: &forging.BlockForger{},
		}
		n.liveLifecycleMu.Lock()
		defer n.liveLifecycleMu.Unlock()
		require.ErrorContains(t, n.BlockProducerReady(), "restore or truncate")
	})
}
