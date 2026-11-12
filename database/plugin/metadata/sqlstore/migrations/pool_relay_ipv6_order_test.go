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

package migrations_test

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"path/filepath"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// Before the migration a relay decoded from a chain certificate or imported
// from a ledger-state snapshot held its IPv6 address in the ledger's wire
// order, each 32-bit word byte-reversed. Genesis relays were parsed from text
// and already hold network order.
func TestPoolRelayIPv6NetworkOrderMigration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, runTo := migratedPrefixDB(t, 59)
	standard := net.ParseIP("2001:db8::1")
	wire := lcommon.PoolRelayIpv6WireOrder(standard)
	require.NotEqual(t, []byte(standard), []byte(wire))

	_, err := db.ExecContext(
		ctx,
		`INSERT INTO pool (id, pool_key_hash) VALUES (1, X'01')`,
	)
	require.NoError(t, err)
	type registration struct {
		id            int
		certificateID int
		addedSlot     int
	}
	for _, reg := range []registration{
		{id: 1, certificateID: 7, addedSlot: 100}, // chain certificate
		{id: 2, certificateID: 0, addedSlot: 0},   // genesis
		{id: 3, certificateID: 0, addedSlot: 500}, // snapshot import
	} {
		_, err := db.ExecContext(ctx, `
INSERT INTO pool_registration (id, pool_id, pool_key_hash, certificate_id, added_slot, registration_index)
VALUES (?, 1, X'01', ?, ?, ?)`, reg.id, reg.certificateID, reg.addedSlot, reg.id)
		require.NoError(t, err)
	}
	insertRelay := func(id, registrationID int, ipv4, ipv6 []byte) {
		t.Helper()
		_, err := db.ExecContext(ctx, `
INSERT INTO pool_registration_relay (id, pool_registration_id, pool_id, ipv4, ipv6, port)
VALUES (?, ?, 1, ?, ?, 3001)`, id, registrationID, ipv4, ipv6)
		require.NoError(t, err)
	}
	ipv4 := []byte{192, 168, 1, 1}
	insertRelay(1, 1, ipv4, []byte(wire))
	insertRelay(2, 2, ipv4, []byte(standard))
	insertRelay(3, 3, nil, []byte(wire))
	insertRelay(4, 1, ipv4, nil)

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runTo(registry)
	// Running the registry again must not swap a second time.
	runTo(registry)

	read := func(id int) (got4, got6 []byte) {
		t.Helper()
		require.NoError(t, db.QueryRowContext(ctx, `
SELECT ipv4, ipv6 FROM pool_registration_relay WHERE id = ?`, id,
		).Scan(&got4, &got6))
		return got4, got6
	}
	_, got := read(1)
	require.Equal(t, []byte(standard), got, "chain relay")
	got4, got := read(2)
	require.Equal(t, []byte(standard), got, "genesis relay is left alone")
	require.Equal(t, ipv4, got4)
	_, got = read(3)
	require.Equal(t, []byte(standard), got, "snapshot relay")
	got4, got = read(4)
	require.Nil(t, got)
	require.Equal(t, ipv4, got4, "IPv4 is never reordered")
	var dirty bool
	require.NoError(
		t,
		db.QueryRowContext(ctx, `SELECT dirty FROM schema_migrations WHERE version = 60`).
			Scan(&dirty),
	)
	require.False(t, dirty)
}

// The swap is its own inverse, so a batch whose rows commit without its cursor,
// or a cursor that commits behind its rows, swaps a relay back on resume. One
// row per batch and a fault after the third batch's update pins that the
// failed batch rolls back whole and the resumed run converts each row once.
func TestPoolRelayIPv6NetworkOrderBackfillResumesAfterFailedBatch(
	t *testing.T,
) {
	t.Parallel()
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	migrations.SeedMigratedPrefix(t, databasePath, 59)
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	standard := net.ParseIP("2001:db8::1")
	wire := lcommon.PoolRelayIpv6WireOrder(standard)
	_, err = db.ExecContext(ctx, `
INSERT INTO pool (id, pool_key_hash) VALUES (1, X'01')`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `
INSERT INTO pool_registration (id, pool_id, pool_key_hash, certificate_id, added_slot, registration_index)
VALUES (1, 1, X'01', 7, 100, 1), (2, 1, X'01', 0, 0, 2)`)
	require.NoError(t, err)
	const chainRelays = 4
	for id := 1; id <= chainRelays; id++ {
		_, err := db.ExecContext(ctx, `
INSERT INTO pool_registration_relay (id, pool_registration_id, pool_id, ipv6, port)
VALUES (?, 1, 1, ?, 3001)`, id, []byte(wire))
		require.NoError(t, err)
	}
	_, err = db.ExecContext(ctx, `
INSERT INTO pool_registration_relay (id, pool_registration_id, pool_id, ipv6, port)
VALUES (?, 2, 1, ?, 3001)`, chainRelays+1, []byte(standard))
	require.NoError(t, err)

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	index := slices.IndexFunc(registry, func(m migrations.Migration) bool {
		return m.Version == 60
	})
	require.GreaterOrEqual(t, index, 0)
	registry[index].BatchSize = 1
	backfill := registry[index].Backfill
	fault := errors.New("injected batch failure")
	calls := 0
	registry[index].Backfill = func(
		ctx context.Context,
		batch migrations.Batch,
	) (migrations.BatchResult, error) {
		result, err := backfill(ctx, batch)
		calls++
		if err == nil && calls == 3 {
			return result, fault
		}
		return result, err
	}
	runner := migrations.Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker:   migrations.NewFileLocker(databasePath + ".migrate.lock"),
	}
	require.ErrorIs(t, runner.Run(ctx), fault)

	read := func(id int) []byte {
		t.Helper()
		var got []byte
		require.NoError(t, db.QueryRowContext(ctx, `
SELECT ipv6 FROM pool_registration_relay WHERE id = ?`, id).Scan(&got))
		return got
	}
	var cursor string
	var dirty bool
	require.NoError(t, db.QueryRowContext(ctx, `
SELECT cursor, dirty FROM schema_migrations WHERE version = 60`,
	).Scan(&cursor, &dirty))
	require.Equal(t, "2", cursor)
	require.True(t, dirty)
	for id := 1; id <= chainRelays; id++ {
		want := []byte(wire)
		if id <= 2 {
			want = []byte(standard)
		}
		require.Equal(t, want, read(id), "relay %d after failed batch", id)
	}

	registry[index].Backfill = backfill
	require.NoError(t, runner.Run(ctx))
	for id := 1; id <= chainRelays+1; id++ {
		require.Equal(t, []byte(standard), read(id), "relay %d", id)
	}
	require.NoError(t, db.QueryRowContext(ctx, `
SELECT dirty FROM schema_migrations WHERE version = 60`).Scan(&dirty))
	require.False(t, dirty)
}
