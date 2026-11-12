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
	"net"
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
