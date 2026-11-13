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
	"bytes"
	"context"
	"fmt"
	"math/big"
	"math/rand/v2"
	"net"
	"sort"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

type poolRestoreEventKind int

const (
	poolRestoreRegister poolRestoreEventKind = iota
	poolRestoreRetire
	poolRestoreOpCert
)

// poolRestoreEvent is one pool certificate (or op-cert use) at a slot.
type poolRestoreEvent struct {
	owners   [][]byte
	relays   []int
	kind     poolRestoreEventKind
	pool     int
	slot     uint64
	variant  byte
	sequence uint64
	epoch    uint64
}

const poolRestoreTargetSlot = 1500

// poolRestoreWorkload builds a deterministic certificate history over ten
// pools. The explicit prefix pins one pool per case the restore has to get
// right; the seeded tail adds further re-registrations, owner changes,
// retirements and cancelled retirements around the target slot.
func poolRestoreWorkload() []poolRestoreEvent {
	owner := func(b ...byte) [][]byte {
		ret := make([][]byte, 0, len(b))
		for _, v := range b {
			ret = append(ret, bytes.Repeat([]byte{v}, 28))
		}
		return ret
	}
	events := []poolRestoreEvent{
		// 0: registered only before the target and never touched again.
		{kind: poolRestoreRegister, pool: 0, slot: 100, variant: 1,
			owners: owner(0xe0), relays: []int{1}},
		// 1: owner set and parameters change after the target.
		{kind: poolRestoreRegister, pool: 1, slot: 110, variant: 1,
			owners: owner(0xe1, 0xe2), relays: []int{1, 2}},
		{kind: poolRestoreRegister, pool: 1, slot: 2000, variant: 2,
			owners: owner(0xe3), relays: []int{3}},
		// 2: first registered after the target, so the pool must vanish.
		{kind: poolRestoreRegister, pool: 2, slot: 1800, variant: 1,
			owners: owner(0xe4, 0xe5), relays: []int{1, 2, 3}},
		{kind: poolRestoreOpCert, pool: 2, slot: 1900, sequence: 3},
		// 3: retired before the target, re-registered after (cancelled
		// retirement is a later registration; the earlier retirement stays).
		{kind: poolRestoreRegister, pool: 3, slot: 120, variant: 1,
			owners: owner(0xe6), relays: []int{2}},
		{kind: poolRestoreRetire, pool: 3, slot: 900, epoch: 9},
		{kind: poolRestoreRegister, pool: 3, slot: 1700, variant: 2,
			owners: owner(0xe7, 0xe8), relays: []int{3}},
		// 4: retired after the target only.
		{kind: poolRestoreRegister, pool: 4, slot: 130, variant: 1,
			owners: owner(0xe9), relays: []int{1}},
		{kind: poolRestoreRetire, pool: 4, slot: 2100, epoch: 12},
		// 5: registered, retired and re-registered all after the target.
		{kind: poolRestoreRegister, pool: 5, slot: 1600, variant: 1,
			owners: owner(0xea), relays: []int{2}},
		{kind: poolRestoreRetire, pool: 5, slot: 1650, epoch: 15},
		{kind: poolRestoreRegister, pool: 5, slot: 1750, variant: 2,
			owners: owner(0xeb, 0xec), relays: []int{1}},
		// 6: op-cert counter advances past the target.
		{kind: poolRestoreRegister, pool: 6, slot: 140, variant: 1,
			owners: owner(0xed), relays: []int{3}},
		{kind: poolRestoreOpCert, pool: 6, slot: 400, sequence: 1},
		{kind: poolRestoreOpCert, pool: 6, slot: 2200, sequence: 5},
	}
	rng := rand.New(rand.NewPCG(6368, 1))
	slot := uint64(200)
	for i := range 40 {
		slot += uint64(rng.IntN(90)) + 20
		pool := 7 + rng.IntN(3)
		if i < 3 {
			pool = 7 + i
		}
		ev := poolRestoreEvent{pool: pool, slot: slot}
		switch {
		case i < 3 || rng.IntN(3) > 0:
			ev.kind = poolRestoreRegister
			ev.variant = byte(1 + rng.IntN(5))
			for range rng.IntN(4) {
				ev.owners = append(ev.owners,
					bytes.Repeat([]byte{byte(0xf0 + rng.IntN(16))}, 28))
			}
			for range rng.IntN(4) {
				ev.relays = append(ev.relays, 1+rng.IntN(3))
			}
		case rng.IntN(2) == 0:
			ev.kind = poolRestoreRetire
			ev.epoch = uint64(slot / 100)
		default:
			ev.kind = poolRestoreOpCert
			ev.sequence = uint64(i)
		}
		events = append(events, ev)
	}
	// Owners within one registration must be distinct for a stable snapshot.
	for i := range events {
		seen := map[string]bool{}
		kept := events[i].owners[:0:0]
		for _, o := range events[i].owners {
			if !seen[string(o)] {
				seen[string(o)] = true
				kept = append(kept, o)
			}
		}
		events[i].owners = kept
	}
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].slot < events[j].slot
	})
	return events
}

func poolRestoreKeyHash(pool int) []byte {
	return bytes.Repeat([]byte{byte(0x40 + pool)}, 28)
}

func applyPoolRestoreEvents(
	t *testing.T,
	store *Store,
	events []poolRestoreEvent,
) {
	t.Helper()
	ctx := context.Background()
	for _, ev := range events {
		hash := poolRestoreKeyHash(ev.pool)
		switch ev.kind {
		case poolRestoreRegister:
			v := uint64(ev.variant)
			pool := &models.Pool{
				PoolKeyHash: hash,
				Pledge:      types.Uint64(100 * v),
				Cost:        types.Uint64(200 + v),
				Margin: &types.Rat{
					Rat: big.NewRat(int64(v), 100),
				},
				VrfKeyHash: bytes.Repeat(
					[]byte{byte(0xa0) + ev.variant},
					32,
				),
				RewardAccount: bytes.Repeat(
					[]byte{byte(0xb0) + ev.variant},
					28,
				),
				RewardAccountCredentialTag: uint8(v % 2),
			}
			reg := &models.PoolRegistration{
				PoolKeyHash:                hash,
				AddedSlot:                  ev.slot,
				Pledge:                     pool.Pledge,
				Cost:                       pool.Cost,
				Margin:                     pool.Margin,
				VrfKeyHash:                 pool.VrfKeyHash,
				RewardAccount:              pool.RewardAccount,
				RewardAccountCredentialTag: pool.RewardAccountCredentialTag,
			}
			for _, o := range ev.owners {
				reg.Owners = append(reg.Owners,
					models.PoolRegistrationOwner{KeyHash: o})
			}
			for _, r := range ev.relays {
				ip := net.IPv4(10, 0, 0, byte(r))
				relayType := 0
				reg.Relays = append(reg.Relays, models.PoolRegistrationRelay{
					Ipv4: &ip, Type: &relayType, Port: uint(3000 + r),
				})
			}
			require.NoError(t, store.ImportPool(pool, reg, nil))
		case poolRestoreRetire:
			_, err := store.instrumentedQueryer(store.writeDB).
				ExecContext(ctx, `
INSERT INTO pool_retirement (pool_key_hash, certificate_id, pool_id, epoch, added_slot)
SELECT pool_key_hash, 0, id, ?, ? FROM pool WHERE pool_key_hash = ?`,
					ev.epoch, ev.slot, hash)
			require.NoError(t, err)
		case poolRestoreOpCert:
			require.NoError(t, store.UpdatePoolOpCertSequence(
				lcommon.PoolKeyHash(hash), ev.sequence, ev.slot, nil))
		}
	}
}

// poolRestoreSnapshot renders every row RestorePoolStateAtSlot is
// responsible for into slot-independent text, keyed by the pool key hash so
// surrogate ids do not matter. It also reports child rows that do not join
// back to a surviving pool_registration and pool.
func poolRestoreSnapshot(t *testing.T, store *Store) []string {
	t.Helper()
	ctx := context.Background()
	var out []string
	collect := func(label, query string) {
		rows, err := store.writeDB.QueryContext(ctx, query)
		require.NoError(t, err, label)
		defer rows.Close()
		cols, err := rows.Columns()
		require.NoError(t, err)
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			require.NoError(t, rows.Scan(ptrs...))
			parts := make([]string, len(vals))
			for i, v := range vals {
				if b, ok := v.([]byte); ok {
					parts[i] = fmt.Sprintf("%x", b)
				} else {
					parts[i] = fmt.Sprint(v)
				}
			}
			out = append(out, label+": "+strings.Join(parts, "|"))
		}
		require.NoError(t, rows.Err())
	}
	collect("pool", `
SELECT pool_key_hash, pledge, cost, margin, vrf_key_hash, reward_account,
       reward_account_credential_tag, latest_op_cert_sequence
FROM pool ORDER BY pool_key_hash`)
	collect("registration", `
SELECT p.pool_key_hash, r.added_slot, r.pledge, r.cost, r.margin,
       r.vrf_key_hash, r.reward_account, r.reward_account_credential_tag
FROM pool_registration r JOIN pool p ON p.id = r.pool_id
ORDER BY p.pool_key_hash, r.added_slot`)
	collect("owner", `
SELECT p.pool_key_hash, r.added_slot, o.key_hash
FROM pool_registration_owner o
JOIN pool_registration r ON r.id = o.pool_registration_id
JOIN pool p ON p.id = o.pool_id AND p.id = r.pool_id
ORDER BY p.pool_key_hash, r.added_slot, o.key_hash`)
	collect("relay", `
SELECT p.pool_key_hash, r.added_slot, l.ipv4, l.port
FROM pool_registration_relay l
JOIN pool_registration r ON r.id = l.pool_registration_id
JOIN pool p ON p.id = l.pool_id AND p.id = r.pool_id
ORDER BY p.pool_key_hash, r.added_slot, l.port`)
	collect("retirement", `
SELECT pool_key_hash, epoch, added_slot FROM pool_retirement
ORDER BY pool_key_hash, added_slot`)
	collect("opcert", `
SELECT pool_key_hash, slot, sequence FROM pool_opcert_sequence
ORDER BY pool_key_hash, slot`)
	var orphanOwners, orphanRelays, ownerCount, relayCount int
	require.NoError(t, store.writeDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pool_registration_owner`).Scan(&ownerCount))
	require.NoError(t, store.writeDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pool_registration_relay`).Scan(&relayCount))
	require.NoError(t, store.writeDB.QueryRowContext(ctx, `
SELECT COUNT(*) FROM pool_registration_owner o
WHERE NOT EXISTS (SELECT 1 FROM pool_registration r
                  WHERE r.id = o.pool_registration_id AND r.pool_id = o.pool_id)`,
	).Scan(&orphanOwners))
	require.NoError(t, store.writeDB.QueryRowContext(ctx, `
SELECT COUNT(*) FROM pool_registration_relay l
WHERE NOT EXISTS (SELECT 1 FROM pool_registration r
                  WHERE r.id = l.pool_registration_id AND r.pool_id = l.pool_id)`,
	).Scan(&orphanRelays))
	require.Zero(t, orphanOwners, "owner rows without a matching registration")
	require.Zero(t, orphanRelays, "relay rows without a matching registration")
	out = append(out, fmt.Sprintf("counts: owners=%d relays=%d",
		ownerCount, relayCount))
	return out
}

// testRestorePoolStateAtSlotMatchesOracle applies a full certificate history
// to one store, restores it to poolRestoreTargetSlot the way
// TruncateAfterSlot does (pool state first, then certificates), and compares
// every pool, registration, owner, relay, retirement and op-cert row with a
// second store that only ever saw the certificates at or below the target.
func testRestorePoolStateAtSlotMatchesOracle(
	t *testing.T,
	newStore func(t *testing.T) *Store,
) {
	t.Helper()
	events := poolRestoreWorkload()
	var upTo []poolRestoreEvent
	for _, ev := range events {
		if ev.slot <= poolRestoreTargetSlot {
			upTo = append(upTo, ev)
		}
	}
	// The workload has to exercise each case, or the comparison proves less
	// than it appears to.
	require.Greater(t, len(events), len(upTo)+5)
	pools := map[int][2]int{}
	for _, ev := range events {
		if ev.kind != poolRestoreRegister {
			continue
		}
		c := pools[ev.pool]
		if ev.slot <= poolRestoreTargetSlot {
			c[0]++
		} else {
			c[1]++
		}
		pools[ev.pool] = c
	}
	require.Equal(
		t,
		[2]int{0, 1},
		pools[2],
		"pool first registered after target",
	)
	require.Equal(t, [2]int{1, 1}, pools[1], "pool re-registered after target")

	store := newStore(t)
	applyPoolRestoreEvents(t, store, events)
	before := poolRestoreSnapshot(t, store)

	require.NoError(t, store.RestorePoolStateAtSlot(poolRestoreTargetSlot, nil))
	require.NoError(
		t,
		store.DeleteCertificatesAfterSlot(poolRestoreTargetSlot, nil),
	)
	// opcert rows are removed by the restore itself.
	restored := poolRestoreSnapshot(t, store)

	oracle := newStore(t)
	applyPoolRestoreEvents(t, oracle, upTo)
	expected := poolRestoreSnapshot(t, oracle)

	require.NotEqual(t, before, restored, "restore must discard later state")
	require.Equal(t, expected, restored)
}

func TestRestorePoolStateAtSlotMatchesOracleSQLite(t *testing.T) {
	t.Parallel()
	testRestorePoolStateAtSlotMatchesOracle(t, func(t *testing.T) *Store {
		return newMigratedSQLiteStore(t)
	})
}
