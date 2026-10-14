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

package blockfrost

import (
	"context"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandlePoolsRetired(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.poolsRetired.items = []PoolRetiringInfo{{PoolID: "pool1abc", Epoch: 210}}
	node.poolsRetired.total = 101
	recorder := get(t, node, "/api/v0/pools/retired?count=1&page=2&order=desc")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, PaginationParams{Count: 1, Page: 2, Order: PaginationOrderDesc}, node.poolsRetired.params)
	assert.Equal(t, "101", recorder.Header().Get("X-Pagination-Count-Total"))
	assert.JSONEq(t, `[{"pool_id":"pool1abc","epoch":210}]`, recorder.Body.String())

	node.poolsRetired.err = errors.New("boom")
	assert.Equal(t, http.StatusInternalServerError, get(t, node, "/api/v0/pools/retired").Code)
	assert.Equal(t, http.StatusBadRequest, get(t, node, "/api/v0/pools/retired?count=0").Code)
}

func TestHandlePoolHistory(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.poolHistory.items = []PoolHistoryInfo{{
		Epoch: 233, Blocks: 22, ActiveStake: "20485965693569",
		ActiveSize: 1.2345, DelegatorsCount: 115,
		Rewards: "206936253674159", Fees: "1290968354",
	}}
	node.poolHistory.total = 1
	recorder := get(t, node, "/api/v0/pools/pool1xyz/history?order=desc")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, "pool1xyz", node.poolHistory.key)
	assert.Equal(t, PaginationOrderDesc, node.poolHistory.params.Order)
	assert.JSONEq(t, `[{
		"epoch": 233, "blocks": 22, "active_stake": "20485965693569",
		"active_size": 1.2345, "delegators_count": 115,
		"rewards": "206936253674159", "fees": "1290968354"
	}]`, recorder.Body.String())
}

func TestHandlePoolRelays(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.poolRelays.value = []PoolRelayInfo{
		{IPv4: "4.4.4.4", Port: 3001},
		{DNS: "relay.example", Port: 3002},
		{DNSSrv: "_srv.example"},
	}
	recorder := get(t, node, "/api/v0/pools/pool1xyz/relays")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, "pool1xyz", node.poolRelays.key)
	assert.Empty(t, recorder.Header().Get("X-Pagination-Count-Total"), "relays are not paginated")
	assert.JSONEq(t, `[
		{"ipv4":"4.4.4.4","ipv6":null,"dns":null,"dns_srv":null,"port":3001},
		{"ipv4":null,"ipv6":null,"dns":"relay.example","dns_srv":null,"port":3002},
		{"ipv4":null,"ipv6":null,"dns":null,"dns_srv":"_srv.example","port":0}
	]`, recorder.Body.String())

	node.poolRelays.value = nil
	assert.JSONEq(t, `[]`, get(t, node, "/api/v0/pools/pool1xyz/relays").Body.String())
}

func TestHandlePoolListsAndErrors(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.poolDelegators.items = []PoolDelegatorInfo{{Address: "stake1abc", LiveStake: "1234"}}
	node.poolDelegators.total = 1
	node.poolBlocks.items = []string{"b1", "b2"}
	node.poolBlocks.total = 2
	node.poolUpdates.items = []PoolUpdateInfo{{TxHash: "aa", CertIndex: 1, Action: "registered"}}
	node.poolUpdates.total = 1

	recorder := get(t, node, "/api/v0/pools/pool1xyz/delegators")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.JSONEq(t, `[{"address":"stake1abc","live_stake":"1234"}]`, recorder.Body.String())
	recorder = get(t, node, "/api/v0/pools/pool1xyz/blocks?count=2")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.JSONEq(t, `["b1","b2"]`, recorder.Body.String())
	assert.Equal(t, 2, node.poolBlocks.params.Count)
	recorder = get(t, node, "/api/v0/pools/pool1xyz/updates")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.JSONEq(t, `[{"tx_hash":"aa","cert_index":1,"action":"registered"}]`, recorder.Body.String())

	for _, path := range []string{"history", "relays", "delegators", "blocks", "updates"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			for _, tc := range []struct {
				err    error
				status int
			}{
				{ErrInvalidPoolID, http.StatusBadRequest},
				{models.ErrPoolNotFound, http.StatusNotFound},
				{errors.New("boom"), http.StatusInternalServerError},
			} {
				node := &mockNode{}
				node.poolHistory.err = tc.err
				node.poolRelays.err = tc.err
				node.poolDelegators.err = tc.err
				node.poolBlocks.err = tc.err
				node.poolUpdates.err = tc.err
				recorder := get(t, node, "/api/v0/pools/pool1xyz/"+path)
				assert.Equal(t, tc.status, recorder.Code, "%s %v", path, tc.err)
				assert.NotContains(t, recorder.Body.String(), "boom")
			}
		})
	}
}

// addPool registers a pool with the given relays through the metadata
// store's import path, which writes the pool row and its registration.
func (fx *epochFixture) addPool(
	t *testing.T,
	poolKeyHash []byte,
	relays []models.PoolRegistrationRelay,
) string {
	t.Helper()
	pool := &models.Pool{
		PoolKeyHash:   poolKeyHash,
		VrfKeyHash:    bytes32of(32, 0x77),
		RewardAccount: bytes32of(28, 0x66),
		Pledge:        types.Uint64(1),
		Cost:          types.Uint64(2),
	}
	registration := &models.PoolRegistration{
		PoolKeyHash:   poolKeyHash,
		VrfKeyHash:    pool.VrfKeyHash,
		RewardAccount: pool.RewardAccount,
		Pledge:        pool.Pledge,
		Cost:          pool.Cost,
		AddedSlot:     10,
		Relays:        relays,
	}
	require.NoError(t, fx.adapter.ledgerState.Database().Metadata().ImportPool(
		pool, registration, nil,
	))
	id, err := poolIDString(poolKeyHash)
	require.NoError(t, err)
	return id
}

func TestNodeAdapterPoolRelays(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ipv4 := net.ParseIP("4.4.4.4").To4()
	ipv6 := net.ParseIP("2001:db8::1")
	single, multi := 1, 2
	poolID := fx.addPool(t, fx.poolA, []models.PoolRegistrationRelay{
		{Ipv4: &ipv4, Port: 3001},
		{Ipv6: &ipv6, Port: 3002},
		{Hostname: "relay.example", Port: 3003, Type: &single},
		{Hostname: "_srv.example", Type: &multi},
		{Hostname: "legacy-srv.example"},
		{Hostname: "legacy.example", Port: 3004},
		{Hostname: "noport.example", Type: &single},
	})
	relays, err := fx.adapter.PoolRelays(context.Background(), poolID)
	require.NoError(t, err)
	assert.Equal(t, []PoolRelayInfo{
		{IPv4: "4.4.4.4", Port: 3001},
		{IPv6: "2001:db8::1", Port: 3002},
		{DNS: "relay.example", Port: 3003},
		{DNSSrv: "_srv.example"},
		{DNSSrv: "legacy-srv.example"},
		{DNS: "legacy.example", Port: 3004},
		{DNS: "noport.example"},
	}, relays)

	_, err = fx.adapter.PoolRelays(context.Background(), "not-a-pool")
	assert.ErrorIs(t, err, ErrInvalidPoolID)
	unknown, err := poolIDString(bytes32of(28, 0x01))
	require.NoError(t, err)
	_, err = fx.adapter.PoolRelays(context.Background(), unknown)
	assert.ErrorIs(t, err, models.ErrPoolNotFound)
}

func TestNodeAdapterPoolHistory(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	poolID := fx.addPool(t, fx.poolA, nil)

	// Labels 0 and 1 are the distributions active in epochs 1 and 2; label 2
	// would be active in epoch 3, which has not begun.
	for _, snapshot := range []struct {
		label  uint64
		stake  string
		delegs int
	}{{0, "700", 3}, {1, "900", 4}, {2, "1100", 5}} {
		_, err := fx.raw.Exec(
			`INSERT INTO pool_stake_snapshot (epoch, snapshot_type, pool_key_hash, total_stake, delegator_count, captured_slot) VALUES (?, 'mark', ?, ?, ?, 0)`,
			snapshot.label, fx.poolA, snapshot.stake, snapshot.delegs,
		)
		require.NoError(t, err)
	}
	_, err := fx.raw.Exec(
		`INSERT INTO pool_stake_snapshot (epoch, snapshot_type, pool_key_hash, total_stake, delegator_count, captured_slot) VALUES (0, 'set', ?, '1', 1, 0)`,
		fx.poolA,
	)
	require.NoError(t, err)
	_, err = fx.raw.Exec(
		`INSERT INTO reward_pool_output (pool_key_hash, epoch, optimal_reward, total_reward, leader_reward, member_reward_total, owner_stake, undistributed, unspendable, captured_slot, boundary_slot) VALUES (?, 0, '0', '50', '10', '40', '0', '0', '0', 0, 0)`,
		fx.poolA,
	)
	require.NoError(t, err)

	history, total, err := fx.adapter.PoolHistory(ctx, poolID, PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc})
	require.NoError(t, err)
	assert.Equal(t, 2, total, "the snapshot for a future epoch and non-mark snapshots are not history")
	require.Len(t, history, 2)
	assert.Equal(t, PoolHistoryInfo{
		Epoch: 1, Blocks: 2, ActiveStake: "700", ActiveSize: 700.0 / 5000.0,
		DelegatorsCount: 3, Rewards: "40", Fees: "10",
	}, history[0])
	assert.Equal(t, PoolHistoryInfo{
		Epoch: 2, Blocks: 1, ActiveStake: "900", ActiveSize: 900.0 / 9000.0,
		DelegatorsCount: 4, Rewards: "0", Fees: "0",
	}, history[1], "an epoch whose rewards are not calculated reports zero rewards")

	history, total, err = fx.adapter.PoolHistory(ctx, poolID, PaginationParams{Count: 1, Page: 1, Order: PaginationOrderDesc})
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	require.Len(t, history, 1)
	assert.Equal(t, uint64(2), history[0].Epoch)

	_, _, err = fx.adapter.PoolHistory(ctx, "not-a-pool", PaginationParams{Count: 1, Page: 1})
	assert.ErrorIs(t, err, ErrInvalidPoolID)
}

func TestNodeAdapterPoolDelegatorsAndBlocks(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	poolID := fx.addPool(t, fx.poolA, nil)

	for _, delegator := range []struct {
		key        byte
		tag        uint8
		stake      string
		registered bool
		slot       uint64
		pool       []byte
	}{
		{0x01, 0, "300", true, 200, fx.poolA},
		{0x02, 1, "100", true, 100, fx.poolA},
		{0x03, 0, "999", false, 50, fx.poolA},
		{0x04, 0, "555", true, 10, fx.poolB},
	} {
		_, err := fx.raw.Exec(
			`INSERT INTO reward_live_stake (pool_key_hash, staking_key, credential_tag, utxo_stake, reward_stake, total_stake, registered, pool_delegation_slot, updated_slot) VALUES (?, ?, ?, ?, '0', ?, ?, ?, 0)`,
			delegator.pool, bytes32of(28, delegator.key), delegator.tag,
			delegator.stake, delegator.stake, delegator.registered, delegator.slot,
		)
		require.NoError(t, err)
	}
	address := func(tag uint8, key byte) string {
		addr, err := fx.adapter.stakeAddressFromTag(tag, bytes32of(28, key))
		require.NoError(t, err)
		return addr
	}

	delegators, total, err := fx.adapter.PoolDelegators(ctx, poolID, PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc})
	require.NoError(t, err)
	assert.Equal(t, 2, total, "unregistered and other pools' delegators are excluded")
	assert.Equal(t, []PoolDelegatorInfo{
		{Address: address(1, 0x02), LiveStake: "100"},
		{Address: address(0, 0x01), LiveStake: "300"},
	}, delegators, "oldest delegation first")

	delegators, total, err = fx.adapter.PoolDelegators(ctx, poolID, PaginationParams{Count: 1, Page: 1, Order: PaginationOrderDesc})
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Equal(t, []PoolDelegatorInfo{{Address: address(0, 0x01), LiveStake: "300"}}, delegators)

	hexOf := func(b byte) string { return hex.EncodeToString(fill32(b)) }
	blocks, total, err := fx.adapter.PoolBlocks(ctx, poolID, PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc})
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Equal(t, []string{hexOf(0x11), hexOf(0x13), hexOf(0x14)}, blocks)
	blocks, total, err = fx.adapter.PoolBlocks(ctx, poolID, PaginationParams{Count: 2, Page: 2, Order: PaginationOrderDesc})
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Equal(t, []string{hexOf(0x11)}, blocks)
}

// addCert records a transaction carrying one certificate and returns the
// certificate id, so a registration or retirement row can reference it.
func (fx *epochFixture) addCert(
	t *testing.T,
	txHash []byte,
	slot uint64,
	blockIndex int,
	certIndex int,
) int64 {
	t.Helper()
	tx := &models.Transaction{Hash: txHash, Slot: slot, BlockIndex: uint32(blockIndex), Valid: true}
	insertAdapterTransaction(t, fx.raw, tx)
	result, err := fx.raw.Exec(
		`INSERT INTO certs (transaction_id, certificate_id, slot, cert_index, cert_type) VALUES (?, 0, ?, ?, 3)`,
		tx.ID, slot, certIndex,
	)
	require.NoError(t, err)
	id, err := result.LastInsertId()
	require.NoError(t, err)
	return id
}

func TestNodeAdapterPoolUpdates(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	poolID := fx.addPool(t, fx.poolA, nil)

	registered := fx.addCert(t, fill32(0xa1), 100, 0, 2)
	retired := fx.addCert(t, fill32(0xa2), 200, 1, 0)
	reregistered := fx.addCert(t, fill32(0xa3), 300, 0, 4)
	// The imported registration is synthetic: rewire it to the first
	// certificate and add the later ones as real rows.
	_, err := fx.raw.Exec(`UPDATE pool_registration SET certificate_id = ?, added_slot = 100`, registered)
	require.NoError(t, err)
	_, err = fx.raw.Exec(
		`INSERT INTO pool_retirement (pool_key_hash, certificate_id, pool_id, epoch, added_slot) VALUES (?, ?, 1, 2, 200)`,
		fx.poolA, retired,
	)
	require.NoError(t, err)
	_, err = fx.raw.Exec(
		`INSERT INTO pool_registration (pool_key_hash, certificate_id, pool_id, added_slot, pledge, cost) VALUES (?, ?, 1, 300, '1', '2')`,
		fx.poolA, reregistered,
	)
	require.NoError(t, err)

	hexOf := func(b byte) string { return hex.EncodeToString(fill32(b)) }
	updates, total, err := fx.adapter.PoolUpdates(ctx, poolID, PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc})
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Equal(t, []PoolUpdateInfo{
		{TxHash: hexOf(0xa1), CertIndex: 2, Action: "registered"},
		{TxHash: hexOf(0xa2), CertIndex: 0, Action: "deregistered"},
		{TxHash: hexOf(0xa3), CertIndex: 4, Action: "registered"},
	}, updates)

	updates, total, err = fx.adapter.PoolUpdates(ctx, poolID, PaginationParams{Count: 2, Page: 1, Order: PaginationOrderDesc})
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Equal(t, []PoolUpdateInfo{
		{TxHash: hexOf(0xa3), CertIndex: 4, Action: "registered"},
		{TxHash: hexOf(0xa2), CertIndex: 0, Action: "deregistered"},
	}, updates)

	// A registration with no transaction (a ledger-state import) is not an
	// update the chain carried.
	_, err = fx.raw.Exec(
		`INSERT INTO pool_registration (pool_key_hash, certificate_id, pool_id, added_slot, pledge, cost) VALUES (?, 0, 1, 5, '1', '2')`,
		fx.poolA,
	)
	require.NoError(t, err)
	_, total, err = fx.adapter.PoolUpdates(ctx, poolID, PaginationParams{Count: 100, Page: 1})
	require.NoError(t, err)
	assert.Equal(t, 3, total)
}

func TestNodeAdapterPoolsRetired(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	retiredPool := bytes32of(28, 0xc1)
	pendingPool := bytes32of(28, 0xc2)
	cancelledPool := bytes32of(28, 0xc3)
	retiredID := fx.addPool(t, retiredPool, nil)
	fx.addPool(t, pendingPool, nil)
	fx.addPool(t, cancelledPool, nil)

	retire := func(pool []byte, epoch uint64, slot uint64, tx byte) {
		cert := fx.addCert(t, fill32(tx), slot, 0, 0)
		_, err := fx.raw.Exec(
			`INSERT INTO pool_retirement (pool_key_hash, certificate_id, pool_id, epoch, added_slot) VALUES (?, ?, 1, ?, ?)`,
			pool, cert, epoch, slot,
		)
		require.NoError(t, err)
	}
	// Registrations imported at slot 10; retirements land after them.
	retire(retiredPool, 1, 100, 0xd1)   // took effect in epoch 1: retired
	retire(pendingPool, 5, 100, 0xd2)   // epoch 5 has not arrived: retiring
	retire(cancelledPool, 1, 100, 0xd3) // cancelled by a later registration
	cert := fx.addCert(t, fill32(0xd4), 200, 0, 0)
	_, err := fx.raw.Exec(
		`INSERT INTO pool_registration (pool_key_hash, certificate_id, pool_id, added_slot, pledge, cost) VALUES (?, ?, 1, 200, '1', '2')`,
		cancelledPool, cert,
	)
	require.NoError(t, err)

	retired, total, err := fx.adapter.PoolsRetired(ctx, PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc})
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Equal(t, []PoolRetiringInfo{{PoolID: retiredID, Epoch: 1}}, retired)

	retired, total, err = fx.adapter.PoolsRetired(ctx, PaginationParams{Count: 100, Page: 2, Order: PaginationOrderDesc})
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Empty(t, retired)

	// The retiring list, which shares the query, still reports only the
	// pending one.
	retiring, total, err := fx.adapter.PoolsRetiring(ctx, PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc})
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	require.Len(t, retiring, 1)
	assert.Equal(t, uint64(5), retiring[0].Epoch)
}

// Retirements recorded at the same point, as a bootstrap import writes them,
// tie on every ordering column but the pool, so pages must still partition
// the list and the two orders must reverse each other.
func TestNodeAdapterPoolsRetiredOrderTiedRetirementsByPool(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	var ids []string
	for _, pool := range [][]byte{bytes32of(28, 0xc2), bytes32of(28, 0xc1)} {
		ids = append(ids, fx.addPool(t, pool, nil))
		_, err := fx.raw.Exec(
			`INSERT INTO pool_retirement (pool_key_hash, certificate_id, pool_id, epoch, added_slot) VALUES (?, 0, 1, 1, 100)`,
			pool,
		)
		require.NoError(t, err)
	}
	page := func(number int, order string) string {
		rows, total, err := fx.adapter.PoolsRetired(ctx, PaginationParams{Count: 1, Page: number, Order: order})
		require.NoError(t, err)
		require.Equal(t, 2, total)
		require.Len(t, rows, 1)
		return rows[0].PoolID
	}
	first, second := page(1, PaginationOrderAsc), page(2, PaginationOrderAsc)
	assert.Equal(t, ids[1], first, "ties order by pool key hash")
	assert.Equal(t, ids[0], second)
	assert.Equal(t, second, page(1, PaginationOrderDesc))
	assert.Equal(t, first, page(2, PaginationOrderDesc))
}

func (fx *epochFixture) addMarkSnapshot(t *testing.T, label uint64, stake string) {
	t.Helper()
	_, err := fx.raw.Exec(
		`INSERT INTO pool_stake_snapshot (epoch, snapshot_type, pool_key_hash, total_stake, delegator_count, captured_slot) VALUES (?, 'mark', ?, ?, 1, 0)`,
		label, fx.poolA, stake,
	)
	require.NoError(t, err)
}

// An epoch at or below a bootstrap anchor has its blocks in the imported
// counts, not in the blocks this node observed.
func TestNodeAdapterPoolHistoryMergesImportedBlockCounts(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	poolID := fx.addPool(t, fx.poolA, nil)
	fx.addMarkSnapshot(t, 0, "700")
	meta := fx.adapter.ledgerState.Database().Metadata()
	// The anchor sits between epoch 1's two blocks of pool A.
	require.NoError(t, meta.SetSyncState("mithril_ledger_slot", "100000", nil))
	require.NoError(t, meta.SaveImportedPoolBlockCounts([]models.ImportedPoolBlockCount{{
		Epoch: 1, PoolKeyHash: fx.poolA, BlocksProduced: 4, CapturedSlot: 100000,
	}}, nil))
	require.NoError(t, meta.SaveImportedEpochBlockTotal(1, 4, 100000, nil))

	history, _, err := fx.adapter.PoolHistory(ctx, poolID, asc)
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, 5, history[0].Blocks, "4 imported plus the one observed above the anchor")

	// Without imported counts for that epoch its blocks are unknown.
	require.NoError(t, meta.DeleteImportedPoolBlockCountsForEpoch(1, nil))
	_, _, err = fx.adapter.PoolHistory(ctx, poolID, asc)
	assert.ErrorIs(t, err, ErrHistoryNotRetained)
}

// A pool's active size divides by the epoch's total active stake, which is
// known only once the epoch summary is ready.
func TestNodeAdapterPoolHistoryRefusesUnreadySummary(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	poolID := fx.addPool(t, fx.poolA, nil)
	fx.addMarkSnapshot(t, 0, "700")
	_, err := fx.raw.Exec(`UPDATE epoch_summary SET snapshot_ready = FALSE WHERE epoch = 0`)
	require.NoError(t, err)
	_, _, err = fx.adapter.PoolHistory(context.Background(), poolID, asc)
	assert.ErrorIs(t, err, ErrHistoryNotRetained)

	_, err = fx.raw.Exec(`DELETE FROM epoch_summary WHERE epoch = 0`)
	require.NoError(t, err)
	_, _, err = fx.adapter.PoolHistory(context.Background(), poolID, asc)
	assert.ErrorIs(t, err, ErrHistoryNotRetained, "a missing summary is not a zero total")
}
