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
	"database/sql"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// get serves one GET request through the full router, so a test also proves
// the route is registered and outranks the unsupported-operation catch-all.
func get(
	t *testing.T,
	node BlockfrostNode,
	target string,
) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	newTestBlockfrost(node).handler().ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, target, nil),
	)
	return recorder
}

func TestHandleLatestEpochReportsOutputFeesAndActiveStake(t *testing.T) {
	t.Parallel()
	activeStake := "123456789"
	node := &mockNode{epoch: EpochInfo{
		Epoch:       7,
		Output:      "9000000",
		Fees:        "180000",
		ActiveStake: &activeStake,
	}}
	recorder := get(t, node, "/api/v0/epochs/latest")
	require.Equal(t, http.StatusOK, recorder.Code)
	var body map[string]any
	require.NoError(t, jsonDecode(recorder, &body))
	assert.Equal(t, "9000000", body["output"])
	assert.Equal(t, "180000", body["fees"])
	assert.Equal(t, "123456789", body["active_stake"])
}

func TestHandleLatestEpochActiveStakeIsNullWithoutSnapshot(t *testing.T) {
	t.Parallel()
	node := &mockNode{epoch: EpochInfo{Epoch: 0, Output: "0", Fees: "0"}}
	recorder := get(t, node, "/api/v0/epochs/latest")
	require.Equal(t, http.StatusOK, recorder.Code)
	var body map[string]any
	require.NoError(t, jsonDecode(recorder, &body))
	value, present := body["active_stake"]
	assert.True(t, present, "active_stake is a required nullable field")
	assert.Nil(t, value)
}

func TestHandleEpoch(t *testing.T) {
	t.Parallel()
	activeStake := "42"
	node := &mockNode{}
	node.epochByNumber.value = EpochInfo{
		Epoch:          500,
		StartTime:      1,
		EndTime:        2,
		FirstBlockTime: 3,
		LastBlockTime:  4,
		BlockCount:     5,
		TxCount:        6,
		Output:         "7",
		Fees:           "8",
		ActiveStake:    &activeStake,
	}
	recorder := get(t, node, "/api/v0/epochs/500")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, uint64(500), node.epochByNumber.number)
	assert.JSONEq(t, `{
		"epoch": 500, "start_time": 1, "end_time": 2,
		"first_block_time": 3, "last_block_time": 4,
		"block_count": 5, "tx_count": 6,
		"output": "7", "fees": "8", "active_stake": "42"
	}`, recorder.Body.String())
}

func TestHandleEpochErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		target string
		err    error
		status int
	}{
		{"not a number", "/api/v0/epochs/abc", nil, http.StatusBadRequest},
		{"negative", "/api/v0/epochs/-1", nil, http.StatusBadRequest},
		{"unknown", "/api/v0/epochs/9", ErrEpochNotFound, http.StatusNotFound},
		{"failure", "/api/v0/epochs/9", errors.New("boom"), http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			node := &mockNode{}
			node.epochByNumber.err = tc.err
			recorder := get(t, node, tc.target)
			assert.Equal(t, tc.status, recorder.Code)
			assert.NotContains(t, recorder.Body.String(), "boom")
		})
	}
}

func TestHandleEpochNextAndPrevious(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		path  string
		stub  func(*mockNode) *pageStub[EpochInfo]
		order bool
	}{
		{"next", "next", func(m *mockNode) *pageStub[EpochInfo] { return &m.epochsAfter }, false},
		{"previous", "previous", func(m *mockNode) *pageStub[EpochInfo] { return &m.epochsBefore }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			node := &mockNode{}
			stub := tc.stub(node)
			stub.items = []EpochInfo{{Epoch: 11, Output: "1", Fees: "2"}, {Epoch: 12, Output: "3", Fees: "4"}}
			stub.total = 250
			recorder := get(t, node, "/api/v0/epochs/10/"+tc.path+"?count=2&page=3")
			require.Equal(t, http.StatusOK, recorder.Code)
			assert.Equal(t, uint64(10), stub.number)
			assert.Equal(t, 2, stub.params.Count)
			assert.Equal(t, 3, stub.params.Page)
			assert.Equal(t, "250", recorder.Header().Get("X-Pagination-Count-Total"))
			assert.Equal(t, "125", recorder.Header().Get("X-Pagination-Page-Total"))
			var body []map[string]any
			require.NoError(t, jsonDecode(recorder, &body))
			require.Len(t, body, 2)
			assert.EqualValues(t, 11, body[0]["epoch"])
			assert.EqualValues(t, 12, body[1]["epoch"])
		})
	}
}

func TestHandleEpochNextEmptyIsArrayAndStatuses(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	recorder := get(t, node, "/api/v0/epochs/10/next")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.JSONEq(t, `[]`, recorder.Body.String())

	node.epochsAfter.err = ErrEpochNotFound
	assert.Equal(t, http.StatusNotFound, get(t, node, "/api/v0/epochs/10/next").Code)
	node.epochsAfter.err = errors.New("boom")
	assert.Equal(t, http.StatusInternalServerError, get(t, node, "/api/v0/epochs/10/next").Code)
	assert.Equal(t, http.StatusBadRequest, get(t, node, "/api/v0/epochs/10/next?count=101").Code)
	assert.Equal(t, http.StatusBadRequest, get(t, node, "/api/v0/epochs/x/next").Code)
}

func TestHandleEpochStakes(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.epochStakes.items = []EpochStakeInfo{{StakeAddress: "stake1abc", PoolID: "pool1abc", Amount: "99"}}
	node.epochStakes.total = 1
	recorder := get(t, node, "/api/v0/epochs/12/stakes?count=5")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, uint64(12), node.epochStakes.number)
	assert.Equal(t, 5, node.epochStakes.params.Count)
	assert.JSONEq(t, `[{"stake_address":"stake1abc","pool_id":"pool1abc","amount":"99"}]`, recorder.Body.String())
	assert.Equal(t, "1", recorder.Header().Get("X-Pagination-Count-Total"))

	node.epochStakes.err = ErrEpochNotFound
	assert.Equal(t, http.StatusNotFound, get(t, node, "/api/v0/epochs/12/stakes").Code)
}

// The Blockfrost schema defines only count and page for these lists, so order
// is ignored like any other undefined query parameter: neither applied nor
// validated.
func TestHandleUnorderedEpochListsIgnoreOrder(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		path   string
		params func(*mockNode) PaginationParams
	}{
		{"/api/v0/epochs/1/stakes", func(n *mockNode) PaginationParams { return n.epochStakes.params }},
		{"/api/v0/epochs/1/stakes/pool1xyz", func(n *mockNode) PaginationParams { return n.epochPoolStakes.params }},
		{"/api/v0/epochs/1/next", func(n *mockNode) PaginationParams { return n.epochsAfter.params }},
		{"/api/v0/epochs/1/previous", func(n *mockNode) PaginationParams { return n.epochsBefore.params }},
	} {
		for _, order := range []string{"desc", "sideways"} {
			t.Run(tc.path+"?order="+order, func(t *testing.T) {
				t.Parallel()
				node := &mockNode{}
				recorder := get(t, node, tc.path+"?count=5&order="+order)
				require.Equal(t, http.StatusOK, recorder.Code)
				params := tc.params(node)
				assert.Equal(t, PaginationOrderAsc, params.Order)
				assert.Equal(t, 5, params.Count)
			})
		}
	}
}

func TestHandleEpochPoolStakes(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.epochPoolStakes.items = []EpochPoolStakeInfo{{StakeAddress: "stake1abc", Amount: "99"}}
	node.epochPoolStakes.total = 1
	recorder := get(t, node, "/api/v0/epochs/12/stakes/pool1xyz")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, uint64(12), node.epochPoolStakes.number)
	assert.Equal(t, "pool1xyz", node.epochPoolStakes.key)
	assert.JSONEq(t, `[{"stake_address":"stake1abc","amount":"99"}]`, recorder.Body.String())

	node.epochPoolStakes.err = ErrInvalidPoolID
	assert.Equal(t, http.StatusBadRequest, get(t, node, "/api/v0/epochs/12/stakes/bad").Code)
	node.epochPoolStakes.err = ErrEpochNotFound
	assert.Equal(t, http.StatusNotFound, get(t, node, "/api/v0/epochs/12/stakes/pool1xyz").Code)
}

func TestHandleEpochStakesNotRetained(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.epochStakes.err = ErrHistoryNotRetained
	node.epochPoolStakes.err = ErrHistoryNotRetained
	for _, target := range []string{
		"/api/v0/epochs/12/stakes",
		"/api/v0/epochs/12/stakes/pool1xyz",
	} {
		recorder := get(t, node, target)
		assert.Equal(t, http.StatusNotFound, recorder.Code, target)
		assert.Contains(t, recorder.Body.String(), "not retained by this node", target)
	}
}

func TestHandleEpochBlocks(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.epochBlocks.items = []string{"aa", "bb"}
	node.epochBlocks.total = 2
	recorder := get(t, node, "/api/v0/epochs/12/blocks?order=desc&count=10")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, uint64(12), node.epochBlocks.number)
	assert.Equal(t, PaginationOrderDesc, node.epochBlocks.params.Order)
	assert.JSONEq(t, `["aa","bb"]`, recorder.Body.String())

	node.epochPoolBlocks.items = []string{"cc"}
	node.epochPoolBlocks.total = 1
	recorder = get(t, node, "/api/v0/epochs/12/blocks/pool1xyz?order=asc")
	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, "pool1xyz", node.epochPoolBlocks.key)
	assert.Equal(t, PaginationOrderAsc, node.epochPoolBlocks.params.Order)
	assert.JSONEq(t, `["cc"]`, recorder.Body.String())

	node.epochPoolBlocks.err = ErrInvalidPoolID
	assert.Equal(t, http.StatusBadRequest, get(t, node, "/api/v0/epochs/12/blocks/bad").Code)
}

// epochFixture is a preview-network adapter with three 86400-slot epochs,
// the tip inside the third, and the transactions, blocks and stake rows the
// epoch endpoints aggregate.
type epochFixture struct {
	adapter *NodeAdapter
	raw     *sql.DB
	poolA   []byte
	poolB   []byte
}

func newEpochFixture(t *testing.T) *epochFixture {
	t.Helper()
	nodeConfig, err := cardano.NewCardanoNodeConfigFromEmbedFS(
		cardano.EmbeddedConfigFS,
		cardano.EmbeddedConfigPath("preview"),
	)
	require.NoError(t, err)
	adapter, raw, _ := newDBBackedAdapter(t, nodeConfig)
	for epoch := range uint64(3) {
		_, err := raw.Exec(
			`INSERT INTO epoch (epoch_id, start_slot, length_in_slots, slot_length, era_id) VALUES (?, ?, 86400, 1000, 6)`,
			epoch, epoch*86400,
		)
		require.NoError(t, err)
	}
	require.NoError(t, adapter.ledgerState.PrepareEpochCacheForStartup(context.Background()))
	adapter.ledgerState.SetTipForTesting(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 172900, Hash: fill32(0xee)},
		BlockNumber: 5,
	})

	// Epoch 1: a valid transaction, a two-output transaction and an invalid
	// one whose collateral return is what reached the chain. Epoch 2: one.
	for _, tx := range []*models.Transaction{
		{Hash: fill32(1), Slot: 90000, Valid: true, Fee: 10, Outputs: []models.Utxo{{TxId: fill32(1), Amount: 100}}},
		{Hash: fill32(2), Slot: 100000, Valid: true, Fee: 20, Outputs: []models.Utxo{
			{TxId: fill32(2), OutputIdx: 0, Amount: 200},
			{TxId: fill32(2), OutputIdx: 1, Amount: 300},
		}},
		{
			Hash: fill32(3), Slot: 110000, Valid: false, Fee: 5, CollateralFee: 5,
			Outputs:          []models.Utxo{{TxId: fill32(3), Amount: 999}},
			CollateralReturn: &models.Utxo{TxId: fill32(4), Amount: 50},
		},
		{Hash: fill32(5), Slot: 172850, Valid: true, Fee: 7, Outputs: []models.Utxo{{TxId: fill32(5), Amount: 70}}},
	} {
		insertAdapterTransaction(t, raw, tx)
	}

	poolA := bytes32of(28, 0xa1)
	poolB := bytes32of(28, 0xb2)
	for _, block := range []struct {
		slot uint64
		hash byte
		pool []byte
	}{
		{90000, 0x11, poolA},
		{100000, 0x12, poolB},
		{110000, 0x13, poolA},
		{172850, 0x14, poolA},
	} {
		_, err := raw.Exec(
			`INSERT INTO block_nonce (hash, nonce, slot, is_checkpoint) VALUES (?, ?, ?, FALSE)`,
			fill32(block.hash), fill32(0x00), block.slot,
		)
		require.NoError(t, err)
		_, err = raw.Exec(
			`INSERT INTO pool_opcert_sequence (pool_key_hash, slot, sequence) VALUES (?, ?, 1)`,
			block.pool, block.slot,
		)
		require.NoError(t, err)
	}

	for _, summary := range []struct {
		epoch uint64
		stake string
		ready bool
	}{{0, "5000", true}, {1, "9000", true}, {2, "11000", false}} {
		_, err := raw.Exec(
			`INSERT INTO epoch_summary (epoch, total_active_stake, total_pool_count, total_delegators, boundary_slot, snapshot_ready) VALUES (?, ?, 2, 3, 0, ?)`,
			summary.epoch, summary.stake, summary.ready,
		)
		require.NoError(t, err)
	}

	// Snapshot label 0 is the distribution active in epoch 1.
	for _, stake := range []struct {
		pool       []byte
		key        byte
		tag        uint8
		amount     string
		registered bool
	}{
		{poolA, 0x01, 0, "700", true},
		{poolA, 0x02, 1, "200", true},
		{poolA, 0x03, 0, "50", false},
		{poolB, 0x04, 0, "100", true},
	} {
		_, err := raw.Exec(
			`INSERT INTO reward_stake_input (pool_key_hash, staking_key, epoch, credential_tag, stake, owner, registered, captured_slot, boundary_slot) VALUES (?, ?, 0, ?, ?, FALSE, ?, 0, 0)`,
			stake.pool, bytes32of(28, stake.key), stake.tag, stake.amount, stake.registered,
		)
		require.NoError(t, err)
	}
	return &epochFixture{adapter: adapter, raw: raw, poolA: poolA, poolB: poolB}
}

func bytes32of(n int, b byte) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

func TestNodeAdapterEpochAggregates(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()

	info, err := fx.adapter.Epoch(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), info.Epoch)
	assert.Equal(t, 3, info.BlockCount)
	assert.Equal(t, 3, info.TxCount)
	// 10 + 20, plus the 5 collateral fee an invalid transaction pays.
	assert.Equal(t, "35", info.Fees)
	// 100 + 200 + 300 from valid transactions, and the 50 collateral return of
	// the invalid one rather than its discarded 999.
	assert.Equal(t, "650", info.Output)
	require.NotNil(t, info.ActiveStake)
	assert.Equal(t, "5000", *info.ActiveStake, "epoch 1 runs on snapshot label 0")
	assert.Equal(t, info.EndTime-info.StartTime, int64(86400))
	assert.Less(t, info.FirstBlockTime, info.LastBlockTime)

	info, err = fx.adapter.Epoch(ctx, 0)
	require.NoError(t, err)
	assert.Nil(t, info.ActiveStake, "epoch 0 has no snapshot")
	assert.Equal(t, "0", info.Fees)
	assert.Equal(t, "0", info.Output)

	info, err = fx.adapter.Epoch(ctx, 2)
	require.NoError(t, err)
	assert.Equal(t, 1, info.BlockCount)
	assert.Equal(t, "7", info.Fees)
	assert.Equal(t, "70", info.Output)
	require.NotNil(t, info.ActiveStake)
	assert.Equal(t, "9000", *info.ActiveStake)

	_, err = fx.adapter.Epoch(ctx, 3)
	assert.ErrorIs(t, err, ErrEpochNotFound)

	current, err := fx.adapter.CurrentEpoch(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), current.Epoch)
	assert.Equal(t, "7", current.Fees)
	assert.Equal(t, "70", current.Output)
	require.NotNil(t, current.ActiveStake)
	assert.Equal(t, "9000", *current.ActiveStake)
}

func TestNodeAdapterEpochActiveStakeNullWhenSnapshotNotReady(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	_, err := fx.raw.Exec(`UPDATE epoch_summary SET snapshot_ready = FALSE WHERE epoch = 1`)
	require.NoError(t, err)
	info, err := fx.adapter.Epoch(context.Background(), 2)
	require.NoError(t, err)
	assert.Nil(t, info.ActiveStake)
}

func TestNodeAdapterEpochsAfterAndBefore(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	numbers := func(items []EpochInfo) []uint64 {
		out := []uint64{}
		for _, item := range items {
			out = append(out, item.Epoch)
		}
		return out
	}
	page := func(count, page int) PaginationParams {
		return PaginationParams{Count: count, Page: page, Order: PaginationOrderAsc}
	}

	items, total, err := fx.adapter.EpochsAfter(ctx, 0, page(100, 1))
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Equal(t, []uint64{1, 2}, numbers(items))

	items, total, err = fx.adapter.EpochsAfter(ctx, 0, page(1, 2))
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Equal(t, []uint64{2}, numbers(items))

	items, total, err = fx.adapter.EpochsAfter(ctx, 2, page(100, 1))
	require.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Empty(t, items)

	items, total, err = fx.adapter.EpochsBefore(ctx, 2, page(100, 1))
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Equal(t, []uint64{1, 0}, numbers(items), "nearest epoch first")

	items, total, err = fx.adapter.EpochsBefore(ctx, 2, page(1, 2))
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Equal(t, []uint64{0}, numbers(items))

	items, total, err = fx.adapter.EpochsBefore(ctx, 0, page(100, 1))
	require.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Empty(t, items)

	items, total, err = fx.adapter.EpochsAfter(ctx, 0, page(100, 5))
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Empty(t, items, "a page past the end is empty, not an error")

	_, _, err = fx.adapter.EpochsAfter(ctx, 3, page(100, 1))
	assert.ErrorIs(t, err, ErrEpochNotFound)
	_, _, err = fx.adapter.EpochsBefore(ctx, 3, page(100, 1))
	assert.ErrorIs(t, err, ErrEpochNotFound)
}

func TestNodeAdapterEpochBlocks(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	hexOf := func(b byte) string { return hex.EncodeToString(fill32(b)) }

	hashes, total, err := fx.adapter.EpochBlocks(ctx, 1, PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc})
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Equal(t, []string{hexOf(0x11), hexOf(0x12), hexOf(0x13)}, hashes)

	hashes, total, err = fx.adapter.EpochBlocks(ctx, 1, PaginationParams{Count: 2, Page: 1, Order: PaginationOrderDesc})
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Equal(t, []string{hexOf(0x13), hexOf(0x12)}, hashes)

	hashes, total, err = fx.adapter.EpochBlocks(ctx, 1, PaginationParams{Count: 2, Page: 2, Order: PaginationOrderDesc})
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Equal(t, []string{hexOf(0x11)}, hashes)

	poolA, err := poolIDString(fx.poolA)
	require.NoError(t, err)
	hashes, total, err = fx.adapter.EpochPoolBlocks(ctx, 1, poolA, PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc})
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Equal(t, []string{hexOf(0x11), hexOf(0x13)}, hashes)

	hashes, total, err = fx.adapter.EpochPoolBlocks(ctx, 2, poolA, PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc})
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Equal(t, []string{hexOf(0x14)}, hashes)

	hashes, total, err = fx.adapter.EpochPoolBlocks(ctx, 0, poolA, PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc})
	require.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Empty(t, hashes)

	_, _, err = fx.adapter.EpochPoolBlocks(ctx, 1, "not-a-pool", PaginationParams{Count: 1, Page: 1})
	assert.ErrorIs(t, err, ErrInvalidPoolID)
	_, _, err = fx.adapter.EpochBlocks(ctx, 9, PaginationParams{Count: 1, Page: 1})
	assert.ErrorIs(t, err, ErrEpochNotFound)
}

// Nonce pruning keeps an epoch's checkpoint row and deletes the rest, so a
// pruned epoch still lists one block. The block lists must report that
// history as unavailable rather than serve the short list as complete.
func TestNodeAdapterBlockListsRefuseUnretainedHistory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	asc := PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc}
	hexOf := func(b byte) string { return hex.EncodeToString(fill32(b)) }

	t.Run("pruned epoch", func(t *testing.T) {
		t.Parallel()
		fx := newEpochFixture(t)
		poolID := fx.addPool(t, fx.poolA, nil)
		// Epoch 1 keeps only its checkpoint; epoch 2 is retained in full.
		_, err := fx.raw.Exec(`UPDATE block_nonce SET is_checkpoint = TRUE WHERE slot IN (90000, 172850)`)
		require.NoError(t, err)
		_, err = fx.raw.Exec(`DELETE FROM block_nonce WHERE slot IN (100000, 110000)`)
		require.NoError(t, err)
		_, err = fx.raw.Exec(
			`INSERT INTO block_nonce (hash, nonce, slot, is_checkpoint) VALUES (?, ?, 172870, FALSE)`,
			fill32(0x15), fill32(0x00),
		)
		require.NoError(t, err)

		_, _, err = fx.adapter.EpochBlocks(ctx, 1, asc)
		assert.ErrorIs(t, err, ErrHistoryNotRetained)
		_, _, err = fx.adapter.EpochPoolBlocks(ctx, 1, poolID, asc)
		assert.ErrorIs(t, err, ErrHistoryNotRetained)
		_, _, err = fx.adapter.PoolBlocks(ctx, poolID, asc)
		assert.ErrorIs(t, err, ErrHistoryNotRetained)

		hashes, total, err := fx.adapter.EpochBlocks(ctx, 2, asc)
		require.NoError(t, err)
		assert.Equal(t, 2, total)
		assert.Equal(t, []string{hexOf(0x14), hexOf(0x15)}, hashes)

		hashes, total, err = fx.adapter.EpochBlocks(ctx, 0, asc)
		require.NoError(t, err, "an epoch with no blocks is empty, not pruned")
		assert.Equal(t, 0, total)
		assert.Empty(t, hashes)
	})

	t.Run("bootstrap anchor", func(t *testing.T) {
		t.Parallel()
		fx := newEpochFixture(t)
		poolID := fx.addPool(t, fx.poolA, nil)
		meta := fx.adapter.ledgerState.Database().Metadata()
		require.NoError(t, meta.SetSyncState("mithril_ledger_slot", "100000", nil))

		_, _, err := fx.adapter.EpochBlocks(ctx, 1, asc)
		assert.ErrorIs(t, err, ErrHistoryNotRetained)
		_, _, err = fx.adapter.EpochPoolBlocks(ctx, 0, poolID, asc)
		assert.ErrorIs(t, err, ErrHistoryNotRetained)
		_, _, err = fx.adapter.PoolBlocks(ctx, poolID, asc)
		assert.ErrorIs(t, err, ErrHistoryNotRetained)

		hashes, total, err := fx.adapter.EpochPoolBlocks(ctx, 2, poolID, asc)
		require.NoError(t, err)
		assert.Equal(t, 1, total)
		assert.Equal(t, []string{hexOf(0x14)}, hashes)
	})
}

// A Mithril-bootstrapped node applies no block at or below its anchor, so the
// block lists refuse that range until the historical backfill has completed
// and left block_nonce rows there.
func TestNodeAdapterBlockListsAfterBackfillBelowAnchor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	asc := PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc}
	hexOf := func(b byte) string { return hex.EncodeToString(fill32(b)) }
	setup := func(t *testing.T, completed bool) *epochFixture {
		t.Helper()
		fx := newEpochFixture(t)
		meta := fx.adapter.ledgerState.Database().Metadata()
		require.NoError(t, meta.SetSyncState("mithril_ledger_slot", "100000", nil))
		require.NoError(t, meta.SetBackfillCheckpoint(&models.BackfillCheckpoint{
			Phase:     "metadata",
			LastSlot:  100000,
			Completed: completed,
		}, nil))
		return fx
	}

	t.Run("completed backfill serves history below the anchor", func(t *testing.T) {
		t.Parallel()
		fx := setup(t, true)
		hashes, total, err := fx.adapter.EpochBlocks(ctx, 1, asc)
		require.NoError(t, err)
		assert.Equal(t, 3, total)
		assert.Equal(t, []string{hexOf(0x11), hexOf(0x12), hexOf(0x13)}, hashes)
	})

	t.Run("epoch before the first indexed block is not retained", func(t *testing.T) {
		t.Parallel()
		fx := setup(t, true)
		_, _, err := fx.adapter.EpochBlocks(ctx, 0, asc)
		assert.ErrorIs(t, err, ErrHistoryNotRetained)
	})

	t.Run("interrupted backfill is not retained", func(t *testing.T) {
		t.Parallel()
		fx := setup(t, false)
		_, _, err := fx.adapter.EpochBlocks(ctx, 1, asc)
		assert.ErrorIs(t, err, ErrHistoryNotRetained)
	})

	t.Run("backfill that wrote no rows is not retained", func(t *testing.T) {
		t.Parallel()
		fx := setup(t, true)
		_, err := fx.raw.Exec(`DELETE FROM block_nonce WHERE slot <= 100000`)
		require.NoError(t, err)
		_, _, err = fx.adapter.EpochBlocks(ctx, 1, asc)
		assert.ErrorIs(t, err, ErrHistoryNotRetained)
	})
}

func TestNodeAdapterEpochStakes(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	address := func(tag uint8, key byte) string {
		addr, err := fx.adapter.stakeAddressFromTag(tag, bytes32of(28, key))
		require.NoError(t, err)
		return addr
	}
	poolA, err := poolIDString(fx.poolA)
	require.NoError(t, err)
	poolB, err := poolIDString(fx.poolB)
	require.NoError(t, err)
	poolC, err := poolIDString(bytes32of(28, 0xc3))
	require.NoError(t, err)

	stakes, total, err := fx.adapter.EpochStakes(ctx, 1, PaginationParams{Count: 100, Page: 1})
	require.NoError(t, err)
	assert.Equal(t, 3, total, "the unregistered credential is not part of the distribution")
	assert.Equal(t, []EpochStakeInfo{
		{StakeAddress: address(0, 0x01), PoolID: poolA, Amount: "700"},
		{StakeAddress: address(1, 0x02), PoolID: poolA, Amount: "200"},
		{StakeAddress: address(0, 0x04), PoolID: poolB, Amount: "100"},
	}, stakes)
	assert.Contains(t, address(1, 0x02), "stake_test17", "script credentials use the script stake address header")

	stakes, total, err = fx.adapter.EpochStakes(ctx, 1, PaginationParams{Count: 2, Page: 2})
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	require.Len(t, stakes, 1)
	assert.Equal(t, "100", stakes[0].Amount)

	poolStakes, total, err := fx.adapter.EpochPoolStakes(ctx, 1, poolA, PaginationParams{Count: 100, Page: 1})
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Equal(t, []EpochPoolStakeInfo{
		{StakeAddress: address(0, 0x01), Amount: "700"},
		{StakeAddress: address(1, 0x02), Amount: "200"},
	}, poolStakes)

	// Label 1's summary reports three delegators yet no per-credential rows
	// remain, which is what pruning leaves behind; an empty list would read
	// as a complete distribution.
	_, _, err = fx.adapter.EpochStakes(ctx, 2, PaginationParams{Count: 100, Page: 1})
	assert.ErrorIs(t, err, ErrHistoryNotRetained)
	_, _, err = fx.adapter.EpochPoolStakes(ctx, 2, poolA, PaginationParams{Count: 100, Page: 1})
	assert.ErrorIs(t, err, ErrHistoryNotRetained)

	// A pool with no delegators in a retained distribution is a genuine
	// empty answer, not a pruned one.
	poolStakes, total, err = fx.adapter.EpochPoolStakes(ctx, 1, poolC, PaginationParams{Count: 100, Page: 1})
	require.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Empty(t, poolStakes)

	// A captured snapshot with no delegators is a genuine empty distribution.
	_, err = fx.raw.Exec(`UPDATE epoch_summary SET total_delegators = 0 WHERE epoch = 1`)
	require.NoError(t, err)
	stakes, total, err = fx.adapter.EpochStakes(ctx, 2, PaginationParams{Count: 100, Page: 1})
	require.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Empty(t, stakes)

	// A snapshot that was never captured is unavailable, not empty.
	_, err = fx.raw.Exec(`DELETE FROM epoch_summary WHERE epoch = 1`)
	require.NoError(t, err)
	_, _, err = fx.adapter.EpochStakes(ctx, 2, PaginationParams{Count: 100, Page: 1})
	assert.ErrorIs(t, err, ErrHistoryNotRetained)

	stakes, total, err = fx.adapter.EpochStakes(ctx, 0, PaginationParams{Count: 100, Page: 1})
	require.NoError(t, err)
	assert.Equal(t, 0, total, "epoch 0 has no snapshot")
	assert.Empty(t, stakes)

	_, _, err = fx.adapter.EpochStakes(ctx, 9, PaginationParams{Count: 1, Page: 1})
	assert.ErrorIs(t, err, ErrEpochNotFound)
	_, _, err = fx.adapter.EpochPoolStakes(ctx, 1, "not-a-pool", PaginationParams{Count: 1, Page: 1})
	assert.ErrorIs(t, err, ErrInvalidPoolID)
}

func TestEpochWindow(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		epoch, current uint64
		after          bool
		params         PaginationParams
		first          uint64
		count, total   int
	}{
		{"after full", 5, 8, true, PaginationParams{Count: 100, Page: 1}, 6, 3, 3},
		{"after second page", 5, 8, true, PaginationParams{Count: 2, Page: 2}, 8, 1, 3},
		{"after past end", 5, 8, true, PaginationParams{Count: 2, Page: 3}, 0, 0, 3},
		{"after at tip", 8, 8, true, PaginationParams{Count: 2, Page: 1}, 0, 0, 0},
		{"before full", 5, 8, false, PaginationParams{Count: 100, Page: 1}, 4, 5, 5},
		{"before second page", 5, 8, false, PaginationParams{Count: 2, Page: 2}, 2, 2, 5},
		{"before epoch zero", 0, 8, false, PaginationParams{Count: 2, Page: 1}, 0, 0, 0},
		{"invalid page", 5, 8, true, PaginationParams{Count: 2, Page: 0}, 0, 0, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			first, count, total := epochWindow(tc.epoch, tc.current, tc.after, tc.params)
			assert.Equal(t, tc.first, first)
			assert.Equal(t, tc.count, count)
			assert.Equal(t, tc.total, total)
		})
	}
}

// A Leios closure transaction is stored at the certifying block's slot but
// executes at its context slot; its outputs count in the epoch it executed
// in, as its fee does.
func TestNodeAdapterEpochOutputUsesLeiosExecutionSlot(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	tx := &models.Transaction{
		Hash: fill32(0x41), Slot: 172805, Valid: true, Fee: 3,
		Outputs: []models.Utxo{{TxId: fill32(0x41), Amount: 40}},
	}
	insertAdapterTransaction(t, fx.raw, tx)
	_, err := fx.raw.Exec(
		`INSERT INTO leios_transaction_context (transaction_id, slot) VALUES (?, 172700)`,
		tx.ID,
	)
	require.NoError(t, err)

	info, err := fx.adapter.Epoch(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, "38", info.Fees)
	assert.Equal(t, "690", info.Output)
	info, err = fx.adapter.Epoch(ctx, 2)
	require.NoError(t, err)
	assert.Equal(t, "7", info.Fees)
	assert.Equal(t, "70", info.Output)
}
