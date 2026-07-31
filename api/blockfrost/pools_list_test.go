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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package blockfrost

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	sqliteplugin "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// ---------------------------------------------------------------------
// Handler tests (mockNode)
// ---------------------------------------------------------------------

// TestHandlePoolsList covers the basic response shape: a flat JSON array
// of bech32 pool ID strings (pool_list), plus the X-Pagination-* headers.
func TestHandlePoolsList(t *testing.T) {
	mock := &mockNode{
		poolsList: []string{
			"pool1pu5jlj4q9w9jlxeu370a3c9myx47md5j5m2str0naunn2q3lkdy",
			"pool1hn7hlwrschqykupwwrtdfkvt2u4uaxvsgxyh6z63703p2knj288",
		},
		poolsListTotal: 2,
	}
	b := newTestBlockfrost(mock)
	req := httptest.NewRequest(http.MethodGet, "/api/v0/pools", nil)
	w := httptest.NewRecorder()
	b.handlePoolsList(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "2", w.Header().Get("X-Pagination-Count-Total"))
	assert.Equal(t, "1", w.Header().Get("X-Pagination-Page-Total"))

	var resp []string
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, mock.poolsList, resp)

	// Default pagination: count=100, page=1, order=asc.
	assert.Equal(t, DefaultPaginationCount, mock.poolsListParams.Count)
	assert.Equal(t, DefaultPaginationPage, mock.poolsListParams.Page)
	assert.Equal(t, PaginationOrderAsc, mock.poolsListParams.Order)
}

// TestHandlePoolsListPaginationParams verifies count/page/order query
// parameters reach PoolsList unchanged.
func TestHandlePoolsListPaginationParams(t *testing.T) {
	mock := &mockNode{}
	b := newTestBlockfrost(mock)
	req := httptest.NewRequest(
		http.MethodGet, "/api/v0/pools?count=5&page=3&order=desc", nil,
	)
	w := httptest.NewRecorder()
	b.handlePoolsList(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 5, mock.poolsListParams.Count)
	assert.Equal(t, 3, mock.poolsListParams.Page)
	assert.Equal(t, PaginationOrderDesc, mock.poolsListParams.Order)
}

// TestHandlePoolsListInvalidPagination covers Blockfrost-exact fastify
// validation error messages (ParsePaginationStrict), matching the sibling
// /pools/retiring handler's pagination behavior.
func TestHandlePoolsListInvalidPagination(t *testing.T) {
	b := newTestBlockfrost(&mockNode{})
	req := httptest.NewRequest(
		http.MethodGet, "/api/v0/pools?order=sideways", nil,
	)
	w := httptest.NewRecorder()
	b.handlePoolsList(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(
		t,
		"querystring/order must be equal to one of the allowed values",
		resp.Message,
	)
}

// TestHandlePoolsListInvalidCount covers the count-out-of-range case
// specifically, since it exercises a different ParsePaginationStrict
// branch than order.
func TestHandlePoolsListInvalidCount(t *testing.T) {
	b := newTestBlockfrost(&mockNode{})
	req := httptest.NewRequest(http.MethodGet, "/api/v0/pools?count=101", nil)
	w := httptest.NewRecorder()
	b.handlePoolsList(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "querystring/count must be <= 100", resp.Message)
}

// TestHandlePoolsListEmpty covers the no-active-pools case: the response
// must be an empty JSON array, not null, with zeroed pagination headers.
func TestHandlePoolsListEmpty(t *testing.T) {
	mock := &mockNode{poolsList: []string{}, poolsListTotal: 0}
	b := newTestBlockfrost(mock)
	req := httptest.NewRequest(http.MethodGet, "/api/v0/pools", nil)
	w := httptest.NewRecorder()
	b.handlePoolsList(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "0", w.Header().Get("X-Pagination-Count-Total"))
	assert.Equal(t, "0", w.Header().Get("X-Pagination-Page-Total"))
	assert.Equal(t, "[]\n", w.Body.String())
}

// TestHandlePoolsListDatabaseFailure covers a backing-store failure: it
// must surface as a generic 500, not be silently swallowed.
func TestHandlePoolsListDatabaseFailure(t *testing.T) {
	mock := &mockNode{poolsListErr: errors.New("database is closed")}
	b := newTestBlockfrost(mock)
	req := httptest.NewRequest(http.MethodGet, "/api/v0/pools", nil)
	w := httptest.NewRecorder()
	b.handlePoolsList(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	var resp ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "failed to retrieve pools", resp.Message)
}

// TestPoolsListRouteDoesNotSwallowSiblings is the acceptance test for
// route registration required by #3011: "GET /api/v0/pools" must resolve
// to handlePoolsList, and every other pools route --
// "/pools/extended", "/pools/retiring", "/pools/{pool_id}/metadata", and
// "/pools/{pool_id}" -- must still resolve to its own handler rather than
// being captured by the bare "/pools" route or the "/pools/{pool_id}"
// wildcard. This exercises the real http.ServeMux built by
// (*Blockfrost).handler(), not a direct method call, so it verifies Go's
// actual pattern-specificity resolution rather than assuming it (Go 1.22+
// ServeMux prefers the most specific literal match). This is additive to
// TestPoolsRouteOrderingPoolDetailDoesNotSwallowSiblings
// (pool_detail_test.go), which predates the "/pools" route and is left
// unmodified.
func TestPoolsListRouteDoesNotSwallowSiblings(t *testing.T) {
	metadataURL := "https://example.com/pool.json"
	mock := &mockNode{
		poolsListTotal:     1,
		poolsList:          []string{"pool1list"},
		poolsRetiringTotal: 1,
		poolsRetiring: []PoolRetiringInfo{
			{PoolID: "pool1retiring", Epoch: 10},
		},
		pools: []PoolExtendedInfo{
			{PoolID: "pool1extended"},
		},
		poolDetail: PoolDetailInfo{PoolID: "pool1detail"},
		poolMetadata: PoolMetadataInfo{
			PoolID: "pool1metadata",
			// A non-nil URL is required for handlePoolMetadata to return
			// the full PoolMetadataResponse; a nil URL (no registered
			// anchor) instead answers with an empty JSON object, which
			// would make this route-precedence check indistinguishable
			// from a routing bug.
			URL: &metadataURL,
		},
	}
	b := newTestBlockfrost(mock)
	handler := b.handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v0/pools", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var listResp []string
	require.NoError(t, json.NewDecoder(w.Body).Decode(&listResp))
	require.Equal(t, []string{"pool1list"}, listResp)

	req = httptest.NewRequest(http.MethodGet, "/api/v0/pools/extended", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var extendedResp []PoolExtendedResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&extendedResp))
	require.Len(t, extendedResp, 1)
	assert.Equal(t, "pool1extended", extendedResp[0].PoolID)

	req = httptest.NewRequest(http.MethodGet, "/api/v0/pools/retiring", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var retiringResp []PoolRetiringResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&retiringResp))
	require.Len(t, retiringResp, 1)
	assert.Equal(t, "pool1retiring", retiringResp[0].PoolID)

	req = httptest.NewRequest(
		http.MethodGet, "/api/v0/pools/pool1notretiringnorextended/metadata", nil,
	)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var metadataResp PoolMetadataResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&metadataResp))
	assert.Equal(t, "pool1metadata", metadataResp.PoolID)

	req = httptest.NewRequest(
		http.MethodGet, "/api/v0/pools/pool1notretiringnorextended", nil,
	)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var detailResp PoolDetailResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&detailResp))
	assert.Equal(t, "pool1detail", detailResp.PoolID)
}

// ---------------------------------------------------------------------
// Adapter tests (real sqlite-backed NodeAdapter)
// ---------------------------------------------------------------------

// ensurePoolsListChainTip writes the tip and epoch rows
// GetActivePoolKeyHashesOrdered needs to resolve "active as of the
// current tip". Slot 1000 / epoch length 2000 gives headroom for the
// added_slot values used by the ordering tests below (up to slot 500).
func ensurePoolsListChainTip(
	t *testing.T,
	store *sqliteplugin.MetadataStoreSqlite,
) {
	t.Helper()
	require.NoError(t, store.DB().
		Where("id = ?", 1).
		Attrs(models.Tip{Slot: 1000}).
		FirstOrCreate(&models.Tip{ID: 1}).Error)
	require.NoError(t, store.DB().
		Where("epoch_id = ?", 0).
		Attrs(models.Epoch{StartSlot: 0, LengthInSlots: 2000}).
		FirstOrCreate(&models.Epoch{EpochId: 0}).Error)
}

// seedPoolListPool creates a bare pool row and returns its surrogate ID
// (needed to attach PoolRegistration/PoolRetirement rows by PoolID).
func seedPoolListPool(
	t *testing.T,
	store *sqliteplugin.MetadataStoreSqlite,
	poolKeyHash []byte,
) uint {
	t.Helper()
	pool := &models.Pool{PoolKeyHash: poolKeyHash}
	require.NoError(t, store.DB().Create(pool).Error)
	return pool.ID
}

// seedPoolListRegistration adds a registration certificate for an
// already-created pool. certificateID is 0 for registrations that do not
// need a real (block_index, cert_index) tie-break position (a distinct
// added_slot alone is enough to order them), matching seedExtendedPool's
// use of AddedSlot alone elsewhere in this package.
func seedPoolListRegistration(
	t *testing.T,
	store *sqliteplugin.MetadataStoreSqlite,
	poolID uint,
	poolKeyHash []byte,
	addedSlot uint64,
	certificateID uint,
) {
	t.Helper()
	require.NoError(t, store.DB().Create(&models.PoolRegistration{
		PoolID:        poolID,
		PoolKeyHash:   poolKeyHash,
		AddedSlot:     addedSlot,
		CertificateID: certificateID,
	}).Error)
}

// seedPoolListRetirement adds a retirement certificate for an
// already-created pool.
func seedPoolListRetirement(
	t *testing.T,
	store *sqliteplugin.MetadataStoreSqlite,
	poolID uint,
	poolKeyHash []byte,
	addedSlot uint64,
	epoch uint64,
) {
	t.Helper()
	require.NoError(t, store.DB().Create(&models.PoolRetirement{
		PoolID:      poolID,
		PoolKeyHash: poolKeyHash,
		AddedSlot:   addedSlot,
		Epoch:       epoch,
	}).Error)
}

// seedPoolListCert creates a transaction plus one certificate row so a
// registration's on-chain position can be pinned to a specific
// (block_index, cert_index), for the same-slot tie-break tests. txID and
// certID must be unique across the whole test (they are primary keys).
func seedPoolListCert(
	t *testing.T,
	store *sqliteplugin.MetadataStoreSqlite,
	txID uint,
	certID uint,
	slot uint64,
	blockIndex uint32,
	certIndex uint,
) uint {
	t.Helper()
	// Transaction.Hash has a uniqueIndex; txID is small and unique per
	// call, so it doubles as a distinct fill byte.
	hash := fill32(byte(txID))
	var existing models.Transaction
	err := store.DB().Where("id = ?", txID).First(&existing).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		require.NoError(t, store.DB().Create(&models.Transaction{
			ID:         txID,
			Slot:       slot,
			BlockIndex: blockIndex,
			Hash:       hash,
		}).Error)
	} else {
		require.NoError(t, err)
	}
	require.NoError(t, store.DB().Create(&models.Certificate{
		ID:            certID,
		TransactionID: txID,
		Slot:          slot,
		CertIndex:     certIndex,
	}).Error)
	return certID
}

func poolListBech32(t *testing.T, poolKeyHash []byte) string {
	t.Helper()
	return lcommon.PoolId(lcommon.NewBlake2b224(poolKeyHash)).String()
}

// TestNodeAdapterPoolsListOrderingAndActiveSet is the empirical ordering
// test #3011 calls for: it seeds a mix of pools exercising every ordering
// and active-set edge case in one DB, then asserts the adapter's asc
// order matches the expected chain position exactly, desc is the exact
// reverse of asc, and excluded (effectively retired) pools never appear.
//
// Cases, by expected asc position:
//  0. reregisteredCancelled: first registered at slot 1, a retirement at
//     slot 6/epoch 0 looks effective, but a second registration at slot
//     400 cancels it (the pool is active because its LATEST registration
//     postdates the retirement). Its sort position is still keyed on its
//     FIRST registration (slot 1) -- proving ordering uses first
//     registration, not latest.
//  1. oldest: single registration at slot 10.
//  2. reregisteredMargin: first registered at slot 50, then re-registers
//     at slot 500 (simulating a margin/relay update). If ordering used
//     the latest registration, this pool would sort last; since it uses
//     the first, it sorts right after "oldest".
//  3. retiredFuture: registered at slot 90, retirement at slot 91
//     targeting epoch 5 (still in the future relative to epoch 0 at the
//     tip) -- pending retirement, still active.
//     4-6. Three pools registered in the same slot (100), disambiguated by
//     block_index then cert_index: (blk0,cert0) < (blk0,cert1) <
//     (blk1,cert0).
//
// retiredEffective (registered slot 5, retired slot 6/epoch 0, which has
// already started) must not appear at all.
func TestNodeAdapterPoolsListOrderingAndActiveSet(t *testing.T) {
	adapter, store, _ := newDBBackedAdapter(t)
	ensurePoolsListChainTip(t, store)

	reregisteredCancelledHash := fill32(0x01)[:28]
	oldestHash := fill32(0x02)[:28]
	reregisteredMarginHash := fill32(0x03)[:28]
	retiredFutureHash := fill32(0x04)[:28]
	ssBlk0Cert0Hash := fill32(0x05)[:28]
	ssBlk0Cert1Hash := fill32(0x06)[:28]
	ssBlk1Cert0Hash := fill32(0x07)[:28]
	retiredEffectiveHash := fill32(0x08)[:28]

	// reregisteredCancelled: slot 1 -> retirement slot 6 (epoch 0) ->
	// re-registration slot 400 cancels it.
	id := seedPoolListPool(t, store, reregisteredCancelledHash)
	seedPoolListRegistration(t, store, id, reregisteredCancelledHash, 1, 0)
	seedPoolListRetirement(t, store, id, reregisteredCancelledHash, 6, 0)
	seedPoolListRegistration(t, store, id, reregisteredCancelledHash, 400, 0)

	// oldest: slot 10.
	id = seedPoolListPool(t, store, oldestHash)
	seedPoolListRegistration(t, store, id, oldestHash, 10, 0)

	// reregisteredMargin: slot 50, then slot 500 (later param update).
	id = seedPoolListPool(t, store, reregisteredMarginHash)
	seedPoolListRegistration(t, store, id, reregisteredMarginHash, 50, 0)
	seedPoolListRegistration(t, store, id, reregisteredMarginHash, 500, 0)

	// retiredFuture: slot 90, retirement slot 91 targets epoch 5 (future).
	id = seedPoolListPool(t, store, retiredFutureHash)
	seedPoolListRegistration(t, store, id, retiredFutureHash, 90, 0)
	seedPoolListRetirement(t, store, id, retiredFutureHash, 91, 5)

	// Same-slot trio at slot 100: tx 1 (block_index 0) carries two certs
	// (cert_index 0 and 1); tx 2 (block_index 1) carries one cert.
	cert1 := seedPoolListCert(t, store, 1, 1, 100, 0, 0)
	cert2 := seedPoolListCert(t, store, 1, 2, 100, 0, 1)
	cert3 := seedPoolListCert(t, store, 2, 3, 100, 1, 0)

	id = seedPoolListPool(t, store, ssBlk0Cert0Hash)
	seedPoolListRegistration(t, store, id, ssBlk0Cert0Hash, 100, cert1)
	id = seedPoolListPool(t, store, ssBlk0Cert1Hash)
	seedPoolListRegistration(t, store, id, ssBlk0Cert1Hash, 100, cert2)
	id = seedPoolListPool(t, store, ssBlk1Cert0Hash)
	seedPoolListRegistration(t, store, id, ssBlk1Cert0Hash, 100, cert3)

	// retiredEffective: slot 5, retired at slot 6/epoch 0 -- epoch 0 has
	// already started at the tip's epoch (0), so this pool is excluded.
	id = seedPoolListPool(t, store, retiredEffectiveHash)
	seedPoolListRegistration(t, store, id, retiredEffectiveHash, 5, 0)
	seedPoolListRetirement(t, store, id, retiredEffectiveHash, 6, 0)

	wantAsc := []string{
		poolListBech32(t, reregisteredCancelledHash),
		poolListBech32(t, oldestHash),
		poolListBech32(t, reregisteredMarginHash),
		poolListBech32(t, retiredFutureHash),
		poolListBech32(t, ssBlk0Cert0Hash),
		poolListBech32(t, ssBlk0Cert1Hash),
		poolListBech32(t, ssBlk1Cert0Hash),
	}
	retiredEffectiveID := poolListBech32(t, retiredEffectiveHash)

	ascResult, ascTotal, err := adapter.PoolsList(PaginationParams{
		Count: 100, Page: 1, Order: PaginationOrderAsc,
	})
	require.NoError(t, err)
	assert.Equal(t, len(wantAsc), ascTotal)
	assert.Equal(t, wantAsc, ascResult)
	assert.NotContains(t, ascResult, retiredEffectiveID)

	descResult, descTotal, err := adapter.PoolsList(PaginationParams{
		Count: 100, Page: 1, Order: PaginationOrderDesc,
	})
	require.NoError(t, err)
	assert.Equal(t, len(wantAsc), descTotal)

	wantDesc := slices.Clone(wantAsc)
	slices.Reverse(wantDesc)
	assert.Equal(
		t, wantDesc, descResult,
		"desc must be the exact reverse of asc",
	)
}

// TestNodeAdapterPoolsListPagination covers count/page slicing over a
// controlled, fully ordered set of active pools.
func TestNodeAdapterPoolsListPagination(t *testing.T) {
	adapter, store, _ := newDBBackedAdapter(t)
	ensurePoolsListChainTip(t, store)

	var hashes [][]byte
	for i := range 5 {
		h := fill32(byte(0x10 + i))[:28]
		hashes = append(hashes, h)
		id := seedPoolListPool(t, store, h)
		seedPoolListRegistration(t, store, id, h, uint64(10*(i+1)), 0)
	}

	wantIDs := make([]string, len(hashes))
	for i, h := range hashes {
		wantIDs[i] = poolListBech32(t, h)
	}

	result, total, err := adapter.PoolsList(PaginationParams{
		Count: 2, Page: 2, Order: PaginationOrderAsc,
	})
	require.NoError(t, err)
	assert.Equal(t, 5, total)
	assert.Equal(t, wantIDs[2:4], result)

	// Past the last page: empty, non-nil, total unchanged.
	result, total, err = adapter.PoolsList(PaginationParams{
		Count: 2, Page: 10, Order: PaginationOrderAsc,
	})
	require.NoError(t, err)
	assert.Equal(t, 5, total)
	assert.NotNil(t, result)
	assert.Empty(t, result)
}

// TestNodeAdapterPoolsListEmpty covers the no-active-pools case: no error,
// an empty (non-nil) slice, and a zero total.
func TestNodeAdapterPoolsListEmpty(t *testing.T) {
	adapter, _, _ := newDBBackedAdapter(t)

	result, total, err := adapter.PoolsList(PaginationParams{
		Count: 100, Page: 1, Order: PaginationOrderAsc,
	})
	require.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.NotNil(t, result)
	assert.Empty(t, result)
}

// TestNodeAdapterPoolsListDatabaseFailure guards against a backing-store
// failure being silently swallowed: a broken query must surface as an
// error, not an incomplete success response.
func TestNodeAdapterPoolsListDatabaseFailure(t *testing.T) {
	adapter, store, _ := newDBBackedAdapter(t)
	ensurePoolsListChainTip(t, store)

	id := seedPoolListPool(t, store, fill32(0xaa)[:28])
	seedPoolListRegistration(t, store, id, fill32(0xaa)[:28], 0, 0)

	require.NoError(t, store.DB().Exec("DROP TABLE pool_registration").Error)

	_, _, err := adapter.PoolsList(PaginationParams{
		Count: 100, Page: 1, Order: PaginationOrderAsc,
	})
	require.Error(t, err)
}

// TestNodeAdapterPoolsListQueryPlan is the query-cost acceptance check:
// it captures the literal SQL GetActivePoolKeyHashesOrdered issues and
// runs EXPLAIN QUERY PLAN against it, logging the plan so the actual
// per-request cost is visible rather than assumed. This mirrors
// TestNodeAdapterPoolsExtendedMetadataSingleBatchedQuery's technique
// (adapter_pools_extended_db_test.go).
func TestNodeAdapterPoolsListQueryPlan(t *testing.T) {
	adapter, store, _ := newDBBackedAdapter(t)
	ensurePoolsListChainTip(t, store)

	id := seedPoolListPool(t, store, fill32(0xbb)[:28])
	seedPoolListRegistration(t, store, id, fill32(0xbb)[:28], 0, 0)

	var capturedSQL string
	var capturedVars []any
	const callbackName = "test:capture_pools_list_query"
	// db.Raw(...).Scan(...) resolves via *gorm.DB.Rows() internally,
	// which runs the Row callback chain ("gorm:row"), not the Query
	// chain ("gorm:query") that Find/First use -- confirmed against
	// gorm.io/gorm's Scan/Rows implementation, since registering on the
	// Query chain here never observed this call.
	require.NoError(t, store.ReadDB().Callback().Row().
		After("gorm:row").
		Register(callbackName, func(tx *gorm.DB) {
			sql := tx.Statement.SQL.String()
			if !strings.Contains(sql, "reg_ranked") {
				return
			}
			if capturedSQL == "" {
				capturedSQL = sql
				capturedVars = append([]any(nil), tx.Statement.Vars...)
			}
		}))
	t.Cleanup(func() {
		_ = store.ReadDB().Callback().Row().Remove(callbackName)
	})

	_, _, err := adapter.PoolsList(PaginationParams{
		Count: 100, Page: 1, Order: PaginationOrderAsc,
	})
	require.NoError(t, err)
	require.NotEmpty(t, capturedSQL, "did not capture the pools-list query")

	planRows, err := store.DB().
		Raw("EXPLAIN QUERY PLAN "+capturedSQL, capturedVars...).
		Rows()
	require.NoError(t, err)
	defer planRows.Close()

	var details []string
	for planRows.Next() {
		var id, parent, notUsed int
		var detail string
		require.NoError(t, planRows.Scan(&id, &parent, &notUsed, &detail))
		details = append(details, detail)
	}
	require.NotEmpty(t, details)
	t.Logf("pools-list query: %s | vars=%v", capturedSQL, capturedVars)
	t.Logf("EXPLAIN QUERY PLAN: %v", details)
}
