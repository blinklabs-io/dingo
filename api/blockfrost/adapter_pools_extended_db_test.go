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
	"encoding/hex"
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	sqliteplugin "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// ensureExtendedPoolChainTip makes pools registered at AddedSlot 0
// resolve as active. GetActivePoolKeyHashes resolves "active" against the
// chain tip (the `tip` table), and a freshly created LedgerState -- with
// or without Start() -- never writes one until a block has actually been
// synced; without a tip row it silently returns no pools rather than an
// error (see GetActivePoolKeyHashes, database/plugin/metadata/sqlite/pool.go).
// Start() against the devnet genesis (newDBBackedAdapterWithProtocolParams)
// already writes the epoch_id = 0 row (start_slot 0, length_in_slots 5 per
// the devnet genesis), so Attrs here only matters for newDBBackedAdapter,
// which starts with neither row.
func ensureExtendedPoolChainTip(
	t *testing.T,
	store *sqliteplugin.MetadataStoreSqlite,
) {
	t.Helper()
	require.NoError(t, store.DB().
		Where("id = ?", 1).
		Attrs(models.Tip{Slot: 0}).
		FirstOrCreate(&models.Tip{ID: 1}).Error)
	require.NoError(t, store.DB().
		Where("epoch_id = ?", 0).
		Attrs(models.Epoch{StartSlot: 0, LengthInSlots: 1000}).
		FirstOrCreate(&models.Epoch{EpochId: 0}).Error)
}

// seedExtendedPool creates one pool with a registration, an active-stake
// snapshot, a live-stake delegator, and observed block production,
// exercising the same real-DB path PoolsExtended reads from (metadata
// store -> pool -> registration -> account/utxo -> pool_opcert_sequence),
// rather than a hand-built PoolExtendedInfo. metadataURL/metadataHash may
// be empty/nil for a pool with no registered anchor. AddedSlot is 0 for
// every row so the pool is active at a freshly started ledger's tip
// (genesis, slot 0), matching GetActivePoolKeyHashesAtSlot's added_slot <=
// slot requirement.
func seedExtendedPool(
	t *testing.T,
	store *sqliteplugin.MetadataStoreSqlite,
	db *database.Database,
	seed byte,
	activeStake uint64,
	liveStake uint64,
	blocks int,
	metadataURL string,
	metadataHash []byte,
) (poolKeyHash []byte) {
	t.Helper()
	poolKeyHash = fill32(seed)[:28]
	pool := &models.Pool{
		PoolKeyHash: poolKeyHash,
		VrfKeyHash:  fill32(seed + 0x10),
		Pledge:      types.Uint64(1_000_000_000),
		Cost:        types.Uint64(340_000_000),
		Margin:      &types.Rat{Rat: big.NewRat(1, 20)},
		Registration: []models.PoolRegistration{
			{
				PoolKeyHash:  poolKeyHash,
				MetadataUrl:  metadataURL,
				MetadataHash: metadataHash,
				AddedSlot:    0,
			},
		},
	}
	require.NoError(t, store.DB().Create(pool).Error)

	if liveStake > 0 {
		stakingKey := fill32(seed + 0x20)[:28]
		require.NoError(t, store.DB().Create(&models.Account{
			StakingKey: stakingKey,
			Pool:       poolKeyHash,
			Active:     true,
			AddedSlot:  0,
		}).Error)
		require.NoError(t, store.DB().Create(&models.Utxo{
			TxId:       fill32(seed + 0x30),
			OutputIdx:  0,
			StakingKey: stakingKey,
			Amount:     types.Uint64(liveStake),
			AddedSlot:  0,
		}).Error)
	}

	require.NoError(t, store.DB().Create(&models.PoolStakeSnapshot{
		Epoch:        0,
		SnapshotType: "mark",
		PoolKeyHash:  poolKeyHash,
		TotalStake:   types.Uint64(activeStake),
	}).Error)

	pkh := lcommon.PoolKeyHash(poolKeyHash)
	for i := range blocks {
		require.NoError(
			t, db.UpdatePoolOpCertSequence(pkh, uint64(i+1), uint64(i), nil),
		)
	}

	return poolKeyHash
}

// seedExtendedPoolMetadataDoc inserts a cached off-chain metadata row for
// the given pool's anchor, matching the exact table
// GetOffchainMetadataBatch reads.
func seedExtendedPoolMetadataDoc(
	t *testing.T,
	store *sqliteplugin.MetadataStoreSqlite,
	url string,
	hash []byte,
	status string,
	content []byte,
	lastError string,
) {
	t.Helper()
	require.NoError(t, store.DB().Create(&models.OffchainMetadata{
		SourceType: models.OffchainMetadataSourcePool,
		URL:        url,
		Hash:       hash,
		Status:     status,
		Content:    content,
		LastError:  lastError,
	}).Error)
}

// TestNodeAdapterPoolsExtendedEmpty covers the no-active-pools case: no
// query beyond GetActivePoolKeyHashes should run, and the result is an
// empty (non-nil) slice.
func TestNodeAdapterPoolsExtendedEmpty(t *testing.T) {
	adapter, _, _ := newDBBackedAdapterWithProtocolParams(t)

	pools, err := adapter.PoolsExtended()
	require.NoError(t, err)
	assert.Empty(t, pools)
	assert.NotNil(t, pools)
}

// TestNodeAdapterPoolsExtendedFullResponse seeds three pools covering the
// three metadata states pool_list_extended's nullable metadata object can
// take (no anchor at all, anchor with a failed/invalid fetch, anchor with
// a successfully validated fetch), plus distinct active/live stake and
// block-production counts, then verifies every PoolExtendedInfo field the
// adapter computes from real DB state, including live_saturation computed
// from the real devnet nOpt (100).
func TestNodeAdapterPoolsExtendedFullResponse(t *testing.T) {
	adapter, store, db := newDBBackedAdapterWithProtocolParams(t)
	ensureExtendedPoolChainTip(t, store)

	// Pool A: no registered metadata anchor at all -> metadata: null.
	poolA := seedExtendedPool(t, store, db, 0x01, 1_000_000_000, 2_000_000_000, 3, "", nil)

	// Pool B: anchor present, cached fetch failed (hash mismatch) ->
	// metadata is a non-null object with url/hash and the error object,
	// but every off-chain field left null.
	urlB := "https://example.com/pool-b.json"
	hashB := fill32(0x42)
	poolB := seedExtendedPool(t, store, db, 0x02, 500_000_000, 100_000_000, 1, urlB, hashB)
	seedExtendedPoolMetadataDoc(
		t, store, urlB, hashB, models.OffchainMetadataStatusFailed,
		nil, models.OffchainFetchErrHashMismatch,
	)

	// Pool C: anchor present, cached fetch succeeded and validates ->
	// metadata is fully populated with no error.
	urlC := "https://example.com/pool-c.json"
	hashC := fill32(0x43)
	poolC := seedExtendedPool(t, store, db, 0x03, 250_000_000, 0, 0, urlC, hashC)
	seedExtendedPoolMetadataDoc(
		t, store, urlC, hashC, models.OffchainMetadataStatusFetched,
		[]byte(`{"name":"Pool C","description":"A pool.",`+
			`"ticker":"PLC","homepage":"https://example.com"}`),
		"",
	)

	pools, err := adapter.PoolsExtended()
	require.NoError(t, err)
	// >= 3, not == 3: Start() against the devnet genesis registers its own
	// genesis-staking pool (see newDBBackedAdapterWithProtocolParams's
	// doc comment on low-numbered synthetic rows), so the exact total
	// active-pool count is not just the three seeded here.
	require.GreaterOrEqual(t, len(pools), 3)

	byHex := make(map[string]PoolExtendedInfo, len(pools))
	for _, p := range pools {
		byHex[p.Hex] = p
	}

	const wantNOpt = 100 // config/cardano/devnet/shelley-genesis.json
	// live_saturation's denominator is the per-pool saturation threshold
	// totalCirculation / nOpt, where totalCirculation is
	// MaxLovelaceSupply minus Reserves. It is deliberately NOT
	// totalActiveStake -- see totalCirculation's doc comment in
	// adapter_pool_detail.go, and ledger/rewards, for why the two differ
	// (using active stake here overstated saturation by ~1.68x on
	// mainnet-shaped inputs).
	//
	// Both inputs come from the same devnet genesis this test starts a
	// real LedgerState against, rather than being restated from the
	// implementation: MaxLovelaceSupply is fixed by the genesis file and
	// Reserves is whatever the ledger actually persisted.
	const wantMaxLovelaceSupply = uint64(2_000_000_000_000) // config/cardano/devnet/shelley-genesis.json
	var networkState models.NetworkState
	require.NoError(
		t, store.DB().Order("slot DESC").First(&networkState).Error,
	)
	wantCirculation := wantMaxLovelaceSupply -
		uint64(networkState.Reserves)
	saturationThreshold := float64(wantCirculation) / float64(wantNOpt)

	// Pool A: no anchor.
	a, ok := byHex[hex.EncodeToString(poolA)]
	require.True(t, ok)
	assert.Equal(t, "1000000000", a.ActiveStake)
	assert.Equal(t, "2000000000", a.LiveStake)
	assert.Equal(t, uint64(3), a.BlocksMinted)
	assert.InDelta(
		t, float64(2_000_000_000)/saturationThreshold, a.LiveSaturation, 1e-6,
	)
	assert.Equal(t, "1000000000", a.DeclaredPledge)
	assert.Equal(t, "340000000", a.FixedCost)
	assert.InDelta(t, 0.05, a.MarginCost, 0.0001)
	assert.Nil(t, a.Metadata)

	// Pool B: failed fetch.
	b, ok := byHex[hex.EncodeToString(poolB)]
	require.True(t, ok)
	assert.Equal(t, uint64(1), b.BlocksMinted)
	require.NotNil(t, b.Metadata)
	require.NotNil(t, b.Metadata.URL)
	assert.Equal(t, urlB, *b.Metadata.URL)
	require.NotNil(t, b.Metadata.Hash)
	assert.Equal(t, hex.EncodeToString(hashB), *b.Metadata.Hash)
	require.NotNil(t, b.Metadata.Error)
	assert.Equal(t, "HASH_MISMATCH", b.Metadata.Error.Code)
	assert.Nil(t, b.Metadata.Name)
	assert.Nil(t, b.Metadata.Ticker)

	// Pool C: successful fetch.
	c, ok := byHex[hex.EncodeToString(poolC)]
	require.True(t, ok)
	assert.Equal(t, uint64(0), c.BlocksMinted)
	require.NotNil(t, c.Metadata)
	assert.Nil(t, c.Metadata.Error)
	require.NotNil(t, c.Metadata.Name)
	assert.Equal(t, "Pool C", *c.Metadata.Name)
	require.NotNil(t, c.Metadata.Ticker)
	assert.Equal(t, "PLC", *c.Metadata.Ticker)
	require.NotNil(t, c.Metadata.Description)
	assert.Equal(t, "A pool.", *c.Metadata.Description)
	require.NotNil(t, c.Metadata.Homepage)
	assert.Equal(t, "https://example.com", *c.Metadata.Homepage)
}

// TestNodeAdapterPoolsExtendedProtocolParamsUnavailable covers the case
// where protocol parameters have not been loaded yet: live_saturation is a
// required, non-nullable float in the OpenAPI schema, and 0.0 is itself a
// legitimate saturation value, so there is no schema-compatible
// placeholder for "unknown". PoolsExtended must fail the whole request
// rather than guess, matching PoolDetail's identical requirement.
func TestNodeAdapterPoolsExtendedProtocolParamsUnavailable(t *testing.T) {
	adapter, store, db := newDBBackedAdapter(t)
	ensureExtendedPoolChainTip(t, store)
	seedExtendedPool(t, store, db, 0xe1, 1_000_000, 1_000_000, 0, "", nil)

	_, err := adapter.PoolsExtended()
	require.ErrorContains(t, err, "protocol parameters")
}

// TestNodeAdapterPoolsExtendedDatabaseFailure guards against a
// backing-store failure being silently swallowed into an incomplete
// success response: a broken stake query must surface as an error.
func TestNodeAdapterPoolsExtendedDatabaseFailure(t *testing.T) {
	adapter, store, db := newDBBackedAdapterWithProtocolParams(t)
	ensureExtendedPoolChainTip(t, store)
	seedExtendedPool(t, store, db, 0xcc, 1_000_000, 1_000_000, 0, "", nil)

	require.NoError(t, store.DB().Exec("DROP TABLE account").Error)

	_, err := adapter.PoolsExtended()
	require.ErrorContains(t, err, "get live stake")
}

// TestNodeAdapterPoolsExtendedMetadataSingleBatchedQuery is the
// performance acceptance test for #2489's hard requirement: metadata for a
// whole page of pools must come from one batched query, not one query per
// pool. It seeds five pools each with their own distinct metadata anchor
// and cached document, counts how many queries actually touch
// offchain_metadata while serving PoolsExtended, and asserts it stays at
// exactly one regardless of pool count. It also captures that query's SQL
// and runs EXPLAIN QUERY PLAN against it, asserting the plan uses the
// (source_type, url, hash) unique index rather than a table scan.
func TestNodeAdapterPoolsExtendedMetadataSingleBatchedQuery(t *testing.T) {
	adapter, store, db := newDBBackedAdapterWithProtocolParams(t)
	ensureExtendedPoolChainTip(t, store)

	const poolCount = 5
	seededHex := make(map[string]bool, poolCount)
	for i := range poolCount {
		seed := byte(0x50 + i)
		url := "https://example.com/pool-" + hex.EncodeToString([]byte{seed}) + ".json"
		hash := fill32(seed)
		pkh := seedExtendedPool(t, store, db, seed, 1_000_000, 1_000_000, 0, url, hash)
		seededHex[hex.EncodeToString(pkh)] = true
		seedExtendedPoolMetadataDoc(
			t, store, url, hash, models.OffchainMetadataStatusFetched,
			[]byte(`{"name":"Pool","description":"d.",`+
				`"ticker":"ABC","homepage":"https://example.com"}`),
			"",
		)
	}

	queryCount := 0
	var capturedSQL string
	var capturedVars []any
	const callbackName = "test:count_offchain_metadata_queries"
	// Registered on ReadDB(), not DB(): resolveReadDB (used by every
	// PoolsExtended read, including GetOffchainMetadataBatch) routes to
	// the separate read-connection pool for a file-based sqlite database
	// (see ReadDB's doc comment, database/plugin/metadata/sqlite/database.go),
	// which is a distinct *gorm.DB from DB() and does not share its
	// registered callbacks.
	require.NoError(t, store.ReadDB().Callback().Query().
		After("gorm:query").
		Register(callbackName, func(tx *gorm.DB) {
			if tx.Statement.Table != "offchain_metadata" {
				return
			}
			queryCount++
			if capturedSQL == "" {
				capturedSQL = tx.Statement.SQL.String()
				capturedVars = append([]any(nil), tx.Statement.Vars...)
			}
		}))
	t.Cleanup(func() {
		_ = store.ReadDB().Callback().Query().Remove(callbackName)
	})

	pools, err := adapter.PoolsExtended()
	require.NoError(t, err)
	// >= poolCount, not == poolCount: Start() against the devnet genesis
	// registers its own genesis-staking pool alongside the poolCount
	// seeded here (see ensureExtendedPoolChainTip's doc comment); only
	// the seeded pools are asserted on below.
	require.GreaterOrEqual(t, len(pools), poolCount)
	seenSeeded := 0
	for _, p := range pools {
		if !seededHex[p.Hex] {
			continue
		}
		seenSeeded++
		require.NotNil(t, p.Metadata)
		assert.Equal(t, "Pool", *p.Metadata.Name)
	}
	assert.Equal(t, poolCount, seenSeeded)
	assert.Equal(
		t, 1, queryCount,
		"expected exactly one batched off-chain metadata query for the "+
			"whole page of %d pools, not one per pool", poolCount,
	)
	require.NotEmpty(t, capturedSQL, "did not capture the batched metadata query")

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
	t.Logf("offchain_metadata query: %s | vars=%v", capturedSQL, capturedVars)
	t.Logf("EXPLAIN QUERY PLAN: %v", details)
	for _, detail := range details {
		assert.NotContains(t, detail, "SCAN", "query plan: %v", details)
	}
	assert.Contains(
		t,
		strings.Join(details, " | "),
		"idx_offchain_metadata_source_url_hash",
		"query plan did not use the (source_type, url, hash) index: %v",
		details,
	)
}
