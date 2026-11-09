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

package koiosparity

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	txParityHash   = "aa11"
	txParityPrev   = "bb22"
	txParityRef    = "cc33"
	txParityColl   = "dd44"
	txParityAddr   = "addr_test1qexample"
	txParityPolicy = "65a9"
)

// koiosTx is a valid transaction with one of each association.
func koiosTx() KoiosTxInfoItem {
	return KoiosTxInfoItem{
		TxHash: txParityHash,
		Inputs: []KoiosTxInfoUtxoRef{
			{TxHash: txParityPrev, TxIndex: 1},
		},
		CollateralInputs: []KoiosTxInfoUtxoRef{
			{TxHash: txParityColl, TxIndex: 0},
		},
		ReferenceInputs: []KoiosTxInfoUtxoRef{
			{TxHash: txParityRef, TxIndex: 2},
		},
		Outputs: []KoiosTxInfoOutput{{
			TxHash:  txParityHash,
			TxIndex: 0,
			Value:   "5000000",
			AssetList: KoiosTxInfoAssetList{
				{PolicyID: txParityPolicy, AssetName: "74", Quantity: "7"},
			},
			PaymentAddr: struct {
				Bech32 string `json:"bech32"`
			}{Bech32: txParityAddr},
		}},
	}
}

// dingoTx is the stored form of koiosTx.
func dingoTx() DingoTransaction {
	return DingoTransaction{
		Hash:            txParityHash,
		Valid:           true,
		Inputs:          []string{txParityPrev + "#1"},
		Collateral:      []string{txParityColl + "#0"},
		ReferenceInputs: []string{txParityRef + "#2"},
		Outputs: []string{
			"0|" + txParityAddr + "|5000000|" + txParityPolicy + ".74=7",
		},
	}
}

func compareOne(dt DingoTransaction, kt KoiosTxInfoItem) []CheckMismatch {
	return CompareEpochTransactions(
		"preview",
		10,
		[]DingoTransaction{dt},
		map[string]KoiosTxInfoItem{kt.TxHash: kt},
		time.Now(),
	)
}

func TestCompareEpochTransactionsMatchingIsClean(t *testing.T) {
	t.Parallel()
	require.Empty(t, compareOne(dingoTx(), koiosTx()))
}

func TestCompareEpochTransactionsReportsEachDifferingSet(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		mutate func(*DingoTransaction)
		field  string
	}{
		{
			"inputs",
			func(d *DingoTransaction) { d.Inputs = []string{txParityPrev + "#9"} },
			"tx:" + txParityHash + ":inputs",
		},
		{
			"collateral",
			func(d *DingoTransaction) { d.Collateral = nil },
			"tx:" + txParityHash + ":collateral_inputs",
		},
		{
			"reference inputs",
			func(d *DingoTransaction) { d.ReferenceInputs = nil },
			"tx:" + txParityHash + ":reference_inputs",
		},
		{
			"output lovelace",
			func(d *DingoTransaction) {
				d.Outputs = []string{
					"0|" + txParityAddr + "|4999999|" + txParityPolicy + ".74=7",
				}
			},
			"tx:" + txParityHash + ":outputs",
		},
		{
			"output asset amount",
			func(d *DingoTransaction) {
				d.Outputs = []string{
					"0|" + txParityAddr + "|5000000|" + txParityPolicy + ".74=8",
				}
			},
			"tx:" + txParityHash + ":outputs",
		},
		{
			"extra output",
			func(d *DingoTransaction) {
				d.Outputs = append(d.Outputs, "1|"+txParityAddr+"|1")
			},
			"tx:" + txParityHash + ":outputs",
		},
		{
			"validity",
			func(d *DingoTransaction) { d.Valid = false },
			"tx:" + txParityHash + ":valid",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d := dingoTx()
			tc.mutate(&d)
			got := compareOne(d, koiosTx())
			require.Len(t, got, 1)
			assert.Equal(t, tc.field, got[0].Field)
			assert.Equal(t, CategoryValueMismatch, got[0].Category)
			assert.Equal(t, StatusFail, DetermineStatus(got))
			assert.Equal(t, uint64(10), got[0].Epoch)
			assert.Equal(t, "preview", got[0].Network)
		})
	}
}

func TestCompareEpochTransactionsPhase2InvalidUsesConsumedAndProduced(
	t *testing.T,
) {
	t.Parallel()

	valid := false
	kt := koiosTx()
	kt.PlutusContracts = []KoiosTxInfoPlutusContract{{ValidContract: &valid}}
	kt.CollateralOutput = &KoiosTxInfoOutput{
		TxHash:  txParityHash,
		TxIndex: 1,
		Value:   "3000000",
		PaymentAddr: struct {
			Bech32 string `json:"bech32"`
		}{Bech32: txParityAddr},
	}
	// The ledger spent the collateral and created the collateral return;
	// the body's inputs and outputs were never applied.
	dt := DingoTransaction{
		Hash:            txParityHash,
		Valid:           false,
		Inputs:          []string{txParityColl + "#0"},
		Collateral:      []string{txParityColl + "#0"},
		ReferenceInputs: []string{txParityRef + "#2"},
		Outputs:         []string{"1|" + txParityAddr + "|3000000"},
	}
	require.Empty(t, compareOne(dt, kt))

	// A stored body output instead of the collateral return is a difference.
	dt.Outputs = []string{
		"0|" + txParityAddr + "|5000000|" + txParityPolicy + ".74=7",
	}
	got := compareOne(dt, kt)
	require.Len(t, got, 1)
	assert.Equal(t, "tx:"+txParityHash+":outputs", got[0].Field)
}

func TestCompareEpochTransactionsMissingReferenceIsError(t *testing.T) {
	t.Parallel()

	got := CompareEpochTransactions(
		"preview", 10,
		[]DingoTransaction{dingoTx()},
		map[string]KoiosTxInfoItem{},
		time.Now(),
	)
	require.Len(t, got, 1)
	assert.Equal(t, "tx_info:"+txParityHash, got[0].Field)
	assert.Equal(t, CategoryDBMissing, got[0].Category)
	assert.Equal(t, StatusError, DetermineStatus(got))
}

func TestDingoTransactionFromModel(t *testing.T) {
	t.Parallel()

	prevHash := []byte{0xbb, 0x22}
	refHash := []byte{0xcc, 0x33}
	collHash := []byte{0xdd, 0x44}
	txHash := []byte{0xaa, 0x11}
	tx := &models.Transaction{
		Hash:  txHash,
		Valid: true,
		Inputs: []models.Utxo{
			{TxId: prevHash, OutputIdx: 1},
		},
		Collateral: []models.Utxo{
			{TxId: collHash, OutputIdx: 0},
		},
		ReferenceInputs: []models.Utxo{
			{TxId: refHash, OutputIdx: 2},
		},
		Outputs: []models.Utxo{{
			TxId:      txHash,
			OutputIdx: 0,
			Address:   []byte(txParityAddr),
			Amount:    types.Uint64(5_000_000),
			Assets: []models.Asset{{
				PolicyId: []byte{0x65, 0xa9},
				Name:     []byte{0x74},
				Amount:   types.Uint64(7),
			}},
		}},
	}
	got := dingoTransactionFromModel(tx)
	require.Equal(t, dingoTx(), got)
	require.Empty(t, compareOne(got, koiosTx()))

	// A collateral return is a produced output at its own index.
	tx.CollateralReturn = &models.Utxo{
		TxId:      txHash,
		OutputIdx: 1,
		Address:   []byte(txParityAddr),
		Amount:    types.Uint64(3_000_000),
	}
	got = dingoTransactionFromModel(tx)
	assert.Contains(
		t, got.Outputs, "1|"+txParityAddr+"|3000000",
	)
}

func TestDatabaseSourceTransactionSampleSkippedInCoreMode(t *testing.T) {
	t.Parallel()

	db := newTestDatabaseSourceDB(t)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)
	sample, err := source.GetEpochTransactionSample(
		context.Background(), 3, EpochTransactionSampleSize,
	)
	require.NoError(t, err)
	require.Empty(t, sample, "core storage does not retain spent UTxOs")
}

// txSampleStub makes a DatabaseSource report a fixed transaction sample.
type txSampleStub struct {
	RewardParitySource
	sample []DingoTransaction
}

func (s *txSampleStub) GetEpochTransactionSample(
	context.Context, uint64, int,
) ([]DingoTransaction, error) {
	return s.sample, nil
}

func txMismatchFields(t *testing.T, ms []CheckMismatch) []string {
	t.Helper()
	var out []string
	for _, m := range ms {
		if strings.HasPrefix(m.Field, "tx:") ||
			strings.HasPrefix(m.Field, "tx_info:") {
			out = append(out, m.Field)
		}
	}
	return out
}

// TestCheckEpochComparesTransactionSample drives the production entry point:
// a stored transaction that differs from Koios must surface in the epoch's
// result and in the persisted mismatch records.
func TestCheckEpochComparesTransactionSample(t *testing.T) {
	t.Parallel()

	const network = "preview"
	const epoch = uint64(3)

	db := newTestDatabaseSourceDB(t)
	base, err := NewDatabaseSource(db)
	require.NoError(t, err)

	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	closedLongAgo := time.Now().Add(-24 * time.Hour * 400).UTC()
	require.NoError(t, cache.CommitEpochData(KoiosEpochInfo{
		Network:      network,
		Epoch:        epoch,
		ActiveStake:  "1",
		EpochEndTime: closedLongAgo,
		FetchedAt:    closedLongAgo,
	}, nil, nil))
	require.NoError(t, cache.UpsertTxInfos(
		network, []KoiosTxInfoItem{koiosTx()}, closedLongAgo,
	))

	bad := dingoTx()
	bad.ReferenceInputs = nil
	source := &txSampleStub{
		RewardParitySource: base,
		sample:             []DingoTransaction{bad},
	}

	result, err := CheckEpoch(
		context.Background(), cache, source, network, epoch, 0, false,
		slog.New(slog.DiscardHandler),
	)
	require.NoError(t, err)
	want := []string{"tx:" + txParityHash + ":reference_inputs"}
	require.Equal(t, want, txMismatchFields(t, result.Mismatches))

	stored, err := cache.GetMismatches(network, epoch, "")
	require.NoError(t, err)
	require.Equal(t, want, txMismatchFields(t, stored))

	// A matching sample adds nothing.
	source.sample = []DingoTransaction{dingoTx()}
	result, err = CheckEpoch(
		context.Background(), cache, source, network, epoch, 0, false,
		slog.New(slog.DiscardHandler),
	)
	require.NoError(t, err)
	require.Empty(t, txMismatchFields(t, result.Mismatches))
}

func TestCacheTxInfoRoundTripsReferenceInputs(t *testing.T) {
	t.Parallel()

	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck
	require.NoError(t, cache.UpsertTxInfos(
		"preview", []KoiosTxInfoItem{koiosTx()}, time.Now(),
	))
	got, err := cache.GetTxInfos("preview", []string{txParityHash})
	require.NoError(t, err)
	require.Equal(
		t,
		koiosTx().ReferenceInputs,
		got[txParityHash].ReferenceInputs,
	)
}

// TestObserverFetchesOnlyUncachedSampledTransactions pins the fetch half:
// the sampled hashes reach Koios once, land in the cache, and a repeat call
// asks for nothing.
func TestObserverFetchesOnlyUncachedSampledTransactions(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	var requested atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			var body struct {
				TxHashes []string `json:"_tx_hashes"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			requested.Store(body.TxHashes)
			items := make([]KoiosTxInfoItem, 0, len(body.TxHashes))
			for _, h := range body.TxHashes {
				item := koiosTx()
				item.TxHash = h
				items = append(items, item)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(items)
		}))
	defer srv.Close()

	base, err := NewDatabaseSource(newTestDatabaseSourceDB(t))
	require.NoError(t, err)
	second := dingoTx()
	second.Hash = "ee55"
	o, err := NewObserver(ObserverConfig{
		Network:   "preview",
		CachePath: filepath.Join(t.TempDir(), "cache.db"),
		Source: &txSampleStub{
			RewardParitySource: base,
			sample:             []DingoTransaction{dingoTx(), second},
		},
		BaseURL:               srv.URL,
		AllowInsecureHTTP:     true,
		AllowPrivateAddresses: true,
		Logger:                slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = o.Stop(context.Background()) })

	// Cached ahead of time: must not be requested again.
	require.NoError(t, o.cache.UpsertTxInfos(
		"preview", []KoiosTxInfoItem{koiosTx()}, time.Now(),
	))

	require.NoError(t, o.fetchTransactionsIfNeeded(context.Background(), 3))
	require.Equal(t, int32(1), requests.Load())
	require.Equal(t, []string{"ee55"}, requested.Load())
	got, err := o.cache.GetTxInfos("preview", []string{txParityHash, "ee55"})
	require.NoError(t, err)
	require.Len(t, got, 2)

	require.NoError(t, o.fetchTransactionsIfNeeded(context.Background(), 3))
	require.Equal(t, int32(1), requests.Load(), "everything is cached now")
}

// TestObserverFetchIfNeededCachesTransactionSample drives fetchIfNeeded, the
// entry both observer queues use, so the transaction fetch cannot be dropped
// from it while fetchTransactionsIfNeeded still passes on its own.
func TestObserverFetchIfNeededCachesTransactionSample(t *testing.T) {
	t.Parallel()

	epochSrv := newFakeKoiosServer(t, map[uint64]*fakeEpochRef{
		5: {activeStake: "1000000", treasury: "10", reserves: "20", fees: "30"},
	})
	target, err := url.Parse(epochSrv.URL)
	require.NoError(t, err)
	proxy := httputil.NewSingleHostReverseProxy(target)
	front := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/tx_info" {
				proxy.ServeHTTP(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]KoiosTxInfoItem{koiosTx()})
		}))
	defer front.Close()

	base, err := NewDatabaseSource(newTestDatabaseSourceDB(t))
	require.NoError(t, err)
	o, err := NewObserver(ObserverConfig{
		Network:   "preview",
		CachePath: filepath.Join(t.TempDir(), "cache.db"),
		Source: &txSampleStub{
			RewardParitySource: base,
			sample:             []DingoTransaction{dingoTx()},
		},
		BaseURL:               front.URL,
		AllowInsecureHTTP:     true,
		AllowPrivateAddresses: true,
		Logger:                slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = o.Stop(context.Background()) })

	require.NoError(t, o.fetchIfNeeded(context.Background(), 5))
	got, err := o.cache.GetTxInfos("preview", []string{txParityHash})
	require.NoError(t, err)
	require.Contains(t, got, txParityHash)
}

// TestCacheTxInfoIgnoresRowsWithoutReferenceInputs: a row stored before
// ReferenceInputs existed decodes with none, which would read as a
// transaction that references nothing, so it must be re-fetched.
// TestObserverRetriesTransientEmptyTxInfo pins the bounded retry around
// /tx_info: the public host intermittently answers a valid request with 200
// and an empty array, and one such answer must not become an ERROR epoch.
func TestObserverRetriesTransientEmptyTxInfo(t *testing.T) {
	t.Parallel()

	const attempts = 3
	cases := []struct {
		name       string
		emptyFirst int32
		wantCached bool
		wantCalls  int32
	}{
		{"recovers after empty answers", attempts - 1, true, attempts},
		{"gives up after the attempt bound", attempts + 1, false, attempts},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(
				func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					if calls.Add(1) <= tc.emptyFirst {
						_ = json.NewEncoder(w).Encode([]KoiosTxInfoItem{})
						return
					}
					_ = json.NewEncoder(w).Encode([]KoiosTxInfoItem{koiosTx()})
				}))
			defer srv.Close()

			base, err := NewDatabaseSource(newTestDatabaseSourceDB(t))
			require.NoError(t, err)
			o, err := NewObserver(ObserverConfig{
				Network:   "preview",
				CachePath: filepath.Join(t.TempDir(), "cache.db"),
				Source: &txSampleStub{
					RewardParitySource: base,
					sample:             []DingoTransaction{dingoTx()},
				},
				BaseURL:               srv.URL,
				AllowInsecureHTTP:     true,
				AllowPrivateAddresses: true,
				FetchRetryAttempts:    attempts,
				FetchRetryDelay:       time.Millisecond,
				Logger:                slog.New(slog.DiscardHandler),
			})
			require.NoError(t, err)
			t.Cleanup(func() { _ = o.Stop(context.Background()) })

			require.NoError(
				t, o.fetchTransactionsIfNeeded(context.Background(), 5),
			)
			got, err := o.cache.GetTxInfos("preview", []string{txParityHash})
			require.NoError(t, err)
			if tc.wantCached {
				require.Contains(t, got, txParityHash)
			} else {
				require.Empty(t, got)
			}
			require.Equal(t, tc.wantCalls, calls.Load())
		})
	}
}

func TestCacheTxInfoIgnoresRowsWithoutReferenceInputs(t *testing.T) {
	t.Parallel()

	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck
	require.NoError(t, cache.UpsertTxInfos(
		"preview", []KoiosTxInfoItem{koiosTx()}, time.Now(),
	))
	_, err = cache.db.Exec(
		`UPDATE koios_tx_info SET payload_version = 2`,
	)
	require.NoError(t, err)
	got, err := cache.GetTxInfos("preview", []string{txParityHash})
	require.NoError(t, err)
	require.Empty(t, got)
}

// TestDatabaseSourceTransactionSampleAPIModeWalksEpochBlocks runs the real
// sampling path on an API-mode database: it finds the epoch, walks its slot
// range, and reports no transactions for blocks that carry none.
func TestDatabaseSourceTransactionSampleAPIModeWalksEpochBlocks(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     t.TempDir(),
		StorageMode: types.StorageModeAPI,
	})
	require.NoError(t, err)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	raw := sourceSQLDB(t, db)
	_, err = raw.db.Exec(
		`INSERT INTO epoch (epoch_id, start_slot, era_id, slot_length, length_in_slots)
		VALUES (3, 1000, 5, 1000, 500)`,
	)
	require.NoError(t, err)
	require.NoError(t, db.BlockCreate(models.Block{
		Slot: 1100, Hash: bytes.Repeat([]byte{0x01}, 32),
		Cbor: []byte{0x82, 0x01}, Type: 1,
	}, nil))

	sample, err := source.GetEpochTransactionSample(
		context.Background(), 3, EpochTransactionSampleSize,
	)
	require.NoError(t, err)
	require.Empty(t, sample)

	// An epoch with no row is nothing to sample, not an error.
	sample, err = source.GetEpochTransactionSample(
		context.Background(), 99, EpochTransactionSampleSize,
	)
	require.NoError(t, err)
	require.Empty(t, sample)
}

const (
	txParityBlockSlot    = 127444628
	txParityBlockHash    = "ec0a1469c3ed4d747dd13fcbb59232e1b2f7cd32b16576730dd1002796eda3be"
	txParityScriptTxHash = "ff740cdc5650212514eb4ad26ae136093b5933ada512050f3be1656816c40217"
	txParityScriptTxIdx  = 5
)

// TestDatabaseSourceTransactionSampleMatchesRecordedKoios stores a real
// Preview block through the production write path and compares what the
// sample reads back against the recorded /tx_info answer for the same
// transactions. It covers what hand-built models cannot: the spend, collateral
// and reference associations the writer records, and the address, lovelace and
// asset values read back from the block's CBOR.
func TestDatabaseSourceTransactionSampleMatchesRecordedKoios(t *testing.T) {
	t.Parallel()

	rawHex, err := os.ReadFile("testdata/tx_parity_preview_block.hex")
	require.NoError(t, err)
	blockCbor, err := hex.DecodeString(strings.TrimSpace(string(rawHex)))
	require.NoError(t, err)
	var recorded []KoiosTxInfoItem
	rawJSON, err := os.ReadFile("testdata/tx_parity_preview_koios.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(rawJSON, &recorded))
	koios := make(map[string]KoiosTxInfoItem, len(recorded))
	for _, item := range recorded {
		koios[item.TxHash] = item
	}

	cases := []struct {
		name      string
		invalidTx bool
	}{
		{"valid block", false},
		{"phase-2-invalid script transaction", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			stored := blockCbor
			want := koios
			if tc.invalidTx {
				stored, want = markTxParityScriptTxInvalid(
					t, blockCbor, koios,
				)
			}
			sample := sampleStoredTxParityBlock(t, stored)
			require.Len(t, sample, len(recorded))
			require.Empty(t, CompareEpochTransactions(
				"preview", 3, sample, want, time.Now(),
			))
			if tc.invalidTx {
				for _, tx := range sample {
					if tx.Hash != txParityScriptTxHash {
						continue
					}
					assert.False(t, tx.Valid)
					assert.Equal(t, tx.Collateral, tx.Inputs)
					assert.Len(t, tx.Outputs, 1)
					assert.True(t, strings.HasPrefix(tx.Outputs[0], "3|"),
						"collateral return sits at index len(outputs)")
				}
			}
		})
	}
}

// markTxParityScriptTxInvalid lists the script transaction in the block's
// invalid-transaction set, the block's final element, and flips the Koios
// verdict to match, so both sides describe a phase-2 failure of a transaction
// that carries a collateral return.
func markTxParityScriptTxInvalid(
	t *testing.T,
	blockCbor []byte,
	koios map[string]KoiosTxInfoItem,
) ([]byte, map[string]KoiosTxInfoItem) {
	t.Helper()
	require.Equal(t, byte(0x80), blockCbor[len(blockCbor)-1],
		"fixture block must have an empty invalid-transaction set")
	stored := append(
		append([]byte{}, blockCbor[:len(blockCbor)-1]...),
		0x81, txParityScriptTxIdx,
	)
	want := maps.Clone(koios)
	invalid := false
	item := want[txParityScriptTxHash]
	item.PlutusContracts = []KoiosTxInfoPlutusContract{
		{ValidContract: &invalid},
	}
	want[txParityScriptTxHash] = item
	return stored, want
}

// sampleStoredTxParityBlock writes the block to an API-mode database and
// returns the epoch sample. The block's external inputs are seeded as bare
// utxo rows, because the association writers only link rows that exist.
func sampleStoredTxParityBlock(
	t *testing.T,
	blockCbor []byte,
) []DingoTransaction {
	t.Helper()
	ctx := context.Background()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     t.TempDir(),
		StorageMode: types.StorageModeAPI,
	})
	require.NoError(t, err)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	// The tampered invalid-transaction set no longer matches the header's
	// body hash, which is not what this test is about.
	block, err := ledger.NewBlockFromCbor(
		ledger.BlockTypeConway,
		blockCbor,
		lcommon.VerifyConfig{SkipBodyHashValidation: true},
	)
	require.NoError(t, err)
	hash, err := hex.DecodeString(txParityBlockHash)
	require.NoError(t, err)

	raw := sourceSQLDB(t, db)
	_, err = raw.db.Exec(
		`INSERT INTO epoch (epoch_id, start_slot, era_id, slot_length, length_in_slots)
		VALUES (3, 127000000, 6, 1000, 1000000)`,
	)
	require.NoError(t, err)
	require.NoError(t, db.BlockCreate(models.Block{
		Slot: txParityBlockSlot, Hash: hash, Cbor: blockCbor,
		Type:     ledger.BlockTypeConway,
		Number:   1,
		PrevHash: bytes.Repeat([]byte{0x09}, 32),
	}, nil))

	txs := block.Transactions()
	known := make(map[string]struct{})
	for _, tx := range txs {
		var inputs []lcommon.TransactionInput
		inputs = append(inputs, tx.Inputs()...)
		inputs = append(inputs, tx.Collateral()...)
		inputs = append(inputs, tx.ReferenceInputs()...)
		for _, in := range inputs {
			key := in.String()
			if _, ok := known[key]; ok {
				continue
			}
			known[key] = struct{}{}
			_, err = raw.db.Exec(
				`INSERT INTO utxo (tx_id, output_idx, amount, added_slot,
					deleted_slot, payment_script, credential_tag)
				VALUES (?, ?, '1', 1, 0, 0, 0)`,
				in.Id().Bytes(), in.Index(),
			)
			require.NoError(t, err)
		}
		for _, produced := range tx.Produced() {
			known[produced.Id.String()] = struct{}{}
		}
	}

	offsets, err := database.NewBlockIndexer(
		txParityBlockSlot, hash,
	).ComputeOffsets(blockCbor, block)
	require.NoError(t, err)
	point := ocommon.NewPoint(txParityBlockSlot, hash)
	acc := db.NewValidatedBlockBatchAccumulator()
	require.NotNil(t, acc)
	txn := db.Transaction(ctx, true)
	for i, tx := range txs {
		require.NoError(t, db.SetTransactionBatched(
			ctx, tx, point, uint32(i), 0, nil, nil, offsets, acc, txn, 10,
		))
	}
	require.NoError(t, db.FlushBatch(acc, txn))
	require.NoError(t, txn.Commit())

	sample, err := source.GetEpochTransactionSample(
		ctx, 3, EpochTransactionSampleSize,
	)
	require.NoError(t, err)
	return sample
}
