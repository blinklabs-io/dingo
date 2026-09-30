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

package ledger

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/sha3"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"maps"
	"math"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/dingo/ledger/rewards"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/blinklabs-io/dingo/ledgerstate"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/consensus"
	byronconsensus "github.com/blinklabs-io/gouroboros/consensus/byron"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
	omockfixtures "github.com/blinklabs-io/ouroboros-mock/fixtures"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newActiveSlotCoeffLedgerState builds a minimal LedgerState whose Shelley
// genesis carries the given activeSlotsCoeff JSON literal.
func newActiveSlotCoeffLedgerState(
	t *testing.T,
	coeffJSON string,
) *LedgerState {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": `+coeffJSON+`,
		"securityParam": 432,
		"systemStart": "2022-10-25T00:00:00Z"
	}`)))
	return &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
}

// TestActiveSlotCoeffRatIsExactGenesisRational pins that the leader-check
// coefficient accessor returns the genesis value exactly, and that the float64
// accessor does not.
//
// A Shelley genesis "activeSlotsCoeff": 0.05 decodes to exactly 1/20.
// ActiveSlotCoeff() divides the numerator and denominator as float64, and the
// nearest binary64 value to 0.05 is strictly GREATER than 1/20, so a threshold
// derived from it is strictly larger than the reference node's — a node using it
// can only over-claim leader slots, never miss any. That is the one-sided
// signature reported in dingo #2798, so the direction is pinned here even though
// the magnitude (~5.6e-17 relative) is far too small to account for the three
// phantom slots per epoch reported there.
func TestActiveSlotCoeffRatIsExactGenesisRational(t *testing.T) {
	t.Parallel()

	ls := newActiveSlotCoeffLedgerState(t, "0.05")

	exact := ls.ActiveSlotCoeffRat()
	require.NotNil(t, exact)
	require.Equal(t, 0, exact.Cmp(big.NewRat(1, 20)),
		"ActiveSlotCoeffRat must return the genesis value exactly")

	approx := new(big.Rat).SetFloat64(ls.ActiveSlotCoeff())
	require.NotNil(t, approx)
	require.Equal(t, 1, approx.Cmp(exact),
		"the float64 accessor must be strictly greater than 1/20, which is "+
			"why the leader check must not use it")
}

// TestActiveSlotCoeffRatReturnsCopy proves callers cannot mutate shared genesis
// state through the returned pointer. big.Rat is mutable, and the leader
// schedule hands this value to the consensus package.
func TestActiveSlotCoeffRatReturnsCopy(t *testing.T) {
	t.Parallel()

	ls := newActiveSlotCoeffLedgerState(t, "0.05")

	first := ls.ActiveSlotCoeffRat()
	require.NotNil(t, first)
	first.SetInt64(7)

	second := ls.ActiveSlotCoeffRat()
	require.NotNil(t, second)
	require.Equal(t, 0, second.Cmp(big.NewRat(1, 20)),
		"mutating a returned coefficient must not corrupt the genesis value")
}

// TestActiveSlotCoeffRatWithoutGenesis returns nil rather than a degenerate
// zero value, so callers can fall back explicitly.
func TestActiveSlotCoeffRatWithoutGenesis(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	require.Nil(t, ls.ActiveSlotCoeffRat())
}

var benchmarkDiscardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

const storageModeBenchmarkStartSlot = 10000

const storageModeBenchmarkSkippedInputHash = "e3ca57e8f323265742a8f4e79ff9af884c9ff8719bd4f7788adaea4c33ba07b6"
const storageModeBenchmarkSkippedInputIndex = 3
const blockProcessingBenchmarkFixtureBlockCount = 4096

// Helper functions for benchmark seeding

// openImmutableTestDB opens the immutable test database
func openImmutableTestDB(b *testing.B) *immutable.ImmutableDb {
	b.Helper()
	immDb, err := immutable.New("../database/immutable/testdata")
	if err != nil {
		b.Fatal(err)
	}
	return immDb
}

// fixturePointsFromSlots resolves each requested slot to the exact point of
// the first fixture block at or after it, discarding repeats.
//
// ImmutableDb.GetBlock compares the stored hash against the point's hash, so
// a point carrying no hash matches nothing however real its slot is. The
// fixture's blocks are also 20 slots apart, so most slots hold no block of
// their own. BlockIterator seeks on slot alone and ignores the hash, which
// makes it the only way to turn a slot into a point GetBlock will accept.
func fixturePointsFromSlots(
	b *testing.B,
	immDb *immutable.ImmutableDb,
	slots []uint64,
) []ocommon.Point {
	b.Helper()
	points := make([]ocommon.Point, 0, len(slots))
	seen := make(map[string]struct{}, len(slots))
	for _, slot := range slots {
		block := firstFixtureBlockFrom(b, immDb, slot)
		if block == nil {
			continue
		}
		if _, dup := seen[string(block.Hash)]; dup {
			continue
		}
		seen[string(block.Hash)] = struct{}{}
		points = append(points, ocommon.NewPoint(block.Slot, block.Hash))
	}
	return points
}

// fixturePointsFrom returns the points of up to count consecutive fixture
// blocks at or after startSlot.
func fixturePointsFrom(
	b *testing.B,
	immDb *immutable.ImmutableDb,
	startSlot uint64,
	count int,
) []ocommon.Point {
	b.Helper()
	iter, err := immDb.BlocksFromPoint(ocommon.NewPoint(startSlot, nil))
	if err != nil {
		b.Fatalf("open fixture iterator at slot %d: %v", startSlot, err)
	}
	defer func() { _ = iter.Close() }()
	points := make([]ocommon.Point, 0, count)
	for len(points) < count {
		block, err := iter.Next()
		if err != nil {
			b.Fatalf("read fixture block after slot %d: %v", startSlot, err)
		}
		if block == nil {
			break
		}
		points = append(points, ocommon.NewPoint(block.Slot, block.Hash))
	}
	if len(points) == 0 {
		b.Fatalf("fixture holds no block at or after slot %d", startSlot)
	}
	return points
}

// firstFixtureBlockFrom returns the first fixture block at or after slot, or
// nil when the fixture ends before it.
func firstFixtureBlockFrom(
	b *testing.B,
	immDb *immutable.ImmutableDb,
	slot uint64,
) *immutable.Block {
	b.Helper()
	iter, err := immDb.BlocksFromPoint(ocommon.NewPoint(slot, nil))
	if err != nil {
		b.Fatalf("open fixture iterator at slot %d: %v", slot, err)
	}
	defer func() { _ = iter.Close() }()
	block, err := iter.Next()
	if err != nil {
		b.Fatalf("read fixture block at or after slot %d: %v", slot, err)
	}
	return block
}

// seedBlocksAtPoints stores each fixture block in db, looking it up by its
// own point, and returns the number stored. Block indexes run sequentially
// from database.BlockInitialIndex so a benchmark can query the seeded blocks
// by index.
//
// It fails the benchmark when nothing was stored. A query benchmark that
// times an empty database measures the miss path its NoData twin already
// covers, and publishes that timing as though the fixture had been read.
//
// Seeding writes to the block store only. Accounts, pools, DReps, datums,
// protocol parameters, nonces and registrations are produced by applying a
// block, not by storing one, so a RealData benchmark querying one of those
// tables still measures a miss however many blocks are seeded.
func seedBlocksAtPoints(
	b *testing.B,
	db *database.Database,
	immDb *immutable.ImmutableDb,
	points []ocommon.Point,
) int {
	b.Helper()
	seeded := 0
	for _, point := range points {
		block, err := immDb.GetBlock(point)
		if err != nil {
			b.Fatalf("read fixture block at slot %d: %v", point.Slot, err)
		}
		if block == nil {
			b.Fatalf(
				"fixture holds no block at slot %d with hash %x",
				point.Slot,
				point.Hash,
			)
		}
		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			b.Fatalf("decode fixture block at slot %d: %v", block.Slot, err)
		}
		blockModel := models.Block{
			ID:       database.BlockInitialIndex + uint64(seeded),
			Slot:     block.Slot,
			Hash:     block.Hash,
			Number:   0,
			Type:     uint(ledgerBlock.Type()),
			PrevHash: ledgerBlock.PrevHash().Bytes(),
			Cbor:     ledgerBlock.Cbor(),
		}
		if err := db.BlockCreate(blockModel, nil); err != nil {
			b.Fatalf("store fixture block at slot %d: %v", block.Slot, err)
		}
		seeded++
	}
	if seeded == 0 {
		b.Fatal("seeded no blocks; benchmark would time an empty database")
	}
	return seeded
}

type benchmarkUtxoRef struct {
	txID      []byte
	outputIdx uint32
	address   ledger.Address
}

func seedUtxosAtPoints(
	b *testing.B,
	db *database.Database,
	immDb *immutable.ImmutableDb,
	points []ocommon.Point,
	maxRows int,
) []benchmarkUtxoRef {
	b.Helper()
	txn := db.Transaction(true)
	defer txn.Release()
	seen := make(map[string]struct{})
	refs := make([]benchmarkUtxoRef, 0, maxRows)
	for _, point := range points {
		block, err := immDb.GetBlock(point)
		if err != nil {
			b.Fatalf("read UTxO fixture block at slot %d: %v", point.Slot, err)
		}
		if block == nil {
			b.Fatalf("UTxO fixture block at slot %d not found", point.Slot)
		}
		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			b.Fatalf("decode UTxO fixture block at slot %d: %v", block.Slot, err)
		}
		for _, tx := range ledgerBlock.Transactions() {
			for _, produced := range tx.Produced() {
				model, err := models.UtxoLedgerToModel(produced, point.Slot)
				if err != nil {
					b.Fatalf("convert fixture UTxO at slot %d: %v", point.Slot, err)
				}
				key := fmt.Sprintf("%x#%d", model.TxId, model.OutputIdx)
				if _, dup := seen[key]; dup {
					continue
				}
				seen[key] = struct{}{}
				if len(model.Cbor) == 0 {
					b.Fatalf("fixture UTxO %s has no output CBOR", key)
				}
				if err := db.CreateUtxo(txn, &model); err != nil {
					b.Fatalf("seed fixture UTxO %s: %v", key, err)
				}
				if err := db.Blob().SetUtxo(
					txn.Blob(),
					model.TxId,
					model.OutputIdx,
					model.Cbor,
				); err != nil {
					b.Fatalf("seed fixture UTxO CBOR %s: %v", key, err)
				}
				refs = append(refs, benchmarkUtxoRef{
					txID:      append([]byte(nil), model.TxId...),
					outputIdx: model.OutputIdx,
					address:   produced.Output.Address(),
				})
				if len(refs) == maxRows {
					break
				}
			}
			if len(refs) == maxRows {
				break
			}
		}
		if len(refs) == maxRows {
			break
		}
	}
	if len(refs) == 0 {
		b.Fatal("fixture points produced no UTxOs; benchmark would time a miss")
	}
	if err := txn.Commit(); err != nil {
		b.Fatalf("commit fixture UTxOs: %v", err)
	}
	return refs
}

func fixtureEraTransitionPoints(
	b *testing.B,
	immDb *immutable.ImmutableDb,
) []ocommon.Point {
	b.Helper()
	iterator, err := immDb.BlocksFromPoint(ocommon.NewPoint(0, nil))
	if err != nil {
		b.Fatalf("open era-transition fixture iterator: %v", err)
	}
	defer func() { _ = iterator.Close() }()
	points := make([]ocommon.Point, 0, 8)
	seenEras := make(map[uint]struct{})
	for range 100_000 {
		block, err := iterator.Next()
		if err != nil {
			b.Fatalf("read era-transition fixture: %v", err)
		}
		if block == nil {
			break
		}
		if _, seen := seenEras[block.Type]; seen {
			continue
		}
		seenEras[block.Type] = struct{}{}
		points = append(points, ocommon.NewPoint(block.Slot, block.Hash))
	}
	if len(points) < 2 {
		b.Fatalf(
			"fixture contains %d era(s); era-transition benchmark needs at least two",
			len(points),
		)
	}
	return points
}

// seedBlocksFromSlots seeds db with the fixture block at or after each
// requested slot and returns the number stored.
func seedBlocksFromSlots(
	b *testing.B,
	db *database.Database,
	immDb *immutable.ImmutableDb,
	slots []uint64,
) int {
	b.Helper()
	return seedBlocksAtPoints(
		b,
		db,
		immDb,
		fixturePointsFromSlots(b, immDb, slots),
	)
}

// BenchmarkBlockMemoryUsage benchmarks memory usage per block processed
func BenchmarkBlockMemoryUsage(b *testing.B) {
	b.ReportAllocs()

	// Open the immutable database with real Cardano preview testnet data
	immDb, err := immutable.New("../database/immutable/testdata")
	if err != nil {
		b.Fatal(err)
	}

	// Get a few real blocks to process (use BlocksFromPoint iterator)
	originPoint := ocommon.NewPoint(0, nil) // Start from genesis
	iterator, err := immDb.BlocksFromPoint(originPoint)
	if err != nil {
		b.Fatal(err)
	}
	defer iterator.Close()

	// Get the first 10 blocks for benchmarking
	var realBlocks []*immutable.Block
	for i := range 10 {
		block, err := iterator.Next()
		if err != nil {
			b.Fatal(err)
		}
		if block == nil {
			if i == 0 {
				b.Skip("No blocks available in testdata")
			}
			break
		}
		realBlocks = append(realBlocks, block)
	}

	if len(realBlocks) == 0 {
		b.Skip("No blocks available for benchmarking")
	}

	// Reset timer after setup
	b.ResetTimer()

	// Benchmark memory usage during real block processing
	for i := 0; b.Loop(); i++ {
		block := realBlocks[i%len(realBlocks)]

		// Decode block (this is where most memory allocation happens)
		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			// Skip problematic blocks in benchmark
			continue
		}

		// Simulate typical block processing operations that allocate memory
		_ = ledgerBlock.Hash()
		_ = ledgerBlock.PrevHash()
		_ = ledgerBlock.Type()
		_ = ledgerBlock.Cbor()
	}
}

// BenchmarkUtxoLookupByAddressNoData benchmarks UTxO lookup by address on empty database
func BenchmarkUtxoLookupByAddressNoData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Create a test address
	paymentKey := make([]byte, 28) // dummy 28-byte key hash
	stakeKey := make([]byte, 28)
	testAddr, err := ledger.NewAddressFromParts(0, 0, paymentKey, stakeKey)
	if err != nil {
		b.Fatal(err)
	}

	// Reset timer after setup
	b.ResetTimer()

	// Benchmark lookup (on empty database for now)
	for b.Loop() {
		_, err := db.UtxosByAddress(
			[]ledger.Address{testAddr},
			database.MaxUtxosByAddressResults,
			nil,
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUtxoLookupByAddressRealData queries addresses from decoded fixture outputs.
func BenchmarkUtxoLookupByAddressRealData(b *testing.B) {
	db, err := dbtest.NewDatabase(b, &database.Config{DataDir: ""})
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)
	immDb := openImmutableTestDB(b)
	refs := seedUtxosAtPoints(
		b, db, immDb, fixturePointsFrom(b, immDb, 0, 100), 256,
	)
	address := refs[0].address
	got, err := db.UtxosByAddress(
		[]ledger.Address{address}, database.MaxUtxosByAddressResults, nil,
	)
	if err != nil {
		b.Fatalf("preflight UTxO address lookup: %v", err)
	}
	if len(got) == 0 {
		b.Fatal("fixture address returned no UTxOs; benchmark would time a miss")
	}
	b.ResetTimer()
	for b.Loop() {
		if _, err := db.UtxosByAddress(
			[]ledger.Address{address},
			database.MaxUtxosByAddressResults,
			nil,
		); err != nil {
			b.Fatalf("UTxO address lookup: %v", err)
		}
	}
	b.ReportMetric(float64(len(refs)), "utxos")
}

// BenchmarkUtxoLookupByRefNoData benchmarks UTxO lookup by reference on empty database
func BenchmarkUtxoLookupByRefNoData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Create a test transaction reference
	testTxId := make([]byte, 32) // dummy 32-byte tx ID
	for i := range testTxId {
		testTxId[i] = byte(i % 256)
	}
	testOutputIdx := uint32(0)

	// Reset timer after setup
	b.ResetTimer()

	// Benchmark lookup (on empty database for now)
	for b.Loop() {
		// UtxoByRef returns nil, ErrUtxoNotFound for missing UTxOs
		// This is expected and not an error for benchmarking
		_, err := db.UtxoByRef(testTxId, testOutputIdx, nil)
		if err != nil && !errors.Is(err, database.ErrUtxoNotFound) {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// BenchmarkUtxoLookupByRefRealData queries a reference from a decoded fixture output.
func BenchmarkUtxoLookupByRefRealData(b *testing.B) {
	db, err := dbtest.NewDatabase(b, &database.Config{DataDir: ""})
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)
	immDb := openImmutableTestDB(b)
	refs := seedUtxosAtPoints(
		b, db, immDb, fixturePointsFrom(b, immDb, 0, 100), 256,
	)
	ref := refs[0]
	got, err := db.UtxoByRef(ref.txID, ref.outputIdx, nil)
	if err != nil {
		b.Fatalf("preflight UTxO reference lookup: %v", err)
	}
	if got == nil {
		b.Fatal("fixture reference returned no UTxO; benchmark would time a miss")
	}
	b.ResetTimer()
	for b.Loop() {
		if _, err := db.UtxoByRef(ref.txID, ref.outputIdx, nil); err != nil {
			b.Fatalf("UTxO reference lookup: %v", err)
		}
	}
	b.ReportMetric(float64(len(refs)), "utxos")
}

// BenchmarkBlockRetrievalByIndexNoData benchmarks block retrieval by index on empty database
func BenchmarkBlockRetrievalByIndexNoData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Reset timer after setup
	b.ResetTimer()

	// Benchmark retrieval (will return error for non-existent block)
	for b.Loop() {
		// BlockByIndex returns nil, ErrBlockNotFound for missing blocks
		// This is expected and not an error for benchmarking
		_, err := db.BlockByIndex(1, nil)
		if err != nil && !errors.Is(err, models.ErrBlockNotFound) {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// BenchmarkBlockRetrievalByIndexRealData benchmarks block retrieval by index against real seeded data
func BenchmarkBlockRetrievalByIndexRealData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Seed database with real blocks
	immDb := openImmutableTestDB(b)

	// Sample blocks from across the fixture
	sampleSlots := []uint64{1000, 5000, 10000, 50000, 100000}
	seeded := seedBlocksFromSlots(b, db, immDb, sampleSlots)
	b.Logf("Seeded %d fixture blocks", seeded)

	// Reset timer after seeding
	b.ResetTimer()

	// Benchmark retrieval against real seeded data
	for i := 0; b.Loop(); i++ {
		blockID := uint64((i % len(sampleSlots)) + 1)
		// BlockByIndex returns nil, ErrBlockNotFound for missing blocks
		// This is expected and not an error for benchmarking
		_, err := db.BlockByIndex(blockID, nil)
		if err != nil && !errors.Is(err, models.ErrBlockNotFound) {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// BenchmarkTransactionHistoryQueriesNoData benchmarks transaction lookup by hash on empty database
func BenchmarkTransactionHistoryQueriesNoData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Create a test transaction hash
	testTxHash := make([]byte, 32) // dummy 32-byte hash
	for i := range testTxHash {
		testTxHash[i] = byte(i % 256)
	}

	// Reset timer after setup
	b.ResetTimer()

	// Benchmark lookup (on empty database for now)
	for b.Loop() {
		_, err := db.Metadata().GetTransactionByHash(testTxHash, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTransactionHistoryQueriesRealData benchmarks transaction lookup by hash against real seeded data
// NOTE: Currently uses synthetic transaction hashes that won't match seeded data,
// so this measures query performance against empty results with real blocks present.
// TODO: Extract real transaction hashes from seeded blocks for more realistic benchmarking.
func BenchmarkTransactionHistoryQueriesRealData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Seed database with fixture blocks
	immDb := openImmutableTestDB(b)

	// Sample blocks from across the fixture
	sampleSlots := []uint64{10000, 50000, 100000, 150000, 200000}
	seeded := seedBlocksFromSlots(b, db, immDb, sampleSlots)
	b.Logf("Seeded %d fixture blocks", seeded)

	// Create test transaction hashes (use real-looking hashes)
	// NOTE: These are synthetic hashes that won't match seeded transactions,
	// making this benchmark equivalent to NoData variant. Real transaction
	// hash extraction would require parsing block contents, which is complex.
	testTxHashes := make([][]byte, 10)
	for j := range testTxHashes {
		hash := make([]byte, 32)
		for i := range hash {
			hash[i] = byte((j*32 + i) % 256)
		}
		testTxHashes[j] = hash
	}

	// Reset timer after seeding
	b.ResetTimer()

	// Benchmark lookup against real seeded data
	for i := 0; b.Loop(); i++ {
		hash := testTxHashes[i%len(testTxHashes)]
		_, err := db.Metadata().GetTransactionByHash(hash, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAccountLookupByStakeKeyNoData benchmarks account lookup by stake key on empty database
func BenchmarkAccountLookupByStakeKeyNoData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Create a test stake key
	testStakeKey := make([]byte, 28) // 28-byte stake key hash
	for i := range testStakeKey {
		testStakeKey[i] = byte(i % 256)
	}

	// Reset timer after setup
	b.ResetTimer()

	// Benchmark lookup (on empty database for now)
	for b.Loop() {
		_, err := db.Metadata().
			GetAccountByCredential(0, testStakeKey, false, nil)
		if err != nil && !errors.Is(err, models.ErrAccountNotFound) {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// BenchmarkAccountLookupByStakeKeyRealData benchmarks account lookup by stake key against real seeded data
func BenchmarkAccountLookupByStakeKeyRealData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Seed database with real blocks
	immDb := openImmutableTestDB(b)

	// Sample blocks from across the fixture
	sampleSlots := []uint64{1000, 5000, 10000, 50000, 100000}
	seeded := seedBlocksFromSlots(b, db, immDb, sampleSlots)
	b.Logf("Seeded %d fixture blocks", seeded)

	// Create test stake keys
	testStakeKeys := make([][]byte, 10)
	for j := range testStakeKeys {
		key := make([]byte, 28)
		for i := range key {
			key[i] = byte((j*28 + i) % 256)
		}
		testStakeKeys[j] = key
	}

	// Reset timer after seeding
	b.ResetTimer()

	// Benchmark lookup against real seeded data
	for i := 0; b.Loop(); i++ {
		key := testStakeKeys[i%len(testStakeKeys)]
		_, err := db.Metadata().GetAccountByCredential(0, key, false, nil)
		if err != nil && !errors.Is(err, models.ErrAccountNotFound) {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// BenchmarkPoolLookupByKeyHashNoData benchmarks pool lookup by key hash on empty database
func BenchmarkPoolLookupByKeyHashNoData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Create a test pool key hash (28 bytes)
	testPoolKeyHash := lcommon.PoolKeyHash(make([]byte, 28))
	for i := range testPoolKeyHash {
		testPoolKeyHash[i] = byte(i % 256)
	}

	// Reset timer after setup
	b.ResetTimer()

	// Benchmark lookup (on empty database for now)
	for b.Loop() {
		_, err := db.Metadata().GetPool(testPoolKeyHash, false, nil)
		if err != nil && !errors.Is(err, models.ErrPoolNotFound) {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// BenchmarkPoolLookupByKeyHashRealData benchmarks pool lookup by key hash against real seeded data
func BenchmarkPoolLookupByKeyHashRealData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Seed database with real blocks
	immDb := openImmutableTestDB(b)

	// Sample blocks from across the fixture
	sampleSlots := []uint64{1000, 5000, 10000, 50000, 100000}
	seeded := seedBlocksFromSlots(b, db, immDb, sampleSlots)
	b.Logf("Seeded %d fixture blocks", seeded)

	// Create test pool key hashes
	testPoolKeyHashes := make([]lcommon.PoolKeyHash, 10)
	for j := range testPoolKeyHashes {
		hash := make([]byte, 28)
		for i := range hash {
			hash[i] = byte((j*28 + i) % 256)
		}
		testPoolKeyHashes[j] = lcommon.PoolKeyHash(hash)
	}

	// Reset timer after seeding
	b.ResetTimer()

	// Benchmark lookup against real seeded data
	for i := 0; b.Loop(); i++ {
		hash := testPoolKeyHashes[i%len(testPoolKeyHashes)]
		_, err := db.Metadata().GetPool(hash, false, nil)
		if err != nil && !errors.Is(err, models.ErrPoolNotFound) {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// BenchmarkDRepLookupByKeyHashNoData benchmarks DRep lookup by key hash on empty database
func BenchmarkDRepLookupByKeyHashNoData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Create a test DRep credential (32 bytes)
	testDRepCredential := make([]byte, 32)
	for i := range testDRepCredential {
		testDRepCredential[i] = byte(i % 256)
	}

	// Reset timer after setup
	b.ResetTimer()

	// Benchmark lookup (on empty database for now)
	for b.Loop() {
		_, err := db.Metadata().GetDrep(testDRepCredential, false, nil)
		if err != nil && !errors.Is(err, models.ErrDrepNotFound) {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// BenchmarkDRepLookupByKeyHashRealData benchmarks DRep lookup by key hash against real seeded data
func BenchmarkDRepLookupByKeyHashRealData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Seed database with real blocks
	immDb := openImmutableTestDB(b)

	// Sample blocks from across the fixture
	sampleSlots := []uint64{1000, 5000, 10000, 50000, 100000}
	seeded := seedBlocksFromSlots(b, db, immDb, sampleSlots)
	b.Logf("Seeded %d fixture blocks", seeded)

	// Create test DRep credentials
	testDRepCredentials := make([][]byte, 10)
	for j := range testDRepCredentials {
		cred := make([]byte, 32)
		for i := range cred {
			cred[i] = byte((j*32 + i) % 256)
		}
		testDRepCredentials[j] = cred
	}

	// Reset timer after seeding
	b.ResetTimer()

	// Benchmark lookup against real seeded data
	for i := 0; b.Loop(); i++ {
		cred := testDRepCredentials[i%len(testDRepCredentials)]
		_, err := db.Metadata().GetDrep(cred, false, nil)
		if err != nil && !errors.Is(err, models.ErrDrepNotFound) {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// BenchmarkDatumLookupByHashNoData benchmarks datum lookup by hash on empty database
func BenchmarkDatumLookupByHashNoData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Create a test datum hash (32 bytes)
	testDatumHash := lcommon.Blake2b256(make([]byte, 32))
	for i := range testDatumHash {
		testDatumHash[i] = byte(i % 256)
	}

	// Reset timer after setup
	b.ResetTimer()

	// Benchmark lookup (on empty database for now)
	for b.Loop() {
		// In the sqlite implementation, GetDatum returns (nil, nil) for missing datums.
		// Receiving a nil datum with no error is expected here and not a failure.
		_, err := db.Metadata().GetDatum(testDatumHash, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDatumLookupByHashRealData benchmarks datum lookup by hash against real seeded data
func BenchmarkDatumLookupByHashRealData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Seed database with real blocks
	immDb := openImmutableTestDB(b)

	// Sample blocks from across the fixture
	sampleSlots := []uint64{1000, 5000, 10000, 50000, 100000}
	seeded := seedBlocksFromSlots(b, db, immDb, sampleSlots)
	b.Logf("Seeded %d fixture blocks", seeded)

	// Create test datum hashes
	testDatumHashes := make([]lcommon.Blake2b256, 10)
	for j := range testDatumHashes {
		hash := make([]byte, 32)
		for i := range hash {
			hash[i] = byte((j*32 + i) % 256)
		}
		testDatumHashes[j] = lcommon.Blake2b256(hash)
	}

	// Reset timer after seeding
	b.ResetTimer()

	// Benchmark lookup against real seeded data
	for i := 0; b.Loop(); i++ {
		hash := testDatumHashes[i%len(testDatumHashes)]
		// GetDatum returns nil, nil for missing datums
		// This is expected and not an error for benchmarking
		_, err := db.Metadata().GetDatum(hash, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkProtocolParametersLookupByEpochNoData benchmarks protocol parameters lookup by epoch on empty database
func BenchmarkProtocolParametersLookupByEpochNoData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Create test epochs
	testEpochs := []uint64{1, 10, 50, 100, 200}

	// Reset timer after setup
	b.ResetTimer()

	// Benchmark lookup (on empty database for now)
	for i := 0; b.Loop(); i++ {
		epoch := testEpochs[i%len(testEpochs)]
		_, err := db.Metadata().GetPParams(epoch, ledger.EraIdShelley, nil)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// BenchmarkProtocolParametersLookupByEpochRealData benchmarks protocol parameters lookup by epoch against real seeded data
func BenchmarkProtocolParametersLookupByEpochRealData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Seed database with real blocks
	immDb := openImmutableTestDB(b)

	// Sample blocks from across the fixture
	sampleSlots := []uint64{1000, 5000, 10000, 50000, 100000}
	seeded := seedBlocksFromSlots(b, db, immDb, sampleSlots)
	b.Logf("Seeded %d fixture blocks", seeded)

	// Create test epochs
	testEpochs := []uint64{1, 10, 50, 100, 200}

	// Reset timer after seeding
	b.ResetTimer()

	// Benchmark lookup against real seeded data
	for i := 0; b.Loop(); i++ {
		epoch := testEpochs[i%len(testEpochs)]
		_, err := db.Metadata().GetPParams(epoch, ledger.EraIdShelley, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBlockNonceLookupNoData benchmarks block nonce lookup on empty database
func BenchmarkBlockNonceLookupNoData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Create test points (slot, hash)
	testPoints := make([]ocommon.Point, 10)
	for j := range testPoints {
		slot := uint64(1000 + j*1000)
		hash := make([]byte, 32)
		for i := range hash {
			hash[i] = byte((j*32 + i) % 256)
		}
		testPoints[j] = ocommon.NewPoint(slot, hash)
	}

	// Reset timer after setup
	b.ResetTimer()

	// Benchmark lookup (on empty database for now)
	for i := 0; b.Loop(); i++ {
		point := testPoints[i%len(testPoints)]
		// GetBlockNonce returns empty nonce for missing blocks
		// This is expected and not an error for benchmarking
		_, err := db.Metadata().GetBlockNonce(point, nil)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// BenchmarkBlockNonceLookupRealData benchmarks block nonce lookup against real seeded data
func BenchmarkBlockNonceLookupRealData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Seed database with real blocks
	immDb := openImmutableTestDB(b)

	// Sample blocks from across the fixture
	sampleSlots := []uint64{1000, 5000, 10000, 50000, 100000}
	seeded := seedBlocksFromSlots(b, db, immDb, sampleSlots)
	b.Logf("Seeded %d fixture blocks", seeded)

	// Synthetic points: the nonce table is empty, so these measure the miss
	// path. Seeding a block does not store a nonce for it.
	testPoints := make([]ocommon.Point, 10)
	for j := range testPoints {
		slot := uint64(1000 + j*1000)
		hash := make([]byte, 32)
		for i := range hash {
			hash[i] = byte((j*32 + i) % 256)
		}
		testPoints[j] = ocommon.NewPoint(slot, hash)
	}

	// Reset timer after seeding
	b.ResetTimer()

	// Benchmark lookup against real seeded data
	for i := 0; b.Loop(); i++ {
		point := testPoints[i%len(testPoints)]
		// GetBlockNonce returns empty nonce for missing blocks
		// This is expected and not an error for benchmarking
		_, err := db.Metadata().GetBlockNonce(point, nil)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// BenchmarkStakeRegistrationLookupsNoData benchmarks stake registration lookups on empty database
func BenchmarkStakeRegistrationLookupsNoData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Create test stake keys
	testStakeKeys := make([][]byte, 10)
	for j := range testStakeKeys {
		key := make([]byte, 28)
		for i := range key {
			key[i] = byte((j*28 + i) % 256)
		}
		testStakeKeys[j] = key
	}

	// Reset timer after setup
	b.ResetTimer()

	// Benchmark lookup (on empty database for now)
	for i := 0; b.Loop(); i++ {
		stakeKey := testStakeKeys[i%len(testStakeKeys)]
		_, err := db.Metadata().
			GetStakeRegistrationsByCredential(0, stakeKey, nil)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// BenchmarkStakeRegistrationLookupsRealData benchmarks stake registration lookups against real seeded data
func BenchmarkStakeRegistrationLookupsRealData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Seed database with real blocks
	immDb := openImmutableTestDB(b)

	// Sample blocks from across the fixture
	sampleSlots := []uint64{1000, 5000, 10000, 50000, 100000}
	seeded := seedBlocksFromSlots(b, db, immDb, sampleSlots)
	b.Logf("Seeded %d fixture blocks", seeded)

	// Create test stake keys
	testStakeKeys := make([][]byte, 10)
	for j := range testStakeKeys {
		key := make([]byte, 28)
		for i := range key {
			key[i] = byte((j*28 + i) % 256)
		}
		testStakeKeys[j] = key
	}

	// Reset timer after seeding
	b.ResetTimer()

	// Benchmark lookup against real seeded data
	for i := 0; b.Loop(); i++ {
		stakeKey := testStakeKeys[i%len(testStakeKeys)]
		_, err := db.Metadata().
			GetStakeRegistrationsByCredential(0, stakeKey, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPoolRegistrationLookupsNoData benchmarks pool registration lookups on empty database
func BenchmarkPoolRegistrationLookupsNoData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Create test pool key hashes
	testPoolKeyHashes := make([]lcommon.PoolKeyHash, 10)
	for j := range testPoolKeyHashes {
		hash := make([]byte, 28)
		for i := range hash {
			hash[i] = byte((j*28 + i) % 256)
		}
		testPoolKeyHashes[j] = lcommon.PoolKeyHash(hash)
	}

	// Reset timer after setup
	b.ResetTimer()

	// Benchmark lookup (on empty database for now)
	for i := 0; b.Loop(); i++ {
		poolKeyHash := testPoolKeyHashes[i%len(testPoolKeyHashes)]
		_, err := db.Metadata().GetPoolRegistrations(poolKeyHash, nil)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// BenchmarkPoolRegistrationLookupsRealData benchmarks pool registration lookups against real seeded data
func BenchmarkPoolRegistrationLookupsRealData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Seed database with real blocks
	immDb := openImmutableTestDB(b)

	// Sample blocks from across the fixture
	sampleSlots := []uint64{1000, 5000, 10000, 50000, 100000}
	seeded := seedBlocksFromSlots(b, db, immDb, sampleSlots)
	b.Logf("Seeded %d fixture blocks", seeded)

	// Create test pool key hashes
	testPoolKeyHashes := make([]lcommon.PoolKeyHash, 10)
	for j := range testPoolKeyHashes {
		hash := make([]byte, 28)
		for i := range hash {
			hash[i] = byte((j*28 + i) % 256)
		}
		testPoolKeyHashes[j] = lcommon.PoolKeyHash(hash)
	}

	// Reset timer after seeding
	b.ResetTimer()

	// Benchmark lookup against real seeded data
	for i := 0; b.Loop(); i++ {
		poolKeyHash := testPoolKeyHashes[i%len(testPoolKeyHashes)]
		_, err := db.Metadata().GetPoolRegistrations(poolKeyHash, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEraTransitionPerformance benchmarks processing blocks across Cardano era transitions
func BenchmarkEraTransitionPerformance(b *testing.B) {
	// Open immutable database
	immDb := openImmutableTestDB(b)

	// Sample blocks from different eras across the fixture. Processing them
	// in slot order naturally spans whatever era transitions it contains.
	var sampleSlots []uint64
	for slot := uint64(1); slot <= 200000; slot += 1000 {
		sampleSlots = append(sampleSlots, slot)
	}

	var blocks []*immutable.Block
	var currentEra uint = 999 // sentinel value
	for _, point := range fixturePointsFromSlots(b, immDb, sampleSlots) {
		block, err := immDb.GetBlock(point)
		if err != nil {
			b.Fatalf("read fixture block at slot %d: %v", point.Slot, err)
		}
		if block == nil {
			b.Fatalf("fixture block at slot %d not found", point.Slot)
		}

		// Include this block if it's a different era than the last one we processed
		// or if we haven't collected many blocks yet
		if block.Type != currentEra || len(blocks) < 20 {
			blocks = append(blocks, block)
			currentEra = block.Type
			if len(blocks) >= 50 { // Limit to reasonable number
				break
			}
		}
	}
	if len(blocks) == 0 {
		b.Fatal("collected no fixture blocks; benchmark would time an empty loop")
	}

	// Reset timer after setup
	b.ResetTimer()

	// Benchmark processing blocks across era transitions
	for b.Loop() {
		for _, block := range blocks {
			ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
			if err != nil {
				b.Fatal(err)
			}

			// Simulate basic block processing (just accessing key properties)
			_ = ledgerBlock.Hash()
			_ = ledgerBlock.PrevHash()
			_ = ledgerBlock.Type()
		}
	}
}

// BenchmarkEraTransitionPerformanceRealData reads and decodes stored fixture blocks at era boundaries.
func BenchmarkEraTransitionPerformanceRealData(b *testing.B) {
	db, err := dbtest.NewDatabase(b, &database.Config{DataDir: ""})
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)
	immDb := openImmutableTestDB(b)
	points := fixtureEraTransitionPoints(b, immDb)
	seeded := seedBlocksAtPoints(b, db, immDb, points)
	for _, point := range points {
		stored, err := database.BlockByPoint(db, point)
		if err != nil {
			b.Fatalf("read seeded block at slot %d: %v", point.Slot, err)
		}
		if !bytes.Equal(stored.Hash, point.Hash) {
			b.Fatalf("seeded block at slot %d has a different hash", point.Slot)
		}
	}
	b.ResetTimer()
	for b.Loop() {
		for _, point := range points {
			stored, err := database.BlockByPoint(db, point)
			if err != nil {
				b.Fatalf("read block at slot %d: %v", point.Slot, err)
			}
			ledgerBlock, err := ledger.NewBlockFromCbor(
				uint(stored.Type),
				stored.Cbor,
			)
			if err != nil {
				b.Fatalf("decode block at slot %d: %v", point.Slot, err)
			}
			_ = ledgerBlock.Hash()
			_ = ledgerBlock.PrevHash()
			_ = ledgerBlock.Type()
		}
	}
	b.ReportMetric(float64(seeded), "fixture_blocks")
}

// BenchmarkIndexBuildingTime benchmarks the time to build indexes for new blocks
func BenchmarkIndexBuildingTime(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Open the immutable database with real Cardano preview testnet data
	immDb, err := immutable.New("../database/immutable/testdata")
	if err != nil {
		b.Fatal(err)
	}

	// Get a few real blocks to process for index building
	originPoint := ocommon.NewPoint(0, nil) // Start from genesis
	iterator, err := immDb.BlocksFromPoint(originPoint)
	if err != nil {
		b.Fatal(err)
	}
	defer iterator.Close()

	// Get the first 5 blocks for benchmarking
	var realBlocks []*immutable.Block
	for i := range 5 {
		block, err := iterator.Next()
		if err != nil {
			b.Fatal(err)
		}
		if block == nil {
			if i == 0 {
				b.Skip("No blocks available in testdata")
			}
			break
		}
		realBlocks = append(realBlocks, block)
	}

	if len(realBlocks) == 0 {
		b.Skip("No blocks available for benchmarking")
	}

	// Reset timer after setup
	b.ResetTimer()

	// Benchmark actual index building for real blocks
	for i := 0; b.Loop(); i++ {
		block := realBlocks[i%len(realBlocks)]

		// Decode block
		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			// Skip problematic blocks in benchmark
			continue
		}

		// Build block index (this is the primary index building operation)
		blockModel := models.Block{
			ID:       uint64(i + 1), // Simple incrementing ID for benchmark
			Slot:     block.Slot,
			Hash:     block.Hash,
			Number:   uint64(i + 1),
			Type:     uint(ledgerBlock.Type()),
			PrevHash: ledgerBlock.PrevHash().Bytes(),
			Cbor:     ledgerBlock.Cbor(),
		}

		// Store block (this builds the primary block index)
		if err := db.BlockCreate(blockModel, nil); err != nil {
			// Skip on error for benchmark (e.g., duplicate key)
			continue
		}
	}
}

// BenchmarkRealBlockReading benchmarks reading real blocks from Cardano testnet data
func BenchmarkRealBlockReading(b *testing.B) {
	// Open the immutable database with real Cardano preview testnet data
	immDb, err := immutable.New("../database/immutable/testdata")
	if err != nil {
		b.Fatal(err)
	}

	// Get the tip to know the range of available blocks
	tip, err := immDb.GetTip()
	if err != nil {
		b.Fatal(err)
	}
	if tip == nil {
		b.Skip("No blocks available in test database")
	}

	// Five blocks near the tip. GetBlock matches on the full hash, so each
	// point must carry the block's own hash; the five slots below the tip
	// hold no block at all, since the fixture's blocks are 20 slots apart.
	const nearTipSlots = 1000
	startSlot := uint64(0)
	if tip.Slot > nearTipSlots {
		startSlot = tip.Slot - nearTipSlots
	}
	testPoints := fixturePointsFrom(b, immDb, startSlot, 5)

	// Reset timer after setup
	b.ResetTimer()

	for i := 0; b.Loop(); i++ {
		point := testPoints[i%len(testPoints)]
		block, err := immDb.GetBlock(point)
		if err != nil {
			b.Fatal(err)
		}
		if block == nil {
			b.Fatalf("fixture block at slot %d not found", point.Slot)
		}
	}
}

// BenchmarkRealBlockProcessing benchmarks end-to-end processing of real Cardano blocks
func BenchmarkRealBlockProcessing(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Open the immutable database with real Cardano preview testnet data
	immDb, err := immutable.New("../database/immutable/testdata")
	if err != nil {
		b.Fatal(err)
	}

	// Get a few real blocks to process (use BlocksFromPoint iterator)
	originPoint := ocommon.NewPoint(0, nil) // Start from genesis
	iterator, err := immDb.BlocksFromPoint(originPoint)
	if err != nil {
		b.Fatal(err)
	}
	defer iterator.Close()

	// Get the first 5 blocks
	var realBlocks []*immutable.Block
	for i := range 5 {
		block, err := iterator.Next()
		if err != nil {
			b.Fatal(err)
		}
		if block == nil {
			b.Fatalf("Not enough blocks in database, only got %d", i)
		}
		realBlocks = append(realBlocks, block)
	}
	// b.Logf("Successfully loaded %d real blocks", len(realBlocks))

	if len(realBlocks) == 0 {
		b.Skip("No blocks available for benchmarking")
	}

	// Reset timer after setup
	b.ResetTimer()

	// Benchmark storing real blocks in database
	for i := 0; b.Loop(); i++ {
		block := realBlocks[i%len(realBlocks)]

		// Debug: check block data
		if block == nil {
			b.Fatal("block is nil")
		}
		if len(block.Cbor) == 0 {
			b.Fatal("block.Cbor is empty")
		}

		// Convert immutable block to ledger block
		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			b.Fatalf("NewBlockFromCbor failed: %v", err)
		}

		// Debug: check if ledgerBlock is nil
		if ledgerBlock == nil {
			b.Fatal("ledgerBlock is nil")
		}

		// Store block directly in database (simplified version of chain.AddBlock)
		point := ocommon.NewPoint(block.Slot, block.Hash)
		blockModel := models.Block{
			ID:       uint64(i + 1), // Simple incrementing ID for benchmark
			Slot:     point.Slot,
			Hash:     point.Hash,
			Number:   0, // Placeholder - will fix after debugging
			Type:     uint(ledgerBlock.Type()),
			PrevHash: ledgerBlock.PrevHash().Bytes(),
			Cbor:     ledgerBlock.Cbor(),
		}

		// Store in database
		if err := db.BlockCreate(blockModel, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRealDataQueries benchmarks database queries against real Cardano data
func BenchmarkRealDataQueries(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Open the immutable database with real Cardano preview testnet data
	immDb, err := immutable.New("../database/immutable/testdata")
	if err != nil {
		b.Fatal(err)
	}

	// Seed database with real blocks (first 100 blocks for realistic data)
	originPoint := ocommon.NewPoint(0, nil)
	iterator, err := immDb.BlocksFromPoint(originPoint)
	if err != nil {
		b.Fatal(err)
	}
	defer iterator.Close()

	// Load and store 100 real blocks
	for i := range 100 {
		block, err := iterator.Next()
		if err != nil {
			b.Fatal(err)
		}
		if block == nil {
			break // End of data
		}

		// Convert and store block
		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			b.Fatal(err)
		}

		blockModel := models.Block{
			ID:       uint64(i + 1),
			Slot:     block.Slot,
			Hash:     block.Hash,
			Number:   0,
			Type:     uint(ledgerBlock.Type()),
			PrevHash: ledgerBlock.PrevHash().Bytes(),
			Cbor:     ledgerBlock.Cbor(),
		}

		if err := db.BlockCreate(blockModel, nil); err != nil {
			b.Fatal(err)
		}
	}

	// Reset timer after seeding database
	b.ResetTimer()

	// Benchmark block retrieval queries against real data
	for i := 0; b.Loop(); i++ {
		// Query for a random block ID (1-100)
		blockID := uint64((i % 100) + 1)
		_, err := db.BlockByIndex(blockID, nil)
		if err != nil && !errors.Is(err, models.ErrBlockNotFound) {
			b.Fatal(err)
		}
	}
}

// BenchmarkChainSyncFromGenesis benchmarks processing blocks from genesis using real immutable testdata
func BenchmarkChainSyncFromGenesis(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Open immutable database with real testdata
	immDb, err := immutable.New("../database/immutable/testdata")
	if err != nil {
		b.Fatal(err)
	}

	// Start from genesis (origin point)
	genesisPoint := ocommon.NewPointOrigin()

	// Reset timer after setup
	b.ResetTimer()

	// Process blocks sequentially (simulate chain sync)
	// Each benchmark iteration processes up to 100 blocks
	blocksProcessed := 0
	for b.Loop() {
		// Create iterator for each benchmark iteration
		iterator, err := immDb.BlocksFromPoint(genesisPoint)
		if err != nil {
			b.Fatal(err)
		}

		blocksProcessed = 0
		for blocksProcessed < 100 {
			block, err := iterator.Next()
			if err != nil {
				b.Fatal(err)
			}
			if block == nil {
				// End of chain
				break
			}

			// Decode block to ensure it's valid
			ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
			if err != nil {
				// Skip problematic blocks
				continue
			}

			// Store block in database (minimal processing)
			blockModel := models.Block{
				ID:       block.Slot, // Use slot as ID
				Slot:     block.Slot,
				Hash:     block.Hash,
				Number:   uint64(blocksProcessed),
				Type:     uint(ledgerBlock.Type()),
				PrevHash: ledgerBlock.PrevHash().Bytes(),
				Cbor:     ledgerBlock.Cbor(),
			}

			if err := db.BlockCreate(blockModel, nil); err != nil {
				// Skip if block already exists or other error
				continue
			}

			blocksProcessed++
		}
		iterator.Close() // Close iterator after each iteration
	}

	// Report metrics
	b.ReportMetric(float64(blocksProcessed), "blocks_processed")
}

// BenchmarkTransactionValidation benchmarks transaction validation using real transactions from testnet data
func BenchmarkTransactionValidation(b *testing.B) {
	// Open immutable database with real testnet data
	immDb, err := immutable.New("../database/immutable/testdata")
	if err != nil {
		b.Fatal(err)
	}

	// Set up ledger state for validation
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Create chain manager
	chainManager, err := chain.NewManager(db, nil)
	if err != nil {
		b.Fatal(err)
	}

	// Create ledger state for validation
	ledgerCfg := LedgerStateConfig{
		Database:     db,
		ChainManager: chainManager,
	}
	ledgerState, err := NewLedgerState(ledgerCfg)
	if err != nil {
		b.Fatal(err)
	}

	// Find blocks with transactions
	originPoint := ocommon.NewPoint(0, nil)
	iterator, err := immDb.BlocksFromPoint(originPoint)
	if err != nil {
		b.Fatal(err)
	}
	defer iterator.Close()

	var sampleTxs []lcommon.Transaction
	txCount := 0

	// Collect up to 10 sample transactions from real blocks
	for txCount < 10 {
		block, err := iterator.Next()
		if err != nil {
			break
		}
		if block == nil {
			break
		}

		// Decode block
		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			b.Logf("Could not parse block: %v", err)
			continue
		}

		// Extract transactions from block
		blockTxs := ledgerBlock.Transactions()
		for _, tx := range blockTxs {
			if txCount >= 10 {
				break
			}
			sampleTxs = append(sampleTxs, tx)
			txCount++
		}

		if txCount >= 10 {
			break
		}
	}

	if len(sampleTxs) == 0 {
		b.Skip("No transactions found in test data")
	}

	// Reset timer after setup
	b.ResetTimer()

	// Benchmark transaction validation
	for i := 0; b.Loop(); i++ {
		tx := sampleTxs[i%len(sampleTxs)]

		// Validate transaction (this will test UTxO validation and any other rules)
		err := ledgerState.ValidateTx(tx)
		if err != nil {
			// For benchmark purposes, we expect some validation failures due to missing UTxO context
			// This is still useful for measuring validation performance
			_ = err // Ignore error for benchmark
		}
	}
}

// BenchmarkBlockProcessingThroughput measures blocks/second processing throughput
// using real Cardano testnet data in a continuous processing loop
func BenchmarkBlockProcessingThroughput(b *testing.B) {
	seedModels, blocks := loadBlockProcessingFixture(b)
	db, ledgerState := newBlockProcessingBenchmarkLedgerState(
		b,
		seedModels,
	)
	b.Cleanup(func() { dbtest.CloseDatabase(db) })

	b.Logf("Loaded %d blocks for throughput testing", len(blocks))

	b.ResetTimer()

	processedBlocks := 0
	blockIdx := 0
	for i := 0; b.Loop(); i++ {
		if blockIdx == len(blocks) {
			b.StopTimer()
			if err := dbtest.CloseDatabase(db); err != nil {
				b.Fatal(err)
			}
			db, ledgerState = newBlockProcessingBenchmarkLedgerState(
				b,
				seedModels,
			)
			blockIdx = 0
			b.StartTimer()
		}
		block := blocks[blockIdx]
		blockIdx++

		// Convert to ledger block
		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			b.Fatalf("NewBlockFromCbor failed: %v", err)
		}

		// Live blockfetch uses a blob-only txn for chain insertion.
		txn := db.BlobTxn(true)

		// Add block to chain (this includes transaction validation and state updates)
		if err := ledgerState.Chain().AddBlock(ledgerBlock, txn); err != nil {
			_ = txn.Rollback()
			b.Fatalf("AddBlock failed: %v", err)
		}

		// Commit transaction to finalize DB resources for this iteration
		if err := txn.Commit(); err != nil {
			_ = txn.Rollback()
			b.Fatalf("Failed to commit transaction: %v", err)
		}

		processedBlocks++
	}

	// Report blocks per second
	b.ReportMetric(float64(processedBlocks)/b.Elapsed().Seconds(), "blocks/sec")
}

// BenchmarkBlockfetchNearTipThroughput measures the live blockfetch handler
// when we are effectively at tip, so each received block is flushed
// immediately instead of waiting for a multi-block commit batch.
func BenchmarkBlockfetchNearTipThroughput(b *testing.B) {
	seedModels, blocks := loadBlockProcessingFixture(b)
	db, ledgerState := newBlockProcessingBenchmarkLedgerState(
		b,
		seedModels,
	)
	b.Cleanup(func() { dbtest.CloseDatabase(db) })

	b.Logf("Loaded %d blocks for near-tip blockfetch testing", len(blocks))

	b.ResetTimer()

	processedBlocks := 0
	blockIdx := 0
	for i := 0; b.Loop(); i++ {
		if blockIdx == len(blocks) {
			b.StopTimer()
			if err := dbtest.CloseDatabase(db); err != nil {
				b.Fatal(err)
			}
			db, ledgerState = newBlockProcessingBenchmarkLedgerState(
				b,
				seedModels,
			)
			blockIdx = 0
			b.StartTimer()
		}
		block := blocks[blockIdx]
		blockIdx++

		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			b.Fatalf("NewBlockFromCbor failed: %v", err)
		}
		evt := BlockfetchEvent{
			Block: ledgerBlock,
			Point: ocommon.NewPoint(block.Slot, block.Hash),
			Type:  block.Type,
		}

		if err := handleEventBlockfetchBlockDeferred(ledgerState, evt, nil); err != nil {
			b.Fatalf("handleEventBlockfetchBlock failed: %v", err)
		}
		// Flush after each block to simulate near-tip behavior where
		// blocks are committed individually rather than batched.
		if err := ledgerState.flushPendingBlockfetchBlocksDeferred(nil); err != nil {
			b.Fatalf("flushPendingBlockfetchBlocks failed: %v", err)
		}

		processedBlocks++
	}

	b.ReportMetric(float64(processedBlocks)/b.Elapsed().Seconds(), "blocks/sec")
}

// BenchmarkBlockfetchNearTipThroughputPredecoded measures the live
// blockfetch near-tip path with block decoding removed from the timed region.
func BenchmarkBlockfetchNearTipThroughputPredecoded(b *testing.B) {
	seedModels, rawBlocks := loadBlockProcessingFixture(b)
	blocks := make([]ledger.Block, 0, len(rawBlocks))
	points := make([]ocommon.Point, 0, len(rawBlocks))
	for _, block := range rawBlocks {
		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			b.Fatalf("predecode block: %v", err)
		}
		blocks = append(blocks, ledgerBlock)
		points = append(points, ocommon.NewPoint(block.Slot, block.Hash))
	}

	db, ledgerState := newBlockProcessingBenchmarkLedgerState(
		b,
		seedModels,
	)
	b.Cleanup(func() { dbtest.CloseDatabase(db) })

	b.Logf(
		"Loaded %d predecoded blocks for near-tip blockfetch testing",
		len(blocks),
	)

	b.ResetTimer()

	processedBlocks := 0
	blockIdx := 0
	for i := 0; b.Loop(); i++ {
		if blockIdx == len(blocks) {
			b.StopTimer()
			if err := dbtest.CloseDatabase(db); err != nil {
				b.Fatal(err)
			}
			db, ledgerState = newBlockProcessingBenchmarkLedgerState(
				b,
				seedModels,
			)
			blockIdx = 0
			b.StartTimer()
		}
		evt := BlockfetchEvent{
			Block: blocks[blockIdx],
			Point: points[blockIdx],
			Type:  uint(blocks[blockIdx].Type()),
		}
		blockIdx++

		if err := handleEventBlockfetchBlockDeferred(ledgerState, evt, nil); err != nil {
			b.Fatalf("handleEventBlockfetchBlock failed: %v", err)
		}
		if err := ledgerState.flushPendingBlockfetchBlocksDeferred(nil); err != nil {
			b.Fatalf("flushPendingBlockfetchBlocks failed: %v", err)
		}

		processedBlocks++
	}

	b.ReportMetric(float64(processedBlocks)/b.Elapsed().Seconds(), "blocks/sec")
}

// BenchmarkBlockfetchNearTipFlushOnlyPredecoded isolates the one-block near-tip
// flush path after blockfetch has already validated and queued the block.
func BenchmarkBlockfetchNearTipFlushOnlyPredecoded(b *testing.B) {
	seedModels, rawBlocks := loadBlockProcessingFixture(b)
	blocks := make([]ledger.Block, 0, len(rawBlocks))
	points := make([]ocommon.Point, 0, len(rawBlocks))
	for _, block := range rawBlocks {
		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			b.Fatalf("predecode block: %v", err)
		}
		blocks = append(blocks, ledgerBlock)
		points = append(points, ocommon.NewPoint(block.Slot, block.Hash))
	}

	db, ledgerState := newBlockProcessingBenchmarkLedgerState(
		b,
		seedModels,
	)
	b.Cleanup(func() { dbtest.CloseDatabase(db) })

	b.Logf(
		"Loaded %d predecoded blocks for near-tip flush-only testing",
		len(blocks),
	)

	b.ResetTimer()

	processedBlocks := 0
	blockIdx := 0
	for i := 0; b.Loop(); i++ {
		if blockIdx == len(blocks) {
			b.StopTimer()
			if err := dbtest.CloseDatabase(db); err != nil {
				b.Fatal(err)
			}
			db, ledgerState = newBlockProcessingBenchmarkLedgerState(
				b,
				seedModels,
			)
			blockIdx = 0
			b.StartTimer()
		}
		ledgerState.pendingBlockfetchEvents = append(
			ledgerState.pendingBlockfetchEvents[:0],
			BlockfetchEvent{
				Block: blocks[blockIdx],
				Point: points[blockIdx],
				Type:  uint(blocks[blockIdx].Type()),
			},
		)
		blockIdx++

		if err := ledgerState.flushPendingBlockfetchBlocksDeferred(nil); err != nil {
			b.Fatalf("flushPendingBlockfetchBlocks failed: %v", err)
		}

		processedBlocks++
	}

	b.ReportMetric(float64(processedBlocks)/b.Elapsed().Seconds(), "blocks/sec")
}

// BenchmarkBlockfetchNearTipQueuedHeaderPredecoded measures the near-tip
// blockfetch path when the matching header has already been queued, which is
// the common live-sync case before full block arrival.
func BenchmarkBlockfetchNearTipQueuedHeaderPredecoded(b *testing.B) {
	seedModels, rawBlocks := loadBlockProcessingFixture(b)
	blocks := make([]ledger.Block, 0, len(rawBlocks))
	points := make([]ocommon.Point, 0, len(rawBlocks))
	for _, block := range rawBlocks {
		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			b.Fatalf("predecode block: %v", err)
		}
		blocks = append(blocks, ledgerBlock)
		points = append(points, ocommon.NewPoint(block.Slot, block.Hash))
	}

	db, ledgerState := newBlockProcessingBenchmarkLedgerState(
		b,
		seedModels,
	)
	b.Cleanup(func() { dbtest.CloseDatabase(db) })

	b.Logf(
		"Loaded %d predecoded blocks for queued-header near-tip testing",
		len(blocks),
	)

	b.ResetTimer()

	processedBlocks := 0
	blockIdx := 0
	for i := 0; b.Loop(); i++ {
		if blockIdx == len(blocks) {
			b.StopTimer()
			if err := dbtest.CloseDatabase(db); err != nil {
				b.Fatal(err)
			}
			db, ledgerState = newBlockProcessingBenchmarkLedgerState(
				b,
				seedModels,
			)
			blockIdx = 0
			b.StartTimer()
		}
		block := blocks[blockIdx]
		point := points[blockIdx]
		blockIdx++

		if err := ledgerState.chain.AddBlockHeader(block.Header()); err != nil {
			b.Fatalf("AddBlockHeader failed: %v", err)
		}

		evt := BlockfetchEvent{
			Block: block,
			Point: point,
			Type:  uint(block.Type()),
		}

		if err := handleEventBlockfetchBlockDeferred(ledgerState, evt, nil); err != nil {
			b.Fatalf("handleEventBlockfetchBlock failed: %v", err)
		}
		if err := ledgerState.flushPendingBlockfetchBlocksDeferred(nil); err != nil {
			b.Fatalf("flushPendingBlockfetchBlocks failed: %v", err)
		}

		processedBlocks++
	}

	b.ReportMetric(float64(processedBlocks)/b.Elapsed().Seconds(), "blocks/sec")
}

// BenchmarkVerifyBlockHeader isolates the cryptographic header
// verification path used by blockfetch near tip.
func BenchmarkVerifyBlockHeader(b *testing.B) {
	const blockCount = 32

	testBlocks := make([]*testBlockResult, 0, blockCount)
	for i := range blockCount {
		var seed [32]byte
		seed[0] = byte(i + 1)
		testBlocks = append(testBlocks, createTestBlock(b, seed, 0, tamperNone))
	}

	epoch := models.Epoch{
		EpochId:       0,
		StartSlot:     0,
		LengthInSlots: 1000,
		Nonce:         slices.Clone(testBlocks[0].epochNonce),
	}
	epochNonceHex := hex.EncodeToString(testBlocks[0].epochNonce)

	b.Run("direct", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; b.Loop(); i++ {
			if err := verifyBlockHeaderHex(
				testBlocks[i%len(testBlocks)].block,
				epochNonceHex,
				testBlocks[0].slotsPerKesPeriod,
			); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("ledger_state", func(b *testing.B) {
		ledgerState := &LedgerState{
			epochCache: []models.Epoch{epoch},
			currentEpoch: models.Epoch{
				EpochId:       epoch.EpochId,
				StartSlot:     epoch.StartSlot,
				LengthInSlots: epoch.LengthInSlots,
				Nonce:         slices.Clone(epoch.Nonce),
			},
			config: LedgerStateConfig{
				CardanoNodeConfig: newTestShelleyGenesisCfg(b),
				Logger:            benchmarkDiscardLogger,
			},
			epochNonceHexCache: make(map[uint64]epochNonceHexCacheEntry),
		}
		// The epoch cache is read through the published consensus snapshot,
		// not the raw field, so it must be published before use even for
		// this single-threaded literal construction.
		ledgerState.publishSnapshotsLocked()

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; b.Loop(); i++ {
			// verifyBlockHeaderStatelessCrypto isolates the cryptographic
			// path this benchmark measures. Keep epoch cache advancement
			// disabled because it also requires ls.db, which this literal
			// LedgerState intentionally does not provide. Likewise,
			// verifyBlockHeaderCrypto runs verifyBlockHeaderState, which
			// looks up the pool's registered VRF key and stake snapshot
			// through ls.db.
			if _, err := ledgerState.verifyBlockHeaderStatelessCrypto(
				testBlocks[i%len(testBlocks)].block,
				false,
			); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkBlockfetchVerifiedHeaderDispatch measures the near-tip blockfetch
// handler when the fetched block matches a header that chainsync already
// verified and queued. This is the common BP path where duplicate VRF/KES
// verification should be avoidable.
func BenchmarkBlockfetchVerifiedHeaderDispatch(b *testing.B) {
	testBlock := createTestBlock(b, [32]byte{0x42}, 0, tamperNone)

	db, err := dbtest.NewDatabaseWithOptions(b, dbtest.Options{
		Config: &database.Config{
			DataDir: "",
			Logger:  benchmarkDiscardLogger,
		},
		// Serve mode selects the compact-block-metadata storage path this
		// near-tip dispatch benchmark is meant to measure.
		RunMode: "serve",
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = dbtest.CloseDatabase(db) })

	chainManager, err := chain.NewManager(db, nil)
	if err != nil {
		b.Fatal(err)
	}
	ledgerState, err := NewLedgerState(LedgerStateConfig{
		Database:           db,
		ChainManager:       chainManager,
		CardanoNodeConfig:  newTestShelleyGenesisCfg(b),
		ValidateHistorical: true,
		Logger:             benchmarkDiscardLogger,
	})
	if err != nil {
		b.Fatal(err)
	}
	epoch := models.Epoch{
		EpochId:       0,
		StartSlot:     0,
		LengthInSlots: 1000,
		Nonce:         slices.Clone(testBlock.epochNonce),
	}
	ledgerState.currentEpoch = epoch
	ledgerState.epochCache = []models.Epoch{epoch}
	ledgerState.publishSnapshotsLocked()

	if err := ledgerState.chain.AddBlockHeader(testBlock.block.Header()); err != nil {
		b.Fatalf("seed header queue: %v", err)
	}

	evt := BlockfetchEvent{
		Block: testBlock.block,
		Point: ocommon.NewPoint(
			testBlock.block.SlotNumber(),
			testBlock.block.Header().Hash().Bytes(),
		),
		Type: uint(testBlock.block.Type()),
	}
	if testBlock.block.Header().SlotNumber() != evt.Point.Slot ||
		!slices.Equal(testBlock.block.Header().Hash().Bytes(), evt.Point.Hash) {
		b.Fatal("seeded header does not match benchmark block point")
	}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if err := handleEventBlockfetchBlockDeferred(ledgerState, evt, nil); err != nil {
			b.Fatal(err)
		}
		ledgerState.pendingBlockfetchEvents = ledgerState.pendingBlockfetchEvents[:0]
	}
}

// BenchmarkBlockProcessingThroughputPredecoded measures block-processing
// throughput with block CBOR decoding removed from the timed region.
func BenchmarkBlockProcessingThroughputPredecoded(b *testing.B) {
	seedModels, rawBlocks := loadBlockProcessingFixture(b)
	blocks := make([]ledger.Block, 0, len(rawBlocks))
	for _, block := range rawBlocks {
		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			b.Fatalf("predecode block: %v", err)
		}
		blocks = append(blocks, ledgerBlock)
	}

	db, ledgerState := newBlockProcessingBenchmarkLedgerState(
		b,
		seedModels,
	)
	b.Cleanup(func() { dbtest.CloseDatabase(db) })

	b.Logf("Loaded %d predecoded blocks for throughput testing", len(blocks))

	b.ResetTimer()

	processedBlocks := 0
	blockIdx := 0
	for i := 0; b.Loop(); i++ {
		if blockIdx == len(blocks) {
			b.StopTimer()
			if err := dbtest.CloseDatabase(db); err != nil {
				b.Fatal(err)
			}
			db, ledgerState = newBlockProcessingBenchmarkLedgerState(
				b,
				seedModels,
			)
			blockIdx = 0
			b.StartTimer()
		}
		block := blocks[blockIdx]
		blockIdx++
		txn := db.BlobTxn(true)

		if err := ledgerState.Chain().AddBlock(block, txn); err != nil {
			_ = txn.Rollback()
			b.Fatalf("AddBlock failed: %v", err)
		}
		if err := txn.Commit(); err != nil {
			_ = txn.Rollback()
			b.Fatalf("Failed to commit transaction: %v", err)
		}

		processedBlocks++
	}

	b.ReportMetric(float64(processedBlocks)/b.Elapsed().Seconds(), "blocks/sec")
}

// BenchmarkBlockBatchProcessingThroughput measures batched block-processing
// throughput using Chain.AddBlocks, which matches the normal immutable-load
// path more closely than per-block AddBlock.
func BenchmarkBlockBatchProcessingThroughput(b *testing.B) {
	seedModels, batchBlocks, _, batchSize := loadBatchProcessingFixture(b)

	b.ResetTimer()

	processedBlocks := 0
	for b.Loop() {
		b.StopTimer()
		db, ledgerState := newBatchBenchmarkLedgerState(b, seedModels)
		b.StartTimer()
		if err := ledgerState.Chain().AddBlocks(batchBlocks); err != nil {
			_ = dbtest.CloseDatabase(db)
			b.Fatal(err)
		}
		b.StopTimer()
		if err := dbtest.CloseDatabase(db); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		processedBlocks += batchSize
	}

	b.ReportMetric(float64(processedBlocks)/b.Elapsed().Seconds(), "blocks/sec")
}

// BenchmarkRawBlockBatchProcessingThroughput measures batched header-only
// processing throughput using Chain.AddRawBlocks, matching the optimized
// load path used during immutable imports.
func BenchmarkRawBlockBatchProcessingThroughput(b *testing.B) {
	seedModels, _, rawBlocks, batchSize := loadBatchProcessingFixture(b)

	b.ResetTimer()

	processedBlocks := 0
	for b.Loop() {
		b.StopTimer()
		db, ledgerState := newBatchBenchmarkLedgerState(b, seedModels)
		b.StartTimer()
		if err := ledgerState.Chain().AddRawBlocks(rawBlocks); err != nil {
			_ = dbtest.CloseDatabase(db)
			b.Fatal(err)
		}
		b.StopTimer()
		if err := dbtest.CloseDatabase(db); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		processedBlocks += batchSize
	}

	b.ReportMetric(float64(processedBlocks)/b.Elapsed().Seconds(), "blocks/sec")
}

func loadBatchProcessingFixture(
	b *testing.B,
) ([]models.Block, []ledger.Block, []chain.RawBlock, int) {
	b.Helper()

	immDb := openImmutableTestDB(b)
	iterator, err := immDb.BlocksFromPoint(ocommon.NewPoint(0, nil))
	if err != nil {
		b.Fatal(err)
	}
	defer iterator.Close()

	const seedCount = 5
	const batchSize = 50

	seedModels := make([]models.Block, 0, seedCount)
	for len(seedModels) < seedCount {
		block, err := iterator.Next()
		if err != nil {
			b.Fatal(err)
		}
		if block == nil {
			b.Skip("insufficient blocks available for batch benchmark seed")
		}
		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			continue
		}
		seedModels = append(seedModels, models.Block{
			ID:       uint64(len(seedModels) + 1),
			Slot:     block.Slot,
			Hash:     slices.Clone(block.Hash),
			Number:   0,
			Type:     uint(ledgerBlock.Type()),
			PrevHash: ledgerBlock.PrevHash().Bytes(),
			Cbor:     slices.Clone(block.Cbor),
		})
	}

	batchBlocks := make([]ledger.Block, 0, batchSize)
	rawBlocks := make([]chain.RawBlock, 0, batchSize)
	for len(batchBlocks) < batchSize {
		block, err := iterator.Next()
		if err != nil {
			b.Fatal(err)
		}
		if block == nil {
			b.Skip("insufficient blocks available for batch benchmark")
		}
		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			continue
		}
		batchBlocks = append(batchBlocks, ledgerBlock)
		rawBlocks = append(rawBlocks, chain.RawBlock{
			Slot:        ledgerBlock.SlotNumber(),
			Hash:        slices.Clone(block.Hash),
			BlockNumber: ledgerBlock.BlockNumber(),
			Type:        uint(block.Type),
			PrevHash:    ledgerBlock.PrevHash().Bytes(),
			Cbor:        slices.Clone(block.Cbor),
		})
	}

	return seedModels, batchBlocks, rawBlocks, batchSize
}

// blockNumberFollowsParent mirrors chain.blockNumberContiguous, which is
// unexported: a block number must be exactly parent+1, except a Byron-era
// epoch boundary block which legitimately repeats its parent's number.
func blockNumberFollowsParent(
	eraId uint8,
	blockNumber, parentNumber uint64,
) bool {
	if blockNumber == parentNumber+1 {
		return true
	}
	return eraId == byron.EraIdByron && blockNumber == parentNumber
}

func loadBlockProcessingFixture(
	b *testing.B,
) ([]models.Block, []*immutable.Block) {
	b.Helper()

	immDb := openImmutableTestDB(b)
	iterator, err := immDb.BlocksFromPoint(ocommon.NewPoint(0, nil))
	if err != nil {
		b.Fatal(err)
	}
	defer iterator.Close()

	const seedCount = 5
	const blockCount = blockProcessingBenchmarkFixtureBlockCount

	var prevHash []byte
	var prevNumber uint64
	var havePrev bool

	seedModels := make([]models.Block, 0, seedCount)
	for len(seedModels) < seedCount {
		block, err := iterator.Next()
		if err != nil {
			b.Fatal(err)
		}
		if block == nil {
			b.Skip(
				"insufficient blocks available for throughput benchmark seed",
			)
		}
		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			continue
		}
		blockNumber := ledgerBlock.BlockNumber()
		prevHashBytes := ledgerBlock.PrevHash().Bytes()
		if havePrev {
			if !bytes.Equal(prevHashBytes, prevHash) {
				b.Fatalf(
					"seed block %x has prev hash %x that does not match parent hash %x",
					block.Hash,
					prevHashBytes,
					prevHash,
				)
			}
			if !blockNumberFollowsParent(
				ledgerBlock.Era().Id,
				blockNumber,
				prevNumber,
			) {
				b.Fatalf(
					"seed block %x claims block number %d that is not contiguous with parent %d",
					block.Hash,
					blockNumber,
					prevNumber,
				)
			}
		}
		seedModels = append(seedModels, models.Block{
			ID:       uint64(len(seedModels) + 1),
			Slot:     block.Slot,
			Hash:     slices.Clone(block.Hash),
			Number:   blockNumber,
			Type:     uint(ledgerBlock.Type()),
			PrevHash: prevHashBytes,
			Cbor:     slices.Clone(block.Cbor),
		})
		prevHash = slices.Clone(block.Hash)
		prevNumber = blockNumber
		havePrev = true
	}

	blocks := make([]*immutable.Block, 0, blockCount)
	for len(blocks) < blockCount {
		block, err := iterator.Next()
		if err != nil {
			b.Fatal(err)
		}
		if block == nil {
			b.Skip("insufficient blocks available for throughput benchmark")
		}
		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			continue
		}
		blockNumber := ledgerBlock.BlockNumber()
		prevHashBytes := ledgerBlock.PrevHash().Bytes()
		if !bytes.Equal(prevHashBytes, prevHash) {
			b.Fatalf(
				"timed block %x has prev hash %x that does not match parent hash %x",
				block.Hash,
				prevHashBytes,
				prevHash,
			)
		}
		if !blockNumberFollowsParent(
			ledgerBlock.Era().Id,
			blockNumber,
			prevNumber,
		) {
			b.Fatalf(
				"timed block %x claims block number %d that is not contiguous with parent %d",
				block.Hash,
				blockNumber,
				prevNumber,
			)
		}
		tmpBlock := &immutable.Block{
			Slot: block.Slot,
			Type: block.Type,
			Cbor: slices.Clone(block.Cbor),
			Hash: slices.Clone(block.Hash),
		}
		blocks = append(blocks, tmpBlock)
		prevHash = slices.Clone(block.Hash)
		prevNumber = blockNumber
	}

	return seedModels, blocks
}

func newBatchBenchmarkLedgerState(
	b *testing.B,
	seedModels []models.Block,
) (*database.Database, *LedgerState) {
	b.Helper()

	db, err := dbtest.NewDatabase(b, &database.Config{DataDir: ""})
	if err != nil {
		b.Fatal(err)
	}
	for _, blockModel := range seedModels {
		tmpModel := blockModel
		if err := db.BlockCreate(tmpModel, nil); err != nil {
			_ = dbtest.CloseDatabase(db)
			b.Fatalf("seed batch benchmark block: %v", err)
		}
	}
	chainManager, err := chain.NewManager(db, nil)
	if err != nil {
		_ = dbtest.CloseDatabase(db)
		b.Fatal(err)
	}
	ledgerState, err := NewLedgerState(LedgerStateConfig{
		Database:     db,
		ChainManager: chainManager,
	})
	if err != nil {
		_ = dbtest.CloseDatabase(db)
		b.Fatal(err)
	}
	return db, ledgerState
}

func newBlockProcessingBenchmarkLedgerState(
	b *testing.B,
	seedModels []models.Block,
) (*database.Database, *LedgerState) {
	b.Helper()

	db, err := dbtest.NewDatabaseWithOptions(b, dbtest.Options{
		Config: &database.Config{
			DataDir: "",
			Logger:  benchmarkDiscardLogger,
		},
		// Serve mode selects the compact-block-metadata storage path used by
		// the block-processing benchmarks that build on this ledger state.
		RunMode: "serve",
	})
	if err != nil {
		b.Fatal(err)
	}
	for _, blockModel := range seedModels {
		tmpModel := blockModel
		if err := db.BlockCreate(tmpModel, nil); err != nil {
			_ = dbtest.CloseDatabase(db)
			b.Fatalf("seed block processing benchmark block: %v", err)
		}
	}
	chainManager, err := chain.NewManager(db, nil)
	if err != nil {
		_ = dbtest.CloseDatabase(db)
		b.Fatal(err)
	}
	ledgerState, err := NewLedgerState(LedgerStateConfig{
		Database:     db,
		ChainManager: chainManager,
	})
	if err != nil {
		_ = dbtest.CloseDatabase(db)
		b.Fatal(err)
	}
	return db, ledgerState
}

// BenchmarkConcurrentQueries measures database performance under concurrent query load
// using real Cardano testnet data - simulates multiple clients querying simultaneously
func BenchmarkConcurrentQueries(b *testing.B) {
	// Set up database with real data
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := dbtest.NewDatabase(b, config)
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(db)

	// Open immutable database
	immDb := openImmutableTestDB(b)

	// Seed the first ten fixture blocks. Requesting ten consecutive slots
	// instead would resolve onto one block, because the fixture's blocks are
	// 20 slots apart.
	seeded := seedBlocksAtPoints(
		b,
		db,
		immDb,
		fixturePointsFrom(b, immDb, 0, 10),
	)
	if seeded != 10 {
		b.Fatalf("seeded %d blocks; concurrent query benchmark requires 10", seeded)
	}
	refs := seedUtxosAtPoints(
		b,
		db,
		immDb,
		fixturePointsFrom(b, immDb, 0, 100),
		256,
	)
	addresses := make([]ledger.Address, 0, len(refs))
	for _, ref := range refs {
		addresses = append(addresses, ref.address)
	}
	addressRows, err := db.UtxosByAddress(
		addresses[:1], database.MaxUtxosByAddressResults, nil,
	)
	if err != nil || len(addressRows) == 0 {
		b.Fatalf("preflight address query returned %d rows: %v", len(addressRows), err)
	}
	refRow, err := db.UtxoByRef(refs[0].txID, refs[0].outputIdx, nil)
	if err != nil || refRow == nil {
		b.Fatalf("preflight UTxO reference query returned %v: %v", refRow, err)
	}
	blockRow, err := db.BlockByIndex(database.BlockInitialIndex, nil)
	if err != nil || len(blockRow.Hash) == 0 {
		b.Fatalf("preflight block query returned %v: %v", blockRow, err)
	}

	// Define different types of queries to run concurrently
	queryTypes := []string{
		"utxo_address",
		"utxo_ref",
		"block_retrieval",
	}

	// Number of concurrent workers
	numWorkers := 10

	// Set parallelism for concurrent execution
	b.SetParallelism(numWorkers)

	// Reset timer after setup
	b.ResetTimer()
	b.ReportMetric(float64(seeded), "fixture_blocks")
	b.ReportMetric(float64(len(refs)), "utxos")

	// Run benchmark with concurrent queries
	queryErrors := make(chan error, 1)
	recordQueryError := func(err error) {
		select {
		case queryErrors <- err:
		default:
		}
	}
	b.RunParallel(func(pb *testing.PB) {
		workerID := 0
		for pb.Next() {
			queryType := queryTypes[workerID%len(queryTypes)]

			switch queryType {
			case "utxo_address":
				res, err := db.UtxosByAddress(
					[]ledger.Address{addresses[workerID%len(addresses)]},
					database.MaxUtxosByAddressResults,
					nil,
				)
				if err != nil || len(res) == 0 {
					recordQueryError(
						fmt.Errorf("address query returned %d rows: %v", len(res), err),
					)
					return
				}

			case "utxo_ref":
				ref := refs[workerID%len(refs)]
				res, err := db.UtxoByRef(ref.txID, ref.outputIdx, nil)
				if err != nil || res == nil {
					recordQueryError(
						fmt.Errorf("reference query returned %v: %v", res, err),
					)
					return
				}

			case "block_retrieval":
				index := database.BlockInitialIndex + uint64(workerID%seeded)
				res, err := db.BlockByIndex(index, nil)
				if err != nil || len(res.Hash) == 0 {
					recordQueryError(
						fmt.Errorf("block query returned %v: %v", res, err),
					)
					return
				}
			}

			workerID++
		}
	})
	select {
	case err := <-queryErrors:
		b.Fatal(err)
	default:
	}

	// Report queries per second
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}

type storageModeBenchmarkBlock struct {
	block        ledger.Block
	point        ocommon.Point
	model        models.Block
	offsets      *database.BlockIngestionResult
	transactions []storageModeBenchmarkTx
	txCount      int
}

type storageModeBenchmarkTx struct {
	tx           lcommon.Transaction
	updateEpoch  uint64
	paramUpdates map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate
	certDeposits map[int]uint64
}

type storageModeBenchmarkFixture struct {
	seedBlocks    []storageModeBenchmarkBlock
	measureBlocks []storageModeBenchmarkBlock
}

func storageModeBenchmarkCanIngestBlock(
	db *database.Database,
	block storageModeBenchmarkBlock,
) (bool, error) {
	for _, tx := range block.block.Transactions() {
		skipInputs := func(inputs []lcommon.TransactionInput) bool {
			for _, input := range inputs {
				if input.Id().
					String() ==
					storageModeBenchmarkSkippedInputHash &&
					input.Index() == storageModeBenchmarkSkippedInputIndex {
					return true
				}
			}
			return false
		}

		if skipInputs(tx.Inputs()) ||
			skipInputs(tx.Collateral()) ||
			skipInputs(tx.ReferenceInputs()) ||
			skipInputs(tx.Consumed()) {
			return false, nil
		}

		checkInputs := func(
			inputs []lcommon.TransactionInput,
			includeSpent bool,
		) (bool, error) {
			for _, input := range inputs {
				var err error
				if includeSpent {
					_, err = db.UtxoByRefIncludingSpent(
						input.Id().Bytes(),
						input.Index(),
						nil,
					)
				} else {
					_, err = db.UtxoByRef(input.Id().Bytes(), input.Index(), nil)
				}
				if err == nil {
					continue
				}
				if errors.Is(err, database.ErrUtxoNotFound) {
					return false, nil
				}
				return false, err
			}
			return true, nil
		}

		ok, err := checkInputs(tx.Inputs(), false)
		if !ok || err != nil {
			return ok, err
		}
		ok, err = checkInputs(tx.Collateral(), false)
		if !ok || err != nil {
			return ok, err
		}
		ok, err = checkInputs(tx.ReferenceInputs(), false)
		if !ok || err != nil {
			return ok, err
		}
		ok, err = checkInputs(tx.Consumed(), true)
		if !ok || err != nil {
			return ok, err
		}
	}
	return true, nil
}

func loadStorageModeBenchmarkFixture(
	b *testing.B,
	maxBlocks int,
	seedTargetTxs int,
	measureTargetTxs int,
) storageModeBenchmarkFixture {
	b.Helper()

	immDb := openImmutableTestDB(b)
	iterator, err := immDb.BlocksFromPoint(
		ocommon.NewPoint(storageModeBenchmarkStartSlot, nil),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer iterator.Close()

	ret := storageModeBenchmarkFixture{
		seedBlocks:    make([]storageModeBenchmarkBlock, 0, maxBlocks),
		measureBlocks: make([]storageModeBenchmarkBlock, 0, maxBlocks),
	}
	fixtureDb, err := dbtest.NewDatabase(b, &database.Config{
		DataDir:     "",
		Logger:      benchmarkDiscardLogger,
		StorageMode: types.StorageModeCore,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer dbtest.CloseDatabase(fixtureDb)
	seededTxs := 0
	measuredTxs := 0
	for len(ret.seedBlocks)+len(ret.measureBlocks) < maxBlocks &&
		measuredTxs < measureTargetTxs {
		immBlock, err := iterator.Next()
		if err != nil {
			b.Fatal(err)
		}
		if immBlock == nil {
			break
		}

		ledgerBlock, err := ledger.NewBlockFromCbor(
			immBlock.Type,
			immBlock.Cbor,
		)
		if err != nil {
			continue
		}
		point := ocommon.NewPoint(immBlock.Slot, immBlock.Hash)
		indexer := database.NewBlockIndexer(point.Slot, point.Hash)
		offsets, err := indexer.ComputeOffsets(immBlock.Cbor, ledgerBlock)
		if err != nil {
			b.Fatalf("compute offsets for slot %d: %v", point.Slot, err)
		}

		txs := ledgerBlock.Transactions()
		txCount := len(txs)
		if txCount == 0 {
			continue
		}
		benchmarkTxs := make([]storageModeBenchmarkTx, 0, txCount)
		for _, tx := range txs {
			updateEpoch, paramUpdates := tx.ProtocolParameterUpdates()
			certs := tx.Certificates()
			var certDeposits map[int]uint64
			if len(certs) > 0 {
				certDeposits = make(map[int]uint64, len(certs))
				for i := range certs {
					certDeposits[i] = 0
				}
			}
			benchmarkTxs = append(benchmarkTxs, storageModeBenchmarkTx{
				tx:           tx,
				updateEpoch:  updateEpoch,
				paramUpdates: paramUpdates,
				certDeposits: certDeposits,
			})
		}
		blockData := storageModeBenchmarkBlock{
			block: ledgerBlock,
			point: point,
			model: models.Block{
				ID: uint64(
					len(ret.seedBlocks) + len(ret.measureBlocks) + 1,
				),
				Slot:     point.Slot,
				Hash:     point.Hash,
				Number:   ledgerBlock.BlockNumber(),
				Type:     uint(ledgerBlock.Type()),
				PrevHash: ledgerBlock.PrevHash().Bytes(),
				Cbor:     ledgerBlock.Cbor(),
			},
			offsets:      offsets,
			transactions: benchmarkTxs,
			txCount:      txCount,
		}

		if seededTxs < seedTargetTxs {
			ret.seedBlocks = append(ret.seedBlocks, blockData)
			seededTxs += txCount
			if _, err := ingestStorageModeBenchmarkBlocks(
				fixtureDb,
				[]storageModeBenchmarkBlock{blockData},
			); err != nil {
				b.Fatalf(
					"seed storage mode fixture block at slot %d: %v",
					point.Slot,
					err,
				)
			}
			continue
		}
		ok, err := storageModeBenchmarkCanIngestBlock(fixtureDb, blockData)
		if err != nil {
			b.Fatalf(
				"preflight storage mode fixture block at slot %d: %v",
				point.Slot,
				err,
			)
		}
		if !ok {
			continue
		}
		ret.measureBlocks = append(ret.measureBlocks, blockData)
		measuredTxs += txCount
		if _, err := ingestStorageModeBenchmarkBlocks(
			fixtureDb,
			[]storageModeBenchmarkBlock{blockData},
		); err != nil {
			b.Fatalf(
				"ingest storage mode fixture block at slot %d: %v",
				point.Slot,
				err,
			)
		}
	}
	if len(ret.measureBlocks) == 0 {
		b.Skip("no blocks available for storage mode benchmark")
	}
	return ret
}

func ingestStorageModeBenchmarkBlocks(
	db *database.Database,
	blocks []storageModeBenchmarkBlock,
) (int, error) {
	txn := db.Transaction(true)
	defer txn.Rollback() //nolint:errcheck

	totalTxs := 0
	for _, blockData := range blocks {
		if err := db.BlockCreate(blockData.model, txn); err != nil {
			return totalTxs, fmt.Errorf(
				"BlockCreate slot %d: %w", blockData.point.Slot, err,
			)
		}
		for txIdx, txData := range blockData.transactions {
			if err := db.SetTransaction(
				txData.tx,
				blockData.point,
				uint32(txIdx),
				txData.updateEpoch,
				txData.paramUpdates,
				txData.certDeposits,
				blockData.offsets,
				txn,
			); err != nil {
				return totalTxs, fmt.Errorf(
					"SetTransaction slot %d tx %d: %w",
					blockData.point.Slot, txIdx, err,
				)
			}
			totalTxs++
		}
	}

	if err := txn.Commit(); err != nil {
		return totalTxs, fmt.Errorf(
			"Commit after %d txs: %w", totalTxs, err,
		)
	}
	return totalTxs, nil
}

// BenchmarkStorageModeIngest compares real block ingestion in core and api
// storage modes using the same block batch and offset computation path.
func BenchmarkStorageModeIngest(b *testing.B) {
	fixture := loadStorageModeBenchmarkFixture(b, 300, 500, 200)
	modeNames := []string{types.StorageModeCore, types.StorageModeAPI}

	for _, mode := range modeNames {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()

			totalBlocks := 0
			totalTxs := 0
			for b.Loop() {
				b.StopTimer()
				db, err := dbtest.NewDatabase(b, &database.Config{
					DataDir:     "",
					Logger:      benchmarkDiscardLogger,
					StorageMode: mode,
				})
				if err != nil {
					b.Fatal(err)
				}
				if _, err := ingestStorageModeBenchmarkBlocks(
					db,
					fixture.seedBlocks,
				); err != nil {
					_ = dbtest.CloseDatabase(db)
					b.Fatalf("seed storage mode benchmark blocks: %v", err)
				}

				b.StartTimer()
				txCount, err := ingestStorageModeBenchmarkBlocks(
					db,
					fixture.measureBlocks,
				)

				b.StopTimer()
				closeErr := dbtest.CloseDatabase(db)
				if err != nil {
					b.Fatal(err)
				}
				if closeErr != nil {
					b.Fatal(closeErr)
				}
				b.StartTimer()

				totalBlocks += len(fixture.measureBlocks)
				totalTxs += txCount
			}

			b.ReportMetric(
				float64(totalBlocks)/b.Elapsed().Seconds(),
				"blocks_ingested/sec",
			)
			b.ReportMetric(
				float64(totalTxs)/b.Elapsed().Seconds(),
				"txs_ingested/sec",
			)
		})
	}
}

// BenchmarkStorageModeIngestSteadyState isolates ingest cost from DB-open and
// initial seeding overhead by reusing a single database per sub-benchmark and
// resetting state between iterations with a transaction rollback.
func BenchmarkStorageModeIngestSteadyState(b *testing.B) {
	fixture := loadStorageModeBenchmarkFixture(b, 300, 500, 200)
	modeNames := []string{types.StorageModeCore, types.StorageModeAPI}

	for _, mode := range modeNames {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()

			db, err := dbtest.NewDatabase(b, &database.Config{
				DataDir:     "",
				Logger:      benchmarkDiscardLogger,
				StorageMode: mode,
			})
			if err != nil {
				b.Fatal(err)
			}
			defer dbtest.CloseDatabase(db)

			if _, err := ingestStorageModeBenchmarkBlocks(db, fixture.seedBlocks); err != nil {
				b.Fatalf(
					"seed steady-state storage mode benchmark blocks: %v",
					err,
				)
			}

			totalBlocks := 0
			totalTxs := 0
			for b.Loop() {
				txn := db.Transaction(true)
				if txn == nil {
					b.Fatal("nil transaction")
				}

				txCount := 0
				for _, blockData := range fixture.measureBlocks {
					if err := db.BlockCreate(blockData.model, txn); err != nil {
						_ = txn.Rollback()
						b.Fatal(err)
					}
					for txIdx, txData := range blockData.transactions {
						if err := db.SetTransaction(
							txData.tx,
							blockData.point,
							uint32(txIdx),
							txData.updateEpoch,
							txData.paramUpdates,
							txData.certDeposits,
							blockData.offsets,
							txn,
						); err != nil {
							_ = txn.Rollback()
							b.Fatal(err)
						}
						txCount++
					}
				}
				b.StopTimer()
				if err := txn.Rollback(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				totalBlocks += len(fixture.measureBlocks)
				totalTxs += txCount
			}

			b.ReportMetric(
				float64(totalBlocks)/b.Elapsed().Seconds(),
				"blocks_ingested/sec",
			)
			b.ReportMetric(
				float64(totalTxs)/b.Elapsed().Seconds(),
				"txs_ingested/sec",
			)
		})
	}
}

// pausingLedgerReadIterator is a ledgerReadIterator whose Next calls block
// waiting on resume immediately before returning the result at index
// pauseAtIndex, closing paused first so a test can observe that the call is
// in flight. It exists to put ledgerReadChainIterator's gather loop in a
// known, held-open state -- "already fetched some raw blocks, about to
// fetch/hand off more" -- so a concurrent goroutine can probe whether
// blockPipelineGatherMutex is held during that window.
type pausingLedgerReadIterator struct {
	ctx                 context.Context
	results             []*chain.ChainIteratorResult
	pauseAtIdx          int
	calls               int
	paused              chan struct{}
	resume              chan struct{}
	blockingNextStarted chan struct{}
}

func (p *pausingLedgerReadIterator) Next(
	blocking bool,
) (*chain.ChainIteratorResult, error) {
	idx := p.calls
	p.calls++
	if idx == p.pauseAtIdx {
		close(p.paused)
		<-p.resume
	}
	if idx < len(p.results) {
		return p.results[idx], nil
	}
	if !blocking {
		return nil, chain.ErrIteratorChainTip
	}
	close(p.blockingNextStarted)
	// Mirror a real blocking iterator call: cancellation releases the call.
	// blockingNextStarted lets the test order cancellation after entry without
	// using a wall-clock delay as its sequencing mechanism.
	<-p.ctx.Done()
	return nil, p.ctx.Err()
}

// TestLedgerReadChainIteratorHoldsGatherMutexAcrossGather verifies that
// rollback coordination holds the gather mutex across iterator gathering:
// drainBlockPipelineBeforeRollback only waits for work
// already Submitted to blockPipeline, so a rollback landing while
// ledgerReadChainIterator has already pulled raw blocks off the chain
// iterator into its local batch, but has not yet reached decodeReadChainBatch
// (Submit), would previously go unnoticed -- WaitForDrain sees nothing
// pending and returns immediately.
//
// blockPipelineGatherMutex closes that window by having the reader hold its
// read lock for the whole gather-then-submit span. This test proves the
// reader actually holds it there (not just after Submit): while the
// scripted iterator's second Next call is deliberately paused -- i.e. one
// raw block already gathered, mid-way through gathering the next -- a
// concurrent TryLock for the write side (the lock rollbackChainAndStateDeferred
// takes) must fail. Once the reader delivers its batch and the mutex is no
// longer needed, TryLock must succeed.
func TestLedgerReadChainIteratorHoldsGatherMutexAcrossGather(t *testing.T) {
	t.Parallel()

	block1, point1 := buildDecodableTestBlock(t, 10, 1)
	block2, point2 := buildDecodableTestBlock(t, 20, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	iter := &pausingLedgerReadIterator{
		ctx: ctx,
		results: []*chain.ChainIteratorResult{
			{Point: point1, Block: block1},
			{Point: point2, Block: block2},
		},
		// Pause immediately before the second Next call, i.e. after the
		// first raw block has already been appended to the reader's
		// local batch and it is about to fetch more.
		pauseAtIdx:          1,
		paused:              make(chan struct{}),
		resume:              make(chan struct{}),
		blockingNextStarted: make(chan struct{}),
	}

	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	resultCh := make(chan readChainResult)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		ls.ledgerReadChainIterator(ctx, iter, resultCh)
	}()

	testutil.RequireReceive(
		t, iter.paused, testutil.AsyncWait,
		"reader never reached the paused mid-gather point",
	)

	// The reader is mid-gather with one raw block already collected. The
	// write-side lock rollbackChainAndStateDeferred takes must not be obtainable
	// right now.
	require.False(
		t,
		ls.blockPipelineGatherMutex.TryLock(),
		"blockPipelineGatherMutex.Lock() succeeded while the reader was "+
			"mid-gather -- a concurrent rollback could truncate the chain "+
			"while stale raw blocks are still about to be submitted",
	)

	close(iter.resume)

	result := testutil.RequireReceive(
		t, resultCh, testutil.AsyncWait,
		"reader never delivered its gathered batch",
	)
	require.False(t, result.rollback)
	require.Len(t, result.blocks, 2)

	// The gather-plus-submit span has ended; the write lock must now be
	// obtainable.
	require.Eventually(t, func() bool {
		if ls.blockPipelineGatherMutex.TryLock() {
			ls.blockPipelineGatherMutex.Unlock()
			return true
		}
		return false
	}, testutil.AsyncWait, 5*time.Millisecond,
		"blockPipelineGatherMutex remained held after the batch was "+
			"delivered",
	)

	close(result.done)
	testutil.RequireReceive(
		t, iter.blockingNextStarted, testutil.AsyncWait,
		"reader never entered the blocking iterator call",
	)
	cancel()
	testutil.RequireReceive(
		t, readerDone, testutil.AsyncWait,
		"ledgerReadChainIterator did not exit after cancellation",
	)
}

func TestBlockReferenceScriptLimitAdmission(t *testing.T) {
	for _, over := range []bool{false, true} {
		name := "at limit"
		if over {
			name = "over limit"
		}
		t.Run(name, func(t *testing.T) {
			db := newTestDB(t)
			address, err := lcommon.NewAddressFromParts(
				lcommon.AddressTypeKeyNone,
				0,
				bytes.Repeat([]byte{1}, 28),
				nil,
			)
			require.NoError(t, err)
			block := &conway.ConwayBlock{
				BlockHeader: &conway.ConwayBlockHeader{},
			}
			block.BlockHeader.Body.BlockNumber = 1
			block.BlockHeader.Body.Slot = 1
			block.BlockHeader.Body.ProtoVersion.Major = 11
			for i := range 6 {
				size := int(conway.MaxRefScriptSizePerBlock / 6)
				if i == 5 {
					size += int(conway.MaxRefScriptSizePerBlock % 6)
					if over {
						size++
					}
				}
				require.Less(t, uint64(size), conway.MaxRefScriptSizePerTx)
				txID := bytes.Repeat([]byte{byte(i + 1)}, 32)
				input := shelley.ShelleyTransactionInput{
					TxId: lcommon.NewBlake2b256(txID),
				}
				output := &babbage.BabbageTransactionOutput{
					OutputAddress: address,
					TxOutScriptRef: &lcommon.ScriptRef{
						Type:   lcommon.ScriptRefTypePlutusV3,
						Script: make(lcommon.PlutusV3Script, size),
					},
				}
				encoded, err := cbor.Encode(output)
				require.NoError(t, err)
				require.NoError(
					t,
					db.Transaction(true).Do(func(txn *database.Txn) error {
						if err := db.CreateUtxo(txn, &models.Utxo{TxId: txID, OutputIdx: 0, AddedSlot: 0}); err != nil {
							return err
						}
						return db.Blob().SetUtxo(txn.Blob(), txID, 0, encoded)
					}),
				)
				block.TransactionBodies = append(
					block.TransactionBodies,
					conway.ConwayTransactionBody{
						TxReferenceInputs: cbor.NewSetType(
							[]shelley.ShelleyTransactionInput{input},
							false,
						),
					},
				)
				block.TransactionWitnessSets = append(
					block.TransactionWitnessSets,
					conway.ConwayTransactionWitnessSet{},
				)
			}
			encodedBlock, err := cbor.EncodeGeneric(block)
			require.NoError(t, err)
			block.SetCbor(encodedBlock)
			bodySize, err := serializedBlockBodySize(block)
			require.NoError(t, err)
			block.BlockHeader.Body.BlockBodySize = bodySize
			encodedBlock, err = cbor.EncodeGeneric(block)
			require.NoError(t, err)
			block.SetCbor(encodedBlock)
			pp := &conway.ConwayProtocolParameters{
				MaxBlockBodySize:   100000,
				MaxBlockHeaderSize: 100000,
				ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
					Major: 11,
				},
			}
			sentinel := errors.New("transaction validator reached")
			era := eras.ConwayEraDesc
			era.ValidateTxFunc = func(lcommon.Transaction, uint64, lcommon.LedgerState, lcommon.ProtocolParameters) error {
				return sentinel
			}
			nodeConfig := newTestShelleyGenesisCfg(t)
			nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
			ls := &LedgerState{
				db:             db,
				activeEras:     []eras.EraDesc{era, eras.DijkstraEraDesc},
				currentEra:     era,
				currentPParams: pp,
				config: LedgerStateConfig{
					Logger:            testLogger(),
					CardanoNodeConfig: nodeConfig,
				},
			}
			ls.metrics.init(prometheus.NewRegistry())
			ls.publishSnapshotsLocked()
			for path, run := range map[string]func() error{
				"imported_previous_era": func() error {
					currentParams := &dijkstra.DijkstraProtocolParameters{
						ConwayProtocolParameters: *pp,
						MaxRefScriptSizePerBlock: 1,
					}
					currentParams.ProtocolVersion.Major = dijkstra.MinProtocolVersionDijkstra
					return db.Transaction(true).Do(func(txn *database.Txn) error {
						_, err := ls.ledgerProcessBlock(txn, ocommon.NewPoint(1, block.Hash().Bytes()), block, true, false, false, nil, envelopeParent{origin: true}, nil, eras.DijkstraEraDesc, currentParams, pp, 0, 0, false)
						return err
					})
				},
				"imported": func() error {
					return db.Transaction(true).Do(func(txn *database.Txn) error {
						_, err := ls.ledgerProcessBlock(txn, ocommon.NewPoint(1, block.Hash().Bytes()), block, true, false, false, nil, envelopeParent{origin: true}, nil, era, pp, nil, 0, 0, false)
						return err
					})
				},
				"forged": func() error { return ls.validateForgedTxs(block) },
			} {
				t.Run(path, func(t *testing.T) {
					err := run()
					if over {
						var limit lcommon.RefScriptSizePerBlockTooLargeError
						require.ErrorAs(
							t,
							err,
							&limit,
							"aggregate reference-script limit must reject before transaction validation",
						)
						require.Equal(
							t,
							conway.MaxRefScriptSizePerBlock+1,
							limit.BlockSize,
						)
					} else {
						require.ErrorIs(t, err, sentinel, "exact block limit must reach later transaction validation")
					}
				})
			}
		})
	}
}

// newChainUpdateEvent builds a throwaway chain.update event for saturating the
// bus in the deadlock regression below.
func newChainUpdateEvent() event.Event {
	return event.NewEvent(chain.ChainUpdateEventType, chain.ChainBlockEvent{})
}

// TestBlockfetchDrainDefersChainUpdatePastLedgerMutex is the regression guard
// for the chainsync/blockfetch drain deadlock (blinklabs-io/dingo preview
// freeze), rewritten to exercise the lane-saturation path the six-block test
// could not reach.
//
// The ledger drains fetched blocks via flushPendingBlockfetchBlocks while
// holding chainsyncBlockfetchMutex. If that drain publishes chain.update
// inline, a terminal chain.update subscriber that stops draining stalls the
// publish WITH the mutex held; handleEventChainsync then blocks acquiring the
// same mutex, the ledger.chainsync buffer fills, and the node deadlocks.
//
// This test puts the bus into the exact state that traps BOTH previously
// attempted inline publishers:
//
//   - a lossless chain.update subscriber whose one buffer slot is filled and
//     never drained, so a synchronous Publish (the original code) blocks; and
//   - the ordered chain.update lane filled to capacity, so a PublishOrdered
//     (the rejected under-lock fix) also blocks -- this is the saturation the
//     maintainer flagged, which a handful of blocks never reaches.
//
// With the bus in that state the drain must still return promptly, because the
// fix hands each block's chain.update back to the ledger's pendingPublishes to
// publish AFTER the mutex is released rather than publishing it inline. Both
// older approaches would block here and fail the timeout.
func TestBlockfetchDrainDefersChainUpdatePastLedgerMutex(t *testing.T) {
	t.Parallel()

	eventBus := event.NewEventBus(nil, nil)
	// Stop releases the goroutines parked on the stalled subscriber / full
	// lane at the end of the test. Run it under a bounded wait: if Stop's
	// shutdown path ever regresses and fails to release those parked
	// publishers, an unbounded t.Cleanup(eventBus.Stop) would hang the whole
	// `go test` binary in Stop with no diagnostic. Fail the test instead so
	// the regression is visible.
	t.Cleanup(func() {
		stopped := make(chan struct{})
		go func() {
			eventBus.Stop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(testutil.AsyncWait):
			t.Error(
				"eventBus.Stop did not return: it failed to " +
					"release the publishers parked on the stalled subscriber / " +
					"full ordered lane (shutdown backpressure-release regressed)",
			)
		}
	})

	// Terminal chain.update subscriber, buffer 1, deliberately never drained.
	// Lossless (SubscriberBackpressureBlock) means a full buffer blocks the
	// publisher forever rather than dropping -- the stalled-subscriber
	// condition behind the freeze.
	subId, ch := eventBus.SubscribeWithBufferPolicy(
		chain.ChainUpdateEventType,
		1,
		event.SubscriberBackpressureBlock,
	)
	require.NotZero(t, subId)
	require.NotNil(t, ch)

	// Fill the subscriber's single buffer slot so any further synchronous
	// Publish blocks. Confirm the stall is real: a second inline Publish must
	// not complete.
	eventBus.Publish(chain.ChainUpdateEventType, newChainUpdateEvent())
	directBlocked := make(chan struct{})
	go func() {
		eventBus.Publish(chain.ChainUpdateEventType, newChainUpdateEvent())
		close(directBlocked)
	}()
	select {
	case <-directBlocked:
		t.Fatal(
			"inline Publish did not block on the stalled subscriber; " +
				"the test cannot exercise the deadlock condition",
		)
	case <-time.After(500 * time.Millisecond):
	}

	// Saturate the ordered chain.update lane to capacity. The lane worker
	// parks on the stalled subscriber, so every enqueued event stays in the
	// lane; once it is full a PublishOrdered blocks too.
	go func() {
		for range event.OrderedQueueSize + 8 {
			eventBus.PublishOrdered(
				chain.ChainUpdateEventType,
				newChainUpdateEvent(),
			)
		}
	}()
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			50*time.Millisecond,
		)
		defer cancel()
		// A bounded publish that cannot enqueue reports false: the lane is
		// full.
		return !eventBus.PublishOrderedContext(
			ctx,
			chain.ChainUpdateEventType,
			newChainUpdateEvent(),
		)
	}, testutil.AsyncWait, 20*time.Millisecond,
		"ordered chain.update lane never reached capacity",
	)

	// Real primary chain wired to the saturated bus, plus a minimal ledger
	// state -- enough for the blockfetch drain path.
	cm, err := chain.NewManager(nil, eventBus)
	require.NoError(t, err)
	c := cm.PrimaryChain()
	require.NotNil(t, c)

	ls := &LedgerState{
		chain: c,
		config: LedgerStateConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: eventBus,
		},
	}

	blocks, err := testfixtures.GenerateConwayChain(1)
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	ls.pendingBlockfetchEvents = []BlockfetchEvent{
		{
			Block: blocks[0],
			Point: ocommon.Point{
				Slot: blocks[0].SlotNumber(),
				Hash: blocks[0].Hash().Bytes(),
			},
		},
	}

	// THE FIX. flushPendingBlockfetchBlocksDeferred runs on the mutex-holding
	// drain path. It must add the block and return promptly, queueing the
	// chain.update on pubs instead of publishing it into the saturated bus.
	// Under the original synchronous Publish, or the rejected under-lock
	// PublishOrdered, this call would block forever on the stalled subscriber
	// / full lane and the timeout below would fire.
	var pubs pendingPublishes
	drained := make(chan error, 1)
	go func() { drained <- ls.flushPendingBlockfetchBlocksDeferred(&pubs) }()
	select {
	case drainErr := <-drained:
		require.NoError(t, drainErr)
	case <-time.After(testutil.AsyncWait):
		t.Fatal(
			"flushPendingBlockfetchBlocks blocked under a saturated " +
				"chain.update lane: the block's chain.update must be deferred " +
				"past chainsyncBlockfetchMutex, not published inline",
		)
	}

	// The chain.update was deferred, not published: nothing reached the
	// saturated bus under the lock. It is no longer requeued onto pubs.events;
	// AddBlockWithPointDeferred enqueued it on the chain's shared sequencer
	// under c.mutex, and the drain registered the chain on pubs.chainDrains so
	// pubs.flush() publishes it (in chain-mutation order) after the mutex is
	// released. That the chain is registered but not yet drained here is the
	// deadlock-avoidance property: publication is deferred past the lock.
	require.Empty(
		t,
		pubs.events,
		"chain.update must not be requeued on the generic pending queue; it "+
			"lives on the chain's shared sequencer",
	)
	require.Equal(
		t,
		[]*chain.Chain{c},
		pubs.chainDrains,
		"the drain must register the chain so its sequencer is flushed after "+
			"the mutex is released",
	)
	// The block really was added to the chain (the drain did its job, it just
	// did not publish).
	require.Equal(t, blocks[0].SlotNumber(), c.Tip().Point.Slot)
}

// boundaryCreditPostSnapshot returns the AccountRewardDelta.PostSnapshot flag of
// the single credit journaled for a credential.
func boundaryCreditPostSnapshot(
	t *testing.T,
	db *sql.DB,
	credential []byte,
) bool {
	t.Helper()
	var postSnapshot bool
	require.NoError(t, db.QueryRow(`
SELECT post_snapshot FROM account_reward_delta
WHERE credential_tag = ? AND staking_key = ? AND withdrawal = FALSE`,
		0, credential).Scan(&postSnapshot))
	return postSnapshot
}

func insertBoundaryAccount(t *testing.T, db *sql.DB, credential []byte) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO account (staking_key, active, reward)
VALUES (?, TRUE, '0')`, credential)
	require.NoError(t, err)
}

// TestBoundaryCreditVisibility_MIRIsIncludedInSnapshot pins MIR credits as
// pre-SNAP.
//
// cardano-ledger's Shelley NEWEPOCH rule runs applyRUpd, then the MIR rule, then
// the EPOCH rule whose first sub-rule is SNAP. MIR credits are therefore part of
// the mark snapshot, so their journal rows must NOT be stamped PostSnapshot —
// an epoch-boundary reconstruction has to retain them exactly like the delayed
// reward update.
func TestBoundaryCreditVisibility_MIRIsIncludedInSnapshot(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newMIRTestLedger(t)

	const (
		epochStartSlot = uint64(100)
		boundarySlot   = uint64(200)
		amount         = uint64(70)
	)
	credential := mirCred28(0x51)
	insertBoundaryAccount(t, gdb, credential)
	require.NoError(t, db.Metadata().SetNetworkState(0, 1_000, 1, nil))
	seedMIRDistribution(
		t,
		gdb,
		0,
		epochStartSlot+1,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: credential, Amount: new(big.Int).SetUint64(amount)},
		},
	)

	runApplyMIRCerts(t, ls, db, epochStartSlot, boundarySlot)

	require.False(
		t,
		boundaryCreditPostSnapshot(t, gdb, credential),
		"MIR runs before SNAP in cardano-ledger, so its credit belongs in the mark snapshot",
	)
}

// TestBoundaryCreditVisibility_PoolReapIsExcludedFromSnapshot pins POOLREAP
// deposit refunds as post-SNAP: cardano-ledger's EPOCH rule runs SNAP before
// POOLREAP, so the refund is not part of the mark snapshot.
func TestBoundaryCreditVisibility_PoolReapIsExcludedFromSnapshot(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newPoolreapTestLedger(t)

	const (
		deposit      = uint64(500)
		newEpoch     = uint64(5)
		boundarySlot = uint64(2000)
	)
	rewardAccount := reapCred28(0x52)
	insertBoundaryAccount(t, gdb, rewardAccount)
	seedRetiringPool(
		t, gdb, reapCred28(0xB2), rewardAccount, deposit, 10, newEpoch, 20,
	)

	runApplyPoolRetirements(t, ls, db, newEpoch, boundarySlot)

	require.True(
		t,
		boundaryCreditPostSnapshot(t, gdb, rewardAccount),
		"POOLREAP runs after SNAP, so its refund must be excluded from the mark snapshot",
	)
}

// TestBoundaryCreditVisibility_StakeRewardIsIncludedInSnapshot pins the delayed
// reward update as pre-SNAP by exercising the crediting primitive it uses
// (Database.AddAccountRewardByCredential), which must leave the journal row
// unstamped.
func TestBoundaryCreditVisibility_StakeRewardIsIncludedInSnapshot(
	t *testing.T,
) {
	t.Parallel()

	_, db, gdb := newPoolreapTestLedger(t)

	credential := reapCred28(0x53)
	insertBoundaryAccount(t, gdb, credential)

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return db.AddAccountRewardByCredential(
			0, credential, 40, 200, make([]byte, 32), txn,
		)
	}))

	require.False(
		t,
		boundaryCreditPostSnapshot(t, gdb, credential),
		"the delayed reward update precedes SNAP and belongs in the mark snapshot",
	)
}

// loadByronGenesisForTest fills fields that make a fixture valid as Byron
// genesis while preserving fields the test is exercising.
func loadByronGenesisForTest(
	t testing.TB,
	cfg *cardano.CardanoNodeConfig,
	r io.Reader,
) error {
	t.Helper()

	raw, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	var genesis map[string]json.RawMessage
	if err := json.Unmarshal(raw, &genesis); err != nil {
		return err
	}
	protocolMagic := uint32(164)
	if protocolConsts, ok := genesis["protocolConsts"]; ok {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(protocolConsts, &fields); err != nil {
			return err
		}
		if magic, ok := fields["protocolMagic"]; ok {
			if err := json.Unmarshal(magic, &protocolMagic); err != nil {
				return err
			}
		}
	}
	issuer := newByronPBFTTestKey(0x31)
	delegate := newByronPBFTTestKey(0x32)
	issuerHash, err := byronconsensus.PBFTVerificationKeyHash(
		issuer.verificationKey,
	)
	if err != nil {
		return err
	}
	certificate := newSignedByronPBFTDelegationCertificate(
		t, protocolMagic, 0, issuer, delegate,
	)
	bootStakeholders, err := json.Marshal(map[string]uint64{
		issuerHash.String(): 1,
	})
	if err != nil {
		return err
	}
	heavyDelegation, err := json.Marshal(map[string]any{
		issuerHash.String(): map[string]any{
			"cert": hex.EncodeToString(certificate[3].([]byte)),
			"delegatePk": base64.StdEncoding.EncodeToString(
				delegate.verificationKey,
			),
			"issuerPk": base64.StdEncoding.EncodeToString(
				issuer.verificationKey,
			),
			"omega": 0,
		},
	})
	if err != nil {
		return err
	}
	defaults := map[string]json.RawMessage{
		"avvmDistr":        json.RawMessage(`{}`),
		"bootStakeholders": bootStakeholders,
		"heavyDelegation":  heavyDelegation,
		"nonAvvmBalances":  json.RawMessage(`{}`),
		"startTime":        json.RawMessage(`1788739200`),
		"blockVersionData": json.RawMessage(`{
			"heavyDelThd":"300000000000","maxBlockSize":"2000000",
			"maxHeaderSize":"2000000","maxProposalSize":"700",
			"maxTxSize":"4096","mpcThd":"20000000000000",
			"scriptVersion":0,"slotDuration":"20000",
			"softforkRule":{"initThd":"900000000000000","minThd":"600000000000000","thdDecrement":"50000000000000"},
			"txFeePolicy":{"multiplier":"43946000000","summand":"155381000000000"},
			"unlockStakeEpoch":"18446744073709551615","updateImplicit":"10000",
			"updateProposalThd":"100000000000000","updateVoteThd":"1000000000000"
		}`),
		"protocolConsts": json.RawMessage(`{"k":108,"protocolMagic":164}`),
	}
	for key, value := range defaults {
		if _, ok := genesis[key]; !ok {
			genesis[key] = value
		}
	}
	for key, nestedDefaults := range map[string]map[string]json.RawMessage{
		"blockVersionData": {
			"heavyDelThd": json.RawMessage(`"300000000000"`), "maxBlockSize": json.RawMessage(`"2000000"`),
			"maxHeaderSize": json.RawMessage(`"2000000"`), "maxProposalSize": json.RawMessage(`"700"`),
			"maxTxSize": json.RawMessage(`"4096"`), "mpcThd": json.RawMessage(`"20000000000000"`),
			"scriptVersion": json.RawMessage(`0`), "slotDuration": json.RawMessage(`"20000"`),
			"softforkRule":     json.RawMessage(`{"initThd":"900000000000000","minThd":"600000000000000","thdDecrement":"50000000000000"}`),
			"txFeePolicy":      json.RawMessage(`{"multiplier":"43946000000","summand":"155381000000000"}`),
			"unlockStakeEpoch": json.RawMessage(`"18446744073709551615"`), "updateImplicit": json.RawMessage(`"10000"`),
			"updateProposalThd": json.RawMessage(`"100000000000000"`), "updateVoteThd": json.RawMessage(`"1000000000000"`),
		},
		"protocolConsts": {"k": json.RawMessage(`108`), "protocolMagic": json.RawMessage(`164`)},
	} {
		var nested map[string]json.RawMessage
		if err := json.Unmarshal(genesis[key], &nested); err != nil {
			return err
		}
		if nested == nil {
			continue
		}
		for nestedKey, value := range nestedDefaults {
			if _, ok := nested[nestedKey]; !ok {
				nested[nestedKey] = value
			}
		}
		encoded, err := json.Marshal(nested)
		if err != nil {
			return err
		}
		genesis[key] = encoded
	}
	completed, err := json.Marshal(genesis)
	if err != nil {
		return err
	}
	return cfg.LoadByronGenesisFromReader(bytes.NewReader(completed))
}

const testByronGenesisJSON = `{
  "avvmDistr": {},
  "blockVersionData": {
    "heavyDelThd": "0", "maxBlockSize": "1",
    "maxHeaderSize": "1", "maxProposalSize": "1",
    "maxTxSize": "1", "mpcThd": "0", "scriptVersion": 0,
    "slotDuration": "20000",
    "softforkRule": {"initThd": "0", "minThd": "0", "thdDecrement": "0"},
    "txFeePolicy": {"multiplier": "0", "summand": "0"},
    "unlockStakeEpoch": "0", "updateImplicit": "0",
    "updateProposalThd": "0", "updateVoteThd": "0"
  },
  "protocolConsts": {"k": 432, "protocolMagic": 2},
  "startTime": 0, "bootStakeholders": {},
  "heavyDelegation": {}, "nonAvvmBalances": {}
}`

func testByronGenesisJSONForK(k uint64) string {
	return strings.Replace(
		testByronGenesisJSON,
		`"k": 432`,
		`"k": `+strconv.FormatUint(k, 10),
		1,
	)
}

func completeTestByronGenesisJSON(t testing.TB, input string) string {
	t.Helper()
	var base map[string]json.RawMessage
	var overrides map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(testByronGenesisJSON), &base))
	require.NoError(t, json.Unmarshal([]byte(input), &overrides))
	for key, value := range overrides {
		if key == "blockVersionData" || key == "protocolConsts" {
			var defaults map[string]json.RawMessage
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(base[key], &defaults))
			require.NoError(t, json.Unmarshal(value, &fields))
			maps.Copy(defaults, fields)
			merged, err := json.Marshal(defaults)
			require.NoError(t, err)
			base[key] = merged
			continue
		}
		base[key] = value
	}
	merged, err := json.Marshal(base)
	require.NoError(t, err)
	return string(merged)
}

// byronBlockTestKey is a VKWitness signing key and the Byron address it
// controls.
type byronBlockTestKey struct {
	private ed25519.PrivateKey
	xpub    []byte
	address lcommon.Address
}

func newByronBlockTestKey(t *testing.T, seedByte byte) byronBlockTestKey {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = seedByte
	}
	private := ed25519.NewKeyFromSeed(seed)
	public, ok := private.Public().(ed25519.PublicKey)
	require.True(t, ok)
	xpub := append(append([]byte{}, public...), make([]byte, 32)...)
	rootCbor, err := cbor.Encode([]any{
		uint64(lcommon.ByronAddressTypePubkey),
		[]any{uint64(lcommon.ByronAddressTypePubkey), xpub},
		cbor.RawMessage{0xa0},
	})
	require.NoError(t, err)
	digest := sha3.Sum256(rootCbor)
	root := lcommon.Blake2b224Hash(digest[:])
	address, err := lcommon.NewByronAddressFromParts(
		lcommon.ByronAddressTypePubkey,
		root.Bytes(),
		lcommon.ByronAddressAttributes{},
	)
	require.NoError(t, err)
	return byronBlockTestKey{private: private, xpub: xpub, address: address}
}

type byronBlockTestInput struct {
	txId  []byte
	index uint32
}

type byronBlockTestOutput struct {
	address lcommon.Address
	amount  uint64
}

// buildByronBlockTestTx assembles and decodes a real Byron transaction whose
// witnesses sign its body under protocolMagic, in signer order.
func buildByronBlockTestTx(
	t *testing.T,
	protocolMagic uint32,
	inputs []byronBlockTestInput,
	outputs []byronBlockTestOutput,
	attributes []byte,
	signers []byronBlockTestKey,
) *byron.ByronTransaction {
	t.Helper()
	wireInputs := make([]any, 0, len(inputs))
	for _, input := range inputs {
		inner, err := cbor.Encode([]any{input.txId, input.index})
		require.NoError(t, err)
		wireInputs = append(wireInputs, []any{0, cbor.WrappedCbor(inner)})
	}
	wireOutputs := make([]any, 0, len(outputs))
	for _, output := range outputs {
		addrCbor, err := cbor.Encode(&output.address)
		require.NoError(t, err)
		wireOutputs = append(
			wireOutputs,
			[]any{cbor.RawMessage(addrCbor), output.amount},
		)
	}
	if attributes == nil {
		attributes = []byte{0xa0}
	}
	body, err := cbor.Encode(
		[]any{wireInputs, wireOutputs, cbor.RawMessage(attributes)},
	)
	require.NoError(t, err)
	bodyHash := lcommon.Blake2b256Hash(body)
	magicCbor, err := cbor.Encode(protocolMagic)
	require.NoError(t, err)
	message := append([]byte{0x01}, magicCbor...)
	message = append(message, 0x58, 0x20)
	message = append(message, bodyHash[:]...)
	witnesses := make([]any, 0, len(signers))
	for _, signer := range signers {
		payload, err := cbor.Encode(
			[][]byte{signer.xpub, ed25519.Sign(signer.private, message)},
		)
		require.NoError(t, err)
		witnesses = append(
			witnesses,
			[]any{uint64(0), cbor.WrappedCbor(payload)},
		)
	}
	txCbor, err := cbor.Encode(
		[]any{cbor.RawMessage(body), witnesses},
	)
	require.NoError(t, err)
	tx, err := byron.NewByronTransactionFromCbor(txCbor)
	require.NoError(t, err)
	require.Equal(t, bodyHash, tx.WireId())
	return tx
}

// processByronReferenceRuleBlock applies one Byron block holding tx through
// ledgerProcessBlock with a real LedgerState, database and Byron genesis.
func processByronReferenceRuleBlock(
	t *testing.T,
	db *database.Database,
	nodeConfig *cardano.CardanoNodeConfig,
	tx lcommon.Transaction,
) error {
	t.Helper()
	pparams, err := eras.NewByronProtocolParametersFromGenesis(
		nodeConfig.ByronGenesis(),
	)
	require.NoError(t, err)
	return processByronBlockWithPParams(t, db, nodeConfig, tx, pparams)
}

// processByronBlockWithPParams applies one Byron block holding tx through
// ledgerProcessBlock, validating it against pparams as block application
// does with the parameters adopted for the block's epoch.
func processByronBlockWithPParams(
	t *testing.T,
	db *database.Database,
	nodeConfig *cardano.CardanoNodeConfig,
	tx lcommon.Transaction,
	pparams *eras.ByronProtocolParameters,
) error {
	t.Helper()
	ls := &LedgerState{
		db:         db,
		currentEra: eras.ByronEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	block := &envelopeTestBlock{
		header: &envelopeTestHeader{
			cbor:   []byte{0x80},
			slot:   1,
			number: 1,
			era:    byron.EraByron,
		},
		cbor: []byte{0x82, 0x80, 0x80},
		txs:  []lcommon.Transaction{tx},
	}
	var txHash [32]byte
	copy(txHash[:], tx.Hash().Bytes())
	offsets := &database.BlockIngestionResult{
		TxOffsets: map[[32]byte]database.CborOffset{
			txHash: {
				BlockSlot:  1,
				ByteLength: uint32(len(tx.Cbor())), // #nosec G115
			},
		},
		UtxoOffsets: make(map[database.UtxoRef]database.CborOffset),
	}
	for _, utxo := range tx.Produced() {
		offsets.UtxoOffsets[database.UtxoRef{
			TxId:      txHash,
			OutputIdx: utxo.Id.Index(),
		}] = database.CborOffset{BlockSlot: 1, ByteLength: 1}
	}
	return db.Transaction(true).Do(func(txn *database.Txn) error {
		_, err := ls.ledgerProcessBlock(
			txn,
			ocommon.Point{Slot: 1, Hash: block.Hash().Bytes()},
			block,
			true,
			false,
			false,
			nil,
			envelopeParent{origin: true},
			offsets,
			eras.ByronEraDesc,
			pparams,
			nil,
			0,
			0,
			false,
		)
		return err
	})
}

// TestLedgerProcessBlockByronReferenceRules drives real, correctly signed
// Byron transactions through block application for #4379, #4381, #4394,
// #4401 and #4405. The genesis supplies ppMaxTxSize, a zero fee policy and
// the mainnet protocol magic the witnesses sign under, so each case isolates
// one rule.
func TestLedgerProcessBlockByronReferenceRules(t *testing.T) {
	t.Parallel()

	nodeConfig := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		loadByronGenesisForTest(t, nodeConfig, strings.NewReader(`{
		"blockVersionData": {
			"slotDuration": "20000",
			"maxTxSize": "600",
			"txFeePolicy": {"summand": "0", "multiplier": "0"}
		},
		"protocolConsts": {"k": 2160, "protocolMagic": 764824073}
	}`)),
	)
	const protocolMagic = 764824073
	keyA := newByronBlockTestKey(t, 0x61)
	keyB := newByronBlockTestKey(t, 0x62)
	payTo := newByronBlockTestKey(t, 0x63).address

	tests := []struct {
		name  string
		build func(t *testing.T, db *database.Database) *byron.ByronTransaction
		check func(t *testing.T, err error)
	}{
		{
			name: "witnesses in input order are accepted",
			build: func(t *testing.T, db *database.Database) *byron.ByronTransaction {
				a := seedByronUtxoWithAmount(t, db, 0x01, keyA.address, 1_000)
				b := seedByronUtxoWithAmount(t, db, 0x02, keyB.address, 1_000)
				return buildByronBlockTestTx(t, protocolMagic,
					[]byronBlockTestInput{{a, 0}, {b, 0}},
					[]byronBlockTestOutput{{payTo, 1_500}},
					nil, []byronBlockTestKey{keyA, keyB})
			},
			check: func(t *testing.T, err error) { require.NoError(t, err) },
		},
		{
			name: "swapped witnesses are rejected",
			build: func(t *testing.T, db *database.Database) *byron.ByronTransaction {
				a := seedByronUtxoWithAmount(t, db, 0x01, keyA.address, 1_000)
				b := seedByronUtxoWithAmount(t, db, 0x02, keyB.address, 1_000)
				return buildByronBlockTestTx(t, protocolMagic,
					[]byronBlockTestInput{{a, 0}, {b, 0}},
					[]byronBlockTestOutput{{payTo, 1_500}},
					nil, []byronBlockTestKey{keyB, keyA})
			},
			check: func(t *testing.T, err error) {
				var wrongKey eras.WitnessWrongKeyByronError
				require.ErrorAs(t, err, &wrongKey)
			},
		},
		{
			name: "repeated input is accepted",
			build: func(t *testing.T, db *database.Database) *byron.ByronTransaction {
				x := seedByronUtxoWithAmount(t, db, 0x01, keyA.address, 1_000)
				return buildByronBlockTestTx(t, protocolMagic,
					[]byronBlockTestInput{{x, 0}, {x, 0}},
					[]byronBlockTestOutput{{payTo, 800}},
					nil, []byronBlockTestKey{keyA, keyA})
			},
			check: func(t *testing.T, err error) { require.NoError(t, err) },
		},
		{
			name: "input balance above maxLovelaceVal is rejected",
			build: func(t *testing.T, db *database.Database) *byron.ByronTransaction {
				a := seedByronUtxoWithAmount(t, db, 0x01, keyA.address, 30e15)
				b := seedByronUtxoWithAmount(t, db, 0x02, keyB.address, 20e15)
				return buildByronBlockTestTx(t, protocolMagic,
					[]byronBlockTestInput{{a, 0}, {b, 0}},
					[]byronBlockTestOutput{{payTo, 44e15}},
					nil, []byronBlockTestKey{keyA, keyB})
			},
			check: func(t *testing.T, err error) {
				var bound eras.LovelaceBoundByronError
				require.ErrorAs(t, err, &bound)
			},
		},
		{
			name: "transaction above ppMaxTxSize is rejected",
			build: func(t *testing.T, db *database.Database) *byron.ByronTransaction {
				a := seedByronUtxoWithAmount(t, db, 0x01, keyA.address, 1_000)
				outputs := make([]byronBlockTestOutput, 0, 20)
				for range 20 {
					outputs = append(outputs, byronBlockTestOutput{payTo, 1})
				}
				return buildByronBlockTestTx(t, protocolMagic,
					[]byronBlockTestInput{{a, 0}},
					outputs, nil, []byronBlockTestKey{keyA})
			},
			check: func(t *testing.T, err error) {
				var tooLarge eras.TxTooLargeByronError
				require.ErrorAs(t, err, &tooLarge)
			},
		},
		{
			name: "unknown transaction attributes at the limit are rejected",
			build: func(t *testing.T, db *database.Database) *byron.ByronTransaction {
				a := seedByronUtxoWithAmount(t, db, 0x01, keyA.address, 1_000)
				attrs, err := cbor.Encode(
					map[uint8][]byte{9: make([]byte, 128)},
				)
				require.NoError(t, err)
				return buildByronBlockTestTx(t, protocolMagic,
					[]byronBlockTestInput{{a, 0}},
					[]byronBlockTestOutput{{payTo, 900}},
					attrs, []byronBlockTestKey{keyA})
			},
			check: func(t *testing.T, err error) {
				var unknown eras.UnknownAttributesByronError
				require.ErrorAs(t, err, &unknown)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			tx := test.build(t, db)
			test.check(t, processByronReferenceRuleBlock(t, db, nodeConfig, tx))
		})
	}
}

// TestLedgerProcessBlockAllowsSyntheticByronBlocksWithPlaceholderCbor keeps
// structured test blocks out of the decoded-wire size-validation boundary.
// A placeholder Cbor value is not sufficient to apply Byron genesis limits;
// concrete gouroboros Byron blocks are covered by the envelope tests.
func TestLedgerProcessBlockAllowsSyntheticByronBlocksWithPlaceholderCbor(
	t *testing.T,
) {
	db := newTestDB(t)
	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	ls := &LedgerState{
		db:         db,
		currentEra: eras.ByronEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	block := &envelopeTestBlock{
		header: &envelopeTestHeader{
			cbor:   []byte{0x80},
			slot:   1,
			number: 1,
			era:    byron.EraByron,
		},
		cbor: []byte{0x82, 0x80, 0x80},
	}

	err := db.Transaction(true).Do(func(txn *database.Txn) error {
		_, err := ls.ledgerProcessBlock(
			txn,
			ocommon.Point{Slot: 1, Hash: block.Hash().Bytes()},
			block,
			true,
			false,
			false,
			nil,
			envelopeParent{origin: true},
			nil,
			eras.ByronEraDesc,
			&shelley.ShelleyProtocolParameters{},
			nil,
			0,
			0,
			false,
		)
		return err
	})
	require.NoError(t, err)
}

// TestByronAdoptedUpdateChangesSizeLimitsAtAdoptionPoint covers the #4378
// criterion that an adopted update's ppMaxBlockSize and ppMaxHeaderSize
// govern inbound regular-block validation from the adoption point. A real
// proposal is registered, voted, endorsed and stable through the update
// state; with k = 10 the epoch is 100 slots, so it is adopted by the first
// block of epoch 1. The block in the last slot of epoch 0 is measured against
// the limits before adoption and the first block of epoch 1 against the
// adopted ones, using the parameters block application takes from the state.
func TestByronAdoptedUpdateChangesSizeLimitsAtAdoptionPoint(t *testing.T) {
	t.Parallel()
	const (
		protocolMagic = uint32(44)
		securityParam = 10
		wide          = uint64(2_000_000)
	)
	issuer := newByronPBFTTestKey(0x91)
	delegate := newByronPBFTTestKey(0x92)
	certificate := newSignedByronPBFTDelegationCertificate(
		t, protocolMagic, 0, issuer, delegate,
	)
	template := loadRealByronMainBlock(t)
	newBlock := func(
		epoch, slot, number uint64,
		payload []byte,
		version byron.ByronBlockVersion,
	) *byron.ByronMainBlock {
		return newSignedByronPBFTBlockWithBody(
			t, template, protocolMagic, epoch, slot, number,
			lcommon.Blake2b256{}, issuer, delegate, certificate, nil,
			&byronPBFTBodyOverride{
				emptyTransactions: true,
				updatePayload:     payload,
				blockVersion:      &version,
			},
		)
	}
	current := byron.ByronBlockVersion{}
	adoptedVersion := byron.ByronBlockVersion{Minor: 1}
	lastBefore := newBlock(0, 99, 4, byronUpdatePayload(nil), current)
	firstAfter := newBlock(1, 0, 4, byronUpdatePayload(nil), current)
	blockSizes := [2]uint64{
		uint64(len(lastBefore.Cbor())), uint64(len(firstAfter.Cbor())),
	}
	headerSizes := [2]uint64{
		uint64(len(lastBefore.Header().Cbor())),
		uint64(len(firstAfter.Header().Cbor())),
	}
	lowest := func(sizes [2]uint64) uint64 { return min(sizes[0], sizes[1]) }
	highest := func(sizes [2]uint64) uint64 { return max(sizes[0], sizes[1]) }
	ptr := func(v uint64) *uint64 { return &v }

	tests := []struct {
		name string
		// genesis and adopted are {maxBlockSize, maxHeaderSize}.
		genesis, adopted [2]uint64
		// beforeErr and afterErr are the substrings the last block of
		// epoch 0 and the first block of epoch 1 must be rejected with, or
		// empty when they must be accepted.
		beforeErr, afterErr string
	}{
		{
			"lower the block limit below the block",
			[2]uint64{wide, wide},
			[2]uint64{blockSizes[1] - 1, wide},
			"", "exceeds maxBlockSize",
		},
		{
			"lower the block limit to exactly the block",
			[2]uint64{wide, wide},
			[2]uint64{blockSizes[1], wide},
			"", "",
		},
		{
			"lower the header limit below the header",
			[2]uint64{wide, wide},
			[2]uint64{wide, headerSizes[1] - 1},
			"", "exceeds maxHeaderSize",
		},
		{
			"lower the header limit to exactly the header",
			[2]uint64{wide, wide},
			[2]uint64{wide, headerSizes[1]},
			"", "",
		},
		{
			"raise the block limit to admit the block",
			[2]uint64{lowest(blockSizes) - 1, wide},
			[2]uint64{highest(blockSizes), wide},
			"exceeds maxBlockSize", "",
		},
		{
			"raise the header limit to admit the header",
			[2]uint64{wide, lowest(headerSizes) - 1},
			[2]uint64{wide, highest(headerSizes)},
			"exceeds maxHeaderSize", "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			nodeConfig := newGeneratedByronPBFTTestNodeConfig(
				t, protocolMagic, securityParam, issuer, delegate, certificate,
			)
			limits := &nodeConfig.ByronGenesis().BlockVersionData
			limits.MaxBlockSize = int(test.genesis[0])
			limits.MaxHeaderSize = int(test.genesis[1])
			// A transaction and a proposal must fit the smallest limits.
			limits.MaxTxSize = 100
			limits.MaxProposalSize = 700
			ls := &LedgerState{
				config: LedgerStateConfig{CardanoNodeConfig: nodeConfig},
			}
			ls.slotClock = NewSlotClock(
				newMockSlotTimeProvider(time.Unix(0, 0), time.Second, 100),
				DefaultSlotClockConfig(),
			)
			config, err := ls.byronPBFTConfig()
			require.NoError(t, err)
			genesisParams, err := ls.byronGenesisProtocolParameters()
			require.NoError(t, err)
			state, err := newByronPBFTState(config, genesisParams)
			require.NoError(t, err)
			state.update = state.update.Advance(0, 0)

			proposal, proposalId := newByronUpdateProposal(
				t, protocolMagic, delegate, [3]uint64{0, 1, 0},
				ptr(test.adopted[0]), ptr(test.adopted[1]),
			)
			vote := newByronUpdateVote(
				t, protocolMagic, delegate, proposalId, false,
			)
			// Header validation is off: one genesis key signs every block
			// here and would exceed the PBFT signature window. A rejected
			// update payload is then only logged, so the adopted version
			// asserted below is what shows the proposal registered.
			// Registered and confirmed in epoch 0, endorsed once confirmation
			// is 2k slots old, and stable for 4k slots by the next epoch.
			for _, block := range []*byron.ByronMainBlock{
				newBlock(0, 6, 1, byronUpdatePayload(proposal), current),
				newBlock(0, 7, 2, byronUpdatePayload(nil, vote), current),
				newBlock(0, 30, 3, byronUpdatePayload(nil), adoptedVersion),
			} {
				state, err = ls.advanceByronPBFTState(state, block, false)
				require.NoError(t, err)
			}
			require.Zero(t, state.update.AdoptedVersion().Minor)

			check := func(
				block *byron.ByronMainBlock,
				wantVersion uint16,
				wantErr string,
			) {
				t.Helper()
				next, err := ls.advanceByronPBFTState(state, block, false)
				require.NoError(t, err)
				require.Equal(t, wantVersion, next.update.AdoptedVersion().Minor)
				params, ok := byronBlockPParams(block, next, nil).(*eras.ByronProtocolParameters)
				require.True(t, ok)
				err = validateByronBlockSizes(block, params, nodeConfig)
				if wantErr == "" {
					require.NoError(t, err)
					return
				}
				require.ErrorContains(t, err, wantErr)
			}
			check(lastBefore, 0, test.beforeErr)
			check(firstAfter, 1, test.afterErr)
		})
	}
}

// TestCalculateEpochNonce_TPraosToPraosUsesSourceEpochStabilityWindow proves
// that the candidate-freeze cutoff at the Alonzo→Babbage epoch boundary is
// driven by the source epoch's protocol family (TPraos, 3k/f) — not the
// post-transition era (Babbage, Praos, 4k/f).
//
// State up to entering this rollover:
//
//   - currentEpoch = Alonzo (epoch 3, EraId=4, slots 225–299)
//   - currentEra   = Babbage (5)              ← already advanced by
//     applyHardForkTransition
//   - k=6, f=0.4 → TPraos stability window = 3k/f = 45 slots,
//     Praos stability window  = 4k/f = 60 slots
//   - Alonzo cutoff (TPraos): 225 + 75 - 45 = 255
//   - Alonzo cutoff (Praos):  225 + 75 - 60 = 240
//
// The test seeds two pre-stored block-nonce rows at slots 230 and 245:
//
//   - slot 230 is below both cutoffs (always contributes to candidate)
//   - slot 245 sits between the Praos cutoff (240) and the TPraos cutoff
//     (255). It should contribute to candidate iff the cutoff is taken
//     from the SOURCE epoch's era (Alonzo, TPraos)
//
// The fast path freezes candidate at the latest pre-cutoff block's stored
// nonce. So:
//
//   - Correct (source-era cutoff = 255): candidate = nonce(slot 245) =
//     0xab*32. The block at slot 245 is the latest block strictly before
//     255.
//   - Buggy   (post-transition cutoff = 240): candidate = nonce(slot 230)
//     = 0x99*32. The block at slot 245 is past the cutoff and does not
//     contribute, so the latest pre-cutoff block is slot 230.
//
// The test asserts the correct value. Today, calculateEpochNonce passes
// `currentEra.Id` (Babbage) into computeCandidateNonce — i.e. the
// post-transition era — which selects the Praos window and produces the
// buggy value. The fix is to pass `currentEpoch.EraId` (the source
// epoch's era) instead, matching what verify_header.go already does and
// what the function's own comment claims it does.
//
// This is the smaller of the two distinct VRF wedges in #2125: the bug
// only fires at TPraos→Praos boundaries (Alonzo→Babbage) because that's
// the only transition where the two stability-window formulas disagree.
// All other era boundaries within TPraos (Shelley→Allegra, Allegra→Mary,
// Mary→Alonzo) and within Praos (Babbage→Conway) use the same multiplier
// either way and therefore mask the bug.
func TestCalculateEpochNonce_TPraosToPraosUsesSourceEpochStabilityWindow(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	cfg := newAlonzoToBabbageStabilityCfg(t)

	// Deterministic distinct nonces for slots 230 and 245 so the buggy
	// vs correct candidate values are unambiguous.
	nonceAt230 := bytes.Repeat([]byte{0x99}, 32)
	nonceAt245 := bytes.Repeat([]byte{0xab}, 32)
	hashAt230 := bytes.Repeat([]byte{0x01}, 32)
	hashAt245 := bytes.Repeat([]byte{0x02}, 32)
	prevHashAt230 := bytes.Repeat([]byte{0x10}, 32)
	prevHashAt245 := bytes.Repeat([]byte{0x20}, 32)

	// Insert two Alonzo blocks (slot 230 before any cutoff, slot 245
	// between the Praos and TPraos cutoffs) into the blob store, plus
	// pre-stored block-nonce rows so the fast path can compute the
	// candidate without re-decoding CBOR.
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot: 230, Hash: hashAt230, PrevHash: prevHashAt230,
			Cbor:   []byte{0x80}, // empty CBOR array, never decoded by fast path
			Number: 1, Type: 4,   // Alonzo block type
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot: 245, Hash: hashAt245, PrevHash: prevHashAt245,
			Cbor:   []byte{0x80},
			Number: 2, Type: 4,
		}, txn); err != nil {
			return err
		}
		if err := db.SetBlockNonce(
			hashAt230, 230, nonceAt230, false, txn,
		); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAt245, 245, nonceAt245, false, txn,
		)
	}))

	// currentTipBlockNonce is intentionally left empty so the
	// resume-from-tip optimisation in computeEpochNonceForSlot
	// /calculateEpochNonce does not short-circuit and the candidate
	// is recomputed across the full epoch range from prevEvolvingNonce.
	ls := &LedgerState{
		db:         db,
		currentEra: eras.BabbageEraDesc,
		currentEpoch: models.Epoch{
			EpochId:             3,
			StartSlot:           225,
			LengthInSlots:       75,
			SlotLength:          1000,
			EraId:               eras.AlonzoEraDesc.Id,
			Nonce:               bytes.Repeat([]byte{0xee}, 32),
			EvolvingNonce:       bytes.Repeat([]byte{0xee}, 32),
			CandidateNonce:      bytes.Repeat([]byte{0xcc}, 32),
			LastEpochBlockNonce: bytes.Repeat([]byte{0xfa}, 32),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Drive the rollover that fires at the Alonzo→Babbage boundary.
	// processEpochRollover is called with currentEra already advanced
	// to Babbage (state.go:2457 passes eras.Eras[workingEraId] after
	// applyHardForkTransition). What we want to assert is that the
	// inner candidate-nonce computation still picks the cutoff slot
	// from the era of the epoch being CLOSED, not the era being
	// entered.
	var candidate []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		_, _, c, _, err := ls.calculateEpochNonce(
			txn,
			ls.currentEpoch.StartSlot+uint64(ls.currentEpoch.LengthInSlots),
			eras.BabbageEraDesc,
			ls.currentEpoch,
			nil,
		)
		candidate = c
		return err
	}))

	require.Equalf(
		t,
		hex.EncodeToString(nonceAt245),
		hex.EncodeToString(candidate),
		"candidate must freeze at the source epoch's TPraos cutoff "+
			"(slot 255), so the block at slot 245 is the latest "+
			"pre-cutoff contributor and its stored nonce becomes the "+
			"candidate. Got %x — that's the nonce at slot 230, which "+
			"means the freeze cutoff was computed against Babbage's "+
			"Praos window (240) instead of Alonzo's TPraos window "+
			"(255). #2125 Alonzo→Babbage VRF wedge.",
		candidate,
	)
}

// newAlonzoToBabbageStabilityCfg builds a CardanoNodeConfig with concrete
// k and f values that put the Praos cutoff (4k/f = 60) and the TPraos
// cutoff (3k/f = 45) on opposite sides of slot 245 inside an Alonzo epoch
// of length 75 starting at slot 225. The Shelley genesis hash is set so
// computeCandidateNonce's fall-back paths have something to decode.
func newAlonzoToBabbageStabilityCfg(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, loadByronGenesisForTest(t, cfg, strings.NewReader(`{
		"protocolConsts": {
			"k": 6,
			"protocolMagic": 42
		}
	}`)))
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"systemStart": "2026-01-01T00:00:00Z",
		"securityParam": 6,
		"activeSlotsCoeff": 0.4,
		"epochLength": 75,
		"slotLength": 1
	}`)))
	return cfg
}

// TestCalculateEpochNonce_PostMithrilBootstrapFreezesCandidateAtCutoff
// reproduces the persisted-state shape that issue #2128 reports after a
// Mithril bootstrap inside a Conway epoch:
//
//   - The bootstrap epoch row carries CandidateNonce == EvolvingNonce
//     because the snapshot was taken before the candidate-freeze cutoff
//     (psCandidateNonce in cardano-ledger tracks evolving until the
//     stability window closes).
//   - importTip wrote a block_nonce checkpoint at the snapshot tip slot
//     so the resume logic can find the seam between the imported tip
//     and post-import per-block accumulation.
//   - Post-import sync produced block_nonce rows for blocks past the
//     snapshot tip, including blocks straddling the freeze cutoff.
//
// The Conway→Conway rollover that closes the bootstrap epoch must:
//
//   - return candidateNonce frozen at the latest pre-cutoff block's
//     stored nonce (NOT the imported tip-time value), and
//   - return evolvingNonce equal to the last-block-of-epoch's stored
//     nonce.
//
// If the rollover instead returns the imported tip-time value as the
// candidate (i.e. it inherited prevEpoch.CandidateNonce without ever
// iterating past the cutoff), the next epoch's nonce diverges from peers
// and every header in that epoch fails VRF verification — the freeze
// described in #2128.
func TestCalculateEpochNonce_PostMithrilBootstrapFreezesCandidateAtCutoff(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	cfg := newConwayBootstrapStabilityCfg(t)

	// k=6, f=0.4 → 4k/f = 60 slots. Epoch length 75, start slot 1000,
	// end slot 1075. cutoffSlot = 1075 - 60 = 1015.
	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		cutoffSlot  uint64 = 1015
		snapTipSlot uint64 = 1010 // snapshot taken before cutoff
		preCutSlot  uint64 = 1014 // last block strictly before cutoff
		postCutSlot uint64 = 1070 // last block of epoch (post-cutoff)
	)

	// Imported snapshot tip-time evolving == candidate (psCandidate
	// tracks evolving until the stability window closes).
	importedNonce := bytes.Repeat([]byte{0xaa}, 32)
	// Per-block evolving nonces stored by post-import processing.
	// Distinct, deterministic values so a wrong return is unambiguous.
	nonceAtPreCut := bytes.Repeat([]byte{0xbb}, 32)
	nonceAtPostCut := bytes.Repeat([]byte{0xcc}, 32)

	hashAtSnap := bytes.Repeat([]byte{0x10}, 32)
	hashAtPreCut := bytes.Repeat([]byte{0x14}, 32)
	hashAtPostCut := bytes.Repeat([]byte{0x70}, 32)
	prevHashAtSnap := bytes.Repeat([]byte{0x09}, 32)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		// Mithril-imported tip block (no per-block VRF processing
		// for it; importTip writes a single block_nonce checkpoint).
		if err := db.BlockCreate(models.Block{
			Slot:     snapTipSlot,
			Hash:     hashAtSnap,
			PrevHash: prevHashAtSnap,
			Cbor:     []byte{0x80},
			Number:   1,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		// Post-import blocks. CBOR is a stub byte; the fast path
		// computeCandidateNonceFast does not decode block bodies.
		if err := db.BlockCreate(models.Block{
			Slot:     preCutSlot,
			Hash:     hashAtPreCut,
			PrevHash: hashAtSnap,
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot:     postCutSlot,
			Hash:     hashAtPostCut,
			PrevHash: hashAtPreCut,
			Cbor:     []byte{0x80},
			Number:   3,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		// importTip checkpoint at the snapshot tip — Branch B in
		// calculateEpochNonce relies on this row to find the seam
		// between imported state and post-import accumulation.
		if err := db.SetBlockNonce(
			hashAtSnap, snapTipSlot, importedNonce, true, txn,
		); err != nil {
			return err
		}
		if err := db.SetBlockNonce(
			hashAtPreCut, preCutSlot, nonceAtPreCut, false, txn,
		); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAtPostCut, postCutSlot, nonceAtPostCut, false, txn,
		)
	}))

	// LedgerState shaped like the bootstrap epoch row written by
	// generateAndSaveEpochs at import time: EvolvingNonce and
	// CandidateNonce both seeded from the snapshot's mid-epoch value.
	// currentTipBlockNonce intentionally left empty so the
	// resume-from-tip optimisation in calculateEpochNonce takes the
	// Branch B path (block_nonce row search).
	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentEpoch: models.Epoch{
			EpochId:             100,
			StartSlot:           epochStart,
			LengthInSlots:       uint(epochLength),
			SlotLength:          1000,
			EraId:               eras.ConwayEraDesc.Id,
			Nonce:               bytes.Repeat([]byte{0xee}, 32),
			EvolvingNonce:       importedNonce,
			CandidateNonce:      importedNonce,
			LastEpochBlockNonce: bytes.Repeat([]byte{0xfa}, 32),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	var candidate, evolving []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		_, ev, c, _, err := ls.calculateEpochNonce(
			txn,
			epochEnd,
			eras.ConwayEraDesc,
			ls.currentEpoch,
			nil,
		)
		candidate = c
		evolving = ev
		return err
	}))

	require.Equalf(
		t,
		hex.EncodeToString(nonceAtPreCut),
		hex.EncodeToString(candidate),
		"candidate must freeze at the last pre-cutoff block (slot %d). "+
			"Got %x. If this equals importedNonce (0xaa...0xaa), the "+
			"computation inherited the snapshot's mid-epoch candidate "+
			"and never replaced it with the frozen-at-cutoff value — "+
			"#2128 freeze. cutoff=%d, epoch=[%d,%d).",
		preCutSlot, candidate, cutoffSlot, epochStart, epochEnd,
	)
	require.Equalf(
		t,
		hex.EncodeToString(nonceAtPostCut),
		hex.EncodeToString(evolving),
		"evolving must equal the last block's stored nonce (slot %d). "+
			"Got %x.",
		postCutSlot, evolving,
	)
	require.NotEqualf(
		t,
		hex.EncodeToString(candidate),
		hex.EncodeToString(evolving),
		"candidate and evolving must differ once the chain crosses "+
			"the cutoff. Equal values mean the freeze was not applied.",
	)
}

// TestCalculateEpochNonce_PostMithrilBootstrapNoBlocksBeforeCutoff covers
// the edge case where the chain produces NO blocks in the window between
// the snapshot tip and the freeze cutoff (legal under low active-slots
// coefficient or just unlucky leader assignment). The snapshot tip slot
// is itself the last pre-cutoff slot, so the imported tip-time
// EvolvingNonce IS the correct frozen-at-cutoff value: in cardano-ledger,
// psCandidateNonce tracks evolving until the cutoff fires, so a snapshot
// taken at the very last pre-cutoff block has psCandidateNonce ==
// psEvolvingNonce == that block's accumulated evolving nonce.
//
// The rollover must therefore return candidate == importedNonce, NOT
// some other value derived from a phantom pre-cutoff block.
func TestCalculateEpochNonce_PostMithrilBootstrapNoBlocksBeforeCutoff(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	cfg := newConwayBootstrapStabilityCfg(t)

	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		cutoffSlot  uint64 = 1015
		snapTipSlot uint64 = 1014 // last block strictly before cutoff
		postCutSlot uint64 = 1070 // last block of epoch (post-cutoff)
	)

	importedNonce := bytes.Repeat([]byte{0xaa}, 32)
	nonceAtPostCut := bytes.Repeat([]byte{0xcc}, 32)

	hashAtSnap := bytes.Repeat([]byte{0x14}, 32)
	hashAtPostCut := bytes.Repeat([]byte{0x70}, 32)
	prevHashAtSnap := bytes.Repeat([]byte{0x09}, 32)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot:     snapTipSlot,
			Hash:     hashAtSnap,
			PrevHash: prevHashAtSnap,
			Cbor:     []byte{0x80},
			Number:   1,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot:     postCutSlot,
			Hash:     hashAtPostCut,
			PrevHash: hashAtSnap,
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.SetBlockNonce(
			hashAtSnap, snapTipSlot, importedNonce, true, txn,
		); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAtPostCut, postCutSlot, nonceAtPostCut, false, txn,
		)
	}))

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentEpoch: models.Epoch{
			EpochId:             100,
			StartSlot:           epochStart,
			LengthInSlots:       uint(epochLength),
			SlotLength:          1000,
			EraId:               eras.ConwayEraDesc.Id,
			Nonce:               bytes.Repeat([]byte{0xee}, 32),
			EvolvingNonce:       importedNonce,
			CandidateNonce:      importedNonce,
			LastEpochBlockNonce: bytes.Repeat([]byte{0xfa}, 32),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	var candidate, evolving []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		_, ev, c, _, err := ls.calculateEpochNonce(
			txn,
			epochEnd,
			eras.ConwayEraDesc,
			ls.currentEpoch,
			nil,
		)
		candidate = c
		evolving = ev
		return err
	}))

	require.Equalf(
		t,
		hex.EncodeToString(importedNonce),
		hex.EncodeToString(candidate),
		"with no post-import blocks before the cutoff (slot %d), the "+
			"snapshot tip slot %d itself IS the last pre-cutoff block "+
			"and its stored block_nonce (= imported tip-time evolving) "+
			"is the correct frozen candidate. Got %x.",
		cutoffSlot, snapTipSlot, candidate,
	)
	require.Equalf(
		t,
		hex.EncodeToString(nonceAtPostCut),
		hex.EncodeToString(evolving),
		"evolving must equal the last block's stored nonce (slot %d). "+
			"Got %x.",
		postCutSlot, evolving,
	)
}

// TestCalculateEpochNonce_PostMithrilBootstrapWithoutCheckpoint covers
// the operational hazard where a deployment was bootstrapped with an
// importer that did not write the block_nonce checkpoint at the
// snapshot tip slot (older code paths predating PR #2032). Branch B
// in calculateEpochNonce searches for a block_nonce row matching
// prevEpoch.EvolvingNonce; with no checkpoint that row does not
// exist, and the resume seam is not found.
//
// The fast path should still produce correct results because it does
// NOT depend on Branch B — it directly looks up the cutoff block and
// the last block of the epoch by slot, and uses their stored
// block_nonce rows. Those rows exist for every post-import block
// (per-block accumulation correctly chains from the imported
// EvolvingNonce, even though the seed itself is not in block_nonce).
//
// This test guards against any future change that makes the fast
// path require a Branch B match.
func TestCalculateEpochNonce_PostMithrilBootstrapWithoutCheckpoint(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	cfg := newConwayBootstrapStabilityCfg(t)

	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		cutoffSlot  uint64 = 1015
		snapTipSlot uint64 = 1010
		preCutSlot  uint64 = 1014
		postCutSlot uint64 = 1070
	)

	importedNonce := bytes.Repeat([]byte{0xaa}, 32)
	nonceAtPreCut := bytes.Repeat([]byte{0xbb}, 32)
	nonceAtPostCut := bytes.Repeat([]byte{0xcc}, 32)

	hashAtSnap := bytes.Repeat([]byte{0x10}, 32)
	hashAtPreCut := bytes.Repeat([]byte{0x14}, 32)
	hashAtPostCut := bytes.Repeat([]byte{0x70}, 32)
	prevHashAtSnap := bytes.Repeat([]byte{0x09}, 32)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot:     snapTipSlot,
			Hash:     hashAtSnap,
			PrevHash: prevHashAtSnap,
			Cbor:     []byte{0x80},
			Number:   1,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot:     preCutSlot,
			Hash:     hashAtPreCut,
			PrevHash: hashAtSnap,
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot:     postCutSlot,
			Hash:     hashAtPostCut,
			PrevHash: hashAtPreCut,
			Cbor:     []byte{0x80},
			Number:   3,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		// NOTE: deliberately NO checkpoint row at snapTipSlot.
		// Post-import block_nonce rows only.
		if err := db.SetBlockNonce(
			hashAtPreCut, preCutSlot, nonceAtPreCut, false, txn,
		); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAtPostCut, postCutSlot, nonceAtPostCut, false, txn,
		)
	}))

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentEpoch: models.Epoch{
			EpochId:             100,
			StartSlot:           epochStart,
			LengthInSlots:       uint(epochLength),
			SlotLength:          1000,
			EraId:               eras.ConwayEraDesc.Id,
			Nonce:               bytes.Repeat([]byte{0xee}, 32),
			EvolvingNonce:       importedNonce,
			CandidateNonce:      importedNonce,
			LastEpochBlockNonce: bytes.Repeat([]byte{0xfa}, 32),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	var candidate, evolving []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		_, ev, c, _, err := ls.calculateEpochNonce(
			txn,
			epochEnd,
			eras.ConwayEraDesc,
			ls.currentEpoch,
			nil,
		)
		candidate = c
		evolving = ev
		return err
	}))

	require.Equalf(
		t,
		hex.EncodeToString(nonceAtPreCut),
		hex.EncodeToString(candidate),
		"without the snap-tip checkpoint, the fast path must still "+
			"freeze candidate at the last pre-cutoff block (slot %d) "+
			"via direct slot lookup. Got %x.",
		preCutSlot, candidate,
	)
	require.Equalf(
		t,
		hex.EncodeToString(nonceAtPostCut),
		hex.EncodeToString(evolving),
		"without the snap-tip checkpoint, evolving must still equal "+
			"the last block's stored nonce (slot %d). Got %x.",
		postCutSlot, evolving,
	)
}

// TestComputeEpochNonceForSlot_PostMithrilBootstrapMatchesRollover
// mirrors the basic bootstrap scenario but exercises the header
// verification path (advanceEpochCache → computeEpochNonceForSlot)
// instead of the rollover path (calculateEpochNonce). The two paths
// must agree: header verification of any block in the new epoch
// uses the cached epoch-nonce computed by computeEpochNonceForSlot,
// while the persisted epoch row written by processEpochRollover uses
// calculateEpochNonce. Disagreement means peer headers verifying
// against one nonce while we recompute another — the freeze pattern
// in #2128.
func TestComputeEpochNonceForSlot_PostMithrilBootstrapMatchesRollover(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	cfg := newConwayBootstrapStabilityCfg(t)

	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		snapTipSlot uint64 = 1010
		preCutSlot  uint64 = 1014
		postCutSlot uint64 = 1070
	)

	importedNonce := bytes.Repeat([]byte{0xaa}, 32)
	nonceAtPreCut := bytes.Repeat([]byte{0xbb}, 32)
	nonceAtPostCut := bytes.Repeat([]byte{0xcc}, 32)

	hashAtSnap := bytes.Repeat([]byte{0x10}, 32)
	hashAtPreCut := bytes.Repeat([]byte{0x14}, 32)
	hashAtPostCut := bytes.Repeat([]byte{0x70}, 32)
	prevHashAtSnap := bytes.Repeat([]byte{0x09}, 32)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot:     snapTipSlot,
			Hash:     hashAtSnap,
			PrevHash: prevHashAtSnap,
			Cbor:     []byte{0x80},
			Number:   1,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot:     preCutSlot,
			Hash:     hashAtPreCut,
			PrevHash: hashAtSnap,
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot:     postCutSlot,
			Hash:     hashAtPostCut,
			PrevHash: hashAtPreCut,
			Cbor:     []byte{0x80},
			Number:   3,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.SetBlockNonce(
			hashAtSnap, snapTipSlot, importedNonce, true, txn,
		); err != nil {
			return err
		}
		if err := db.SetBlockNonce(
			hashAtPreCut, preCutSlot, nonceAtPreCut, false, txn,
		); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAtPostCut, postCutSlot, nonceAtPostCut, false, txn,
		)
	}))

	prevEpoch := models.Epoch{
		EpochId:             100,
		StartSlot:           epochStart,
		LengthInSlots:       uint(epochLength),
		SlotLength:          1000,
		EraId:               eras.ConwayEraDesc.Id,
		Nonce:               bytes.Repeat([]byte{0xee}, 32),
		EvolvingNonce:       importedNonce,
		CandidateNonce:      importedNonce,
		LastEpochBlockNonce: bytes.Repeat([]byte{0xfa}, 32),
	}

	ls := &LedgerState{
		db:           db,
		currentEra:   eras.ConwayEraDesc,
		currentEpoch: prevEpoch,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	// Header verification path.
	hvNonce, hvEvolving, hvCandidate, hvLab, err :=
		ls.computeEpochNonceForSlot(epochEnd, prevEpoch)
	require.NoError(t, err)

	// Rollover path, run in a transaction (production behaviour).
	var rNonce, rEvolving, rCandidate, rLab []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		n, ev, c, lab, err := ls.calculateEpochNonce(
			txn,
			epochEnd,
			eras.ConwayEraDesc,
			prevEpoch,
			nil,
		)
		rNonce = n
		rEvolving = ev
		rCandidate = c
		rLab = lab
		return err
	}))

	require.Equalf(
		t,
		hex.EncodeToString(rCandidate),
		hex.EncodeToString(hvCandidate),
		"header-verification candidate must match rollover candidate. "+
			"hv=%x, rollover=%x.", hvCandidate, rCandidate,
	)
	require.Equalf(
		t,
		hex.EncodeToString(rEvolving),
		hex.EncodeToString(hvEvolving),
		"header-verification evolving must match rollover evolving. "+
			"hv=%x, rollover=%x.", hvEvolving, rEvolving,
	)
	require.Equalf(
		t,
		hex.EncodeToString(rNonce),
		hex.EncodeToString(hvNonce),
		"header-verification epoch nonce must match rollover epoch "+
			"nonce. hv=%x, rollover=%x. Disagreement here is the "+
			"#2128 freeze: peer headers pass one nonce, our cache "+
			"verifies against another.", hvNonce, rNonce,
	)
	require.Equalf(
		t,
		hex.EncodeToString(rLab),
		hex.EncodeToString(hvLab),
		"header-verification labNonce must match rollover labNonce. "+
			"hv=%x, rollover=%x.", hvLab, rLab,
	)
	// And separately confirm both paths produce the expected candidate
	// (the last pre-cutoff block's stored nonce), just so a future
	// change that breaks both in lock-step doesn't pass this test.
	require.Equal(
		t,
		hex.EncodeToString(nonceAtPreCut),
		hex.EncodeToString(rCandidate),
		"both paths must freeze candidate at last pre-cutoff block",
	)
}

// newConwayBootstrapStabilityCfg builds a CardanoNodeConfig with k=6,
// f=0.4 so 4k/f = 60 — the Conway nonce stability window. With epoch
// length 75 starting at slot 1000, the candidate-freeze cutoff lands at
// slot 1015, which lets the bootstrap test place blocks on each side of
// the cutoff with single-digit slot gaps.
func newConwayBootstrapStabilityCfg(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, loadByronGenesisForTest(t, cfg, strings.NewReader(`{
		"protocolConsts": {
			"k": 6,
			"protocolMagic": 42
		}
	}`)))
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"systemStart": "2026-01-01T00:00:00Z",
		"securityParam": 6,
		"activeSlotsCoeff": 0.4,
		"epochLength": 75,
		"slotLength": 1
	}`)))
	return cfg
}

// shrinkCleanupConsumedUtxosInterval makes the periodic cleanup timer fire on
// a test timescale. The package var is restored on cleanup, so these tests
// must not run in parallel with each other.
func shrinkCleanupConsumedUtxosInterval(t *testing.T, d time.Duration) {
	t.Helper()
	prev := cleanupConsumedUtxosInterval
	cleanupConsumedUtxosInterval = d
	t.Cleanup(func() { cleanupConsumedUtxosInterval = prev })
}

// newCleanupTimerFireSignal returns a hook that reports each timer fire on the
// returned channel. The send is non-blocking so an unread fire never stalls
// the timer callback itself.
func newCleanupTimerFireSignal() (func(), <-chan struct{}) {
	fires := make(chan struct{}, 64)
	return func() {
		select {
		case fires <- struct{}{}:
		default:
		}
	}, fires
}

// drainCleanupTimerFires discards every fire already buffered. Close has
// stopped and drained the timer by the time this is called, so all of them
// happened before it returned; only a fire arriving afterwards is a defect.
// Draining fully -- rather than discarding a single fire -- is what keeps the
// absence assertion from depending on how many intervals elapsed while the
// test was between receives.
func drainCleanupTimerFires(fires <-chan struct{}) {
	for {
		select {
		case <-fires:
		default:
			return
		}
	}
}

// TestCleanupConsumedUtxos_TimerStopsOnClose covers the first half of issue
// #3439: the cleanup timer callback re-arms itself via
// scheduleCleanupConsumedUtxos, so a Close that does not stop it leaves a
// self-perpetuating timer running against a database its owner closes
// immediately after Close returns (LedgerState does not own the database --
// see the note at the end of Close).
// Not t.Parallel: shrinkCleanupConsumedUtxosInterval swaps the package-level
// cleanupConsumedUtxosInterval, which every concurrent LedgerState in this
// package would observe.
func TestCleanupConsumedUtxos_TimerStopsOnClose(t *testing.T) {
	shrinkCleanupConsumedUtxosInterval(t, 5*time.Millisecond)
	db := newTestDBForCleanup(t, types.StorageModeCore)
	ls := newLedgerStateForCleanup(db, 100_000)

	hook, fires := newCleanupTimerFireSignal()
	ls.cleanupConsumedUtxosTimerFiredHook = hook

	ls.scheduleCleanupConsumedUtxos()
	// Two fires prove the callback re-armed itself at least once, so the
	// absence check below is measuring a stopped timer rather than one that
	// simply never started.
	testutil.RequireReceive(
		t, fires, testutil.AsyncWait,
		"cleanup timer must fire while the ledger state is open",
	)
	testutil.RequireReceive(
		t, fires, testutil.AsyncWait,
		"cleanup timer must re-arm itself while the ledger state is open",
	)

	require.NoError(t, ls.Close())

	drainCleanupTimerFires(fires)

	// 200ms is ~40 shrunken intervals. The assertion is that a stopped timer
	// fires zero times, not that a running one fires within a deadline, so
	// runner load cannot turn this into a flake.
	testutil.RequireNoReceive(
		t, fires, 200*time.Millisecond,
		"cleanup timer must not fire after Close returns",
	)
}

// TestCleanupConsumedUtxos_CloseWaitsForActiveCallback covers the drain half
// of the acceptance criteria. Stopping a time.Timer does not wait for an
// AfterFunc callback that has already started, so Close must join the
// in-flight run; otherwise it returns while cleanup is still issuing database
// work, and the owner closes the database out from under it.
func TestCleanupConsumedUtxos_CloseWaitsForActiveCallback(t *testing.T) {
	shrinkCleanupConsumedUtxosInterval(t, 5*time.Millisecond)
	db := newTestDBForCleanup(t, types.StorageModeCore)
	ls := newLedgerStateForCleanup(db, 100_000)

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	// The run hook, not the timer-fired hook: this must block inside the
	// region that has already registered with the drain, which is what Close
	// is required to wait for.
	ls.cleanupConsumedUtxosRunHook = func() {
		once.Do(func() {
			close(entered)
			<-release
		})
	}

	ls.scheduleCleanupConsumedUtxos()
	testutil.RequireReceive(
		t, entered, testutil.AsyncWait,
		"cleanup timer callback must start before Close is called",
	)

	closeReturned := make(chan error, 1)
	go func() { closeReturned <- ls.Close() }()

	testutil.RequireNoReceive(
		t, closeReturned, 200*time.Millisecond,
		"Close must not return while a cleanup callback is in flight",
	)

	close(release)
	err := testutil.RequireReceive(
		t, closeReturned, testutil.AsyncWait,
		"Close must return once the in-flight cleanup callback finishes",
	)
	require.NoError(t, err)
}

// TestCleanupConsumedUtxos_NoDatabaseWorkAfterClose covers the second
// acceptance criterion for the path that has no timer at all: the epoch
// transition fires cleanup as a bare `go ls.cleanupConsumedUtxos()`
// (state.go), which can lose the race with shutdown. Stopping the timer alone
// does not constrain that goroutine.
//
// TestCleanupConsumedUtxos_CoreModePrunes is the positive control: the same
// seeded row and tip are deleted by the same call on an open ledger state, so
// a passing result here cannot come from cleanup being inert.
func TestCleanupConsumedUtxos_NoDatabaseWorkAfterClose(t *testing.T) {
	t.Parallel()

	db := newTestDBForCleanup(t, types.StorageModeCore)
	txId := bytes.Repeat([]byte{0xC5}, 32)
	const (
		addedSlot   uint64 = 1_000
		deletedSlot uint64 = 5_000
		tipSlot     uint64 = 100_000 // > 50_000 default stability window
	)
	seedSpentUtxoForCleanup(t, db, txId, 0, addedSlot, deletedSlot)

	ls := newLedgerStateForCleanup(db, tipSlot)
	require.NoError(t, ls.Close())

	ls.cleanupConsumedUtxos()

	post, err := db.Metadata().GetUtxoIncludingSpent(txId, 0, nil)
	require.NoError(t, err)
	assert.NotNil(
		t, post,
		"cleanup must not begin database work after Close returns",
	)
}

// TestCleanupConsumedUtxos_RepeatedCloseIsSafe covers the repeated-close half
// of the third acceptance criterion. A drain built on sync.WaitGroup is easy
// to get wrong on the second call, and Close is genuinely called twice on the
// live restore/truncate path.
func TestCleanupConsumedUtxos_RepeatedCloseIsSafe(t *testing.T) {
	shrinkCleanupConsumedUtxosInterval(t, 5*time.Millisecond)
	db := newTestDBForCleanup(t, types.StorageModeCore)
	ls := newLedgerStateForCleanup(db, 100_000)

	hook, fires := newCleanupTimerFireSignal()
	ls.cleanupConsumedUtxosTimerFiredHook = hook

	ls.scheduleCleanupConsumedUtxos()
	testutil.RequireReceive(
		t, fires, testutil.AsyncWait,
		"cleanup timer must fire while the ledger state is open",
	)

	require.NoError(t, ls.Close())
	require.NoError(t, ls.Close(), "repeated Close must remain a no-op")

	drainCleanupTimerFires(fires)
	testutil.RequireNoReceive(
		t, fires, 200*time.Millisecond,
		"cleanup timer must stay stopped across repeated Close calls",
	)
}

// TestCleanupConsumedUtxos_ScheduleAfterCloseDoesNotArm covers the re-arm
// window directly: the timer callback calls scheduleCleanupConsumedUtxos
// after running cleanup, so a callback that was already in flight when Close
// stopped the timer would otherwise install a fresh one behind Close's back.
func TestCleanupConsumedUtxos_ScheduleAfterCloseDoesNotArm(t *testing.T) {
	shrinkCleanupConsumedUtxosInterval(t, 5*time.Millisecond)
	db := newTestDBForCleanup(t, types.StorageModeCore)
	ls := newLedgerStateForCleanup(db, 100_000)

	hook, fires := newCleanupTimerFireSignal()
	ls.cleanupConsumedUtxosTimerFiredHook = hook

	require.NoError(t, ls.Close())
	ls.scheduleCleanupConsumedUtxos()

	testutil.RequireNoReceive(
		t, fires, 200*time.Millisecond,
		"scheduling cleanup after Close must not arm a timer",
	)
}

// newTestDBForCleanup builds an in-memory Database in the requested storage
// mode. An empty mode defaults to core (per Database.New).
func newTestDBForCleanup(t *testing.T, mode string) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     "",
		StorageMode: mode,
	})
	require.NoError(t, err)
	return db
}

// seedSpentUtxoForCleanup writes a single spent UTxO row directly. The
// row is "consumed at deletedSlot but still well within the periodic
// cleanup eligibility window" — matching what a normal block application
// would produce after the consumed input was soft-marked.
func seedSpentUtxoForCleanup(
	t *testing.T,
	db *database.Database,
	txId []byte,
	outputIdx uint32,
	addedSlot, deletedSlot uint64,
) {
	t.Helper()
	mdTxn := db.MetadataTxn(true)
	require.NoError(t, mdTxn.Do(func(txn *database.Txn) error {
		return db.CreateUtxo(txn, &models.Utxo{
			TxId:        txId,
			OutputIdx:   outputIdx,
			AddedSlot:   addedSlot,
			DeletedSlot: deletedSlot,
			Amount:      types.Uint64(1),
		})
	}))
}

// newLedgerStateForCleanup wires the minimum surface area
// cleanupConsumedUtxos needs: db, currentTip, currentEra, and a logger.
// CardanoNodeConfig is intentionally left nil so
// calculateStabilityWindowForEra returns the default
// (blockfetchBatchSlotThresholdDefault = 50000); the tip slot is then
// chosen well past that window so consumed-UTxO cleanup is eligible to
// run in core mode. The upstream tip is initialized to the local tip so the
// test represents a node that is near the network tip.
func newLedgerStateForCleanup(
	db *database.Database,
	tipSlot uint64,
) *LedgerState {
	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, nil),
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.syncUpstreamTipSlot.Store(tipSlot)
	return ls
}

// TestCleanupConsumedUtxos_CoreModePrunes asserts the pre-existing
// invariant that core mode hard-deletes consumed UTxO rows once the
// stability window has passed. Without this baseline, the API-mode
// retention test below could pass by accident if the cleanup loop were
// silently dead for both modes.
func TestCleanupConsumedUtxos_CoreModePrunes(t *testing.T) {
	t.Parallel()

	db := newTestDBForCleanup(t, types.StorageModeCore)
	txId := bytes.Repeat([]byte{0xA1}, 32)
	const (
		addedSlot   uint64 = 1_000
		deletedSlot uint64 = 5_000
		tipSlot     uint64 = 100_000 // > 50_000 default stability window
	)
	seedSpentUtxoForCleanup(t, db, txId, 0, addedSlot, deletedSlot)

	pre, err := db.Metadata().GetUtxoIncludingSpent(txId, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, pre, "seed must succeed before cleanup")

	ls := newLedgerStateForCleanup(db, tipSlot)
	ls.cleanupConsumedUtxos()

	post, err := db.Metadata().GetUtxoIncludingSpent(txId, 0, nil)
	require.NoError(t, err)
	assert.Nil(
		t, post,
		"core mode must hard-delete consumed UTxO rows after stability "+
			"window",
	)
}

// TestCleanupConsumedUtxos_PersistsPruneFloor covers the durable marker
// checkUtxoRetentionWindow relies on (ledger/queries.go): every run that
// actually prunes must durably record the floor it used, so a later pin
// check can reject against it even if the tip subsequently moves in a way
// that would otherwise make a freshly-computed floor look more lenient
// (blinklabs-io/dingo#382 review -- see persistConsumedUtxoPruneFloor's doc
// comment for the rollback and era-transition cases this closes).
func TestCleanupConsumedUtxos_PersistsPruneFloor(t *testing.T) {
	t.Parallel()

	db := newTestDBForCleanup(t, types.StorageModeCore)
	const tipSlot = 100_000 // > 50_000 default stability window

	ls := newLedgerStateForCleanup(db, tipSlot)

	before, err := ls.readConsumedUtxoPruneFloor(nil)
	require.NoError(t, err)
	assert.Zero(t, before, "no floor before cleanup has ever run")

	ls.cleanupConsumedUtxos()

	after, err := ls.readConsumedUtxoPruneFloor(nil)
	require.NoError(t, err)
	assert.Equal(
		t, uint64(50_000), after,
		"must persist tipSlot minus the default stability window",
	)
}

func TestCleanupConsumedUtxos_DoesNotWaitForChainsyncMutex(t *testing.T) {
	t.Parallel()

	db := newTestDBForCleanup(t, types.StorageModeCore)
	ls := newLedgerStateForCleanup(db, 100_000)
	ls.chainsyncMutex.Lock()
	defer ls.chainsyncMutex.Unlock()

	done := make(chan struct{})
	go func() {
		ls.cleanupConsumedUtxos()
		close(done)
	}()
	testutil.RequireReceive(
		t,
		done,
		testutil.AsyncWait,
		"consumed UTxO cleanup must not acquire chainsyncMutex",
	)
}

// TestCleanupConsumedUtxos_SkipsDeleteWhenPruneFloorPersistFails covers the
// ordering invariant persistConsumedUtxoPruneFloor's doc comment depends on:
// a failure to durably record the floor must abort this run before any row
// is actually hard-deleted, not just be logged and ignored. Otherwise real
// rows could be pruned with no durable record that floor was ever used,
// letting a later pinned query at or above that floor see no persisted
// floor to reject against and silently answer "absent" for a ref that was
// actually there. The test drops the
// sync_state table (via a raw connection to the same file) so SetSyncState
// fails while the utxo table -- and so UtxosDeleteConsumed -- stays fully
// functional, isolating the failure to exactly the call this test cares
// about.
func TestCleanupConsumedUtxos_SkipsDeleteWhenPruneFloorPersistFails(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     t.TempDir(),
		StorageMode: types.StorageModeCore,
	})
	require.NoError(t, err)

	txId := bytes.Repeat([]byte{0xD2}, 32)
	const (
		addedSlot   uint64 = 1_000
		deletedSlot uint64 = 5_000
		tipSlot     uint64 = 100_000 // > 50_000 default stability window
	)
	seedSpentUtxoForCleanup(t, db, txId, 0, addedSlot, deletedSlot)

	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec("DROP TABLE sync_state")
	require.NoError(t, err)

	ls := newLedgerStateForCleanup(db, tipSlot)
	ls.cleanupConsumedUtxos()

	post, err := db.Metadata().GetUtxoIncludingSpent(txId, 0, nil)
	require.NoError(t, err)
	assert.NotNil(
		t, post,
		"cleanup must not delete rows when persisting the prune floor "+
			"fails, since that would prune without any durable record "+
			"that pruning happened",
	)
}

func TestCleanupConsumedUtxos_ProcessesOneBoundedBatch(t *testing.T) {
	t.Parallel()

	db := newTestDBForCleanup(t, types.StorageModeCore)
	mdTxn := db.MetadataTxn(true)
	require.NoError(t, mdTxn.Do(func(txn *database.Txn) error {
		for idx := 0; idx <= cleanupConsumedUtxoBatchSize; idx++ {
			txID := bytes.Repeat([]byte{0}, 32)
			binary.BigEndian.PutUint32(txID[:4], uint32(idx+1))
			if err := db.CreateUtxo(txn, &models.Utxo{
				TxId:        txID,
				OutputIdx:   0,
				AddedSlot:   1_000,
				DeletedSlot: 5_000,
				Amount:      types.Uint64(1),
			}); err != nil {
				return err
			}
		}
		return nil
	}))

	ls := newLedgerStateForCleanup(db, 100_000)
	ls.cleanupConsumedUtxos()

	remaining, err := db.Metadata().GetUtxosDeletedBeforeSlot(
		50_000,
		cleanupConsumedUtxoBatchSize+1,
		nil,
	)
	require.NoError(t, err)
	assert.Len(t, remaining, 1, "one eligible row must remain for a later run")

	ls.cleanupConsumedUtxos()
	remaining, err = db.Metadata().GetUtxosDeletedBeforeSlot(
		50_000,
		cleanupConsumedUtxoBatchSize+1,
		nil,
	)
	require.NoError(t, err)
	assert.Empty(t, remaining, "the next run must resume the bounded cleanup")
}

func TestCleanupConsumedUtxos_DefersDuringCatchup(t *testing.T) {
	t.Parallel()

	db := newTestDBForCleanup(t, types.StorageModeCore)
	txId := bytes.Repeat([]byte{0xA3}, 32)
	const (
		addedSlot   uint64 = 1_000
		deletedSlot uint64 = 5_000
		tipSlot     uint64 = 100_000
		upstreamTip uint64 = 200_000
	)
	seedSpentUtxoForCleanup(t, db, txId, 0, addedSlot, deletedSlot)

	ls := newLedgerStateForCleanup(db, tipSlot)
	ls.syncUpstreamTipSlot.Store(upstreamTip)
	ls.cleanupConsumedUtxos()

	post, err := db.Metadata().GetUtxoIncludingSpent(txId, 0, nil)
	require.NoError(t, err)
	assert.NotNil(t, post, "cleanup must defer while the ledger is catching up")
}

// TestCleanupConsumedUtxos_RunsWithoutKnownUpstreamTip covers the
// distinction the catch-up deferral has to make: an upstream tip of 0 means
// unknown, not "infinitely far behind". A node that has never connected to a
// peer -- or that lost its last active connection, which zeroes the value in
// chainsync.go -- would otherwise defer cleanup for as long as it stays
// peerless, growing the utxo table without bound in core mode, and silently:
// no error, no crash. Cleanup ran off the local tip alone before the deferral
// existed, so that is the behavior an unknown upstream tip falls back to.
func TestCleanupConsumedUtxos_RunsWithoutKnownUpstreamTip(t *testing.T) {
	t.Parallel()

	db := newTestDBForCleanup(t, types.StorageModeCore)
	txId := bytes.Repeat([]byte{0xB4}, 32)
	const (
		addedSlot   uint64 = 1_000
		deletedSlot uint64 = 5_000
		// Far past the 50_000 default stability window, so the only
		// reason to retain the row would be the deferral itself.
		tipSlot uint64 = 10_000_000
	)
	seedSpentUtxoForCleanup(t, db, txId, 0, addedSlot, deletedSlot)

	pre, err := db.Metadata().GetUtxoIncludingSpent(txId, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, pre, "seed must succeed before cleanup")

	ls := newLedgerStateForCleanup(db, tipSlot)
	// No peer has ever reported a tip.
	ls.syncUpstreamTipSlot.Store(0)
	ls.cleanupConsumedUtxos()

	post, err := db.Metadata().GetUtxoIncludingSpent(txId, 0, nil)
	require.NoError(t, err)
	assert.Nil(
		t, post,
		"an unknown upstream tip must not defer cleanup: the local tip "+
			"is already far past the stability window",
	)
}

// TestCleanupConsumedUtxos_APIModeRetains is the regression fix for
// issue #2350: in API storage mode the periodic cleanup must leave
// spent UTxO metadata rows in place so historical transaction queries
// can resolve input / collateral / reference-input associations via
// spent_at_tx_id, collateral_by_tx_id, and referenced_by_tx_id.
func TestCleanupConsumedUtxos_APIModeRetains(t *testing.T) {
	t.Parallel()

	db := newTestDBForCleanup(t, types.StorageModeAPI)
	txId := bytes.Repeat([]byte{0xA2}, 32)
	const (
		addedSlot   uint64 = 1_000
		deletedSlot uint64 = 5_000
		tipSlot     uint64 = 100_000
	)
	seedSpentUtxoForCleanup(t, db, txId, 0, addedSlot, deletedSlot)

	ls := newLedgerStateForCleanup(db, tipSlot)
	ls.cleanupConsumedUtxos()

	post, err := db.Metadata().GetUtxoIncludingSpent(txId, 0, nil)
	require.NoError(t, err)
	require.NotNil(
		t, post,
		"API mode must retain spent UTxO row past the cleanup threshold "+
			"so historical transaction queries can still resolve input / "+
			"collateral / reference-input associations",
	)
	assert.Equal(
		t, deletedSlot, post.DeletedSlot,
		"retained row must keep deleted_slot as the spent-state encoding",
	)

	// Live-UTxO queries must still filter the retained row out: it has
	// a non-zero deleted_slot so it is no longer part of the active set.
	live, err := db.Metadata().GetUtxo(txId, 0, nil)
	require.NoError(t, err)
	assert.Nil(t, live,
		"live UTxO view must continue to exclude spent rows in API mode")
}

// GOVCERT rejects an AuthCommitteeHot from a cold credential whose
// csCommitteeCreds entry is CommitteeMemberResigned
// (checkAndOverwriteCommitteeMemberState, ConwayCommitteeHasPreviouslyResigned).
// A potential future member's entry lasts until the next epoch boundary
// (Conway EPOCH, updateCommitteeState), while a seated member's entry
// survives every boundary, so a seated member that resigned stays resigned
// even when a pending proposal would re-elect it.
func TestValidateTxCommitteeAuthorizationAfterResignation(t *testing.T) {
	t.Parallel()

	const epochStartSlot = 100
	tests := []struct {
		name       string
		seated     bool
		resignSlot uint64
		rejected   bool
	}{
		{
			name:       "pending member resigned this epoch",
			resignSlot: epochStartSlot + 1,
			rejected:   true,
		},
		{
			name:       "pending member resigned last epoch",
			resignSlot: epochStartSlot - 1,
		},
		{
			name:       "seated member resigned with a pending re-election",
			seated:     true,
			resignSlot: epochStartSlot - 1,
			rejected:   true,
		},
	}
	for _, era := range []committeeVotingEra{
		committeeVotingConway,
		committeeVotingDijkstra,
	} {
		for _, test := range tests {
			t.Run(era.name+"/"+test.name, func(t *testing.T) {
				t.Parallel()
				pparams := era.pparams(lcommon.ProtocolVersionPlomin)
				lv, db := committeeTestView(t, pparams)
				lv.epochStartSlot = epochStartSlot
				cold, coldKey := committeeTestVotingKey(0x61)
				if test.seated {
					seatCommitteeMembers(t, db, cold)
				} else {
					seatCommitteeMembers(t, db, committeeTestCredential(0x62))
				}
				storeCommitteeUpdateProposal(t, db, 0x63, cold, 10)
				seedCommitteeCredentialResignation(
					t,
					db,
					cold,
					1,
					test.resignSlot,
				)
				_, paymentKey := committeeTestVotingKey(0x64)

				err := committeeVotingValidate(
					t, era, lv, pparams, paymentKey,
					nil,
					[]lcommon.Certificate{authorizeHotCertificate(
						cold,
						committeeTestCredential(0x65),
					)},
					coldKey,
				)
				if test.rejected {
					var resigned conway.ResignedCommitteeMemberHotKeyError
					require.ErrorAs(t, err, &resigned)
				} else {
					require.NoError(t, err)
				}
			})
		}
	}
}

// GOVCERT admits a potential future member when any pending committee
// proposal names it among its new members (isPotentialFutureMember), so a
// newer pending proposal that would remove the credential does not cancel an
// older one that adds it.
func TestValidateTxCommitteeAuthorizationByMemberOfAnyPendingProposal(
	t *testing.T,
) {
	t.Parallel()

	for _, era := range []committeeVotingEra{
		committeeVotingConway,
		committeeVotingDijkstra,
	} {
		t.Run(era.name, func(t *testing.T) {
			t.Parallel()
			pparams := era.pparams(lcommon.ProtocolVersionPlomin)
			lv, db := committeeTestView(t, pparams)
			seatCommitteeMembers(t, db, committeeTestCredential(0x71))
			cold, coldKey := committeeTestVotingKey(0x72)
			storeCommitteeUpdateProposal(t, db, 0x73, cold, 10)
			removal, err := lcommon.NewUpdateCommitteeGovAction(
				nil,
				[]lcommon.Credential{cold},
				nil,
				cbor.Rat{Rat: big.NewRat(2, 3)},
			)
			require.NoError(t, err)
			encoded, err := cbor.Encode(removal)
			require.NoError(t, err)
			require.NoError(
				t,
				db.SetGovernanceProposal(&models.GovernanceProposal{
					TxHash:        governanceTestHash(0x74),
					ActionType:    uint8(lcommon.GovActionTypeUpdateCommittee),
					ExpiresEpoch:  100,
					AnchorHash:    make([]byte, 32),
					ReturnAddress: make([]byte, 29),
					GovActionCbor: encoded,
					AddedSlot:     5,
				}, nil),
			)
			_, paymentKey := committeeTestVotingKey(0x75)

			err = committeeVotingValidate(
				t, era, lv, pparams, paymentKey,
				nil,
				[]lcommon.Certificate{authorizeHotCertificate(
					cold,
					committeeTestCredential(0x76),
				)},
				coldKey,
			)
			require.NoError(t, err)
		})
	}
}

// A member seated by an UpdateCommittee enactment keeps only the committee
// certificates it recorded in the epoch the boundary closes: at the boundary
// before, it was not in the committee, so cardano-ledger dropped its
// csCommitteeCreds entry there (Conway EPOCH, updateCommitteeState). The
// RATIFY tally at the enactment boundary sees the same state: a member with
// no committee entry is not counted, while one with a hot key that did not
// vote counts as No (committeeAcceptedRatio). Here that decides whether a
// treasury withdrawal the incumbent member voted for is ratified.
func TestEpochRolloverSeatsMemberWithOnlyItsClosingEpochAuthorization(
	t *testing.T,
) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		authSlot uint64
		hasHot   bool
	}{
		{name: "authorized before the closing epoch", authSlot: 499},
		{name: "authorized in the closing epoch", authSlot: 500, hasHot: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newTreasuryRolloverFixture(t, 100)
			require.Equal(t, uint64(500), f.currentEpoch.StartSlot)
			cold := lcommon.Credential{
				CredType:   lcommon.CredentialTypeAddrKeyHash,
				Credential: lcommon.NewBlake2b224(repeatByte(28, 0xd1)),
			}
			hot := repeatByte(28, 0xd2)
			action, err := lcommon.NewUpdateCommitteeGovAction(
				nil,
				nil,
				map[*lcommon.Credential]uint64{
					&cold: f.currentEpoch.EpochId + 20,
				},
				cbor.Rat{Rat: big.NewRat(1, 1)},
			)
			require.NoError(t, err)
			actionCbor, err := cbor.Encode(action)
			require.NoError(t, err)
			ratifiedEpoch := f.currentEpoch.EpochId
			ratifiedSlot := f.currentEpoch.StartSlot + 50
			update := &models.GovernanceProposal{
				TxHash:        repeatByte(32, 0xd3),
				ActionType:    uint8(lcommon.GovActionTypeUpdateCommittee),
				ProposedEpoch: f.currentEpoch.EpochId - 2,
				ExpiresEpoch:  f.currentEpoch.EpochId + 20,
				AnchorHash:    repeatByte(32, 0xd4),
				ReturnAddress: repeatByte(29, 0xd5),
				GovActionCbor: actionCbor,
				AddedSlot:     350,
				RatifiedEpoch: &ratifiedEpoch,
				RatifiedSlot:  &ratifiedSlot,
			}
			require.NoError(t, f.db.SetGovernanceProposal(update, nil))
			raw, err := dbtest.RawSQLiteMetadata(t, f.db)
			require.NoError(t, err)
			_, err = raw.Exec(`
INSERT INTO auth_committee_hot (
    cold_credential, host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?)`, cold.Credential[:], hot, 2, tc.authSlot)
			require.NoError(t, err)
			withdrawAddress, returnAddress, _ := f.rewardAddress(t, 0xd6)
			withdrawal := f.addProposal(
				t,
				0xd7,
				510,
				map[*lcommon.Address]uint64{withdrawAddress: 40},
				returnAddress,
				0,
				false,
			)

			result := f.rollover(t, f.currentEpoch, f.currentPParams)
			enacted := f.proposal(t, update)
			require.NotNil(t, enacted.EnactedSlot)

			state, err := governance.LoadCommitteeVotingState(
				f.db, nil, result.NewCurrentEpoch.EpochId,
			)
			require.NoError(t, err)
			wantActive := 1
			if tc.hasHot {
				wantActive = 2
			}
			require.Equal(t, wantActive, state.ActiveMemberCount)
			ratified := f.proposal(t, withdrawal)
			require.Equal(
				t,
				!tc.hasHot,
				ratified.RatifiedSlot != nil,
				"withdrawal ratification: %s",
				fmt.Sprint(ratified.RatifiedSlot),
			)

			lv := f.ls.NewView(nil)
			lv.epochStartSlot = result.NewCurrentEpoch.StartSlot
			member, err := lv.CommitteeHotCredentialMember(lcommon.Credential{
				CredType:   lcommon.CredentialTypeAddrKeyHash,
				Credential: lcommon.NewBlake2b224(hot),
			})
			require.NoError(t, err)
			require.Equal(t, tc.hasHot, member != nil)

			members, err := f.db.GetCommitteeMembers(nil)
			require.NoError(t, err)
			var seated *models.CommitteeMember
			for _, member := range members {
				if lcommon.NewBlake2b224(
					member.ColdCredHash,
				) == cold.Credential {
					seated = member
				}
			}
			require.NotNil(t, seated)
			require.Equal(t, f.currentEpoch.StartSlot, seated.TermStartSlot)
		})
	}
}

// Committee pruning is applied when committee state is read, not by deleting
// rows, so rolling an enactment back restores the pre-boundary answers
// exactly: the seated member is pending again, and its authorization from
// the restored epoch counts again.
func TestCommitteeEnactmentRollbackRestoresPendingAuthorization(t *testing.T) {
	t.Parallel()

	f := newTreasuryRolloverFixture(t, 100)
	cold := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(repeatByte(28, 0xe1)),
	}
	hot := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(repeatByte(28, 0xe2)),
	}
	action, err := lcommon.NewUpdateCommitteeGovAction(
		nil,
		nil,
		map[*lcommon.Credential]uint64{&cold: f.currentEpoch.EpochId + 20},
		cbor.Rat{Rat: big.NewRat(1, 1)},
	)
	require.NoError(t, err)
	actionCbor, err := cbor.Encode(action)
	require.NoError(t, err)
	ratifiedEpoch := f.currentEpoch.EpochId
	ratifiedSlot := f.currentEpoch.StartSlot + 50
	update := &models.GovernanceProposal{
		TxHash:        repeatByte(32, 0xe3),
		ActionType:    uint8(lcommon.GovActionTypeUpdateCommittee),
		ProposedEpoch: f.currentEpoch.EpochId - 2,
		ExpiresEpoch:  f.currentEpoch.EpochId + 20,
		AnchorHash:    repeatByte(32, 0xe4),
		ReturnAddress: repeatByte(29, 0xe5),
		GovActionCbor: actionCbor,
		AddedSlot:     350,
		RatifiedEpoch: &ratifiedEpoch,
		RatifiedSlot:  &ratifiedSlot,
	}
	require.NoError(t, f.db.SetGovernanceProposal(update, nil))
	raw, err := dbtest.RawSQLiteMetadata(t, f.db)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO auth_committee_hot (
    cold_credential, host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?)`, cold.Credential[:], hot.Credential[:], 2, 520)
	require.NoError(t, err)

	type committeeAnswers struct {
		hotKnown bool
		colds    []lcommon.Credential
		elected  bool
		resigned bool
	}
	answers := func(epoch, epochStartSlot uint64) committeeAnswers {
		t.Helper()
		lv := f.ls.NewView(nil).pinCommitteeState(epoch, f.currentPParams)
		lv.epochStartSlot = epochStartSlot
		member, err := lv.CommitteeHotCredentialMember(hot)
		require.NoError(t, err)
		colds, err := lv.CommitteeHotCredentialColdCredentials(hot)
		require.NoError(t, err)
		elected, err := lv.CommitteeCredentialIsElected(cold)
		require.NoError(t, err)
		coldMember, err := lv.CommitteeCredentialMember(cold)
		require.NoError(t, err)
		require.NotNil(t, coldMember)
		return committeeAnswers{
			hotKnown: member != nil,
			colds:    colds,
			elected:  elected,
			resigned: coldMember.Resigned,
		}
	}
	before := answers(f.currentEpoch.EpochId, f.currentEpoch.StartSlot)
	require.Equal(t, committeeAnswers{
		hotKnown: true,
		colds:    []lcommon.Credential{cold},
	}, before)

	result := f.rollover(t, f.currentEpoch, f.currentPParams)
	require.Equal(t, committeeAnswers{
		hotKnown: true,
		colds:    []lcommon.Credential{cold},
		elected:  true,
	}, answers(
		result.NewCurrentEpoch.EpochId,
		result.NewCurrentEpoch.StartSlot,
	))

	boundary := result.NewCurrentEpoch.StartSlot
	require.NoError(t, f.db.DeleteCommitteeMembersAfterSlot(boundary-1, nil))
	require.NoError(t, f.db.DeleteGovernanceProposalsAfterSlot(boundary-1, nil))
	require.Equal(
		t,
		before,
		answers(f.currentEpoch.EpochId, f.currentEpoch.StartSlot),
	)
	// Without the rollback, the same authorization would not survive into the
	// next epoch for a credential that stayed pending.
	require.Equal(t, committeeAnswers{
		colds: []lcommon.Credential{},
	}, answers(result.NewCurrentEpoch.EpochId, boundary))
}

// seatExpiredCommitteeMember seats a cold credential whose term ended before
// the view's epoch. cardano-ledger keeps it in committeeMembers until an
// enacted action removes it, so it is still elected.
func seatExpiredCommitteeMember(
	t *testing.T,
	db *database.Database,
	cold lcommon.Credential,
) {
	t.Helper()
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{{
		ColdCredentialTag: uint8(cold.CredType),
		ColdCredHash:      cold.Credential[:],
		ExpiresEpoch:      1,
	}}, nil))
}

// gOuroboros common.CommitteeVotingState: CommitteeHotCredentialColdCredentials
// returns every cold credential currently authorizing the exact tagged hot
// credential and does not filter by enacted membership or expiry; only a
// resigned cold credential has no authorization to return. The current
// authorizations are the csCommitteeCreds entries that survive the epoch
// boundary (Conway EPOCH updateCommitteeState).
func TestLedgerViewCommitteeHotCredentialColdCredentialsReturnsEveryAuthorization(
	t *testing.T,
) {
	t.Parallel()

	const epochStartSlot = 100
	pparams := committeeVotingConway.pparams(lcommon.ProtocolVersionVanRossem)
	lv, db := committeeTestView(t, pparams)
	lv.pinCommitteeState(5, pparams)
	lv.epochStartSlot = epochStartSlot
	hot := committeeTestCredential(0x11)
	scriptHot := lcommon.Credential{
		CredType:   lcommon.CredentialTypeScriptHash,
		Credential: hot.Credential,
	}

	seated := committeeTestCredential(0x21)
	expired := committeeTestCredential(0x22)
	resignedSeated := committeeTestCredential(0x23)
	movedAway := committeeTestCredential(0x24)
	scriptTwin := committeeTestCredential(0x25)
	seatCommitteeMembers(t, db, seated, resignedSeated, movedAway, scriptTwin)
	seatExpiredCommitteeMember(t, db, expired)
	seedCommitteeCredentialAuthorization(t, db, seated, hot, 1, 1)
	seedCommitteeCredentialAuthorization(t, db, expired, hot, 2, 1)
	seedCommitteeCredentialAuthorization(t, db, resignedSeated, hot, 3, 1)
	seedCommitteeCredentialResignation(t, db, resignedSeated, 4, 2)
	seedCommitteeCredentialAuthorization(t, db, movedAway, hot, 5, 1)
	seedCommitteeCredentialAuthorization(
		t, db, movedAway, committeeTestCredential(0x12), 6, 2,
	)
	seedCommitteeCredentialAuthorization(t, db, scriptTwin, scriptHot, 7, 1)

	pending := committeeTestCredential(0x31)
	pendingResigned := committeeTestCredential(0x32)
	pendingLastEpoch := committeeTestCredential(0x33)
	for i, cold := range []lcommon.Credential{
		pending, pendingResigned, pendingLastEpoch,
	} {
		storeCommitteeUpdateProposal(t, db, byte(0x41+i), cold, 10)
	}
	seedCommitteeCredentialAuthorization(
		t, db, pending, hot, 8, epochStartSlot,
	)
	seedCommitteeCredentialAuthorization(
		t, db, pendingResigned, hot, 9, epochStartSlot,
	)
	seedCommitteeCredentialResignation(
		t, db, pendingResigned, 10, epochStartSlot+1,
	)
	seedCommitteeCredentialAuthorization(
		t, db, pendingLastEpoch, hot, 11, epochStartSlot-1,
	)

	coldCredentials, err := lv.CommitteeHotCredentialColdCredentials(hot)
	require.NoError(t, err)
	require.ElementsMatch(
		t,
		[]lcommon.Credential{seated, expired, pending},
		coldCredentials,
	)
	coldCredentials, err = lv.CommitteeHotCredentialColdCredentials(scriptHot)
	require.NoError(t, err)
	require.Equal(t, []lcommon.Credential{scriptTwin}, coldCredentials)

	for _, cold := range []lcommon.Credential{seated, expired} {
		elected, err := lv.CommitteeCredentialIsElected(cold)
		require.NoError(t, err)
		require.True(t, elected)
	}
	elected, err := lv.CommitteeCredentialIsElected(pending)
	require.NoError(t, err)
	require.False(t, elected)
}

// Reference verdicts for one committee hot voter at PV9, PV10 and PV11:
// VotersDoNotExist unless a surviving csCommitteeCreds entry authorizes the
// hot credential (at every version), plus UnelectedCommitteeVoters from PV11
// unless that entry's cold credential is in the enacted committee. Expiry is
// not consulted by GOV.
func TestValidateTxCommitteeVoterVerdictsByProtocolVersion(t *testing.T) {
	t.Parallel()

	const epochStartSlot = 100
	type verdict int
	const (
		accept verdict = iota
		unknown
		unelected
	)
	versions := []struct {
		label string
		major uint
	}{
		{label: "PV9", major: lcommon.ProtocolVersionPlomin - 1},
		{label: "PV10", major: lcommon.ProtocolVersionPlomin},
		{label: "PV11", major: lcommon.ProtocolVersionVanRossem},
	}
	members := []struct {
		name string
		seed func(
			t *testing.T,
			db *database.Database,
			cold, hot lcommon.Credential,
		)
		verdicts [3]verdict
	}{
		{
			name: "seated",
			seed: func(t *testing.T, db *database.Database, cold, hot lcommon.Credential) {
				seatCommitteeMembers(t, db, cold)
				seedCommitteeCredentialAuthorization(t, db, cold, hot, 1, 1)
			},
			verdicts: [3]verdict{accept, accept, accept},
		},
		{
			name: "seated expired",
			seed: func(t *testing.T, db *database.Database, cold, hot lcommon.Credential) {
				seatExpiredCommitteeMember(t, db, cold)
				seedCommitteeCredentialAuthorization(t, db, cold, hot, 1, 1)
			},
			verdicts: [3]verdict{accept, accept, accept},
		},
		{
			name: "seated resigned",
			seed: func(t *testing.T, db *database.Database, cold, hot lcommon.Credential) {
				seatCommitteeMembers(t, db, cold)
				seedCommitteeCredentialAuthorization(t, db, cold, hot, 1, 1)
				seedCommitteeCredentialResignation(t, db, cold, 2, 2)
			},
			verdicts: [3]verdict{unknown, unknown, unelected},
		},
		{
			name: "pending authorized this epoch",
			seed: func(t *testing.T, db *database.Database, cold, hot lcommon.Credential) {
				seatCommitteeMembers(t, db, committeeTestCredential(0x5f))
				storeCommitteeUpdateProposal(t, db, 0x5e, cold, 10)
				seedCommitteeCredentialAuthorization(
					t, db, cold, hot, 1, epochStartSlot,
				)
			},
			verdicts: [3]verdict{accept, accept, unelected},
		},
		{
			name: "pending authorized last epoch",
			seed: func(t *testing.T, db *database.Database, cold, hot lcommon.Credential) {
				seatCommitteeMembers(t, db, committeeTestCredential(0x5f))
				storeCommitteeUpdateProposal(t, db, 0x5e, cold, 10)
				seedCommitteeCredentialAuthorization(
					t, db, cold, hot, 1, epochStartSlot-1,
				)
			},
			verdicts: [3]verdict{unknown, unknown, unelected},
		},
		{
			name: "pending resigned",
			seed: func(t *testing.T, db *database.Database, cold, hot lcommon.Credential) {
				seatCommitteeMembers(t, db, committeeTestCredential(0x5f))
				storeCommitteeUpdateProposal(t, db, 0x5e, cold, 10)
				seedCommitteeCredentialAuthorization(
					t, db, cold, hot, 1, epochStartSlot,
				)
				seedCommitteeCredentialResignation(
					t, db, cold, 2, epochStartSlot+1,
				)
			},
			verdicts: [3]verdict{unknown, unknown, unelected},
		},
	}
	for _, era := range []committeeVotingEra{
		committeeVotingConway,
		committeeVotingDijkstra,
	} {
		for _, member := range members {
			for i, version := range versions {
				name := fmt.Sprintf(
					"%s/%s/%s",
					era.name,
					member.name,
					version.label,
				)
				want := member.verdicts[i]
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					pparams := era.pparams(version.major)
					lv, db := committeeTestView(t, pparams)
					lv.pinCommitteeState(5, pparams)
					lv.epochStartSlot = epochStartSlot
					cold := committeeTestCredential(0x51)
					hot, hotKey := committeeTestVotingKey(0x52)
					member.seed(t, db, cold, hot)

					err := committeeVotingValidate(
						t, era, lv, pparams, hotKey,
						lcommon.VotingProcedures{committeeVoter(hot): {}},
						nil,
					)
					switch want {
					case accept:
						require.NoError(t, err)
					case unknown:
						requireUnknownCommitteeVoter(t, err)
					case unelected:
						var unelectedErr conway.UnelectedCommitteeVoterError
						require.ErrorAs(t, err, &unelectedErr)
					}
				})
			}
		}
	}
}

// A committee hot credential is compared with its key/script tag on both
// the seated and the unseated authorization paths: a key-hash voter is not
// admitted by a script-hash authorization with the same bytes.
func TestValidateTxCommitteeHotCredentialTagByAuthorizationPath(t *testing.T) {
	t.Parallel()

	const epochStartSlot = 100
	for _, era := range []committeeVotingEra{
		committeeVotingConway,
		committeeVotingDijkstra,
	} {
		for _, seated := range []bool{true, false} {
			for _, scriptAuthorization := range []bool{false, true} {
				name := era.name + "/pending"
				if seated {
					name = era.name + "/seated"
				}
				if scriptAuthorization {
					name += "/script-hash authorization"
				} else {
					name += "/key-hash authorization"
				}
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					pparams := era.pparams(lcommon.ProtocolVersionPlomin)
					lv, db := committeeTestView(t, pparams)
					lv.epochStartSlot = epochStartSlot
					cold := committeeTestCredential(0x61)
					voter, voterKey := committeeTestVotingKey(0x62)
					authorized := voter
					if scriptAuthorization {
						authorized = lcommon.Credential{
							CredType:   lcommon.CredentialTypeScriptHash,
							Credential: voter.Credential,
						}
					}
					if seated {
						seatCommitteeMembers(t, db, cold)
					} else {
						seatCommitteeMembers(t, db, committeeTestCredential(0x63))
						storeCommitteeUpdateProposal(t, db, 0x64, cold, 10)
					}
					seedCommitteeCredentialAuthorization(
						t, db, cold, authorized, 1, epochStartSlot,
					)

					err := committeeVotingValidate(
						t, era, lv, pparams, voterKey,
						lcommon.VotingProcedures{committeeVoter(voter): {}},
						nil,
					)
					if scriptAuthorization {
						requireUnknownCommitteeVoter(t, err)
					} else {
						require.NoError(t, err)
					}
				})
			}
		}
	}
}

// A seated committee_member row whose cold hash is not 28 bytes is corrupt
// state. Resolving an unseated authorization consults the seated set, so the
// lookup must fail rather than truncate or zero-pad the stored bytes into a
// credential: a 29-byte row whose first 28 bytes are the pending member's
// hash would otherwise hide that member's authorization, and a short row
// would otherwise be silently ignored. Validation fails closed on the error.
func TestLedgerViewCommitteeHotAuthorizationRejectsMalformedSeatedColdHash(
	t *testing.T,
) {
	t.Parallel()

	for _, length := range []int{
		lcommon.Blake2b224Size - 1,
		lcommon.Blake2b224Size + 1,
	} {
		t.Run(fmt.Sprintf("%d bytes", length), func(t *testing.T) {
			t.Parallel()
			era := committeeVotingConway
			pparams := era.pparams(lcommon.ProtocolVersionPlomin)
			lv, db := committeeTestView(t, pparams)
			pending := committeeTestCredential(0x91)
			hot, hotKey := committeeTestVotingKey(0x92)
			malformed := make([]byte, length)
			copy(malformed, pending.Credential[:])
			require.NoError(t, db.SetCommitteeMembers(
				[]*models.CommitteeMember{{
					ColdCredentialTag: uint8(pending.CredType),
					ColdCredHash:      malformed,
					ExpiresEpoch:      10,
				}},
				nil,
			))
			storeCommitteeUpdateProposal(t, db, 0x93, pending, 10)
			seedCommitteeCredentialAuthorization(t, db, pending, hot, 1, 1)

			member, err := lv.CommitteeHotCredentialMember(hot)
			require.Nil(t, member)
			require.ErrorContains(t, err, "invalid blake2b-224 hash")
			coldCredentials, err := lv.CommitteeHotCredentialColdCredentials(
				hot,
			)
			require.Nil(t, coldCredentials)
			require.ErrorContains(t, err, fmt.Sprintf("got %d", length))

			err = committeeVotingValidate(
				t, era, lv, pparams, hotKey,
				lcommon.VotingProcedures{committeeVoter(hot): {}},
				nil,
			)
			var lookup conway.CommitteeMemberLookupError
			require.ErrorAs(t, err, &lookup)
			require.ErrorContains(t, err, "invalid blake2b-224 hash")
		})
	}
}

func TestSameConnectionIdHandlesPartialNilAddrs(t *testing.T) {
	t.Parallel()

	remoteAddr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001}
	remoteOnly := ouroboros.ConnectionId{RemoteAddr: remoteAddr}
	remoteOnlySame := ouroboros.ConnectionId{
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
	}
	remoteOnlyOther := ouroboros.ConnectionId{
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3002},
	}
	localOnly := ouroboros.ConnectionId{LocalAddr: remoteAddr}
	fullId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
	}

	if !sameConnectionId(remoteOnly, remoteOnly) {
		t.Fatal(
			"sameConnectionId() = false, want true for identical remote-only ids",
		)
	}
	if !sameConnectionId(remoteOnly, remoteOnlySame) {
		t.Fatal(
			"sameConnectionId() = false, want true for equal remote-only ids",
		)
	}
	if sameConnectionId(remoteOnly, remoteOnlyOther) {
		t.Fatal(
			"sameConnectionId() = true, want false for differing remote-only ids",
		)
	}
	if sameConnectionId(remoteOnly, localOnly) {
		t.Fatal(
			"sameConnectionId() = true, want false for remote-only vs local-only",
		)
	}
	if sameConnectionId(remoteOnly, ouroboros.ConnectionId{}) {
		t.Fatal("sameConnectionId() = true, want false for remote-only vs zero")
	}
	if sameConnectionId(remoteOnly, fullId) {
		t.Fatal(
			"sameConnectionId() = true, want false for remote-only vs full id",
		)
	}
}

func TestConnIdKeyHandlesPartialNilAddrs(t *testing.T) {
	t.Parallel()

	remoteOnly := ouroboros.ConnectionId{
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
	}
	localOnly := ouroboros.ConnectionId{
		LocalAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
	}

	if connIdKey(ouroboros.ConnectionId{}) != "" {
		t.Fatal("connIdKey() != \"\" for zero value")
	}
	if connIdKey(remoteOnly) == "" {
		t.Fatal("connIdKey() = \"\" for remote-only id, want non-empty")
	}
	if connIdKey(remoteOnly) == connIdKey(localOnly) {
		t.Fatal(
			"connIdKey() equal for remote-only vs local-only, want distinct",
		)
	}
}

func TestConwayWithdrawalDRepGateCredentialBoundary(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	keyBytes := bytes.Repeat([]byte{0x42}, lcommon.Blake2b224Size)
	keyHash := lcommon.NewBlake2b224(keyBytes)
	keyRewardAddr, err := lcommon.NewAddressFromBytes(
		append([]byte{0xE1}, keyBytes...),
	)
	require.NoError(t, err)

	scriptRewardAddr, err := lcommon.NewAddress(
		"stake17xt4n07cnlafzefqvne69mmxmnzu2t9gtd27jw9d9yvc7uscsd3d3",
	)
	require.NoError(t, err)
	scriptCredential, ok := scriptRewardAddr.StakeCredential()
	require.True(t, ok)
	require.Equal(
		t,
		uint(lcommon.CredentialTypeScriptHash),
		scriptCredential.CredType,
	)

	for _, account := range []*models.Account{
		{
			StakingKey:    keyHash.Bytes(),
			CredentialTag: 0,
			Reward:        1_000_000,
			Active:        true,
		},
		{
			StakingKey:    scriptCredential.Credential.Bytes(),
			CredentialTag: 1,
			Reward:        1_000_000,
			Active:        true,
		},
	} {
		require.NoError(t, db.CreateAccount(nil, account))
	}

	ls := &LedgerState{db: db}
	pp := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: lcommon.ProtocolVersionPlomin,
		},
	}
	validate := func(t *testing.T, withdrawals map[*lcommon.Address]uint64) error {
		t.Helper()
		tx := mockledger.NewTransactionBuilder()
		tx.WithValid(true)
		tx.WithWithdrawals(withdrawals)
		txn := db.Transaction(false)
		var validationErr error
		require.NoError(t, txn.Do(func(txn *database.Txn) error {
			validationErr = conway.UtxoValidateWithdrawals(
				tx,
				0,
				&LedgerView{txn: txn, ls: ls},
				pp,
			)
			return nil
		}))
		return validationErr
	}

	t.Run("script hash is exempt", func(t *testing.T) {
		require.NoError(t, validate(t, map[*lcommon.Address]uint64{
			&scriptRewardAddr: 1_000_000,
		}))
	})

	t.Run("key hash remains gated", func(t *testing.T) {
		err := validate(t, map[*lcommon.Address]uint64{
			&keyRewardAddr: 1_000_000,
		})
		var target conway.WithdrawalNotDelegatedToDRepError
		require.ErrorAs(t, err, &target)
		require.Equal(t, keyRewardAddr, target.RewardAddress)
	})
}

// incorrectRefundSubstring is the message
// conway.CertificateRefundIncorrectError renders. These tests match on the
// message rather than on a rule index, which is an offset into an upstream
// slice and moves whenever gouroboros inserts or reorders a rule.
const incorrectRefundSubstring = "incorrect refund for certificate type 17"

const (
	// Deliberately different values. drepRefundTestPparamDeposit is what a
	// *registration* certificate must supply, and is the value a refund
	// would be judged against if the deregistration path fell back to
	// protocol parameters; drepRefundTestRecordedDeposit is what the
	// registration row actually recorded. Keeping them apart is what stops
	// these tests passing for the wrong reason.
	drepRefundTestPparamDeposit   = 1_000_000
	drepRefundTestRecordedDeposit = 500_000_000
)

func drepRefundTestPparams() *conway.ConwayProtocolParameters {
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: 9,
		},
		KeyDeposit:           2_000_000,
		DRepDeposit:          drepRefundTestPparamDeposit,
		MaxTxSize:            16_384,
		MaxValueSize:         5_000,
		CollateralPercentage: 150,
		MaxCollateralInputs:  3,
	}
}

func drepRefundTestCredential(seed byte) lcommon.Credential {
	return lcommon.Credential{
		CredType: lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(
			bytes.Repeat([]byte{seed}, lcommon.AddressHashSize),
		),
	}
}

// drepDeregistrationTx builds a real *conway.ConwayTransaction carrying a
// single DRep deregistration, which is the certificate whose refund
// conway.UtxoValidateCertificateDeposits checks against the deposit the
// ledger state reports for the credential.
func drepDeregistrationTx(
	cred lcommon.Credential,
	refund int64,
) *conway.ConwayTransaction {
	cert := &lcommon.DeregistrationDrepCertificate{
		CertType:       uint(lcommon.CertificateTypeDeregistrationDrep),
		DrepCredential: cred,
		Amount:         refund,
	}
	return &conway.ConwayTransaction{
		TxIsValid: true,
		Body: conway.ConwayTransactionBody{
			TxCertificates: []lcommon.CertificateWrapper{
				{
					Type: uint(
						lcommon.CertificateTypeDeregistrationDrep,
					),
					Certificate: cert,
				},
			},
		},
	}
}

// seedImportedDrep writes the DRep and registration rows the Mithril
// ledger-state import produces: a registration_drep row with no certificate
// behind it, so certificate_id stays 0 while deposit_amount carries the real
// amount owed. On a bootstrapped node this is frequently a DRep's only
// registration row.
func seedImportedDrep(
	t *testing.T,
	db *database.Database,
	cred lcommon.Credential,
	deposit uint64,
	slot uint64,
	active bool,
) {
	t.Helper()
	tag, err := models.CredentialTagFromUint(uint(cred.CredType))
	require.NoError(t, err)
	hash := append([]byte(nil), cred.Credential[:]...)
	require.NoError(t, db.Metadata().ImportDrep(
		&models.Drep{
			CredentialTag: tag,
			Credential:    hash,
			AddedSlot:     slot,
			Active:        active,
		},
		&models.RegistrationDrep{
			CredentialTag:  tag,
			DrepCredential: hash,
			AddedSlot:      slot,
			DepositAmount:  types.Uint64(deposit),
		},
		nil,
	))
}

// TestDRepDeregistrationRefundsRecordedDeposit is the regression test for the
// live rejection this fix addresses. LedgerView.DRepRegistration is the
// common.DRepState gouroboros consults for a DRep deregistration's refund;
// it built a DRepRegistration without assigning Deposit, so every refund was
// judged against zero and a certificate supplying the real deposit was
// rejected with "incorrect refund for certificate type 17: supplied
// 500000000, expected 0".
//
// The assertion is on the validation outcome through the production Conway
// rule with a real *LedgerView, not on the helper's return value, because the
// defect was a plausible internal value becoming the wrong consensus
// decision.
func TestDRepDeregistrationRefundsRecordedDeposit(t *testing.T) {
	t.Parallel()

	lv, db := newStakeRefundTestView(t)
	cred := drepRefundTestCredential(0xd1)
	seedImportedDrep(t, db, cred, drepRefundTestRecordedDeposit, 100, true)
	pp := drepRefundTestPparams()

	// Balanced at the recorded deposit: accepted.
	require.NoError(t, conway.UtxoValidateCertificateDeposits(
		drepDeregistrationTx(cred, drepRefundTestRecordedDeposit),
		200,
		lv,
		pp,
	))

	// A refund of zero is what the defect accepted, and it must now be
	// rejected. This is the assertion that cannot hold both before and
	// after the fix.
	require.ErrorContains(
		t,
		conway.UtxoValidateCertificateDeposits(
			drepDeregistrationTx(cred, 0),
			200,
			lv,
			pp,
		),
		incorrectRefundSubstring,
	)

	// And the recorded value must win over the protocol parameter, so the
	// acceptance above is not a fallback to DRepDeposit that happened to
	// balance.
	require.ErrorContains(
		t,
		conway.UtxoValidateCertificateDeposits(
			drepDeregistrationTx(cred, drepRefundTestPparamDeposit),
			200,
			lv,
			pp,
		),
		incorrectRefundSubstring,
	)

	// Supporting evidence for why the acceptance holds.
	reg, err := lv.DRepRegistration(cred)
	require.NoError(t, err)
	require.NotNil(t, reg)
	require.NotNil(t, reg.Deposit)
	require.Equal(t, uint64(drepRefundTestRecordedDeposit), *reg.Deposit)
}

// TestDRepRegistrationsReportRecordedDeposits covers the plural view, which
// gouroboros declares on common.DRepState alongside the singular form. It
// reads its deposits through the batched query, so this is what executes
// GetDrepLastRegistrationDeposits' derived-table join.
func TestDRepRegistrationsReportRecordedDeposits(t *testing.T) {
	t.Parallel()

	lv, db := newStakeRefundTestView(t)
	active := drepRefundTestCredential(0xd2)
	inactive := drepRefundTestCredential(0xd3)
	seedImportedDrep(t, db, active, drepRefundTestRecordedDeposit, 100, true)
	// Registered once, since deregistered. Its registration history must
	// not appear in a listing of active DReps.
	seedImportedDrep(t, db, inactive, 900_000_000, 101, false)

	regs, err := lv.DRepRegistrations()
	require.NoError(t, err)
	require.Len(t, regs, 1)
	require.Equal(t, active, regs[0].Credential)
	require.NotNil(t, regs[0].Deposit)
	require.Equal(t, uint64(drepRefundTestRecordedDeposit), *regs[0].Deposit)
}

// TestDRepRegistrationReportsNilForUnregisteredCredential pins the absence
// case the batched map leaves out entirely: a DRep row with no
// registration_drep history reports no deposit rather than an error, through
// both views.
func TestDRepRegistrationReportsNilForUnregisteredCredential(t *testing.T) {
	t.Parallel()

	lv, db := newStakeRefundTestView(t)
	cred := drepRefundTestCredential(0xd4)
	tag, err := models.CredentialTagFromUint(uint(cred.CredType))
	require.NoError(t, err)
	require.NoError(t, db.CreateDrep(nil, &models.Drep{
		CredentialTag: tag,
		Credential:    append([]byte(nil), cred.Credential[:]...),
		AddedSlot:     100,
		Active:        true,
	}))

	reg, err := lv.DRepRegistration(cred)
	require.NoError(t, err)
	require.NotNil(t, reg)
	require.Nil(t, reg.Deposit)

	regs, err := lv.DRepRegistrations()
	require.NoError(t, err)
	require.Len(t, regs, 1)
	require.Nil(t, regs[0].Deposit)
}

// inconsistentDepositSubstring is the message
// conway.DRepDepositStateInconsistentError renders when a registration
// reports no recorded deposit. Matched on the message rather than on a rule
// index, which moves whenever gouroboros inserts or reorders a rule.
const inconsistentDepositSubstring = "registered DRep credential has no recorded deposit"

// newDrepFallbackTestView returns a *LedgerView whose published consensus
// snapshot is Conway with drepRefundTestPparams, so the unknown-deposit
// fallback resolves through the same era certificate-deposit function the
// certificate write path uses. newStakeRefundTestView leaves the snapshot
// unpublished and the era Shelley, which is the "current era charges no DRep
// deposit" case rather than this one.
func newDrepFallbackTestView(
	t *testing.T,
) (*LedgerView, *database.Database) {
	t.Helper()
	ls, db := newRewardCalculationTestLedger(t)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = drepRefundTestPparams()
	ls.publishSnapshotsLocked()
	return &LedgerView{ls: ls}, db
}

// seedActiveDrepWithoutRegistration reproduces the state the vote-replay
// recovery path leaves behind. ledger/governance/processing.go calls
// InsertDrepIfAbsent when a valid DRep vote proves the credential exists
// on-chain but the metadata row was lost during recovery or bootstrap; that
// writes an active drep row and no registration_drep row at all, so the
// credential is registered with no recorded deposit.
func seedActiveDrepWithoutRegistration(
	t *testing.T,
	db *database.Database,
	cred lcommon.Credential,
	slot uint64,
) {
	t.Helper()
	tag, err := models.CredentialTagFromUint(uint(cred.CredType))
	require.NoError(t, err)
	require.NoError(t, db.InsertDrepIfAbsent(
		tag,
		cred.Credential[:],
		slot,
		"",
		nil,
		true,
		nil,
	))
	// The recovery path really does leave no registration row behind; if it
	// ever starts writing one this test is measuring the wrong thing.
	recorded, err := db.GetDrepLastRegistrationDeposit(
		tag,
		cred.Credential[:],
		nil,
	)
	require.NoError(t, err)
	require.Nil(
		t,
		recorded,
		"the recovery path must leave no recorded deposit for this test to exercise the fallback",
	)
}

// TestDrepDeregistrationFallsBackToCurrentDepositWhenUnrecorded is the
// regression test. An active DRep with no registration_drep row reported
// Deposit == nil, and gouroboros fails closed on that
// (DRepDepositStateInconsistentError), so the deregistration was rejected and
// a node reaching that block stopped making progress. The refund is now
// judged against the DRep deposit the current protocol parameters charge,
// which is what the certificate write path would have recorded.
func TestDrepDeregistrationFallsBackToCurrentDepositWhenUnrecorded(
	t *testing.T,
) {
	lv, db := newDrepFallbackTestView(t)
	cred := drepRefundTestCredential(0xe1)
	seedActiveDrepWithoutRegistration(t, db, cred, 100)

	pp := drepRefundTestPparams()
	tx := drepDeregistrationTx(cred, drepRefundTestPparamDeposit)
	// The rule is invoked directly so a nil error means the refund
	// comparison actually ran and matched, rather than that the substring
	// was absent because an unrelated rule failed first.
	require.NoError(
		t,
		conway.UtxoValidateCertificateDeposits(tx, 200, lv, pp),
	)
	if err := eras.ValidateTxConway(tx, 200, lv, pp); err != nil {
		require.NotContains(t, err.Error(), inconsistentDepositSubstring)
		require.NotContains(t, err.Error(), incorrectRefundSubstring)
	}

	// Supporting evidence for why the acceptance holds.
	reg, err := lv.DRepRegistration(cred)
	require.NoError(t, err)
	require.NotNil(t, reg)
	require.NotNil(
		t,
		reg.Deposit,
		"an unrecorded deposit must be reported as the current parameter, not as absence",
	)
	require.Equal(t, uint64(drepRefundTestPparamDeposit), *reg.Deposit)
}

// TestDrepDeregistrationRejectsWrongRefundWhenUnrecorded is the mandatory
// negative case: the fallback must not become a licence to accept any refund.
// A deregistration over the same unrecorded registration that supplies a
// different amount is still rejected, including the zero the absence would
// have been misread as.
func TestDrepDeregistrationRejectsWrongRefundWhenUnrecorded(t *testing.T) {
	lv, db := newDrepFallbackTestView(t)
	cred := drepRefundTestCredential(0xe2)
	seedActiveDrepWithoutRegistration(t, db, cred, 100)

	pp := drepRefundTestPparams()
	for _, refund := range []int64{0, drepRefundTestPparamDeposit + 1, -1} {
		err := conway.UtxoValidateCertificateDeposits(
			drepDeregistrationTx(cred, refund),
			200,
			lv,
			pp,
		)
		require.ErrorContains(t, err, incorrectRefundSubstring)
	}
}

// TestDrepDeregistrationPrefersRecordedDepositOverFallback is the second
// mandatory negative case. The recorded deposit and the current parameter are
// deliberately different values, so the two possible answers give opposite
// outcomes and the test cannot pass by accident: a recorded deposit must win,
// and the fallback must not be reached.
func TestDrepDeregistrationPrefersRecordedDepositOverFallback(t *testing.T) {
	lv, db := newDrepFallbackTestView(t)
	cred := drepRefundTestCredential(0xe3)
	require.NotEqual(
		t,
		uint64(drepRefundTestPparamDeposit),
		uint64(drepRefundTestRecordedDeposit),
		"the recorded deposit must differ from the parameter to discriminate",
	)
	seedImportedDrep(t, db, cred, drepRefundTestRecordedDeposit, 100, true)

	pp := drepRefundTestPparams()
	require.NoError(t, conway.UtxoValidateCertificateDeposits(
		drepDeregistrationTx(cred, drepRefundTestRecordedDeposit),
		200,
		lv,
		pp,
	))
	require.ErrorContains(
		t,
		conway.UtxoValidateCertificateDeposits(
			drepDeregistrationTx(cred, drepRefundTestPparamDeposit),
			200,
			lv,
			pp,
		),
		incorrectRefundSubstring,
	)
}

// TestDrepDeregistrationKeepsRecordedZeroAuthoritative pins the distinction
// the fallback must preserve. A zero dRepDeposit is a legitimate
// configuration, so a recorded zero is a real value: folding it into the
// unrecorded case would refund the current parameter and reject a valid
// deregistration.
func TestDrepDeregistrationKeepsRecordedZeroAuthoritative(t *testing.T) {
	lv, db := newDrepFallbackTestView(t)
	cred := drepRefundTestCredential(0xe4)
	seedImportedDrep(t, db, cred, 0, 100, true)

	reg, err := lv.DRepRegistration(cred)
	require.NoError(t, err)
	require.NotNil(t, reg)
	require.NotNil(
		t,
		reg.Deposit,
		"a recorded zero must stay a value, not become absence",
	)
	require.Equal(t, uint64(0), *reg.Deposit)

	pp := drepRefundTestPparams()
	require.NoError(t, conway.UtxoValidateCertificateDeposits(
		drepDeregistrationTx(cred, 0),
		200,
		lv,
		pp,
	))
	require.ErrorContains(
		t,
		conway.UtxoValidateCertificateDeposits(
			drepDeregistrationTx(cred, drepRefundTestPparamDeposit),
			200,
			lv,
			pp,
		),
		incorrectRefundSubstring,
	)
}

// TestDrepRegistrationsFallBackForUnrecordedDeposit covers the plural view,
// which builds its deposits from a batched map lookup and so has its own
// absence path.
func TestDrepRegistrationsFallBackForUnrecordedDeposit(t *testing.T) {
	lv, db := newDrepFallbackTestView(t)
	unrecorded := drepRefundTestCredential(0xe5)
	recorded := drepRefundTestCredential(0xe6)
	seedActiveDrepWithoutRegistration(t, db, unrecorded, 100)
	seedImportedDrep(t, db, recorded, drepRefundTestRecordedDeposit, 101, true)

	registrations, err := lv.DRepRegistrations()
	require.NoError(t, err)
	byCredential := map[string]*uint64{}
	for _, reg := range registrations {
		byCredential[string(reg.Credential.Credential[:])] = reg.Deposit
	}

	got := byCredential[string(unrecorded.Credential[:])]
	require.NotNil(t, got, "the plural view must not report absence either")
	require.Equal(t, uint64(drepRefundTestPparamDeposit), *got)

	got = byCredential[string(recorded.Credential[:])]
	require.NotNil(t, got)
	require.Equal(t, uint64(drepRefundTestRecordedDeposit), *got)
}

// TestDrepRegistrationReportsAbsenceInAPreConwayEra keeps the fallback from
// inventing a refund where the current era charges no DRep deposit.
//
// The era has to be published for this to mean anything. CertDepositShelley
// through CertDepositBabbage have no *RegistrationDrepCertificate case and
// fall through to "default: return 0, nil", so before the drepDepositParams
// guard this reported a non-nil zero and gouroboros accepted a zero refund
// instead of failing closed.
func TestDrepRegistrationReportsAbsenceInAPreConwayEra(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	ls.currentEra = eras.BabbageEraDesc
	ls.currentPParams = &babbage.BabbageProtocolParameters{
		KeyDeposit: 2_000_000,
		MaxTxSize:  16_384,
	}
	ls.publishSnapshotsLocked()
	lv := &LedgerView{ls: ls}
	// The era's own deposit function really does answer zero-without-error
	// for a DRep registration, which is what makes the guard load-bearing
	// rather than defensive.
	deposit, err := eras.BabbageEraDesc.CertDepositFunc(
		&lcommon.RegistrationDrepCertificate{},
		ls.currentPParams,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(0), deposit)

	cred := drepRefundTestCredential(0xe7)
	seedActiveDrepWithoutRegistration(t, db, cred, 100)

	reg, err := lv.DRepRegistration(cred)
	require.NoError(t, err)
	require.NotNil(t, reg)
	require.Nil(
		t,
		reg.Deposit,
		"a pre-Conway era has no DRep deposit to fall back to; absence must be reported",
	)

	registrations, err := lv.DRepRegistrations()
	require.NoError(t, err)
	require.Len(t, registrations, 1)
	require.Nil(
		t,
		registrations[0].Deposit,
		"the plural view must report absence in a pre-Conway era too",
	)

	require.ErrorContains(
		t,
		conway.UtxoValidateCertificateDeposits(
			drepDeregistrationTx(cred, drepRefundTestPparamDeposit),
			200,
			lv,
			drepRefundTestPparams(),
		),
		inconsistentDepositSubstring,
	)
}

// TestDrepRegistrationReportsAbsenceWithoutAPublishedSnapshot covers the other
// way the fallback has nothing to report: no consensus snapshot has been
// published yet, so there are no current parameters to read. Kept separate
// from the pre-Conway case because newStakeRefundTestView reaches this branch
// and never the era one -- the era field it sets is never read.
func TestDrepRegistrationReportsAbsenceWithoutAPublishedSnapshot(t *testing.T) {
	lv, db := newStakeRefundTestView(t)
	require.Nil(
		t,
		lv.ls.loadConsensusSnapshot(),
		"this fixture must leave the snapshot unpublished for this test to mean what it says",
	)
	cred := drepRefundTestCredential(0xe8)
	seedActiveDrepWithoutRegistration(t, db, cred, 100)

	reg, err := lv.DRepRegistration(cred)
	require.NoError(t, err)
	require.NotNil(t, reg)
	require.Nil(t, reg.Deposit)

	require.ErrorContains(
		t,
		conway.UtxoValidateCertificateDeposits(
			drepDeregistrationTx(cred, drepRefundTestPparamDeposit),
			200,
			lv,
			drepRefundTestPparams(),
		),
		inconsistentDepositSubstring,
	)
}

// TestDrepRegistrationReportsAbsenceForTypedNilParams covers the other way the
// capability assertion can be satisfied without a usable deposit behind it.
//
// A typed-nil *DijkstraProtocolParameters implements drepDepositParams, so the
// call-site guard admits it; CertDepositDijkstra then asserted the type
// successfully and dereferenced nil, panicking inside DRep view construction.
// It now reports ErrIncompatibleProtocolParams, which currentDRepDeposit turns
// into absence, so gouroboros fails closed as it does for every other
// no-deposit-available case.
//
// The two guards are complementary and both are asserted here: the era helper
// is what stops the panic, and the call-site capability check is what keeps a
// pre-Conway era from reaching it at all.
func TestDrepRegistrationReportsAbsenceForTypedNilParams(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	ls.currentEra = eras.DijkstraEraDesc
	ls.currentPParams = (*dijkstra.DijkstraProtocolParameters)(nil)
	ls.publishSnapshotsLocked()
	lv := &LedgerView{ls: ls}

	// The typed nil really does satisfy the capability the call-site guard
	// tests, so this case reaches the era helper rather than stopping early.
	_, implements := ls.currentPParams.(drepDepositParams)
	require.True(
		t,
		implements,
		"a typed-nil pointer must still satisfy drepDepositParams for this test to exercise the era helper",
	)

	cred := drepRefundTestCredential(0xe9)
	seedActiveDrepWithoutRegistration(t, db, cred, 100)

	require.NotPanics(t, func() {
		reg, err := lv.DRepRegistration(cred)
		require.NoError(t, err)
		require.NotNil(t, reg)
		require.Nil(
			t,
			reg.Deposit,
			"unusable parameters must report absence, not a fabricated deposit",
		)
	})
	require.NotPanics(t, func() {
		registrations, err := lv.DRepRegistrations()
		require.NoError(t, err)
		require.Len(t, registrations, 1)
		require.Nil(t, registrations[0].Deposit)
	})

	require.ErrorContains(
		t,
		conway.UtxoValidateCertificateDeposits(
			drepDeregistrationTx(cred, drepRefundTestPparamDeposit),
			200,
			lv,
			drepRefundTestPparams(),
		),
		inconsistentDepositSubstring,
	)
}

// epochBoundaryBenchPartialPrecompute commits the first half of the round's
// pool chunks and stops, as a restart between two chunks would.
func epochBoundaryBenchPartialPrecompute(
	b *testing.B,
	f *epochBoundaryBenchFixture,
) {
	epochBoundaryBenchPartialPrecomputeT(b, f)
}

func epochBoundaryBenchPartialPrecomputeT(
	b testing.TB,
	f *epochBoundaryBenchFixture,
) {
	b.Helper()
	evt := epochBoundaryBenchPrecomputeEvent()
	round, ok, err := f.ls.resolveStakeRewardPrecomputeRound(
		evt.NewEpoch+1,
		evt.BoundarySlot,
		epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch+1),
	)
	require.NoError(b, err)
	require.True(b, ok)
	chunks := (len(round.poolInputs) + f.ls.rewardPrecomputeChunkSize() - 1) /
		f.ls.rewardPrecomputeChunkSize()
	for range chunks / 2 {
		done, err := f.ls.stakeRewardPrecomputeChunkStep(round)
		require.NoError(b, err)
		require.False(b, done)
	}
}

func (ls *LedgerState) waitEpochBoundaryBenchBackground() {
	_ = ls.WaitEpochBoundaryJob(context.Background())
	ls.ratificationWG.Wait()
	ls.deferredStakeInputsWG.Wait()
	ls.rewardCreditCompactionWG.Wait()
}

// wireDeferredBoundarySnapshot mirrors node.go's deferred mark snapshot
// wiring.
func wireDeferredBoundarySnapshot(ls *LedgerState, mgr *snapshot.Manager) {
	ls.SetDeferredEpochBoundarySnapshotHooks(
		mgr.DeferEpochBoundaryCapture,
		mgr.DiscardEpochBoundaryCapture,
		func(
			txn *database.Txn,
			evt event.EpochTransitionEvent,
		) (DeferredBoundarySnapshot, error) {
			return mgr.PrepareEpochBoundarySnapshot(
				context.Background(), txn, evt,
			)
		},
	)
}

// epochBoundaryBenchWireDeferred mirrors node.go's
// wireDeferredRewardStakeInputs.
func epochBoundaryBenchWireDeferred(ls *LedgerState, mgr *snapshot.Manager) {
	ls.SetEpochBoundaryDeferredStakeInputsHook(
		func(
			txn *database.Txn,
		) (uint64, uint64, []*models.RewardStakeInput, bool) {
			deferred, ok := mgr.TakeDeferredRewardStakeInputs(txn)
			if !ok {
				return 0, 0, nil, false
			}
			return deferred.Epoch, deferred.BoundarySlot, deferred.Inputs, true
		},
	)
	mgr.SetDeferRewardStakeInputs(true)
}

// epochBoundaryBenchShape is the row-count shape of a synthetic mainnet-like
// ledger. The defaults follow the mainnet 655->656 boundary: 1,309,350
// delegators across 2,676 pools and 1,053 DReps.
type epochBoundaryBenchShape struct {
	pools             int
	delegators        int
	dreps             int
	utxosPerDelegator int
	proposals         int
	drepVotes         int
	spoVotes          int
	ccMembers         int
}

func epochBoundaryBenchShapeFromEnv(tb testing.TB) epochBoundaryBenchShape {
	tb.Helper()
	shape := epochBoundaryBenchShape{
		pools:             2_676,
		delegators:        1_309_350,
		dreps:             1_053,
		utxosPerDelegator: 2,
		proposals:         40,
		drepVotes:         400,
		spoVotes:          300,
		ccMembers:         7,
	}
	envInt := func(name string, dst *int) {
		raw := os.Getenv(name)
		if raw == "" {
			return
		}
		v, err := strconv.Atoi(raw)
		require.NoError(tb, err, name)
		*dst = v
	}
	envInt("DINGO_BENCH_POOLS", &shape.pools)
	envInt("DINGO_BENCH_DELEGATORS", &shape.delegators)
	envInt("DINGO_BENCH_DREPS", &shape.dreps)
	envInt("DINGO_BENCH_UTXOS_PER_DELEGATOR", &shape.utxosPerDelegator)
	envInt("DINGO_BENCH_PROPOSALS", &shape.proposals)
	return shape
}

const (
	epochBoundaryBenchEpochLength = uint64(432_000)
	// The rollover under measurement ends this epoch, so it applies the
	// reward round whose snapshot, performance and pots epochs are 8, 9 and
	// 10.
	epochBoundaryBenchEndedEpoch = uint64(10)
	epochBoundaryBenchMaxSupply  = uint64(45_000_000_000_000_000)
	epochBoundaryBenchReserves   = uint64(7_600_000_000_000_000)
	epochBoundaryBenchTreasury   = uint64(1_600_000_000_000_000)
	epochBoundaryBenchFees       = uint64(31_000_000_000)
	epochBoundaryBenchBlocks     = 21_600
)

func epochBoundaryBenchStart(epoch uint64) uint64 {
	return epoch * epochBoundaryBenchEpochLength
}

func epochBoundaryBenchNodeConfig(tb testing.TB) *cardano.CardanoNodeConfig {
	tb.Helper()
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("5a", 32),
	}
	require.NoError(tb, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.05,
		"epochLength": 432000,
		"maxLovelaceSupply": 45000000000000000,
		"securityParam": 2160,
		"slotLength": 1,
		"updateQuorum": 5,
		"systemStart": "2017-09-23T21:44:51Z"
	}`)))
	return cfg
}

func epochBoundaryBenchPParams() *conway.ConwayProtocolParameters {
	rat := func(n, d int64) *cbor.Rat { return &cbor.Rat{Rat: big.NewRat(n, d)} }
	p := donationTestConwayPParams(10)
	p.MinFeeA = 44
	p.MinFeeB = 155_381
	p.MaxBlockBodySize = 90_112
	p.MaxTxSize = 16_384
	p.MaxBlockHeaderSize = 1_100
	p.KeyDeposit = 2_000_000
	p.PoolDeposit = 500_000_000
	p.MaxEpoch = 18
	p.NOpt = 500
	p.A0 = rat(3, 10)
	p.Rho = rat(3, 1000)
	p.Tau = rat(1, 5)
	p.MinPoolCost = 170_000_000
	p.AdaPerUtxoByte = 4_310
	p.MinCommitteeSize = 7
	p.CommitteeTermLimit = 146
	p.GovActionValidityPeriod = 6
	p.GovActionDeposit = 100_000_000_000
	p.DRepDeposit = 500_000_000
	p.DRepInactivityPeriod = 20
	p.MinFeeRefScriptCostPerByte = rat(15, 1)
	return p
}

// epochBoundaryBenchHash returns a deterministic 28-byte hash in its own
// domain, so credentials, pools and DReps never collide.
func epochBoundaryBenchHash(domain byte, index uint64) []byte {
	h := make([]byte, 28)
	h[0] = domain
	binary.BigEndian.PutUint64(h[20:], index)
	return h
}

// splitmix64 gives the fixture a heavy-tailed but reproducible stake
// distribution without seeding math/rand.
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// epochBoundaryBenchStake is log-uniform between 1 ADA and 100,000 ADA,
// which puts 1.3M delegators at roughly 11B ADA of active stake.
func epochBoundaryBenchStake(index uint64) uint64 {
	u := float64(splitmix64(index)>>11) / float64(1<<53)
	return uint64(math.Pow(10, 6+5*u))
}

type epochBoundaryBenchFixture struct {
	ls      *LedgerState
	db      *database.Database
	shape   epochBoundaryBenchShape
	pparams *conway.ConwayProtocolParameters
	epochs  map[uint64]models.Epoch
	phases  *epochBoundaryPhaseRecorder
}

// epochBoundaryPhaseRecorder collects the "epoch rollover phase" Debug records
// timeRolloverPhase emits, so the benchmark reports the same per-phase
// durations an operator reads from the log.
type epochBoundaryPhaseRecorder struct {
	mu     sync.Mutex
	phases []epochBoundaryPhase
}

type epochBoundaryPhase struct {
	name     string
	duration time.Duration
}

func (r *epochBoundaryPhaseRecorder) Enabled(
	context.Context,
	slog.Level,
) bool {
	return true
}

func (r *epochBoundaryPhaseRecorder) Handle(
	_ context.Context,
	rec slog.Record,
) error {
	if rec.Message != "epoch rollover phase" {
		return nil
	}
	var name string
	var seconds float64
	rec.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "phase":
			name = a.Value.String()
		case "duration_seconds":
			seconds = a.Value.Float64()
		}
		return true
	})
	r.mu.Lock()
	r.phases = append(r.phases, epochBoundaryPhase{
		name:     name,
		duration: time.Duration(seconds * float64(time.Second)),
	})
	r.mu.Unlock()
	return nil
}

func (r *epochBoundaryPhaseRecorder) WithAttrs([]slog.Attr) slog.Handler {
	return r
}

func (r *epochBoundaryPhaseRecorder) WithGroup(string) slog.Handler {
	return r
}

func (r *epochBoundaryPhaseRecorder) reset() {
	r.mu.Lock()
	r.phases = nil
	r.mu.Unlock()
}

func (r *epochBoundaryPhaseRecorder) snapshot() []epochBoundaryPhase {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]epochBoundaryPhase(nil), r.phases...)
}

// newEpochBoundaryBenchFixture seeds a fresh database, or copies the seeded
// template in templateDir when one is named.
func newEpochBoundaryBenchFixture(
	tb testing.TB,
	shape epochBoundaryBenchShape,
	templateDir string,
) *epochBoundaryBenchFixture {
	tb.Helper()
	dataDir := tb.TempDir()
	if template := templateDir; template != "" {
		epochBoundaryBenchTemplate(tb, template, shape)
		require.NoError(tb, os.CopyFS(
			dataDir, os.DirFS(filepath.Join(template, "data")),
		))
	}
	db, err := dbtest.NewDatabase(tb, &database.Config{DataDir: dataDir})
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	f := &epochBoundaryBenchFixture{
		db:      db,
		shape:   shape,
		pparams: epochBoundaryBenchPParams(),
		epochs:  epochBoundaryBenchEpochs(),
		phases:  &epochBoundaryPhaseRecorder{},
	}
	if templateDir == "" {
		f.seed(tb)
	}
	f.wire(tb)
	return f
}

// epochBoundaryBenchTemplate seeds the shared template once, so repeated
// runs -- and runs of two different trees -- measure the same database.
func epochBoundaryBenchTemplate(
	tb testing.TB,
	dir string,
	shape epochBoundaryBenchShape,
) {
	tb.Helper()
	ready := filepath.Join(dir, "ready")
	want := fmt.Sprintf("%+v", shape)
	if raw, err := os.ReadFile(ready); err == nil {
		require.Equal(tb, want, string(raw), "template shape mismatch")
		return
	}
	dataDir := filepath.Join(dir, "data")
	require.NoError(tb, os.RemoveAll(dataDir))
	require.NoError(tb, os.MkdirAll(dataDir, 0o755))
	db, err := dbtest.NewDatabase(tb, &database.Config{DataDir: dataDir})
	require.NoError(tb, err)
	f := &epochBoundaryBenchFixture{
		db:      db,
		shape:   shape,
		pparams: epochBoundaryBenchPParams(),
		epochs:  epochBoundaryBenchEpochs(),
	}
	f.seed(tb)
	require.NoError(tb, dbtest.CloseDatabase(db))
	require.NoError(tb, os.WriteFile(ready, []byte(want), 0o644))
}

func (f *epochBoundaryBenchFixture) seed(tb testing.TB) {
	tb.Helper()
	start := time.Now()
	f.seedEpochs(tb)
	f.seedBulk(tb)
	bulk := time.Now()
	f.seedGovernance(tb)
	tb.Logf(
		"seeded: bulk %.1fs, governance %.1fs",
		bulk.Sub(start).Seconds(), time.Since(bulk).Seconds(),
	)
}

func (f *epochBoundaryBenchFixture) wire(tb testing.TB) {
	tb.Helper()
	db := f.db
	f.ls = &LedgerState{
		db:             db,
		currentEra:     eras.ConwayEraDesc,
		currentEpoch:   f.epochs[epochBoundaryBenchEndedEpoch],
		currentPParams: f.pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: epochBoundaryBenchNodeConfig(tb),
			Logger:            slog.New(f.phases),
		},
	}
	mgr := snapshot.NewManager(db, nil, slog.New(slog.NewTextHandler(
		io.Discard, nil,
	)))
	f.ls.SetEpochBoundarySnapshotStakeHook(
		func(txn *database.Txn, evt event.EpochTransitionEvent) error {
			return mgr.ComputeEpochBoundarySnapshot(
				context.Background(),
				txn,
				evt,
			)
		},
	)
	f.ls.SetEpochBoundarySnapshotHook(
		func(txn *database.Txn, evt event.EpochTransitionEvent) error {
			return mgr.CaptureEpochBoundarySnapshot(
				context.Background(),
				txn,
				evt,
			)
		},
	)
	epochBoundaryBenchWireDeferred(f.ls, mgr)
	wireDeferredBoundarySnapshot(f.ls, mgr)
	f.ls.SetCurrentBoundarySPOStakeHook(
		func(
			txn *database.Txn,
			evt event.EpochTransitionEvent,
		) ([]*models.PoolStakeSnapshot, error) {
			return mgr.CurrentBoundarySPOStakeRows(
				context.Background(),
				txn,
				evt,
			)
		},
	)
}

func epochBoundaryBenchNonce(epoch uint64) []byte {
	nonce := make([]byte, 32)
	binary.BigEndian.PutUint64(nonce[24:], epoch+1)
	return nonce
}

func epochBoundaryBenchEpochs() map[uint64]models.Epoch {
	epochs := make(map[uint64]models.Epoch)
	for epoch := uint64(0); epoch <= epochBoundaryBenchEndedEpoch; epoch++ {
		nonce := epochBoundaryBenchNonce(epoch)
		epochs[epoch] = models.Epoch{
			EpochId:             epoch,
			StartSlot:           epochBoundaryBenchStart(epoch),
			Nonce:               nonce,
			EvolvingNonce:       nonce,
			CandidateNonce:      nonce,
			LastEpochBlockNonce: nonce,
			EraId:               eras.ConwayEraDesc.Id,
			SlotLength:          1_000,
			LengthInSlots:       uint(epochBoundaryBenchEpochLength),
		}
	}
	return epochs
}

func (f *epochBoundaryBenchFixture) seedEpochs(tb testing.TB) {
	tb.Helper()
	pparamsCbor, err := cbor.Encode(f.pparams)
	require.NoError(tb, err)
	for epoch := uint64(0); epoch <= epochBoundaryBenchEndedEpoch; epoch++ {
		e := f.epochs[epoch]
		require.NoError(tb, f.db.SetEpoch(
			e.StartSlot, epoch, e.Nonce, e.EvolvingNonce, e.CandidateNonce,
			e.LastEpochBlockNonce, e.EraId, e.SlotLength, e.LengthInSlots,
			nil,
		))
		require.NoError(tb, f.db.SetPParams(
			pparamsCbor, e.StartSlot, epoch, eras.ConwayEraDesc.Id, nil,
		))
	}
	require.NoError(tb, f.db.Metadata().SetNetworkState(
		epochBoundaryBenchTreasury,
		epochBoundaryBenchReserves,
		epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch),
		nil,
	))
}

type epochBoundaryBenchPool struct {
	key           []byte
	rewardAccount []byte
	margin        string
	cost          uint64
	pledge        uint64
	stake         uint64
	delegators    int
}

// seedBulk writes the delegator-scaled tables directly through SQL: at 1.3M
// rows, the model writers' per-row bookkeeping would dominate setup.
func (f *epochBoundaryBenchFixture) seedBulk(tb testing.TB) {
	tb.Helper()
	raw, err := dbtest.RawSQLiteMetadata(tb, f.db)
	require.NoError(tb, err)
	defer raw.Close()
	tx, err := raw.Begin()
	require.NoError(tb, err)
	defer func() { _ = tx.Rollback() }()
	prepare := func(query string) *sql.Stmt {
		stmt, err := tx.Prepare(query)
		require.NoError(tb, err)
		return stmt
	}
	poolStmt := prepare(`
INSERT INTO pool (id, pool_key_hash, vrf_key_hash, reward_account,
    reward_account_credential_tag, margin, pledge, cost)
VALUES (?, ?, ?, ?, 0, ?, ?, ?)`)
	poolRegStmt := prepare(`
INSERT INTO pool_registration (id, pool_id, pool_key_hash, vrf_key_hash,
    reward_account, reward_account_credential_tag, margin, pledge, cost,
    added_slot, deposit_amount, deposit_held)
VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, 1, '500000000', '500000000')`)
	ownerStmt := prepare(`
INSERT INTO pool_registration_owner (key_hash, pool_registration_id, pool_id)
VALUES (?, ?, ?)`)
	accountStmt := prepare(`
INSERT INTO account (staking_key, credential_tag, pool, drep, drep_type,
    added_slot, created_slot, reward, active, expiration_epoch)
VALUES (?, 0, ?, ?, ?, 1, 1, ?, 1, 0)`)
	liveStmt := prepare(`
INSERT INTO reward_live_stake (pool_key_hash, staking_key, credential_tag,
    utxo_stake, reward_stake, total_stake, registered, pool_delegation_slot,
    updated_slot, calculation_version)
VALUES (?, ?, 0, ?, ?, ?, 1, 1, 1, ?)`)
	utxoStmt := prepare(`
INSERT INTO utxo (tx_id, output_idx, payment_key, staking_key, credential_tag,
    added_slot, deleted_slot, amount, payment_script)
VALUES (?, ?, ?, ?, 0, 1, 0, ?, 0)`)
	stakeInputStmt := prepare(`
INSERT INTO reward_stake_input (pool_key_hash, staking_key, epoch,
    credential_tag, stake, owner, registered, captured_slot, boundary_slot)
VALUES (?, ?, ?, 0, ?, ?, 1, ?, ?)`)
	poolInputStmt := prepare(`
INSERT INTO reward_pool_input (margin, pool_key_hash, reward_account, epoch,
    pledge, delegated_stake, owner_stake, cost, delegator_count,
    reward_account_credential_tag, captured_slot, boundary_slot)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)`)
	poolSnapStmt := prepare(`
INSERT INTO pool_stake_snapshot (epoch, snapshot_type, pool_key_hash,
    total_stake, delegator_count, captured_slot, calculation_version)
VALUES (?, 'mark', ?, ?, ?, ?, ?)`)
	blockStmt := prepare(`
INSERT INTO pool_opcert_sequence (pool_key_hash, slot, sequence)
VALUES (?, ?, 0)`)

	shape := f.shape
	pools := make([]epochBoundaryBenchPool, shape.pools)
	for p := range pools {
		pool := &pools[p]
		pool.key = epochBoundaryBenchHash(0x10, uint64(p)+1)
		pool.rewardAccount = epochBoundaryBenchHash(0x20, uint64(p)+1)
		// Margins span the reference's rounding extremes: 0, 1 and
		// ordinary fractions.
		switch p % 7 {
		case 0:
			pool.margin = "0/1"
		case 1:
			pool.margin = "1/1"
		default:
			pool.margin = fmt.Sprintf("%d/1000", 5+(p%40))
		}
		pool.cost = 170_000_000 + uint64(p%5)*85_000_000
		pool.pledge = 1_000_000_000 * (1 + uint64(p%50))
	}
	// Every pool's own reward account delegates its pledge to the pool, so
	// the pledge check passes and owners are exercised.
	calcVersion := models.RewardStakeCalculationVersion
	var nextUtxo uint64
	delegatorPool := func(d int) int {
		// A skewed assignment: low-numbered pools attract more delegators,
		// and the last pool attracts none, so a zero-stake pool is present.
		u := float64(splitmix64(uint64(d)^0xabcdef)>>11) / float64(1<<53)
		p := int(float64(shape.pools-1) * u * u)
		if p >= shape.pools-1 {
			p = shape.pools - 2
		}
		return p
	}
	type stakeRow struct {
		pool  int
		key   []byte
		stake uint64
		owner bool
	}
	rows := make([]stakeRow, 0, shape.delegators+shape.pools)
	writeAccount := func(
		key []byte, pool []byte, stake uint64, reward uint64, d uint64,
	) {
		var drep any
		drepType := models.DrepTypeAddrKeyHash
		switch r := splitmix64(d^0x77) % 20; {
		case r < 11 && shape.dreps > 0:
			drep = epochBoundaryBenchHash(
				0x40,
				splitmix64(d)%uint64(shape.dreps)+1,
			)
		case r < 14:
			drepType = models.DrepTypeAlwaysAbstain
		case r < 15:
			drepType = models.DrepTypeAlwaysNoConfidence
		default:
			drepType = 0
		}
		_, err := accountStmt.Exec(
			key, pool, drep, drepType, strconv.FormatUint(reward, 10),
		)
		require.NoError(tb, err)
		utxoStake := stake - reward
		_, err = liveStmt.Exec(
			pool, key,
			strconv.FormatUint(utxoStake, 10),
			strconv.FormatUint(reward, 10),
			strconv.FormatUint(stake, 10),
			calcVersion,
		)
		require.NoError(tb, err)
		n := max(shape.utxosPerDelegator, 1)
		remaining := utxoStake
		for i := range n {
			amount := remaining / uint64(n-i)
			remaining -= amount
			nextUtxo++
			txID := make([]byte, 32)
			binary.BigEndian.PutUint64(txID[24:], nextUtxo)
			_, err := utxoStmt.Exec(
				txID, 0, epochBoundaryBenchHash(0x50, nextUtxo), key,
				strconv.FormatUint(amount, 10),
			)
			require.NoError(tb, err)
		}
	}
	for p := range pools {
		pool := &pools[p]
		_, err := poolStmt.Exec(
			p+1, pool.key, epochBoundaryBenchHash(0x11, uint64(p)+1),
			pool.rewardAccount, pool.margin,
			strconv.FormatUint(pool.pledge, 10),
			strconv.FormatUint(pool.cost, 10),
		)
		require.NoError(tb, err)
		_, err = poolRegStmt.Exec(
			p+1, p+1, pool.key, epochBoundaryBenchHash(0x11, uint64(p)+1),
			pool.rewardAccount, pool.margin,
			strconv.FormatUint(pool.pledge, 10),
			strconv.FormatUint(pool.cost, 10),
		)
		require.NoError(tb, err)
		_, err = ownerStmt.Exec(pool.rewardAccount, p+1, p+1)
		require.NoError(tb, err)
		if p == shape.pools-1 {
			// The zero-stake pool: registered, no delegators, not even its
			// owner.
			continue
		}
		ownerStake := pool.pledge
		writeAccount(
			pool.rewardAccount, pool.key, ownerStake, 0,
			uint64(p)+0x1_0000_0000,
		)
		rows = append(rows, stakeRow{
			pool: p, key: pool.rewardAccount, stake: ownerStake, owner: true,
		})
		pool.stake += ownerStake
		pool.delegators++
	}
	for d := range shape.delegators {
		p := delegatorPool(d)
		key := epochBoundaryBenchHash(0x30, uint64(d)+1)
		stake := epochBoundaryBenchStake(uint64(d))
		reward := stake / 200
		writeAccount(key, pools[p].key, stake, reward, uint64(d))
		rows = append(rows, stakeRow{pool: p, key: key, stake: stake})
		pools[p].stake += stake
		pools[p].delegators++
	}
	var totalStake uint64
	for _, pool := range pools {
		totalStake += pool.stake
	}
	// The go, set and mark reward bases (snapshot epochs 8, 9, 10) and
	// their leader-election rows. Stake inputs are identical across the
	// three epochs: only the row count matters to the boundary's cost.
	for _, epoch := range []uint64{8, 9, 10} {
		boundary := epochBoundaryBenchStart(epoch)
		captured := boundary - 1
		for _, row := range rows {
			_, err := stakeInputStmt.Exec(
				pools[row.pool].key, row.key, epoch,
				strconv.FormatUint(row.stake, 10), row.owner,
				captured, boundary,
			)
			require.NoError(tb, err)
		}
		var poolCount, delegatorCount int
		for p := range pools {
			pool := &pools[p]
			if pool.delegators == 0 {
				continue
			}
			poolCount++
			delegatorCount += pool.delegators
			_, err := poolInputStmt.Exec(
				pool.margin, pool.key, pool.rewardAccount, epoch,
				strconv.FormatUint(pool.pledge, 10),
				strconv.FormatUint(pool.stake, 10),
				strconv.FormatUint(pool.pledge, 10),
				strconv.FormatUint(pool.cost, 10),
				pool.delegators, captured, boundary,
			)
			require.NoError(tb, err)
			_, err = poolSnapStmt.Exec(
				epoch, pool.key, strconv.FormatUint(pool.stake, 10),
				pool.delegators, captured, calcVersion,
			)
			require.NoError(tb, err)
		}
		_, err := tx.Exec(`
INSERT INTO reward_snapshot (epoch, snapshot_type, total_active_stake,
    total_pool_count, total_delegators, captured_slot, boundary_slot,
    epoch_nonce, protocol_version, authoritative, calculation_version,
    excluded_active_stake)
VALUES (?, 'mark', ?, ?, ?, ?, ?, ?, 10, 1, ?, '0')`,
			epoch, strconv.FormatUint(totalStake, 10), poolCount,
			delegatorCount, captured, boundary,
			f.epochs[epoch].Nonce, calcVersion,
		)
		require.NoError(tb, err)
		_, err = tx.Exec(`
INSERT INTO epoch_summary (epoch, total_active_stake, total_pool_count,
    total_delegators, epoch_nonce, boundary_slot, snapshot_ready)
VALUES (?, ?, ?, ?, ?, ?, 1)`,
			epoch, strconv.FormatUint(totalStake, 10), poolCount,
			delegatorCount, f.epochs[epoch].Nonce, boundary,
		)
		require.NoError(tb, err)
	}
	// Performance epoch 9: blocks in proportion to stake.
	perfStart := epochBoundaryBenchStart(9)
	slot := perfStart
	for p := range pools {
		share := uint64(
			float64(epochBoundaryBenchBlocks) *
				float64(pools[p].stake) / float64(totalStake),
		)
		for range share {
			_, err := blockStmt.Exec(pools[p].key, slot)
			require.NoError(tb, err)
			slot += 20
		}
	}
	_, err = tx.Exec(`
INSERT INTO reward_ada_pots (epoch, treasury, reserves, fees, rewards,
    captured_slot)
VALUES (?, ?, ?, ?, '0', ?)`,
		epochBoundaryBenchEndedEpoch,
		strconv.FormatUint(epochBoundaryBenchTreasury, 10),
		strconv.FormatUint(epochBoundaryBenchReserves, 10),
		strconv.FormatUint(epochBoundaryBenchFees, 10),
		epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch),
	)
	require.NoError(tb, err)
	_, err = tx.Exec(
		`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
		make([]byte, 32),
		epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch+1)-1,
		10_000_000,
	)
	require.NoError(tb, err)
	require.NoError(tb, tx.Commit())
}

func (f *epochBoundaryBenchFixture) seedGovernance(tb testing.TB) {
	tb.Helper()
	shape := f.shape
	raw, err := dbtest.RawSQLiteMetadata(tb, f.db)
	require.NoError(tb, err)
	defer raw.Close()
	tx, err := raw.Begin()
	require.NoError(tb, err)
	defer func() { _ = tx.Rollback() }()
	drepStmt, err := tx.Prepare(`
INSERT INTO drep (credential, credential_tag, added_slot, last_activity_epoch,
    expiry_epoch, active)
VALUES (?, 0, 1, ?, ?, 1)`)
	require.NoError(tb, err)
	drepRegStmt, err := tx.Prepare(`
INSERT INTO registration_drep (drep_credential, credential_tag, added_slot,
    deposit_amount)
VALUES (?, 0, 1, '500000000')`)
	require.NoError(tb, err)
	for r := range shape.dreps {
		cred := epochBoundaryBenchHash(0x40, uint64(r)+1)
		_, err := drepStmt.Exec(cred, epochBoundaryBenchEndedEpoch, 40)
		require.NoError(tb, err)
		_, err = drepRegStmt.Exec(cred)
		require.NoError(tb, err)
	}
	for c := range shape.ccMembers {
		_, err := tx.Exec(`
INSERT INTO auth_committee_hot (cold_credential, host_credential,
    certificate_id, added_slot)
VALUES (?, ?, ?, 1)`,
			epochBoundaryBenchHash(0x60, uint64(c)+1),
			epochBoundaryBenchHash(0x61, uint64(c)+1),
			c+1,
		)
		require.NoError(tb, err)
	}
	require.NoError(tb, tx.Commit())

	members := make([]*models.CommitteeMember, 0, shape.ccMembers)
	for c := range shape.ccMembers {
		members = append(members, &models.CommitteeMember{
			ColdCredHash: epochBoundaryBenchHash(0x60, uint64(c)+1),
			ExpiresEpoch: 100,
			AddedSlot:    1,
		})
	}
	require.NoError(tb, f.db.SetCommitteeMembers(members, nil))
	require.NoError(tb, f.db.SetCommitteeQuorum(big.NewRat(2, 3), 1, nil))

	for i := range shape.proposals {
		returnKey := epochBoundaryBenchHash(0x30, uint64(i)*997+1)
		returnAddr, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeNoneKey, lcommon.AddressNetworkMainnet,
			nil, returnKey,
		)
		require.NoError(tb, err)
		returnAddrBytes, err := returnAddr.Bytes()
		require.NoError(tb, err)
		var actionType lcommon.GovActionType
		var actionCbor []byte
		if i%4 == 0 {
			actionType = lcommon.GovActionTypeTreasuryWithdrawal
			actionCbor, err = cbor.Encode(&lcommon.TreasuryWithdrawalGovAction{
				Type: uint(lcommon.GovActionTypeTreasuryWithdrawal),
				Withdrawals: map[*lcommon.Address]uint64{
					&returnAddr: 1_000_000_000_000,
				},
			})
		} else {
			actionType = lcommon.GovActionTypeInfo
			actionCbor, err = cbor.Encode(&lcommon.InfoGovAction{
				Type: uint(lcommon.GovActionTypeInfo),
			})
		}
		require.NoError(tb, err)
		txHash := make([]byte, 32)
		binary.BigEndian.PutUint64(txHash[24:], uint64(i)+1)
		txHash[0] = 0x70
		proposal := &models.GovernanceProposal{
			TxHash:        txHash,
			ActionIndex:   0,
			ActionType:    uint8(actionType),
			ProposedEpoch: epochBoundaryBenchEndedEpoch - uint64(i%3),
			ExpiresEpoch:  epochBoundaryBenchEndedEpoch + 6,
			AnchorURL:     "https://example.invalid/proposal",
			AnchorHash:    txHash,
			Deposit:       f.pparams.GovActionDeposit,
			ReturnAddress: returnAddrBytes,
			GovActionCbor: actionCbor,
			AddedSlot: epochBoundaryBenchStart(
				epochBoundaryBenchEndedEpoch,
			) + 100,
		}
		require.NoError(tb, f.db.SetGovernanceProposal(proposal, nil))
		vote := func(voterType uint8, cred []byte, choice uint8) {
			require.NoError(tb, f.db.SetGovernanceVote(&models.GovernanceVote{
				ProposalID:      proposal.ID,
				VoterType:       voterType,
				VoterCredential: cred,
				Vote:            choice,
				AddedSlot:       proposal.AddedSlot + 1,
			}, nil))
		}
		for v := range min(shape.drepVotes, shape.dreps) {
			r := (uint64(i)*131 + uint64(v)) % uint64(shape.dreps)
			vote(
				models.VoterTypeDRep,
				epochBoundaryBenchHash(0x40, r+1),
				uint8(splitmix64(r^uint64(i))%3),
			)
		}
		for v := range min(shape.spoVotes, shape.pools) {
			p := (uint64(i)*17 + uint64(v)) % uint64(shape.pools)
			vote(
				models.VoterTypeSPO,
				epochBoundaryBenchHash(0x10, p+1),
				uint8(splitmix64(p^uint64(i))%3),
			)
		}
		for c := range shape.ccMembers {
			vote(
				models.VoterTypeCC,
				epochBoundaryBenchHash(0x61, uint64(c)+1),
				models.VoteYes,
			)
		}
	}
}

// rollover runs the real boundary in one write transaction, exactly as the
// block pipeline does, and returns the time spent inside the transaction body
// and in its commit.
func (f *epochBoundaryBenchFixture) rollover(
	tb testing.TB,
) (time.Duration, time.Duration, []epochBoundaryPhase) {
	tb.Helper()
	f.phases.reset()
	start := time.Now()
	f.ls.fenceRewardPrecompute()
	var bodyDone time.Time
	txn := f.db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		_, err := f.ls.processEpochRollover(
			txn,
			f.epochs[epochBoundaryBenchEndedEpoch],
			eras.ConwayEraDesc,
			f.pparams,
			false,
		)
		bodyDone = time.Now()
		return err
	})
	require.NoError(tb, err)
	end := time.Now()
	return bodyDone.Sub(start), end.Sub(bodyDone), f.phases.snapshot()
}

// precomputeEvent is the epoch transition into the ended epoch: the event
// the reward precompute for the measured boundary is queued from.
func epochBoundaryBenchPrecomputeEvent() event.EpochTransitionEvent {
	return event.EpochTransitionEvent{
		PreviousEpoch: epochBoundaryBenchEndedEpoch - 1,
		NewEpoch:      epochBoundaryBenchEndedEpoch,
		BoundarySlot:  epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch),
		SnapshotSlot: epochBoundaryBenchStart(
			epochBoundaryBenchEndedEpoch,
		) - 1,
	}
}

func reportEpochBoundaryPhases(
	b *testing.B,
	label string,
	body, commit time.Duration,
	phases []epochBoundaryPhase,
) {
	b.Helper()
	sorted := append([]epochBoundaryPhase(nil), phases...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].duration > sorted[j].duration
	})
	var sb strings.Builder
	fmt.Fprintf(
		&sb, "%s: whole boundary %.3fs (body %.3fs, commit %.3fs)",
		label, (body + commit).Seconds(), body.Seconds(), commit.Seconds(),
	)
	for _, phase := range sorted {
		fmt.Fprintf(&sb, "; %s %.3fs", phase.name, phase.duration.Seconds())
	}
	b.Log(sb.String())
	b.ReportMetric((body + commit).Seconds(), "boundary_s")
	for _, phase := range phases {
		b.ReportMetric(phase.duration.Seconds(), phase.name+"_s")
	}
}

// BenchmarkEpochBoundaryMainnetShape measures the whole epoch boundary --
// every phase of processEpochRollover plus its commit -- on a mainnet-shaped
// ledger, with the reward precompute complete, partial and missing. Run it
// with -benchtime=1x: each sub-benchmark seeds its own database, which takes
// longer than the boundary it measures. DINGO_BENCH_DELEGATORS and
// DINGO_BENCH_POOLS scale the shape down for a quick run.
func BenchmarkEpochBoundaryMainnetShape(b *testing.B) {
	shape := epochBoundaryBenchShapeFromEnv(b)
	for _, state := range []string{"complete", "partial", "missing"} {
		b.Run("precompute="+state, func(b *testing.B) {
			for range b.N {
				b.StopTimer()
				f := newEpochBoundaryBenchFixture(
					b, shape, os.Getenv("DINGO_BENCH_TEMPLATE_DIR"),
				)
				precomputeStart := time.Now()
				switch state {
				case "complete":
					require.NoError(
						b,
						f.ls.precomputeStakeRewardsAfterEpochTransition(
							epochBoundaryBenchPrecomputeEvent(),
						),
					)
				case "partial":
					epochBoundaryBenchPartialPrecompute(b, f)
				}
				b.Logf(
					"precompute (%s, off the apply path): %.3fs",
					state, time.Since(precomputeStart).Seconds(),
				)
				b.StartTimer()
				body, commit, phases := f.rollover(b)
				b.StopTimer()
				reportEpochBoundaryPhases(
					b, "precompute="+state, body, commit, phases,
				)
				completion := time.Now()
				f.ls.waitEpochBoundaryBenchBackground()
				b.Logf(
					"background completion after the boundary: %.3fs",
					time.Since(completion).Seconds(),
				)
			}
		})
	}
}

// epochBoundaryDumpShape is small enough to seed in seconds and still covers
// the rounding and eligibility edges: margins 0 and 1, a zero-stake pool,
// owners, DRep, always-abstain and no-confidence delegators.
func epochBoundaryDumpShape() epochBoundaryBenchShape {
	shape := epochBoundaryBenchShape{
		pools:             23,
		delegators:        1_500,
		dreps:             17,
		utxosPerDelegator: 2,
		proposals:         6,
		drepVotes:         12,
		spoVotes:          9,
		ccMembers:         3,
	}
	if raw := os.Getenv("DINGO_BOUNDARY_DUMP_DELEGATORS"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			shape.delegators = n
			shape.pools = max(shape.pools, n/400)
		}
	}
	return shape
}

// dumpEpochBoundaryState renders every table an epoch boundary writes, in a
// stable order and without surrogate keys, so the same boundary on two code
// versions can be compared byte for byte.
func dumpEpochBoundaryState(t *testing.T, raw *sql.DB) string {
	t.Helper()
	queries := []struct{ name, query string }{
		{"account", `SELECT credential_tag, hex(staking_key), reward, active,
    hex(pool), hex(drep), drep_type FROM account
ORDER BY credential_tag, staking_key`},
		{"account_reward_delta", `SELECT credential_tag, hex(staking_key),
    hex(tx_hash), amount, previous_reward, added_slot, withdrawal,
    post_snapshot FROM account_reward_delta
ORDER BY credential_tag, staking_key, tx_hash, added_slot, withdrawal`},
		{"reward_live_stake", `SELECT credential_tag, hex(staking_key),
    hex(pool_key_hash), utxo_stake, reward_stake, total_stake, registered,
    pool_delegation_slot, updated_slot, calculation_version
FROM reward_live_stake ORDER BY credential_tag, staking_key`},
		{"network_state", `SELECT slot, treasury, reserves FROM network_state
ORDER BY slot`},
		{"reward_ada_pots", `SELECT epoch, treasury, reserves, fees, rewards,
    captured_slot FROM reward_ada_pots ORDER BY epoch`},
		{"reward_pool_output", `SELECT epoch, hex(pool_key_hash),
    apparent_performance, optimal_reward, total_reward, leader_reward,
    member_reward_total, owner_stake, undistributed, unspendable,
    boundary_slot FROM reward_pool_output ORDER BY epoch, pool_key_hash`},
		{"reward_account_output", `SELECT epoch, credential_tag,
    hex(staking_key), hex(pool_key_hash), reward_type, amount, spendable,
    guarded, boundary_slot FROM reward_account_output
ORDER BY epoch, credential_tag, staking_key, pool_key_hash, reward_type`},
		{
			"pool_stake_snapshot",
			`SELECT epoch, snapshot_type, hex(pool_key_hash),
    total_stake, delegator_count, captured_slot, calculation_version,
    reward_account_auto_vote, reward_account_auto_vote_resolved
FROM pool_stake_snapshot ORDER BY epoch, snapshot_type, pool_key_hash`,
		},
		{"reward_snapshot", `SELECT epoch, snapshot_type, total_active_stake,
    total_pool_count, total_delegators, captured_slot, boundary_slot,
    hex(epoch_nonce), protocol_version, authoritative, calculation_version,
    excluded_active_stake FROM reward_snapshot
ORDER BY epoch, snapshot_type`},
		{"reward_pool_input", `SELECT epoch, hex(pool_key_hash), margin,
    hex(reward_account), pledge, delegated_stake, owner_stake, cost,
    delegator_count, captured_slot, boundary_slot FROM reward_pool_input
ORDER BY epoch, pool_key_hash`},
		{
			"reward_stake_input",
			`SELECT epoch, hex(pool_key_hash), credential_tag,
    hex(staking_key), stake, owner, registered, captured_slot, boundary_slot
FROM reward_stake_input
ORDER BY epoch, pool_key_hash, credential_tag, staking_key`,
		},
		{"epoch_summary", `SELECT epoch, total_active_stake, total_pool_count,
    total_delegators, hex(epoch_nonce), boundary_slot, snapshot_ready
FROM epoch_summary ORDER BY epoch`},
		{"governance_proposal", `SELECT hex(tx_hash), action_index,
    enacted_epoch, enacted_slot, ratified_epoch, ratified_slot, expired_epoch,
    expired_slot FROM governance_proposal ORDER BY tx_hash, action_index`},
		{"drep", `SELECT credential_tag, hex(credential), active,
    last_activity_epoch, expiry_epoch FROM drep
ORDER BY credential_tag, credential`},
		{"epoch", `SELECT epoch_id, start_slot, hex(nonce), hex(evolving_nonce),
    hex(candidate_nonce), era_id FROM epoch ORDER BY epoch_id`},
	}
	var sb strings.Builder
	for _, q := range queries {
		rows, err := raw.Query(q.query)
		require.NoError(t, err, q.name)
		cols, err := rows.Columns()
		require.NoError(t, err)
		fmt.Fprintf(&sb, "== %s\n", q.name)
		for rows.Next() {
			values := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			require.NoError(t, rows.Scan(ptrs...))
			for i, v := range values {
				if b, ok := v.([]byte); ok {
					v = string(b)
				}
				if i > 0 {
					sb.WriteString("|")
				}
				fmt.Fprintf(&sb, "%v", v)
			}
			sb.WriteString("\n")
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	return sb.String()
}

// dumpDRepVotingPower renders every DRep's voting power as governance reads it
// at the end of the boundary.
func dumpDRepVotingPower(t *testing.T, f *epochBoundaryBenchFixture) string {
	t.Helper()
	dreps, err := f.db.GetActiveDreps(nil)
	require.NoError(t, err)
	refs := make([]models.StakeCredentialRef, 0, len(dreps))
	for _, drep := range dreps {
		refs = append(refs, models.NewStakeCredentialRef(
			drep.CredentialTag, drep.Credential,
		))
	}
	powers, err := f.db.GetDRepVotingPowerBatch(refs, 0, nil)
	require.NoError(t, err)
	byType, err := f.db.GetDRepVotingPowerByType(
		[]uint64{
			models.DrepTypeAlwaysAbstain, models.DrepTypeAlwaysNoConfidence,
		}, 0, nil,
	)
	require.NoError(t, err)
	lines := make([]string, 0, len(powers)+2)
	for key, power := range powers {
		lines = append(lines, fmt.Sprintf("%x=%d", key, power))
	}
	sort.Strings(lines)
	lines = append(lines, fmt.Sprintf(
		"abstain=%d no_confidence=%d",
		byType[models.DrepTypeAlwaysAbstain],
		byType[models.DrepTypeAlwaysNoConfidence],
	))
	return strings.Join(lines, "\n") + "\n"
}

// deregisterEpochBoundaryDumpDelegators deregisters every 97th delegator
// during the ended epoch, after any precompute ran, the way a deregistration
// certificate does: the account, its live stake row and the certificate row.
// Their rewards become unspendable at the boundary.
func deregisterEpochBoundaryDumpDelegators(t *testing.T, raw *sql.DB) {
	t.Helper()
	shape := epochBoundaryDumpShape()
	for d := 0; d < shape.delegators; d += 97 {
		key := epochBoundaryBenchHash(0x30, uint64(d)+1)
		slot := epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch) + 1_000 +
			uint64(d)
		for _, stmt := range []string{
			`UPDATE account SET active = 0, pool = NULL, added_slot = ?
WHERE credential_tag = 0 AND staking_key = ?`,
			`UPDATE reward_live_stake SET registered = 0, pool_key_hash = NULL,
    updated_slot = ? WHERE credential_tag = 0 AND staking_key = ?`,
			`INSERT INTO deregistration (added_slot, staking_key, credential_tag,
    amount) VALUES (?, ?, 0, '2000000')`,
		} {
			_, err := raw.Exec(stmt, slot, key)
			require.NoError(t, err)
		}
	}
}

// TestEpochBoundaryDumpForDifferential runs one boundary on the dump fixture
// for each precompute state and writes the resulting state to
// $DINGO_BOUNDARY_DUMP_DIR, so the same test on two code versions produces
// files to diff. It is skipped unless that directory is set.
func TestEpochBoundaryDumpForDifferential(t *testing.T) {
	t.Parallel()
	dir := os.Getenv("DINGO_BOUNDARY_DUMP_DIR")
	if dir == "" {
		t.Skip("set DINGO_BOUNDARY_DUMP_DIR to write boundary dumps")
	}
	for _, state := range []string{"complete", "partial", "missing"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			f := newEpochBoundaryBenchFixture(t, epochBoundaryDumpShape(), "")
			switch state {
			case "complete":
				require.NoError(
					t,
					f.ls.precomputeStakeRewardsAfterEpochTransition(
						epochBoundaryBenchPrecomputeEvent(),
					),
				)
			case "partial":
				epochBoundaryBenchPartialPrecomputeT(t, f)
			}
			raw, err := dbtest.RawSQLiteMetadata(t, f.db)
			require.NoError(t, err)
			defer raw.Close()
			deregisterEpochBoundaryDumpDelegators(t, raw)
			f.rollover(t)
			f.ls.waitEpochBoundaryBenchBackground()
			// DRep power is read from the derived balances; the tables are
			// dumped with every credit folded into its account, the shape an
			// eager boundary writes.
			power := dumpDRepVotingPower(t, f)
			settleRewardCredits(t, f.ls)
			dump := dumpEpochBoundaryState(t, raw) + "== drep_power\n" +
				power
			require.NoError(t, os.WriteFile(
				filepath.Join(dir, "boundary-"+state+".txt"),
				[]byte(dump), 0o644,
			))
		})
	}
}

func smallEpochBoundaryBenchShape() epochBoundaryBenchShape {
	return epochBoundaryBenchShape{
		pools:             6,
		delegators:        60,
		dreps:             4,
		utxosPerDelegator: 1,
		proposals:         2,
		drepVotes:         4,
		spoVotes:          3,
		ccMembers:         3,
	}
}

// TestEpochRolloverMarkSnapshotIncludesSameBoundaryRewards pins the SNAP
// ordering contract: the mark snapshot captured at a boundary includes the
// reward round that boundary applies, so every credited delegator's frozen
// stake is its pre-boundary stake plus its reward.
func TestEpochRolloverMarkSnapshotIncludesSameBoundaryRewards(t *testing.T) {
	t.Parallel()
	f := newEpochBoundaryBenchFixture(t, smallEpochBoundaryBenchShape(), "")
	require.NoError(t, f.ls.precomputeStakeRewardsAfterEpochTransition(
		epochBoundaryBenchPrecomputeEvent(),
	))
	before, err := f.db.Metadata().GetRewardStakeInputs(
		epochBoundaryBenchEndedEpoch, nil,
	)
	require.NoError(t, err)
	outputs, err := f.db.Metadata().GetRewardAccountOutputs(8, nil)
	require.NoError(t, err)
	require.NotEmpty(t, outputs)

	f.rollover(t)
	f.ls.waitEpochBoundaryBenchBackground()

	after, err := f.db.Metadata().GetRewardStakeInputs(
		epochBoundaryBenchEndedEpoch+1, nil,
	)
	require.NoError(t, err)
	credit := make(map[string]uint64)
	for _, output := range outputs {
		if output.Spendable && !output.Guarded {
			credit[string(output.StakingKey)] += uint64(output.Amount)
		}
	}
	require.NotEmpty(t, credit)
	stakeAfter := make(map[string]uint64, len(after))
	for _, input := range after {
		stakeAfter[string(input.StakingKey)] = uint64(input.Stake)
	}
	for _, input := range before {
		key := string(input.StakingKey)
		require.Equal(
			t, uint64(input.Stake)+credit[key], stakeAfter[key],
			"mark stake of %x must include the reward credited at the"+
				" same boundary", input.StakingKey,
		)
	}
}

// TestBoundaryRechecksRegistrationChangedAfterPrecompute pins the boundary's
// eligibility recheck: a delegator deregistered after the precompute ran is
// not credited, and exactly its reward moves to the treasury, although the
// precompute recorded its output as spendable.
func TestBoundaryRechecksRegistrationChangedAfterPrecompute(t *testing.T) {
	t.Parallel()
	run := func(deregister bool) (*epochBoundaryBenchFixture, uint64) {
		f := newEpochBoundaryBenchFixture(t, epochBoundaryDumpShape(), "")
		require.NoError(t, f.ls.precomputeStakeRewardsAfterEpochTransition(
			epochBoundaryBenchPrecomputeEvent(),
		))
		if deregister {
			raw, err := dbtest.RawSQLiteMetadata(t, f.db)
			require.NoError(t, err)
			deregisterEpochBoundaryDumpDelegators(t, raw)
			require.NoError(t, raw.Close())
		}
		f.rollover(t)
		f.ls.waitEpochBoundaryBenchBackground()
		state, err := f.db.Metadata().GetNetworkState(nil)
		require.NoError(t, err)
		return f, uint64(state.Treasury)
	}
	_, treasuryKept := run(false)
	f, treasuryDeregistered := run(true)

	deregistered := make(map[string]bool)
	for d := 0; d < epochBoundaryDumpShape().delegators; d += 97 {
		deregistered[string(epochBoundaryBenchHash(0x30, uint64(d)+1))] = true
	}
	outputs, err := f.db.Metadata().GetRewardAccountOutputs(8, nil)
	require.NoError(t, err)
	var moved uint64
	for _, output := range outputs {
		if !deregistered[string(output.StakingKey)] {
			continue
		}
		require.False(
			t, output.Spendable,
			"a delegator deregistered before the boundary is unspendable",
		)
		moved += uint64(output.Amount)
	}
	require.NotZero(t, moved, "fixture must deregister a rewarded delegator")
	for key := range deregistered {
		account, err := f.db.GetAccountByCredential(0, []byte(key), true, nil)
		require.NoError(t, err)
		require.Equal(
			t, stakeRewardSeedReward(key), uint64(account.Reward),
			"a deregistered delegator is not credited",
		)
	}
	require.Equal(
		t, treasuryKept+moved, treasuryDeregistered,
		"exactly the deregistered delegators' rewards move to the treasury",
	)
}

// TestBoundaryPromotesRewardAfterReregistration pins the other eligibility
// transition: a row made nonspendable by a deregistration before precompute is
// credited when the credential registers and delegates again before boundary.
func TestBoundaryPromotesRewardAfterReregistration(t *testing.T) {
	t.Parallel()
	type outcome struct {
		storedReward uint64
		balance      *uint64
		treasury     uint64
		amount       uint64
		spendable    bool
	}
	run := func(reregister bool) outcome {
		f := newEpochBoundaryBenchFixture(t, epochBoundaryDumpShape(), "")
		key := epochBoundaryBenchHash(0x30, 1)
		raw, err := dbtest.RawSQLiteMetadata(t, f.db)
		require.NoError(t, err)
		var pool []byte
		require.NoError(t, raw.QueryRow(
			`SELECT pool FROM account WHERE credential_tag = 0 AND staking_key = ?`,
			key,
		).Scan(&pool))
		deregisterSlot := epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch) + 1_000
		_, err = raw.Exec(`
UPDATE account SET active = 0, pool = NULL, added_slot = ?
WHERE credential_tag = 0 AND staking_key = ?`, deregisterSlot, key)
		require.NoError(t, err)
		_, err = raw.Exec(`
UPDATE reward_live_stake SET registered = 0, pool_key_hash = NULL,
    updated_slot = ? WHERE credential_tag = 0 AND staking_key = ?`,
			deregisterSlot, key,
		)
		require.NoError(t, err)
		_, err = raw.Exec(`
INSERT INTO deregistration (added_slot, staking_key, credential_tag, amount)
VALUES (?, ?, 0, '2000000')`, deregisterSlot, key)
		require.NoError(t, err)
		require.NoError(t, raw.Close())

		require.NoError(t, f.ls.precomputeStakeRewardsAfterEpochTransition(
			epochBoundaryBenchPrecomputeEvent(),
		))
		precomputed, err := f.db.Metadata().GetRewardAccountOutputs(8, nil)
		require.NoError(t, err)
		var targetAmount uint64
		for _, output := range precomputed {
			if string(output.StakingKey) == string(key) {
				require.False(t, output.Spendable,
					"precompute observes the deregistered account")
				targetAmount += uint64(output.Amount)
			}
		}
		require.Positive(t, targetAmount)

		if reregister {
			raw, err = dbtest.RawSQLiteMetadata(t, f.db)
			require.NoError(t, err)
			registerSlot := deregisterSlot + 1_000
			_, err = raw.Exec(`
UPDATE account SET active = 1, pool = ?, added_slot = ?
WHERE credential_tag = 0 AND staking_key = ?`, pool, registerSlot, key)
			require.NoError(t, err)
			_, err = raw.Exec(`
UPDATE reward_live_stake SET registered = 1, pool_key_hash = ?,
    updated_slot = ? WHERE credential_tag = 0 AND staking_key = ?`,
				pool, registerSlot, key,
			)
			require.NoError(t, err)
			_, err = raw.Exec(`
INSERT INTO registration (staking_key, credential_tag, added_slot)
VALUES (?, 0, ?)`, key, registerSlot)
			require.NoError(t, err)
			_, err = raw.Exec(`
INSERT INTO stake_delegation
    (staking_key, credential_tag, pool_key_hash, added_slot)
VALUES (?, 0, ?, ?)`, key, pool, registerSlot)
			require.NoError(t, err)
			require.NoError(t, raw.Close())
		}

		f.rollover(t)
		f.ls.waitEpochBoundaryBenchBackground()
		account, err := f.db.GetAccountByCredential(0, key, true, nil)
		require.NoError(t, err)
		var balance *uint64
		if account.Active {
			balance, err = (&LedgerView{ls: f.ls}).RewardAccountBalance(
				lcommon.Credential{
					CredType: lcommon.CredentialTypeAddrKeyHash,
					Credential: lcommon.CredentialHash(
						lcommon.NewBlake2b224(key),
					),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, balance)
		}
		outputs, err := f.db.Metadata().GetRewardAccountOutputs(8, nil)
		require.NoError(t, err)
		var spendable bool
		for _, output := range outputs {
			if string(output.StakingKey) == string(key) {
				spendable = output.Spendable
			}
		}
		state, err := f.db.Metadata().GetNetworkState(nil)
		require.NoError(t, err)
		return outcome{
			storedReward: uint64(account.Reward), balance: balance,
			treasury: uint64(state.Treasury),
			amount:   targetAmount, spendable: spendable,
		}
	}

	deregistered := run(false)
	reregistered := run(true)
	require.False(t, deregistered.spendable)
	require.True(t, reregistered.spendable)
	baseReward := stakeRewardSeedReward(string(epochBoundaryBenchHash(0x30, 1)))
	require.Equal(t, baseReward, deregistered.storedReward)
	require.Equal(t, baseReward, reregistered.storedReward,
		"the boundary keeps deferred credits out of account.reward")
	require.Nil(t, deregistered.balance)
	require.NotNil(t, reregistered.balance)
	require.Equal(t, baseReward+reregistered.amount, *reregistered.balance)
	require.Equal(t, deregistered.treasury,
		reregistered.treasury+reregistered.amount,
		"the re-registered reward moves from treasury to the account")
}

// stakeRewardSeedReward is the reward balance the fixture seeds for a
// delegator key.
func stakeRewardSeedReward(key string) uint64 {
	index := binary.BigEndian.Uint64([]byte(key)[20:]) - 1
	return epochBoundaryBenchStake(index) / 200
}

// newHookTestLedger builds a minimal LedgerState with a discard logger, matching
// the logger NewLedgerState installs when none is configured.
func newHookTestLedger(t *testing.T) (*LedgerState, *database.Database) {
	t.Helper()
	db := newDonationTestDB(t)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	return ls, db
}

// TestCaptureEpochBoundarySnapshotHookNil verifies the rollover capture is a
// no-op (and does not error) when no hook is installed — preserving the
// event-driven fallback-only behavior.
func TestCaptureEpochBoundarySnapshotHookNil(t *testing.T) {
	t.Parallel()

	ls, db := newHookTestLedger(t)

	result := &EpochRolloverResult{
		NewCurrentEpoch: models.Epoch{EpochId: 1, StartSlot: 432000},
	}
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.captureEpochBoundarySnapshot(
			txn, models.Epoch{EpochId: 0}, result,
		)
	}))
}

// TestCaptureEpochBoundarySnapshotHookInvoked verifies the hook is called inside
// the rollover transaction with an event derived from the new/previous epoch.
func TestCaptureEpochBoundarySnapshotHookInvoked(t *testing.T) {
	t.Parallel()

	ls, db := newHookTestLedger(t)

	var called bool
	var got event.EpochTransitionEvent
	ls.SetEpochBoundarySnapshotHook(
		func(_ *database.Txn, evt event.EpochTransitionEvent) error {
			called = true
			got = evt
			return nil
		},
	)

	result := &EpochRolloverResult{
		NewCurrentEpoch: models.Epoch{
			EpochId:   1,
			StartSlot: 432000,
			Nonce:     []byte{0xaa, 0xbb},
		},
	}
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.captureEpochBoundarySnapshot(
			txn, models.Epoch{EpochId: 0}, result,
		)
	}))

	require.True(t, called, "hook must be invoked during the rollover")
	require.Equal(t, uint64(0), got.PreviousEpoch)
	require.Equal(t, uint64(1), got.NewEpoch)
	require.Equal(t, uint64(432000), got.BoundarySlot)
	require.Equal(t, uint64(431999), got.SnapshotSlot)
	require.Equal(t, []byte{0xaa, 0xbb}, got.EpochNonce)
}

// TestCaptureEpochBoundarySnapshotHookFailureDeferred verifies that a hook
// failure is swallowed (the rollover is not aborted) and that the failed
// capture's writes are rolled back to the savepoint rather than committed.
func TestCaptureEpochBoundarySnapshotHookFailureDeferred(t *testing.T) {
	t.Parallel()

	ls, db := newHookTestLedger(t)

	ls.SetEpochBoundarySnapshotHook(
		func(txn *database.Txn, evt event.EpochTransitionEvent) error {
			// Write a row, then fail: the savepoint rollback must discard it.
			if err := db.Metadata().SaveRewardSnapshot(&models.RewardSnapshot{
				Epoch:           evt.NewEpoch,
				SnapshotType:    "mark",
				CapturedSlot:    1,
				BoundarySlot:    1,
				ProtocolVersion: 8,
				Authoritative:   true,
			}, txn.Metadata()); err != nil {
				return err
			}
			return errors.New("capture boom")
		},
	)

	result := &EpochRolloverResult{
		NewCurrentEpoch: models.Epoch{EpochId: 1, StartSlot: 432000},
	}
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		// Must NOT surface the hook error: capture failures defer to the
		// event-driven fallback rather than wedging the rollover.
		return ls.captureEpochBoundarySnapshot(
			txn, models.Epoch{EpochId: 0}, result,
		)
	}))

	snap, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.Nil(t, snap,
		"a failed capture must be rolled back to the savepoint, not committed")
}

const (
	forecastBoundaryEpoch      = uint64(4)
	forecastByronEpochLength   = uint(21_600)
	forecastShelleyEpochLength = uint(432_000)
	forecastBoundaryStartSlot  = uint64(64_800)
	forecastWithinEraStartSlot = uint64(43_200)
)

func newEpochCacheForecastLedger(
	t *testing.T,
	epoch models.Epoch,
	transition hardfork.TransitionInfo,
	configuredBoundary bool,
) *LedgerState {
	t.Helper()
	cfg := newTestEraHistoryCfg(t)
	cfg.ShelleyGenesisHash = strings.Repeat("01", 32)
	if configuredBoundary {
		enabled := true
		boundary := forecastBoundaryEpoch
		cfg.ExperimentalHardForksEnabled = &enabled
		cfg.TestShelleyHardForkAtEpoch = &boundary
	}
	ls := &LedgerState{
		currentEpoch:   epoch,
		currentEra:     eras.ByronEraDesc,
		epochCache:     []models.Epoch{epoch},
		transitionInfo: transition,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger: slog.New(
				slog.NewJSONHandler(io.Discard, nil),
			),
		},
	}
	ls.publishSnapshotsLocked()
	return ls
}

func TestAdvanceEpochCacheRejectsHardForkBoundary(t *testing.T) {
	t.Parallel()

	require.NotEqual(t, forecastByronEpochLength, forecastShelleyEpochLength,
		"fixture must expose the previous-era length overlap")
	lastByronEpoch := models.Epoch{
		EpochId:       forecastBoundaryEpoch - 1,
		StartSlot:     forecastBoundaryStartSlot,
		LengthInSlots: forecastByronEpochLength,
		SlotLength:    20_000,
		EraId:         eras.ByronEraDesc.Id,
	}

	for _, tc := range []struct {
		name               string
		transition         hardfork.TransitionInfo
		configuredBoundary bool
	}{
		{
			name:       "confirmed transition",
			transition: hardfork.NewTransitionKnown(forecastBoundaryEpoch),
		},
		{
			name:               "configured epoch trigger",
			transition:         hardfork.NewTransitionUnknown(),
			configuredBoundary: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ls := newEpochCacheForecastLedger(
				t, lastByronEpoch, tc.transition, tc.configuredBoundary,
			)

			err := ls.advanceEpochCache()
			require.ErrorContains(t, err, "hard-fork boundary")
			require.ErrorIs(t, err, errEpochCacheForecastBoundary)
			require.Len(t, ls.loadConsensusSnapshot().epochCache, 1,
				"forecast must not publish a previous-era boundary row")

			_, err = ls.epochForSlot(
				lastByronEpoch.StartSlot + uint64(lastByronEpoch.LengthInSlots),
			)
			require.Error(t, err,
				"post-fork slot must remain uncovered until full rollover")
		})
	}
}

func TestAdvanceEpochCachePreservesWithinEraForecast(t *testing.T) {
	t.Parallel()

	lastByronEpoch := models.Epoch{
		EpochId:       forecastBoundaryEpoch - 2,
		StartSlot:     forecastWithinEraStartSlot,
		LengthInSlots: forecastByronEpochLength,
		SlotLength:    20_000,
		EraId:         eras.ByronEraDesc.Id,
	}
	ls := newEpochCacheForecastLedger(
		t,
		lastByronEpoch,
		hardfork.NewTransitionKnown(forecastBoundaryEpoch),
		true,
	)

	require.NoError(t, ls.advanceEpochCache())
	cache := ls.loadConsensusSnapshot().epochCache
	require.Len(t, cache, 2)
	forecast := cache[1]
	require.Equal(t, forecastBoundaryEpoch-1, forecast.EpochId)
	require.Equal(t, eras.ByronEraDesc.Id, forecast.EraId)
	require.Equal(t, forecastByronEpochLength, forecast.LengthInSlots)

	got, err := ls.epochForSlot(forecast.StartSlot)
	require.NoError(t, err)
	require.Equal(t, forecast, got)
}

func TestHeaderVerificationEpochDefersAtHardForkBoundary(t *testing.T) {
	t.Parallel()

	lastByronEpoch := models.Epoch{
		EpochId:       forecastBoundaryEpoch - 1,
		StartSlot:     forecastBoundaryStartSlot,
		LengthInSlots: forecastByronEpochLength,
		SlotLength:    20_000,
		EraId:         eras.ByronEraDesc.Id,
	}
	ls := newEpochCacheForecastLedger(
		t,
		lastByronEpoch,
		hardfork.NewTransitionKnown(forecastBoundaryEpoch),
		false,
	)

	_, err := ls.headerVerificationEpoch(
		lastByronEpoch.StartSlot+uint64(lastByronEpoch.LengthInSlots),
		true,
	)
	require.ErrorContains(t, err, "hard-fork boundary")
	require.ErrorIs(t, err, errHeaderVerificationDeferred,
		"boundary wait must not be classified as an honest-peer fault")
	require.Len(t, ls.loadConsensusSnapshot().epochCache, 1)
}

// TestEpochNonceUsesCarriedLastEpochBlockNonce verifies the cardano-ledger
// epoch-nonce assembly (#2734): the epoch nonce mixes the frozen candidate with
// the CARRIED last-block-of-previous-epoch nonce
// (prevEpoch.LastEpochBlockNonce == cardano praosStateLastEpochBlockNonce),
// NOT the hash of the last block of the epoch being closed. The closing epoch's
// last block hash is stored on the new epoch record for use at the NEXT
// boundary.
//
// Confirmed against preview mainnet: for the 1347->1348 boundary, koios
// eta_1348 = blake2b(frozenCandidate || epoch1347.LastEpochBlockNonce), where
// epoch1347.LastEpochBlockNonce is the last block of epoch 1346 (the carried
// value), not the last block of epoch 1347.
func TestEpochNonceUsesCarriedLastEpochBlockNonce(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	cfg := newConwayBootstrapStabilityCfg(t)

	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		preCutSlot  uint64 = 1014
		postCutSlot uint64 = 1070
	)

	importedNonce := bytes.Repeat([]byte{0xaa}, 32)
	nonceAtPreCut := bytes.Repeat([]byte{0xbb}, 32) // frozen candidate
	nonceAtPostCut := bytes.Repeat([]byte{0xcc}, 32)
	carriedLab := bytes.Repeat(
		[]byte{0xfa},
		32,
	) // = last block of the PREVIOUS epoch

	hashAtPreCut := bytes.Repeat([]byte{0x14}, 32)
	hashAtPostCut := bytes.Repeat(
		[]byte{0x70},
		32,
	) // last block of the CLOSING epoch
	prevHashAtPreCut := bytes.Repeat([]byte{0x09}, 32)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot: preCutSlot, Hash: hashAtPreCut, PrevHash: prevHashAtPreCut,
			Cbor: []byte{0x80}, Number: 1, Type: conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot: postCutSlot, Hash: hashAtPostCut, PrevHash: hashAtPreCut,
			Cbor: []byte{0x80}, Number: 2, Type: conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.SetBlockNonce(
			hashAtPreCut, preCutSlot, nonceAtPreCut, false, txn); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAtPostCut, postCutSlot, nonceAtPostCut, false, txn)
	}))

	prevEpoch := models.Epoch{
		EpochId:             100,
		StartSlot:           epochStart,
		LengthInSlots:       uint(epochLength),
		SlotLength:          1000,
		EraId:               eras.ConwayEraDesc.Id,
		Nonce:               bytes.Repeat([]byte{0xee}, 32),
		EvolvingNonce:       importedNonce,
		CandidateNonce:      importedNonce,
		LastEpochBlockNonce: carriedLab,
	}

	ls := &LedgerState{
		db:           db,
		currentEra:   eras.ConwayEraDesc,
		currentEpoch: prevEpoch,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	hvNonce, _, hvCandidate, hvLab, err :=
		ls.computeEpochNonceForSlot(epochEnd, prevEpoch)
	require.NoError(t, err)

	var rNonce, rCandidate, rLab []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		n, _, c, lab, err := ls.calculateEpochNonce(
			txn, epochEnd, eras.ConwayEraDesc, prevEpoch,
			nil,
		)
		rNonce, rCandidate, rLab = n, c, lab
		return err
	}))

	// Correct assembly: eta = candidate ⭒ carriedLab (prevEpoch.LastEpochBlockNonce).
	wantEta, err := lcommon.CalculateEpochNonce(nonceAtPreCut, carriedLab, nil)
	require.NoError(t, err)
	// Old (buggy) assembly: eta = candidate ⭒ last block of closing epoch.
	oldEta, err := lcommon.CalculateEpochNonce(
		nonceAtPreCut,
		hashAtPostCut,
		nil,
	)
	require.NoError(t, err)

	require.Equal(
		t,
		hex.EncodeToString(nonceAtPreCut),
		hex.EncodeToString(
			rCandidate,
		),
		"candidate is the frozen pre-cutoff nonce",
	)
	require.Equal(
		t,
		hex.EncodeToString(wantEta.Bytes()),
		hex.EncodeToString(rNonce),
		"epoch nonce must mix the candidate with the CARRIED lab, not the closing epoch's last block",
	)
	require.NotEqual(
		t,
		hex.EncodeToString(oldEta.Bytes()),
		hex.EncodeToString(rNonce),
		"epoch nonce must NOT use the closing epoch's own last block (the #2734 bug)",
	)
	// The carried lab stored for the NEXT boundary is the PARENT hash of the
	// closing epoch's last block (prevHashToNonce(lastBlock.prevHash) ==
	// hashAtPreCut, the post-cutoff block's PrevHash), NOT the last block's own
	// hash — a one-block Praos lag (#2734 eta_1349 root cause).
	require.Equal(
		t,
		hex.EncodeToString(hashAtPreCut),
		hex.EncodeToString(rLab),
		"stored lastEpochBlockNonce must be the closing epoch's last-block PrevHash, for the next boundary",
	)
	require.NotEqual(
		t,
		hex.EncodeToString(hashAtPostCut),
		hex.EncodeToString(rLab),
		"stored lastEpochBlockNonce must NOT be the closing epoch's last block's own hash",
	)

	// The eager header-verification path must agree with the rollover path.
	require.Equal(t, hex.EncodeToString(rNonce), hex.EncodeToString(hvNonce),
		"eager epoch nonce must match rollover")
	require.Equal(
		t,
		hex.EncodeToString(rCandidate),
		hex.EncodeToString(hvCandidate),
		"eager candidate must match rollover",
	)
	require.Equal(t, hex.EncodeToString(rLab), hex.EncodeToString(hvLab),
		"eager lab must match rollover")
}

// TestEpochNonceGenesisEdgeUsesNeutralLab pins the from-genesis initialization
// of the carried lastEpochBlockNonce (#2734). When the initial epoch is created
// (no prior nonce), the epoch/evolving/candidate nonces are the genesis nonce
// but the carried lab is Neutral (nil) — NOT the genesis nonce. cardano-ledger
// initializes praosStateLastEpochBlockNonce to NeutralNonce at genesis
// (Cardano.Ledger.Shelley.API.Protocol.initialChainDepState: csLabNonce =
// NeutralNonce; Cardano.Protocol.TPraos.BHeader.prevHashToNonce GenesisHash =
// NeutralNonce), so the FIRST from-genesis boundary uses the identity
// (eta_1 = candidate ⭒ NeutralNonce = candidate). Devnet confirmed cardano's
// epoch-1 epochNonce == its candidate. Seeding the lab with the genesis nonce
// instead combines and diverges at the first boundary. The Mithril bootstrap
// path is unaffected (bootstrap epoch imports a non-nil lastEpochBlockNonce and
// never takes this branch).
func TestEpochNonceGenesisEdgeUsesNeutralLab(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	cfg := newConwayBootstrapStabilityCfg(t)
	// ShelleyGenesisHash set by the helper is 32 bytes of 0x11.
	genesisHash := bytes.Repeat([]byte{0x11}, 32)

	// Initial epoch: no nonce/evolving/candidate yet (from-genesis creation).
	initialEpoch := models.Epoch{
		EpochId: 0,
		EraId:   eras.ConwayEraDesc.Id,
	}

	ls := &LedgerState{
		db:           db,
		currentEra:   eras.ConwayEraDesc,
		currentEpoch: initialEpoch,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	hvNonce, hvEvolving, hvCandidate, hvLab, err :=
		ls.computeEpochNonceForSlot(500, initialEpoch)
	require.NoError(t, err)

	var rNonce, rEvolving, rCandidate, rLab []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		n, ev, c, lab, err := ls.calculateEpochNonce(
			txn, 500, eras.ConwayEraDesc, initialEpoch,
			nil,
		)
		rNonce, rEvolving, rCandidate, rLab = n, ev, c, lab
		return err
	}))

	// Epoch/evolving/candidate are the genesis nonce for the initial epoch.
	require.Equal(
		t,
		hex.EncodeToString(genesisHash),
		hex.EncodeToString(rNonce),
		"initial epoch nonce is the genesis nonce",
	)
	require.Equal(
		t,
		hex.EncodeToString(genesisHash),
		hex.EncodeToString(rEvolving),
		"initial evolving nonce is the genesis nonce",
	)
	require.Equal(
		t,
		hex.EncodeToString(genesisHash),
		hex.EncodeToString(rCandidate),
		"initial candidate nonce is the genesis nonce",
	)
	// The key #2734 assertion: the carried lab is Neutral (nil), NOT the genesis
	// nonce, so the first from-genesis boundary uses the identity.
	require.Empty(
		t,
		rLab,
		"initial carried lastEpochBlockNonce must be Neutral (nil), not the genesis nonce",
	)

	// The eager header-verification path must agree with the rollover path.
	require.Equal(t, hex.EncodeToString(rNonce), hex.EncodeToString(hvNonce))
	require.Equal(
		t,
		hex.EncodeToString(rEvolving),
		hex.EncodeToString(hvEvolving),
	)
	require.Equal(
		t,
		hex.EncodeToString(rCandidate),
		hex.EncodeToString(hvCandidate),
	)
	require.Empty(t, hvLab,
		"eager path initial carried lab must also be Neutral (nil)")
}

// Mainnet epoch 259 is the only epoch in Cardano's history whose protocol
// parameters carried a non-neutral extraEntropy. The values below are the real
// inputs and the real resulting eta0, so a node that drops the extraEntropy
// term computes a different leader schedule for that whole epoch and rejects
// every header in it.
//
//   - extraEntropy and the resulting nonce: Koios epoch_params for epoch 259
//     (extra_entropy / nonce); every other epoch from 208 to 656 carries
//     extra_entropy null.
//   - the same triple is pinned as a test vector in gouroboros
//     ledger/common/nonce_test.go.
//
// The carried lab (mainnetEpoch259Lab) is a mainnet block in epoch 257, which
// is what identifies the consuming epoch as 259 rather than 260: the TICKN
// state's prev-hash nonce lags the boundary by one epoch, so the boundary INTO
// epoch E mixes a block hash from epoch E-2.
const (
	mainnetEpoch259ExtraEntropy = "d982e06fd33e7440b43cefad529b7ecafbaa255e38178ad4189a37e4ce9bf1fa"
	mainnetEpoch259Candidate    = "d1340a9c1491f0face38d41fd5c82953d0eb48320d65e952414a0c5ebaf87587"
	mainnetEpoch259Lab          = "ee91d679b0a6ce3015b894c575c799e971efac35c7a8cbdc2b3f579005e69abd"
	mainnetEpoch259Nonce        = "0022cfa563a5328c4fb5c8017121329e964c26ade5d167b1bd9b2ec967772b60"
)

// maryPParamsWithExtraEntropy returns a minimally-populated Mary parameter set
// carrying extraEntropy, plus its CBOR. A nil rational field encodes as CBOR
// null and decodes back, so only ExtraEntropy needs a real value here.
func maryPParamsWithExtraEntropy(
	t *testing.T,
	entropy []byte,
) (*mary.MaryProtocolParameters, []byte) {
	t.Helper()
	pp := &mary.MaryProtocolParameters{
		ProtocolMajor: 4,
		// Block sizes the votedFuturePParams guard accepts.
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
	}
	if len(entropy) == lcommon.Blake2b256Size {
		pp.ExtraEntropy.Type = lcommon.NonceTypeNonce
		copy(pp.ExtraEntropy.Value[:], entropy)
	}
	data, err := cbor.Encode(pp)
	require.NoError(t, err)
	// Guard the fixture: the production path reads this back through the era's
	// own decoder, so a shape that does not round-trip would make the test
	// pass for the wrong reason.
	decoded, err := eras.DecodePParamsMary(data)
	require.NoError(t, err)
	decodedMary, ok := decoded.(*mary.MaryProtocolParameters)
	require.True(t, ok)
	require.Equal(t, pp.ExtraEntropy, decodedMary.ExtraEntropy)
	return pp, data
}

// TestComputeEpochNonceForSlotFoldsExtraEntropy covers the header-verification
// path (advanceEpochCache -> computeEpochNonceForSlot), which computes the new
// epoch's nonce speculatively, before the rollover enacts that epoch's
// protocol parameters. The extraEntropy it must fold therefore comes from the
// pending update submitted in the previous epoch, exactly as cardano-ledger's
// TICKF forecast supplies it to TICKN.
func TestComputeEpochNonceForSlotFoldsExtraEntropy(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	cfg := newConwayBootstrapStabilityCfg(t)

	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		preCutSlot  uint64 = 1014
		postCutSlot uint64 = 1070
		prevEpochID uint64 = 258
	)

	entropy := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)
	frozenCandidate := mustDecodeHex(t, mainnetEpoch259Candidate)
	carriedLab := mustDecodeHex(t, mainnetEpoch259Lab)

	importedNonce := mustDecodeHex(t, mainnetEpoch259Lab)
	nonceAtPostCut := mustDecodeHex(t, mainnetEpoch259Nonce)
	hashAtPreCut := mustDecodeHex(t, mainnetEpoch259Candidate)
	hashAtPostCut := mustDecodeHex(t, mainnetEpoch259Nonce)
	prevHashAtPreCut := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot: preCutSlot, Hash: hashAtPreCut, PrevHash: prevHashAtPreCut,
			Cbor: []byte{0x80}, Number: 1, Type: mary.BlockTypeMary,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot: postCutSlot, Hash: hashAtPostCut, PrevHash: hashAtPreCut,
			Cbor: []byte{0x80}, Number: 2, Type: mary.BlockTypeMary,
		}, txn); err != nil {
			return err
		}
		if err := db.SetBlockNonce(
			hashAtPreCut, preCutSlot, frozenCandidate, false, txn); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAtPostCut, postCutSlot, nonceAtPostCut, false, txn)
	}))

	// Parameters in effect for the ending epoch: no extra entropy yet.
	_, baseCbor := maryPParamsWithExtraEntropy(t, nil)
	require.NoError(t, db.SetPParams(
		baseCbor, epochStart, prevEpochID, eras.MaryEraDesc.Id, nil,
	))

	// The genesis-key update proposal submitted during the ending epoch, which
	// the rollover will enact as the new epoch's parameters.
	entropyNonce := lcommon.Nonce{Type: lcommon.NonceTypeNonce}
	copy(entropyNonce.Value[:], entropy)
	// A parameter update is a sparse map. Encoding the struct also serializes
	// unset rational fields as null, which the classic update decoder rejects.
	updateCbor, err := cbor.Encode(map[uint]any{
		13: entropyNonce,
	})
	require.NoError(t, err)
	require.NoError(t, db.SetPParamUpdate(
		[]byte{0x01}, updateCbor, epochStart+1, prevEpochID, nil,
	))

	prevEpoch := models.Epoch{
		EpochId:             prevEpochID,
		StartSlot:           epochStart,
		LengthInSlots:       uint(epochLength),
		SlotLength:          1000,
		EraId:               eras.MaryEraDesc.Id,
		Nonce:               mustDecodeHex(t, mainnetEpoch259Lab),
		EvolvingNonce:       importedNonce,
		CandidateNonce:      importedNonce,
		LastEpochBlockNonce: carriedLab,
	}

	ls := &LedgerState{
		db:           db,
		currentEra:   eras.MaryEraDesc,
		currentEpoch: prevEpoch,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	nonce, _, candidate, _, err := ls.computeEpochNonceForSlot(
		epochEnd, prevEpoch,
	)
	require.NoError(t, err)
	// Guard the fixture: the frozen candidate must be the mainnet value, or
	// the nonce comparison below would be testing arithmetic on other inputs.
	require.Equal(
		t,
		frozenCandidate,
		candidate,
		"candidate nonce must freeze at the pre-cutoff block nonce",
	)

	withoutEntropy, err := lcommon.CalculateEpochNonce(
		frozenCandidate, carriedLab, nil,
	)
	require.NoError(t, err)
	require.NotEqual(
		t,
		withoutEntropy.Bytes(),
		nonce,
		"epoch nonce must not be the extraEntropy-free value",
	)
	require.Equal(
		t,
		mainnetEpoch259Nonce,
		hex.EncodeToString(nonce),
		"epoch nonce must fold the pending extraEntropy update",
	)
}

// TestCalculateEpochNonceFoldsExtraEntropy covers the authoritative rollover
// path, which writes the epoch record every later consumer reads. Unlike the
// header-verification path it does not forecast: the boundary has already
// enacted the new epoch's protocol parameters, and their extraEntropy is what
// the nonce must mix.
func TestCalculateEpochNonceFoldsExtraEntropy(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	cfg := newConwayBootstrapStabilityCfg(t)

	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		preCutSlot  uint64 = 1014
		postCutSlot uint64 = 1070
		prevEpochID uint64 = 258
	)

	entropy := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)
	frozenCandidate := mustDecodeHex(t, mainnetEpoch259Candidate)
	carriedLab := mustDecodeHex(t, mainnetEpoch259Lab)

	importedNonce := mustDecodeHex(t, mainnetEpoch259Lab)
	nonceAtPostCut := mustDecodeHex(t, mainnetEpoch259Nonce)
	hashAtPreCut := mustDecodeHex(t, mainnetEpoch259Candidate)
	hashAtPostCut := mustDecodeHex(t, mainnetEpoch259Nonce)
	prevHashAtPreCut := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot: preCutSlot, Hash: hashAtPreCut, PrevHash: prevHashAtPreCut,
			Cbor: []byte{0x80}, Number: 1, Type: mary.BlockTypeMary,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot: postCutSlot, Hash: hashAtPostCut, PrevHash: hashAtPreCut,
			Cbor: []byte{0x80}, Number: 2, Type: mary.BlockTypeMary,
		}, txn); err != nil {
			return err
		}
		if err := db.SetBlockNonce(
			hashAtPreCut, preCutSlot, frozenCandidate, false, txn); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAtPostCut, postCutSlot, nonceAtPostCut, false, txn)
	}))

	prevEpoch := models.Epoch{
		EpochId:             prevEpochID,
		StartSlot:           epochStart,
		LengthInSlots:       uint(epochLength),
		SlotLength:          1000,
		EraId:               eras.MaryEraDesc.Id,
		Nonce:               mustDecodeHex(t, mainnetEpoch259Lab),
		EvolvingNonce:       importedNonce,
		CandidateNonce:      importedNonce,
		LastEpochBlockNonce: carriedLab,
	}

	ls := &LedgerState{
		db:           db,
		currentEra:   eras.MaryEraDesc,
		currentEpoch: prevEpoch,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	enacted, _ := maryPParamsWithExtraEntropy(t, entropy)
	neutral, _ := maryPParamsWithExtraEntropy(t, nil)

	var withEntropy, withoutParam []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		n, _, candidate, _, err := ls.calculateEpochNonce(
			txn, epochEnd, eras.MaryEraDesc, prevEpoch, enacted,
		)
		if err != nil {
			return err
		}
		require.Equal(
			t,
			frozenCandidate,
			candidate,
			"candidate nonce must freeze at the pre-cutoff block nonce",
		)
		withEntropy = n
		n, _, _, _, err = ls.calculateEpochNonce(
			txn, epochEnd, eras.MaryEraDesc, prevEpoch, neutral,
		)
		withoutParam = n
		return err
	}))

	require.Equal(
		t,
		mainnetEpoch259Nonce,
		hex.EncodeToString(withEntropy),
		"epoch nonce must fold the enacted extraEntropy",
	)

	// Negative case: the same inputs with a neutral extraEntropy must produce
	// the unmixed nonce, so the parameter is what moves the result rather than
	// anything else in the fixture.
	expectedNeutral, err := lcommon.CalculateEpochNonce(
		frozenCandidate, carriedLab, nil,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		expectedNeutral.Bytes(),
		withoutParam,
		"a neutral extraEntropy must leave the epoch nonce unmixed",
	)
}

// TestCalculateEpochNonceNeutralLabMixesExtraEntropy covers the boundary where
// the carried lastEpochBlockNonce is NeutralNonce and the extraEntropy is not.
// NeutralNonce is the identity of the nonce operator, so the assembly collapses
// to candidateNonce ⭒ extraEntropy -- not to candidateNonce alone, which is
// what the NeutralNonce short-circuit returns when the entropy term is dropped.
func TestCalculateEpochNonceNeutralLabMixesExtraEntropy(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	cfg := newConwayBootstrapStabilityCfg(t)

	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		preCutSlot  uint64 = 1014
		postCutSlot uint64 = 1070
		prevEpochID uint64 = 258
	)

	entropy := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)
	frozenCandidate := mustDecodeHex(t, mainnetEpoch259Candidate)

	importedNonce := mustDecodeHex(t, mainnetEpoch259Lab)
	nonceAtPostCut := mustDecodeHex(t, mainnetEpoch259Nonce)
	hashAtPreCut := mustDecodeHex(t, mainnetEpoch259Candidate)
	hashAtPostCut := mustDecodeHex(t, mainnetEpoch259Nonce)
	prevHashAtPreCut := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot: preCutSlot, Hash: hashAtPreCut, PrevHash: prevHashAtPreCut,
			Cbor: []byte{0x80}, Number: 1, Type: mary.BlockTypeMary,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot: postCutSlot, Hash: hashAtPostCut, PrevHash: hashAtPreCut,
			Cbor: []byte{0x80}, Number: 2, Type: mary.BlockTypeMary,
		}, txn); err != nil {
			return err
		}
		if err := db.SetBlockNonce(
			hashAtPreCut, preCutSlot, frozenCandidate, false, txn); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAtPostCut, postCutSlot, nonceAtPostCut, false, txn)
	}))

	prevEpoch := models.Epoch{
		EpochId:        prevEpochID,
		StartSlot:      epochStart,
		LengthInSlots:  uint(epochLength),
		SlotLength:     1000,
		EraId:          eras.MaryEraDesc.Id,
		Nonce:          mustDecodeHex(t, mainnetEpoch259Lab),
		EvolvingNonce:  importedNonce,
		CandidateNonce: importedNonce,
		// NeutralNonce: no carried last-block nonce.
		LastEpochBlockNonce: nil,
	}

	ls := &LedgerState{
		db:           db,
		currentEra:   eras.MaryEraDesc,
		currentEpoch: prevEpoch,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	enacted, _ := maryPParamsWithExtraEntropy(t, entropy)

	var nonce []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		n, _, _, _, err := ls.calculateEpochNonce(
			txn, epochEnd, eras.MaryEraDesc, prevEpoch, enacted,
		)
		nonce = n
		return err
	}))

	require.NotEqual(
		t,
		frozenCandidate,
		nonce,
		"a non-neutral extraEntropy must not leave the candidate nonce unmixed",
	)
	want, err := lcommon.CalculateRollingNonce(frozenCandidate, entropy)
	require.NoError(t, err)
	require.Equal(t, want.Bytes(), nonce)
}

// TestHealEmptyLabNoncesFoldsExtraEntropy covers the startup lab-recovery path,
// which recomputes a stored epoch's nonce from chain data. That epoch is in the
// past, so its extraEntropy comes from the protocol parameters recorded for it
// rather than from a forecast.
func TestHealEmptyLabNoncesFoldsExtraEntropy(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	const (
		entropyEpoch uint64 = 259
		prevEpoch    uint64 = 258
	)

	entropy := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)
	candidate := mustDecodeHex(t, mainnetEpoch259Candidate)
	carriedLab := mustDecodeHex(t, mainnetEpoch259Lab)

	boundaryHash := mustDecodeHex(t, mainnetEpoch259Nonce)
	boundaryPrevHash := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     150,
		Hash:     boundaryHash,
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     mary.BlockTypeMary,
	}, nil))

	_, entropyCbor := maryPParamsWithExtraEntropy(t, entropy)
	require.NoError(t, db.SetPParams(
		entropyCbor, 200, entropyEpoch, eras.MaryEraDesc.Id, nil,
	))

	ls := &LedgerState{
		db:                db,
		mithrilLedgerSlot: 150,
		epochCache: []models.Epoch{
			{
				EpochId:             prevEpoch,
				StartSlot:           100,
				LengthInSlots:       100,
				EraId:               eras.MaryEraDesc.Id,
				Nonce:               mustDecodeHex(t, mainnetEpoch259Lab),
				CandidateNonce:      mustDecodeHex(t, mainnetEpoch259Nonce),
				LastEpochBlockNonce: carriedLab,
			},
			{
				EpochId:        entropyEpoch,
				StartSlot:      200,
				LengthInSlots:  100,
				EraId:          eras.MaryEraDesc.Id,
				CandidateNonce: candidate,
				// NeutralNonce-collapsed (wrong) nonce: eta == candidateNonce.
				Nonce:               append([]byte(nil), candidate...),
				LastEpochBlockNonce: nil, // corrupted: empty lab
			},
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.healEmptyLabNonces()

	withoutEntropy, err := lcommon.CalculateEpochNonce(
		candidate, carriedLab, nil,
	)
	require.NoError(t, err)
	require.NotEqual(
		t,
		withoutEntropy.Bytes(),
		ls.epochCache[1].Nonce,
		"recomputed epoch nonce must not be the extraEntropy-free value",
	)
	require.Equal(
		t,
		mainnetEpoch259Nonce,
		hex.EncodeToString(ls.epochCache[1].Nonce),
		"recomputed epoch nonce must fold the epoch's recorded extraEntropy",
	)
}

// TestRecordedExtraEntropyForEpochTracksParameterHistory pins the parameter
// history the nonce assembly depends on: a pparams row is written for the epoch
// its change takes effect in, and a later epoch resolves to the newest row at or
// before it. Mainnet set extraEntropy for epoch 259 and reset it for 260, so a
// value that stayed sticky past its reset would corrupt every later epoch's
// nonce -- the opposite failure to dropping it.
func TestRecordedExtraEntropyForEpochTracksParameterHistory(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	entropy := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)

	_, entropyCbor := maryPParamsWithExtraEntropy(t, entropy)
	require.NoError(t, db.SetPParams(
		entropyCbor, 200, 259, eras.MaryEraDesc.Id, nil,
	))
	_, neutralCbor := maryPParamsWithExtraEntropy(t, nil)
	require.NoError(t, db.SetPParams(
		neutralCbor, 300, 260, eras.MaryEraDesc.Id, nil,
	))

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	for _, tc := range []struct {
		name  string
		epoch uint64
		want  []byte
	}{
		{"before the update", 258, nil},
		{"the epoch it was enacted for", 259, entropy},
		{"the epoch it was reset for", 260, nil},
		{"after the reset", 261, nil},
	} {
		got, err := ls.recordedExtraEntropyForEpoch(
			tc.epoch, eras.MaryEraDesc.Id,
		)
		require.NoError(t, err, tc.name)
		require.Equal(t, tc.want, got, tc.name)
	}

	// Conway has no extraEntropy parameter at all, so the Mary rows must not
	// leak into a later era's lookup.
	got, err := ls.recordedExtraEntropyForEpoch(259, eras.ConwayEraDesc.Id)
	require.NoError(t, err)
	require.Nil(t, got)
}

// TestExtraEntropyFromPParamsStopsAtPraos pins the era boundary of the TICKN
// extraEntropy term against the parameter set's protocol version rather than
// its Go type. A hard fork out of Alonzo enacts Alonzo-typed parameters whose
// protocol version is already Babbage's, and the first Praos epoch takes no
// extraEntropy term.
func TestExtraEntropyFromPParamsStopsAtPraos(t *testing.T) {
	t.Parallel()

	entropy := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)
	var nonce lcommon.Nonce
	nonce.Type = lcommon.NonceTypeNonce
	copy(nonce.Value[:], entropy)

	for _, tc := range []struct {
		name   string
		params lcommon.ProtocolParameters
		want   []byte
	}{
		{
			"shelley",
			&shelley.ShelleyProtocolParameters{
				ProtocolMajor: 2,
				ExtraEntropy:  nonce,
			},
			entropy,
		},
		{
			"mary",
			&mary.MaryProtocolParameters{
				ProtocolMajor: 4,
				ExtraEntropy:  nonce,
			},
			entropy,
		},
		{
			"alonzo",
			&alonzo.AlonzoProtocolParameters{
				ProtocolMajor: 6,
				ExtraEntropy:  nonce,
			},
			entropy,
		},
		{
			"alonzo parameters carrying the babbage protocol version",
			&alonzo.AlonzoProtocolParameters{
				ProtocolMajor: babbage.MinProtocolVersionBabbage,
				ExtraEntropy:  nonce,
			},
			nil,
		},
	} {
		require.Equal(
			t,
			tc.want,
			extraEntropyFromPParams(tc.params),
			tc.name,
		)
	}
}

// TestAssembleEpochNonceNeutralCandidateReturnsEntropy pins the identity law of
// the nonce ⭒ operator on its left operand. cardano-ledger's Semigroup Nonce
// gives NeutralNonce <> x = x, so a neutral candidate with a non-neutral
// extraEntropy and no lab must yield the entropy itself.
//
// gouroboros' CalculateRollingNonce cannot serve this case: it coerces its
// right operand from a raw VRF output, so for an all-zero left operand it
// returns blake2b_256(right) rather than right.
func TestAssembleEpochNonceNeutralCandidateReturnsEntropy(t *testing.T) {
	t.Parallel()

	entropy := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)
	neutral := make([]byte, lcommon.Blake2b256Size)

	got, err := assembleEpochNonce(neutral, nil, entropy)
	require.NoError(t, err)
	require.Equal(
		t,
		entropy,
		got,
		"NeutralNonce is the identity of the nonce operator, so a neutral "+
			"candidate must leave extraEntropy unchanged",
	)

	hashed := lcommon.Blake2b256Hash(entropy)
	require.NotEqual(
		t,
		hashed.Bytes(),
		got,
		"the entropy must not be re-hashed, which is what the rolling-nonce "+
			"helper would do for a neutral left operand",
	)
}

// TestEpochNonce_SnapshotTipPastCutoff covers the one Mithril-bootstrap
// shape the existing #2128 suite does not: a snapshot whose tip slot lies
// PAST the candidate-freeze cutoff of its epoch. In that shape the imported
// epoch row carries CandidateNonce != EvolvingNonce — psCandidateNonce
// froze at the cutoff (before the tip) while psEvolvingNonce kept rolling
// to the tip. The existing tests always set the two equal (snapshot taken
// before the cutoff), so this exercises the distinct-values path.
//
// Expected: the bootstrap epoch's rollover must return candidate equal to
// the imported (frozen) CandidateNonce — NOT the imported EvolvingNonce,
// and NOT a value re-derived from a pre-cutoff block (there are none with
// stored nonces; pre-cutoff blocks were imported as immutable). Both the
// rollover path (calculateEpochNonce) and the header-verification path
// (computeEpochNonceForSlot) must agree, because the first header of the
// new epoch is verified against the eagerly-cached value.
func TestEpochNonce_SnapshotTipPastCutoff(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	cfg := newConwayBootstrapStabilityCfg(t)

	// k=6, f=0.4 -> 4k/f = 60. Epoch [1000,1075), cutoff = 1075-60 = 1015.
	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		cutoffSlot  uint64 = 1015
		preImpSlot  uint64 = 1010 // pre-cutoff, imported immutable (no nonce row)
		snapTipSlot uint64 = 1040 // snapshot tip: PAST the cutoff
		postCutSlot uint64 = 1070 // last block of epoch (post-import)
	)

	// Imported tip-time evolving nonce (psEvolvingNonce at slot 1040).
	importedEvolving := bytes.Repeat([]byte{0xaa}, 32)
	// Imported frozen candidate (psCandidateNonce, frozen at the cutoff
	// well before the tip). Distinct from evolving on purpose.
	importedCandidate := bytes.Repeat([]byte{0xdd}, 32)
	// Per-block evolving nonce stored by post-import processing.
	nonceAtPostCut := bytes.Repeat([]byte{0xcc}, 32)

	hashPreImp := bytes.Repeat([]byte{0x10}, 32)
	hashAtSnap := bytes.Repeat([]byte{0x40}, 32)
	hashAtPostCut := bytes.Repeat([]byte{0x70}, 32)
	prevHashPreImp := bytes.Repeat([]byte{0x09}, 32)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		// Pre-cutoff block imported as immutable: present in the blob
		// store, but with NO block_nonce row (importTip only checkpoints
		// the tip).
		if err := db.BlockCreate(models.Block{
			Slot:     preImpSlot,
			Hash:     hashPreImp,
			PrevHash: prevHashPreImp,
			Cbor:     []byte{0x80},
			Number:   1,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		// Snapshot tip block (past the cutoff).
		if err := db.BlockCreate(models.Block{
			Slot:     snapTipSlot,
			Hash:     hashAtSnap,
			PrevHash: hashPreImp,
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		// Post-import block.
		if err := db.BlockCreate(models.Block{
			Slot:     postCutSlot,
			Hash:     hashAtPostCut,
			PrevHash: hashAtSnap,
			Cbor:     []byte{0x80},
			Number:   3,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		// importTip checkpoint at the snapshot tip = imported evolving.
		if err := db.SetBlockNonce(
			hashAtSnap, snapTipSlot, importedEvolving, true, txn,
		); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAtPostCut, postCutSlot, nonceAtPostCut, false, txn,
		)
	}))

	prevEpoch := models.Epoch{
		EpochId:             100,
		StartSlot:           epochStart,
		LengthInSlots:       uint(epochLength),
		SlotLength:          1000,
		EraId:               eras.ConwayEraDesc.Id,
		Nonce:               bytes.Repeat([]byte{0xee}, 32),
		EvolvingNonce:       importedEvolving,
		CandidateNonce:      importedCandidate,
		LastEpochBlockNonce: bytes.Repeat([]byte{0xfa}, 32),
	}

	ls := &LedgerState{
		db:           db,
		currentEra:   eras.ConwayEraDesc,
		currentEpoch: prevEpoch,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	// Header-verification (eager) path.
	hvNonce, hvEvolving, hvCandidate, hvLab, err :=
		ls.computeEpochNonceForSlot(epochEnd, prevEpoch)
	require.NoError(t, err)

	// Rollover (authoritative) path.
	var rNonce, rEvolving, rCandidate, rLab []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		n, ev, c, lab, err := ls.calculateEpochNonce(
			txn, epochEnd, eras.ConwayEraDesc, prevEpoch,
			nil,
		)
		rNonce, rEvolving, rCandidate, rLab = n, ev, c, lab
		return err
	}))

	require.Equalf(
		t,
		hex.EncodeToString(importedCandidate),
		hex.EncodeToString(rCandidate),
		"with the snapshot tip past the cutoff (tip=%d, cutoff=%d), the "+
			"frozen candidate is the imported psCandidateNonce. Got %x. "+
			"If this equals importedEvolving (0xaa...) the computation "+
			"confused evolving for candidate.",
		snapTipSlot, cutoffSlot, rCandidate,
	)
	require.Equalf(
		t,
		hex.EncodeToString(nonceAtPostCut),
		hex.EncodeToString(rEvolving),
		"evolving must equal the last block's stored nonce (slot %d). Got %x.",
		postCutSlot, rEvolving,
	)
	require.NotEqual(
		t,
		hex.EncodeToString(rCandidate),
		hex.EncodeToString(rEvolving),
		"candidate (frozen pre-tip) and evolving (at tip) must differ",
	)

	// Eager path must match rollover, or the first new-epoch header is
	// verified against a nonce the rollover later disagrees with -> VRF
	// failure at turnover.
	require.Equal(t,
		hex.EncodeToString(rCandidate), hex.EncodeToString(hvCandidate),
		"eager candidate must match rollover candidate")
	require.Equal(t,
		hex.EncodeToString(rEvolving), hex.EncodeToString(hvEvolving),
		"eager evolving must match rollover evolving")
	require.Equalf(t,
		hex.EncodeToString(rNonce), hex.EncodeToString(hvNonce),
		"eager epoch nonce must match rollover epoch nonce. hv=%x rollover=%x",
		hvNonce, rNonce)
	require.Equal(t,
		hex.EncodeToString(rLab), hex.EncodeToString(hvLab),
		"eager labNonce must match rollover labNonce")
}

// TestEpochNonceFormula validates the epoch nonce formula:
//
//	epochNonce(N+1) = blake2b_256(candidateNonce(N) || lastEpochBlockNonce(N))
//
// This uses deterministic synthetic inputs to verify the CalculateEpochNonce
// function produces the correct blake2b_256 hash of the concatenated nonces.
func TestEpochNonceFormula(t *testing.T) {
	t.Parallel()

	// Synthetic 32-byte candidateNonce
	candidateNonce := mustDecodeHex(
		t,
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	)
	// Synthetic 32-byte lastEpochBlockNonce (hash of last block)
	lastEpochBlockNonce := mustDecodeHex(
		t,
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	)

	// Compute epoch nonce using the production function
	result, err := lcommon.CalculateEpochNonce(
		candidateNonce,
		lastEpochBlockNonce,
		nil,
	)
	require.NoError(t, err)

	// Verify the result matches blake2b_256(candidateNonce || lastEpochBlockNonce)
	concat := append(candidateNonce, lastEpochBlockNonce...)
	expected := lcommon.Blake2b256Hash(concat)
	assert.Equal(
		t,
		hex.EncodeToString(expected.Bytes()),
		hex.EncodeToString(result.Bytes()),
		"epoch nonce should equal blake2b_256(candidateNonce || lastEpochBlockNonce)",
	)
}

// TestEpochNonceNeutralIdentity verifies the neutral nonce semantics:
// when lastEpochBlockNonce is nil (epoch 0→1 transition), the caller
// uses candidateNonce directly as the epoch nonce (bypassing
// CalculateEpochNonce). This mirrors the production code in
// calculateEpochNonce and computeEpochNonceForSlot.
func TestEpochNonceNeutralIdentity(t *testing.T) {
	t.Parallel()

	candidateNonce := mustDecodeHex(
		t,
		"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
	)

	// Simulate the NeutralNonce path: when lastEpochBlockNonce is nil,
	// the epoch nonce IS the candidateNonce (identity element of ⭒).
	var lastEpochBlockNonce []byte // nil = NeutralNonce
	var epochNonce []byte
	if len(lastEpochBlockNonce) == 0 {
		epochNonce = candidateNonce
	} else {
		result, err := lcommon.CalculateEpochNonce(
			candidateNonce,
			lastEpochBlockNonce,
			nil,
		)
		require.NoError(t, err)
		epochNonce = result.Bytes()
	}

	assert.Equal(
		t,
		hex.EncodeToString(candidateNonce),
		hex.EncodeToString(epochNonce),
		"with NeutralNonce, epoch nonce should equal candidateNonce",
	)
}

// TestEpochNonceNonCommutative verifies that the nonce semigroup
// operator is NOT commutative: blake2b_256(a || b) != blake2b_256(b || a).
func TestEpochNonceNonCommutative(t *testing.T) {
	t.Parallel()

	a := mustDecodeHex(
		t,
		"1111111111111111111111111111111111111111111111111111111111111111",
	)
	b := mustDecodeHex(
		t,
		"2222222222222222222222222222222222222222222222222222222222222222",
	)

	resultAB, err := lcommon.CalculateEpochNonce(a, b, nil)
	require.NoError(t, err)

	resultBA, err := lcommon.CalculateEpochNonce(b, a, nil)
	require.NoError(t, err)

	assert.NotEqual(
		t,
		hex.EncodeToString(resultAB.Bytes()),
		hex.EncodeToString(resultBA.Bytes()),
		"nonce combination should not be commutative",
	)
}

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

func TestEraTransitionPathAllowsPrimeBoundaryPair(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	path, ok := ls.eraTransitionPath(
		eras.MaryEraDesc.Id,
		eras.BabbageEraDesc.Id,
		true,
	)
	require.True(t, ok)
	require.Equal(
		t,
		[]uint{eras.AlonzoEraDesc.Id, eras.BabbageEraDesc.Id},
		path,
	)
}

func TestEraTransitionsRunAfterSourceEraPParamEnactment(t *testing.T) {
	t.Parallel()

	path := []uint{eras.BabbageEraDesc.Id}
	before, after := splitEraTransitionsForRollover(path)

	require.Empty(t, before,
		"successor transitions must not replace the source era before rollover")
	require.Equal(t, path, after,
		"the successor transition must run after source-era pparam enactment")
}

func TestEraTransitionPathRejectsLargerJump(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	path, ok := ls.eraTransitionPath(
		eras.MaryEraDesc.Id,
		eras.ConwayEraDesc.Id,
		true,
	)
	require.False(t, ok)
	require.Nil(t, path)
}

func TestBoundaryEraForBlockUsesSuccessorHeaderEra(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	target, allowTwoTransitions := ls.boundaryEraForBlock(
		eras.MaryEraDesc.Id,
		eras.AlonzoEraDesc.Id,
		7,
		true,
	)
	require.Equal(t, eras.BabbageEraDesc.Id, target)
	require.True(t, allowTwoTransitions)
}

func TestBoundaryEraForBlockDoesNotAdvanceFromHeaderAlone(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	target, allowTwoTransitions := ls.boundaryEraForBlock(
		eras.AlonzoEraDesc.Id,
		eras.AlonzoEraDesc.Id,
		eras.BabbageEraDesc.MinMajorVersion,
		true,
	)
	require.Equal(
		t,
		eras.AlonzoEraDesc.Id,
		target,
		"an Alonzo block remains Alonzo even when its header advertises protocol major 7",
	)
	require.False(t, allowTwoTransitions)
}

func TestBoundaryEraForBlockRejectsNonAdjacentHeaderEra(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	target, allowTwoTransitions := ls.boundaryEraForBlock(
		eras.MaryEraDesc.Id,
		eras.AlonzoEraDesc.Id,
		eras.ConwayEraDesc.MinMajorVersion,
		true,
	)
	require.Equal(t, eras.AlonzoEraDesc.Id, target)
	require.False(t, allowTwoTransitions)
}

func TestEraAdvancementRejectsRawTwoStepBodyJumpWithoutHeaderElevation(
	t *testing.T,
) {
	t.Parallel()

	ls := &LedgerState{}
	target, allowTwoTransitions := ls.boundaryEraForBlock(
		eras.MaryEraDesc.Id,
		eras.BabbageEraDesc.Id,
		eras.BabbageEraDesc.MinMajorVersion,
		true,
	)
	require.Equal(t, eras.BabbageEraDesc.Id, target)
	require.False(t, allowTwoTransitions)

	_, ok := ls.eraTransitionPath(
		eras.MaryEraDesc.Id,
		target,
		allowTwoTransitions,
	)
	require.False(
		t,
		ok,
		"a raw two-era body jump must not skip the omitted era",
	)
}

// newBoundaryRolloverLedger builds a LedgerState positioned at the end of a
// Shelley epoch, with the persisted epoch record the rollover needs. The
// returned pparams carry Shelley's protocol major, so a snapshot captured
// before a boundary's era transitions records a different major than one
// captured after them.
func newBoundaryRolloverLedger(
	t *testing.T,
) (*LedgerState, *database.Database) {
	t.Helper()

	const shelleyGenesisJSON = `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"epochLength": 432000,
		"slotLength": 1,
		"protocolParams": {
			"protocolVersion": {"major": 2, "minor": 0},
			"decentralisationParam": 1,
			"maxBlockBodySize": 65536,
			"maxBlockHeaderSize": 1100,
			"maxTxSize": 16384,
			"minFeeA": 44,
			"minFeeB": 155381,
			"minUTxOValue": 1000000,
			"keyDeposit": 2000000,
			"poolDeposit": 500000000,
			"eMax": 18,
			"nOpt": 150,
			"a0": 0.3,
			"rho": 0.003,
			"tau": 0.2,
			"minPoolCost": 340000000
		},
		"systemStart": "2022-10-25T00:00:00Z"
	}`
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d",
	}
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	currentEpoch := models.Epoch{
		EpochId:       5,
		StartSlot:     500,
		SlotLength:    1000,
		LengthInSlots: 100,
		EraId:         eras.ShelleyEraDesc.Id,
	}
	require.NoError(t, db.SetEpoch(
		currentEpoch.StartSlot, currentEpoch.EpochId,
		nil, nil, nil, nil,
		currentEpoch.EraId, currentEpoch.SlotLength,
		currentEpoch.LengthInSlots,
		nil,
	))

	rat := func() *cbor.Rat { return &cbor.Rat{Rat: big.NewRat(1, 2)} }
	ls := &LedgerState{
		db:           db,
		currentEra:   eras.ShelleyEraDesc,
		currentEpoch: currentEpoch,
		currentPParams: &shelley.ShelleyProtocolParameters{
			ProtocolMajor:    shelley.MinProtocolVersionShelley,
			MinFeeA:          44,
			A0:               rat(),
			Rho:              rat(),
			Tau:              rat(),
			Decentralization: rat(),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	return ls, db
}

// TestBoundaryEraTransitionsSnapshotRecordsFinalProtocolVersion drives a
// two-era boundary the way ledgerProcessBlocksFromSource does: the rollover
// runs first so source-era pparam updates are enacted, then the remaining era
// transitions are applied. The authoritative mark snapshot must be captured
// once, after those transitions, so its protocol version is the one the new
// epoch actually runs at. Capturing it at the end of the rollover records the
// source era's major instead, and that value is durable.
func TestBoundaryEraTransitionsSnapshotRecordsFinalProtocolVersion(
	t *testing.T,
) {
	t.Parallel()

	ls, db := newBoundaryRolloverLedger(t)

	var captures []event.EpochTransitionEvent
	ls.SetEpochBoundarySnapshotHook(
		func(_ *database.Txn, evt event.EpochTransitionEvent) error {
			captures = append(captures, evt)
			return nil
		},
	)

	transitionPath, ok := ls.eraTransitionPath(
		eras.ShelleyEraDesc.Id,
		eras.MaryEraDesc.Id,
		true,
	)
	require.True(t, ok)
	require.Len(t, transitionPath, 2)

	var result *EpochRolloverResult
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var err error
		result, err = ls.processEpochRollover(
			txn,
			ls.currentEpoch,
			ls.currentEra,
			ls.currentPParams,
			true,
		)
		if err != nil {
			return err
		}
		require.True(t, result.BoundarySnapshotDeferred,
			"a multi-era boundary must defer the mark snapshot capture")
		require.Empty(t, captures,
			"the rollover must not capture the mark snapshot before the "+
				"boundary's era transitions have run")

		transitions, err := ls.applyBoundaryEraTransitions(
			txn, ls.currentEpoch, transitionPath, result,
		)
		if err != nil {
			return err
		}
		require.Len(t, transitions, 2)
		return nil
	}))

	if result == nil {
		t.Fatal("epoch rollover returned no result")
	}
	require.Len(t, captures, 1,
		"the deferred capture must run exactly once, not be re-run")
	require.Equal(
		t,
		uint(mary.MinProtocolVersionMary),
		captures[0].ProtocolVersion,
		"the mark snapshot must record the protocol major of the era the "+
			"new epoch runs at, not the era the rollover started in",
	)
	require.Equal(t, eras.MaryEraDesc.Id, result.NewCurrentEra.Id)
	require.Equal(t, eras.MaryEraDesc.Id, result.NewCurrentEpoch.EraId)
	require.False(t, result.BoundarySnapshotDeferred,
		"the deferred capture must be marked as taken")

	// The event the caller publishes after commit is built from the same
	// result, so the durable row and the event must agree.
	require.Equal(
		t,
		captures[0].ProtocolVersion,
		ls.protocolMajorForEvent(
			result.NewCurrentPParams, result.NewCurrentEra,
		),
	)
}

func TestBoundaryEraTransitionUsesTargetEraTiming(t *testing.T) {
	t.Parallel()

	ls, db := newBoundaryRolloverLedger(t)

	sourceEra := ls.currentEra
	sourceEra.EpochLengthFunc = func(
		*cardano.CardanoNodeConfig,
	) (uint, uint, error) {
		return 20_000, 21_600, nil
	}
	ls.currentEra = sourceEra

	var result *EpochRolloverResult
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var err error
		result, err = ls.processEpochRollover(
			txn,
			ls.currentEpoch,
			sourceEra,
			ls.currentPParams,
			true,
		)
		if err != nil {
			return err
		}
		_, err = ls.applyBoundaryEraTransitions(
			txn,
			ls.currentEpoch,
			[]uint{eras.AllegraEraDesc.Id},
			result,
		)
		return err
	}))
	if result == nil {
		t.Fatal("epoch rollover returned no result")
	}

	wantSlotLength, wantEpochLength, err := eras.AllegraEraDesc.EpochLengthFunc(
		ls.config.CardanoNodeConfig,
	)
	require.NoError(t, err)
	require.Equal(t, wantSlotLength, result.NewCurrentEpoch.SlotLength)
	require.Equal(t, wantEpochLength, result.NewCurrentEpoch.LengthInSlots)
	require.Equal(t, wantSlotLength, result.SchedulerIntervalMs)

	var cachedEpoch *models.Epoch
	for i := range result.NewEpochCache {
		if result.NewEpochCache[i].EpochId == result.NewCurrentEpoch.EpochId {
			cachedEpoch = &result.NewEpochCache[i]
			break
		}
	}
	require.NotNil(t, cachedEpoch)
	require.Equal(t, wantSlotLength, cachedEpoch.SlotLength)
	require.Equal(t, wantEpochLength, cachedEpoch.LengthInSlots)

	persistedEpoch, err := db.GetEpoch(result.NewCurrentEpoch.EpochId, nil)
	require.NoError(t, err)
	require.NotNil(t, persistedEpoch)
	require.Equal(t, wantSlotLength, persistedEpoch.SlotLength)
	require.Equal(t, wantEpochLength, persistedEpoch.LengthInSlots)
}

// TestSingleEraBoundaryRolloverCapturesSnapshotInRollover covers the common
// path: with no era transitions deferred, the rollover still captures the mark
// snapshot itself, at its own era's protocol version.
func TestSingleEraBoundaryRolloverCapturesSnapshotInRollover(t *testing.T) {
	t.Parallel()

	ls, db := newBoundaryRolloverLedger(t)

	var captures []event.EpochTransitionEvent
	ls.SetEpochBoundarySnapshotHook(
		func(_ *database.Txn, evt event.EpochTransitionEvent) error {
			captures = append(captures, evt)
			return nil
		},
	)

	var result *EpochRolloverResult
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var err error
		result, err = ls.processEpochRollover(
			txn,
			ls.currentEpoch,
			ls.currentEra,
			ls.currentPParams,
			false,
		)
		return err
	}))

	if result == nil {
		t.Fatal("epoch rollover returned no result")
	}
	require.False(t, result.BoundarySnapshotDeferred)
	require.Len(t, captures, 1)
	require.Equal(
		t,
		uint(shelley.MinProtocolVersionShelley),
		captures[0].ProtocolVersion,
	)
	require.Equal(t, eras.ShelleyEraDesc.Id, result.NewCurrentEra.Id)
}

func TestProtocolParamsForSlot_UnavailableShape(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		cfg    *cardano.CardanoNodeConfig
		era    eras.EraDesc
		params lcommon.ProtocolParameters
	}{
		{
			name: "missing config", era: eras.ShelleyEraDesc,
			params: &shelley.ShelleyProtocolParameters{ProtocolMajor: 2},
		},
		{
			name: "invalid config", cfg: &cardano.CardanoNodeConfig{},
			era:    eras.ShelleyEraDesc,
			params: &shelley.ShelleyProtocolParameters{ProtocolMajor: 2},
		},
		{
			name: "current era unavailable", cfg: newAllegraAtEpoch1Cfg(t),
			era: eras.DijkstraEraDesc, params: &dijkstra.DijkstraProtocolParameters{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ls := &LedgerState{
				currentEra: tc.era,
				currentEpoch: models.Epoch{
					EpochId: 0, StartSlot: 0, LengthInSlots: 75, EraId: tc.era.Id,
				},
				currentPParams: tc.params,
				config:         LedgerStateConfig{CardanoNodeConfig: tc.cfg},
			}
			ls.publishSnapshotsLocked()
			require.Same(t, tc.params, ls.ProtocolParamsForSlot(74),
				"current-epoch parameters do not require a forecast")
			require.Nil(t, ls.ProtocolParamsForSlot(75),
				"a future epoch with unavailable shape must not use current parameters")
		})
	}
	// The existing boundary test exercises a valid scheduled Shelley-to-Allegra
	// forecast; unavailable-shape handling must retain that path.
}

func TestProtocolParamsForSlot_UnavailableTransition(t *testing.T) {
	t.Parallel()

	for _, missingSuccessor := range []bool{false, true} {
		name := "hard fork error"
		if missingSuccessor {
			name = "missing successor"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ls := &LedgerState{
				currentEra: eras.ShelleyEraDesc,
				currentEpoch: models.Epoch{
					EpochId: 0, StartSlot: 0, LengthInSlots: 75,
					EraId: eras.ShelleyEraDesc.Id,
				},
				currentPParams: &babbage.BabbageProtocolParameters{},
				config: LedgerStateConfig{
					CardanoNodeConfig: newAllegraAtEpoch1Cfg(t),
					Logger:            slog.New(slog.DiscardHandler),
				},
			}
			if missingSuccessor {
				ls.currentPParams = &shelley.ShelleyProtocolParameters{ProtocolMajor: 2}
				ls.activeEras = []eras.EraDesc{eras.ShelleyEraDesc}
			}
			ls.publishSnapshotsLocked()
			require.Same(t, ls.currentPParams, ls.ProtocolParamsForSlot(74))
			require.Nil(t, ls.ProtocolParamsForSlot(75),
				"an unresolved scheduled transition must not return pre-fork parameters")
		})
	}
}

func TestProtocolParamsForSlot_PendingUpdateFailure(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	// A selected but undecodable proposal is an error, not an absent update.
	require.NoError(t, db.SetPParamUpdate([]byte{0xaa}, []byte{0xff}, 50, 0, nil))
	pparams := &shelley.ShelleyProtocolParameters{ProtocolMajor: 2}
	ls := &LedgerState{
		db: db, currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId: 0, StartSlot: 0, LengthInSlots: 100,
			EraId: eras.ShelleyEraDesc.Id,
		},
		currentPParams: pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: newShelleyUpdateQuorum1Cfg(t),
			Logger:            slog.New(slog.DiscardHandler),
		},
	}
	ls.publishSnapshotsLocked()
	require.Same(t, pparams, ls.ProtocolParamsForSlot(99))
	require.Nil(t, ls.ProtocolParamsForSlot(100),
		"a failed pending update must not return stale parameters")
}

func TestGenesisOverlayRejectsUnavailableProtocolParams(t *testing.T) {
	t.Parallel()

	for _, available := range []bool{false, true} {
		name := "unavailable"
		if available {
			name = "available"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cfg := newGenesisDelegateShelleyGenesisCfg(t,
				strings.Repeat("22", 28), strings.Repeat("33", 32))
			epoch := models.Epoch{
				EpochId: 0, StartSlot: 0, LengthInSlots: 75,
				EraId: eras.BabbageEraDesc.Id,
			}
			ls := &LedgerState{
				currentEra: eras.BabbageEraDesc, currentEpoch: epoch,
				epochCache: []models.Epoch{epoch},
				config:     LedgerStateConfig{CardanoNodeConfig: cfg},
			}
			if available {
				ls.currentPParams = &babbage.BabbageProtocolParameters{}
			}
			ls.publishSnapshotsLocked()
			handled, err := ls.verifyGenesisDelegateHeader(
				&mockBabbageBlock{slot: 50}, false)
			if available {
				require.NoError(t, err)
				require.False(t, handled,
					"Babbage parameters correctly disable the genesis overlay")
				return
			}
			require.ErrorContains(t, err, "protocol parameters unavailable")
			require.True(t, handled,
				"unavailable parameters must not fall through as a non-overlay slot")
		})
	}
}

// shelleyOnlyGenesisCfg returns a config with a Shelley genesis and no Byron
// genesis. ByronGenesisFile is optional (config/cardano/node.go loads it only
// when non-empty) and a Shelley-only config without it is a supported shape
// (see the setEpochCache era-start comment in state.go), but
// eras.BuildShapeForEras still builds Byron era params for every config, so no
// hard-fork shape -- and therefore no forecast -- can be built from one.
func shelleyOnlyGenesisCfg(t testing.TB) *cardano.CardanoNodeConfig {
	t.Helper()
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"slotLength": 1,
		"epochLength": 432000,
		"systemStart": "2022-10-25T00:00:00Z"
	}`
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(
		strings.NewReader(shelleyGenesisJSON),
	))
	require.Nil(t, cfg.ByronGenesis())
	return cfg
}

// newShelleyOnlyForecastLedger builds a LedgerState whose epoch cache covers
// slots [100_000, 532_000) but whose config cannot produce a hard-fork shape.
func newShelleyOnlyForecastLedger(t testing.TB) *LedgerState {
	t.Helper()
	ls := &LedgerState{
		epochCache: []models.Epoch{{
			EpochId:       500,
			StartSlot:     100_000,
			SlotLength:    1_000,
			LengthInSlots: 432_000,
			EraId:         eras.ConwayEraDesc.Id,
			Nonce:         []byte("nonce"),
		}},
		currentEra: eras.ConwayEraDesc,
		currentEpoch: models.Epoch{
			EpochId:       500,
			StartSlot:     100_000,
			LengthInSlots: 432_000,
		},
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(200_000, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: shelleyOnlyGenesisCfg(t),
		},
	}
	ls.publishSnapshotsLocked()
	return ls
}

// TestHeaderVerificationEpoch_ForecastBuildFailureDeferred pins the
// classification of a hard-fork summary that cannot be BUILT: the era shape,
// the genesis behind it, and the epoch cache are local inputs, so the failure
// says nothing about the header. ouroboros/chainsync.go routes every
// non-deferred header error to ConnectionRecycleRequestedEvent, so returning
// the build failure unwrapped recycles the honest peer that served the header
// and stalls the node at every epoch boundary.
func TestHeaderVerificationEpoch_ForecastBuildFailureDeferred(t *testing.T) {
	t.Parallel()

	ls := newShelleyOnlyForecastLedger(t)

	// Confirm the premise: the config genuinely cannot build a summary.
	_, sumErr := ls.HardForkSummary()
	require.Error(t, sumErr, "Shelley-only config must not build a shape")

	// A slot past the cached epoch forces the summary path.
	_, err := ls.headerVerificationEpoch(532_000, false)
	require.Error(t, err)
	require.ErrorIs(t, err, errHeaderVerificationDeferred,
		"an unbuildable forecast must not be reported as a peer fault")
	require.True(t, IsHeaderVerificationDeferred(err))
}

// TestSlotToTime_CachedSlotWithoutForecast pins SlotToTime against
// SlotToEpoch: a slot the epoch cache already covers has known era parameters
// and must convert without a forecast. Returning the summary-build error
// verbatim leaves the slot clock (ledger/slot_clock.go) unable to resolve a
// slot boundary, retrying every 100ms for the life of the process.
func TestSlotToTime_CachedSlotWithoutForecast(t *testing.T) {
	t.Parallel()

	ls := newShelleyOnlyForecastLedger(t)

	// SlotToEpoch already answers from the cache alone.
	epoch, err := ls.SlotToEpoch(200_000)
	require.NoError(t, err)
	assert.Equal(t, uint64(500), epoch.EpochId)

	// The same slot must convert to a time without a forecast. The cache
	// anchors relative time at its first entry's StartSlot, exactly as
	// hardForkSummaryAnchoredAt does, so slot 200_000 is 100_000 slots of
	// 1000ms past SystemStart.
	when, err := ls.SlotToTime(200_000)
	require.NoError(t, err,
		"a slot inside the epoch cache must not require a forecast")
	assert.Equal(
		t,
		time.Date(2022, 10, 25, 0, 0, 0, 0, time.UTC).
			Add(100_000*time.Second),
		when.UTC(),
	)

	// The absence case: a slot the cache does NOT cover has no known era
	// parameters, so it must still fail rather than be extrapolated.
	_, err = ls.SlotToTime(532_000)
	require.Error(t, err,
		"a slot past the epoch cache must not be answered without a forecast")
	_, err = ls.SlotToTime(99_999)
	require.Error(t, err,
		"a slot before the epoch cache must not be answered without a forecast")
}

func TestSlotToTime_NearNowWithoutForecast(t *testing.T) {
	t.Parallel()

	ls := newShelleyOnlyForecastLedger(t)
	const slot = uint64(532_000)
	want := ls.config.CardanoNodeConfig.ShelleyGenesis().SystemStart.Add(
		time.Duration(slot) * time.Second,
	)
	ls.timeConv().nowFunc = func() time.Time { return want }

	when, err := ls.SlotToTime(slot)
	require.NoError(t, err)
	assert.Equal(t, want, when)
}

// TestConsensusModeForEpoch_UnresolvableShapeFailsClosed pins the
// forward-looking era walk as fail-closed. An unavailable shape breaks the
// walk, and answering with the CURRENT era's mode for a future epoch reports
// exactly what a scheduled hard fork changes. The control fixes Babbage at
// epoch 501 under the same current era (Alonzo, TPraos), so the two cases
// differ by mode and not merely by error: with the shape resolvable the
// forecast is CPraos.
func TestConsensusModeForEpoch_UnresolvableShapeFailsClosed(t *testing.T) {
	t.Parallel()

	newLedger := func(cfg *cardano.CardanoNodeConfig) *LedgerState {
		enabled := true
		cfg.ExperimentalHardForksEnabled = &enabled
		babbage := uint64(501)
		cfg.TestBabbageHardForkAtEpoch = &babbage
		ls := &LedgerState{
			epochCache: []models.Epoch{{
				EpochId:       500,
				StartSlot:     100_000,
				SlotLength:    1_000,
				LengthInSlots: 432_000,
				EraId:         eras.AlonzoEraDesc.Id,
			}},
			currentEra: eras.AlonzoEraDesc,
			currentEpoch: models.Epoch{
				EpochId:       500,
				StartSlot:     100_000,
				LengthInSlots: 432_000,
			},
			currentTip: ochainsync.Tip{
				Point: ocommon.NewPoint(200_000, []byte("tip")),
			},
			config: LedgerStateConfig{CardanoNodeConfig: cfg},
		}
		ls.publishSnapshotsLocked()
		return ls
	}

	// Control: with a resolvable shape the walk crosses the scheduled
	// Babbage boundary and reports CPraos for the future epoch.
	control := newLedger(newTestEraHistoryCfg(t))
	mode, err := control.ConsensusModeForEpoch(600)
	require.NoError(t, err)
	assert.Equal(t, consensus.ConsensusModeCPraos, mode,
		"the scheduled Babbage fork must be reflected in the forecast")

	broken := newLedger(shelleyOnlyGenesisCfg(t))
	_, shapeErr := broken.eraShapeWithError()
	require.Error(t, shapeErr, "the premise: no shape can be built")

	// An epoch the cache already covers is not a forecast and still answers.
	mode, err = broken.ConsensusModeForEpoch(500)
	require.NoError(t, err)
	assert.Equal(t, consensus.ConsensusModeTPraos, mode)

	// The current epoch and earlier read applied state and still answer.
	mode, err = broken.ConsensusModeForEpoch(499)
	require.NoError(t, err)
	assert.Equal(t, consensus.ConsensusModeTPraos, mode)

	// The future epoch needs the walk, so it must fail closed instead of
	// reporting Alonzo's TPraos across the scheduled Babbage boundary.
	_, err = broken.ConsensusModeForEpoch(600)
	require.Error(t, err,
		"a future-epoch consensus mode must fail closed without a shape")
}

// TestCreateGenesisBlockFileBackedNoFKError drives the real genesis sync path
// (createGenesisBlock -> database.SetGenesisTransaction -> UtxoLedgerToModel ->
// metadata SetGenesisTransaction) on a file-backed SQLite store, where
// foreign_keys=ON is enforced.
//
// Genesis UTxOs are unspent/unreferenced, so the utxo columns spent_at_tx_id /
// referenced_by_tx_id / collateral_by_tx_id (FKs to transaction(hash)) must be
// stored as SQL NULL. If they are bound as an empty blob, the FK fails with
// "FOREIGN KEY constraint failed (787)". This is the failure reported for
// BURSA_SYNC=genesis on preview/devnet.
//
// It also re-runs createGenesisBlock to confirm idempotency.
func TestCreateGenesisBlockFileBackedNoFKError(t *testing.T) {
	t.Parallel()

	networks := []struct {
		name       string
		configPath string
	}{
		{name: "preview", configPath: "preview/config.json"},
		{name: "devnet", configPath: "devnet/config.json"},
	}

	for _, nw := range networks {
		t.Run(nw.name, func(t *testing.T) {
			// File-backed store: enables foreign_keys(1) + WAL, the
			// production configuration that matches the reported failure.
			db, err := dbtest.NewDatabase(t, &database.Config{
				DataDir: t.TempDir(),
			})
			require.NoError(t, err)

			nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
				nw.configPath,
				nw.name,
				cardano.EmbeddedConfigFS,
			)
			require.NoError(t, err)

			ls := &LedgerState{
				db: db,
				config: LedgerStateConfig{
					Database:          db,
					CardanoNodeConfig: nodeCfg,
					Logger: slog.New(
						slog.NewTextHandler(io.Discard, nil),
					),
				},
			}

			// First run: must not hit the FK 787 error.
			require.NoError(
				t,
				ls.createGenesisBlock(),
				"createGenesisBlock should not fail with FK constraint",
			)

			raw, err := dbtest.RawSQLiteMetadata(t, db)
			require.NoError(t, err)

			// At least one genesis UTxO should have been created so the
			// assertions below are meaningful.
			var utxoCount int64
			require.NoError(t, raw.QueryRow(
				"SELECT COUNT(*) FROM utxo",
			).Scan(&utxoCount))
			require.Greater(t, utxoCount, int64(0), "genesis UTxOs created")

			// The hash FK columns must be stored as NULL, never empty blobs.
			var nonNull int64
			require.NoError(t, raw.QueryRow(`
SELECT COUNT(*) FROM utxo
WHERE spent_at_tx_id IS NOT NULL
   OR referenced_by_tx_id IS NOT NULL
   OR collateral_by_tx_id IS NOT NULL`,
			).Scan(&nonNull))
			require.Equal(
				t,
				int64(0),
				nonNull,
				"genesis UTxOs must store NULL hash FKs, not empty blobs",
			)

			// Second run: idempotent, still no error.
			require.NoError(
				t,
				ls.createGenesisBlock(),
				"re-running createGenesisBlock must remain idempotent",
			)
		})
	}
}

// The Musashi Conway genesis declares three genesis committee members, all
// key-hash cold credentials, each expiring at epoch 293.
var musashiGenesisCommitteeColdKeys = []string{
	"0fa32e5f69a89afa3f5e1074660b975dde8e5a89c1b8004d49501e33",
	"518a0c96344656d332625e33aa680b6c25bbce6b5972a30adf1dce8d",
	"8feda2412bec6f79bc5996a5055bcff28d230cb9c85fb9d5e8743a46",
}

const musashiGenesisCommitteeExpiry = 293

func TestGenesisCommitteeStateUnavailableWithoutHistory(t *testing.T) {
	t.Parallel()
	ls, db := genesisConstitutionTestState(t)
	for _, cfg := range []*cardano.CardanoNodeConfig{
		ls.config.CardanoNodeConfig,
		{},
		nil,
	} {
		ls.config.CardanoNodeConfig = cfg
		require.Zero(t, committeeMemberRowCount(t, db))
		available, err := ls.NewView(nil).CommitteeStateAvailable()
		require.NoError(t, err)
		require.False(t, available)
	}
}

func TestEmptyGenesisCommitteeLookupFailure(t *testing.T) {
	t.Parallel()
	ls, _ := genesisConstitutionTestState(t)
	ls.config.CardanoNodeConfig.ConwayGenesis().Committee.Members = nil
	wantErr := errors.New("committee storage unavailable")
	ls.db = newStorageFaultTestDB(t, errInjectingMetadataStore{
		getCommitteeMembersErr: wantErr,
	})
	available, err := ls.NewView(nil).CommitteeStateAvailable()
	require.ErrorIs(t, err, wantErr)
	require.False(t, available)
}

func TestEmptyGenesisCommitteeStateAvailable(t *testing.T) {
	t.Parallel()

	for _, members := range []map[string]int{nil, {}} {
		name := "empty map"
		if members == nil {
			name = "nil map"
		}
		t.Run(name, func(t *testing.T) {
			ls, db := genesisConstitutionTestState(t)
			ls.config.CardanoNodeConfig.ConwayGenesis().Committee.Members = members
			for range 2 {
				require.NoError(t, ls.createGenesisBlock())
				require.Zero(t, committeeMemberRowCount(t, db))
				available, err := ls.NewView(nil).CommitteeStateAvailable()
				require.NoError(t, err)
				require.True(
					t,
					available,
					"empty genesis committee is authoritative",
				)
			}
		})
	}
}

func TestEmptyGenesisCommitteeValidation(t *testing.T) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)
	ls.config.CardanoNodeConfig.ConwayGenesis().Committee.Members = map[string]int{}
	require.NoError(t, ls.createGenesisBlock())
	require.Zero(t, committeeMemberRowCount(t, db))
	pp := &conway.ConwayProtocolParameters{}
	ls.currentPParams = pp
	ls.publishSnapshotsLocked()
	lv := ls.NewView(nil)

	for _, tag := range []uint{
		lcommon.CredentialTypeAddrKeyHash,
		lcommon.CredentialTypeScriptHash,
	} {
		cold := committeeTestCredential(0xe1)
		cold.CredType = tag
		hot := committeeTestCredential(0xe2)
		hot.CredType = tag
		certs := []lcommon.Certificate{
			&lcommon.AuthCommitteeHotCertificate{
				CertType:       uint(lcommon.CertificateTypeAuthCommitteeHot),
				ColdCredential: cold,
				HotCredential:  hot,
			},
			&lcommon.ResignCommitteeColdCertificate{
				CertType: uint(
					lcommon.CertificateTypeResignCommitteeCold,
				),
				ColdCredential: cold,
			},
		}
		for _, cert := range certs {
			t.Run(
				fmt.Sprintf("tag %d/%s", tag, certificateName(cert)),
				func(t *testing.T) {
					tx := &conway.ConwayTransaction{
						TxIsValid: true,
						Body: conway.ConwayTransactionBody{
							TxCertificates: []lcommon.CertificateWrapper{{
								Type: cert.Type(), Certificate: cert,
							}},
						},
					}
					err := eras.ValidateTxConway(tx, 0, lv, pp)
					var notMember conway.NotCommitteeMemberError
					require.ErrorAs(t, err, &notMember)
					require.Equal(t, cold.Credential, notMember.Credential)
					require.Equal(t, certificateName(cert), notMember.Operation)
				},
			)
		}

		voterType := uint8(lcommon.VoterTypeConstitutionalCommitteeHotKeyHash)
		if tag == lcommon.CredentialTypeScriptHash {
			voterType = lcommon.VoterTypeConstitutionalCommitteeHotScriptHash
		}
		voter := &lcommon.Voter{Type: voterType, Hash: hot.Credential}
		tx := &conway.ConwayTransaction{
			TxIsValid: true,
			Body: conway.ConwayTransactionBody{
				TxVotingProcedures: lcommon.VotingProcedures{voter: {}},
			},
		}
		t.Run(fmt.Sprintf("tag %d/vote", tag), func(t *testing.T) {
			err := eras.ValidateTxConway(tx, 0, lv, pp)
			var unknown conway.UnknownVoterError
			require.ErrorAs(t, err, &unknown)
			require.Equal(t, *voter, unknown.Voter)
		})

		// GOVCERT permits a proposed member even when no committee is seated.
		storeCommitteeUpdateProposal(t, db, byte(0xe3+tag), cold, 90)
		for _, cert := range certs {
			tx := &conway.ConwayTransaction{
				TxIsValid: true,
				Body: conway.ConwayTransactionBody{
					TxCertificates: []lcommon.CertificateWrapper{{
						Type: cert.Type(), Certificate: cert,
					}},
				},
			}
			err := eras.ValidateTxConway(tx, 0, lv, pp)
			var notMember conway.NotCommitteeMemberError
			require.False(t, errors.As(err, &notMember), "%v", err)
			var lookup conway.CommitteeMemberLookupError
			require.False(t, errors.As(err, &lookup), "%v", err)
		}
	}
}

func TestEmptyGenesisCommitteeReferenceResignation(t *testing.T) {
	t.Parallel()

	root, err := conformance.ExtractEmbeddedTestdata(t.TempDir())
	require.NoError(t, err)
	vector, err := conformance.DecodeTestVector(filepath.Join(
		root,
		"eras",
		"conway",
		"impl",
		"dump",
		"Conway.Imp.ConwayImpSpec_-_Version_10.GOVCERT.fails_for.resigning_a_nonexistent_CC_member_hotkey",
		"1",
	))
	require.NoError(t, err)
	initial, err := conformance.ParseInitialState(vector.InitialState)
	require.NoError(t, err)
	require.Len(t, vector.Events, 1)
	event := vector.Events[0]
	require.Equal(t, conformance.EventTypeTransaction, event.Type)
	require.False(t, event.Success)
	tx, err := conway.NewConwayTransactionFromCbor(event.TxBytes)
	require.NoError(t, err)
	require.Len(t, tx.Certificates(), 1)
	cert, ok := tx.Certificates()[0].(*lcommon.ResignCommitteeColdCertificate)
	require.True(t, ok)
	for credential := range initial.CommitteeMembersByCredential {
		require.NotEqual(
			t,
			cert.ColdCredential.Credential,
			credential.Credential,
		)
	}
	pp, err := conformance.NewPParamsLoaderFromTestdata(root).
		LoadForVector(vector, initial)
	require.NoError(t, err)

	ls, _ := genesisConstitutionTestState(t)
	ls.config.CardanoNodeConfig.ConwayGenesis().Committee.Members = map[string]int{}
	require.NoError(t, ls.createGenesisBlock())
	ls.currentPParams = pp
	ls.currentEpoch = models.Epoch{EpochId: initial.CurrentEpoch}
	ls.publishSnapshotsLocked()
	// The reference rejects this unknown cold credential with a seated
	// committee. Removing that committee cannot make the credential eligible.
	// Check that same GOVCERT error with the production empty-genesis view;
	// this is a rule-level projection, not a replay of the full vector state.
	err = eras.ValidateTxConway(tx, event.Slot, ls.NewView(nil), pp)
	var notMember conway.NotCommitteeMemberError
	require.ErrorAs(t, err, &notMember)
	require.Equal(t, cert.ColdCredential.Credential, notMember.Credential)
	require.Equal(t, "resign", notMember.Operation)
}

// TestCreateGenesisBlockSeedsCommittee proves a node initialized from Conway
// genesis recognizes every genesis Constitutional Committee member for
// hot-key authorization. Without the seed (blinklabs-io/dingo#3785) a
// genesis member never touched by an UpdateCommittee action has no row at
// all, and AuthCommitteeHot/ResignCommitteeCold validation rejects it as
// "not a CC member" even though the real chain has recognized it since the
// hard fork.
func TestCreateGenesisBlockSeedsCommittee(t *testing.T) {
	t.Parallel()

	ls, _ := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock())

	lv := &LedgerView{ls: ls}
	for _, coldKeyHex := range musashiGenesisCommitteeColdKeys {
		coldKey, err := hex.DecodeString(coldKeyHex)
		require.NoError(t, err)
		member, err := lv.CommitteeCredentialMember(lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: lcommon.NewBlake2b224(coldKey),
		})
		require.NoError(t, err)
		require.NotNil(
			t,
			member,
			"genesis committee member %s must resolve",
			coldKeyHex,
		)
		require.False(t, member.Resigned)
		require.Equal(
			t,
			uint64(musashiGenesisCommitteeExpiry),
			member.ExpiryEpoch,
		)
	}
}

// TestCreateGenesisBlockSeedsCommitteeOnExistingDatabase covers the upgrade
// path, which is the population that actually has the bug: a node already
// synced from genesis on a build that never seeded the committee.
//
// Such a database has matching genesis CBOR and a nonzero tip, so
// createGenesisBlock takes its early-return branch and never reaches the
// genesis-creation transaction. Seeding only from that transaction would
// therefore fix new nodes and leave every existing one broken.
func TestCreateGenesisBlockSeedsCommitteeOnExistingDatabase(t *testing.T) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)

	// Stand in for a database written by a build with no committee seed:
	// genesis CBOR present and matching, a tip well past zero, and no
	// committee_member rows at all.
	genesisHash, err := GenesisBlockHash(ls.config.CardanoNodeConfig)
	require.NoError(t, err)
	require.NoError(t, db.SetGenesisCbor(0, genesisHash[:], []byte{0x80}, nil))
	ls.currentTip.Point = ocommon.Point{Slot: 1_000_000}
	require.Equal(t, 0, committeeMemberRowCount(t, db))

	require.NoError(t, ls.createGenesisBlock())

	lv := &LedgerView{ls: ls}
	for _, coldKeyHex := range musashiGenesisCommitteeColdKeys {
		coldKey, err := hex.DecodeString(coldKeyHex)
		require.NoError(t, err)
		member, err := lv.CommitteeCredentialMember(lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: lcommon.NewBlake2b224(coldKey),
		})
		require.NoError(t, err)
		require.NotNil(
			t,
			member,
			"genesis committee member %s must be backfilled on an existing database",
			coldKeyHex,
		)
		require.Equal(
			t,
			uint64(musashiGenesisCommitteeExpiry),
			member.ExpiryEpoch,
		)
	}
}

// TestCreateGenesisBlockCommitteeReplayIdempotent proves re-running genesis
// initialization over a store that already holds the genesis committee
// leaves a single row per member rather than a duplicate soft-delete/insert
// pair.
func TestCreateGenesisBlockCommitteeReplayIdempotent(t *testing.T) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock())
	require.NoError(t, ls.createGenesisBlock())

	require.Equal(
		t,
		len(musashiGenesisCommitteeColdKeys),
		committeeMemberRowCount(t, db),
	)
}

// TestCreateGenesisBlockCommitteeEnactmentWins proves a real UpdateCommittee
// enactment for a genesis cold credential outranks the genesis seed, and
// that a later genesis initialization pass does not revert it back to the
// genesis term -- the hazard a naive unconditional reseed on every startup
// would create.
func TestCreateGenesisBlockCommitteeEnactmentWins(t *testing.T) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock())

	coldKey, err := hex.DecodeString(musashiGenesisCommitteeColdKeys[0])
	require.NoError(t, err)
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{
		{
			ColdCredentialTag: 0,
			ColdCredHash:      coldKey,
			ExpiresEpoch:      999,
			TermStartSlot:     100,
			TermStartSlotSet:  true,
			AddedSlot:         100,
		},
	}, nil))

	ls.currentTip.Point = ocommon.Point{Slot: 200}
	require.NoError(t, ls.createGenesisBlock())

	lv := &LedgerView{ls: ls}
	member, err := lv.CommitteeCredentialMember(lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(coldKey),
	})
	require.NoError(t, err)
	require.NotNil(t, member)
	require.Equal(t, uint64(999), member.ExpiryEpoch)

	// The other two genesis members are untouched and still resolve.
	for _, coldKeyHex := range musashiGenesisCommitteeColdKeys[1:] {
		otherKey, err := hex.DecodeString(coldKeyHex)
		require.NoError(t, err)
		other, err := lv.CommitteeCredentialMember(lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: lcommon.NewBlake2b224(otherKey),
		})
		require.NoError(t, err)
		require.NotNil(t, other)
		require.Equal(
			t,
			uint64(musashiGenesisCommitteeExpiry),
			other.ExpiryEpoch,
		)
	}
}

// TestParseGenesisCommitteeCredential exercises both credential prefixes and
// the rejection paths for malformed genesis committee member keys.
func TestParseGenesisCommitteeCredential(t *testing.T) {
	t.Parallel()

	keyHash := bytes.Repeat([]byte{0xab}, 28)
	tag, hash, err := parseGenesisCommitteeCredential(
		"keyHash-" + hex.EncodeToString(keyHash),
	)
	require.NoError(t, err)
	require.Equal(t, uint8(lcommon.CredentialTypeAddrKeyHash), tag)
	require.Equal(t, keyHash, hash)

	scriptHash := bytes.Repeat([]byte{0xcd}, 28)
	tag, hash, err = parseGenesisCommitteeCredential(
		"scriptHash-" + hex.EncodeToString(scriptHash),
	)
	require.NoError(t, err)
	require.Equal(t, uint8(lcommon.CredentialTypeScriptHash), tag)
	require.Equal(t, scriptHash, hash)

	_, _, err = parseGenesisCommitteeCredential("bogus-deadbeef")
	require.Error(t, err)

	_, _, err = parseGenesisCommitteeCredential("keyHash-nothex")
	require.Error(t, err)

	_, _, err = parseGenesisCommitteeCredential("keyHash-abcd")
	require.Error(t, err)
}

// TestEnsureGenesisCommitteeRejectsNegativeExpiry proves a malformed Conway
// genesis fails initialization instead of seating a member with a wrapped
// term.
//
// conway-genesis.json models the committee expiry as a bare JSON number, so it
// decodes into a signed int. Converting a negative value straight to the
// store's unsigned epoch would wrap it to a near-maximum uint64 -- a term no
// epoch boundary would ever expire -- so the seed must refuse it outright.
func TestEnsureGenesisCommitteeRejectsNegativeExpiry(t *testing.T) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)

	// The embedded config is parsed fresh on every load, so mutating this
	// state's genesis below cannot leak into the other tests in this file.
	other, _ := genesisConstitutionTestState(t)
	require.NotSame(
		t,
		ls.config.CardanoNodeConfig.ConwayGenesis(),
		other.config.CardanoNodeConfig.ConwayGenesis(),
		"each test state must own its genesis for the mutation below to be safe",
	)

	members := ls.config.CardanoNodeConfig.ConwayGenesis().Committee.Members
	rawKey := "keyHash-" + musashiGenesisCommitteeColdKeys[0]
	require.Contains(t, members, rawKey)
	members[rawKey] = -1

	err := ls.ensureGenesisCommittee(nil)
	require.ErrorContains(t, err, "negative expiry epoch -1")
	require.ErrorContains(t, err, musashiGenesisCommitteeColdKeys[0])
	require.Equal(
		t,
		0,
		committeeMemberRowCount(t, db),
		"a malformed genesis committee must seat no members at all",
	)
}

// TestEnsureGenesisCommitteeRejectsNegativeExpiryWhenAlreadySeeded proves the
// malformed-genesis check fails closed on the upgrade path too.
//
// Validating the expiry only after the already-seeded check would let the same
// malformed genesis start a node whose database happens to hold rows already
// while refusing a fresh one, even though the genesis file is equally
// malformed in both cases.
func TestEnsureGenesisCommitteeRejectsNegativeExpiryWhenAlreadySeeded(
	t *testing.T,
) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock())
	seeded := committeeMemberRowCount(t, db)
	require.Equal(t, len(musashiGenesisCommitteeColdKeys), seeded)

	members := ls.config.CardanoNodeConfig.ConwayGenesis().Committee.Members
	rawKey := "keyHash-" + musashiGenesisCommitteeColdKeys[0]
	require.Contains(t, members, rawKey)
	members[rawKey] = -7

	err := ls.ensureGenesisCommittee(nil)
	require.ErrorContains(t, err, "negative expiry epoch -7")
	require.ErrorContains(t, err, musashiGenesisCommitteeColdKeys[0])
	require.Equal(
		t,
		seeded,
		committeeMemberRowCount(t, db),
		"the failed check must not add or remove rows",
	)
}

// TestGenesisCommitteeExpiryEpoch covers the signed-to-unsigned conversion
// directly, including the most negative int, which is the value a straight
// conversion wraps furthest.
func TestGenesisCommitteeExpiryEpoch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		expiry  int
		want    uint64
		wantErr bool
	}{
		{"zero", 0, 0, false},
		{
			"musashi genesis expiry",
			musashiGenesisCommitteeExpiry,
			musashiGenesisCommitteeExpiry,
			false,
		},
		{"negative one", -1, 0, true},
		{"most negative int", math.MinInt, 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := genesisCommitteeExpiryEpoch(tc.expiry)
			if tc.wantErr {
				require.Error(t, err)
				require.Zero(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// committeeMemberRowCount returns the number of stored committee_member rows.
func committeeMemberRowCount(t *testing.T, db *database.Database) int {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	defer func() { require.NoError(t, raw.Close()) }()
	var count int
	require.NoError(
		t,
		raw.QueryRow("SELECT COUNT(*) FROM committee_member").Scan(&count),
	)
	return count
}

// The constitution the Musashi Conway genesis declares. Genesis
// initialization must record exactly these bytes, since guardrails
// validation compares the script hash against every parameter-change and
// treasury-withdrawal proposal's policy hash.
const (
	musashiConstitutionURL = "ipfs://" +
		"bafkreiazhhawe7sjwuthcfgl3mmv2swec7sukvclu3oli7qdyz4uhhuvmy"
	musashiConstitutionAnchorHash = "2a61e2f4b63442978140c77a70daab396" +
		"1b22b12b63b13949a390c097214d1c5"
	musashiConstitutionScriptHash = "fa24fb305126805cf2164c161d852a0e" +
		"7330cf988f1fe558cf7d4a64"
)

// genesisConstitutionTestState builds a LedgerState over a file-backed test
// database with the Musashi configuration, whose Conway genesis declares a
// constitution with a guardrails script.
func genesisConstitutionTestState(
	t *testing.T,
) (*LedgerState, *database.Database) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"musashi/config.json",
		"musashi",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Database:          db,
			CardanoNodeConfig: nodeCfg,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
		},
	}
	return ls, db
}

// requireGenesisConstitution asserts the view reports exactly the
// constitution the Musashi Conway genesis declares.
func requireGenesisConstitution(t *testing.T, lv *LedgerView) {
	t.Helper()
	got, err := lv.Constitution()
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, musashiConstitutionURL, got.Anchor.Url)
	require.Equal(
		t,
		musashiConstitutionAnchorHash,
		hex.EncodeToString(got.Anchor.DataHash[:]),
	)
	require.Equal(
		t,
		musashiConstitutionScriptHash,
		hex.EncodeToString(got.ScriptHash),
	)
}

// TestCreateGenesisBlockSeedsConstitution proves a node initialized from
// Conway genesis reports the genesis constitution, so guardrails validation
// accepts a treasury-withdrawal proposal carrying the genesis guardrails
// script hash and rejects one carrying none. Without the seed the lookup
// fails closed and every such proposal is rejected until a NewConstitution
// action is enacted.
func TestCreateGenesisBlockSeedsConstitution(t *testing.T) {
	t.Parallel()

	ls, _ := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock())

	lv := &LedgerView{ls: ls}
	requireGenesisConstitution(t, lv)

	scriptHash, err := hex.DecodeString(musashiConstitutionScriptHash)
	require.NoError(t, err)
	require.NoError(t, constitutionTestGuardrails(t, lv, scriptHash))

	err = constitutionTestGuardrails(t, lv, nil)
	require.Error(t, err)
	var mismatch conway.InvalidGuardrailsScriptHashError
	require.ErrorAs(t, err, &mismatch)
	require.Equal(t, scriptHash, mismatch.Expected)
}

// TestCreateGenesisBlockConstitutionReplayIdempotent proves re-running
// genesis initialization over a store that already holds the genesis
// constitution leaves a single slot-0 row rather than a duplicate.
func TestCreateGenesisBlockConstitutionReplayIdempotent(t *testing.T) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock())
	require.NoError(t, ls.createGenesisBlock())

	requireGenesisConstitution(t, &LedgerView{ls: ls})
	require.Equal(t, 1, constitutionRowCount(t, db))
}

// TestCreateGenesisBlockConstitutionSeededOnRestart proves the restart path
// -- an existing database whose genesis CBOR already matches, which returns
// before genesis storage is rewritten -- still records the genesis
// constitution. A database created before the constitution was seeded
// reaches genesis initialization only through that path.
func TestCreateGenesisBlockConstitutionSeededOnRestart(t *testing.T) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock())

	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec("DELETE FROM constitution")
	require.NoError(t, err)
	require.NoError(t, raw.Close())
	require.Equal(t, 0, constitutionRowCount(t, db))

	// Advance past genesis so the second run takes the existing-database
	// path instead of rewriting genesis storage.
	ls.currentTip.Point = ocommon.Point{Slot: 100}
	require.NoError(t, ls.createGenesisBlock())

	requireGenesisConstitution(t, &LedgerView{ls: ls})
}

// TestCreateGenesisBlockConstitutionEnactmentWins proves an enacted
// NewConstitution action outranks the slot-0 genesis seed, and that a later
// genesis initialization pass does not restore the genesis constitution over
// it.
func TestCreateGenesisBlockConstitutionEnactmentWins(t *testing.T) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock())

	enactedAnchor := bytes.Repeat([]byte{0xe1}, lcommon.Blake2b256Size)
	enactedScript := bytes.Repeat([]byte{0xe2}, lcommon.Blake2b224Size)
	require.NoError(t, db.SetConstitution(&models.Constitution{
		AnchorURL:  "https://example.invalid/enacted",
		AnchorHash: enactedAnchor,
		PolicyHash: enactedScript,
		AddedSlot:  100,
	}, nil))

	ls.currentTip.Point = ocommon.Point{Slot: 200}
	require.NoError(t, ls.createGenesisBlock())

	lv := &LedgerView{ls: ls}
	got, err := lv.Constitution()
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "https://example.invalid/enacted", got.Anchor.Url)
	require.Equal(t, enactedAnchor, got.Anchor.DataHash[:])
	require.Equal(t, enactedScript, got.ScriptHash)

	require.NoError(t, constitutionTestGuardrails(t, lv, enactedScript))
}

// constitutionRowCount returns the number of stored constitution rows.
func constitutionRowCount(t *testing.T, db *database.Database) int {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	defer func() { require.NoError(t, raw.Close()) }()
	var count int
	require.NoError(
		t,
		raw.QueryRow("SELECT COUNT(*) FROM constitution").Scan(&count),
	)
	return count
}

// TestCreateGenesisBlockSkipsGenesisStakingAfterMithrilBootstrap is the
// same-bug-class regression test the #4151 PR review recommended: it
// found that SetGenesisStaking (and SetGenesisGovernance, covered by
// TestCreateGenesisBlockSkipsGenesisGovernanceAfterMithrilBootstrap below)
// weren't gated by the same bootstrappedFromMithril guard as genesis UTxO
// insertion. Both upsert current-state rows (ON CONFLICT ... DO UPDATE),
// and a Mithril-bootstrapped node's imported ledger snapshot
// (ledgerstate/import.go's importCertState) already reflects the correct
// current pool/delegation state as of the bootstrap point -- reapplying
// stale genesis-config values would silently resurrect a pool genuinely
// retired (or a delegation genuinely changed) before the bootstrap point.
//
// Uses the embedded devnet config, the only bundled network that declares
// a nonzero genesis pool + stake delegation (mainnet/preview/preprod/
// musashi all declare zero, so the bug was unreachable there, but live on
// devnet).
func TestCreateGenesisBlockSkipsGenesisStakingAfterMithrilBootstrap(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"devnet/config.json",
		"devnet",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)

	genesisPools, _, err := nodeCfg.ShelleyGenesis().InitialPools()
	require.NoError(t, err)
	require.NotEmpty(
		t, genesisPools,
		"devnet must declare at least one genesis pool for this test to "+
			"be meaningful",
	)
	var poolIdHex string
	for k := range genesisPools {
		poolIdHex = k
		break
	}
	poolKeyHash, err := hex.DecodeString(poolIdHex)
	require.NoError(t, err)

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Database:          db,
			CardanoNodeConfig: nodeCfg,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
		},
	}
	// Same bootstrap shape as
	// TestCreateGenesisBlockSkipsUtxoInsertionAfterMithrilBootstrap: a
	// currentTip past slot 0 with no genesis CBOR yet stored.
	ls.currentTip.Point.Slot = 42
	require.NoError(t, ls.createGenesisBlock())

	_, err = db.GetPool(lcommon.PoolKeyHash(poolKeyHash), true, nil)
	require.ErrorIs(
		t, err, models.ErrPoolNotFound,
		"a genesis pool must not be (re-)inserted after a Mithril "+
			"bootstrap -- the imported ledger snapshot is the only "+
			"authority on whether it is still registered or was already "+
			"retired before the bootstrap point",
	)

	// Re-running (as a real startup would on every restart) must remain
	// idempotent and continue to skip insertion.
	require.NoError(t, ls.createGenesisBlock())
	_, err = db.GetPool(lcommon.PoolKeyHash(poolKeyHash), true, nil)
	require.ErrorIs(t, err, models.ErrPoolNotFound)
}

// TestCreateGenesisBlockSkipsGenesisGovernanceAfterMithrilBootstrap covers
// the same guard for SetGenesisGovernance. No bundled network config
// declares a genesis DRep today (devnet's conway-genesis.json has an
// empty initialDReps), so this synthesizes a minimal one via
// LoadConwayGenesisFromReader, the same test-only escape hatch
// config/cardano/node.go documents "mostly for tests".
func TestCreateGenesisBlockSkipsGenesisGovernanceAfterMithrilBootstrap(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"devnet/config.json",
		"devnet",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)

	const drepKeyHashHex = "00112233445566778899aabbccddeeff001122334455667788990011"
	require.Len(t, drepKeyHashHex, 56, "must decode to 28 bytes")
	conwayGenesisJson := `{
		"poolVotingThresholds": {},
		"dRepVotingThresholds": {},
		"committeeMinSize": 0,
		"committeeMaxTermLength": 0,
		"govActionLifetime": 0,
		"govActionDeposit": 0,
		"dRepDeposit": 0,
		"dRepActivity": 0,
		"minFeeRefScriptCostPerByte": null,
		"plutusV3CostModel": [],
		"constitution": {"anchor": {"dataHash": "", "url": ""}, "script": ""},
		"committee": {"members": {}, "threshold": null},
		"delegs": {},
		"initialDReps": {
			"keyHash-` + drepKeyHashHex + `": {
				"expiry": 500,
				"deposit": 500000000,
				"anchor": null
			}
		}
	}`
	require.NoError(
		t,
		nodeCfg.LoadConwayGenesisFromReader(
			strings.NewReader(conwayGenesisJson),
		),
	)

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Database:          db,
			CardanoNodeConfig: nodeCfg,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
		},
	}
	ls.currentTip.Point.Slot = 42
	require.NoError(t, ls.createGenesisBlock())

	drepKeyHash, err := hex.DecodeString(drepKeyHashHex)
	require.NoError(t, err)
	_, err = db.GetDrep(drepKeyHash, true, nil)
	require.ErrorIs(
		t, err, models.ErrDrepNotFound,
		"a genesis DRep must not be (re-)inserted after a Mithril "+
			"bootstrap -- the imported ledger snapshot is the only "+
			"authority on current DRep/delegation state",
	)

	require.NoError(t, ls.createGenesisBlock())
	_, err = db.GetDrep(drepKeyHash, true, nil)
	require.ErrorIs(t, err, models.ErrDrepNotFound)
}

func TestGenesisUtxoStorageAndRetrieval(t *testing.T) {
	t.Parallel()

	// Create temp directory for database
	tmpDir, err := os.MkdirTemp("", "genesis_utxo_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	logger := slog.New(
		slog.NewTextHandler(
			os.Stdout,
			&slog.HandlerOptions{Level: slog.LevelDebug},
		),
	)

	// Create database
	dbConfig := &database.Config{
		DataDir: tmpDir,
		Logger:  logger,
	}
	db, err := dbtest.NewDatabase(t, dbConfig)
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	// Load cardano config from embedded preview network
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"preview/config.json",
		"preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)

	// Get genesis UTxOs
	byronGenesis := nodeCfg.ByronGenesis()
	require.NotNil(t, byronGenesis)

	byronGenesisUtxos, err := byronGenesis.GenesisUtxos()
	require.NoError(t, err)
	t.Logf("Found %d Byron genesis UTxOs", len(byronGenesisUtxos))

	if len(byronGenesisUtxos) == 0 {
		t.Skip("No Byron genesis UTxOs to test")
	}

	// First, encode the genesis outputs to CBOR (like createGenesisBlock does)
	encodedUtxos := make([]lcommon.Utxo, len(byronGenesisUtxos))
	for i, utxo := range byronGenesisUtxos {
		// Encode the output to CBOR
		cborData, err := cbor.Encode(utxo.Output)
		require.NoError(t, err, "Failed to encode output %d", i)

		// Create a new Utxo with CBOR-encoded output
		switch output := utxo.Output.(type) {
		case byron.ByronTransactionOutput:
			newOutput := output
			(&newOutput).SetCbor(cborData)
			encodedUtxos[i] = lcommon.Utxo{
				Id:     utxo.Id,
				Output: newOutput,
			}
		default:
			t.Fatalf("Unexpected output type: %T", utxo.Output)
		}
		utxoTxID := utxo.Id.Id()
		t.Logf("Encoded UTxO %x#%d: %d bytes",
			utxoTxID[:8], utxo.Id.Index(), len(cborData))
	}

	// Get the Byron genesis hash to use as the synthetic block hash
	genesisHash, err := GenesisBlockHash(nodeCfg)
	require.NoError(t, err)

	// Create a transaction to store genesis UTxOs
	txn := db.Transaction(true)
	err = txn.Do(func(txn *database.Txn) error {
		// Build and store genesis block CBOR
		utxoOffsets := make(map[database.UtxoRef]database.CborOffset)

		// Build synthetic genesis block CBOR (simplified version)
		// First, encode each UTxO to get its CBOR
		var blockCbor []byte

		// Track offsets as we build the block
		for _, utxo := range encodedUtxos {
			txId := utxo.Id.Id().Bytes()
			outputIdx := utxo.Id.Index()

			// Get the output CBOR
			outputCbor := utxo.Output.Cbor()
			if len(outputCbor) == 0 {
				return fmt.Errorf(
					"UTxO %x#%d still has no CBOR after encoding",
					txId[:8], outputIdx,
				)
			}

			var txHashArray [32]byte
			copy(txHashArray[:], txId)

			ref := database.UtxoRef{
				TxId:      txHashArray,
				OutputIdx: outputIdx,
			}

			// Track offset within block (simplified: just concatenate)
			offset := uint32(len(blockCbor))
			blockCbor = append(blockCbor, outputCbor...)

			utxoOffsets[ref] = database.CborOffset{
				BlockSlot:  0,
				BlockHash:  genesisHash,
				ByteOffset: offset,
				ByteLength: uint32(len(outputCbor)),
			}

			t.Logf("UTxO %x#%d: offset=%d, length=%d",
				txId[:8], outputIdx, offset, len(outputCbor))
		}

		// Store the genesis block CBOR
		t.Logf("Storing genesis block CBOR: %d bytes", len(blockCbor))
		if err := db.SetGenesisCbor(0, genesisHash[:], blockCbor, txn); err != nil {
			return err
		}

		// Now store each genesis transaction
		for _, utxo := range encodedUtxos {
			txId := utxo.Id.Id().Bytes()
			outputIdx := utxo.Id.Index()

			var txHashArray [32]byte
			copy(txHashArray[:], txId)

			ref := database.UtxoRef{
				TxId:      txHashArray,
				OutputIdx: outputIdx,
			}

			offset, ok := utxoOffsets[ref]
			if !ok {
				return fmt.Errorf(
					"no offset for UTxO %x#%d after building block",
					txId[:8], outputIdx,
				)
			}

			// Store the offset
			offsetData := database.EncodeUtxoOffset(&offset)
			t.Logf(
				"Storing UTxO offset: %s",
				hex.EncodeToString(offsetData[:20]),
			)

			blob := db.Blob()
			if blob == nil {
				return fmt.Errorf("blob store is nil")
			}
			blobTxn := txn.Blob()
			if blobTxn == nil {
				return fmt.Errorf("blob transaction is nil")
			}

			if err := blob.SetUtxo(blobTxn, txId, outputIdx, offsetData); err != nil {
				return err
			}
		}

		return nil
	})
	require.NoError(t, err)

	// Now try to retrieve each genesis UTxO
	t.Log("Retrieving genesis UTxOs...")
	for _, utxo := range byronGenesisUtxos {
		txIDHash := utxo.Id.Id()
		txId := txIDHash[:]
		txIDPrefix := txIDHash[:8]
		outputIdx := utxo.Id.Index()

		// Try to get the UTxO from blob store
		readTxn := db.Transaction(false)
		blob := db.Blob()
		require.NotNil(t, blob)
		blobTxn := readTxn.Blob()
		require.NotNil(t, blobTxn)

		data, err := blob.GetUtxo(blobTxn, txId, outputIdx)
		if err != nil {
			t.Errorf("Failed to get UTxO %s#%d from blob: %v",
				hex.EncodeToString(txIDPrefix), outputIdx, err)
			readTxn.Rollback() //nolint:errcheck
			continue
		}

		t.Logf(
			"Retrieved data for %x#%d: %d bytes, first bytes: %s",
			txIDPrefix,
			outputIdx,
			len(data),
			hex.EncodeToString(data[:min(20, len(data))]),
		)

		// Check if it's an offset
		if database.IsUtxoOffsetStorage(data) {
			t.Logf("Data is offset storage (has DOFF magic)")

			// Decode the offset
			offset, err := database.DecodeUtxoOffset(data)
			require.NoError(
				t,
				err,
				"Failed to decode offset for %x#%d",
				txIDPrefix,
				outputIdx,
			)

			t.Logf(
				"Offset: slot=%d, hash=%x, offset=%d, length=%d",
				offset.BlockSlot,
				offset.BlockHash[:8],
				offset.ByteOffset,
				offset.ByteLength,
			)

			// Try to get the block
			blockCbor, _, err := blob.GetBlock(
				blobTxn,
				offset.BlockSlot,
				offset.BlockHash[:],
			)
			if err != nil {
				t.Errorf(
					"Failed to get block for offset: slot=%d, hash=%x, error=%v",
					offset.BlockSlot,
					offset.BlockHash[:8],
					err,
				)
				readTxn.Rollback() //nolint:errcheck
				continue
			}

			t.Logf("Got block: %d bytes", len(blockCbor))

			// Extract the UTxO CBOR
			end := uint64(offset.ByteOffset) + uint64(offset.ByteLength)
			if end > uint64(len(blockCbor)) {
				t.Errorf(
					"Offset out of bounds: offset=%d, length=%d, block_size=%d",
					offset.ByteOffset,
					offset.ByteLength,
					len(blockCbor),
				)
			} else {
				utxoCbor := blockCbor[offset.ByteOffset:end]
				t.Logf("Extracted UTxO CBOR: %d bytes, first bytes: %s",
					len(utxoCbor), hex.EncodeToString(utxoCbor[:min(20, len(utxoCbor))]))
			}
		} else {
			t.Logf("Data is raw CBOR (legacy format)")
		}

		readTxn.Rollback() //nolint:errcheck
	}
}

// TestCreateGenesisBlockSkipsUtxoInsertionAfterMithrilBootstrap is the
// regression test for blinklabs-io/dingo#4151: after a Mithril bootstrap,
// createGenesisBlock unconditionally recreated every Byron/Shelley genesis
// UTxO as a live row, without checking whether the imported ledger snapshot
// already reflects that output as spent. Found via cmd/node-parity against
// a real cardano-node: genesis-declared funds that the real chain spent long
// ago reappeared as live in dingo's answer, byte-for-byte matching the raw
// genesis declaration.
//
// Simulates the bootstrap shape the same way
// TestCreateGenesisBlockBackfillsMissingNetworkState does: a currentTip past
// slot 0 with no genesis CBOR yet stored, which is exactly the condition
// createGenesisBlock's own comment attributes to "after Mithril bootstrap
// which imports ledger state and ImmutableDB blocks but does not create the
// synthetic genesis block." Proves createGenesisBlock no longer inserts a
// live row for a real preview genesis UTxO on that path, while still
// creating the synthetic genesis block CBOR structurally (other code, e.g.
// this same function's own HasGenesisCbor short-circuit, depends on it
// existing).
func TestCreateGenesisBlockSkipsUtxoInsertionAfterMithrilBootstrap(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"preview/config.json",
		"preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)

	byronGenesisUtxos, err := nodeCfg.ByronGenesis().GenesisUtxos()
	require.NoError(t, err)
	require.NotEmpty(
		t, byronGenesisUtxos,
		"preview config must declare at least one Byron genesis UTxO "+
			"for this test to be meaningful",
	)
	sample := byronGenesisUtxos[0]
	sampleTxId := sample.Id.Id().Bytes()
	sampleOutputIdx := sample.Id.Index()

	genesisHash, err := GenesisBlockHash(nodeCfg)
	require.NoError(t, err)

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Database:          db,
			CardanoNodeConfig: nodeCfg,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
		},
	}
	// No genesis CBOR pre-seeded (unlike the normal from-genesis case),
	// and currentTip already past slot 0: this is exactly the shape
	// createGenesisBlock's own comment attributes to a fresh Mithril
	// bootstrap, before it has ever run.
	ls.currentTip.Point.Slot = 42
	require.NoError(t, ls.createGenesisBlock())

	require.True(
		t, db.HasGenesisCbor(0, genesisHash[:]),
		"the synthetic genesis block CBOR must still be created "+
			"structurally, even though its UTxOs are not inserted as live",
	)

	exists, err := db.UtxoExists(sampleTxId, sampleOutputIdx, nil)
	require.NoError(t, err)
	require.False(
		t, exists,
		"a genesis UTxO must not be (re-)inserted as a live row after a "+
			"Mithril bootstrap -- the imported ledger snapshot is the only "+
			"authority on whether it is still live or was already spent "+
			"before the bootstrap point",
	)

	// Re-running (as a real startup would on every restart) must remain
	// idempotent and continue to skip insertion.
	require.NoError(t, ls.createGenesisBlock())
	exists, err = db.UtxoExists(sampleTxId, sampleOutputIdx, nil)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestFailedEnactmentClearRestoresRatificationOnRollback(t *testing.T) {
	t.Parallel()

	f := newTreasuryRolloverFixture(t, 100)
	withdrawAddress, _, _ := f.rewardAddress(t, 0x91)
	proposal := f.addProposal(
		t,
		0x92,
		501,
		map[*lcommon.Address]uint64{withdrawAddress: 40},
		[]byte{0xff},
		1,
		true,
	)
	before := f.proposal(t, proposal)
	require.NotNil(t, before.RatifiedSlot)
	originalRatifiedSlot := *before.RatifiedSlot

	result := f.rollover(t, f.currentEpoch, f.currentPParams)
	cleared := f.proposal(t, proposal)
	require.Nil(t, cleared.RatifiedSlot)

	rollbackPoint := result.NewCurrentEpoch.StartSlot - 1
	require.GreaterOrEqual(t, rollbackPoint, originalRatifiedSlot)
	require.NoError(
		t,
		f.db.DeleteGovernanceProposalsAfterSlot(rollbackPoint, nil),
	)

	restored := f.proposal(t, proposal)
	require.NotNil(
		t,
		restored.RatifiedSlot,
		"rollback must restore the earlier ratification marker",
	)
	require.Equal(t, originalRatifiedSlot, *restored.RatifiedSlot)
}

func TestNodeLocalEnactmentWriteErrorAbortsBoundary(t *testing.T) {
	t.Parallel()

	f := newTreasuryRolloverFixture(t, 100)
	withdrawAddress, returnAddress, stakeCredential := f.rewardAddress(t, 0xa1)
	proposal := f.addProposal(
		t,
		0xa2,
		501,
		map[*lcommon.Address]uint64{withdrawAddress: 40},
		returnAddress,
		0,
		true,
	)
	before := f.proposal(t, proposal)
	require.NotNil(t, before.RatifiedSlot)
	originalRatifiedSlot := *before.RatifiedSlot

	raw, err := dbtest.RawSQLiteMetadata(t, f.db)
	require.NoError(t, err)
	_, err = raw.Exec(`
CREATE TRIGGER fail_governance_enact
BEFORE UPDATE OF enacted_slot ON governance_proposal
WHEN NEW.enacted_slot IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'injected enactment write failure');
END`)
	require.NoError(t, err)

	txn := f.db.Transaction(true)
	err = txn.Do(func(txn *database.Txn) error {
		_, rolloverErr := f.ls.processEpochRollover(
			txn,
			f.currentEpoch,
			eras.ConwayEraDesc,
			f.currentPParams,
			false,
		)
		return rolloverErr
	})
	assert.Error(t, err, "a storage error must abort the boundary transaction")

	after := f.proposal(t, proposal)
	require.NotNil(t, after.RatifiedSlot)
	assert.Equal(
		t,
		originalRatifiedSlot,
		*after.RatifiedSlot,
		"an aborted boundary must preserve the earlier ratification marker",
	)
	assert.Nil(t, after.EnactedSlot)
	assert.Zero(t, f.accountReward(t, stakeCredential))
	treasury, _, _ := networkState(t, f.db)
	assert.Equal(t, uint64(100), treasury)
	advancedEpoch, epochErr := f.db.Metadata().GetEpoch(
		f.currentEpoch.EpochId+1,
		nil,
	)
	require.NoError(t, epochErr)
	assert.Nil(t, advancedEpoch)
}

func TestEnactmentWriteHealthyControlCommitsBoundary(t *testing.T) {
	t.Parallel()

	f := newTreasuryRolloverFixture(t, 100)
	withdrawAddress, returnAddress, stakeCredential := f.rewardAddress(t, 0xb1)
	proposal := f.addProposal(
		t,
		0xb2,
		501,
		map[*lcommon.Address]uint64{withdrawAddress: 40},
		returnAddress,
		0,
		true,
	)

	result := f.rollover(t, f.currentEpoch, f.currentPParams)
	after := f.proposal(t, proposal)
	require.NotNil(t, after.EnactedSlot)
	require.Equal(t, result.NewCurrentEpoch.StartSlot, *after.EnactedSlot)
	require.Equal(t, uint64(40), f.accountReward(t, stakeCredential))
	treasury, _, _ := networkState(t, f.db)
	require.Equal(t, uint64(60), treasury)
	advancedEpoch, err := f.db.Metadata().GetEpoch(
		f.currentEpoch.EpochId+1,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, advancedEpoch)
}

const treasuryRolloverGenesisHash = "0101010101010101010101010101010101010101010101010101010101010101"

type treasuryRolloverFixture struct {
	ls             *LedgerState
	db             *database.Database
	currentEpoch   models.Epoch
	currentPParams *conway.ConwayProtocolParameters
	hotCredential  []byte
}

func newTreasuryRolloverFixture(
	t *testing.T,
	treasury uint64,
) *treasuryRolloverFixture {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	cfg := newTestEraHistoryCfg(t)
	cfg.ShelleyGenesisHash = treasuryRolloverGenesisHash
	currentEpoch := newTestEpoch(5, 500, 100, eras.ConwayEraDesc.Id)
	require.NoError(t, db.SetEpoch(
		currentEpoch.StartSlot,
		currentEpoch.EpochId,
		currentEpoch.Nonce,
		currentEpoch.EvolvingNonce,
		currentEpoch.CandidateNonce,
		currentEpoch.LastEpochBlockNonce,
		currentEpoch.EraId,
		currentEpoch.SlotLength,
		currentEpoch.LengthInSlots,
		nil,
	))
	require.NoError(t, db.Metadata().SetNetworkState(treasury, 1_000, 499, nil))

	pparams := donationTestConwayPParams(10)
	pparams.MinCommitteeSize = 1
	pparams.DRepVotingThresholds.TreasuryWithdrawal = cbor.Rat{
		Rat: big.NewRat(0, 1),
	}

	coldCredential := repeatByte(28, 0xc1)
	hotCredential := repeatByte(28, 0xc2)
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{{
		ColdCredHash: coldCredential,
		ExpiresEpoch: currentEpoch.EpochId + 20,
		AddedSlot:    1,
	}}, nil))
	require.NoError(t, db.SetCommitteeQuorum(big.NewRat(1, 1), 1, nil))
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO auth_committee_hot (
    cold_credential, host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?)`, coldCredential, hotCredential, 1, 1)
	require.NoError(t, err)

	ls := &LedgerState{
		db:             db,
		currentEra:     eras.ConwayEraDesc,
		currentEpoch:   currentEpoch,
		currentPParams: pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger: slog.New(slog.NewJSONHandler(
				io.Discard,
				nil,
			)),
		},
	}
	return &treasuryRolloverFixture{
		ls:             ls,
		db:             db,
		currentEpoch:   currentEpoch,
		currentPParams: pparams,
		hotCredential:  hotCredential,
	}
}

func (f *treasuryRolloverFixture) rewardAddress(
	t *testing.T,
	marker byte,
) (*lcommon.Address, []byte, []byte) {
	t.Helper()
	stakeCredential := repeatByte(28, marker)
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCredential,
	)
	require.NoError(t, err)
	addressBytes, err := address.Bytes()
	require.NoError(t, err)
	require.NoError(t, f.db.CreateAccount(nil, &models.Account{
		StakingKey: stakeCredential,
		Reward:     types.Uint64(0),
		Active:     true,
	}))
	return &address, addressBytes, stakeCredential
}

func (f *treasuryRolloverFixture) addProposal(
	t *testing.T,
	marker byte,
	addedSlot uint64,
	withdrawals map[*lcommon.Address]uint64,
	returnAddress []byte,
	deposit uint64,
	ratified bool,
) *models.GovernanceProposal {
	t.Helper()
	actionCbor, err := cbor.Encode(&lcommon.TreasuryWithdrawalGovAction{
		Type:        2,
		Withdrawals: withdrawals,
	})
	require.NoError(t, err)
	proposal := &models.GovernanceProposal{
		TxHash:        repeatByte(32, marker),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: f.currentEpoch.EpochId,
		ExpiresEpoch:  f.currentEpoch.EpochId + 20,
		AnchorURL:     "https://example.invalid/treasury-withdrawal",
		AnchorHash:    repeatByte(32, marker+1),
		Deposit:       deposit,
		ReturnAddress: returnAddress,
		GovActionCbor: actionCbor,
		AddedSlot:     addedSlot,
	}
	if ratified {
		ratifiedEpoch := f.currentEpoch.EpochId
		ratifiedSlot := f.currentEpoch.StartSlot + 50
		proposal.RatifiedEpoch = &ratifiedEpoch
		proposal.RatifiedSlot = &ratifiedSlot
	}
	require.NoError(t, f.db.SetGovernanceProposal(proposal, nil))
	require.NoError(t, f.db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      proposal.ID,
		VoterType:       models.VoterTypeCC,
		VoterCredential: f.hotCredential,
		Vote:            models.VoteYes,
		AddedSlot:       addedSlot + 1,
	}, nil))
	return proposal
}

func (f *treasuryRolloverFixture) rollover(
	t *testing.T,
	currentEpoch models.Epoch,
	currentPParams lcommon.ProtocolParameters,
) *EpochRolloverResult {
	t.Helper()
	var result *EpochRolloverResult
	txn := f.db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		var rolloverErr error
		result, rolloverErr = f.ls.processEpochRollover(
			txn,
			currentEpoch,
			eras.ConwayEraDesc,
			currentPParams,
			false,
		)
		return rolloverErr
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	return result
}

func (f *treasuryRolloverFixture) proposal(
	t *testing.T,
	proposal *models.GovernanceProposal,
) *models.GovernanceProposal {
	t.Helper()
	loaded, err := f.db.GetGovernanceProposal(
		proposal.TxHash,
		proposal.ActionIndex,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	return loaded
}

func (f *treasuryRolloverFixture) accountReward(
	t *testing.T,
	stakeCredential []byte,
) uint64 {
	t.Helper()
	account, err := f.db.GetAccountByCredential(
		0,
		stakeCredential,
		false,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, account)
	return uint64(account.Reward)
}

func TestProcessEpochRolloverTreasuryRatificationUsesRunningBudget(
	t *testing.T,
) {
	t.Parallel()

	f := newTreasuryRolloverFixture(t, 100)
	firstAddress, firstReturn, firstCredential := f.rewardAddress(t, 0x11)
	secondAddress, secondReturn, secondCredential := f.rewardAddress(t, 0x21)
	thirdAddress, thirdReturn, _ := f.rewardAddress(t, 0x31)
	fourthAddress, fourthReturn, fourthCredential := f.rewardAddress(t, 0x41)
	fifthAddress, _, _ := f.rewardAddress(t, 0x51)

	first := f.addProposal(
		t, 0x61, 501,
		map[*lcommon.Address]uint64{firstAddress: 70},
		firstReturn, 0, false,
	)
	second := f.addProposal(
		t, 0x62, 503,
		map[*lcommon.Address]uint64{secondAddress: 40},
		secondReturn, 0, false,
	)
	overflow := f.addProposal(
		t, 0x63, 505,
		map[*lcommon.Address]uint64{
			thirdAddress: ^uint64(0),
			fifthAddress: 1,
		},
		thirdReturn, 0, false,
	)
	fourth := f.addProposal(
		t, 0x64, 507,
		map[*lcommon.Address]uint64{fourthAddress: 30},
		fourthReturn, 0, false,
	)

	firstRollover := f.rollover(t, f.currentEpoch, f.currentPParams)
	assert.Equal(
		t,
		f.currentEpoch.EpochId+1,
		firstRollover.NewCurrentEpoch.EpochId,
	)
	assert.NotNil(t, f.proposal(t, first).RatifiedEpoch)
	assert.Nil(t, f.proposal(t, second).RatifiedEpoch)
	assert.Nil(t, f.proposal(t, overflow).RatifiedEpoch)
	assert.NotNil(t, f.proposal(t, fourth).RatifiedEpoch)

	secondRollover := f.rollover(
		t,
		firstRollover.NewCurrentEpoch,
		firstRollover.NewCurrentPParams,
	)
	assert.Equal(
		t,
		f.currentEpoch.EpochId+2,
		secondRollover.NewCurrentEpoch.EpochId,
	)
	assert.NotNil(t, f.proposal(t, first).EnactedEpoch)
	assert.Nil(t, f.proposal(t, second).RatifiedEpoch)
	assert.Nil(t, f.proposal(t, overflow).RatifiedEpoch)
	assert.NotNil(t, f.proposal(t, fourth).EnactedEpoch)
	assert.Equal(t, uint64(70), f.accountReward(t, firstCredential))
	assert.Equal(t, uint64(0), f.accountReward(t, secondCredential))
	assert.Equal(t, uint64(30), f.accountReward(t, fourthCredential))
	treasury, _, _ := networkState(t, f.db)
	assert.Zero(t, treasury)
}

func TestProcessEpochRolloverEnactmentFailureRollsBackAndRetries(
	t *testing.T,
) {
	t.Parallel()

	f := newTreasuryRolloverFixture(t, 100)
	failingAddress, failingReturn, failingCredential := f.rewardAddress(t, 0x71)
	succeedingAddress, succeedingReturn, succeedingCredential := f.rewardAddress(
		t,
		0x72,
	)
	failing := f.addProposal(
		t, 0x73, 501,
		map[*lcommon.Address]uint64{failingAddress: 60},
		[]byte{0xff}, 1, true,
	)
	succeeding := f.addProposal(
		t, 0x74, 503,
		map[*lcommon.Address]uint64{succeedingAddress: 50},
		succeedingReturn, 0, true,
	)
	firstRollover := f.rollover(t, f.currentEpoch, f.currentPParams)
	assert.Equal(
		t,
		f.currentEpoch.EpochId+1,
		firstRollover.NewCurrentEpoch.EpochId,
	)
	failedAfterRollover := f.proposal(t, failing)
	assert.Nil(t, failedAfterRollover.EnactedEpoch)
	assert.Nil(t, failedAfterRollover.RatifiedEpoch)
	assert.Nil(t, failedAfterRollover.ExpiredEpoch)
	assert.Nil(t, failedAfterRollover.DeletedSlot)
	assert.NotNil(t, f.proposal(t, succeeding).EnactedEpoch)
	assert.Zero(t, f.accountReward(t, failingCredential))
	assert.Equal(t, uint64(50), f.accountReward(t, succeedingCredential))
	treasury, reserves, _ := networkState(t, f.db)
	assert.Equal(t, uint64(50), treasury)

	retry := f.proposal(t, failing)
	retry.ReturnAddress = failingReturn
	require.NoError(t, f.db.SetGovernanceProposal(retry, nil))
	require.NoError(t, f.db.Metadata().SetNetworkState(
		70,
		reserves,
		firstRollover.NewCurrentEpoch.StartSlot+50,
		nil,
	))

	secondRollover := f.rollover(
		t,
		firstRollover.NewCurrentEpoch,
		firstRollover.NewCurrentPParams,
	)
	assert.Equal(
		t,
		f.currentEpoch.EpochId+2,
		secondRollover.NewCurrentEpoch.EpochId,
	)
	assert.Nil(t, f.proposal(t, failing).EnactedEpoch)
	assert.NotNil(t, f.proposal(t, failing).RatifiedEpoch)
	assert.Zero(t, f.accountReward(t, failingCredential))
	assert.Equal(t, uint64(50), f.accountReward(t, succeedingCredential))
	treasury, _, _ = networkState(t, f.db)
	assert.Equal(t, uint64(70), treasury)

	thirdRollover := f.rollover(
		t,
		secondRollover.NewCurrentEpoch,
		secondRollover.NewCurrentPParams,
	)
	assert.Equal(
		t,
		f.currentEpoch.EpochId+3,
		thirdRollover.NewCurrentEpoch.EpochId,
	)
	assert.NotNil(t, f.proposal(t, failing).EnactedEpoch)
	assert.Equal(t, uint64(60), f.accountReward(t, failingCredential))
	assert.Equal(t, uint64(50), f.accountReward(t, succeedingCredential))
	treasury, _, _ = networkState(t, f.db)
	assert.Equal(t, uint64(10), treasury)
}

func TestProcessEpochRolloverReplayEnactmentFailureRemainsFatal(
	t *testing.T,
) {
	t.Parallel()

	f := newTreasuryRolloverFixture(t, 100)
	withdrawAddress, _, stakeCredential := f.rewardAddress(t, 0x81)
	proposal := f.addProposal(
		t, 0x82, 501,
		map[*lcommon.Address]uint64{withdrawAddress: 40},
		[]byte{0xff}, 1, true,
	)
	enactedEpoch := f.currentEpoch.EpochId + 1
	enactedSlot := f.currentEpoch.StartSlot +
		uint64(f.currentEpoch.LengthInSlots)
	proposal.EnactedEpoch = &enactedEpoch
	proposal.EnactedSlot = &enactedSlot
	require.NoError(t, f.db.SetGovernanceProposal(proposal, nil))

	txn := f.db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		_, rolloverErr := f.ls.processEpochRollover(
			txn,
			f.currentEpoch,
			eras.ConwayEraDesc,
			f.currentPParams,
			false,
		)
		return rolloverErr
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "replay enacted proposal")
	assert.Zero(t, f.accountReward(t, stakeCredential))
	treasury, _, _ := networkState(t, f.db)
	assert.Equal(t, uint64(100), treasury)
	newEpoch, err := f.db.Metadata().GetEpoch(enactedEpoch, nil)
	require.NoError(t, err)
	assert.Nil(t, newEpoch)
}

// hardForkRatifyFixture reproduces the exact Preview Plomin hard-fork
// incident (dingo#4441) at a real epoch-rollover level: a HardForkInitiation
// proposal, 49 SPO votes' worth of yes/no stake collapsed into two pools
// carrying the real observed mark[740]/mark[741]/mark[742] ratios
// (0.4779/0.4757/0.6283), and a single seated CC member voting yes with a
// 1/1 quorum -- matching the live incident's committee_member/
// auth_committee_hot state. Still at protocol major 9 (bootstrap), so the
// DRep gate is bypassed entirely and only the SPO gate governs ratification,
// exactly as it did on the real network.
type hardForkRatifyFixture struct {
	ls       *LedgerState
	db       *database.Database
	proposal *models.GovernanceProposal
	pparams  *conway.ConwayProtocolParameters
}

const (
	hfrYesPool    = "hfr-yes-pool-2222222222222"
	hfrSilentPool = "hfr-silent-pool-11111111111"
)

func newHardForkRatifyFixture(t *testing.T) *hardForkRatifyFixture {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	// currentEpoch is epoch 741, already on disk; the fixture's first
	// rollover call transitions the boundary into 742.
	currentEpoch := newTestEpoch(741, 74_100, 100, eras.ConwayEraDesc.Id)
	require.NoError(t, db.SetEpoch(
		currentEpoch.StartSlot,
		currentEpoch.EpochId,
		currentEpoch.Nonce,
		currentEpoch.EvolvingNonce,
		currentEpoch.CandidateNonce,
		currentEpoch.LastEpochBlockNonce,
		currentEpoch.EraId,
		currentEpoch.SlotLength,
		currentEpoch.LengthInSlots,
		nil,
	))

	// PV9: still bootstrap, the real incident's protocol version.
	pparams := donationTestConwayPParams(9)
	pparams.MinCommitteeSize = 1

	// mark[740]/mark[741]/mark[742], the exact ratios dingo#4441 measured on
	// Preview. Yes stake is hfrYesPool's explicit Yes vote; the remainder is
	// hfrSilentPool, which casts no vote at all -- HardForkInitiation always
	// keeps a silent pool's stake in the active denominator as implicit No
	// (tallySPOVotes), matching cardano-ledger's checkDisallowedVotes-adjacent
	// SPO semantics for this action type.
	for _, row := range []struct {
		epoch    uint64
		yesStake uint64
	}{
		{740, 4_779}, // ratio 0.4779, below the 0.51 threshold
		{741, 4_757}, // ratio 0.4757, below the 0.51 threshold
		{742, 6_283}, // ratio 0.6283, clears the 0.51 threshold
	} {
		require.NoError(t, db.Metadata().SavePoolStakeSnapshot(
			&models.PoolStakeSnapshot{
				Epoch:        row.epoch,
				SnapshotType: models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:  []byte(hfrYesPool),
				TotalStake:   types.Uint64(row.yesStake),
			},
			nil,
		))
		require.NoError(t, db.Metadata().SavePoolStakeSnapshot(
			&models.PoolStakeSnapshot{
				Epoch:        row.epoch,
				SnapshotType: models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:  []byte(hfrSilentPool),
				TotalStake:   types.Uint64(10_000 - row.yesStake),
			},
			nil,
		))
	}

	// Committee: one seated member, 1/1 quorum -- matches the live
	// incident's committee_member/auth_committee_hot state exactly (issue
	// text: "committee_member holds one seated member ... auth_committee_hot
	// maps it to ... the CC yes ratio is 1/1").
	coldCredential := repeatByte(28, 0xC1)
	hotCredential := repeatByte(28, 0xC2)
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{{
		ColdCredHash: coldCredential,
		ExpiresEpoch: 1000,
		AddedSlot:    1,
	}}, nil))
	require.NoError(t, db.SetCommitteeQuorum(big.NewRat(1, 1), 1, nil))
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO auth_committee_hot (
    cold_credential, host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?)`, coldCredential, hotCredential, 1, 1)
	require.NoError(t, err)

	action := &lcommon.HardForkInitiationGovAction{Type: 1}
	action.ProtocolVersion.Major = 10
	action.ProtocolVersion.Minor = 0
	actionCbor, err := cbor.Encode(action)
	require.NoError(t, err)

	proposal := &models.GovernanceProposal{
		TxHash:        repeatByte(32, 0x49),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
		ProposedEpoch: 737,
		ExpiresEpoch:  767,
		// Deposit 0 keeps ratificationEnactmentPrecondition's return-address
		// decode (which real proposals need) out of scope here -- this test
		// is about the SPO stake epoch selection, not deposit handling.
		Deposit:       0,
		ReturnAddress: repeatByte(29, 0),
		AnchorURL:     "https://example.invalid/plomin",
		AnchorHash:    repeatByte(32, 0x4a),
		GovActionCbor: actionCbor,
		AddedSlot:     1,
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))
	loaded, err := db.GetGovernanceProposal(proposal.TxHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, loaded)

	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      loaded.ID,
		VoterType:       models.VoterTypeCC,
		VoterCredential: hotCredential,
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))
	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      loaded.ID,
		VoterType:       models.VoterTypeSPO,
		VoterCredential: []byte(hfrYesPool),
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))

	cfg := newTestEraHistoryCfg(t)
	cfg.ShelleyGenesisHash = treasuryRolloverGenesisHash
	ls := &LedgerState{
		db:             db,
		currentEra:     eras.ConwayEraDesc,
		currentEpoch:   currentEpoch,
		currentPParams: pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	return &hardForkRatifyFixture{
		ls:       ls,
		db:       db,
		proposal: loaded,
		pparams:  pparams,
	}
}

func (f *hardForkRatifyFixture) rollover(
	t *testing.T,
	currentEpoch models.Epoch,
	pparams lcommon.ProtocolParameters,
) *EpochRolloverResult {
	t.Helper()
	var result *EpochRolloverResult
	txn := f.db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		var rolloverErr error
		result, rolloverErr = f.ls.processEpochRollover(
			txn,
			currentEpoch,
			eras.ConwayEraDesc,
			pparams,
			false,
		)
		return rolloverErr
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	// RATIFY runs after the boundary commits; its marks are durable once
	// the decision settles.
	require.NoError(t, f.ls.WaitEpochBoundaryJob(t.Context()))
	return result
}

func (f *hardForkRatifyFixture) reloadProposal(
	t *testing.T,
) *models.GovernanceProposal {
	t.Helper()
	loaded, err := f.db.GetGovernanceProposal(
		f.proposal.TxHash, f.proposal.ActionIndex, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	return loaded
}

// TestHardForkInitiation_RatifiesAtRealIncidentBoundary reproduces
// dingo#4441: the Preview Plomin hard fork (protocol major 9 -> 10) must
// ratify at the boundary into epoch 742, using mark[742] (ratio 0.6283),
// not mark[740] (0.4779) -- reproducing the real network's
// ratified_epoch=742. Before the stakeEpochFor fix this proposal never
// ratifies at this boundary (it would need to wait until mark[742] became
// readable as mark[newEpoch-2], i.e. two epochs later than upstream, and in
// the live incident the node halted on a downstream PV9-bootstrap
// validation rule before ever reaching that point).
func TestHardForkInitiation_RatifiesAtRealIncidentBoundary(t *testing.T) {
	t.Parallel()

	f := newHardForkRatifyFixture(t)

	before := f.reloadProposal(t)
	require.Nil(t, before.RatifiedEpoch,
		"proposal must start unratified")

	result := f.rollover(t, f.ls.currentEpoch, f.pparams)
	require.Equal(t, uint64(742), result.NewCurrentEpoch.EpochId)

	after := f.reloadProposal(t)
	require.NotNil(t, after.RatifiedEpoch,
		"HardForkInitiation must ratify at the boundary into 742 using "+
			"mark[742] (0.6283 >= 0.51); mark[740] (0.4779) alone would "+
			"never clear the threshold")
	require.Equal(t, uint64(742), *after.RatifiedEpoch)
}

// TestHardForkInitiation_EnactsOneBoundaryAfterRatification extends the
// above through the enactment boundary, reproducing the real chain's
// ratified_epoch=742 / enacted_epoch=743 pair end to end: protocol major
// must read 10 only after the boundary into 743, not before.
func TestHardForkInitiation_EnactsOneBoundaryAfterRatification(t *testing.T) {
	t.Parallel()

	f := newHardForkRatifyFixture(t)

	ratifyResult := f.rollover(t, f.ls.currentEpoch, f.pparams)
	require.Equal(t, uint64(742), ratifyResult.NewCurrentEpoch.EpochId)
	ratified := f.reloadProposal(t)
	require.NotNil(t, ratified.RatifiedEpoch)
	require.Nil(t, ratified.EnactedEpoch,
		"ratification and enactment must land on different boundaries")

	enactResult := f.rollover(
		t, ratifyResult.NewCurrentEpoch, f.pparams,
	)
	require.Equal(t, uint64(743), enactResult.NewCurrentEpoch.EpochId)
	require.NotNil(t, enactResult.NewCurrentPParams)
	conwayParams, ok := enactResult.NewCurrentPParams.(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	require.Equal(t, uint(10), conwayParams.ProtocolVersion.Major,
		"protocol major must read 10 only after the boundary into 743, "+
			"matching the real network's enacted_epoch=743")

	enacted := f.reloadProposal(t)
	require.NotNil(t, enacted.EnactedEpoch)
	require.Equal(t, uint64(743), *enacted.EnactedEpoch)
}

// hardForkRatifyLiveStakeFixture is hardForkRatifyFixture's sibling for
// proving the *plumbing* half of dingo#4441, not the epoch-offset half: it
// seeds live Pool/Account/UTxO delegation state -- never a pre-written
// pool_stake_snapshot "mark" row -- and drives the same real
// processEpochRollover path through the same epoch-boundary hooks node.go
// wires in production. That is the only way governance's RATIFY phase can
// see mark[NewEpoch] at all: that row is written only at the very end of the
// same rollover transaction, after RATIFY has already run (see
// SetCurrentBoundarySPOStakeHook's doc comment).
type hardForkRatifyLiveStakeFixture struct {
	*hardForkRatifyFixture
	snapshotMgr *snapshot.Manager
}

const (
	hfrLiveYesPool    = "hfr-live-yes-pool-3333333333"
	hfrLiveSilentPool = "hfr-live-silent-pool-4444444"
)

// newHardForkRatifyLiveStakeFixture builds the same proposal/committee/vote
// state as newHardForkRatifyFixture, but backs the SPO stake with live
// Pool/Account/UTxO rows at the decisive 0.6283 ratio instead of a
// pre-written mark[742] row, and wires the authoritative persist hook
// unconditionally (see the comment below for why not the SNAP-point
// fast-path hook too). wireCurrentBoundaryHook controls only the new
// dingo#4441 hook (SetCurrentBoundarySPOStakeHook), so a test can wire
// everything else exactly like production and isolate what that one hook
// contributes.
func newHardForkRatifyLiveStakeFixture(
	t *testing.T,
	wireCurrentBoundaryHook bool,
) *hardForkRatifyLiveStakeFixture {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	currentEpoch := newTestEpoch(741, 74_100, 100, eras.ConwayEraDesc.Id)
	require.NoError(t, db.SetEpoch(
		currentEpoch.StartSlot,
		currentEpoch.EpochId,
		currentEpoch.Nonce,
		currentEpoch.EvolvingNonce,
		currentEpoch.CandidateNonce,
		currentEpoch.LastEpochBlockNonce,
		currentEpoch.EraId,
		currentEpoch.SlotLength,
		currentEpoch.LengthInSlots,
		nil,
	))

	pparams := donationTestConwayPParams(9)
	pparams.MinCommitteeSize = 1

	// Live delegation state at the decisive ratio (0.6283, mark[742] in the
	// real incident) -- no pool_stake_snapshot row exists for epoch 742 at
	// all. ImportPool + CreateAccount + CreateUtxo is exactly how a normal
	// block-processing run builds the stake the SNAP-point calculator reads.
	seedLiveDelegatedStake(t, db, hfrLiveYesPool, 6_283)
	seedLiveDelegatedStake(t, db, hfrLiveSilentPool, 3_717)

	coldCredential := repeatByte(28, 0xD1)
	hotCredential := repeatByte(28, 0xD2)
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{{
		ColdCredHash: coldCredential,
		ExpiresEpoch: 1000,
		AddedSlot:    1,
	}}, nil))
	require.NoError(t, db.SetCommitteeQuorum(big.NewRat(1, 1), 1, nil))
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO auth_committee_hot (
    cold_credential, host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?)`, coldCredential, hotCredential, 1, 1)
	require.NoError(t, err)

	action := &lcommon.HardForkInitiationGovAction{Type: 1}
	action.ProtocolVersion.Major = 10
	action.ProtocolVersion.Minor = 0
	actionCbor, err := cbor.Encode(action)
	require.NoError(t, err)

	proposal := &models.GovernanceProposal{
		TxHash:        repeatByte(32, 0x51),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
		ProposedEpoch: 737,
		ExpiresEpoch:  767,
		Deposit:       0,
		ReturnAddress: repeatByte(29, 0),
		AnchorURL:     "https://example.invalid/plomin-live",
		AnchorHash:    repeatByte(32, 0x52),
		GovActionCbor: actionCbor,
		AddedSlot:     1,
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))
	loaded, err := db.GetGovernanceProposal(proposal.TxHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, loaded)

	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      loaded.ID,
		VoterType:       models.VoterTypeCC,
		VoterCredential: hotCredential,
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))
	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      loaded.ID,
		VoterType:       models.VoterTypeSPO,
		VoterCredential: []byte(hfrLiveYesPool),
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))

	cfg := newTestEraHistoryCfg(t)
	cfg.ShelleyGenesisHash = treasuryRolloverGenesisHash
	ls := &LedgerState{
		db:             db,
		currentEra:     eras.ConwayEraDesc,
		currentEpoch:   currentEpoch,
		currentPParams: pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Wire the authoritative persist hook, matching production, but not the
	// SNAP-point fast-path stake hook: this synthetic fixture seeds
	// Pool/Account/Utxo rows directly rather than through real block
	// processing, so the live reward aggregate ComputeEpochBoundarySnapshot's
	// fast path reads is empty here even though the historical
	// reconstruction both CaptureEpochBoundarySnapshot and
	// CurrentBoundarySPOStakeRows fall back to is not. That fallback path is
	// exactly what a real node also uses whenever the fast path is unset or
	// fails (TestCaptureEpochBoundarySnapshotStakeHookFailureDeferred), so
	// this is still a realistic, supported configuration -- not a
	// fixture-only shortcut.
	snapshotMgr := snapshot.NewManager(db, event.NewEventBus(nil, nil), nil)
	ls.SetEpochBoundarySnapshotHook(
		func(txn *database.Txn, evt event.EpochTransitionEvent) error {
			return snapshotMgr.CaptureEpochBoundarySnapshot(
				context.Background(), txn, evt,
			)
		},
	)
	if wireCurrentBoundaryHook {
		ls.SetCurrentBoundarySPOStakeHook(
			func(
				txn *database.Txn,
				evt event.EpochTransitionEvent,
			) ([]*models.PoolStakeSnapshot, error) {
				return snapshotMgr.CurrentBoundarySPOStakeRows(
					context.Background(), txn, evt,
				)
			},
		)
	}

	return &hardForkRatifyLiveStakeFixture{
		hardForkRatifyFixture: &hardForkRatifyFixture{
			ls:       ls,
			db:       db,
			proposal: loaded,
			pparams:  pparams,
		},
		snapshotMgr: snapshotMgr,
	}
}

// seedLiveDelegatedStake registers a pool with one delegator holding amount
// (in whole ADA-equivalent units matching the fixture's 10_000-unit ratio
// scale) as live Pool/Account/UTxO state, matching how a normal sync
// populates the tables the SNAP-point calculator reads.
func seedLiveDelegatedStake(
	t *testing.T,
	db *database.Database,
	poolKeyHash string,
	amount uint64,
) {
	t.Helper()
	stakingKey := append([]byte(nil), []byte(poolKeyHash)...)
	for len(stakingKey) < 28 {
		stakingKey = append(stakingKey, 0)
	}
	stakingKey = stakingKey[:28]

	require.NoError(t, db.ImportPool(nil, &models.Pool{
		PoolKeyHash: []byte(poolKeyHash),
		VrfKeyHash:  make([]byte, 32),
		Pledge:      1_000_000,
		Cost:        340_000_000,
		Margin:      &types.Rat{Rat: big.NewRat(1, 100)},
	}, &models.PoolRegistration{
		PoolKeyHash: []byte(poolKeyHash),
		AddedSlot:   74_100,
		Pledge:      1_000_000,
		Cost:        340_000_000,
		Margin:      &types.Rat{Rat: big.NewRat(1, 100)},
		VrfKeyHash:  make([]byte, 32),
	}))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: stakingKey,
		Pool:       []byte(poolKeyHash),
		AddedSlot:  74_100,
		Active:     true,
	}))
	require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
		TxId:       repeatByte(32, poolKeyHash[len(poolKeyHash)-1]),
		OutputIdx:  0,
		StakingKey: stakingKey,
		Amount:     types.Uint64(amount),
		AddedSlot:  74_100,
	}))
}

// TestHardForkInitiation_RatifiesFromLiveStakeWithHookWired proves the
// production wiring (node.go's SetCurrentBoundarySPOStakeHook alongside the
// other two epoch-boundary hooks) makes RATIFY see mark[NewEpoch] from live
// state, with no pre-written pool_stake_snapshot row involved at all.
func TestHardForkInitiation_RatifiesFromLiveStakeWithHookWired(t *testing.T) {
	t.Parallel()

	f := newHardForkRatifyLiveStakeFixture(t, true)

	result := f.rollover(t, f.ls.currentEpoch, f.pparams)
	require.Equal(t, uint64(742), result.NewCurrentEpoch.EpochId)

	after := f.reloadProposal(t)
	require.NotNil(t, after.RatifiedEpoch,
		"wiring SetCurrentBoundarySPOStakeHook must let RATIFY see "+
			"mark[742] from live state and ratify at 0.6283 >= 0.51")
	require.Equal(t, uint64(742), *after.RatifiedEpoch)
}

// TestHardForkInitiation_NeverRatifiesWithoutCurrentBoundaryHook is the
// negative control for the plumbing half of dingo#4441: even after the
// stakeEpochFor offset fix, wiring only the pre-existing SNAP-point stake
// and capture hooks (exactly as before this change) leaves governance
// reading the not-yet-written mark[742] row and permanently unable to
// ratify -- worse than the original epoch-lag bug, not better. This is the
// scenario SetCurrentBoundarySPOStakeHook's doc comment warns a production
// node must never leave unwired.
func TestHardForkInitiation_NeverRatifiesWithoutCurrentBoundaryHook(
	t *testing.T,
) {
	t.Parallel()

	f := newHardForkRatifyLiveStakeFixture(t, false)

	result := f.rollover(t, f.ls.currentEpoch, f.pparams)
	require.Equal(t, uint64(742), result.NewCurrentEpoch.EpochId)

	after := f.reloadProposal(t)
	require.Nil(t, after.RatifiedEpoch,
		"without SetCurrentBoundarySPOStakeHook wired, governance falls "+
			"back to reading the not-yet-persisted mark[742] row and must "+
			"see zero SPO stake, never ratifying")
}

// A hard fork is found only after the SNAP point has registered the deferred
// capture, so the boundary must withdraw it when it keeps the capture itself;
// a leftover entry stops the epoch-transition fallback from writing mark[743]
// if the boundary's own capture does not persist.
func TestHardForkBoundaryDiscardsDeferredSnapshotCapture(t *testing.T) {
	t.Parallel()

	f := newHardForkRatifyFixture(t)
	ratifyResult := f.rollover(t, f.ls.currentEpoch, f.pparams)
	require.Equal(t, uint64(742), ratifyResult.NewCurrentEpoch.EpochId)

	noop := func(*database.Txn, event.EpochTransitionEvent) error { return nil }
	f.ls.SetEpochBoundarySnapshotStakeHook(noop)
	f.ls.SetEpochBoundarySnapshotHook(noop)
	f.ls.SetCurrentBoundarySPOStakeHook(
		func(
			*database.Txn, event.EpochTransitionEvent,
		) ([]*models.PoolStakeSnapshot, error) {
			return nil, nil
		},
	)
	var captured, discarded []uint64
	f.ls.SetDeferredEpochBoundarySnapshotHooks(
		func(_ *database.Txn, evt event.EpochTransitionEvent) error {
			captured = append(captured, evt.NewEpoch)
			return nil
		},
		func(epoch uint64) { discarded = append(discarded, epoch) },
		func(
			*database.Txn, event.EpochTransitionEvent,
		) (DeferredBoundarySnapshot, error) {
			return nil, errors.New("hard-fork boundary deferred its snapshot")
		},
	)

	enactResult := f.rollover(t, ratifyResult.NewCurrentEpoch, f.pparams)
	require.Equal(t, uint64(743), enactResult.NewCurrentEpoch.EpochId)
	require.Equal(t, []uint64{743}, captured,
		"the SNAP point must register the capture before the hard fork is known")
	require.Equal(t, []uint64{743}, discarded)
}

// TestHealEmptyLabNoncesRepairsAndRecomputes verifies that healEmptyLabNonces
// restores an epoch's empty LastEpochBlockNonce from its boundary block's
// PrevHash and recomputes that epoch's nonce from the PREVIOUS epoch's carried
// lab (η(E) = candidate(E) ⭒ lab(E-1), the cardano-ledger assembly — NOT the
// epoch's own lab, which would shift eta by one epoch). A pre-fix
// BlockBeforeSlot endorser-block collision could persist an empty lab,
// collapsing the next epoch's nonce to the NeutralNonce identity
// (η == candidateNonce) and failing every leader-VRF check in that epoch (the
// Dijkstra/Leios at-tip wedge).
func TestHealEmptyLabNoncesRepairsAndRecomputes(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	// The last block of the epoch preceding epoch 5 (slot < 200). Its PrevHash
	// is the lab value epoch 5 must recover to.
	boundaryHash := bytes.Repeat([]byte{0x01}, 32)
	boundaryPrevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     150,
		Hash:     boundaryHash,
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     6,
	}, nil))

	candidate := bytes.Repeat([]byte{0xaa}, 32)
	// Epoch 4 is covered by the Mithril trust boundary: its imported carried
	// lab is trusted verbatim and feeds epoch 5's nonce.
	carriedLab := bytes.Repeat([]byte{0xfa}, 32)
	ls := &LedgerState{
		db:                db,
		mithrilLedgerSlot: 150,
		epochCache: []models.Epoch{
			{
				EpochId:             4,
				StartSlot:           100,
				LengthInSlots:       100,
				Nonce:               bytes.Repeat([]byte{0xee}, 32),
				CandidateNonce:      bytes.Repeat([]byte{0xed}, 32),
				LastEpochBlockNonce: carriedLab,
			},
			{
				EpochId:        5,
				StartSlot:      200,
				LengthInSlots:  100,
				CandidateNonce: candidate,
				// NeutralNonce-collapsed (wrong) nonce: η == candidateNonce.
				Nonce:               append([]byte(nil), candidate...),
				LastEpochBlockNonce: nil, // corrupted: empty lab
			},
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.healEmptyLabNonces()

	// Epoch 5's lab is recovered from the boundary block's PrevHash.
	require.Equal(
		t,
		boundaryPrevHash,
		ls.epochCache[1].LastEpochBlockNonce,
		"empty lab must be restored from the boundary block's PrevHash",
	)

	// Epoch 5's nonce is recomputed as candidateNonce ⭒ epoch 4's carried lab,
	// no longer the NeutralNonce-collapsed value.
	want, err := lcommon.CalculateEpochNonce(candidate, carriedLab, nil)
	require.NoError(t, err)
	require.Equal(t, want.Bytes(), ls.epochCache[1].Nonce)
	require.NotEqual(
		t,
		candidate,
		ls.epochCache[1].Nonce,
		"epoch nonce must no longer be the NeutralNonce-collapsed candidate",
	)
	// The one-epoch-shifted assembly (candidate ⭒ epoch 5's OWN lab) must NOT
	// be produced — that is the #2734 divergence.
	shifted, err := lcommon.CalculateEpochNonce(
		candidate,
		boundaryPrevHash,
		nil,
	)
	require.NoError(t, err)
	require.NotEqual(
		t,
		shifted.Bytes(),
		ls.epochCache[1].Nonce,
		"epoch nonce must not mix the candidate with the epoch's own lab",
	)
}

// TestHealEmptyLabNoncesBoundsToRecentEpochs verifies the repair only touches
// the recent window: repairing every historical epoch on each restart is one
// block lookup per epoch and needlessly slow, and older labs never feed a
// runtime nonce, so an epoch older than the window must be left untouched even
// when it has a repairable (empty) lab.
func TestHealEmptyLabNoncesBoundsToRecentEpochs(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	// A single boundary block precedes every epoch's start slot, so any epoch
	// that is actually processed repairs its empty lab to this PrevHash.
	boundaryPrevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       1,
		Slot:     50,
		Hash:     bytes.Repeat([]byte{0x01}, 32),
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   1,
		Type:     6,
	}, nil))

	const n = healLabNonceRecentEpochs + 3
	epochs := make([]models.Epoch, n)
	for i := range epochs {
		epochs[i] = models.Epoch{
			EpochId:        uint64(i + 1),
			StartSlot:      uint64((i + 1) * 100),
			LengthInSlots:  100,
			Nonce:          bytes.Repeat([]byte{0xee}, 32),
			CandidateNonce: bytes.Repeat([]byte{0xaa}, 32),
			// LastEpochBlockNonce left empty (repairable)
		}
	}
	ls := &LedgerState{
		db:         db,
		epochCache: epochs,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.healEmptyLabNonces()

	require.Empty(
		t,
		ls.epochCache[0].LastEpochBlockNonce,
		"epoch older than the recent window must be left untouched, not repaired",
	)
	require.Equal(t, boundaryPrevHash, ls.epochCache[n-1].LastEpochBlockNonce,
		"the most recent epoch must still be repaired")
}

// TestHealEmptyLabNoncesRepairsOldestInWindowNonce verifies the bounded scan
// includes one predecessor epoch so the oldest in-window epoch's nonce — which
// mixes the PREVIOUS epoch's lab — is repaired from a verified predecessor lab
// rather than left stale. Without the predecessor the first in-window nonce
// check has no verified previous lab and is skipped.
func TestHealEmptyLabNoncesRepairsOldestInWindowNonce(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	prevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       1,
		Slot:     50,
		Hash:     bytes.Repeat([]byte{0x01}, 32),
		PrevHash: prevHash,
		Cbor:     []byte{0x80},
		Number:   1,
		Type:     6,
	}, nil))

	candidate := bytes.Repeat([]byte{0xaa}, 32)
	const n = healLabNonceRecentEpochs + 3
	epochs := make([]models.Epoch, n)
	for i := range epochs {
		epochs[i] = models.Epoch{
			EpochId:        uint64(i + 1),
			StartSlot:      uint64((i + 1) * 100),
			LengthInSlots:  100,
			CandidateNonce: candidate,
			// NeutralNonce-collapsed (wrong) nonce and empty lab, both repairable.
			Nonce: append([]byte(nil), candidate...),
		}
	}
	ls := &LedgerState{
		db:         db,
		epochCache: epochs,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.healEmptyLabNonces()

	// The oldest in-window epoch is scanned right after the predecessor whose
	// lab the scan verifies. Its nonce must be recomputed as candidate ⭒ the
	// predecessor's repaired lab (prevHash), not left as the collapsed candidate.
	oldest := n - healLabNonceRecentEpochs
	want, err := lcommon.CalculateEpochNonce(candidate, prevHash, nil)
	require.NoError(t, err)
	require.Equal(
		t,
		want.Bytes(),
		ls.epochCache[oldest].Nonce,
		"oldest in-window epoch nonce must be repaired from the verified predecessor lab",
	)
	require.NotEqual(
		t,
		candidate,
		ls.epochCache[oldest].Nonce,
		"oldest in-window epoch nonce must no longer be the collapsed candidate",
	)
}

// TestHealEmptyLabNoncesLeavesValidRecordsUntouched verifies the recovery is a
// no-op when no epoch has a repairable lab mismatch — it must not perturb
// correct state.
func TestHealEmptyLabNoncesLeavesValidRecordsUntouched(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	lab := bytes.Repeat([]byte{0xcc}, 32)
	nonce := bytes.Repeat([]byte{0xdd}, 32)
	ls := &LedgerState{
		db: db,
		epochCache: []models.Epoch{
			{
				EpochId:             6,
				StartSlot:           300,
				LengthInSlots:       100,
				LastEpochBlockNonce: lab,
				Nonce:               nonce,
				CandidateNonce:      bytes.Repeat([]byte{0xaa}, 32),
			},
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.healEmptyLabNonces()

	require.Equal(t, lab, ls.epochCache[0].LastEpochBlockNonce)
	require.Equal(t, nonce, ls.epochCache[0].Nonce)
}

func TestHealEmptyLabNoncesLeavesParentHashLabUntouched(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	boundaryHash := bytes.Repeat([]byte{0x01}, 32)
	boundaryPrevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     250,
		Hash:     boundaryHash,
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     6,
	}, nil))

	candidate := bytes.Repeat([]byte{0xaa}, 32)
	nonce, err := lcommon.CalculateEpochNonce(candidate, boundaryPrevHash, nil)
	require.NoError(t, err)
	epochs := []models.Epoch{
		{
			EpochId:             6,
			StartSlot:           300,
			LengthInSlots:       100,
			Nonce:               nonce.Bytes(),
			CandidateNonce:      candidate,
			LastEpochBlockNonce: boundaryPrevHash,
		},
	}
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	repaired := ls.healEmptyLabNoncesInPlace(epochs)

	require.False(t, repaired)
	require.Equal(t, boundaryPrevHash, epochs[0].LastEpochBlockNonce)
	require.NotEqual(t, boundaryHash, epochs[0].LastEpochBlockNonce)
	require.Equal(t, nonce.Bytes(), epochs[0].Nonce)
}

// TestHealEmptyLabNoncesRepairsLabWhenCandidateMissing verifies that the lab
// repair does not depend on a stored candidate nonce: the lab feeds the NEXT
// boundary's eta directly, so leaving it in a stale shape just because this
// epoch's nonce cannot be re-verified would wedge the next rollover. The
// nonce itself must be left untouched (no candidate to recompute it from).
func TestHealEmptyLabNoncesRepairsLabWhenCandidateMissing(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	boundaryHash := bytes.Repeat([]byte{0x01}, 32)
	boundaryPrevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     250,
		Hash:     boundaryHash,
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     6,
	}, nil))

	cfg := newConwayBootstrapStabilityCfg(t)
	oldLab := bytes.Repeat([]byte{0x99}, 32)
	oldNonce := bytes.Repeat([]byte{0xaa}, 32)
	epochs := []models.Epoch{
		{
			EpochId:             6,
			StartSlot:           300,
			LengthInSlots:       100,
			Nonce:               oldNonce,
			CandidateNonce:      nil,
			LastEpochBlockNonce: oldLab,
		},
	}
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	repaired := ls.healEmptyLabNoncesInPlace(epochs)

	require.True(t, repaired)
	require.Equal(t, boundaryPrevHash, epochs[0].LastEpochBlockNonce,
		"stale lab must be repaired even without a stored candidate")
	require.Equal(t, oldNonce, epochs[0].Nonce,
		"nonce must be untouched when no candidate is stored")
	require.Empty(t, epochs[0].CandidateNonce)
}

// TestHealEmptyLabNoncesRepairsEmptyLabWithoutCandidate mirrors the test above
// for an empty (rather than stale) lab.
func TestHealEmptyLabNoncesRepairsEmptyLabWithoutCandidate(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	boundaryHash := bytes.Repeat([]byte{0x01}, 32)
	boundaryPrevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     250,
		Hash:     boundaryHash,
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     6,
	}, nil))

	oldNonce := bytes.Repeat([]byte{0xaa}, 32)
	epochs := []models.Epoch{
		{
			EpochId:             6,
			StartSlot:           300,
			LengthInSlots:       100,
			Nonce:               oldNonce,
			CandidateNonce:      nil,
			LastEpochBlockNonce: nil,
		},
	}
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	repaired := ls.healEmptyLabNoncesInPlace(epochs)

	require.True(t, repaired)
	require.Equal(t, boundaryPrevHash, epochs[0].LastEpochBlockNonce,
		"empty lab must be repaired even without a stored candidate")
	require.Equal(t, oldNonce, epochs[0].Nonce,
		"nonce must be untouched when no candidate is stored")
}

func TestHealEmptyLabNoncesSkipsMissingCandidateBeforeBoundaryLookup(
	t *testing.T,
) {
	t.Parallel()

	oldLab := bytes.Repeat([]byte{0x99}, 32)
	oldNonce := bytes.Repeat([]byte{0xaa}, 32)
	epochs := []models.Epoch{
		{
			EpochId:             6,
			StartSlot:           300,
			LengthInSlots:       100,
			Nonce:               oldNonce,
			CandidateNonce:      nil,
			LastEpochBlockNonce: oldLab,
		},
	}
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	require.NotPanics(t, func() {
		repaired := ls.healEmptyLabNoncesInPlace(epochs)
		require.False(t, repaired)
	})
	require.Equal(t, oldLab, epochs[0].LastEpochBlockNonce)
	require.Equal(t, oldNonce, epochs[0].Nonce)
}

// TestHealEmptyLabNoncesLeavesFirstPraosEpochLabNeutral verifies the heal does
// not "repair" the first Praos epoch's nil lab to the last pre-Praos (Byron)
// block's parent hash. cardano-ledger initializes praosStateLastEpochBlockNonce
// to NeutralNonce at the Praos start (initialChainDepState csLabNonce =
// NeutralNonce), and the initial-epoch branch of calculateEpochNonce stores a
// nil lab — rewriting it would diverge the next boundary's eta on any chain
// with a Byron era (mainnet, preprod).
func TestHealEmptyLabNoncesLeavesFirstPraosEpochLabNeutral(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	// Last Byron block before the Shelley start at slot 200. Without the
	// first-Praos guard, its PrevHash would be written as the lab.
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     150,
		Hash:     bytes.Repeat([]byte{0x01}, 32),
		PrevHash: bytes.Repeat([]byte{0xbb}, 32),
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     1, // Byron main
	}, nil))

	genesisNonce := bytes.Repeat([]byte{0x11}, 32)
	epochs := []models.Epoch{
		{
			// Pre-Praos (Byron) epoch: no nonce, no candidate, no lab.
			EpochId:       3,
			StartSlot:     100,
			LengthInSlots: 100,
		},
		{
			// First Praos epoch: nonce/candidate are the genesis nonce, lab is
			// Neutral (nil).
			EpochId:        4,
			StartSlot:      200,
			LengthInSlots:  100,
			Nonce:          append([]byte(nil), genesisNonce...),
			CandidateNonce: append([]byte(nil), genesisNonce...),
		},
	}
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	repaired := ls.healEmptyLabNoncesInPlace(epochs)

	require.False(t, repaired)
	require.Empty(t, epochs[0].LastEpochBlockNonce)
	require.Empty(t, epochs[1].LastEpochBlockNonce,
		"first Praos epoch's lab must stay NeutralNonce (nil)")
	require.Equal(t, genesisNonce, epochs[1].Nonce,
		"first Praos epoch's nonce must stay the genesis nonce")
}

func TestHealEmptyLabNoncesTrustsMithrilCoveredEpoch(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	boundaryHash := bytes.Repeat([]byte{0x01}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     250,
		Hash:     boundaryHash,
		PrevHash: bytes.Repeat([]byte{0xbb}, 32),
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     6,
	}, nil))

	candidate := bytes.Repeat([]byte{0xaa}, 32)
	importedLab := bytes.Repeat([]byte{0x99}, 32)
	importedNonce := bytes.Repeat([]byte{0xdd}, 32)
	epochs := []models.Epoch{
		{
			EpochId:             6,
			StartSlot:           300,
			LengthInSlots:       100,
			Nonce:               importedNonce,
			CandidateNonce:      candidate,
			LastEpochBlockNonce: importedLab,
		},
	}
	ls := &LedgerState{
		db:                db,
		mithrilLedgerSlot: 350,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	repaired := ls.healEmptyLabNoncesInPlace(epochs)

	require.False(t, repaired)
	require.Equal(t, importedLab, epochs[0].LastEpochBlockNonce)
	require.Equal(t, importedNonce, epochs[0].Nonce)
	require.NotEqual(t, boundaryHash, epochs[0].LastEpochBlockNonce)
}

func TestHealEmptyLabNoncesInPlaceRepairsReloadedEpochs(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	boundaryHash := bytes.Repeat([]byte{0x01}, 32)
	boundaryPrevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     250,
		Hash:     boundaryHash,
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     6,
	}, nil))

	candidate := bytes.Repeat([]byte{0xaa}, 32)
	// Epoch 5 is Mithril-trusted; its carried lab feeds epoch 6's nonce.
	carriedLab := bytes.Repeat([]byte{0xfa}, 32)
	epochs := []models.Epoch{
		{
			EpochId:             5,
			StartSlot:           200,
			LengthInSlots:       100,
			Nonce:               bytes.Repeat([]byte{0xee}, 32),
			CandidateNonce:      bytes.Repeat([]byte{0xed}, 32),
			LastEpochBlockNonce: carriedLab,
		},
		{
			EpochId:             6,
			StartSlot:           300,
			LengthInSlots:       100,
			Nonce:               append([]byte(nil), candidate...),
			CandidateNonce:      candidate,
			LastEpochBlockNonce: nil,
		},
	}
	ls := &LedgerState{
		db:                db,
		mithrilLedgerSlot: 250,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	repaired := ls.healEmptyLabNoncesInPlace(epochs)

	want, err := lcommon.CalculateEpochNonce(candidate, carriedLab, nil)
	require.NoError(t, err)
	require.True(t, repaired)
	require.Equal(t, boundaryPrevHash, epochs[1].LastEpochBlockNonce)
	require.Equal(t, want.Bytes(), epochs[1].Nonce)
}

func TestLoadEpochsRefreshesCurrentEpochAfterHealing(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	// Last block of epoch 4 (before slot 200): its PrevHash is epoch 5's lab.
	epoch5BoundaryPrevHash := bytes.Repeat([]byte{0x44}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       2,
		Slot:     150,
		Hash:     bytes.Repeat([]byte{0x02}, 32),
		PrevHash: epoch5BoundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   2,
		Type:     6,
	}, nil))
	// Last block of epoch 5 (before slot 300): its PrevHash is epoch 6's lab.
	boundaryHash := bytes.Repeat([]byte{0x01}, 32)
	boundaryPrevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     250,
		Hash:     boundaryHash,
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     6,
	}, nil))

	candidate := bytes.Repeat([]byte{0xaa}, 32)
	require.NoError(t, db.SetEpoch(
		200,
		5,
		bytes.Repeat([]byte{0x55}, 32),
		nil,
		nil,
		nil,
		eras.ShelleyEraDesc.Id,
		1,
		100,
		nil,
	))
	require.NoError(t, db.SetEpoch(
		300,
		6,
		append([]byte(nil), candidate...),
		nil,
		candidate,
		nil,
		eras.ShelleyEraDesc.Id,
		1,
		100,
		nil,
	))

	// Epoch 6's nonce mixes its candidate with epoch 5's carried lab (repaired
	// to the epoch-5 boundary block's PrevHash), not with epoch 6's own lab.
	want, err := lcommon.CalculateEpochNonce(
		candidate,
		epoch5BoundaryPrevHash,
		nil,
	)
	require.NoError(t, err)

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	txn := db.Transaction(true)
	defer txn.Release()
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.loadEpochs(txn)
	}))

	require.Equal(t, want.Bytes(), ls.epochCache[1].Nonce)
	require.Equal(t, want.Bytes(), ls.currentEpoch.Nonce)
	require.Equal(t, want.Bytes(), ls.EpochNonce(6))
	require.NotEqual(t, candidate, ls.EpochNonce(6))
}

// rollbackWindowFixture reproduces the state a node holds while a rollback's
// metadata truncation is still in flight.
//
// rollbackChainAndStateDeferred rewinds ls.chain (which physically removes the
// rolled-away block rows) before calling ls.rollback, and ls.rollback only
// assigns ls.currentTip once TruncateAfterSlot has committed. On a large
// metadata database that truncation takes tens of seconds, and for that whole
// window the chain tip is the rollback point while ls.currentTip still names
// the block the chain rollback already deleted.
type rollbackWindowFixture struct {
	ls          *LedgerState
	blocks      []models.Block
	rollbackTo  models.Block
	staleLedger ochainsync.Tip
}

func newRollbackWindowFixture(t *testing.T) *rollbackWindowFixture {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	blocks := make([]models.Block, 0, 5)
	for slot := uint64(1); slot <= 5; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 5}))

	tipBlock := blocks[len(blocks)-1]
	ledgerTip := ochainsync.Tip{
		Point:       makeTestPoint(tipBlock),
		BlockNumber: tipBlock.Number,
	}
	require.NoError(t, db.SetTip(ledgerTip, nil))

	ls := &LedgerState{
		db:    db,
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.currentTip = ledgerTip

	// Rewind the chain exactly as rollbackChainAndStateDeferred does, and
	// deliberately do NOT run ls.rollback: this is the in-flight-truncation
	// window.
	rollbackTo := blocks[2]
	require.NoError(t, ls.chain.Rollback(makeTestPoint(rollbackTo)))

	return &rollbackWindowFixture{
		ls:          ls,
		blocks:      blocks,
		rollbackTo:  rollbackTo,
		staleLedger: ledgerTip,
	}
}

// TestRollbackWindowPreconditions pins the exact state the bug depends on, so
// a future change that makes the window unreachable fails here loudly rather
// than silently turning the regression tests below into tautologies.
func TestRollbackWindowPreconditions(t *testing.T) {
	f := newRollbackWindowFixture(t)

	// The chain has been rewound to the rollback point...
	chainTip := f.ls.chain.Tip()
	require.Equal(t, f.rollbackTo.Slot, chainTip.Point.Slot)

	// ...but the ledger tip still names the block that rewind deleted.
	f.ls.RLock()
	ledgerTip := f.ls.currentTip
	f.ls.RUnlock()
	require.Equal(t, f.staleLedger.Point.Slot, ledgerTip.Point.Slot)
	require.Greater(t, ledgerTip.Point.Slot, chainTip.Point.Slot)

	// The ledger tip's block row is gone, which is what makes
	// authoritativeRecentChainPoints bail out.
	_, err := database.BlockByPoint(f.ls.db, ledgerTip.Point)
	require.ErrorIs(t, err, models.ErrBlockNotFound)

	// And the "chain is usable" guard is false, so IntersectPoints takes the
	// authoritative path rather than the chain path.
	require.False(t, f.ls.primaryChainTipAtOrAheadOfLedgerTip())
}

// TestAuthoritativeRecentChainPointsFallsBackToChainTipWhenLedgerTipMissing
// asserts the ledger never reports "I have no chain points" while it demonstrably
// holds a chain. Before the fix this returned an empty slice with a nil error,
// which buildDefaultChainsyncIntersectPoints turned into an origin-only
// MsgFindIntersect.
func TestAuthoritativeRecentChainPointsFallsBackToChainTipWhenLedgerTipMissing(
	t *testing.T,
) {
	f := newRollbackWindowFixture(t)

	points, err := f.ls.authoritativeRecentChainPoints(4)
	require.NoError(t, err)
	require.NotEmpty(
		t,
		points,
		"ledger reported no chain points while holding a chain tip at slot %d",
		f.ls.chain.Tip().Point.Slot,
	)

	// The newest point offered must be the rollback point (the chain tip),
	// not the deleted ledger tip.
	assert.Equal(t, f.rollbackTo.Slot, points[0].Slot)
	assert.Equal(t, f.rollbackTo.Hash, points[0].Hash)

	// No point may name a block that no longer exists.
	for _, point := range points {
		assert.LessOrEqual(
			t,
			point.Slot,
			f.rollbackTo.Slot,
			"offered a point above the rollback point",
		)
	}

	// Recent points below the tip must still be walked, so a peer that has
	// also rewound can intersect deeper than the tip.
	require.Greater(t, len(points), 1, "expected recent points below the tip")
	assert.Equal(t, f.blocks[1].Slot, points[1].Slot)
}

// TestIntersectPointsDoesNotCollapseToEmptyDuringRollbackWindow is the
// end-to-end ledger-level assertion: the entry point chainsync actually calls
// must not return an empty set in this window.
func TestIntersectPointsDoesNotCollapseToEmptyDuringRollbackWindow(
	t *testing.T,
) {
	f := newRollbackWindowFixture(t)

	points, err := f.ls.IntersectPoints(4)
	require.NoError(t, err)
	require.NotEmpty(
		t,
		points,
		"IntersectPoints returned nothing during an in-flight rollback; "+
			"chainsync would offer origin only",
	)
	assert.Equal(t, f.rollbackTo.Slot, points[0].Slot)
	assert.Equal(t, f.rollbackTo.Hash, points[0].Hash)
}

// TestIntersectPointsStillEmptyAtOriginWithNoChain guards the fallback from
// over-reaching: a node that genuinely has no chain must still report no
// points, so a fresh node syncs from origin as designed.
func TestIntersectPointsStillEmptyAtOriginWithNoChain(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 5}))
	ls := &LedgerState{
		db:    db,
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	points, err := ls.IntersectPoints(4)
	require.NoError(t, err)
	assert.Empty(t, points)
}

// TestAuthoritativeRecentChainPointsIgnoresChainTipAheadOfLedgerTip pins the
// boundary of the fallback introduced for the rollback window. A chain tip at
// or ahead of the ledger tip is unapplied forward work -- possibly a fork that
// does not descend from the ledger tip at all -- and must NOT be offered as an
// intersect point, which is the invariant the primary-chain ancestor check
// (#2309) exists to protect. Only a chain tip strictly below the ledger tip,
// the signature of an in-flight rewind, qualifies.
func TestAuthoritativeRecentChainPointsIgnoresChainTipAheadOfLedgerTip(
	t *testing.T,
) {
	f := newRollbackWindowFixture(t)

	// Extend the rewound chain past the (missing) ledger tip with a fork
	// block, so the chain tip is now ahead of the ledger tip.
	forkHash := bytes.Repeat([]byte{0xfe}, 32)
	require.NoError(t, f.ls.chain.AddRawBlocks([]chain.RawBlock{
		{
			Slot:        f.staleLedger.Point.Slot + 5,
			Hash:        forkHash,
			BlockNumber: f.staleLedger.BlockNumber + 1,
			Type:        1,
			PrevHash:    f.rollbackTo.Hash,
			Cbor:        []byte{0x80},
		},
	}))
	require.Greater(
		t,
		f.ls.chain.Tip().Point.Slot,
		f.staleLedger.Point.Slot,
	)

	points, err := f.ls.authoritativeRecentChainPoints(4)
	require.NoError(t, err)
	assert.Empty(
		t,
		points,
		"must not offer unapplied forward chain state as intersect points",
	)
}

// countingWarnHandler counts Warn records carrying a given message.
type countingWarnHandler struct {
	slog.Handler
	message string
	count   *atomic.Int64
}

func (h countingWarnHandler) Handle(
	ctx context.Context,
	record slog.Record,
) error {
	if record.Level == slog.LevelWarn && record.Message == h.message {
		h.count.Add(1)
	}
	return nil
}

func (h countingWarnHandler) Enabled(
	_ context.Context,
	level slog.Level,
) bool {
	return level >= slog.LevelWarn
}

// TestRecentChainPointsFallbackAnchorPropagatesStorageError verifies a real
// storage failure is surfaced rather than silently degraded into "no anchor".
// Swallowing it would turn a transient database fault into an origin-only
// intersect list, i.e. a request that the peer replay the chain from genesis.
func TestRecentChainPointsFallbackAnchorPropagatesStorageError(t *testing.T) {
	f := newRollbackWindowFixture(t)

	// Sanity: the anchor resolves while the database is healthy.
	block, ok, err := f.ls.recentChainPointsFallbackAnchor(f.staleLedger)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, f.rollbackTo.Slot, block.Slot)

	require.NoError(t, dbtest.CloseDatabase(f.ls.db))

	_, ok, err = f.ls.recentChainPointsFallbackAnchor(f.staleLedger)
	require.Error(t, err, "storage failure must not be swallowed")
	assert.False(t, ok)
	assert.NotErrorIs(
		t,
		err,
		models.ErrBlockNotFound,
		"a real storage fault must not be reported as a missing block",
	)
}

// TestAuthoritativeRecentChainPointsPropagatesAnchorStorageError verifies the
// propagated error reaches the caller instead of becoming an empty point list.
func TestAuthoritativeRecentChainPointsPropagatesAnchorStorageError(
	t *testing.T,
) {
	f := newRollbackWindowFixture(t)
	require.NoError(t, dbtest.CloseDatabase(f.ls.db))

	points, err := f.ls.authoritativeRecentChainPoints(4)
	require.Error(t, err)
	assert.Nil(t, points)
}

// TestIntersectAnchorFallbackWarnIsThrottled verifies the anchor-fallback
// warning does not flood. authoritativeRecentChainPoints runs on every
// chainsync client start, and during the truncation window peer governance
// reconnects roughly once a second across every peer, so an unthrottled
// warning would emit hundreds of lines for a single rollback.
func TestIntersectAnchorFallbackWarnIsThrottled(t *testing.T) {
	f := newRollbackWindowFixture(t)

	var warns atomic.Int64
	f.ls.config.Logger = slog.New(countingWarnHandler{
		Handler: slog.NewJSONHandler(io.Discard, nil),
		message: "ledger tip block missing, anchoring intersect points on primary chain tip",
		count:   &warns,
	})

	for range 50 {
		points, err := f.ls.authoritativeRecentChainPoints(4)
		require.NoError(t, err)
		require.NotEmpty(t, points)
	}

	assert.Equal(
		t,
		int64(1),
		warns.Load(),
		"anchor-fallback warning must be throttled, not emitted per call",
	)
}

// TestRollbackWindowIntersectAnchorReportsRollbackPoint verifies the exported
// anchor, which chainsync relies on, names the rollback point during the
// window.
func TestRollbackWindowIntersectAnchorReportsRollbackPoint(t *testing.T) {
	f := newRollbackWindowFixture(t)

	point, ok, err := f.ls.RollbackWindowIntersectAnchor()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, f.rollbackTo.Slot, point.Slot)
	assert.Equal(t, f.rollbackTo.Hash, point.Hash)
}

// TestRollbackWindowIntersectAnchorAbsentWhenLedgerTipPresent verifies the
// anchor is offered only inside the window: a self-consistent ledger whose tip
// row is readable needs no rescue.
func TestRollbackWindowIntersectAnchorAbsentWhenLedgerTipPresent(t *testing.T) {
	f := newRollbackWindowFixture(t)

	// Move the ledger tip onto a block that still exists.
	f.ls.Lock()
	f.ls.currentTip = ochainsync.Tip{
		Point:       makeTestPoint(f.rollbackTo),
		BlockNumber: f.rollbackTo.Number,
	}
	f.ls.Unlock()

	_, ok, err := f.ls.RollbackWindowIntersectAnchor()
	require.NoError(t, err)
	assert.False(t, ok)
}

// announcingMockHeader is a ranking-block header that carries a Leios
// endorser-block announcement, as a Dijkstra-era header does.
type announcingMockHeader struct {
	mockHeader
	ebHash lcommon.Blake2b256
	ebSize uint64
}

func (m announcingMockHeader) LeiosAnnouncement() (
	lcommon.Blake2b256,
	uint64,
	bool,
) {
	return m.ebHash, m.ebSize, true
}

// headerStreamFixture is a LedgerState whose chain publishes onto a real event
// bus, so tests can observe the ordered chain.header stream the Leios vote
// manager consumes.
type headerStreamFixture struct {
	ls     *LedgerState
	bus    *event.EventBus
	connId ouroboros.ConnectionId
	ch     <-chan event.Event
}

func newHeaderStreamLedger(t *testing.T) *headerStreamFixture {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	cm, err := chain.NewManager(nil, bus)
	require.NoError(t, err)
	subId, ch := bus.Subscribe(chain.ChainHeaderEventType)
	t.Cleanup(func() { bus.Unsubscribe(chain.ChainHeaderEventType, subId) })
	ls := &LedgerState{
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			EventBus: bus,
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	return &headerStreamFixture{
		ls:  ls,
		bus: bus,
		connId: ouroboros.ConnectionId{
			LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
			RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
		},
		ch: ch,
	}
}

func announcingHeader(
	slot uint64,
	name string,
	prevHash lcommon.Blake2b256,
	blockNumber uint64,
	ebHash lcommon.Blake2b256,
) announcingMockHeader {
	return announcingMockHeader{
		mockHeader: mockHeader{
			hash:        lcommon.NewBlake2b256([]byte(name)),
			prevHash:    prevHash,
			blockNumber: blockNumber,
			slot:        slot,
		},
		ebHash: ebHash,
		ebSize: 4096,
	}
}

// TestChainsyncHeaderAdmissionAnnouncesOnlyWhenCryptoVerified pins the ledger
// half of the crypto gate on the header stream.
//
// chainsyncHeaderCryptoPolicy admits a roll-forward header without verifying
// its VRF/KES on two paths: a slot covered by an imported Mithril snapshot and
// no cached epoch nonce for the slot (verification deferred to blockfetch).
// Both reach
// chain.AddBlockHeader, not AddVerifiedBlockHeader. Announcing such a header
// would let a chainsync peer make this node sign and publish a BLS vote for a
// ranking block it never authenticated, taking the (slot, voterId) pair the
// honest block's own vote needs.
func TestChainsyncHeaderAdmissionAnnouncesOnlyWhenCryptoVerified(
	t *testing.T,
) {
	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	header := announcingHeader(
		577, "hdr-1", lcommon.NewBlake2b256(nil), 1, ebHash,
	)
	point := ocommon.NewPoint(header.slot, header.hash.Bytes())

	// The fixture has no cached epoch nonce, so the policy defers verification
	// and the handler takes the AddBlockHeader branch -- the real end-to-end
	// unverified admission.
	t.Run("unverified admission is queued, not announced", func(t *testing.T) {
		fixture := newHeaderStreamLedger(t)
		verifyNow, trusted := fixture.ls.chainsyncHeaderCryptoPolicy(
			header.slot,
		)
		require.False(t, verifyNow, "fixture must exercise the unverified path")
		require.False(t, trusted)

		require.NoError(
			t,
			fixture.ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
				ConnectionId: fixture.connId,
				BlockHeader:  header,
				Point:        point,
				Tip: ochainsync.Tip{
					Point:       ocommon.NewPoint(60001, []byte("tip-1")),
					BlockNumber: 60001,
				},
			}),
		)
		require.Equal(t, 1, fixture.ls.chain.HeaderCount())

		testutil.RequireNoReceive(
			t,
			fixture.ch,
			500*time.Millisecond,
			"an unverified header must not arm a vote",
		)
	})

	// The verified branch of the same handler is one call:
	// ls.chain.AddVerifiedBlockHeader(e.BlockHeader). It is driven directly
	// here because a header that both announces a Leios endorser block and
	// passes real VRF/KES cannot be synthesized in this suite: gouroboros
	// VerifyBlock dispatches on the concrete era header type, so wrapping a
	// valid Babbage header to add LeiosAnnouncement fails verification with
	// "unsupported block type for VRF verification". The gate itself is
	// covered from both sides in chain:
	// TestHeaderAnnouncementRequiresCryptoVerifiedHeader.
	t.Run("verified admission announces", func(t *testing.T) {
		fixture := newHeaderStreamLedger(t)
		require.NoError(t, fixture.ls.chain.AddVerifiedBlockHeader(header))
		fixture.ls.chain.PublishPendingChainUpdates()

		evt := testutil.RequireReceive(
			t,
			fixture.ch,
			testutil.AsyncWait,
			"announcement published from verified header admission",
		)
		data, ok := evt.Data.(chain.ChainHeaderAnnouncementEvent)
		require.True(t, ok)
		assert.Equal(t, uint64(577), data.Slot)
		assert.Equal(t, header.hash, data.RbHash)
		assert.Equal(t, ebHash, data.EbHash)
		assert.NotZero(t, data.Seq)
	})
}

// TestChainsyncHeaderQueueClearedInvalidatesAnnouncement covers the case where
// header admission succeeds but blockfetch startup then fails: the queue is
// discarded and no rollback is published, because no block was ever added.
// Without the invalidation on the same stream, the announcement would outlive
// the header and the vote manager could vote for a ranking block that is not
// on our chain.
//
// The announcing header is admitted verified (the only kind that announces);
// the header that drives the handler into its failing blockfetch start chains
// onto it and announces nothing of its own, so the single announcement under
// test is unambiguous.
func TestChainsyncHeaderQueueClearedInvalidatesAnnouncement(t *testing.T) {
	fixture := newHeaderStreamLedger(t)
	// No BlockfetchRequestRangeFunc is wired, so every blockfetch start
	// attempt fails and the handler exhausts its fallbacks.
	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	announcing := announcingHeader(
		577, "hdr-1", lcommon.NewBlake2b256(nil), 1, ebHash,
	)
	require.NoError(t, fixture.ls.chain.AddVerifiedBlockHeader(announcing))

	follower := mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-2")),
		prevHash:    announcing.hash,
		blockNumber: 2,
		slot:        578,
	}
	point := ocommon.NewPoint(follower.slot, follower.hash.Bytes())

	require.NoError(
		t,
		fixture.ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
			ConnectionId: fixture.connId,
			BlockHeader:  follower,
			Point:        point,
			// Tip equal to the header keeps the handler out of the
			// header-accumulation branches so it reaches blockfetch.
			Tip: ochainsync.Tip{Point: point, BlockNumber: 2},
		}),
	)
	assert.Zero(
		t,
		fixture.ls.chain.HeaderCount(),
		"failed blockfetch start discards the queued headers",
	)

	announcement := testutil.RequireReceive(
		t, fixture.ch, testutil.AsyncWait, "announcement",
	)
	announced, ok := announcement.Data.(chain.ChainHeaderAnnouncementEvent)
	require.True(t, ok, "got %T", announcement.Data)
	assert.Equal(t, announcing.hash, announced.RbHash)

	invalidation := testutil.RequireReceive(
		t, fixture.ch, testutil.AsyncWait, "invalidation for the discarded header",
	)
	invalid, ok := invalidation.Data.(chain.ChainHeaderInvalidationEvent)
	require.True(t, ok, "got %T", invalidation.Data)
	assert.Equal(t, chain.HeaderInvalidationQueueCleared, invalid.Reason)
	assert.Contains(t, invalid.RbHashes, announcing.hash)
	assert.Greater(t, invalid.Seq, announced.Seq)
}

// TestForkResolutionAnnouncesOnlyTheVerifiedIncomingHeader covers the second
// way an announcing header reaches the header queue. A header that does not
// fit the current tip is queued by tryResolveFork rather than by the direct
// admission path, and that branch returns before the caller's ordinary
// bookkeeping runs. Emitting from the chain's own header-queue mutation is
// what keeps this path covered.
//
// tryResolveFork re-queues a whole fork path: the header this event delivered,
// plus earlier headers replayed from recorded peer history. Only the delivered
// one carries a crypto verdict, so only it may be admitted verified, and only
// a verified admission announces (see addForkPathHeader). Both halves are
// asserted here at that composition site.
func TestForkResolutionAnnouncesOnlyTheVerifiedIncomingHeader(t *testing.T) {
	for _, tc := range []struct {
		name           string
		cryptoVerified bool
		wantAnnounce   bool
	}{
		{
			name:           "verified incoming header announces",
			cryptoVerified: true,
			wantAnnounce:   true,
		},
		{
			name:           "unverified incoming header does not announce",
			cryptoVerified: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bus := event.NewEventBus(nil, nil)
			t.Cleanup(bus.Stop)
			fixture := newChainsyncRollbackFixtureWithBus(t, bus)
			subId, ch := bus.Subscribe(chain.ChainHeaderEventType)
			defer bus.Unsubscribe(chain.ChainHeaderEventType, subId)
			// Keep the test at header admission; no blockfetch worker is
			// needed.
			fixture.ls.chainsyncBlockfetchReadyChan = make(chan struct{})

			ebHash := lcommon.NewBlake2b256([]byte("fork-announced-eb"))
			header := announcingHeader(
				fixture.currentTip.Point.Slot+10,
				"fork-announcing-header",
				lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
				fixture.ancestorTip.BlockNumber+1,
				ebHash,
			)
			// The header does not fit the current tip; that failure is the
			// condition tryResolveFork exists to handle.
			var notFitErr chain.BlockNotFitChainTipError
			require.ErrorAs(
				t,
				fixture.ls.chain.AddBlockHeader(header),
				&notFitErr,
			)
			advertisedSlot := ^uint64(0)

			resolved, err := fixture.ls.tryResolveFork(
				ChainsyncEvent{
					ConnectionId: fixture.connId,
					Point: ocommon.NewPoint(
						header.slot,
						header.hash.Bytes(),
					),
					BlockHeader: header,
					Tip: ochainsync.Tip{
						Point: ocommon.NewPoint(
							advertisedSlot,
							[]byte("unbound-fork-tip"),
						),
						BlockNumber: advertisedSlot,
					},
				},
				notFitErr,
				nil,
				tc.cryptoVerified,
			)
			require.NoError(t, err)
			require.True(t, resolved)
			// The header was queued through fork resolution, not direct
			// admission.
			require.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
			require.Equal(t, 1, fixture.ls.chain.HeaderCount())
			fixture.ls.chain.PublishPendingChainUpdates()

			// The rollback's invalidation precedes anything the fork
			// resolution queued after it.
			invalidation := testutil.RequireReceive(
				t, ch, testutil.AsyncWait, "rollback invalidation",
			)
			invalid, ok := invalidation.Data.(chain.ChainHeaderInvalidationEvent)
			require.True(t, ok, "got %T", invalidation.Data)
			assert.Equal(t, chain.HeaderInvalidationRollback, invalid.Reason)

			if !tc.wantAnnounce {
				testutil.RequireNoReceive(
					t,
					ch,
					500*time.Millisecond,
					"an unverified fork header must not arm a vote",
				)
				return
			}

			announcement := testutil.RequireReceive(
				t, ch, testutil.AsyncWait, "announcement from fork resolution",
			)
			announced, ok := announcement.Data.(chain.ChainHeaderAnnouncementEvent)
			require.True(t, ok, "got %T", announcement.Data)
			assert.Equal(t, header.hash, announced.RbHash)
			assert.Equal(t, ebHash, announced.EbHash)
			assert.Greater(
				t,
				announced.Seq,
				invalid.Seq,
				"the fork header is admitted after the rollback that made room for it",
			)
			testutil.RequireNoReceive(
				t,
				ch,
				300*time.Millisecond,
				"the incoming fork header must be announced exactly once",
			)
		})
	}
}

// newChainsyncRollbackFixtureWithBus mirrors newChainsyncRollbackFixture but
// gives the chain a real event bus so its deferred header/rollback events can
// be observed.
func newChainsyncRollbackFixtureWithBus(
	t *testing.T,
	bus *event.EventBus,
) *chainsyncRollbackFixture {
	t.Helper()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, bus)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 2}),
	)

	ancestorHash := testHashBytes("ancestor-block")
	currentHash := testHashBytes("current-block")
	ancestorBlock := chain.RawBlock{
		Slot:        10,
		Hash:        ancestorHash,
		BlockNumber: 1,
		Type:        1,
		Cbor:        []byte{0x80},
	}
	currentBlock := chain.RawBlock{
		Slot:        20,
		Hash:        currentHash,
		BlockNumber: 2,
		Type:        1,
		PrevHash:    ancestorHash,
		Cbor:        []byte{0x80},
	}
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks([]chain.RawBlock{
			ancestorBlock,
			currentBlock,
		}),
	)

	ls, err := NewLedgerState(
		LedgerStateConfig{
			Database:          db,
			ChainManager:      cm,
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	)
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	// Attached after construction so NewLedgerState does not register the
	// node-level subscribers this focused test does not want.
	ls.config.EventBus = bus

	ancestorTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(ancestorBlock.Slot, ancestorBlock.Hash),
		BlockNumber: ancestorBlock.BlockNumber,
	}
	currentTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(currentBlock.Slot, currentBlock.Hash),
		BlockNumber: currentBlock.BlockNumber,
	}
	ancestorNonce := []byte("nonce-ancestor")
	currentNonce := []byte("nonce-current")
	require.NoError(t, db.SetBlockNonce(
		ancestorTip.Point.Hash,
		ancestorTip.Point.Slot,
		ancestorNonce,
		true,
		nil,
	))
	require.NoError(t, db.SetBlockNonce(
		currentTip.Point.Hash, currentTip.Point.Slot, currentNonce, false, nil,
	))
	require.NoError(t, db.SetTip(currentTip, nil))

	ls.currentTip = currentTip
	ls.currentTipBlockNonce = append([]byte(nil), currentNonce...)
	ls.chainsyncState = SyncingChainsyncState
	ls.publishSnapshotsLocked()

	return &chainsyncRollbackFixture{
		ls:          ls,
		ancestorTip: ancestorTip,
		currentTip:  currentTip,
		connId: ouroboros.ConnectionId{
			LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
			RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
		},
		ancestorNonce: ancestorNonce,
	}
}

// TestConnectionClosedPublishesHeaderInvalidation covers a header-queue
// discard on a peer-stall path. When the connection that owned the header
// pipeline closes, the queue is discarded and Chain.ClearHeaders enqueues the
// invalidation on the chain-level sequencer -- but this handler previously
// registered no drain, so it sat there until some unrelated handler ran. A
// dead peer is exactly the case where no further event is guaranteed, so the
// announcement would stay armed well past the ten-slot vote window.
func TestConnectionClosedPublishesHeaderInvalidation(t *testing.T) {
	fixture := newHeaderStreamLedger(t)
	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	header := announcingHeader(
		577, "hdr-1", lcommon.NewBlake2b256(nil), 1, ebHash,
	)
	require.NoError(t, fixture.ls.chain.AddVerifiedBlockHeader(header))
	fixture.ls.headerPipelineConnId = fixture.connId
	require.Equal(t, 1, fixture.ls.chain.HeaderCount())

	// The announcement is still undrained on the sequencer; the closed
	// connection must publish it and the invalidation that voids it.
	fixture.ls.handleConnectionClosedEvent(event.NewEvent(
		ConnectionClosedEventType,
		ConnectionClosedEvent{ConnectionId: fixture.connId},
	))
	assert.Zero(t, fixture.ls.chain.HeaderCount())

	announcement := testutil.RequireReceive(
		t, fixture.ch, testutil.AsyncWait, "announcement",
	)
	announced, ok := announcement.Data.(chain.ChainHeaderAnnouncementEvent)
	require.True(t, ok, "got %T", announcement.Data)
	assert.Equal(t, header.hash, announced.RbHash)

	invalidation := testutil.RequireReceive(
		t,
		fixture.ch,
		testutil.AsyncWait,
		"invalidation published without any later event",
	)
	invalid, ok := invalidation.Data.(chain.ChainHeaderInvalidationEvent)
	require.True(t, ok, "got %T", invalidation.Data)
	assert.Equal(t, chain.HeaderInvalidationQueueCleared, invalid.Reason)
	assert.Contains(t, invalid.RbHashes, header.hash)
	assert.Greater(t, invalid.Seq, announced.Seq)
}

// TestBlockfetchTimeoutDrainsHeaderSequencer covers the other peer-stall path.
// The timeout handler tears the batch down and clears the header queue, and it
// is the last thing that runs for a peer that stopped sending, so it has to
// drain the sequencer rather than leave header events queued behind it.
func TestBlockfetchTimeoutDrainsHeaderSequencer(t *testing.T) {
	fixture := newHeaderStreamLedger(t)
	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	header := announcingHeader(
		577, "hdr-1", lcommon.NewBlake2b256(nil), 1, ebHash,
	)
	require.NoError(t, fixture.ls.chain.AddVerifiedBlockHeader(header))

	var pending pendingPublishes
	func() {
		defer pending.flush()
		fixture.ls.chainsyncBlockfetchMutex.Lock()
		defer fixture.ls.chainsyncBlockfetchMutex.Unlock()
		fixture.ls.handleBlockfetchTimeoutLocked(fixture.connId, &pending)
	}()

	evt := testutil.RequireReceive(
		t,
		fixture.ch,
		testutil.AsyncWait,
		"header events published without any later event",
	)
	announced, ok := evt.Data.(chain.ChainHeaderAnnouncementEvent)
	require.True(t, ok, "got %T", evt.Data)
	assert.Equal(t, header.hash, announced.RbHash)
}

func TestMithrilImportProvidesPreview1398RewardPParams(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	seedEligiblePreviewGoRewardBasis(t, db)

	currentParams := mithrilRewardConwayPParams()
	previousParams := *currentParams
	previousParams.MinFeeA++
	currentData, err := cbor.Encode(currentParams)
	require.NoError(t, err)
	previousData, err := cbor.Encode(&previousParams)
	require.NoError(t, err)

	eraBounds := make([]ledgerstate.EraBound, ledgerstate.EraConway+1)
	nonce := make([]byte, 32)
	require.NoError(t, ledgerstate.ImportLedgerState(
		context.Background(),
		ledgerstate.ImportConfig{
			Database: db,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
			State: &ledgerstate.RawLedgerState{
				PParamsData:         currentData,
				PrevPParamsData:     previousData,
				Epoch:               1397,
				EraIndex:            ledgerstate.EraConway,
				EraBounds:           eraBounds,
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Reserves:            100_000_000,
				Tip: &ledgerstate.SnapshotTip{
					Slot:      1_397_799,
					BlockHash: make([]byte, 32),
				},
			},
			EpochLength: func(uint) (uint, uint, error) {
				return 1, 1_000, nil
			},
		},
	))
	require.NoError(t, db.Metadata().SaveRewardAdaPots(
		&models.RewardAdaPots{
			Epoch:        1397,
			Reserves:     100_000_000,
			CapturedSlot: 1_397_799,
		},
		nil,
	))
	prefilterSlot, err := ls.rewardPrefilterSlot(db.Metadata(), nil, 1397)
	require.NoError(t, err)
	require.LessOrEqual(t, prefilterSlot, uint64(1_397_799))

	epochs, ok := stakeRewardEpochsForNewEpoch(1398)
	require.True(t, ok)
	require.Equal(t, uint64(1395), epochs.snapshot)
	require.Equal(t, uint64(1396), epochs.performance)
	require.Equal(t, uint64(1397), epochs.pots)

	currentEpoch, err := db.Metadata().GetEpoch(1397, nil)
	require.NoError(t, err)
	require.NotNil(t, currentEpoch)
	ls.currentEpoch = *currentEpoch
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = currentParams

	var rollover *EpochRolloverResult
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var rolloverErr error
		rollover, rolloverErr = ls.processEpochRollover(
			txn,
			*currentEpoch,
			eras.ConwayEraDesc,
			currentParams,
			false,
		)
		return rolloverErr
	}))
	require.NotNil(t, rollover)
	require.Equal(t, uint64(1398), rollover.NewCurrentEpoch.EpochId)
	poolOutputs, err := db.Metadata().GetRewardPoolOutputs(1395, nil)
	require.NoError(t, err)
	require.Len(t, poolOutputs, 1)
	require.Positive(t, uint64(poolOutputs[0].TotalReward))
	accountOutputs, err := db.Metadata().GetRewardAccountOutputs(1395, nil)
	require.NoError(t, err)
	require.NotEmpty(t, accountOutputs)
	var credited uint64
	for _, output := range accountOutputs {
		credited += uint64(output.Amount)
	}
	require.Positive(t, credited)
}

func seedEligiblePreviewGoRewardBasis(
	t *testing.T,
	db *database.Database,
) {
	t.Helper()
	const (
		rewardSnapshotEpoch = uint64(1395)
		capturedSlot        = uint64(1_397_799)
		boundarySlot        = uint64(1_395_000)
	)
	poolKey := rewardCalcHash(0x71)
	rewardAccount := rewardCalcHash(0x72)
	member := rewardCalcHash(0x73)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)

	for i := range uint64(10) {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			poolID,
			i+1,
			1_396_640+i,
			nil,
		))
	}
	meta := db.Metadata()
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 1_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     capturedSlot,
		BoundarySlot:     boundarySlot,
		ProtocolVersion:  10,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs(
		[]*models.RewardPoolInput{{
			Epoch:                      rewardSnapshotEpoch,
			PoolKeyHash:                poolKey,
			RewardAccount:              rewardAccount,
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			Pledge:                     500,
			Cost:                       1_000,
			DelegatedStake:             1_000,
			OwnerStake:                 500,
			DelegatorCount:             2,
			CapturedSlot:               capturedSlot,
			BoundarySlot:               boundarySlot,
		}},
		nil,
	))
	require.NoError(t, meta.SaveRewardStakeInputs(
		[]*models.RewardStakeInput{
			{
				Epoch:         rewardSnapshotEpoch,
				PoolKeyHash:   poolKey,
				CredentialTag: 0,
				StakingKey:    rewardAccount,
				Stake:         500,
				Owner:         true,
				Registered:    true,
				CapturedSlot:  capturedSlot,
				BoundarySlot:  boundarySlot,
			},
			{
				Epoch:         rewardSnapshotEpoch,
				PoolKeyHash:   poolKey,
				CredentialTag: 0,
				StakingKey:    member,
				Stake:         500,
				Registered:    true,
				CapturedSlot:  capturedSlot,
				BoundarySlot:  boundarySlot,
			},
		},
		nil,
	))

	pool := models.Pool{PoolKeyHash: poolKey}
	require.NoError(t, db.ImportPool(nil, &pool, &models.PoolRegistration{
		PoolID:      pool.ID,
		PoolKeyHash: poolKey,
		AddedSlot:   boundarySlot,
	}))
	for _, account := range [][]byte{rewardAccount, member} {
		require.NoError(t, db.CreateAccount(nil, &models.Account{
			StakingKey: account,
			Pool:       poolKey,
			Active:     true,
		}))
	}
	rewardCalcSeedStakeCert(
		t,
		db,
		1,
		rewardAccount,
		0,
		boundarySlot,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	rewardCalcSeedStakeCert(
		t,
		db,
		2,
		member,
		0,
		boundarySlot,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
}

func mithrilRewardConwayPParams() *conway.ConwayProtocolParameters {
	params := donationTestConwayPParams(10)
	params.MinFeeA = 44
	params.NOpt = 500
	params.A0 = &cbor.Rat{Rat: big.NewRat(3, 10)}
	params.Rho = &cbor.Rat{Rat: big.NewRat(3, 1000)}
	params.Tau = &cbor.Rat{Rat: big.NewRat(1, 5)}
	return params
}

func newDonationTestDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck
	return db
}

func networkState(
	t *testing.T,
	db *database.Database,
) (treasury, reserves, slot uint64) {
	t.Helper()
	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	return uint64(state.Treasury), uint64(state.Reserves), state.Slot
}

// TestApplyEpochDonations verifies that the ending epoch's donations are added
// to the treasury at the boundary slot, leaving reserves untouched, and that
// only the ended epoch's donations are moved.
func TestApplyEpochDonations(t *testing.T) {
	t.Parallel()

	db := newDonationTestDB(t)
	ls := &LedgerState{db: db}

	// Post-withdrawal treasury/reserves baseline at an earlier slot.
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))
	// Donations for the ending epoch (7) and a later epoch (8) that must not move.
	require.NoError(t, db.Metadata().AddNetworkDonation(60, 7, 100, nil))
	require.NoError(t, db.Metadata().AddNetworkDonation(70, 7, 200, nil))
	require.NoError(t, db.Metadata().AddNetworkDonation(600, 8, 999, nil))

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyEpochDonations(txn, 7, 80)
	}))

	treasury, reserves, slot := networkState(t, db)
	assert.Equal(
		t,
		uint64(1_300),
		treasury,
		"treasury += epoch-7 donations (100+200)",
	)
	assert.Equal(t, uint64(5_000), reserves, "reserves untouched by donations")
	assert.Equal(t, uint64(80), slot, "updated at the boundary slot")
}

// TestApplyEpochDonations_NoDonations is a no-op when the ended epoch had none.
func TestApplyEpochDonations_NoDonations(t *testing.T) {
	t.Parallel()

	db := newDonationTestDB(t)
	ls := &LedgerState{db: db}
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyEpochDonations(txn, 7, 80)
	}))

	treasury, reserves, slot := networkState(t, db)
	assert.Equal(t, uint64(1_000), treasury)
	assert.Equal(t, uint64(5_000), reserves)
	assert.Equal(
		t,
		uint64(50),
		slot,
		"no boundary row written when no donations",
	)
}

// TestEpochDonationWithdrawalRollback exercises the acceptance scenario: a
// treasury withdrawal (modelled as a debited treasury) followed by a donation
// at the boundary, then a rollback past the boundary that restores the prior
// treasury and drops the donation rows so re-application is deterministic.
func TestEpochDonationWithdrawalRollback(t *testing.T) {
	t.Parallel()

	db := newDonationTestDB(t)
	ls := &LedgerState{db: db}

	// Epoch 7 starts with treasury 1_000 (slot 50).
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))
	// A donation block lands mid-epoch.
	require.NoError(t, db.Metadata().AddNetworkDonation(70, 7, 300, nil))
	// At the 7->8 boundary (slot 80) a treasury withdrawal of 400 is enacted
	// first (checked against the pre-donation treasury of 1_000), debiting the
	// treasury to 600...
	require.NoError(t, db.Metadata().SetNetworkState(600, 5_000, 80, nil))
	// ...then the epoch's donations are added on top.
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyEpochDonations(txn, 7, 80)
	}))
	treasury, reserves, slot := networkState(t, db)
	require.Equal(
		t,
		uint64(900),
		treasury,
		"1_000 - 400 withdrawal + 300 donation",
	)
	require.Equal(t, uint64(5_000), reserves)
	require.Equal(t, uint64(80), slot)

	// Roll back past the boundary (to slot 60): the boundary NetworkState row
	// and the donation row are dropped, restoring epoch 7's starting treasury.
	require.NoError(t, db.DeleteNetworkStateAfterSlot(60, nil))
	require.NoError(t, db.DeleteNetworkDonationsAfterSlot(60, nil))

	treasury, reserves, slot = networkState(t, db)
	assert.Equal(
		t,
		uint64(1_000),
		treasury,
		"treasury restored to pre-boundary value",
	)
	assert.Equal(t, uint64(5_000), reserves)
	assert.Equal(t, uint64(50), slot)
	sum, err := db.Metadata().SumNetworkDonationsForEpoch(7, nil)
	require.NoError(t, err)
	assert.Zero(t, sum, "rolled-back donation rows are gone")
}

// donationTestConwayPParams builds Conway pparams with voting thresholds so
// governance.ProcessEpoch's ratification phase has the fields it reads.
func donationTestConwayPParams(major uint) *conway.ConwayProtocolParameters {
	rat := func(n, d int64) cbor.Rat { return cbor.Rat{Rat: big.NewRat(n, d)} }
	p := &conway.ConwayProtocolParameters{}
	p.ProtocolVersion.Major = major
	p.MinCommitteeSize = 3
	p.DRepVotingThresholds = conway.DRepVotingThresholds{
		MotionNoConfidence:    rat(67, 100),
		CommitteeNormal:       rat(67, 100),
		CommitteeNoConfidence: rat(60, 100),
		UpdateToConstitution:  rat(75, 100),
		HardForkInitiation:    rat(60, 100),
		PpNetworkGroup:        rat(67, 100),
		PpEconomicGroup:       rat(67, 100),
		PpTechnicalGroup:      rat(67, 100),
		PpGovGroup:            rat(75, 100),
		TreasuryWithdrawal:    rat(67, 100),
	}
	p.PoolVotingThresholds = conway.PoolVotingThresholds{
		MotionNoConfidence:    rat(51, 100),
		CommitteeNormal:       rat(51, 100),
		CommitteeNoConfidence: rat(51, 100),
		HardForkInitiation:    rat(51, 100),
		PpSecurityGroup:       rat(51, 100),
	}
	return p
}

// TestEpochProcessWithdrawalThenDonation drives the real Conway enactment path:
// a ratified treasury withdrawal is enacted by governance.ProcessEpoch and then
// the ending epoch's donation is applied, exactly as processEpochRollover
// sequences them. It proves the withdrawal is checked/applied against the
// pre-donation treasury (the value the ledger uses at the boundary) and the
// donation is added afterwards.
func TestEpochProcessWithdrawalThenDonation(t *testing.T) {
	t.Parallel()

	db := newDonationTestDB(t)
	ls := &LedgerState{db: db}

	const (
		initialTreasury = uint64(1_000)
		initialReserves = uint64(200)
		withdrawal      = uint64(400)
		donation        = uint64(300)
		endedEpoch      = uint64(4)
		boundarySlot    = uint64(500)
	)

	// Registered reward account that the withdrawal pays out to.
	stakeCred := make([]byte, 28)
	for i := range stakeCred {
		stakeCred[i] = 0x42
	}
	withdrawAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	withdrawAddrBytes, err := withdrawAddr.Bytes()
	require.NoError(t, err)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}))

	// A ratified treasury-withdrawal proposal so ProcessEpoch enacts it.
	withdrawalCbor, err := cbor.Encode(&lcommon.TreasuryWithdrawalGovAction{
		Type:        2,
		Withdrawals: map[*lcommon.Address]uint64{&withdrawAddr: withdrawal},
	})
	require.NoError(t, err)
	ratifiedEpoch := endedEpoch
	ratifiedSlot := uint64(400)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        make([]byte, 32),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: 3,
		ExpiresEpoch:  10,
		RatifiedEpoch: &ratifiedEpoch,
		RatifiedSlot:  &ratifiedSlot,
		AnchorURL:     "https://example.invalid/withdrawal",
		AnchorHash:    make([]byte, 32),
		Deposit:       0,
		ReturnAddress: withdrawAddrBytes,
		GovActionCbor: withdrawalCbor,
		AddedSlot:     101,
	}, nil))

	// Initial treasury/reserves and the ending epoch's donation.
	require.NoError(t, db.Metadata().SetNetworkState(
		initialTreasury, initialReserves, 1, nil,
	))
	require.NoError(t, db.Metadata().AddNetworkDonation(
		70, endedEpoch, donation, nil,
	))

	runBoundary := func() uint64 {
		t.Helper()
		var providerTreasury uint64
		txn := db.Transaction(true)
		require.NoError(t, txn.Do(func(txn *database.Txn) error {
			if _, err := governance.ProcessEpoch(&governance.EpochInput{
				DB:           db,
				Txn:          txn,
				PrevEpoch:    endedEpoch,
				NewEpoch:     endedEpoch + 1,
				BoundarySlot: boundarySlot,
				PParams:      donationTestConwayPParams(10),
				UpdateFn: func(
					p lcommon.ProtocolParameters, _ any,
				) (lcommon.ProtocolParameters, error) {
					return p, nil
				},
			}); err != nil {
				return err
			}
			if err := ls.applyEpochDonations(
				txn,
				endedEpoch,
				boundarySlot,
			); err != nil {
				return err
			}
			var err error
			providerTreasury, err = ls.NewView(txn).TreasuryValue()
			return err
		}))
		return providerTreasury
	}

	require.Equal(t, uint64(900), runBoundary())

	// Withdrawal (400) was applied against the pre-donation treasury (1000),
	// then the donation (300) was added: 1000 - 400 + 300 = 900.
	treasury, reserves, _ := networkState(t, db)
	assert.Equal(t, uint64(900), treasury,
		"treasury = initial - withdrawal + donation")
	assert.Equal(t, initialReserves, reserves)

	// The withdrawal credited the registered reward account.
	account, err := db.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, withdrawal, uint64(account.Reward),
		"withdrawal paid to the reward account")

	// A crash between the boundary transaction and the tip advance replays
	// the boundary after reward application rewrites the absolute pot row.
	// The enacted proposal and reward credit are replay-idempotent, while the
	// provider must still expose the same post-withdrawal, post-donation value.
	require.NoError(t, db.Metadata().SetNetworkState(
		initialTreasury,
		initialReserves,
		boundarySlot,
		nil,
	))
	require.Equal(t, uint64(900), runBoundary())
	requireTreasuryValue(t, ls, nil, 900)

	account, err = db.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, withdrawal, uint64(account.Reward),
		"boundary replay must not double-credit the withdrawal")

	// Rewinding before both the donation block and boundary restores the
	// earlier pot row. These are the same slot-keyed deletes used by the
	// database rollback path.
	require.NoError(t, db.DeleteNetworkStateAfterSlot(1, nil))
	require.NoError(t, db.DeleteNetworkDonationsAfterSlot(1, nil))
	requireTreasuryValue(t, ls, nil, initialTreasury)
}

// TestAddUint64Overflow exercises addUint64 at the exact uint64 max
// boundary: maxUint64-1 plus 1 is the largest sum that fits, plus 2
// overflows.
func TestAddUint64Overflow(t *testing.T) {
	t.Parallel()

	maxUint64 := ^uint64(0)

	sum, err := addUint64(maxUint64-1, 1)
	require.NoError(t, err)
	assert.Equal(t, maxUint64, sum)

	_, err = addUint64(maxUint64-1, 2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "overflows uint64")
}

// TestLedgerDeltaDonateOverflow exercises LedgerDelta.donate at the exact
// uint64 max boundary for d.donation.
func TestLedgerDeltaDonateOverflow(t *testing.T) {
	t.Parallel()

	maxUint64 := ^uint64(0)

	d := &LedgerDelta{donation: maxUint64 - 1}
	require.NoError(t, d.donate(1))
	assert.Equal(t, maxUint64, d.donation)

	d2 := &LedgerDelta{donation: maxUint64 - 1}
	err := d2.donate(2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "overflows uint64")
	assert.Equal(
		t, maxUint64-1, d2.donation,
		"donation left unchanged on overflow",
	)
}

// conwayDonationTx builds a valid Conway transaction whose body carries the
// given treasury donation, for feeding into LedgerDelta donation aggregation.
func conwayDonationTx(donation uint64) *conway.ConwayTransaction {
	return &conway.ConwayTransaction{
		Body:      conway.ConwayTransactionBody{TxDonation: donation},
		TxIsValid: true,
	}
}

// TestLedgerDeltaAccumulateNetworkDonationsOverflow drives the per-tx
// donation summation in accumulateNetworkDonations to the exact uint64 max
// boundary using two real Conway transactions.
func TestLedgerDeltaAccumulateNetworkDonationsOverflow(t *testing.T) {
	t.Parallel()

	maxUint64 := ^uint64(0)

	newDelta := func(donationB uint64) *LedgerDelta {
		return &LedgerDelta{
			Transactions: []TransactionRecord{
				{Tx: conwayDonationTx(maxUint64 - 1), Index: 0},
				{Tx: conwayDonationTx(donationB), Index: 1},
			},
		}
	}

	t.Run("just below overflow succeeds", func(t *testing.T) {
		d := newDelta(1)
		require.NoError(t, d.accumulateNetworkDonations(nil))
		assert.Equal(t, maxUint64, d.donation)
	})

	t.Run("just above overflow fails", func(t *testing.T) {
		d := newDelta(2)
		err := d.accumulateNetworkDonations(nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "overflows uint64")
	})
}

// TestLedgerDeltaRecordNetworkDonationsOverflowPreservesState verifies that
// a donation-sum overflow aborts before any database write: no
// network_donation row is recorded and the network state is untouched.
func TestLedgerDeltaRecordNetworkDonationsOverflowPreservesState(t *testing.T) {
	t.Parallel()

	db := newDonationTestDB(t)
	ls := &LedgerState{db: db}
	maxUint64 := ^uint64(0)

	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))

	delta := &LedgerDelta{
		Transactions: []TransactionRecord{
			{Tx: conwayDonationTx(maxUint64 - 1), Index: 0},
			{Tx: conwayDonationTx(2), Index: 1},
		},
	}

	txn := db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		return delta.recordNetworkDonations(ls, txn, nil)
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "overflows uint64")

	total, err := db.Metadata().SumNetworkDonationsForEpoch(0, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), total, "no donation row recorded on overflow")

	treasury, reserves, slot := networkState(t, db)
	assert.Equal(t, uint64(1_000), treasury, "treasury untouched on overflow")
	assert.Equal(t, uint64(5_000), reserves, "reserves untouched on overflow")
	assert.Equal(
		t,
		uint64(50),
		slot,
		"network state slot untouched on overflow",
	)
}

// TestPersistTipAfterForgedBlockUpdatesPersistedTip verifies that
// persistTipAfterForgedBlock actually advances database.GetTip to match
// the forged block -- forgeBlock's own ls.chain.AddBlock call only
// updates ls.chain's in-memory tip, not the persisted one, unlike the
// normal chainsync/forged-block batch pipeline (which calls db.SetTip
// itself). Without this call, a dev-mode-forged block is written to the
// blob/metadata block tables but invisible to anything relying on the
// persisted tip -- dingoctl's `database info`, a live Truncate's
// deletion boundary, and BlockForger's leader-election check all read
// stale data, and a later Truncate can never reach (and clean up) such a
// block, eventually surfacing as a "persistent chain index gap" error.
func TestPersistTipAfterForgedBlockUpdatesPersistedTip(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	block := newRecordForgedBlockTestBlock(42, 7)
	require.NoError(t, ls.persistTipAfterForgedBlock(block))

	tip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(42), tip.Point.Slot)
	require.Equal(t, block.Hash().Bytes(), tip.Point.Hash)
	require.Equal(t, uint64(7), tip.BlockNumber)
}

func TestLedgerProcessBlockRunsPhase1ForPhase2InvalidTransaction(
	t *testing.T,
) {
	t.Parallel()

	const (
		blockSlot     = uint64(10)
		invalidBefore = uint64(11)
	)
	db := newTestDB(t)
	// This uses the public block-application path. The Dijkstra validator owns
	// the protocol-defined phase-two skip; this regression checks that Dingo
	// still invokes it for phase-one validation.
	// Key 8 is the upstream invalid-before/lower-bound field. Deliberately omit
	// key 3 (invalid-hereafter) so this regression is independent of the
	// separately owned upstream upper-bound implementation.
	txCbor, err := cbor.Encode([]any{
		map[uint]any{
			0: cbor.Tag{Number: 258, Content: []any{}},
			1: []any{},
			2: uint64(0),
			8: invalidBefore,
		},
		map[uint]any{},
		true,
		nil,
	})
	require.NoError(t, err)
	tx, err := dijkstra.NewDijkstraTransactionFromCbor(txCbor)
	require.NoError(t, err)
	tx.TxIsValid = false

	pparams := dijkstraTestProtocolParameters()
	pparams.MaxBlockBodySize = 100_000
	pparams.MaxBlockHeaderSize = 100_000
	var txHash [32]byte
	copy(txHash[:], tx.Hash().Bytes())
	// Fixture CBOR is bounded well below uint32.
	byteLength := uint32(len(txCbor)) // #nosec G115
	offsets := &database.BlockIngestionResult{
		TxOffsets: map[[32]byte]database.CborOffset{
			txHash: {
				BlockSlot:  blockSlot,
				ByteLength: byteLength,
			},
		},
	}
	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: 1,
					Slot:        blockSlot,
					ProtoVersion: babbage.BabbageProtoVersion{
						Major: 12,
					},
				},
			},
		},
		BlockBody: dijkstra.DijkstraBlockBody{
			InvalidTransactions: []uint{0},
			Transactions:        []dijkstra.DijkstraTransaction{*tx},
		},
	}
	bodyCbor, err := block.BlockBody.MarshalCBOR()
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = uint64(len(bodyCbor))
	blockCbor, err := block.MarshalCBOR()
	require.NoError(t, err)
	block.SetCbor(blockCbor)

	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	err = db.Transaction(true).Do(func(txn *database.Txn) error {
		_, err := ls.ledgerProcessBlock(
			txn,
			ocommon.Point{Slot: blockSlot, Hash: []byte("phase-1-invalid-tx")},
			block,
			true,
			false,
			false,
			nil,
			envelopeParent{},
			offsets,
			eras.DijkstraEraDesc,
			pparams,
			nil,
			0,
			0,
			false,
		)
		return err
	})
	var outsideValidityIntervalErr allegra.OutsideValidityIntervalUtxoError
	require.ErrorAs(
		t,
		err,
		&outsideValidityIntervalErr,
		"phase-2-invalid transactions must still run phase-1 rules",
	)
	require.Equal(
		t,
		invalidBefore,
		outsideValidityIntervalErr.ValidityIntervalStart,
	)
	require.Equal(t, blockSlot, outsideValidityIntervalErr.Slot)
}

// A restart that made progress is not backed off at all, and is never stuck.
func TestLedgerPipelineBackoffProgressResets(t *testing.T) {
	t.Parallel()

	for _, consecutive := range []int{0, -1} {
		backoff, stuck := ledgerPipelineBackoff(consecutive)
		require.Zero(t, backoff)
		require.False(t, stuck)
	}
}

// Transient failures back off gently and are not reported as stuck: the
// pipeline restarting a handful of times is normal (a rollback racing the
// iterator, a peer dropping mid-batch) and must not raise an operator alarm.
func TestLedgerPipelineBackoffTransientFailuresAreNotStuck(t *testing.T) {
	t.Parallel()

	prev := time.Duration(0)
	for consecutive := 1; consecutive < noProgressStuckThreshold; consecutive++ {
		backoff, stuck := ledgerPipelineBackoff(consecutive)
		require.False(t, stuck,
			"%d consecutive restarts should still be transient", consecutive)
		require.LessOrEqual(t, backoff, noProgressBackoffMax,
			"transient backoff must stay under the normal ceiling")
		require.GreaterOrEqual(t, backoff, prev,
			"backoff must be monotonic")
		prev = backoff
	}
	require.Equal(t, noProgressBackoffMax, prev,
		"backoff should reach the normal ceiling before the stuck threshold")
}

// A deterministic failure -- a canonical block the node rejects every time --
// never stops repeating. Capping at the transient ceiling means retrying it
// forever at that rate, which is what turned a single rejected block into a
// node that spun every two seconds indefinitely. Past the threshold the
// pipeline is declared stuck and the wait escalates well beyond the transient
// ceiling.
func TestLedgerPipelineBackoffDeterministicFailureEscalates(t *testing.T) {
	t.Parallel()

	_, stuck := ledgerPipelineBackoff(noProgressStuckThreshold)
	require.True(t, stuck, "the stuck threshold should report stuck")

	// Escalates past the transient ceiling rather than sitting on it.
	longRun, stuck := ledgerPipelineBackoff(noProgressStuckThreshold + 20)
	require.True(t, stuck)
	require.Greater(t, longRun, noProgressBackoffMax,
		"a stuck pipeline must back off further than a transient one")
	require.LessOrEqual(t, longRun, noProgressStuckBackoffMax,
		"backoff must stay bounded by the stuck ceiling")

	// And is bounded no matter how long it stays stuck.
	forever, stuck := ledgerPipelineBackoff(1_000_000)
	require.True(t, stuck)
	require.Equal(t, noProgressStuckBackoffMax, forever)
}

// Monotonic across the transient/stuck boundary: the escalation must not dip.
func TestLedgerPipelineBackoffIsMonotonic(t *testing.T) {
	t.Parallel()

	prev := time.Duration(0)
	for consecutive := range noProgressStuckThreshold + 64 {
		backoff, _ := ledgerPipelineBackoff(consecutive)
		require.GreaterOrEqual(t, backoff, prev,
			"backoff dipped at %d consecutive restarts", consecutive)
		prev = backoff
	}
}

// Rejected blocks and unavailable certified blocks share the same no-progress
// budget. The latter has a minimum retry delay, but it must still enter the
// escalating cap; otherwise a deterministic rejection can spin forever at its
// fixed cadence.
func TestLedgerPipelineRetryDelayIsBounded(t *testing.T) {
	t.Parallel()

	minimum := 250 * time.Millisecond
	previous := time.Duration(0)
	for consecutive := range noProgressStuckThreshold + 64 {
		delay, stuck := ledgerPipelineRetryDelay(consecutive, minimum)
		require.GreaterOrEqual(t, delay, minimum)
		require.GreaterOrEqual(t, delay, previous,
			"retry delay dipped at %d consecutive rejections", consecutive)
		if consecutive < noProgressStuckThreshold {
			require.False(t, stuck)
		}
		previous = delay
	}

	stuckDelay, stuck := ledgerPipelineRetryDelay(
		noProgressStuckThreshold,
		minimum,
	)
	require.True(t, stuck)
	require.Greater(t, stuckDelay, minimum,
		"a deterministic rejection must leave its minimum retry cadence")
	forever, stuck := ledgerPipelineRetryDelay(1_000_000, minimum)
	require.True(t, stuck)
	require.Equal(t, noProgressStuckBackoffMax, forever,
		"rejection retry delay must have a finite upper bound")
}

func newPipelineLoopLedger(t *testing.T) *LedgerState {
	t.Helper()
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	return ls
}

// TestLedgerProcessBlocksStopsRetryingOnUnrepairableFailure covers the terminal
// half of issue #3261. Recovery raises errHaltLedgerPipeline once it has
// established that no local replay can change a block's verdict; the restart
// loop must then stop rather than restart into the same block forever, and must
// leave a terminal signal behind for an operator.
func TestLedgerProcessBlocksStopsRetryingOnUnrepairableFailure(t *testing.T) {
	t.Parallel()

	ls := newPipelineLoopLedger(t)

	var attempts atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		ls.ledgerProcessBlocksWithAttempt(
			t.Context(),
			func(context.Context) error {
				attempts.Add(1)
				return fmt.Errorf(
					"process block batch: %w",
					errHaltLedgerPipeline,
				)
			},
		)
	}()
	testutil.RequireReceive(
		t,
		done,
		testutil.AsyncWait,
		"an unrepairable validation failure must stop the ledger pipeline",
	)

	assert.Equal(
		t,
		int64(1),
		attempts.Load(),
		"a halted pipeline must not run another attempt",
	)
	assert.Equal(
		t,
		1.0,
		promtestutil.ToFloat64(ls.metrics.pipelineHalted),
		"a halted pipeline must report its terminal state",
	)
}

// TestLedgerProcessBlocksKeepsRetryingRecoverableFailures is the negative case:
// an ordinary failure must keep restarting the pipeline. Treating every failure
// as terminal would turn a transient database or peer problem into an outage.
func TestLedgerProcessBlocksKeepsRetryingRecoverableFailures(t *testing.T) {
	t.Parallel()

	ls := newPipelineLoopLedger(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var attempts atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		ls.ledgerProcessBlocksWithAttempt(
			ctx,
			func(context.Context) error {
				attempts.Add(1)
				return errors.New("transient read failure")
			},
		)
	}()
	testutil.WaitForCondition(
		t,
		func() bool { return attempts.Load() >= 3 },
		testutil.AsyncWait,
		"a recoverable failure must keep restarting the pipeline",
	)
	assert.Zero(
		t,
		promtestutil.ToFloat64(ls.metrics.pipelineHalted),
		"a retrying pipeline must not report itself halted",
	)

	cancel()
	testutil.RequireReceive(
		t,
		done,
		testutil.AsyncWait,
		"the pipeline loop must exit when its context is cancelled",
	)
}

func TestStopStuckLedgerPipelineDoesNotInvokeFatalCallback(t *testing.T) {
	t.Parallel()

	ls := newPipelineLoopLedger(t)
	var fatalErr error
	ls.config.FatalErrorFunc = func(err error) {
		fatalErr = err
	}
	progress := pipelineProgress{
		consecutiveNoProgress: noProgressStuckThreshold,
		lastTipSlot:           123,
	}

	ls.stopStuckLedgerPipeline(errors.New("rejected block"), progress)
	assert.NoError(t, fatalErr)
}

// TestResetMithrilBoundaryRejectionsRequiresAppliedTipProgress verifies that
// the trust-window tally is keyed on applied ledger progress rather than on a
// reported failing-block identity. Replay can report changing failures while
// rebuilding to the same applied tip; those are still one non-converging run.
func TestResetMithrilBoundaryRejectionsRequiresAppliedTipProgress(
	t *testing.T,
) {
	t.Parallel()

	ls := &LedgerState{}

	rejections, exhausted := ls.observeMithrilBoundaryRejection(500)
	require.Equal(t, 1, rejections)
	require.False(t, exhausted)

	// Replaying to the same or an older applied tip is not progress.
	ls.resetMithrilBoundaryRejections(500)
	rejections, exhausted = ls.observeMithrilBoundaryRejection(499)
	require.Equal(t, 2, rejections)
	require.False(t, exhausted)

	// Advancing the applied high-water mark is real progress and starts a
	// later recovery run with a fresh budget.
	ls.resetMithrilBoundaryRejections(501)
	rejections, exhausted = ls.observeMithrilBoundaryRejection(501)
	assert.Equal(t, 1, rejections)
	assert.False(t, exhausted)

	// Every scheduled rewind depth refused, plus the capped retry the
	// schedule settles on: the legal rewind space is exhausted.
	for rejections < maxMithrilBoundaryRecoveryRejections {
		rejections, exhausted = ls.observeMithrilBoundaryRejection(501)
		require.False(
			t,
			exhausted,
			"%d refusals is still inside the bound",
			rejections,
		)
	}
	_, exhausted = ls.observeMithrilBoundaryRejection(501)
	require.True(
		t,
		exhausted,
		"the tally must be exhausted once every legal rewind depth has been refused",
	)
}

func TestResetAtTipRecoveryDescentClearsSameFailureOnProgress(
	t *testing.T,
) {
	t.Parallel()

	failure := &txValidationError{
		BlockPoint: ocommon.NewPoint(510, []byte("failure-block")),
		TxHash:     []byte("failure-tx"),
	}
	ls := &LedgerState{
		lastAtTipRecovery:         newAtTipRecoveryAttempt(failure),
		atTipRecoveryLastFailSlot: failure.BlockPoint.Slot,
		atTipRecoveryDescentCount: 1,
		atTipRecoveryHolding:      true,
	}
	ls.lastAtTipRecovery.Attempts = 2
	require.Nil(t, ls.mithrilBoundaryRecovery)

	// Replaying to the same applied tip is not progress. Preserve both the
	// same-failure depth and distinct-failure convergence state.
	ls.resetAtTipRecoveryDescent(failure.BlockPoint.Slot)
	require.Equal(t, 2, ls.lastAtTipRecovery.Attempts)
	require.Equal(t, 1, ls.atTipRecoveryDescentCount)
	require.True(t, ls.atTipRecoveryHolding)

	// A committed block past the failing region proves recovery converged.
	// A later at-tip failure must start at the shallow first attempt.
	ls.resetAtTipRecoveryDescent(failure.BlockPoint.Slot + 1)
	require.Nil(t, ls.lastAtTipRecovery)
	require.Zero(t, ls.atTipRecoveryLastFailSlot)
	require.Zero(t, ls.atTipRecoveryDescentCount)
	require.False(t, ls.atTipRecoveryHolding)
}

// previewWedgeLedgerState reproduces the ledger state the from-genesis Preview
// replay was in when it wedged on issue #3844: epoch 40 of the Babbage era, a
// published tip at block 168143 (slot 3516450), and the next two blocks not yet
// reflected in that tip because their batch had not committed. Preview's
// genesis gives the 25920-slot safe zone (see newTestEraHistoryCfg).
func previewWedgeLedgerState(t testing.TB) *LedgerState {
	t.Helper()
	nodeConfig := newTestEraHistoryCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	ls := &LedgerState{
		epochCache: []models.Epoch{{
			EpochId:       previewEraStartEpoch,
			StartSlot:     previewEraStartSlot,
			SlotLength:    1_000,
			LengthInSlots: previewEpochSize,
			EraId:         eras.BabbageEraDesc.Id,
		}},
		currentEra: eras.BabbageEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				previewPublishedTipSlot,
				[]byte("published-tip"),
			),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            testLogger(),
		},
	}
	ls.publishSnapshotsLocked()
	return ls
}

// TestLedgerViewSlotToTimeUsesHorizonAnchor pins the routing at the call site
// the #3844 fix changes. LedgerView.SlotToTime is the converter every Plutus
// script context translates its validity interval through, so the anchor has to
// reach the summary from there and the horizon has to survive the trip.
func TestLedgerViewSlotToTimeUsesHorizonAnchor(t *testing.T) {
	t.Parallel()

	ls := previewWedgeLedgerState(t)

	// Unanchored, this is the wedge: the view falls back to the published tip
	// and refuses the transaction's validity bound.
	unanchored := &LedgerView{ls: ls}
	_, err := unanchored.SlotToTime(previewTxUpperBound)
	require.ErrorIs(t, err, hardfork.ErrPastHorizon,
		"the published tip must still leave this bound past the horizon; "+
			"if it does not, the fixture no longer reproduces #3844")

	// Anchored at the applied block's predecessor, the same bound converts.
	anchored := &LedgerView{ls: ls, horizonAnchorSlot: previewParentSlot}
	when, err := anchored.SlotToTime(previewTxUpperBound)
	require.NoError(t, err,
		"a Plutus validity bound inside the predecessor-anchored horizon "+
			"must translate")
	expected, err := ls.hardForkSummaryAnchoredAt(previewParentSlot)
	require.NoError(t, err)
	wantTime, err := expected.SlotToTime(previewTxUpperBound)
	require.NoError(t, err)
	assert.Equal(t, wantTime, when)

	// The anchor moves the horizon; it does not remove it. cardano-ledger
	// fails a Plutus transaction whose bound cannot be translated
	// (TimeTranslationPastHorizon), so this must stay an error rather than
	// become an in-era extrapolation.
	_, err = anchored.SlotToTime(previewParentHorizon)
	require.ErrorIs(t, err, hardfork.ErrPastHorizon,
		"a bound past the anchored horizon must still be refused")
}

// errHorizonProbeDone stops ledgerProcessBlock right after the probe has run,
// so the assertion is about the LedgerView it was handed rather than about
// everything block application does afterwards.
var errHorizonProbeDone = errors.New("horizon probe complete")

// TestLedgerProcessBlockAnchorsValidationHorizonAtParent proves the anchor is
// actually wired from block application, not merely available on LedgerView.
// The reference implementation ticks from the applied block's immediate
// predecessor, so that predecessor — not the published tip, which lags by a
// whole block batch during replay — is what the safe zone must be measured
// from.
func TestLedgerProcessBlockAnchorsValidationHorizonAtParent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		parentSlot uint64
		wantErr    error
	}{
		{
			// Block 168145's real predecessor on Preview is block 168144 at
			// slot 3516496, so this is the case that has to succeed.
			name:       "applied predecessor",
			parentSlot: previewParentSlot,
		},
		{
			// The published tip trails by one block. Before the fix this was
			// the only anchor available, and it rejected the block.
			name:       "published tip",
			parentSlot: previewPublishedTipSlot,
			wantErr:    hardfork.ErrPastHorizon,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := newTestDB(t)
			ls := previewWedgeLedgerState(t)
			ls.db = db

			var probeErr error
			var probed bool
			testEra := eras.BabbageEraDesc
			testEra.ValidateTxFunc = func(
				_ lcommon.Transaction,
				_ uint64,
				view lcommon.LedgerState,
				_ lcommon.ProtocolParameters,
			) error {
				lv, ok := view.(*LedgerView)
				require.True(t, ok,
					"block application must hand the era validator the "+
						"LedgerView that carries the horizon anchor")
				probed = true
				_, probeErr = lv.SlotToTime(previewTxUpperBound)
				return errHorizonProbeDone
			}
			ls.activeEras = []eras.EraDesc{testEra}

			blocks, err := omockfixtures.GenerateBabbageChain(
				168_145, lcommon.Blake2b256{}, previewBlockSlot, 1, 1,
			)
			require.NoError(t, err)
			block, ok := blocks[0].(*babbage.BabbageBlock)
			require.True(t, ok)
			block.TransactionBodies = []babbage.BabbageTransactionBody{{}}
			block.TransactionWitnessSets = []babbage.BabbageTransactionWitnessSet{
				{},
			}
			pparams := &babbage.BabbageProtocolParameters{
				ProtocolMajor:      8,
				MaxBlockBodySize:   100_000,
				MaxBlockHeaderSize: 100_000,
			}
			processErr := db.Transaction(true).
				Do(func(txn *database.Txn) error {
					_, err := ls.ledgerProcessBlock(
						txn,
						ocommon.NewPoint(
							previewBlockSlot,
							block.Hash().Bytes(),
						),
						block,
						true,
						false,
						false,
						nil,
						envelopeParent{
							slot:        test.parentSlot,
							blockNumber: 168_144,
						},
						&database.BlockIngestionResult{},
						testEra,
						pparams,
						nil,
						previewEraStartEpoch,
						0,
						false,
					)
					return err
				})
			require.ErrorIs(t, processErr, errHorizonProbeDone)
			require.True(t, probed)
			if test.wantErr != nil {
				require.ErrorIs(t, probeErr, test.wantErr)
				return
			}
			require.NoError(t, probeErr,
				"the block that wedged the Preview replay must convert its "+
					"Plutus validity bound")
		})
	}
}

// newPPUPWindowLedgerState builds a ledger whose Shelley genesis carries the
// given security parameter and active-slot coefficient and whose epoch cache
// holds epochs.
func newPPUPWindowLedgerState(
	t *testing.T,
	securityParam int,
	activeSlotsCoeff *big.Rat,
	epochs []models.Epoch,
) *LedgerState {
	t.Helper()
	cfg := newGenesisDelegateShelleyGenesisCfg(
		t,
		strings.Repeat("aa", lcommon.Blake2b224Size),
		strings.Repeat("bb", lcommon.Blake2b256Size),
	)
	genesis := cfg.ShelleyGenesis()
	genesis.SecurityParam = securityParam
	genesis.ActiveSlotsCoeff = cbor.Rat{Rat: activeSlotsCoeff}
	ls := &LedgerState{}
	ls.config.CardanoNodeConfig = cfg
	ls.consensus.Store(&consensusSnapshot{epochCache: epochs})
	return ls
}

// The reference slot of no return is
// epochInfoFirst (succ e) *- Duration (2 * stabilityWindow), with
// stabilityWindow = computeStabilityWindow k f = ceiling (3k/f)
// (cardano-ledger Cardano.Ledger.Slot.getTheSlotOfNoReturn and
// Cardano.Ledger.Shelley.StabilityWindow). When 3k/f is not an integer,
// 2 * ceiling (3k/f) is larger than floor (6k/f).
func TestProtocolParameterUpdateWindowMatchesReferenceSlotOfNoReturn(
	t *testing.T,
) {
	t.Parallel()
	for _, tc := range []struct {
		name             string
		securityParam    int
		activeSlotsCoeff *big.Rat
		epoch            models.Epoch
		noReturn         uint64
	}{
		{
			// Mainnet and preprod: 2 * 3 * 2160 / 0.05 = 259200.
			name:             "mainnet first Shelley epoch",
			securityParam:    2160,
			activeSlotsCoeff: big.NewRat(1, 20),
			epoch:            models.Epoch{EpochId: 208, StartSlot: 4_492_800, LengthInSlots: 432_000},
			noReturn:         4_492_800 + 432_000 - 259_200,
		},
		{
			// Preview: 2 * 3 * 432 / 0.05 = 51840.
			name:             "preview",
			securityParam:    432,
			activeSlotsCoeff: big.NewRat(1, 20),
			epoch:            models.Epoch{EpochId: 700, StartSlot: 60_480_000, LengthInSlots: 86_400},
			noReturn:         60_480_000 + 86_400 - 51_840,
		},
		{
			// 3k/f = 30/7: ceiling 5, so 2 * 5 = 10 where floor (60/7) = 8.
			name:             "fractional window below one half",
			securityParam:    1,
			activeSlotsCoeff: big.NewRat(7, 10),
			epoch:            models.Epoch{EpochId: 4, StartSlot: 500, LengthInSlots: 100},
			noReturn:         600 - 10,
		},
		{
			// 3k/f = 300/7: ceiling 43, so 2 * 43 = 86 where floor (600/7) = 85.
			name:             "fractional window above one half",
			securityParam:    1,
			activeSlotsCoeff: big.NewRat(7, 100),
			epoch:            models.Epoch{EpochId: 4, StartSlot: 500, LengthInSlots: 200},
			noReturn:         700 - 86,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ls := newPPUPWindowLedgerState(
				t,
				tc.securityParam,
				tc.activeSlotsCoeff,
				[]models.Epoch{tc.epoch},
			)
			view := &LedgerView{ls: ls}
			for _, slot := range []uint64{
				tc.epoch.StartSlot,
				tc.noReturn - 1,
				tc.noReturn,
				tc.epoch.StartSlot + uint64(tc.epoch.LengthInSlots) - 1,
			} {
				epoch, noReturn, err := view.ProtocolParameterUpdateWindow(slot)
				require.NoError(t, err, "slot %d", slot)
				require.Equal(t, tc.epoch.EpochId, epoch, "slot %d", slot)
				require.Equal(t, tc.noReturn, noReturn, "slot %d", slot)
			}
		})
	}
}

type ppupWindowTestTx struct {
	lcommon.Transaction
	epoch   uint64
	updates map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate
	witness lcommon.TransactionWitnessSet
}

func (tx ppupWindowTestTx) ProtocolParameterUpdates() (
	uint64,
	map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate,
) {
	return tx.epoch, tx.updates
}

func (tx ppupWindowTestTx) Witnesses() lcommon.TransactionWitnessSet {
	return tx.witness
}

type ppupWindowTestWitnessSet struct {
	lcommon.TransactionWitnessSet
	vkeys []lcommon.VkeyWitness
}

func (w ppupWindowTestWitnessSet) Vkey() []lcommon.VkeyWitness {
	return w.vkeys
}

// ppupWindowRuleState takes the voting window from the real LedgerView and
// stubs only the genesis-delegate lookup, which reads the metadata store.
type ppupWindowRuleState struct {
	*LedgerView
	genesisKey lcommon.Blake2b224
	delegate   lcommon.Blake2b224
}

func (s ppupWindowRuleState) GenesisDelegateForGenesisKey(
	genesisKey lcommon.Blake2b224,
	_ uint64,
) (lcommon.Blake2b224, bool, error) {
	if genesisKey != s.genesisKey {
		return lcommon.Blake2b224{}, false, nil
	}
	return s.delegate, true, nil
}

func ppupWindowValidator(
	t *testing.T,
	descriptors []lcommon.UtxoValidationRuleDescriptor,
) lcommon.UtxoValidationRuleFunc {
	t.Helper()
	for _, descriptor := range descriptors {
		if descriptor.Id == lcommon.UtxoValidationRuleProtocolParameterUpdates {
			return descriptor.Validator
		}
	}
	t.Fatal("classic protocol parameter update rule is not registered")
	return nil
}

// TestClassicPPUPWindowBoundariesThroughEraRules drives each Shelley-family
// era's registered PPUP rule with the LedgerView window on both sides of
// every boundary of the first mainnet Shelley epoch.
func TestClassicPPUPWindowBoundariesThroughEraRules(t *testing.T) {
	t.Parallel()
	const (
		epoch     = uint64(208)
		start     = uint64(4_492_800)
		length    = uint64(432_000)
		noReturn  = start + length - 259_200
		nextStart = start + length
	)
	ls := newPPUPWindowLedgerState(
		t,
		2160,
		big.NewRat(1, 20),
		[]models.Epoch{
			{EpochId: epoch, StartSlot: start, LengthInSlots: uint(length)},
			{
				EpochId:       epoch + 1,
				StartSlot:     nextStart,
				LengthInSlots: uint(length),
			},
		},
	)
	vkey := bytes.Repeat([]byte{7}, 32)
	state := ppupWindowRuleState{
		LedgerView: &LedgerView{ls: ls},
		genesisKey: lcommon.Blake2b224Hash(bytes.Repeat([]byte{6}, 32)),
		delegate:   lcommon.Blake2b224Hash(vkey),
	}
	witness := ppupWindowTestWitnessSet{
		vkeys: []lcommon.VkeyWitness{{Vkey: vkey}},
	}
	eraCases := []struct {
		name        string
		descriptors func() []lcommon.UtxoValidationRuleDescriptor
		update      lcommon.ProtocolParameterUpdate
		pparams     lcommon.ProtocolParameters
	}{
		{
			"Shelley",
			shelley.UtxoValidationRuleDescriptors,
			shelley.ShelleyProtocolParameterUpdate{},
			&shelley.ShelleyProtocolParameters{},
		},
		{
			"Allegra",
			allegra.UtxoValidationRuleDescriptors,
			allegra.AllegraProtocolParameterUpdate{},
			&allegra.AllegraProtocolParameters{},
		},
		{
			"Mary",
			mary.UtxoValidationRuleDescriptors,
			mary.MaryProtocolParameterUpdate{},
			&mary.MaryProtocolParameters{},
		},
		{
			"Alonzo",
			alonzo.UtxoValidationRuleDescriptors,
			alonzo.AlonzoProtocolParameterUpdate{},
			&alonzo.AlonzoProtocolParameters{},
		},
		{
			"Babbage",
			babbage.UtxoValidationRuleDescriptors,
			babbage.BabbageProtocolParameterUpdate{},
			&babbage.BabbageProtocolParameters{},
		},
	}
	slots := []struct {
		name         string
		slot         uint64
		currentEpoch uint64
		forNext      bool
	}{
		{"first slot", start, epoch, false},
		{"last slot before no return", noReturn - 1, epoch, false},
		{"slot of no return", noReturn, epoch, true},
		{"last slot of epoch", nextStart - 1, epoch, true},
		{"first slot of next epoch", nextStart, epoch + 1, false},
	}
	for _, era := range eraCases {
		validate := ppupWindowValidator(t, era.descriptors())
		for _, sc := range slots {
			expected := sc.currentEpoch
			if sc.forNext {
				expected++
			}
			newTx := func(target uint64) ppupWindowTestTx {
				return ppupWindowTestTx{
					epoch: target,
					updates: map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate{
						state.genesisKey: era.update,
					},
					witness: witness,
				}
			}
			t.Run(era.name+"/"+sc.name, func(t *testing.T) {
				t.Parallel()
				require.NoError(
					t,
					validate(newTx(expected), sc.slot, state, era.pparams),
				)
				wrong := expected - 1
				if !sc.forNext {
					wrong = expected + 1
				}
				var epochErr lcommon.ProtocolParameterUpdateEpochError
				require.ErrorAs(
					t,
					validate(newTx(wrong), sc.slot, state, era.pparams),
					&epochErr,
				)
				require.Equal(t, sc.currentEpoch, epochErr.Current)
				require.Equal(t, expected, epochErr.Expected)
				require.Equal(t, sc.forNext, epochErr.ForNextEpoch)
			})
		}
	}
}

// TestProtocolParamsForSlot_ForecastsBumpAtBoundarySlot is the deterministic
// mechanism behind ConsensusAtEachFork's Allegra 1-slot drift in the eras
// DevNet (dingo observes Allegra at slot 75; cardano-producer observes it
// at slot 76).
//
// After d8d01df ("ensure era transitions bump protocol versions") the
// forger reads pparams via ProtocolParamsForSlot, which projects the
// active era forward through any TestXHardForkAtEpoch override. So if a
// dingo node is leader for the boundary slot (slot 75 == start of epoch
// 1) under a config where Allegra is scheduled at epoch 1, it forges in
// Allegra even though the in-memory ledger state still reads Shelley —
// the boundary-crossing block is itself the trigger. Its sole-producer
// rationale is sound (otherwise a single-producer network never crosses
// the fork at all), but it makes the boundary slot's era kind depend on
// who happens to be leader for that slot:
//
//   - dingo leader at slot 75: dingo forges Allegra at slot 75. Its own
//     chain therefore observes "first Allegra block" at slot 75.
//   - cardano-producer leader at slot 75: cardano-node — which does not
//     forecast pparams across a scheduled fork the same way — produces a
//     Shelley boundary block, and the next leader slot in epoch 1 is
//     where its chain first sees an Allegra block.
//
// Whichever node loses the boundary-slot leader election has its first
// Allegra observation pushed to the next leader slot in epoch 1. With
// VRF leader randomness on a small test pool, the cardano-producer side
// of that race is ~1 slot late on average, exactly the drift we observe.
// This test proves the dingo half of the mechanism: at the boundary
// slot, ProtocolParamsForSlot returns Allegra pparams; one slot earlier
// it still returns Shelley pparams.
func TestProtocolParamsForSlot_ForecastsBumpAtBoundarySlot(t *testing.T) {
	t.Parallel()

	cfg := newAllegraAtEpoch1Cfg(t)

	// Concrete Shelley pparams as if we were mid-epoch-0 with the
	// genesis protocol version. major=2 ⇒ Shelley.
	pparams := &shelley.ShelleyProtocolParameters{
		ProtocolMajor: 2,
		ProtocolMinor: 0,
	}

	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId:       0,
			StartSlot:     0,
			LengthInSlots: 75,
			SlotLength:    1000,
			EraId:         eras.ShelleyEraDesc.Id,
		},
		currentPParams: pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	// Slot 74: last slot of epoch 0. Still Shelley by every measure —
	// the schedule's trigger fires AT epoch 1, not before.
	got74 := ls.ProtocolParamsForSlot(74)
	got74Major := got74.(*shelley.ShelleyProtocolParameters).ProtocolMajor
	require.Equalf(
		t,
		uint(2),
		got74Major,
		"slot 74 (last slot of epoch 0) must still report "+
			"Shelley pparams (major=2); got major=%d",
		got74Major,
	)

	// Slot 75: first slot of epoch 1. With Allegra scheduled at
	// epoch 1, ProtocolParamsForSlot walks Shelley.NextEraTrigger=
	// AtEpoch(1) ≤ slotEpoch(1) and applies AllegraEraDesc.HardForkFunc,
	// returning Allegra pparams (major=3). The forger sees major=3,
	// extractPParamsLimits selects the Allegra block layout, and the
	// boundary-slot block is forged as Allegra.
	got75 := ls.ProtocolParamsForSlot(75)
	got75Major := got75.(*shelley.ShelleyProtocolParameters).ProtocolMajor
	require.Equalf(
		t,
		uint(3),
		got75Major,
		"slot 75 (first slot of epoch 1, scheduled Allegra fork) "+
			"must report Allegra pparams (major=3); got major=%d. "+
			"This is the proximate cause of the Allegra drift in "+
			"ConsensusAtEachFork: any node that forges this slot "+
			"will produce an Allegra block at it.",
		got75Major,
	)
}

// TestProtocolParamsForSlot_UsesMultiEraEpochs ensures the target epoch is
// resolved from the complete era history. Dividing an absolute slot by the
// current era's epoch length loses the epochs occupied by a Byron prefix and
// can therefore miss a scheduled fork at the first future Shelley boundary.
func TestProtocolParamsForSlot_UsesMultiEraEpochs(t *testing.T) {
	t.Parallel()

	const (
		byronEpochs       = 2
		byronEpochLength  = uint(100)
		shelleyEpoch      = uint64(207)
		shelleyEpochLen   = uint(432)
		byronEndSlot      = uint64(byronEpochs) * uint64(byronEpochLength)
		currentEpochStart = byronEndSlot + (shelleyEpoch-2)*uint64(
			shelleyEpochLen,
		)
		boundarySlot = currentEpochStart + uint64(shelleyEpochLen)
	)

	cfg := newMultiEraForecastCfg(t, shelleyEpoch+1)
	epochCache := make([]models.Epoch, 0, int(shelleyEpoch)+1)
	for epoch := range uint64(byronEpochs) {
		epochCache = append(epochCache, models.Epoch{
			EpochId:       epoch,
			StartSlot:     epoch * uint64(byronEpochLength),
			SlotLength:    20_000,
			LengthInSlots: byronEpochLength,
			EraId:         eras.ByronEraDesc.Id,
		})
	}
	for epoch := uint64(2); epoch <= shelleyEpoch; epoch++ {
		epochCache = append(epochCache, models.Epoch{
			EpochId:       epoch,
			StartSlot:     byronEndSlot + (epoch-2)*uint64(shelleyEpochLen),
			SlotLength:    1_000,
			LengthInSlots: shelleyEpochLen,
			EraId:         eras.ShelleyEraDesc.Id,
		})
	}

	ls := &LedgerState{
		epochCache:   epochCache,
		currentEra:   eras.ShelleyEraDesc,
		currentEpoch: epochCache[len(epochCache)-1],
		currentTip: ochainsync.Tip{Point: ocommon.NewPoint(
			boundarySlot-1, []byte("tip"),
		)},
		currentPParams: &shelley.ShelleyProtocolParameters{
			ProtocolMajor: 2,
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	got := ls.ProtocolParamsForSlot(boundarySlot)
	gotShelley, ok := got.(*shelley.ShelleyProtocolParameters)
	require.True(t, ok)
	require.Equal(
		t,
		uint(3),
		gotShelley.ProtocolMajor,
		"the first Shelley slot after a Byron prefix must forecast the scheduled fork",
	)
}

// TestProtocolParamsForSlot_FallbackProjectsFromCurrentEpoch verifies the
// bounded-summary error path. A slot beyond the forecast horizon still needs
// an epoch estimate, and that estimate must retain the absolute epoch offset
// introduced by earlier eras.
func TestProtocolParamsForSlot_FallbackProjectsFromCurrentEpoch(t *testing.T) {
	t.Parallel()

	const (
		byronEpochs      = uint64(2)
		byronEpochLength = uint64(100)
		currentEpoch     = uint64(207)
		shelleyEpochLen  = uint64(432)
	)
	currentStart := byronEpochs*byronEpochLength +
		(currentEpoch-byronEpochs)*shelleyEpochLen
	targetEpoch := currentEpoch + 100
	targetSlot := currentStart + (targetEpoch-currentEpoch)*shelleyEpochLen
	cfg := newMultiEraForecastCfg(t, targetEpoch)

	ls := &LedgerState{
		epochCache: []models.Epoch{
			{EpochId: 0, StartSlot: 0, SlotLength: 20_000,
				LengthInSlots: uint(
					byronEpochLength,
				), EraId: eras.ByronEraDesc.Id},
			{EpochId: 1, StartSlot: byronEpochLength, SlotLength: 20_000,
				LengthInSlots: uint(
					byronEpochLength,
				), EraId: eras.ByronEraDesc.Id},
			{EpochId: currentEpoch, StartSlot: currentStart, SlotLength: 1_000,
				LengthInSlots: uint(
					shelleyEpochLen,
				), EraId: eras.ShelleyEraDesc.Id},
		},
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId: currentEpoch, StartSlot: currentStart,
			SlotLength: 1_000, LengthInSlots: uint(shelleyEpochLen),
			EraId: eras.ShelleyEraDesc.Id,
		},
		currentTip: ochainsync.Tip{Point: ocommon.NewPoint(
			currentStart-1, []byte("tip"),
		)},
		currentPParams: &shelley.ShelleyProtocolParameters{ProtocolMajor: 2},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()
	_, err := ls.SlotToEpoch(targetSlot)
	require.ErrorIs(t, err, hardfork.ErrPastHorizon,
		"the target must exercise the bounded-summary fallback")

	got, ok := ls.ProtocolParamsForSlot(targetSlot).(*shelley.ShelleyProtocolParameters)
	require.True(t, ok)
	require.Equal(t, uint(3), got.ProtocolMajor,
		"fallback epoch projection must preserve the Byron epoch offset")
}

func newMultiEraForecastCfg(
	t *testing.T,
	forkEpoch uint64,
) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, loadByronGenesisForTest(t, cfg, strings.NewReader(`{
		"blockVersionData": { "slotDuration": "20000" },
		"protocolConsts": { "k": 1 }
	}`)))
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.4,
		"securityParam": 1,
		"slotLength": 1,
		"epochLength": 432,
		"systemStart": "2026-01-01T00:00:00Z"
	}`)))
	enabled := true
	cfg.ExperimentalHardForksEnabled = &enabled
	cfg.TestAllegraHardForkAtEpoch = &forkEpoch
	return cfg
}

// TestProtocolParamsForSlot_ForecastsPendingPParamUpdateAtNormalBoundary is
// the normal-boundary counterpart of the era-fork forecast test above, and
// the regression guard for issue #3061. Preview launches federated (Shelley
// genesis decentralisationParam = 1) and drops decentralization below 1 at
// the epoch 1->2 boundary through an ordinary on-chain protocol-parameter
// update, not an era hard fork. Before the fix, ProtocolParamsForSlot
// forecast future-epoch params by applying only era HardForkFuncs, so it
// returned the pre-boundary d = 1 for the next epoch. The genesis-overlay
// check then classified the first Praos block of the new epoch (on an
// irregular slot) as a non-active overlay slot and rejected it, deadlocking
// a from-genesis sync at the boundary: entering the new epoch requires
// accepting that block, which requires the post-update d, which only became
// available after entering the epoch.
//
// The pending update was proposed by a transaction the node already applied,
// so it is in ledger state (a PParamUpdate row keyed to the target epoch)
// before the rollover ticks into that epoch. ProtocolParamsForSlot now
// applies it in the forecast, mirroring the rollover's enactment, so the
// next epoch's slots see the lowered d without the row being persisted yet.
func TestProtocolParamsForSlot_ForecastsPendingPParamUpdateAtNormalBoundary(
	t *testing.T,
) {
	t.Parallel()

	cfg := newShelleyUpdateQuorum1Cfg(t)

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	// Seed a pending pparam-update proposal submitted in epoch 0 that lowers
	// decentralization from 1 to 1/2, from a single genesis-key delegate
	// (shelley genesis updateQuorum = 1). Per the Shelley update system a
	// proposal carries its submission epoch (0) and is enacted as epoch 1's
	// parameters at the epoch 0->1 boundary.
	updateCbor, err := cbor.Encode(map[uint64]any{
		12: cbor.Rat{Rat: big.NewRat(1, 2)},
	})
	require.NoError(t, err)
	require.NoError(t, db.SetPParamUpdate(
		[]byte{0xaa}, // genesis key delegate hash
		updateCbor,
		50, // slot within epoch 0 (the submission epoch)
		0,  // submission epoch (enacted for epoch 1 at the 0->1 boundary)
		nil,
	))

	// Concrete Shelley pparams as if mid-epoch-0, fully federated (d = 1).
	pparams := &shelley.ShelleyProtocolParameters{
		ProtocolMajor:    2,
		Decentralization: &cbor.Rat{Rat: big.NewRat(1, 1)},
		// Block sizes the votedFuturePParams guard accepts.
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
	}

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId:       0,
			StartSlot:     0,
			LengthInSlots: 100,
			SlotLength:    1000,
			EraId:         eras.ShelleyEraDesc.Id,
		},
		currentPParams: pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	// Slot 50: still in epoch 0 (the current epoch). The forecast returns
	// the current params unchanged, so d is still 1.
	got50 := ls.ProtocolParamsForSlot(50)
	d50 := got50.(*shelley.ShelleyProtocolParameters).Decentralization
	require.NotNil(t, d50)
	require.Equalf(
		t,
		0,
		d50.Cmp(big.NewRat(1, 1)),
		"slot 50 (epoch 0, current epoch) must still report d=1; got %s",
		d50.RatString(),
	)

	// Slot 150: first-epoch-ahead slot (epoch 1). The pending update
	// enacted at the epoch 0->1 boundary lowers d to 1/2, and the forecast
	// must reflect it BEFORE the ledger has ticked into epoch 1.
	got150 := ls.ProtocolParamsForSlot(150)
	d150 := got150.(*shelley.ShelleyProtocolParameters).Decentralization
	require.NotNil(t, d150)
	require.Equalf(
		t,
		0,
		d150.Cmp(big.NewRat(1, 2)),
		"slot 150 (epoch 1) must report the forecast-lowered d=1/2 from "+
			"the pending pparam update; got %s. A stale d=1 here is the "+
			"#3061 overlay-rejection deadlock.",
		d150.RatString(),
	)

	// The forecast is pure: it must not mutate the shared snapshot's
	// current params (era update functions mutate their pointer in place).
	snapD := ls.GetCurrentPParams().(*shelley.ShelleyProtocolParameters).
		Decentralization
	require.NotNil(t, snapD)
	require.Equalf(
		t,
		0,
		snapD.Cmp(big.NewRat(1, 1)),
		"forecast must not mutate snapshot currentPParams; d is now %s",
		snapD.RatString(),
	)
}

// newShelleyUpdateQuorum1Cfg builds a CardanoNodeConfig whose era transitions
// are all version-gated (no scheduled TriggerAtEpoch fork), so the era-fork
// forecast walk is a no-op and the pending-pparam-update forecast is exercised
// in isolation. updateQuorum = 1 lets a single genesis-key proposal enact.
func newShelleyUpdateQuorum1Cfg(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, loadByronGenesisForTest(t, cfg, strings.NewReader(`{
		"protocolConsts": {
			"k": 6,
			"protocolMagic": 42
		}
	}`)))
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"systemStart": "2026-01-01T00:00:00Z",
		"securityParam": 6,
		"activeSlotsCoeff": 0.05,
		"epochLength": 100,
		"slotLength": 1,
		"updateQuorum": 1
	}`)))
	return cfg
}

// newAllegraAtEpoch1Cfg builds a CardanoNodeConfig that mirrors the eras
// DevNet's testnet.yaml as far as the era-shape forecast is concerned:
// experimental hard forks are enabled and Allegra is scheduled at epoch 1
// (slot 75 with epochLength=75). All other forks are left as AtVersion so
// the forecast walks at most one step.
func newAllegraAtEpoch1Cfg(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, loadByronGenesisForTest(t, cfg, strings.NewReader(`{
		"protocolConsts": {
			"k": 6,
			"protocolMagic": 42
		}
	}`)))
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"systemStart": "2026-01-01T00:00:00Z",
		"securityParam": 6,
		"activeSlotsCoeff": 0.4,
		"epochLength": 75,
		"slotLength": 1
	}`)))
	enabled := true
	allegraEpoch := uint64(1)
	cfg.ExperimentalHardForksEnabled = &enabled
	cfg.TestAllegraHardForkAtEpoch = &allegraEpoch
	return cfg
}

// TestProtocolParamsForSlot_ConcurrentPostForkCallsDoNotRaceOnCostModels
// guards against a concurrent map write crash, not just a -race warning.
// ProtocolParamsForSlot forecasts across a scheduled fork by calling
// HardForkFunc directly on the published snapshot's currentPParams; if a
// HardForkFunc wrapper shares its input's CostModels map instead of cloning
// it (the shape gouroboros's UpgradePParams produces — it copies the
// pparams struct but not the map), concurrent forecasts for the same
// post-fork slot become concurrent writes into that one shared map, which
// Go's runtime terminates the process for rather than reporting as a
// data race. HardForkBabbage (and Conway/Dijkstra) must clone CostModels
// before writing to it for this to be safe.
func TestProtocolParamsForSlot_ConcurrentPostForkCallsDoNotRaceOnCostModels(
	t *testing.T,
) {
	t.Parallel()

	cfg := newAlonzoBabbageAtEpoch1Cfg(t)

	pparams := &alonzo.AlonzoProtocolParameters{
		ProtocolMajor: eras.AlonzoEraDesc.MaxMajorVersion,
		CostModels: map[uint][]int64{
			0: {1, 2, 3},
		},
	}

	ls := &LedgerState{
		currentEra: eras.AlonzoEraDesc,
		currentEpoch: models.Epoch{
			EpochId:       0,
			StartSlot:     0,
			LengthInSlots: 75,
			SlotLength:    1000,
			EraId:         eras.AlonzoEraDesc.Id,
		},
		currentPParams: pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	const goroutines = 16
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			got := ls.ProtocolParamsForSlot(75)
			babbagePParams, ok := got.(*babbage.BabbageProtocolParameters)
			require.True(t, ok)
			require.NotEmpty(t, babbagePParams.CostModels)
		})
	}
	wg.Wait()
}

// newAlonzoBabbageAtEpoch1Cfg schedules Babbage at epoch 1 (slot 75 with
// epochLength=75), mirroring newAllegraAtEpoch1Cfg's shape but for the
// CostModels-bearing Alonzo->Babbage transition.
func newAlonzoBabbageAtEpoch1Cfg(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, loadByronGenesisForTest(t, cfg, strings.NewReader(`{
		"protocolConsts": {
			"k": 6,
			"protocolMagic": 42
		}
	}`)))
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"systemStart": "2026-01-01T00:00:00Z",
		"securityParam": 6,
		"activeSlotsCoeff": 0.4,
		"epochLength": 75,
		"slotLength": 1
	}`)))
	enabled := true
	babbageEpoch := uint64(1)
	cfg.ExperimentalHardForksEnabled = &enabled
	cfg.TestBabbageHardForkAtEpoch = &babbageEpoch
	return cfg
}

// guardedMutexes are the LedgerState locks that an EventBus subscriber
// handler acquires. Publishing while holding one of these can deadlock the
// node, so no function may do both.
//
// Both are listed because RecoverAfterLocalRollback -- the subscriber that
// closes the cycle -- takes chainsyncMutex and then nests
// chainsyncBlockfetchMutex inside it via startQueuedBlockfetchLocked.
// Holding either one while publishing is therefore enough to deadlock;
// guarding only the outer mutex would miss every path that runs under the
// blockfetch lock alone, such as handleEventBlockfetch's.
var guardedMutexes = []string{
	"chainsyncMutex",
	"chainsyncBlockfetchMutex",
}

// inlinePublishingChainMethods are the exported chain.Chain methods that
// publish to the EventBus inline (chain/chain.go): the Add* paths emit
// ChainUpdateEventType and Rollback emits the rollback/fork events. Calling
// one of these as ls.chain.<method> while holding a guarded mutex is the same
// deadlock as publishing directly -- the event's subscriber can need the
// mutex -- so this scan treats an ls.chain.<method> call as a publish.
//
// AddBlockWithPoint is on the list even though no lock holder calls it today
// (its one production caller, flushPendingBlockfetchBlocks, runs unlocked):
// it still publishes inline, so guarding it now stops a future lock holder
// from silently reopening the cycle. Keep this in sync with the
// c.eventBus.Publish call sites in chain/chain.go.
var inlinePublishingChainMethods = []string{
	"AddBlock",
	"AddLocalBlock",
	"AddBlockWithPoint",
	"AddBlocks",
	"AddRawBlocks",
	"AddRawBlocksWithCallback",
	"Rollback",
}

// callGraphInlineChainPublishers returns the set of functions that reach an
// inline-publishing chain method (inlinePublishingChainMethods) either by
// calling it directly as ls.chain.<method> or through any chain of ls.<helper>
// calls. It is the transitive closure the intra-procedural ls.chain.<method>
// guard in violations cannot see on its own: a lock holder that reaches
// ls.chain.Rollback through a helper -- rollbackPrimaryChainInSecurityParamWindows
// in replay_recovery.go, say -- is just as deadlock-prone as one that calls it
// directly, since the helper runs inside the caller's held lock.
//
// It mirrors the ChainsyncResyncEventType closure in
// TestChainsyncResyncPublishPathsUnderLock, but ranges over the whole package
// rather than chainsync.go alone, because the chain-method publishers and the
// recovery helpers that reach them are spread across several files. The call
// graph follows ls.<method> receivers only, matching the rest of this file;
// a publish reached through a stored func value or a non-ls receiver is
// outside its fidelity, exactly as it is for the resync scan.
func callGraphInlineChainPublishers(files []*ast.File) map[string]bool {
	directPublisher := map[string]bool{}
	callees := map[string]map[string]bool{}
	var order []string
	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			name := fn.Name.Name
			if _, seen := callees[name]; !seen {
				order = append(order, name)
				callees[name] = map[string]bool{}
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				// ls.chain.<inlineMethod>(...): a direct inline publish.
				if inner, ok := sel.X.(*ast.SelectorExpr); ok {
					if id, ok := inner.X.(*ast.Ident); ok &&
						id.Name == "ls" && inner.Sel.Name == "chain" &&
						slices.Contains(
							inlinePublishingChainMethods, sel.Sel.Name,
						) {
						directPublisher[name] = true
					}
				}
				// ls.<method>(...): an intra-package call edge.
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "ls" {
					callees[name][sel.Sel.Name] = true
				}
				return true
			})
		}
	}

	reaches := map[string]bool{}
	for name := range directPublisher {
		reaches[name] = true
	}
	// Fixed point: |order| passes suffice, one edge relaxed per pass.
	for range order {
		for name, cs := range callees {
			if reaches[name] {
				continue
			}
			for c := range cs {
				if reaches[c] {
					reaches[name] = true
					break
				}
			}
		}
	}
	return reaches
}

// knownNilQueuePublishersUnderLock is intentionally empty. A guarded caller
// must always pass its pending queue; a nil queue would publish inline.
//
// The ledger.tx undo emit reached from rollbackChainAndStateDeferred is covered by
// neither test, and deliberately so. This scan is intra-procedural and that
// path holds the lock and the publish in different functions, so it does not
// match here; TestChainsyncResyncPublishPathsUnderLock does not match it
// either, since that one parses only chainsync.go and only fires on
// ChainsyncResyncEventType. The path is a documented exception rather than a
// checked one -- see the ledger.tx section of ARCHITECTURE.md for why it has
// to publish under chainsyncMutex and what that requires of subscribers.
var knownNilQueuePublishersUnderLock []string

// TestNoEventBusPublishWhileHoldingChainsyncMutex enforces that nothing in
// this package publishes to the EventBus while holding a mutex that an
// EventBus subscriber needs.
//
// EventBus delivery blocks when a subscriber's buffer is full — deliberate,
// since the bus backpressures rather than dropping events — and
// ChainsyncResyncEventType's subscriber calls RecoverAfterLocalRollback,
// which takes chainsyncMutex. Publish under that lock and the two can wait
// on each other forever: the subscriber wants the lock the publisher holds,
// the publisher wants the buffer capacity the subscriber would free.
//
// It does not stay contained. handleConnectionClosedEvent takes the same
// mutex, so ledger.conn_closed stops draining; node.go's handler
// translating connmanager.conn_closed into ledger.conn_closed then blocks
// inside its own callback, which stops connmanager.conn_closed draining,
// and every subsequent connection close parks another publisher goroutine.
// A DevNet run reproduced exactly that: ~217k "event delivery stalled:
// subscriber not draining type=connmanager.conn_closed" warnings in five
// minutes, the mempool component silent, and the node still forging but no
// longer completing Node-to-Node handshakes.
//
// This covers Publish, PublishBlocking and PublishAsync alike. All three
// wait for capacity rather than dropping, so all three can close the
// cycle; PublishAsync merely does it through the shared async queue and
// its worker pool instead of a single subscriber's buffer.
//
// Queue the event with pendingPublishes and flush it after the unlock
// instead.
//
// The direct EventBus.Publish check is intra-procedural, which is not the
// whole story: a lock holder can also reach such a publish through a helper.
// Those paths are enumerated by TestChainsyncResyncPublishPathsUnderLock
// rather than left to be assumed safe.
//
// The ls.chain.<method> check gets the same transitive treatment here, and
// for the same reason: a lock holder that never names an inline-publishing
// chain method itself but reaches one through a helper (e.g. ls.chain.Rollback
// via rollbackPrimaryChainInSecurityParamWindows) is still holding the lock
// across the publish. callGraphInlineChainPublishers computes that closure and
// violations treats a call to any member as a publish, so source order still
// decides whether the lock is actually held at the call site.
func TestNoEventBusPublishWhileHoldingChainsyncMutex(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	fset := token.NewFileSet()
	var files []*ast.File
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") ||
			strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(
			fset, filepath.Join(".", name), nil, parser.ParseComments,
		)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		files = append(files, file)
	}

	queueParam := queueParamPositions(files)
	require.NotEmpty(t, queueParam,
		"no *pendingPublishes parameter found anywhere in the package;"+
			" the nil-queue check would silently pass on everything")

	transitivePublishers := callGraphInlineChainPublishers(files)
	require.NotEmpty(t, transitivePublishers,
		"no function reaches an inline-publishing chain method; the"+
			" transitive ls.chain.<method> guard would pass on everything")

	checked := 0
	seenKnown := map[string]bool{}
	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			fn, ok := n.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				return true
			}
			checked++
			for _, v := range violations(fn, queueParam, transitivePublishers) {
				if slices.Contains(
					knownNilQueuePublishersUnderLock, fn.Name.Name,
				) {
					seenKnown[fn.Name.Name] = true
					continue
				}
				t.Errorf(
					"%s: %s publishes to the EventBus while holding %s;"+
						" queue it with pendingPublishes and flush after"+
						" the unlock (see pending_publish.go)",
					fset.Position(v.pos), fn.Name.Name, v.mutex,
				)
			}
			return true
		})
	}
	if checked == 0 {
		t.Fatal("no functions inspected; the scan is not working")
	}
	// Bidirectional, like the transitive guard: an entry that no longer
	// violates has been fixed and must be removed, or the list quietly
	// starts excusing something that is already clean.
	for _, name := range knownNilQueuePublishersUnderLock {
		require.True(t, seenKnown[name],
			"%s no longer publishes under a guarded mutex; remove it from"+
				" knownNilQueuePublishersUnderLock", name)
	}
}

// queueParamPositions maps each function to the argument position of its
// *pendingPublishes parameter, if it has one.
//
// Collected across every file before any body is walked: a call can name a
// helper declared later, or in another file, and a lookup that missed
// would silently stop treating a nil queue as a publish.
func queueParamPositions(files []*ast.File) map[string]int {
	out := map[string]int{}
	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Type.Params == nil {
				continue
			}
			idx := 0
			for _, field := range fn.Type.Params.List {
				isQueue := false
				queueType := field.Type
				if ellipsis, ok := queueType.(*ast.Ellipsis); ok {
					queueType = ellipsis.Elt
				}
				if star, ok := queueType.(*ast.StarExpr); ok {
					if id, ok := star.X.(*ast.Ident); ok &&
						id.Name == "pendingPublishes" {
						isQueue = true
					}
				}
				names := max(len(field.Names), 1)
				if isQueue {
					out[fn.Name.Name] = idx
					break
				}
				idx += names
			}
		}
	}
	return out
}

// nilQueueCall reports whether a call hands nil to a queue-taking
// helper's queue parameter, which makes that helper publish immediately
// rather than queueing -- so the caller owns the publish.
func nilQueueCall(
	call *ast.CallExpr,
	sel *ast.SelectorExpr,
	queueParam map[string]int,
) bool {
	ident, ok := sel.X.(*ast.Ident)
	if !ok || ident.Name != "ls" {
		return false
	}
	pos, isQueued := queueParam[sel.Sel.Name]
	if !isQueued || pos >= len(call.Args) {
		return false
	}
	id, ok := call.Args[pos].(*ast.Ident)
	return ok && id.Name == "nil"
}

type violation struct {
	pos   token.Pos
	mutex string
}

type lockEvent struct {
	pos   token.Pos
	kind  string // "lock", "unlock", "deferUnlock", "publish"
	mutex string
}

// violations walks a function in source order, tracking which guarded
// mutexes are held, and reports publishes made while one is.
//
// Source order is a good enough model of control flow for this code: these
// functions either hold a mutex for their whole body via defer, or take and
// release it around a specific region. A deferred unlock keeps the mutex
// held to the end of the function, which is what makes a publish anywhere
// after the Lock unsafe.
func violations(
	fn *ast.FuncDecl,
	queueParam map[string]int,
	transitivePublishers map[string]bool,
) []violation {
	deferred := map[token.Pos]bool{}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if d, ok := n.(*ast.DeferStmt); ok && d.Call != nil {
			deferred[d.Call.Pos()] = true
		}
		return true
	})

	var events []lockEvent
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		// Checked before the two-level assertion below: a nil-queue call
		// is ls.method(...), whose receiver is a plain identifier, not a
		// selector like ls.config.EventBus.
		if nilQueueCall(call, sel, queueParam) {
			events = append(events, lockEvent{
				pos: call.Pos(), kind: "publish",
			})
			return true
		}
		// ls.<helper>(...) where the helper reaches an inline-publishing
		// chain method directly or transitively (see
		// callGraphInlineChainPublishers). This is the transitive extension of
		// the ls.chain.<method> case below: a direct ls.chain.Rollback is
		// caught there, a call that only reaches the publish through a helper
		// is caught here. It is emitted at the call site, so the same source
		// order tracking decides whether a guarded mutex is actually held --
		// a helper call made before the Lock, like
		// rejectRecoveryAtMithrilBoundary's rollback, is correctly not a
		// violation. The receiver is a plain ls identifier, so this is
		// checked before the ls.<x>.<y> assertion below, like nilQueueCall.
		if id, ok := sel.X.(*ast.Ident); ok && id.Name == "ls" &&
			transitivePublishers[sel.Sel.Name] {
			events = append(events, lockEvent{
				pos: call.Pos(), kind: "publish",
			})
			return true
		}
		inner, ok := sel.X.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch sel.Sel.Name {
		case "Lock", "Unlock":
			if !slices.Contains(guardedMutexes, inner.Sel.Name) {
				return true
			}
			kind := "lock"
			if sel.Sel.Name == "Unlock" {
				kind = "unlock"
				if deferred[call.Pos()] {
					kind = "deferUnlock"
				}
			}
			events = append(events, lockEvent{
				pos: call.Pos(), kind: kind, mutex: inner.Sel.Name,
			})
		case "Publish", "PublishBlocking", "PublishAsync",
			"PublishOrdered", "PublishOrderedContext":
			// Only EventBus publishes; other types have Publish methods.
			// PublishAsync is included: it does not park on a
			// subscriber's buffer, but it does wait for room in the
			// shared async queue rather than dropping the event, and that
			// queue is drained by a worker pool whose workers run
			// subscriber handlers. A handler that needs the publisher's
			// mutex parks a worker, the queue fills, and the same cycle
			// closes. PublishOrdered and PublishOrderedContext are
			// included for the same reason against their per-event-type
			// lane.
			if inner.Sel.Name != "EventBus" {
				return true
			}
			events = append(events, lockEvent{
				pos: call.Pos(), kind: "publish",
			})
		default:
			// A publish reached indirectly through the chain layer is just as
			// unsafe as a direct EventBus.Publish. ls.chain.AddBlockWithPoint
			// and its siblings call c.eventBus.Publish inline from inside the
			// chain package (see inlinePublishingChainMethods), so holding a
			// guarded mutex across one closes the same cycle -- a subscriber
			// to ChainUpdateEventType or the rollback events that needs the
			// mutex parks, the buffer fills, the publisher never returns. The
			// direct-EventBus scan above cannot see these because the receiver
			// is ls.chain, not ...EventBus. Match only ls.chain.* so an
			// unrelated type's same-named method (a db txn's Rollback, say) is
			// left alone.
			if inner.Sel.Name != "chain" ||
				!slices.Contains(
					inlinePublishingChainMethods, sel.Sel.Name,
				) {
				return true
			}
			events = append(events, lockEvent{
				pos: call.Pos(), kind: "publish",
			})
		}
		return true
	})

	slices.SortFunc(events, func(a, b lockEvent) int {
		return int(a.pos - b.pos)
	})

	held := map[string]bool{}
	heldToEnd := map[string]bool{}
	var found []violation
	for _, ev := range events {
		switch ev.kind {
		case "lock":
			held[ev.mutex] = true
		case "unlock":
			if !heldToEnd[ev.mutex] {
				held[ev.mutex] = false
			}
		case "deferUnlock":
			// Released only when the function returns.
			heldToEnd[ev.mutex] = true
		case "publish":
			for mu, isHeld := range held {
				if isHeld {
					found = append(found, violation{pos: ev.pos, mutex: mu})
				}
			}
		}
	}
	return found
}

// knownResyncPublishPathsUnderLock is intentionally empty. Every resync
// publish reachable from a guarded lock holder must be queued and flushed
// after the lock is released.
var knownResyncPublishPathsUnderLock []string

// TestChainsyncResyncPublishPathsUnderLock pins the set of helpers that
// can publish ChainsyncResyncEventType while the mutex is held.
//
// It fails in both directions on purpose. A new such path is a new
// deadlock and must be converted rather than appended here; a path that
// disappears should be removed, so the list cannot rot into overstating
// the problem.
func TestChainsyncResyncPublishPathsUnderLock(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(
		fset, filepath.Join(".", "chainsync.go"), nil, parser.ParseComments,
	)
	if err != nil {
		t.Fatalf("parse chainsync.go: %v", err)
	}

	publishesResync := map[string]bool{}
	queuedResync := map[string]bool{}
	callees := map[string]map[string]bool{}
	nilQueueCalls := map[string]map[string]bool{}
	holdsLock := map[string]bool{}
	var order []string

	// Shared with the intra-procedural guard so the two cannot drift: if
	// one learned to recognise a differently spelled nil or receiver and
	// the other did not, a publish path would stop being guarded silently.
	queueParam := queueParamPositions([]*ast.File{file})
	require.NotEmpty(t, queueParam,
		"no *pendingPublishes parameter found; the nil-queue check would"+
			" silently pass on everything")

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		name := fn.Name.Name
		order = append(order, name)
		callees[name] = map[string]bool{}
		nilQueueCalls[name] = map[string]bool{}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.SelectorExpr:
				if node.Sel.Name == "ChainsyncResyncEventType" {
					if usesInlinePublish(fn) {
						publishesResync[name] = true
					} else {
						// Queues instead. Safe only for callers that
						// hand it a queue -- see nilQueueCalls.
						queuedResync[name] = true
					}
				}
			case *ast.CallExpr:
				sel, ok := node.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				// A queue-taking helper called with a nil queue
				// publishes immediately, so the caller is the publisher.
				if nilQueueCall(node, sel, queueParam) {
					nilQueueCalls[name][sel.Sel.Name] = true
				}
				if inner, ok := sel.X.(*ast.SelectorExpr); ok &&
					slices.Contains(guardedMutexes, inner.Sel.Name) &&
					sel.Sel.Name == "Lock" {
					holdsLock[name] = true
				}
				if ident, ok := sel.X.(*ast.Ident); ok && ident.Name == "ls" {
					callees[name][sel.Sel.Name] = true
				}
			}
			return true
		})
	}

	// A caller that hands nil to a queue-taking resync helper makes that
	// helper publish inline, so the caller owns the publish. Without this
	// the guard silently stopped reporting every requestChainsyncResync
	// route the moment that helper was converted to take a queue.
	for caller, targets := range nilQueueCalls {
		for target := range targets {
			if queuedResync[target] {
				publishesResync[caller] = true
			}
		}
	}

	// Transitive closure: anything reaching an inline resync publish.
	reaches := map[string]bool{}
	for name, ok := range publishesResync {
		if ok {
			reaches[name] = true
		}
	}
	for range order {
		for name, cs := range callees {
			if reaches[name] {
				continue
			}
			for c := range cs {
				if reaches[c] {
					reaches[name] = true
					break
				}
			}
		}
	}

	var found []string
	for _, name := range order {
		if holdsLock[name] || !reaches[name] {
			continue
		}
		// Only helpers a lock holder can actually reach.
		reachedFromLockHolder := false
		for holder, cs := range callees {
			if holdsLock[holder] && cs[name] {
				reachedFromLockHolder = true
				break
			}
		}
		if reachedFromLockHolder && !slices.Contains(found, name) {
			found = append(found, name)
		}
	}
	slices.Sort(found)

	expected := slices.Clone(knownResyncPublishPathsUnderLock)
	slices.Sort(expected)
	// Built from guardedMutexes so the message cannot drift as that list
	// grows -- it named only chainsyncMutex after the blockfetch mutex was
	// added, which would misdirect anyone hitting the failure.
	require.Equal(t, expected, found,
		"the set of helpers publishing ChainsyncResyncEventType while"+
			" holding one of %v changed. A new entry is a new deadlock:"+
			" thread a pendingPublishes queue through it instead of adding"+
			" it here. A missing entry means it was fixed -- drop it from"+
			" knownResyncPublishPathsUnderLock.",
		guardedMutexes)
}

// usesInlinePublish reports whether a function publishes directly rather
// than queueing through pendingPublishes.
func usesInlinePublish(fn *ast.FuncDecl) bool {
	inline := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if sel.Sel.Name != "Publish" && sel.Sel.Name != "PublishBlocking" &&
			sel.Sel.Name != "PublishAsync" {
			return true
		}
		if inner, ok := sel.X.(*ast.SelectorExpr); ok &&
			inner.Sel.Name == "EventBus" {
			inline = true
		}
		return true
	})
	return inline
}

// shrinkGatherCoalesceRetryInterval overrides the package-level
// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam for the
// duration of the calling test and restores it on cleanup, so these tests
// must not run in parallel with each other (see cleanup_consumed_utxos's
// shrinkCleanupConsumedUtxosInterval for the same pattern).
func shrinkGatherCoalesceRetryInterval(
	t *testing.T,
	interval time.Duration,
	attempts int,
) {
	t.Helper()
	prevInterval := gatherCoalesceRetryInterval
	prevAttempts := gatherCoalesceMaxAttempts
	gatherCoalesceRetryInterval = interval
	gatherCoalesceMaxAttempts = attempts
	t.Cleanup(func() {
		gatherCoalesceRetryInterval = prevInterval
		gatherCoalesceMaxAttempts = prevAttempts
	})
}

// scriptedGapLedgerReadIterator scripts a fixed sequence of non-blocking
// Next outcomes. A nil entry simulates the iterator momentarily having
// nothing ready (chain.ErrIteratorChainTip on a non-blocking probe) without
// the chain having actually stopped growing -- e.g. the goroutine that
// appends blocks to ls.chain is a beat behind this reader. Once the script
// is exhausted, a non-blocking call keeps returning ErrIteratorChainTip and
// a blocking call waits on ctx.Done(), matching a real iterator genuinely
// caught up to a still-open chain tip.
type scriptedGapLedgerReadIterator struct {
	ctx    context.Context
	script []*chain.ChainIteratorResult
	idx    int
}

func (s *scriptedGapLedgerReadIterator) Next(
	blocking bool,
) (*chain.ChainIteratorResult, error) {
	if s.idx < len(s.script) {
		next := s.script[s.idx]
		s.idx++
		if next == nil {
			return nil, chain.ErrIteratorChainTip
		}
		return next, nil
	}
	if !blocking {
		return nil, chain.ErrIteratorChainTip
	}
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

// TestLedgerReadChainIteratorCoalescesGapsDuringBulkReplay is a regression
// test for dingo#4464's confirmed premature-flush defect: the gather loop
// used to flush a batch the moment a non-blocking iter.Next(false) returned
// chain.ErrIteratorChainTip, even with only one block gathered and 49 more
// blocks about to arrive. On the harness that produced the issue, this
// fragmented an intended 50-block batch into ~6-9 block commits.
//
// This scripts five blocks separated by momentary gaps (including a run of
// three consecutive gaps) with no upstream tip configured, so isNearTip is
// false throughout (bulk-replay/no-known-upstream default) and the
// coalescing wait applies. Before the fix, the first gap alone flushed a
// batch of 1; after it, all five blocks land in one batch.
func TestLedgerReadChainIteratorCoalescesGapsDuringBulkReplay(t *testing.T) {
	// Not t.Parallel: swaps the package-level
	// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam via
	// shrinkGatherCoalesceRetryInterval.
	shrinkGatherCoalesceRetryInterval(t, time.Millisecond, 5)

	block1, point1 := buildDecodableTestBlock(t, 10, 1)
	block2, point2 := buildDecodableTestBlock(t, 20, 2)
	block3, point3 := buildDecodableTestBlock(t, 30, 3)
	block4, point4 := buildDecodableTestBlock(t, 40, 4)
	block5, point5 := buildDecodableTestBlock(t, 50, 5)

	script := []*chain.ChainIteratorResult{
		{Point: point1, Block: block1},
		nil,
		{Point: point2, Block: block2},
		nil,
		nil,
		{Point: point3, Block: block3},
		nil,
		{Point: point4, Block: block4},
		nil,
		nil,
		nil,
		{Point: point5, Block: block5},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	iter := &scriptedGapLedgerReadIterator{ctx: ctx, script: script}

	// Zero-value config: config.GetActiveConnectionFunc is nil and
	// syncUpstreamTipSlot defaults to 0, so UpstreamTipSlot() returns 0 and
	// isNearTip is false for every slot -- the "still catching up, or
	// upstream unknown" default (see isNearTip's doc comment).
	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}

	resultCh := make(chan readChainResult, 1)
	go ls.ledgerReadChainIterator(ctx, iter, resultCh)

	result := testutil.RequireReceive(
		t, resultCh, testutil.AsyncWait,
		"reader never delivered a batch",
	)
	require.NoError(t, result.err)
	require.False(t, result.rollback)
	require.Len(
		t,
		result.blocks,
		5,
		"gaps during bulk replay should be coalesced into one batch instead "+
			"of flushing on the very first empty non-blocking check",
	)
	close(result.done)
}

// TestLedgerReadChainIteratorNearTipFlushesSingleBlockPromptly confirms the
// coalescing wait added for dingo#4464 does not regress live tip-following
// latency: once isNearTip is true, a solitary new block must still commit
// immediately rather than wait for a batch that will never fill.
//
// The retry interval is deliberately set far larger (minutes) than the
// receive deadline (seconds) below, so this does not race a tight timing
// window against scheduler jitter: a correct implementation returns near-
// instantly regardless of load, while a regressed one that started waiting
// would still be asleep by the time the deadline below expires.
func TestLedgerReadChainIteratorNearTipFlushesSingleBlockPromptly(
	t *testing.T,
) {
	// Not t.Parallel: swaps the package-level
	// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam via
	// shrinkGatherCoalesceRetryInterval.
	shrinkGatherCoalesceRetryInterval(t, 10*time.Minute, 1)

	block, point := buildDecodableTestBlock(t, 100, 1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	iter := &scriptedGapLedgerReadIterator{
		ctx:    ctx,
		script: []*chain.ChainIteratorResult{{Point: point, Block: block}},
	}

	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	// Upstream tip at slot 1: the block's slot (100) is at or past it, so
	// isNearTip reports "caught up" (see nearUpstreamTip).
	ls.advanceUpstreamTipSlot(1)

	resultCh := make(chan readChainResult, 1)
	go ls.ledgerReadChainIterator(ctx, iter, resultCh)

	result := testutil.RequireReceive(
		t, resultCh, 10*time.Second,
		"single block at live tip did not commit promptly -- looks like it "+
			"waited on a coalescing batch that will never fill",
	)
	require.NoError(t, result.err)
	require.False(t, result.rollback)
	require.Len(t, result.blocks, 1)
	close(result.done)
}

// farUpstreamTipSlot is far enough past the single-digit slots these tests
// build blocks at to sit well outside the stability window a nil
// CardanoNodeConfig falls back to (blockfetchBatchSlotThresholdDefault,
// 50000), so isNearTip reports "still catching up" against a KNOWN upstream
// tip rather than against the unknown-upstream default.
const farUpstreamTipSlot = 1_000_000

// pausingGapLedgerReadIterator returns one block, then parks inside the Next
// call that reports the first chain-tip gap: it closes gapEntered and waits
// on resume before returning chain.ErrIteratorChainTip. Parking there leaves
// ledgerReadChainIterator holding blockPipelineGatherMutex's read lock with
// one block already gathered and holding no ledger lock, which is the only
// point from which a test can both release the reader into the coalescing
// branch and control what other locks are held when it gets there.
type pausingGapLedgerReadIterator struct {
	ctx        context.Context
	first      *chain.ChainIteratorResult
	calls      int
	gapEntered chan struct{}
	resume     chan struct{}
}

func (p *pausingGapLedgerReadIterator) Next(
	blocking bool,
) (*chain.ChainIteratorResult, error) {
	idx := p.calls
	p.calls++
	if idx == 0 {
		return p.first, nil
	}
	// Next is called serially from the reader goroutine, so indexing is a
	// deterministic way to name the first gap without a sync.Once.
	if idx == 1 {
		close(p.gapEntered)
		<-p.resume
	}
	if !blocking {
		return nil, chain.ErrIteratorChainTip
	}
	<-p.ctx.Done()
	return nil, p.ctx.Err()
}

// tryLockGatherMutex reports whether blockPipelineGatherMutex's write lock --
// the one rollbackChainAndStateDeferred takes -- is obtainable right now,
// releasing it again if it is, so it can be polled.
func tryLockGatherMutex(ls *LedgerState) bool {
	if ls.blockPipelineGatherMutex.TryLock() {
		ls.blockPipelineGatherMutex.Unlock()
		return true
	}
	return false
}

// TestLedgerReadChainIteratorHoldsGatherMutexAcrossCoalesceWait pins the
// safety property the dingo#4464 coalescing branch rests on: unlike the
// genuinely-blocking wait for a still-empty batch, the coalescing wait keeps
// blockPipelineGatherMutex's read lock held, because rawBatch already holds
// real gathered blocks a concurrent rollback must not race ahead of.
//
// TestLedgerReadChainIteratorHoldsGatherMutexAcrossGather does not reach
// here: its scripted reader pauses inside Next, never inside this wait, so
// releasing the lock across the wait leaves the whole ledger package green.
// This test closes that gap by probing for the write lock while the reader
// is inside the wait -- a release there makes the probe succeed.
func TestLedgerReadChainIteratorHoldsGatherMutexAcrossCoalesceWait(
	t *testing.T,
) {
	// Not t.Parallel: swaps the package-level
	// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam via
	// shrinkGatherCoalesceRetryInterval.
	//
	// One attempt, so the pass makes exactly one coalescing wait, and an
	// interval long enough that the probe below fits comfortably inside
	// that single wait rather than racing its end.
	const coalesceWait = 500 * time.Millisecond
	shrinkGatherCoalesceRetryInterval(t, coalesceWait, 1)

	block, point := buildDecodableTestBlock(t, 10, 1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	iter := &pausingGapLedgerReadIterator{
		ctx:        ctx,
		first:      &chain.ChainIteratorResult{Point: point, Block: block},
		gapEntered: make(chan struct{}),
		resume:     make(chan struct{}),
	}

	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	ls.advanceUpstreamTipSlot(farUpstreamTipSlot)

	resultCh := make(chan readChainResult, 1)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		ls.ledgerReadChainIterator(ctx, iter, resultCh)
	}()

	testutil.RequireReceive(
		t, iter.gapEntered, testutil.AsyncWait,
		"reader never reached the chain-tip gap that triggers coalescing",
	)
	// Releasing the iterator sends the reader straight into the coalescing
	// wait: one block is gathered, the batch is under capacity, no attempt
	// has been spent, and the tip is far away.
	close(iter.resume)

	require.Never(
		t,
		func() bool { return tryLockGatherMutex(ls) },
		coalesceWait/2,
		2*time.Millisecond,
		"blockPipelineGatherMutex.Lock() succeeded while the reader was "+
			"inside the coalescing wait holding blocks it has not yet "+
			"submitted -- a concurrent rollback would drain an empty "+
			"blockPipeline and proceed ahead of them",
	)
	// Half the wait has elapsed at most, so the batch cannot have been
	// delivered yet. A delivery here would mean the probe above ran after
	// the pass ended rather than during the wait.
	select {
	case <-resultCh:
		t.Fatal(
			"batch was delivered before the coalescing wait elapsed -- the " +
				"probe above did not cover the wait",
		)
	default:
	}

	result := testutil.RequireReceive(
		t, resultCh, testutil.AsyncWait,
		"reader never delivered its coalesced batch",
	)
	require.NoError(t, result.err)
	require.Len(t, result.blocks, 1)
	close(result.done)

	cancel()
	testutil.RequireReceive(
		t, readerDone, testutil.AsyncWait,
		"ledgerReadChainIterator did not exit after cancellation",
	)
}

// TestLedgerReadChainIteratorTakesNoLedgerLockInsideGatherSpan pins the other
// half of that bound. ARCHITECTURE.md states a worst case for how long a
// rollback blocked on blockPipelineGatherMutex waits, derived purely from
// batchSize, gatherCoalesceMaxAttempts and gatherCoalesceRetryInterval. That
// figure only holds if nothing inside the held span can block on anything
// else. ls.isNearTip reaches calculateStabilityWindow, which takes ls.RLock,
// and Go's RWMutex parks a reader behind a pending writer -- so evaluating it
// inside the span folds an unbounded block-apply wait into the stated bound.
//
// Here a block apply's ls.Lock() is taken while the reader sits in the gather
// span, and the gather pass must still finish and release the gather mutex.
func TestLedgerReadChainIteratorTakesNoLedgerLockInsideGatherSpan(
	t *testing.T,
) {
	// Not t.Parallel: swaps the package-level
	// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam via
	// shrinkGatherCoalesceRetryInterval.
	shrinkGatherCoalesceRetryInterval(t, time.Millisecond, 1)

	block, point := buildDecodableTestBlock(t, 10, 1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	iter := &pausingGapLedgerReadIterator{
		ctx:        ctx,
		first:      &chain.ChainIteratorResult{Point: point, Block: block},
		gapEntered: make(chan struct{}),
		resume:     make(chan struct{}),
	}

	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	ls.advanceUpstreamTipSlot(farUpstreamTipSlot)

	resultCh := make(chan readChainResult, 1)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		ls.ledgerReadChainIterator(ctx, iter, resultCh)
	}()

	testutil.RequireReceive(
		t, iter.gapEntered, testutil.AsyncWait,
		"reader never reached the chain-tip gap that triggers coalescing",
	)

	// The reader is parked inside Next holding the gather read lock and no
	// ledger lock, so this is obtainable now. Holding it across the resume
	// below is what a concurrent block apply does.
	ls.Lock()
	ledgerLockHeld := true
	defer func() {
		if ledgerLockHeld {
			ls.Unlock()
		}
	}()

	close(iter.resume)

	require.Eventually(
		t,
		func() bool { return tryLockGatherMutex(ls) },
		testutil.AsyncWait,
		5*time.Millisecond,
		"gather pass never released blockPipelineGatherMutex while the "+
			"ledger write lock was held -- it takes ls.RLock inside the "+
			"gather span, so the documented coalescing bound also includes "+
			"however long a block apply holds the ledger lock",
	)

	ls.Unlock()
	ledgerLockHeld = false

	result := testutil.RequireReceive(
		t, resultCh, testutil.AsyncWait,
		"reader never delivered its coalesced batch",
	)
	require.NoError(t, result.err)
	require.Len(t, result.blocks, 1)
	close(result.done)

	cancel()
	testutil.RequireReceive(
		t, readerDone, testutil.AsyncWait,
		"ledgerReadChainIterator did not exit after cancellation",
	)
}

// gatherSpanLockProbe records, for every LedgerStateConfig callback
// UpstreamTipSlot makes, whether blockPipelineGatherMutex was already held at
// the moment of the call. TryLock cannot block, so calling it from the reader
// goroutine that may itself hold the read lock is safe: a held read lock
// simply makes it fail.
type gatherSpanLockProbe struct {
	mu             sync.Mutex
	activeConnCals int
	connLiveCalls  int
	insideSpan     int
}

func (p *gatherSpanLockProbe) record(ls *LedgerState, activeConn bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if activeConn {
		p.activeConnCals++
	} else {
		p.connLiveCalls++
	}
	if !tryLockGatherMutex(ls) {
		p.insideSpan++
	}
}

func (p *gatherSpanLockProbe) counts() (int, int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.activeConnCals, p.connLiveCalls, p.insideSpan
}

// TestLedgerReadChainIteratorTakesNoConnectionLocksInsideGatherSpan closes the
// half of the bound that
// TestLedgerReadChainIteratorTakesNoLedgerLockInsideGatherSpan cannot see.
// That test leaves GetActiveConnectionFunc nil, so UpstreamTipSlot
// falls through to the syncUpstreamTipSlot atomic and never reaches the node's
// real wiring. Under that wiring UpstreamTipSlot is not an atomic read at all:
// GetActiveConnectionFunc is node_ledger_config.go's closure into
// withLiveChainsyncState (liveLifecycleMu) and chainsync State.GetClientConnId
// (clientConnIdMutex), and ConnectionLiveFunc reaches
// ConnectionManager.GetConnectionById (connectionsMutex). Evaluating it inside
// the span that holds blockPipelineGatherMutex therefore folds three more
// mutexes into a figure ARCHITECTURE.md derives purely from batchSize and the
// gatherCoalesce* settings.
//
// The upstream tip is read once per gather pass, alongside the stability
// window and before the pass takes any gather read lock, so these callbacks
// must never run with that lock held.
func TestLedgerReadChainIteratorTakesNoConnectionLocksInsideGatherSpan(
	t *testing.T,
) {
	// Not t.Parallel: swaps the package-level
	// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam via
	// shrinkGatherCoalesceRetryInterval.
	shrinkGatherCoalesceRetryInterval(t, time.Millisecond, 5)

	block1, point1 := buildDecodableTestBlock(t, 10, 1)
	block2, point2 := buildDecodableTestBlock(t, 20, 2)

	// One gap between two blocks, so the pass enters the coalescing branch --
	// and therefore evaluates the near-tip term -- with a block already
	// gathered and the gather read lock held.
	script := []*chain.ChainIteratorResult{
		{Point: point1, Block: block1},
		nil,
		{Point: point2, Block: block2},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	iter := &scriptedGapLedgerReadIterator{ctx: ctx, script: script}

	probe := &gatherSpanLockProbe{}
	connId := testRecycleConnId()
	ls := &LedgerState{}
	ls.config = LedgerStateConfig{
		Logger: testLogger(),
		GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
			probe.record(ls, true)
			return &connId
		},
		ConnectionLiveFunc: func(ouroboros.ConnectionId) bool {
			probe.record(ls, false)
			return true
		},
	}

	resultCh := make(chan readChainResult, 1)
	go ls.ledgerReadChainIterator(ctx, iter, resultCh)

	result := testutil.RequireReceive(
		t, resultCh, testutil.AsyncWait,
		"reader never delivered a batch",
	)
	require.NoError(t, result.err)
	require.Len(t, result.blocks, 2)
	close(result.done)

	activeConnCalls, connLiveCalls, insideSpan := probe.counts()
	// Without these the assertion below would hold vacuously: a pass that
	// never reads the upstream tip at all never takes the locks either.
	require.Positive(
		t,
		activeConnCalls,
		"gather pass never read the upstream tip, so this test asserts nothing",
	)
	require.Positive(
		t,
		connLiveCalls,
		"gather pass never checked whether the upstream connection is live, "+
			"so this test asserts nothing",
	)
	require.Zero(
		t,
		insideSpan,
		"UpstreamTipSlot ran with blockPipelineGatherMutex held, so the "+
			"liveLifecycleMu, clientConnIdMutex and connectionsMutex "+
			"acquisitions it makes are inside the span whose wait "+
			"ARCHITECTURE.md bounds from batchSize and the gatherCoalesce* "+
			"settings alone",
	)
}

// TestLedgerReadChainIteratorSkipsCoalesceAfterReachingTip covers the case
// isNearTip alone cannot see. UpstreamTipSlot returns 0 whenever no live
// upstream connection is selected, and isNearTipWithStabilityWindow folds an
// unknown upstream into "not near" -- so a node that has already caught up
// and then loses its upstream would start paying the coalescing wait again,
// gather lock held, including for its own forged blocks. reachedTip latches
// once the node first reaches the stability window and never clears, so it
// distinguishes "catching up and not yet connected" from "was at tip, lost
// the upstream".
//
// The retry interval here is minutes against a seconds-long receive
// deadline, so a regression cannot pass by winning a timing race: a correct
// implementation returns near-instantly, a regressed one is still asleep.
func TestLedgerReadChainIteratorSkipsCoalesceAfterReachingTip(t *testing.T) {
	// Not t.Parallel: swaps the package-level
	// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam via
	// shrinkGatherCoalesceRetryInterval.
	shrinkGatherCoalesceRetryInterval(t, 10*time.Minute, 10)

	block, point := buildDecodableTestBlock(t, 10, 1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	iter := &scriptedGapLedgerReadIterator{
		ctx:    ctx,
		script: []*chain.ChainIteratorResult{{Point: point, Block: block}, nil},
	}

	// No upstream tip: UpstreamTipSlot returns 0 and isNearTip is false for
	// every slot, exactly as during bulk replay. reachedTip is what tells
	// the two apart.
	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	ls.reachedTip.Store(true)

	resultCh := make(chan readChainResult, 1)
	go ls.ledgerReadChainIterator(ctx, iter, resultCh)

	result := testutil.RequireReceive(
		t, resultCh, 10*time.Second,
		"a node that already reached tip and then lost its upstream waited "+
			"on the bulk-replay coalescing batch instead of committing",
	)
	require.NoError(t, result.err)
	require.False(t, result.rollback)
	require.Len(t, result.blocks, 1)
	close(result.done)
}

// TestLedgerReadChainIteratorCommitBatchBlocksSkipsEmptyPasses pins the
// dingo_ledger_commit_batch_blocks histogram to actual submissions. A gather
// pass whose very first non-blocking probe returns chain.ErrIteratorChainTip
// gathers nothing -- the coalescing wait does not apply, because there is no
// partial batch to protect -- yet it still delivers a zero-block result
// downstream. Observing those would accumulate zeros in the lowest bucket of
// the distribution the histogram exists to measure, exactly during the bulk
// replay where the premature-flush symptom is read off it.
func TestLedgerReadChainIteratorCommitBatchBlocksSkipsEmptyPasses(
	t *testing.T,
) {
	// Not t.Parallel: swaps the package-level
	// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam via
	// shrinkGatherCoalesceRetryInterval.
	shrinkGatherCoalesceRetryInterval(t, time.Millisecond, 2)

	block1, point1 := buildDecodableTestBlock(t, 10, 1)
	block2, point2 := buildDecodableTestBlock(t, 20, 2)

	// The leading nil is consumed by the first, non-blocking probe, so that
	// pass gathers no blocks at all and flushes an empty result. The two
	// blocks then arrive on the following pass.
	script := []*chain.ChainIteratorResult{
		nil,
		{Point: point1, Block: block1},
		{Point: point2, Block: block2},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	iter := &scriptedGapLedgerReadIterator{ctx: ctx, script: script}

	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	ls.metrics.init(prometheus.NewRegistry())

	resultCh := make(chan readChainResult, 1)
	go ls.ledgerReadChainIterator(ctx, iter, resultCh)

	empty := testutil.RequireReceive(
		t, resultCh, testutil.AsyncWait,
		"reader never delivered the empty pass",
	)
	require.NoError(t, empty.err)
	require.Empty(t, empty.blocks)
	require.Zero(
		t,
		readCommitBatchBlocksSampleCount(t, ls),
		"an empty gather pass must not be recorded as a commit",
	)
	close(empty.done)

	batch := testutil.RequireReceive(
		t, resultCh, testutil.AsyncWait,
		"reader never delivered the gathered batch",
	)
	require.NoError(t, batch.err)
	require.Len(t, batch.blocks, 2)
	count, sum := readCommitBatchBlocks(t, ls)
	require.Equal(t, uint64(1), count)
	require.InDelta(t, 2.0, sum, 0.0001)
	close(batch.done)
}

func readCommitBatchBlocks(
	t *testing.T,
	ls *LedgerState,
) (uint64, float64) {
	t.Helper()
	metric := &dto.Metric{}
	require.NoError(t, ls.metrics.commitBatchBlocks.Write(metric))
	return metric.GetHistogram().GetSampleCount(),
		metric.GetHistogram().GetSampleSum()
}

func readCommitBatchBlocksSampleCount(
	t *testing.T,
	ls *LedgerState,
) uint64 {
	t.Helper()
	count, _ := readCommitBatchBlocks(t, ls)
	return count
}

// countingGapLedgerReadIterator returns one block and then reports a
// chain-tip gap on every later non-blocking call, recording how many such
// probes were made and how many of them ran while blockPipelineGatherMutex
// was held. A real iterator's Next takes chain.Chain's tip mutex and the
// chain manager's read lock (chain.Chain.iterNext), so each in-span probe is
// one acquisition of the lock the block-append path holds -- the term
// ARCHITECTURE.md's coalescing bound has to account for separately from the
// sleeping, because unlike the near-tip terms it cannot be hoisted out of
// the span.
type countingGapLedgerReadIterator struct {
	ctx    context.Context
	first  *chain.ChainIteratorResult
	ls     *LedgerState
	mu     sync.Mutex
	calls  int
	probes int
	inSpan int
}

func (c *countingGapLedgerReadIterator) Next(
	blocking bool,
) (*chain.ChainIteratorResult, error) {
	c.mu.Lock()
	idx := c.calls
	c.calls++
	c.mu.Unlock()
	if idx == 0 {
		return c.first, nil
	}
	if !blocking {
		// Probe for the write lock before answering, so the sample
		// describes the span the reader is actually inside when it
		// makes this call rather than the moment after it returns.
		held := !tryLockGatherMutex(c.ls)
		c.mu.Lock()
		c.probes++
		if held {
			c.inSpan++
		}
		c.mu.Unlock()
		return nil, chain.ErrIteratorChainTip
	}
	<-c.ctx.Done()
	return nil, c.ctx.Err()
}

func (c *countingGapLedgerReadIterator) counts() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.probes, c.inSpan
}

// TestLedgerReadChainIteratorBoundsChainProbesPerCoalesceGap pins the
// multiplier in the coalescing bound ARCHITECTURE.md states. The sleeping is
// bounded by the gatherCoalesce* settings, but each retry also re-probes the
// iterator while blockPipelineGatherMutex is still held, and that probe
// reaches chain.Chain.iterNext's c.mutex -- the lock addBlockInternal and
// addRawBlocks hold to advance the tip. The number of those acquisitions is
// what the documented worst case multiplies by, so it is the part a later
// change can silently inflate.
//
// A gap that never resolves must therefore cost exactly
// gatherCoalesceMaxAttempts+1 probes: the one that first reports the tip,
// plus one per retry the budget allows. Dropping the
// coalesceAttempts < gatherCoalesceMaxAttempts term in place makes the
// reader probe forever and never deliver the batch.
func TestLedgerReadChainIteratorBoundsChainProbesPerCoalesceGap(
	t *testing.T,
) {
	// Not t.Parallel: swaps the package-level
	// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam via
	// shrinkGatherCoalesceRetryInterval.
	const attempts = 4
	shrinkGatherCoalesceRetryInterval(t, time.Millisecond, attempts)

	block, point := buildDecodableTestBlock(t, 10, 1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	ls.advanceUpstreamTipSlot(farUpstreamTipSlot)

	iter := &countingGapLedgerReadIterator{
		ctx:   ctx,
		first: &chain.ChainIteratorResult{Point: point, Block: block},
		ls:    ls,
	}

	resultCh := make(chan readChainResult, 1)
	go ls.ledgerReadChainIterator(ctx, iter, resultCh)

	// The whole pass is attempts*1ms of sleeping, so a deadline in seconds
	// separates "flushed after exhausting the budget" from "still retrying"
	// without racing scheduler jitter.
	result := testutil.RequireReceive(
		t, resultCh, 10*time.Second,
		"reader never flushed its batch -- the coalesce budget did not "+
			"stop the retry loop, so the probe count the documented bound "+
			"multiplies by is unbounded",
	)
	require.NoError(t, result.err)
	require.False(t, result.rollback)
	require.Len(t, result.blocks, 1)
	close(result.done)

	probes, inSpan := iter.counts()
	require.Equal(
		t, attempts+1, probes,
		"a gap that never resolves must cost one chain probe to find the "+
			"tip plus one per allowed retry; ARCHITECTURE.md's worst case "+
			"multiplies batchSize by gatherCoalesceMaxAttempts on that basis",
	)
	require.Equal(
		t, probes, inSpan,
		"every coalesce probe must run with blockPipelineGatherMutex held "+
			"-- that is what makes each one an acquisition of the chain tip "+
			"lock inside the span, and what the bound has to account for",
	)
}

// newTestShelleyGenesisCfgWithK is newTestShelleyGenesisCfg with a caller
// chosen security parameter, so a windowed rewind can be exercised with a
// window small enough to need several steps over a short test chain.
func newTestShelleyGenesisCfgWithK(
	t testing.TB,
	k int,
) *cardano.CardanoNodeConfig {
	t.Helper()
	shelleyGenesisJSON := fmt.Sprintf(`{
		"activeSlotsCoeff": 0.05,
		"securityParam": %d,
		"slotsPerKESPeriod": 129600,
		"maxKESEvolutions": 62,
		"systemStart": "2022-10-25T00:00:00Z"
	}`, k)
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)
	return cfg
}

// seedTestChain appends count linked blocks to the primary chain, spaced ten
// slots apart so an appended block can always claim tip.Slot+1 without
// colliding with a block that is already stored.
func seedTestChain(
	t testing.TB,
	pc *chain.Chain,
	prefix string,
	count int,
) []chain.RawBlock {
	t.Helper()
	raw := make([]chain.RawBlock, 0, count)
	var prev []byte
	for i := 1; i <= count; i++ {
		h := testHashBytes(fmt.Sprintf("%s-%d", prefix, i))
		raw = append(raw, chain.RawBlock{
			Slot:        uint64(i * 10), //nolint:gosec
			Hash:        h,
			BlockNumber: uint64(i), //nolint:gosec
			Type:        1,
			PrevHash:    prev,
			Cbor:        []byte{0x80},
		})
		prev = h
	}
	require.NoError(t, pc.AddRawBlocks(raw))
	return raw
}

// TestWindowedRewindConvergesWhilePrimaryChainExtends pins the descent
// schedule in rollbackPrimaryChainInSecurityParamWindows against a primary
// chain that keeps growing underneath it, which is what a recovery rewind
// races with on a syncing node: blockfetch appends to the chain under
// chainsyncMutex while the ledger pipeline runs recovery under
// transactionEventMutex, so nothing serialises the two.
//
// The function used to read the chain tip once and then derive every
// intermediate target as snapshot-n*window. One block appended after that
// snapshot makes the next target window+1 below the chain's live tip, and
// Chain.Rollback refuses it as exceeding K. The whole rewind then fails, the
// pipeline restarts, and recovery recomputes the same doomed schedule against
// a tip that has grown further -- issue #3889, where that loop ran for nine
// hours and 1150 restarts without the chain ever being truncated.
//
// Each step must therefore be derived from the chain's live tip, so it is a
// legal K-bounded rollback by construction no matter how far the chain has
// advanced since the rewind began.
func TestWindowedRewindConvergesWhilePrimaryChainExtends(t *testing.T) {
	t.Parallel()

	const (
		securityParam = 8
		blockCount    = 240
	)

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: securityParam}),
	)
	pc := cm.PrimaryChain()
	raw := seedTestChain(t, pc, "windowed-race", blockCount)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfgWithK(t, securityParam),
		EventBus:          bus,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	// SecurityParam() is era-derived; without this the Byron fallback window
	// dwarfs the chain and no intermediate step is ever taken.
	ls.currentEra = eras.ShelleyEraDesc
	require.Equal(
		t,
		securityParam,
		ls.SecurityParam(),
		"rewind window must match the chain manager's k",
	)

	// Extend the primary chain by one block on every windowed-rewind loop
	// iteration, mimicking blockfetch appending while recovery descends.
	// beforeWindowedRewindStep runs synchronously inside
	// rollbackPrimaryChainInSecurityParamWindows at the top of every loop
	// iteration, so an append here is guaranteed to land during a step the
	// rewind is actually taking -- not merely likely to, the way a
	// separately scheduled goroutine racing the rewind would only
	// probabilistically overlap it, with no guarantee it is ever scheduled
	// between the rewind starting and returning. One block per step is
	// slower than the window each step covers, so a rewind that re-reads
	// the live tip still converges.
	var stepCount int
	ls.beforeWindowedRewindStep = func() {
		stepCount++
		tip := pc.Tip()
		next := chain.RawBlock{
			Slot: tip.Point.Slot + 1,
			Hash: testHashBytes(
				fmt.Sprintf("windowed-race-append-%d", stepCount),
			),
			BlockNumber: tip.BlockNumber + 1,
			Type:        1,
			PrevHash:    tip.Point.Hash,
			Cbor:        []byte{0x80},
		}
		require.NoError(t, pc.AddRawBlocks([]chain.RawBlock{next}))
	}

	target := ocommon.NewPoint(raw[0].Slot, raw[0].Hash)
	committed, rewindErr := ls.rollbackPrimaryChainInSecurityParamWindows(
		target,
	)

	require.NotErrorIs(
		t,
		rewindErr,
		chain.ErrRollbackExceedsSecurityParam,
		"a windowed step must stay within K of the chain's live tip",
	)
	require.NoError(t, rewindErr)
	require.True(
		t,
		committed,
		"a descent that reached its target committed its steps",
	)
	// The hook is called from inside the rewind's own loop, so every call it
	// receives is by construction a step the rewind is actively taking; the
	// loop must take at least one such step to reach a target this far
	// behind the seeded chain. This is therefore a guaranteed fact about the
	// run rather than a probability the scheduler could fail to realize.
	require.Positive(
		t,
		stepCount,
		"the windowed rewind must take at least one step that extends the chain",
	)
}

// TestDeterministicTxRecoveryHaltsOnUnreachableRewind pins the second half of
// issue #3889: a recovery rewind the chain refuses as exceeding K is not a
// transient failure, so repeating it at an applied tip that never advances
// must become terminal instead of restarting the pipeline forever.
//
// recoverFromDeterministicTxValidationError used to return the refusal as a
// plain error. ledgerProcessBlocks treats anything that is not
// errHaltLedgerPipeline as retryable, so the node logged "block processing
// failed, restarting pipeline", waited out the backoff, and recomputed the
// same impossible rewind -- 1150 times over nine hours in the report, with the
// stuck-pipeline watchdog correctly announcing that the failure was
// deterministic while the node kept retrying anyway.
func TestDeterministicTxRecoveryHaltsOnUnreachableRewind(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	// A chain k of 2 against the ledger's much larger window means the
	// single rewind step is refused on fork depth, which is the refusal the
	// recovery path has to classify.
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))
	raw := seedTestChain(t, cm.PrimaryChain(), "halt-on-unreachable", 5)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		EventBus:          bus,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	require.Greater(t, ls.SecurityParam(), 2, "ledger window must exceed k")

	// The applied tip sits at the first block and the rejected block is the
	// chain tip, so the rewind target is four blocks below a chain whose k
	// is 2 and every rewind to it is refused.
	ls.currentTip.Point = ocommon.NewPoint(raw[0].Slot, raw[0].Hash)
	validationErr := &txValidationError{
		BlockPoint: ocommon.NewPoint(raw[4].Slot, raw[4].Hash),
		TxHash:     testHashBytes("halt-on-unreachable-tx"),
		Cause: conway.PlutusScriptFailedError{
			Err: errors.New("error explicitly called"),
		},
	}
	require.True(t, isDeterministicTxValidationError(validationErr.Cause))

	var lastErr error
	halted := false
	// Well past any bounded retry budget: an unreachable rewind still being
	// retried after this many attempts is the nine-hour loop.
	for range 32 {
		_, lastErr = ls.recoverFromDeterministicTxValidationError(
			validationErr,
		)
		require.Error(t, lastErr)
		require.ErrorIs(t, lastErr, chain.ErrRollbackExceedsSecurityParam)
		if errors.Is(lastErr, errHaltLedgerPipeline) {
			halted = true
			break
		}
	}
	require.True(
		t,
		halted,
		"a recovery rewind refused as exceeding K must stop the pipeline "+
			"rather than be retried forever: %v",
		lastErr,
	)
}

// TestRecoveryRewindHaltBudgetResetsOnTipProgress pins the other side of that
// budget. The halt is for a rewind that stays unreachable at an applied tip
// that never moves; once the ledger advances past that tip the situation is a
// different one and must start with a fresh budget rather than inherit a tally
// that has nothing to do with it.
func TestRecoveryRewindHaltBudgetResetsOnTipProgress(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))
	raw := seedTestChain(t, cm.PrimaryChain(), "halt-budget-reset", 5)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		EventBus:          bus,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())

	target := ocommon.NewPoint(raw[0].Slot, raw[0].Hash)
	for range maxRecoveryRewindRejections {
		rewindErr := ls.rewindPrimaryChainForRecovery(target)
		require.ErrorIs(
			t,
			rewindErr,
			chain.ErrRollbackExceedsSecurityParam,
		)
		require.NotErrorIs(t, rewindErr, errHaltLedgerPipeline)
	}
	// Forward progress past the tip the refusals could not cross clears the
	// tally, so the budget starts over instead of halting on the next one.
	ls.resetRecoveryRewindRejections(raw[3].Slot)
	rewindErr := ls.rewindPrimaryChainForRecovery(target)
	require.ErrorIs(t, rewindErr, chain.ErrRollbackExceedsSecurityParam)
	require.NotErrorIs(t, rewindErr, errHaltLedgerPipeline)
}

// TestRecoveryRewindHaltsThoughTargetMovesAndDepthGrows pins the shape the
// live reproduction on Preview showed (issue #3889: a replay wedged at slot
// 41098815 for over twenty minutes across 97 rejection attempts on one
// transaction).
//
// Two properties of that run decide whether a fix works:
//
//   - The rewind target is recomputed on every attempt and differs every time
//     -- intermediate points 1768501, 1774034, 1773427, 1779454, 1782037,
//     1769017 -- all beyond K. So the terminal condition cannot key on one
//     target that keeps failing; there is no such target. It keys on the
//     applied ledger tip, which is the thing that is not moving.
//   - The depth that must be rolled back grows monotonically, because the peer
//     keeps extending the fork while the local tip is pinned
//     (fork_path_headers 2462, 5031, 7615; fork_depth 5010 then 6020). A
//     rewind already beyond K never comes back into range on its own, which is
//     what makes the loop unrecoverable rather than merely slow.
//
// The chain is therefore extended between attempts, so every attempt computes
// a different step target against a larger gap, and the halt must still
// arrive.
func TestRecoveryRewindHaltsThoughTargetMovesAndDepthGrows(t *testing.T) {
	t.Parallel()

	const (
		chainK        = 4
		ledgerWindow  = 8
		startingChain = 40
		growthPerTry  = 5
		maxAttempts   = 16
	)

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	// The chain enforces a smaller k than the ledger's rewind window, so every
	// step the descent computes is refused wherever the tip has moved to.
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: chainK}),
	)
	pc := cm.PrimaryChain()
	raw := seedTestChain(t, pc, "moving-target", startingChain)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfgWithK(t, ledgerWindow),
		EventBus:          bus,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	ls.currentEra = eras.ShelleyEraDesc
	require.Equal(t, ledgerWindow, ls.SecurityParam())

	// The applied ledger tip stays at the first block for the whole test:
	// that is the "tip pinned at slot 41098815" half of the report.
	appliedTip := ocommon.NewPoint(raw[0].Slot, raw[0].Hash)
	ls.currentTip.Point = appliedTip
	validationErr := &txValidationError{
		BlockPoint: ocommon.NewPoint(
			raw[startingChain-1].Slot,
			raw[startingChain-1].Hash,
		),
		TxHash: testHashBytes("moving-target-tx"),
		Cause: conway.PlutusScriptFailedError{
			Err: errors.New("error explicitly called"),
		},
	}
	require.True(t, isDeterministicTxValidationError(validationErr.Cause))

	seenTargets := map[string]struct{}{}
	var (
		chainTips []uint64
		halted    bool
		attempts  int
		lastErr   error
	)
	for range maxAttempts {
		attempts++
		chainTips = append(chainTips, pc.Tip().Point.Slot)
		_, lastErr = ls.recoverFromDeterministicTxValidationError(
			validationErr,
		)
		require.ErrorIs(t, lastErr, chain.ErrRollbackExceedsSecurityParam)
		// require.ErrorIs above fails the test on a nil lastErr, which nilaway
		// does not model.
		//nolint:nilaway // non-nil per the require.ErrorIs above
		seenTargets[lastErr.Error()] = struct{}{}
		if errors.Is(lastErr, errHaltLedgerPipeline) {
			halted = true
			break
		}
		// The peer keeps serving the fork while the ledger tip is stuck, so
		// the next attempt faces a deeper rollback than this one did.
		grow := make([]chain.RawBlock, 0, growthPerTry)
		prev := pc.Tip()
		for i := range growthPerTry {
			h := testHashBytes(
				fmt.Sprintf("moving-target-grow-%d-%d", attempts, i),
			)
			grow = append(grow, chain.RawBlock{
				Slot:        prev.Point.Slot + 10,
				Hash:        h,
				BlockNumber: prev.BlockNumber + 1,
				Type:        1,
				PrevHash:    prev.Point.Hash,
				Cbor:        []byte{0x80},
			})
			prev = ochainsync.Tip{
				Point:       ocommon.NewPoint(prev.Point.Slot+10, h),
				BlockNumber: prev.BlockNumber + 1,
			}
		}
		require.NoError(t, pc.AddRawBlocks(grow))
	}

	require.True(
		t,
		halted,
		"a rewind that stays beyond K must stop the pipeline even though "+
			"every attempt computes a different target: %v",
		lastErr,
	)
	require.Greater(
		t,
		len(seenTargets),
		1,
		"the test must exercise a target that moves between attempts",
	)
	require.Greater(
		t,
		// maxAttempts is a positive constant, so the loop above appended at
		// least one tip; nilaway does not reason about the loop bound.
		//nolint:nilaway // the loop above appends at least one entry
		chainTips[len(chainTips)-1],
		chainTips[0],
		"the fork must extend while the applied ledger tip stays pinned",
	)
	require.True(
		t,
		slices.IsSorted(chainTips),
		"required rollback depth must only grow across attempts: %v",
		chainTips,
	)
}

// TestWindowedRewindRefusesRecoveryTargetTheChainDoesNotHold pins that the
// entry check on rollbackPrimaryChainInSecurityParamWindows establishes
// primary-chain membership, not store presence.
//
// The descent commits each step as it goes, so a target it can never reach
// has to be refused before the first truncation. The check used to be a
// database.BlockByPoint lookup, which a target the store still holds but the
// chain has abandoned passes -- the retained-index shape rollbackPointBlock
// documents. The descent then truncated every intermediate step and
// Chain.Rollback refused the final one with ErrRollbackPointNotOnChain,
// leaving the chain shortened for a rewind that never happened.
//
// The target below is written straight into the block store at an index above
// the chain tip, so it is present by point and absent from the chain: the
// store lookup accepts it and the chain's own membership check does not.
func TestWindowedRewindRefusesRecoveryTargetTheChainDoesNotHold(t *testing.T) {
	t.Parallel()

	const (
		securityParam = 8
		blockCount    = 60
	)

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: securityParam}),
	)
	pc := cm.PrimaryChain()
	raw := seedTestChain(t, pc, "target-not-on-chain", blockCount)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfgWithK(t, securityParam),
		EventBus:          bus,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	ls.currentEra = eras.ShelleyEraDesc

	// A block the store holds at an index the chain does not: its slot falls
	// inside the chain's span, so the descent would start, and its index sits
	// above the chain tip, so no chain block occupies it.
	orphan := models.Block{
		ID:       blockCount + 5,
		Slot:     raw[0].Slot + 5,
		Hash:     testHashBytes("target-not-on-chain-orphan"),
		Number:   raw[0].BlockNumber,
		Type:     1,
		PrevHash: raw[0].Hash,
		Cbor:     []byte{0x80},
	}
	require.NoError(t, db.BlockCreate(orphan, nil))
	target := ocommon.NewPoint(orphan.Slot, orphan.Hash)
	_, err = database.BlockByPoint(db, target)
	require.NoError(
		t,
		err,
		"the store must hold the target for this to test anything",
	)

	tipBefore := pc.Tip()
	committed, err := ls.rollbackPrimaryChainInSecurityParamWindows(target)
	require.ErrorIs(t, err, chain.ErrRollbackPointNotOnChain)
	require.Equal(
		t,
		tipBefore,
		pc.Tip(),
		"a target the chain does not hold must be refused before any step is committed",
	)
	require.False(
		t,
		committed,
		"the refusal must report that nothing was truncated",
	)
}

// TestWindowedRewindRefusesSlotZeroTargetTheStoreDoesNotHold pins the entry
// check for a slot-zero target, from this package's side of the boundary.
//
// Slot 0 is a real slot, so a point carrying a hash names a block there. When
// ValidateRollback gated its lookup on point.Slot > 0 such a target skipped
// membership validation, passed the entry check and took the descent all the
// way down, leaving the chain empty and its tip naming a block the store need
// not hold; this package compensated with its own store lookup. ValidateRollback
// now resolves any hash-bearing point, so the refusal comes from the chain's
// membership check and the local lookup is gone -- this test is what proves
// the coverage moved rather than disappeared.
func TestWindowedRewindRefusesSlotZeroTargetTheStoreDoesNotHold(t *testing.T) {
	t.Parallel()

	const (
		securityParam = 8
		blockCount    = 30
	)

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: securityParam}),
	)
	pc := cm.PrimaryChain()
	seedTestChain(t, pc, "slot-zero-target", blockCount)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfgWithK(t, securityParam),
		EventBus:          bus,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	ls.currentEra = eras.ShelleyEraDesc

	target := ocommon.NewPoint(0, testHashBytes("slot-zero-target-absent"))
	tipBefore := pc.Tip()
	committed, err := ls.rollbackPrimaryChainInSecurityParamWindows(target)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
	require.Equal(
		t,
		tipBefore,
		pc.Tip(),
		"a slot-zero target the store does not hold must not truncate the chain",
	)
	require.False(
		t,
		committed,
		"the refusal must report that nothing was truncated",
	)
}

// Byron epoch-boundary blocks take the first slot of their epoch, and the
// epoch's first regular block takes the same slot whenever one is minted
// there. Mainnet does it at every Byron boundary -- the genesis EBB
// 89d9b5a5 at slot 0 is the direct parent of f0f7892b, also at slot 0 -- so a
// chain holding two distinct blocks at one slot is ordinary history rather
// than a fork.
func sameSlotBoundaryBlocks(t testing.TB) []models.Block {
	t.Helper()
	specs := []struct {
		slot uint64
		seed string
	}{
		{slot: 1, seed: "ancestor-1"},
		{slot: 2, seed: "ancestor-2"},
		{slot: 3, seed: "epoch-boundary"},
		{slot: 3, seed: "first-block-of-epoch"},
		{slot: 4, seed: "successor"},
	}
	blocks := make([]models.Block, 0, len(specs))
	for i, spec := range specs {
		hash := sha256.Sum256([]byte(spec.seed))
		block := models.Block{
			ID:     uint64(i + 1), //nolint:gosec
			Slot:   spec.slot,
			Hash:   hash[:],
			Number: uint64(i + 1), //nolint:gosec
			Type:   1,
			Cbor:   []byte{0x80},
		}
		if i > 0 {
			block.PrevHash = append([]byte(nil), blocks[i-1].Hash...)
		}
		blocks = append(blocks, block)
	}
	return blocks
}

type sameSlotRecoveryFixture struct {
	ls           *LedgerState
	cm           *chain.ChainManager
	blocks       []models.Block
	resyncEvents <-chan event.Event
}

func newSameSlotRecoveryFixture(t *testing.T) *sameSlotRecoveryFixture {
	t.Helper()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck

	blocks := sameSlotBoundaryBlocks(t)
	for _, block := range blocks {
		require.NoError(t, db.BlockCreate(block, nil))
	}

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))

	// The ledger has applied the epoch-boundary block. The block that failed
	// validation is its direct successor, which shares its slot.
	boundary := blocks[2]
	ledgerTip := ochainsync.Tip{
		Point:       makeTestPoint(boundary),
		BlockNumber: boundary.Number,
	}
	require.NoError(t, db.SetTip(ledgerTip, nil))

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, resyncEvents := bus.Subscribe(event.ChainsyncResyncEventType)

	ls := &LedgerState{
		db:    db,
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			ChainManager: cm,
			EventBus:     bus,
			Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	ls.currentTip = ledgerTip
	require.NoError(t, ls.reconcilePrimaryChainTipWithLedgerTip())
	require.Equal(t, blocks[4].Slot, cm.PrimaryChain().Tip().Point.Slot,
		"the rejected block and its successor start on the chain")

	return &sameSlotRecoveryFixture{
		ls:           ls,
		cm:           cm,
		blocks:       blocks,
		resyncEvents: resyncEvents,
	}
}

func TestHeaderValidationRecoveryDeclinesPastFailureAtSameSlot(
	t *testing.T,
) {
	t.Parallel()

	f := newSameSlotRecoveryFixture(t)
	boundary := f.blocks[2]
	laterBlock := f.blocks[3]
	laterTip := ochainsync.Tip{
		Point:       makeTestPoint(laterBlock),
		BlockNumber: laterBlock.Number,
	}
	require.NoError(t, f.ls.db.SetTip(laterTip, nil))
	f.ls.currentTip = laterTip
	chainTipBefore := f.cm.PrimaryChain().Tip().Point

	recovered, recoverErr := f.ls.tryRecoverFromHeaderValidationError(
		&headerValidationError{
			BlockPoint: makeTestPoint(boundary),
			Cause:      errors.New("failing EBB precedes the applied block"),
		},
	)

	require.NoError(t, recoverErr)
	require.False(t, recovered,
		"a later same-slot tip must not be treated as preceding the failed EBB")
	require.Equal(t, chainTipBefore, f.cm.PrimaryChain().Tip().Point)
	select {
	case <-f.resyncEvents:
		t.Fatal("declined recovery must not publish a resync")
	default:
	}
}

// The ledger tip is a valid rewind target whenever it is a different block
// from the one that failed and the chain orders it first. At a Byron epoch
// boundary that pair shares a slot, so a slot-only precedence test reports
// "no rewind target precedes it" for a target that does, declines a recovery
// that would have dropped the rejected block, and leaves the pipeline reading
// the same persisted block until the stuck detector fires.
func TestHeaderValidationRecoveryRewindsToSameSlotPredecessor(t *testing.T) {
	t.Parallel()

	f := newSameSlotRecoveryFixture(t)
	boundary := f.blocks[2]
	failing := f.blocks[3]
	require.Equal(t, boundary.Slot, failing.Slot)
	require.NotEqual(t, boundary.Hash, failing.Hash)

	recovered, recoverErr := f.ls.tryRecoverFromHeaderValidationError(
		&headerValidationError{
			BlockPoint: makeTestPoint(failing),
			Cause:      errors.New("VRF leader value exceeds threshold"),
		},
	)
	require.NoError(t, recoverErr)
	require.True(t, recovered,
		"the applied epoch-boundary block precedes the rejected block and "+
			"is a rewind target")
	require.Equal(t, makeTestPoint(boundary),
		f.cm.PrimaryChain().Tip().Point,
		"the chain must be rewound onto the boundary block, not left "+
			"holding the rejected block at the same slot")

	select {
	case evt := <-f.resyncEvents:
		data, ok := evt.Data.(event.ChainsyncResyncEvent)
		require.True(t, ok)
		require.Equal(
			t,
			event.ChainsyncResyncReasonHeaderValidationRecovery,
			data.Reason,
		)
		require.Equal(t, makeTestPoint(boundary), data.Point)
	default:
		t.Fatal("recovery must publish a resync so chainsync re-delivers")
	}
}

// The same slot with the same hash is the ledger tip itself, which is what
// the guard exists to refuse: nothing would be dropped, so reporting a
// recovery would hide the failure from the stuck detector.
func TestHeaderValidationRecoveryDeclinesAtSameSlotSameHash(t *testing.T) {
	t.Parallel()

	f := newSameSlotRecoveryFixture(t)
	chainTipBefore := f.cm.PrimaryChain().Tip().Point

	recovered, recoverErr := f.ls.tryRecoverFromHeaderValidationError(
		&headerValidationError{
			BlockPoint: makeTestPoint(f.blocks[2]),
			Cause:      errors.New("rejected"),
		},
	)
	require.NoError(t, recoverErr)
	require.False(t, recovered,
		"the ledger tip cannot be a rewind target for itself")
	require.Equal(t, chainTipBefore, f.cm.PrimaryChain().Tip().Point,
		"declining must not disturb the chain")
	select {
	case <-f.resyncEvents:
		t.Fatal("a declined recovery must not publish a resync")
	default:
	}
}

// The deterministic transaction-validation recovery carries the identical
// precedence test and the identical Byron boundary exposure.
func TestDeterministicTxRecoveryRewindsToSameSlotPredecessor(t *testing.T) {
	t.Parallel()

	f := newSameSlotRecoveryFixture(t)
	boundary := f.blocks[2]
	failing := f.blocks[3]

	recovered, recoverErr := f.ls.recoverFromDeterministicTxValidationError(
		&txValidationError{
			BlockPoint: makeTestPoint(failing),
			TxHash:     testHashBytes("same-slot-duplicate-input"),
			Cause:      errors.New("duplicate input"),
		},
	)
	require.NoError(t, recoverErr)
	require.True(t, recovered,
		"the applied epoch-boundary block precedes the rejected block and "+
			"is a rewind target")
	require.Equal(t, makeTestPoint(boundary),
		f.cm.PrimaryChain().Tip().Point,
		"the chain must be rewound onto the boundary block")
}

// Same-slot, same-hash still declines on the transaction path.
func TestDeterministicTxRecoveryDeclinesAtSameSlotSameHash(t *testing.T) {
	t.Parallel()

	f := newSameSlotRecoveryFixture(t)
	chainTipBefore := f.cm.PrimaryChain().Tip().Point

	recovered, recoverErr := f.ls.recoverFromDeterministicTxValidationError(
		&txValidationError{
			BlockPoint: makeTestPoint(f.blocks[2]),
			TxHash:     testHashBytes("same-slot-same-hash"),
			Cause:      errors.New("duplicate input"),
		},
	)
	require.NoError(t, recoverErr)
	require.False(t, recovered,
		"the ledger tip cannot be a rewind target for itself")
	require.Equal(t, chainTipBefore, f.cm.PrimaryChain().Tip().Point,
		"declining must not disturb the chain")
}

// A bootstrapped node applies no block at or below its trust anchor, so every
// slot of an epoch that ended below the anchor is uncountable. The blocks were
// nonetheless minted, and the reference credits their rewards, so the counts
// have to come from the snapshot's own BlocksMade rather than from a floor of
// zero.
func TestRewardBlockCountsMergesImportedCountsAcrossTheAnchor(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		performanceEpoch = uint64(2)
		epochStartSlot   = uint64(100)
		epochLength      = 100
		anchorSlot       = uint64(150)
	)
	poolKey := rewardCalcHash(0x81)
	otherPoolKey := rewardCalcHash(0x82)
	retiredPoolKey := rewardCalcHash(0x83)
	var poolID, otherPoolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)
	copy(otherPoolID[:], otherPoolKey)

	require.NoError(t, meta.SetEpoch(
		epochStartSlot,
		performanceEpoch,
		nil, nil, nil, nil,
		eras.ShelleyEraDesc.Id,
		1,
		epochLength,
		nil,
	))
	require.NoError(t, meta.SetSyncState(
		mithrilLedgerSlotSyncKey,
		strconv.FormatUint(anchorSlot, 10),
		nil,
	))
	// Blocks this node applied itself, all strictly above the anchor.
	for _, slot := range []uint64{160, 170} {
		require.NoError(t, db.UpdatePoolOpCertSequence(poolID, slot, slot, nil))
	}
	require.NoError(t, db.UpdatePoolOpCertSequence(otherPoolID, 180, 180, nil))
	// Blocks the snapshot reports for the same epoch, minted at or below the
	// anchor. retiredPoolKey is not one of the pools asked about, but its
	// blocks still belong to the epoch total that every pool's beta divides by.
	require.NoError(t, meta.SaveImportedPoolBlockCounts(
		[]models.ImportedPoolBlockCount{
			{
				Epoch:          performanceEpoch,
				PoolKeyHash:    poolKey,
				BlocksProduced: 5,
				CapturedSlot:   anchorSlot,
			},
			{
				Epoch:          performanceEpoch,
				PoolKeyHash:    otherPoolKey,
				BlocksProduced: 3,
				CapturedSlot:   anchorSlot,
			},
			{
				Epoch:          performanceEpoch,
				PoolKeyHash:    retiredPoolKey,
				BlocksProduced: 2,
				CapturedSlot:   anchorSlot,
			},
		},
		nil,
	))
	require.NoError(t, meta.SaveImportedEpochBlockTotal(
		performanceEpoch,
		5+3+2,
		anchorSlot,
		nil,
	))

	counts, total, known, err := ls.rewardBlockCounts(
		meta,
		nil,
		performanceEpoch,
		[]*models.RewardPoolInput{
			{PoolKeyHash: poolKey},
			{PoolKeyHash: otherPoolKey},
		},
		nil,
	)
	require.NoError(t, err)
	require.True(t, known)
	assert.Equal(t, uint64(2+5), counts[string(poolKey)])
	assert.Equal(t, uint64(1+3), counts[string(otherPoolKey)])
	assert.Equal(t, uint64(3+10), total)
}

// Zero blocks and no block history are different answers. The first is a real
// epoch outcome; the second is an epoch this node cannot count, and reading it
// as zero gives every pool zero performance and credits every delegator
// nothing while reporting a completed round.
func TestRewardBlockCountsUnknownWhenAnchorHidesTheEpoch(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		performanceEpoch = uint64(2)
		epochStartSlot   = uint64(100)
		epochLength      = 100
	)
	poolKey := rewardCalcHash(0x84)

	require.NoError(t, meta.SetEpoch(
		epochStartSlot,
		performanceEpoch,
		nil, nil, nil, nil,
		eras.ShelleyEraDesc.Id,
		1,
		epochLength,
		nil,
	))
	// The anchor sits past the end of the epoch, so none of it is observable.
	require.NoError(t, meta.SetSyncState(
		mithrilLedgerSlotSyncKey,
		strconv.FormatUint(epochStartSlot+epochLength, 10),
		nil,
	))

	_, _, known, err := ls.rewardBlockCounts(
		meta,
		nil,
		performanceEpoch,
		[]*models.RewardPoolInput{{PoolKeyHash: poolKey}},
		nil,
	)
	require.NoError(t, err)
	require.False(
		t,
		known,
		"an epoch that ended below the anchor with no imported counts has "+
			"unknown block counts, not zero",
	)
}

// The imported counts are consulted only for an epoch the anchor actually
// covers. A node that never bootstrapped counts its own blocks exactly as it
// did before.
func TestRewardBlockCountsIgnoresImportedCountsAboveTheAnchor(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		performanceEpoch = uint64(2)
		epochStartSlot   = uint64(100)
		epochLength      = 100
	)
	poolKey := rewardCalcHash(0x85)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)

	require.NoError(t, meta.SetEpoch(
		epochStartSlot,
		performanceEpoch,
		nil, nil, nil, nil,
		eras.ShelleyEraDesc.Id,
		1,
		epochLength,
		nil,
	))
	require.NoError(t, meta.SetSyncState(
		mithrilLedgerSlotSyncKey,
		strconv.FormatUint(epochStartSlot-1, 10),
		nil,
	))
	require.NoError(t, db.UpdatePoolOpCertSequence(poolID, 1, 120, nil))
	require.NoError(t, meta.SaveImportedPoolBlockCounts(
		[]models.ImportedPoolBlockCount{
			{
				Epoch:          performanceEpoch,
				PoolKeyHash:    poolKey,
				BlocksProduced: 7,
				CapturedSlot:   epochStartSlot - 1,
			},
		},
		nil,
	))
	require.NoError(t, meta.SaveImportedEpochBlockTotal(
		performanceEpoch,
		7,
		epochStartSlot-1,
		nil,
	))

	counts, total, known, err := ls.rewardBlockCounts(
		meta,
		nil,
		performanceEpoch,
		[]*models.RewardPoolInput{{PoolKeyHash: poolKey}},
		nil,
	)
	require.NoError(t, err)
	require.True(t, known)
	assert.Equal(t, uint64(1), counts[string(poolKey)])
	assert.Equal(t, uint64(1), total)
}

// The round-level consequence. seedRewardPrecomputeTimingState places ten
// blocks for the single pool inside performance epoch 2; putting the anchor
// past that epoch removes every one of them from the node's reach.
func TestStakeRewardRoundDeclinedWhenAnchorHidesTheBlockCounts(t *testing.T) {
	t.Parallel()

	ls, db := seedRewardPrecomputeTimingState(t, 7)
	var logs bytes.Buffer
	ls.config.Logger = slog.New(slog.NewTextHandler(&logs, nil))

	require.NoError(t, db.Metadata().SetSyncState(
		mithrilLedgerSlotSyncKey,
		"199",
		nil,
	))

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.calculateStakeRewardApplication(
		txn,
		4,
		1_200,
		1_200,
		true,
	)
	require.NoError(t, err)
	require.False(
		t,
		ok,
		"a round whose performance epoch cannot be counted must be declined, "+
			"not distributed as zero",
	)
	require.Nil(t, app)
	assert.Contains(
		t,
		logs.String(),
		"no block counts for the performance epoch",
	)
}

// A recorded anchor sits at or above slot 0 and so covers epoch 0, the
// performance epoch of both bootstrap rounds. Those rounds must still run:
// they distribute no pool or account rewards but do move the ADA pots, and
// declining one would leave treasury and reserves at their genesis values for
// the life of the chain. They are safe because epoch 0's mark snapshot holds
// no pools, and an empty pool set is answered before the anchor is consulted;
// the reference agrees that zero rather than unknown is the answer there,
// since NEWEPOCH's initialRules construct the genesis state with BlocksMade
// Map.empty. This pins that, rather than proving a fix.
func TestBootstrapStakeRewardRoundSurvivesAMithrilAnchor(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	require.NoError(t, meta.SetEpoch(
		0, 0, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil,
	))
	pparamsCbor, err := cbor.Encode(&shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(0, 1),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    7,
	})
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		pparamsCbor, 0, 0, eras.ShelleyEraDesc.Id, nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        0,
		Reserves:     100_000_000,
		CapturedSlot: 0,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:           0,
		SnapshotType:    "mark",
		CapturedSlot:    0,
		BoundarySlot:    0,
		ProtocolVersion: 7,
	}, nil))
	require.NoError(t, meta.SetSyncState(
		mithrilLedgerSlotSyncKey,
		"50",
		nil,
	))

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.calculateStakeRewardApplication(txn, 1, 100, 100, true)
	require.NoError(t, err)
	require.True(
		t,
		ok,
		"an anchor covers epoch 0 by construction; the bootstrap round still "+
			"has to move the pots",
	)
	require.NotNil(t, app)
	assert.True(t, app.epochs.bootstrap)
	assert.Empty(t, app.poolOutputs)
	assert.Empty(t, app.accountOutputs)
}

// The imported counts are not an approximation: for the same epoch they
// reproduce the distribution the node would have computed from its own block
// history, pool output for pool output and account output for account output.
func TestStakeRewardRoundFromImportedBlockCountsMatchesObservedHistory(
	t *testing.T,
) {
	t.Parallel()

	observed := stakeRewardApplicationForTest(t, false)
	imported := stakeRewardApplicationForTest(t, true)

	require.Len(t, observed.poolOutputs, 1)
	require.Len(t, imported.poolOutputs, len(observed.poolOutputs))
	for i, want := range observed.poolOutputs {
		got := imported.poolOutputs[i]
		assert.Equal(t, want.PoolKeyHash, got.PoolKeyHash)
		assert.Equal(t, want.TotalReward, got.TotalReward)
		assert.Equal(t, want.LeaderReward, got.LeaderReward)
		assert.Equal(
			t,
			want.ApparentPerformance.String(),
			got.ApparentPerformance.String(),
		)
	}
	require.NotEmpty(t, observed.accountOutputs)
	require.Len(t, imported.accountOutputs, len(observed.accountOutputs))
	for i, want := range observed.accountOutputs {
		got := imported.accountOutputs[i]
		assert.Equal(t, want.StakingKey, got.StakingKey)
		assert.Equal(t, want.RewardType, got.RewardType)
		assert.Equal(t, want.Amount, got.Amount)
	}
	assert.Positive(t, uint64(observed.poolOutputs[0].TotalReward))
	assert.Equal(t, observed.effectiveRewards, imported.effectiveRewards)
}

// stakeRewardApplicationForTest computes the epoch 4 reward round twice over
// the same state: once from the ten blocks the node itself applied in epoch 2,
// and once with those blocks hidden behind a trust anchor and supplied instead
// as the snapshot's imported counts for the same epoch.
func stakeRewardApplicationForTest(
	t *testing.T,
	fromImport bool,
) *stakeRewardApplication {
	t.Helper()
	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()
	if fromImport {
		require.NoError(t, meta.SetSyncState(
			mithrilLedgerSlotSyncKey,
			"199",
			nil,
		))
		require.NoError(t, meta.SaveImportedPoolBlockCounts(
			[]models.ImportedPoolBlockCount{
				{
					Epoch:          2,
					PoolKeyHash:    rewardCalcHash(0x4a),
					BlocksProduced: 10,
					CapturedSlot:   199,
				},
			},
			nil,
		))
		require.NoError(t, meta.SaveImportedEpochBlockTotal(2, 10, 199, nil))
	}
	txn := db.Transaction(false)
	t.Cleanup(func() { _ = txn.Rollback() })
	app, ok, err := ls.calculateStakeRewardApplication(
		txn,
		4,
		1_200,
		1_200,
		true,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, app)
	return app
}

// TestStakeRewardEpochsForInitialApplication pins the two bootstrap rounds.
// The round into epoch 1 reads genesis pots and empty previous block counts;
// the round into epoch 2 reads epoch 1's pots and epoch 0's blocks. Both have
// empty Go distributions. Byron-prefix networks are suppressed by
// applyStakeRewards' Byron performance-epoch guard, not by this helper.
func TestStakeRewardEpochsForInitialApplication(t *testing.T) {
	t.Parallel()

	_, ok := stakeRewardEpochsForApplication(0)
	require.False(t, ok, "epoch 0 is not a boundary and applies no rewards")

	epochs, ok := stakeRewardEpochsForApplication(1)
	require.True(t, ok)
	require.Equal(t, stakeRewardEpochs{
		snapshot:    0,
		performance: 0,
		pots:        0,
		bootstrap:   true,
	}, epochs)

	epochs, ok = stakeRewardEpochsForApplication(2)
	require.True(t, ok)
	require.Equal(t, stakeRewardEpochs{
		snapshot:    0,
		performance: 0,
		pots:        1,
		bootstrap:   true,
	}, epochs)

	epochs, ok = stakeRewardEpochsForApplication(3)
	require.True(t, ok)
	require.Equal(t, stakeRewardEpochs{
		snapshot:    0,
		performance: 1,
		pots:        2,
	}, epochs)
}

func TestSuppressBootstrapStakeRewardsReturnsAvailableRewardsToReserves(
	t *testing.T,
) {
	t.Parallel()

	result := &rewards.Result{
		PoolRewards:      []rewards.PoolReward{{PoolReward: 600}},
		AccountRewards:   []rewards.AccountReward{{Amount: 600}},
		TotalRewardPot:   1_000,
		AvailableRewards: 800,
		EffectiveRewards: 600,
		Unspendable:      50,
		Undistributed:    150,
	}
	suppressBootstrapStakeRewards(result)

	require.Empty(t, result.PoolRewards)
	require.Empty(t, result.AccountRewards)
	require.Zero(t, result.EffectiveRewards)
	require.Zero(t, result.Unspendable)
	require.Equal(t, uint64(800), result.Undistributed)

	app := &stakeRewardApplication{
		params: rewards.Parameters{
			TreasuryExpansion: big.NewRat(1, 5),
		},
		pots: &models.RewardAdaPots{
			Reserves: types.Uint64(10_000),
			Treasury: types.Uint64(10),
		},
		totalRewardPot:   result.TotalRewardPot,
		availableRewards: result.AvailableRewards,
		undistributed:    result.Undistributed,
	}
	reserves, treasury, err := stakeRewardUpdatedPots(app)
	require.NoError(t, err)
	require.Equal(t, uint64(9_800), reserves)
	require.Equal(t, uint64(210), treasury)
}

func TestBootstrapStakeRewardsRejectStalePrecompute(t *testing.T) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	require.NoError(t, db.Metadata().SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:   1,
		Rewards: types.Uint64(1_000),
	}, nil))

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.precomputedStakeRewardApplication(txn, 2, 200)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)

	app, ok, err = ls.precomputeStakeRewardsCalculate(txn, 2, 100, 200)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)
}

// pendingRoundReads is every reader outside the boundary that can see a
// pending round, rendered so two observations compare with require.Equal.
type pendingRoundReads struct {
	liveInputs     string
	boundaryStake  string
	stakeAtSlot    string
	localStateDist string
	drepPower      string
	drepSingle     string
	// Readers with no reward term, which a pending round cannot move.
	utxoStake  string
	controlled string
}

func renderStakeMaps(stakes, delegators map[string]uint64) string {
	lines := make([]string, 0, len(stakes))
	for key, stake := range stakes {
		lines = append(lines, fmt.Sprintf("%x=%d/%d", key, stake, delegators[key]))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

func observePendingRoundReaders(
	t *testing.T,
	f *epochBoundaryBenchFixture,
	credits map[string]uint64,
) pendingRoundReads {
	t.Helper()
	meta := f.db.Metadata()
	pools, err := meta.GetDelegatedPoolKeyHashes(nil)
	require.NoError(t, err)
	require.NotEmpty(t, pools)
	boundary := epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch + 1)
	var ret pendingRoundReads

	inputs, err := meta.GetLiveStakeInputsForPools(pools, 0, nil)
	require.NoError(t, err)
	lines := make([]string, 0, len(inputs))
	for _, input := range inputs {
		lines = append(lines, fmt.Sprintf("%x/%d/%x=%d",
			input.PoolKeyHash, input.CredentialTag, input.StakingKey,
			uint64(input.Stake)))
	}
	sort.Strings(lines)
	ret.liveInputs = strings.Join(lines, "\n")

	stakes, delegators, err := meta.GetEpochBoundaryStakeByPools(
		pools, boundary-1, boundary, 0, 0, nil,
	)
	require.NoError(t, err)
	ret.boundaryStake = renderStakeMaps(stakes, delegators)

	stakes, delegators, err = meta.GetStakeByPoolsAtSlot(
		pools, boundary+10, 0, 0, nil,
	)
	require.NoError(t, err)
	ret.stakeAtSlot = renderStakeMaps(stakes, delegators)

	dist, err := f.ls.queryShelleyStakeDistribution(QueryPoint{}, nil)
	require.NoError(t, err)
	ret.localStateDist = fmt.Sprintf("%+v", dist)

	ret.drepPower = dumpDRepVotingPower(t, f)
	dreps, err := f.db.GetActiveDreps(nil)
	require.NoError(t, err)
	singles := make([]string, 0, len(dreps))
	for _, drep := range dreps {
		power, err := meta.GetDRepVotingPower(
			drep.CredentialTag, drep.Credential, 0, nil,
		)
		require.NoError(t, err)
		singles = append(singles, fmt.Sprintf("%x=%d", drep.Credential, power))
	}
	sort.Strings(singles)
	ret.drepSingle = strings.Join(singles, "\n")

	stakes, delegators, err = meta.GetStakeByPools(pools, nil)
	require.NoError(t, err)
	ret.utxoStake = renderStakeMaps(stakes, delegators)
	amounts := make([]string, 0, len(credits))
	for key := range credits {
		amount, err := f.db.GetControlledAmountByCredential(0, []byte(key), nil)
		require.NoError(t, err)
		amounts = append(amounts, fmt.Sprintf("%x=%d", key, amount))
	}
	sort.Strings(amounts)
	ret.controlled = strings.Join(amounts, "\n")
	return ret
}

// TestPendingRewardRoundAggregateReadsCountEachCreditOnce pins every aggregate
// and historical reader against a credited round both before any of its
// credits is folded and after some are, as by withdrawals: a credit must be
// counted exactly once, from the account row or from the round, so no reading
// moves when the rest are folded.
func TestPendingRewardRoundAggregateReadsCountEachCreditOnce(t *testing.T) {
	t.Parallel()
	f := creditedRewardRound(t)
	credits := rewardedCredentials(t, f)
	stored := accountRewards(t, f, credits)
	unfolded := observePendingRoundReaders(t, f, credits)

	keys := make([]string, 0, len(credits))
	for key := range credits {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	require.Greater(t, len(keys), 40)
	txn := f.db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		for _, key := range keys[:len(keys)/3] {
			if err := f.ls.foldRewardCreditFor(txn, 0, []byte(key)); err != nil {
				return err
			}
		}
		return nil
	}))
	first, last := keys[0], keys[len(keys)-1]
	require.Equal(t, stored[first]+credits[first],
		accountRewards(t, f, credits)[first], "control: a folded credit")
	require.Equal(t, stored[last], accountRewards(t, f, credits)[last],
		"control: an unfolded credit")
	partial := observePendingRoundReaders(t, f, credits)

	settleRewardCredits(t, f.ls)
	require.Equal(t, stored[last]+credits[last],
		accountRewards(t, f, credits)[last])
	after := observePendingRoundReaders(t, f, credits)

	for name, got := range map[string]pendingRoundReads{
		"unfolded": unfolded, "partly folded": partial,
	} {
		require.Equal(t, after.liveInputs, got.liveInputs,
			"%s: live stake inputs", name)
		require.Equal(t, after.boundaryStake, got.boundaryStake,
			"%s: boundary stake reconstruction", name)
		require.Equal(t, after.stakeAtSlot, got.stakeAtSlot,
			"%s: stake at slot reconstruction", name)
		require.Equal(t, after.localStateDist, got.localStateDist,
			"%s: local state query stake distribution", name)
		require.Equal(t, after.drepPower, got.drepPower,
			"%s: DRep voting power", name)
		require.Equal(t, after.drepSingle, got.drepSingle,
			"%s: single DRep voting power", name)
		require.Equal(t, after.utxoStake, got.utxoStake,
			"%s: UTxO-only pool stake", name)
		require.Equal(t, after.controlled, got.controlled,
			"%s: controlled amount", name)
	}
}

// The first RUPD reads an empty nesBprev, not epoch 0's nesBcur.
// With d=0 this gives eta=0; the same 180 blocks enter the next update,
// giving eta=180/(500*0.4)=0.9. Fees collected in epoch 0 enter that update
// too. These are the reference devnet inputs and pots from issue #4502.
func TestApplyStakeRewardsConwayGenesisPerformance(t *testing.T) {
	t.Parallel()
	ls, db := newRewardCalculationTestLedger(t)
	ls.currentEra = eras.ConwayEraDesc
	require.NoError(t, ls.config.CardanoNodeConfig.
		LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.4,
		"epochLength": 500,
		"maxLovelaceSupply": 6000000000000,
		"securityParam": 40,
		"slotLength": 1,
		"systemStart": "2022-10-25T00:00:00Z"
	}`)))
	pp := mockledger.NewMockConwayProtocolParams()
	pp.NOpt = 150
	pp.A0 = rewardCalcRat(3, 10)
	pp.Rho = rewardCalcRat(3, 1_000)
	pp.Tau = rewardCalcRat(1, 5)
	pp.ProtocolVersion = lcommon.ProtocolParametersProtocolVersion{Major: 10}
	encoded, err := cbor.Encode(&pp)
	require.NoError(t, err)
	meta := db.Metadata()
	for epoch := range uint64(3) {
		require.NoError(t, meta.SetEpoch(
			epoch*500, epoch, nil, nil, nil, nil,
			eras.ConwayEraDesc.Id, 1, 500, nil,
		))
		require.NoError(t, db.SetPParams(
			encoded, epoch*500, epoch, eras.ConwayEraDesc.Id, nil,
		))
	}
	require.NoError(t, meta.SetNetworkState(0, 2_000_000_000_000, 0, nil))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch: 0, Reserves: 2_000_000_000_000,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch: 0, SnapshotType: "mark", ProtocolVersion: 10,
		TotalActiveStake: 2_000_000_000_000,
		TotalPoolCount:   2, TotalDelegators: 2,
	}, nil))
	for _, key := range []byte{0x11, 0x22} {
		poolKey := rewardCalcHash(key)
		poolID := seedLiveStakeFixture(
			t, db, poolKey, bytes.Repeat([]byte{key}, 32),
			1_000_000_000_000, 0,
		)
		require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{{
			Epoch: 0, PoolKeyHash: poolKey, RewardAccount: poolKey,
			Margin:         &types.Rat{Rat: big.NewRat(0, 1)},
			DelegatedStake: 1_000_000_000_000, DelegatorCount: 1,
		}}, nil))
		require.NoError(
			t,
			meta.SaveRewardStakeInputs([]*models.RewardStakeInput{{
				Epoch: 0, PoolKeyHash: poolKey, StakingKey: poolKey,
				Stake: 1_000_000_000_000, Registered: true,
			}}, nil),
		)
		for i := range uint64(90) {
			require.NoError(t, db.UpdatePoolOpCertSequence(
				poolID, i+1, 1+2*i+uint64(key), nil,
			))
		}
	}
	_, err = rewardCalcSQLDB(t, db).Exec(`
INSERT INTO "transaction" (
    id, hash, block_hash, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (1, ?, ?, 60, 7, '400000', '0', '0', 0, TRUE)`,
		[]byte("genesis-performance-tx"), []byte("genesis-performance-block"))
	require.NoError(t, err)

	for _, tc := range []struct {
		epoch    uint64
		treasury uint64
		reserves uint64
		fraction *big.Rat
	}{
		{1, 0, 2_000_000_000_000, big.NewRat(1, 4)},
		{2, 1_080_080_000, 1_998_920_320_000, big.NewRat(1_562_500, 6_251_687)},
	} {
		boundary := tc.epoch * 500
		ended, err := meta.GetEpoch(tc.epoch-1, nil)
		require.NoError(t, err)
		require.NotNil(t, ended)
		txn := db.Transaction(true)
		require.NoError(t, txn.Do(func(txn *database.Txn) error {
			if err := ls.applyStakeRewards(txn, tc.epoch, boundary); err != nil {
				return err
			}
			return ls.saveRewardAdaPotsForEpoch(txn, tc.epoch, *ended, boundary)
		}))
		state, err := meta.GetNetworkState(nil)
		require.NoError(t, err)
		require.NotNil(t, state)
		require.Equal(t, tc.treasury, uint64(state.Treasury),
			"treasury at boundary into epoch %d", tc.epoch)
		require.Equal(t, tc.reserves, uint64(state.Reserves),
			"reserves at boundary into epoch %d", tc.epoch)
		pots, err := meta.GetRewardAdaPots(tc.epoch, nil)
		require.NoError(t, err)
		require.NotNil(t, pots)
		require.Equal(t, state.Treasury, pots.Treasury)
		require.Equal(t, state.Reserves, pots.Reserves)
		if tc.epoch == 1 {
			require.Equal(t, uint64(400_000), uint64(pots.Fees))
		}

		hash := bytes.Repeat([]byte{byte(tc.epoch)}, 32)
		seedBlockAtSlot(t, ls, boundary, hash)
		require.NoError(t, db.SetTip(ochainsync.Tip{
			Point: ocommon.NewPoint(boundary, hash),
		}, nil))
		result, err := ls.Query(stakeDistributionQuery(), QueryPoint{})
		require.NoError(t, err)
		dist := decodeStakeDistributionResult(t, result)
		require.Len(t, dist.Results, 2)
		for _, entry := range dist.Results {
			require.Equal(t, tc.fraction, entry.StakeFraction.Rat,
				"stake fraction at boundary into epoch %d", tc.epoch)
		}
	}
}

// A Mithril bootstrap anchored mid-epoch seeds the imported epoch's own
// RewardAdaPots row with ImportedEpochFees (the fees collected up to and
// including the anchor block) and a CapturedSlot at the anchor. The node's
// locally stored transactions for that epoch only cover slots after the
// anchor -- plus, once the historical backfill (#4061) has run, slots at or
// before it too. saveRewardAdaPotsForEpoch must sum the local fees strictly
// after the anchor and add the imported amount, not sum the whole epoch:
// summing the whole epoch either silently drops the pre-anchor fees (the
// defect in dingo #3975) or double-counts them once backfill has stored
// pre-anchor transactions locally.
func TestSaveRewardAdaPotsForEpochUsesImportedPreAnchorFees(t *testing.T) {
	t.Parallel()
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		endedEpoch           = uint64(5)
		epochStartSlot       = uint64(1000)
		epochLengthInSlots   = uint(100) // slots [1000, 1099]
		anchorSlot           = uint64(1050)
		importedPreAnchor    = uint64(1_000_000)
		postAnchorFee        = uint64(500_000)
		preAnchorBackfillFee = uint64(300_000)
		newEpochBoundarySlot = uint64(1100)
	)

	// Simulates seedImportedRewardBasis's write for the anchor epoch: the
	// pots row this epoch's own boundary would have produced, had the node
	// been running, carrying the pre-anchor fee pot the import derived from
	// State.Fees - snapshots.Fee.
	importedFees := types.Uint64(importedPreAnchor)
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:             endedEpoch,
		CapturedSlot:      anchorSlot,
		ImportedEpochFees: &importedFees,
	}, nil))

	// A transaction at the anchor slot itself: excluded, because the
	// imported amount already accounts for fees up to and including the
	// anchor block. Sum range is (CapturedSlot, epochEnd].
	rewardCalcExecRows(t, db, `
INSERT INTO "transaction" (
    id, hash, block_hash, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (1, ?, ?, ?, 7, ?, '0', '0', 0, TRUE)`,
		[]byte("pre-anchor-backfill-tx"), []byte("pre-anchor-block"),
		anchorSlot, strconv.FormatUint(preAnchorBackfillFee, 10),
	)
	// A transaction after the anchor: included.
	rewardCalcExecRows(t, db, `
INSERT INTO "transaction" (
    id, hash, block_hash, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (2, ?, ?, ?, 7, ?, '0', '0', 0, TRUE)`,
		[]byte("post-anchor-tx"), []byte("post-anchor-block"),
		anchorSlot+30, strconv.FormatUint(postAnchorFee, 10),
	)

	ended := models.Epoch{
		EpochId:       endedEpoch,
		StartSlot:     epochStartSlot,
		LengthInSlots: epochLengthInSlots,
	}
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.saveRewardAdaPotsForEpoch(
			txn, endedEpoch+1, ended, newEpochBoundarySlot,
		)
	}))

	pots, err := meta.GetRewardAdaPots(endedEpoch+1, nil)
	require.NoError(t, err)
	require.NotNil(t, pots)
	require.Equal(
		t,
		importedPreAnchor+postAnchorFee,
		uint64(pots.Fees),
		"fees for the epoch after an imported anchor epoch must be the "+
			"imported pre-anchor amount plus only the post-anchor local sum",
	)
}

// The imported pots row's CapturedSlot is the anchor block's slot, which can
// be any slot of its epoch, including the first and the last. The anchor
// block's own fees are part of ImportedEpochFees at both ends, so the local
// sum must exclude the anchor slot and still add the imported amount.
func TestSaveRewardAdaPotsForEpochImportedAnchorAtEpochEdges(t *testing.T) {
	t.Parallel()
	const (
		endedEpoch         = uint64(5)
		epochStartSlot     = uint64(1000)
		epochLengthInSlots = uint(100) // slots [1000, 1099]
		epochEndSlot       = uint64(1099)
		importedPreAnchor  = uint64(1_000_000)
		anchorBlockFee     = uint64(300_000)
		laterFee           = uint64(500_000)
	)
	tests := []struct {
		name       string
		anchorSlot uint64
		laterSlot  uint64
		want       uint64
	}{
		{
			name:       "anchor at first slot",
			anchorSlot: epochStartSlot,
			laterSlot:  epochStartSlot + 1,
			want:       importedPreAnchor + laterFee,
		},
		{
			name:       "anchor at last slot",
			anchorSlot: epochEndSlot,
			want:       importedPreAnchor,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ls, db := newRewardCalculationTestLedger(t)
			meta := db.Metadata()
			importedFees := types.Uint64(importedPreAnchor)
			require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
				Epoch:             endedEpoch,
				CapturedSlot:      tc.anchorSlot,
				ImportedEpochFees: &importedFees,
			}, nil))
			// A backfilled copy of the anchor block's transaction, already
			// counted in ImportedEpochFees.
			rewardCalcExecRows(t, db, `
INSERT INTO "transaction" (
    id, hash, block_hash, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (1, ?, ?, ?, 7, ?, '0', '0', 0, TRUE)`,
				[]byte("anchor-tx"), []byte("anchor-block"),
				tc.anchorSlot, strconv.FormatUint(anchorBlockFee, 10),
			)
			if tc.laterSlot != 0 {
				rewardCalcExecRows(t, db, `
INSERT INTO "transaction" (
    id, hash, block_hash, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (2, ?, ?, ?, 7, ?, '0', '0', 0, TRUE)`,
					[]byte("later-tx"), []byte("later-block"),
					tc.laterSlot, strconv.FormatUint(laterFee, 10),
				)
			}
			ended := models.Epoch{
				EpochId:       endedEpoch,
				StartSlot:     epochStartSlot,
				LengthInSlots: epochLengthInSlots,
			}
			txn := db.Transaction(true)
			require.NoError(t, txn.Do(func(txn *database.Txn) error {
				return ls.saveRewardAdaPotsForEpoch(
					txn, endedEpoch+1, ended, epochEndSlot+1,
				)
			}))
			pots, err := meta.GetRewardAdaPots(endedEpoch+1, nil)
			require.NoError(t, err)
			require.NotNil(t, pots)
			require.Equal(t, tc.want, uint64(pots.Fees))
		})
	}
}

// TestRewardParametersDecentralizationIsZeroWhenCalculatedInBabbage pins the
// d that startStep reads across the Alonzo to Babbage boundary. The reward
// update runs in the epoch after the performance epoch, in that epoch's era.
// Babbage's PParams has no d field (ppDG = to (const minBound)), and the
// translated prevPParams read back as 0, so the round for the last Alonzo
// epoch uses d = 0 even though the Alonzo parameters held d = 7/10. Reading
// d from the performance epoch's Alonzo parameters overstates eta's
// expectedBlocks denominator reduction and inflates every reward of the
// round (Prime Mainnet performance epoch 39).
//
// Block counts are the exception: BBODY accumulated the performance epoch's
// BlocksMade under that epoch's curPParams, so incrBlocks skipped overlay
// slots with the Alonzo d. The d returned for block counting must stay the
// performance epoch's.
func TestRewardParametersDecentralizationIsZeroWhenCalculatedInBabbage(
	t *testing.T,
) {
	t.Parallel()

	const (
		performanceEpoch = uint64(2)
		potsEpoch        = uint64(3)
	)
	tests := []struct {
		name         string
		calcEra      uint
		expectedDRat *big.Rat
	}{
		{
			name:         "alonzo calculation keeps the performance epoch d",
			calcEra:      eras.AlonzoEraDesc.Id,
			expectedDRat: big.NewRat(7, 10),
		},
		{
			name:         "babbage calculation reads d as zero",
			calcEra:      eras.BabbageEraDesc.Id,
			expectedDRat: big.NewRat(0, 1),
		},
		{
			name:         "conway calculation reads d as zero",
			calcEra:      eras.ConwayEraDesc.Id,
			expectedDRat: big.NewRat(0, 1),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ls, db := newRewardCalculationTestLedger(t)
			meta := db.Metadata()
			pparams := &alonzo.AlonzoProtocolParameters{
				NOpt:             10,
				A0:               rewardCalcRat(0, 1),
				Rho:              rewardCalcRat(1, 100),
				Tau:              rewardCalcRat(1, 5),
				Decentralization: rewardCalcRat(7, 10),
				ProtocolMajor:    6,
			}
			pparamsCbor, err := cbor.Encode(pparams)
			require.NoError(t, err)
			require.NoError(t, meta.SetEpoch(
				100, performanceEpoch, nil, nil, nil, nil,
				eras.AlonzoEraDesc.Id, 1, 100, nil,
			))
			require.NoError(t, meta.SetEpoch(
				200, potsEpoch, nil, nil, nil, nil,
				tc.calcEra, 1, 1_000, nil,
			))
			require.NoError(t, db.SetPParams(
				pparamsCbor, 100, performanceEpoch,
				eras.AlonzoEraDesc.Id, nil,
			))

			txn := db.Transaction(false)
			defer func() { _ = txn.Rollback() }()
			_, params, performanceD, err := ls.rewardParameters(
				txn,
				performanceEpoch,
				potsEpoch,
				&models.RewardAdaPots{Reserves: 100_000_000},
			)
			require.NoError(t, err)
			require.Zero(t, tc.expectedDRat.Cmp(params.Decentralization),
				"d = %s, want %s", params.Decentralization, tc.expectedDRat)
			require.Zero(t, big.NewRat(7, 10).Cmp(performanceD),
				"block-count d = %s, want 7/10", performanceD)
			require.Equal(t, big.NewRat(1, 5), params.TreasuryExpansion,
				"tau still comes from the performance epoch")
		})
	}
}

func TestRewardPrecomputeCoalescesEpochTransitionBurst(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	enqueued := make(chan uint64)
	var (
		processedMu sync.Mutex
		processed   []uint64
	)
	precompute := func(evt event.EpochTransitionEvent) error {
		if evt.NewEpoch == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		processedMu.Lock()
		processed = append(processed, evt.NewEpoch)
		processedMu.Unlock()
		return nil
	}
	eventBus.SubscribeFunc(
		event.EpochTransitionEventType,
		func(evt event.Event) {
			ls.handleRewardPrecomputeEpochTransitionWith(evt, precompute)
			epochEvt := evt.Data.(event.EpochTransitionEvent)
			enqueued <- epochEvt.NewEpoch
		},
	)

	eventBus.Publish(
		event.EpochTransitionEventType,
		event.NewEvent(
			event.EpochTransitionEventType,
			event.EpochTransitionEvent{NewEpoch: 1, EpochNonce: []byte{1}},
		),
	)
	require.Equal(
		t,
		uint64(1),
		testutil.RequireReceive(
			t,
			enqueued,
			testutil.AsyncWait,
			"reward precompute callback did not enqueue first epoch",
		),
	)
	testutil.RequireReceive(
		t,
		firstStarted,
		testutil.AsyncWait,
		"first reward precompute did not start",
	)

	// Deliver a sequence longer than the EventBus default buffer's total capacity
	// while the first simulated calculation remains blocked. Waiting for each
	// callback isolates the behavior under test: callback delivery stays
	// independent of reward calculation, and the ledger retains only the newest
	// pending epoch.
	latestEpoch := uint64(event.DefaultSubscriberBuffer + 100)
	for epoch := uint64(2); epoch <= latestEpoch; epoch++ {
		eventBus.Publish(
			event.EpochTransitionEventType,
			event.NewEvent(
				event.EpochTransitionEventType,
				event.EpochTransitionEvent{
					NewEpoch:   epoch,
					EpochNonce: []byte{byte(epoch)},
				},
			),
		)
		require.Equal(
			t,
			epoch,
			testutil.RequireReceive(
				t,
				enqueued,
				testutil.AsyncWait,
				"reward precompute callback did not enqueue epoch",
			),
		)
	}
	close(releaseFirst)

	done := make(chan struct{})
	go func() {
		ls.rewardPrecomputeWG.Wait()
		close(done)
	}()
	testutil.RequireReceive(
		t,
		done,
		testutil.AsyncWait,
		"coalesced reward precompute did not finish",
	)

	processedMu.Lock()
	defer processedMu.Unlock()
	require.Equal(t, []uint64{1, latestEpoch}, processed)
}

func TestRewardPrecomputeContinuesAfterPanic(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var processed []uint64
	precompute := func(evt event.EpochTransitionEvent) error {
		if evt.NewEpoch == 1 {
			close(firstStarted)
			<-releaseFirst
			panic("broken reward input")
		}
		processed = append(processed, evt.NewEpoch)
		return nil
	}

	ls.queueRewardPrecompute(
		event.EpochTransitionEvent{NewEpoch: 1, EpochNonce: []byte{1}},
		precompute,
	)
	testutil.RequireReceive(
		t,
		firstStarted,
		testutil.AsyncWait,
		"panicking reward precompute did not start",
	)
	ls.queueRewardPrecompute(
		event.EpochTransitionEvent{NewEpoch: 2, EpochNonce: []byte{2}},
		precompute,
	)
	ls.queueRewardPrecompute(
		event.EpochTransitionEvent{NewEpoch: 3, EpochNonce: []byte{3}},
		precompute,
	)
	close(releaseFirst)

	done := make(chan struct{})
	go func() {
		ls.rewardPrecomputeWG.Wait()
		close(done)
	}()
	testutil.RequireReceive(
		t,
		done,
		testutil.AsyncWait,
		"reward precompute worker stopped after panic",
	)
	require.Equal(t, []uint64{3}, processed)
}

func TestRollbackRequeuesRewardPrecompute(t *testing.T) {
	t.Parallel()

	for _, crossEpoch := range []bool{false, true} {
		name := "same epoch"
		if crossEpoch {
			name = "previous epoch"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			ls := fixture.ls
			nonce := testHashBytes("reward-epoch")
			epochLength := uint(100)
			if crossEpoch {
				epochLength = 15
			}
			require.NoError(t, ls.db.SetEpoch(
				0, 3, nonce, nil, nil, nil,
				eras.ShelleyEraDesc.Id, 1000, epochLength, nil,
			))
			pp, err := cbor.Encode(&shelley.ShelleyProtocolParameters{
				ProtocolMajor: 7,
			})
			require.NoError(t, err)
			require.NoError(t, ls.db.SetPParams(
				pp, 0, 3, eras.ShelleyEraDesc.Id, nil,
			))
			epochID := uint64(3)
			if crossEpoch {
				epochID = 4
				require.NoError(t, ls.db.SetEpoch(
					15, epochID, testHashBytes("rolled-away-epoch"),
					nil, nil, nil, eras.ShelleyEraDesc.Id, 1000, 15, nil,
				))
			}
			epoch, err := ls.db.Metadata().GetEpoch(epochID, nil)
			require.NoError(t, err)
			ls.currentEpoch = *epoch
			ls.currentEra = eras.ShelleyEraDesc
			// Keep the worker occupied so the replacement event remains
			// observable after the real rollback path returns.
			ls.rewardPrecomputeRunning = true
			ls.rewardPrecomputePending = &event.EpochTransitionEvent{
				NewEpoch: epochID + 1,
			}
			ls.rewardPrecomputeRetry = &stakeRewardPrecomputeRetry{
				epochEvent: event.EpochTransitionEvent{NewEpoch: epochID + 1},
				cutoffSlot: 100,
			}

			require.NoError(t, ls.rollbackWithBlocks(
				fixture.ancestorTip.Point, nil, false,
			))

			require.Zero(t, ls.rewardInputRollbackActive.Load())
			require.Equal(t, uint64(2), ls.rewardInputGeneration.Load())
			ls.rewardPrecomputeMu.Lock()
			defer ls.rewardPrecomputeMu.Unlock()
			pending := ls.rewardPrecomputePending
			require.NotNil(t, pending, "rollback must replace invalidated work")
			require.Equal(t, uint64(3), pending.NewEpoch,
				"replacement must calculate the surviving epoch's rewards")
			require.Equal(
				t,
				fixture.ancestorTip.Point.Slot,
				pending.BoundarySlot,
				"capture must use the surviving applied tip",
			)
			require.Equal(t, nonce, pending.EpochNonce)
			require.Nil(t, ls.rewardPrecomputeRetry,
				"a rolled-away prefilter retry must not replace fresh work")
		})
	}
}

func TestRollbackTransactionFailureRestoresRewardPrecompute(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	nonce := testHashBytes("reward-epoch")
	require.NoError(t, ls.db.SetEpoch(
		0, 3, nonce, nil, nil, nil,
		eras.ShelleyEraDesc.Id, 1000, 100, nil,
	))
	epoch, err := ls.db.Metadata().GetEpoch(3, nil)
	require.NoError(t, err)
	ls.currentEpoch = *epoch
	ls.currentEra = eras.ShelleyEraDesc
	ls.rewardPrecomputeRunning = true
	queued := &event.EpochTransitionEvent{
		NewEpoch:     4,
		BoundarySlot: 300,
		EpochNonce:   nonce,
	}
	ls.rewardPrecomputePending = queued
	retry := &stakeRewardPrecomputeRetry{
		epochEvent: event.EpochTransitionEvent{NewEpoch: 4},
		cutoffSlot: 300,
		generation: ls.rewardInputGeneration.Load(),
	}
	ls.rewardPrecomputeRetry = retry
	transactionErr := errors.New("injected rollback transaction failure")
	failLedgerRollbackAfterChainTruncation(t, ls, transactionErr)

	err = ls.rollbackWithBlocks(fixture.ancestorTip.Point, nil, false)

	require.ErrorIs(t, err, transactionErr)
	ls.rewardPrecomputeMu.Lock()
	defer ls.rewardPrecomputeMu.Unlock()
	require.NotNil(t, ls.rewardPrecomputePending,
		"a failed rollback must restore the queued transition")
	require.Equal(t, queued.NewEpoch, ls.rewardPrecomputePending.NewEpoch)
	require.Equal(
		t,
		queued.BoundarySlot,
		ls.rewardPrecomputePending.BoundarySlot,
	)
	require.Equal(t, queued.EpochNonce, ls.rewardPrecomputePending.EpochNonce)
	require.NotNil(t, ls.rewardPrecomputeRetry,
		"a failed rollback must restore the deferred prefilter retry")
	require.Equal(t, retry.cutoffSlot, ls.rewardPrecomputeRetry.cutoffSlot)
	require.Equal(t, retry.epochEvent.NewEpoch,
		ls.rewardPrecomputeRetry.epochEvent.NewEpoch)
	require.Equal(t, ls.rewardInputGeneration.Load(),
		ls.rewardPrecomputeRetry.generation,
		"the restored retry must use the new stable generation")
}

func TestRollbackRewardPrecomputePersistsReusableOutputs(t *testing.T) {
	t.Parallel()

	for _, protocolMajor := range []uint{6, 7} {
		t.Run(fmt.Sprintf("protocol %d", protocolMajor), func(t *testing.T) {
			t.Parallel()
			seed, db := seedRewardPrecomputeTimingState(t, protocolMajor)
			cm, err := chain.NewManager(db, nil)
			require.NoError(t, err)
			require.NoError(
				t,
				cm.SetLedger(testSecurityParamLedger{securityParam: 2}),
			)
			nonce := testHashBytes("reward-epoch")
			require.NoError(t, db.SetEpoch(
				200, 3, nonce, nil, nil, nil,
				eras.ShelleyEraDesc.Id, 1, 1_000, nil,
			))
			cfg := seed.config
			cfg.Database = db
			cfg.ChainManager = cm
			ls, err := NewLedgerState(cfg)
			require.NoError(t, err)
			ls.metrics.init(prometheus.NewRegistry())
			t.Cleanup(func() { require.NoError(t, ls.Close()) })
			cutoff, err := ls.rewardPrefilterSlot(db.Metadata(), nil, 3)
			require.NoError(t, err)
			ancestor := chain.RawBlock{
				Slot: cutoff + 1, Hash: testHashBytes("reward-ancestor"),
				BlockNumber: 1, Type: 1, Cbor: []byte{0x80},
			}
			current := chain.RawBlock{
				Slot: cutoff + 2, Hash: testHashBytes("reward-current"),
				PrevHash:    ancestor.Hash,
				BlockNumber: 2, Type: 1, Cbor: []byte{0x80},
			}
			require.NoError(t, cm.PrimaryChain().AddRawBlocks(
				[]chain.RawBlock{ancestor, current},
			))
			for _, block := range []chain.RawBlock{ancestor, current} {
				require.NoError(t, db.SetBlockNonce(
					block.Hash, block.Slot, nonce, true, nil,
				))
			}
			ls.currentTip = ochainsync.Tip{
				Point:       ocommon.NewPoint(current.Slot, current.Hash),
				BlockNumber: current.BlockNumber,
			}
			require.NoError(t, db.SetTip(ls.currentTip, nil))

			require.NoError(t, ls.rollbackWithBlocks(
				ocommon.NewPoint(ancestor.Slot, ancestor.Hash), nil, false,
			))
			ls.rewardPrecomputeWG.Wait()

			outputs, err := db.Metadata().GetRewardPoolOutputs(1, nil)
			require.NoError(t, err)
			require.Len(t, outputs, 1,
				"rollback must replace discarded work before the next boundary")
			require.Equal(t, ancestor.Slot, outputs[0].CapturedSlot)
			require.Equal(t, uint64(1_200), outputs[0].BoundarySlot)
			txn := db.Transaction(false)
			require.NoError(t, txn.Do(func(txn *database.Txn) error {
				app, ok, err := ls.precomputedStakeRewardApplication(
					txn,
					4,
					1_200,
				)
				require.NoError(t, err)
				require.True(t, ok, "next boundary must reuse the replacement")
				require.NotNil(t, app)
				return nil
			}))
		})
	}
}

func TestRewardPrecomputeRetryRejectsAbandonedGeneration(t *testing.T) {
	t.Parallel()

	for _, active := range []bool{false, true} {
		t.Run(fmt.Sprintf("rollback active %t", active), func(t *testing.T) {
			t.Parallel()
			ls := &LedgerState{rewardPrecomputeRunning: true}
			if active {
				ls.rewardInputRollbackActive.Add(1)
			} else {
				ls.rewardInputGeneration.Add(2)
			}
			ls.deferStakeRewardPrecompute(4, 100, 0)
			require.Nil(t, ls.rewardPrecomputeRetry,
				"an old calculation must not reinstall an abandoned retry")

			ls.rewardPrecomputeRetry = &stakeRewardPrecomputeRetry{
				epochEvent: event.EpochTransitionEvent{NewEpoch: 3},
				cutoffSlot: 100,
			}
			ls.maybeQueueStakeRewardPrecomputeRetry(100)
			require.Nil(t, ls.rewardPrecomputePending,
				"an abandoned retry must not replace the current pending epoch")
			require.Nil(t, ls.rewardPrecomputeRetry)
		})
	}
}

func TestRollbackDoesNotRestartRewardsWithoutRestoredState(t *testing.T) {
	t.Parallel()

	for _, noop := range []bool{false, true} {
		t.Run(fmt.Sprintf("no-op %t", noop), func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			ls := fixture.ls
			// An unknown surviving era forces the post-commit state reload
			// to fail; a no-op rollback must not reach that reload at all.
			require.NoError(t, ls.db.SetEpoch(
				0, 3, testHashBytes("unknown-era"), nil, nil, nil,
				255, 1000, 100, nil,
			))
			ls.rewardPrecomputeRunning = true
			pending := &event.EpochTransitionEvent{NewEpoch: 3}
			ls.rewardPrecomputePending = pending
			point := fixture.ancestorTip.Point
			if noop {
				point = fixture.currentTip.Point
			}

			err := ls.rollbackWithBlocks(point, nil, false)
			if noop {
				require.NoError(t, err)
				require.Same(t, pending, ls.rewardPrecomputePending)
				require.Zero(t, ls.rewardInputGeneration.Load())
			} else {
				require.ErrorContains(t, err, "unknown era ID 255")
				require.Nil(t, ls.rewardPrecomputePending,
					"failed reload must not schedule rewards against stale state")
				require.Equal(t, uint64(2), ls.rewardInputGeneration.Load())
			}
			require.Zero(t, ls.rewardInputRollbackActive.Load())
		})
	}
}

func TestCommittedRollbackWithFloorFailureRequeuesRewardPrecompute(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	nonce := testHashBytes("reward-epoch")
	require.NoError(t, ls.db.SetEpoch(
		0, 3, nonce, nil, nil, nil,
		eras.ShelleyEraDesc.Id, 1000, 100, nil,
	))
	pp, err := cbor.Encode(&shelley.ShelleyProtocolParameters{
		ProtocolMajor: 7,
	})
	require.NoError(t, err)
	require.NoError(t, ls.db.SetPParams(
		pp, 0, 3, eras.ShelleyEraDesc.Id, nil,
	))
	epoch, err := ls.db.Metadata().GetEpoch(3, nil)
	require.NoError(t, err)
	ls.currentEpoch = *epoch
	ls.currentEra = eras.ShelleyEraDesc
	// Keep the worker occupied so the replacement stays observable.
	ls.rewardPrecomputeRunning = true
	ls.rewardPrecomputePending = &event.EpochTransitionEvent{NewEpoch: 4}
	floorErr := errors.New("injected durable floor lookup failure")
	base := ls.db
	failing, err := database.New(
		base.Config(),
		database.Stores{
			Blob: base.Blob(),
			Metadata: floorLookupFailingMetadataStore{
				MetadataStore: base.Metadata(),
				err:           floorErr,
			},
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, failing.Close()) })
	ls.db = failing

	err = ls.rollbackWithBlocks(fixture.ancestorTip.Point, nil, false)

	require.ErrorIs(t, err, floorErr)
	_, committed := errors.AsType[*rollbackCommittedError](err)
	require.True(
		t,
		committed,
		"the truncation committed before the floor check",
	)
	require.Equal(t, fixture.ancestorTip, ls.currentTip)
	ls.rewardPrecomputeMu.Lock()
	defer ls.rewardPrecomputeMu.Unlock()
	pending := ls.rewardPrecomputePending
	require.NotNil(t, pending,
		"a committed rollback must replace the work it invalidated")
	require.Equal(t, uint64(3), pending.NewEpoch)
	require.Equal(t, fixture.ancestorTip.Point.Slot, pending.BoundarySlot)
	require.Equal(t, nonce, pending.EpochNonce)
}

func TestRollbackFailureDuringCloseDoesNotRestoreRewardPrecompute(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	ls.rewardPrecomputeRunning = true
	ls.rewardPrecomputePending = &event.EpochTransitionEvent{
		NewEpoch:   4,
		EpochNonce: testHashBytes("reward-epoch"),
	}
	ls.rewardPrecomputeRetry = &stakeRewardPrecomputeRetry{
		epochEvent: event.EpochTransitionEvent{NewEpoch: 4},
		cutoffSlot: 300,
	}
	injected := errors.New("injected rollback failure during close")
	ls.rollbackTruncateAfterSlotFunc = func(
		ocommon.Point,
		uint64,
		*database.Txn,
	) (ochainsync.Tip, []byte, error) {
		// Close marks the ledger closed and discards queued precompute
		// work while this transaction is still open.
		ls.closed.Store(true)
		ls.rewardPrecomputeMu.Lock()
		ls.rewardPrecomputePending = nil
		ls.rewardPrecomputeRetry = nil
		ls.rewardPrecomputeMu.Unlock()
		return ochainsync.Tip{}, nil, injected
	}

	err := ls.rollbackWithBlocks(fixture.ancestorTip.Point, nil, false)

	require.ErrorIs(t, err, injected)
	ls.rewardPrecomputeMu.Lock()
	defer ls.rewardPrecomputeMu.Unlock()
	require.Nil(t, ls.rewardPrecomputePending,
		"a closed ledger must not re-arm the transition Close discarded")
	require.Nil(t, ls.rewardPrecomputeRetry,
		"a closed ledger must not re-arm the retry Close discarded")
}

// The pre-Babbage prefilter reads account registration at the RUPD slot, so a
// rollback across that slot can change which delegators are paid. The
// replacement must be derived from the surviving certificate history and match
// the authoritative boundary calculation exactly.
func TestRollbackRewardPrecomputeDropsAbandonedPrefilterHistory(
	t *testing.T,
) {
	t.Parallel()

	seed, db := seedRewardPrecomputeTimingState(t, 6)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))
	nonce := testHashBytes("reward-epoch")
	require.NoError(t, db.SetEpoch(
		200, 3, nonce, nil, nil, nil,
		eras.ShelleyEraDesc.Id, 1, 1_000, nil,
	))
	cfg := seed.config
	cfg.Database = db
	cfg.ChainManager = cm
	ls, err := NewLedgerState(cfg)
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
	epoch, err := db.Metadata().GetEpoch(3, nil)
	require.NoError(t, err)
	ls.currentEpoch = *epoch
	ls.currentEra = eras.ShelleyEraDesc
	cutoff, err := ls.rewardPrefilterSlot(db.Metadata(), nil, 3)
	require.NoError(t, err)
	member := rewardCalcHash(0x6a)
	// member is registered before the epoch; the abandoned chain deregisters
	// it after the rollback point and before the RUPD slot.
	rewardCalcSeedStakeCert(
		t, db, 21, member, 0, 150,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	rewardCalcSeedStakeCert(
		t, db, 22, member, 0, cutoff-5,
		uint(lcommon.CertificateTypeStakeDeregistration),
	)
	ancestor := chain.RawBlock{
		Slot: cutoff - 10, Hash: testHashBytes("prefilter-ancestor"),
		BlockNumber: 1, Type: 1, Cbor: []byte{0x80},
	}
	abandoned := chain.RawBlock{
		Slot: cutoff + 1, Hash: testHashBytes("prefilter-abandoned"),
		PrevHash:    ancestor.Hash,
		BlockNumber: 2, Type: 1, Cbor: []byte{0x80},
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(
		[]chain.RawBlock{ancestor, abandoned},
	))
	for _, block := range []chain.RawBlock{ancestor, abandoned} {
		require.NoError(t, db.SetBlockNonce(
			block.Hash, block.Slot, nonce, true, nil,
		))
	}
	ls.currentTip = ochainsync.Tip{
		Point:       ocommon.NewPoint(abandoned.Slot, abandoned.Hash),
		BlockNumber: abandoned.BlockNumber,
	}
	require.NoError(t, db.SetTip(ls.currentTip, nil))

	require.NoError(t, ls.precomputeStakeRewardsAfterEpochTransition(
		event.EpochTransitionEvent{
			NewEpoch:     3,
			BoundarySlot: abandoned.Slot,
			EpochNonce:   nonce,
		},
	))
	require.False(t, rewardOutputsPayKey(t, db, member),
		"control: the abandoned chain's prefilter excludes member")

	require.NoError(t, ls.rollbackWithBlocks(
		ocommon.NewPoint(ancestor.Slot, ancestor.Hash), nil, false,
	))
	ls.rewardPrecomputeWG.Wait()

	outputs, err := db.Metadata().GetRewardAccountOutputs(1, nil)
	require.NoError(t, err)
	require.Empty(t, outputs,
		"no output computed from the abandoned history may survive")
	ls.rewardPrecomputeMu.Lock()
	retry := ls.rewardPrecomputeRetry
	ls.rewardPrecomputeMu.Unlock()
	require.NotNil(t, retry,
		"the replacement must wait for the RUPD slot on the surviving chain")
	require.Equal(t, uint64(3), retry.epochEvent.NewEpoch)
	require.Equal(t, cutoff, retry.cutoffSlot)

	replacement := ocommon.NewPoint(
		cutoff+1, testHashBytes("prefilter-replacement"),
	)
	ls.Lock()
	ls.currentTip = ochainsync.Tip{Point: replacement, BlockNumber: 2}
	ls.Unlock()
	ls.maybeQueueStakeRewardPrecomputeRetry(replacement.Slot)
	ls.rewardPrecomputeWG.Wait()

	require.True(t, rewardOutputsPayKey(t, db, member),
		"the replacement must use the surviving registration history")
	txn := db.Transaction(false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		want, ok, err := ls.calculateStakeRewardApplication(
			txn, 4, replacement.Slot, 1_200, false,
		)
		require.NoError(t, err)
		require.True(t, ok)
		poolOutputs, err := db.Metadata().GetRewardPoolOutputs(
			1, txn.Metadata(),
		)
		require.NoError(t, err)
		accountOutputs, err := db.Metadata().GetRewardAccountOutputs(
			1, txn.Metadata(),
		)
		require.NoError(t, err)
		require.Equal(t,
			rewardPoolOutputAmounts(want.poolOutputs),
			rewardPoolOutputAmounts(poolOutputs),
		)
		require.Equal(t,
			rewardAccountOutputAmounts(want.accountOutputs),
			rewardAccountOutputAmounts(accountOutputs),
		)
		pots, err := db.Metadata().GetRewardAdaPots(3, txn.Metadata())
		require.NoError(t, err)
		require.Equal(t, want.totalRewardPot, uint64(pots.Rewards))
		_, reusable, err := ls.precomputedStakeRewardApplication(
			txn, 4, 1_200,
		)
		require.NoError(t, err)
		require.True(
			t,
			reusable,
			"the next boundary must reuse the replacement",
		)
		return nil
	}))
	account, err := db.GetAccountByCredential(0, member, true, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	require.Zero(t, uint64(account.Reward),
		"precomputation must not credit rewards before the boundary")
}

func rewardOutputsPayKey(
	t *testing.T,
	db *database.Database,
	stakingKey []byte,
) bool {
	t.Helper()
	outputs, err := db.Metadata().GetRewardAccountOutputs(1, nil)
	require.NoError(t, err)
	require.NotEmpty(t, outputs)
	for _, output := range outputs {
		if bytes.Equal(output.StakingKey, stakingKey) && output.Amount > 0 {
			return true
		}
	}
	return false
}

func rewardPoolOutputAmounts(outputs []*models.RewardPoolOutput) []string {
	ret := make([]string, 0, len(outputs))
	for _, output := range outputs {
		ret = append(ret, fmt.Sprintf(
			"%x total=%d leader=%d members=%d undistributed=%d unspendable=%d",
			output.PoolKeyHash,
			output.TotalReward,
			output.LeaderReward,
			output.MemberRewardTotal,
			output.Undistributed,
			output.Unspendable,
		))
	}
	slices.Sort(ret)
	return ret
}

func rewardAccountOutputAmounts(
	outputs []*models.RewardAccountOutput,
) []string {
	ret := make([]string, 0, len(outputs))
	for _, output := range outputs {
		ret = append(ret, fmt.Sprintf(
			"%d:%x %s pool=%x amount=%d spendable=%t",
			output.CredentialTag,
			output.StakingKey,
			output.RewardType,
			output.PoolKeyHash,
			output.Amount,
			output.Spendable,
		))
	}
	slices.Sort(ret)
	return ret
}

func TestLedgerStateStartQueuesStartupRewardPrecompute(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))

	nonce := []byte{0x30, 0x93, 0x65, 0x6a}
	require.NoError(t, db.SetEpoch(
		0, 0, nonce, nil, nil, nil,
		eras.ShelleyEraDesc.Id, 1, 100, nil,
	))
	pparamsCbor, err := cbor.Encode(&shelley.ShelleyProtocolParameters{
		ProtocolMajor: 7,
		ProtocolMinor: 0,
	})
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		pparamsCbor, 0, 0, eras.ShelleyEraDesc.Id, nil,
	))

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		ls.Close()
	})

	// Occupy the precompute worker slot before Start. queueRewardPrecompute
	// hands the event to a worker goroutine that clears
	// rewardPrecomputePending under the same mutex, so a worker that reaches
	// the mutex before the hook leaves nothing for the hook to observe. With
	// the slot taken the queued round stays pending, which is what Start owes
	// the in-progress epoch.
	ls.rewardPrecomputeMu.Lock()
	ls.rewardPrecomputeRunning = true
	ls.rewardPrecomputeMu.Unlock()

	startupQueued := make(chan struct{})
	ls.startupRewardPrecomputeHook = func() {
		ls.rewardPrecomputeMu.Lock()
		pending := ls.rewardPrecomputePending
		var queued event.EpochTransitionEvent
		if pending != nil {
			queued = *pending
		}
		ls.rewardPrecomputeMu.Unlock()
		require.NotNil(
			t, pending, "Start must queue the established current epoch",
		)
		require.Equal(t, uint64(0), queued.NewEpoch)
		require.Equal(t, nonce, queued.EpochNonce)
		close(startupQueued)
	}

	_ = ls.Start(t.Context())
	testutil.RequireReceive(t, startupQueued, 2*time.Second, "startup precompute queued")
}

// The EventBus subscription that drives the reward precompute only fires at an
// epoch boundary, so an epoch already in progress when the process starts has
// no event to carry it. Startup must queue that round itself, or the next
// boundary calculates it inline inside the rollover write transaction.
func TestQueueStartupRewardPrecomputeQueuesInProgressEpoch(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	ls.currentEpoch = models.Epoch{
		EpochId:       655,
		StartSlot:     197596800,
		LengthInSlots: 432000,
		Nonce:         []byte{0x30, 0x93, 0x65, 0x6a},
	}

	queued := make(chan event.EpochTransitionEvent, 1)
	ls.queueStartupRewardPrecomputeWith(
		func(evt event.EpochTransitionEvent) error {
			queued <- evt
			return nil
		},
	)

	evt := testutil.RequireReceive(
		t, queued, 2*time.Second, "startup precompute queued",
	)
	// precomputeStakeRewardsAfterEpochTransition derives the application epoch
	// as NewEpoch+1 and uses BoundarySlot as the capture slot, so these two
	// fields are what decide which round gets precomputed.
	require.Equal(t, uint64(655), evt.NewEpoch)
	require.Equal(t, uint64(197596800), evt.BoundarySlot)
	require.Equal(t, uint64(654), evt.PreviousEpoch)
	require.Equal(t, uint64(197596799), evt.SnapshotSlot)
	require.Equal(t, ls.currentEpoch.Nonce, evt.EpochNonce)
}

// A nonce-less or zero-length epoch is one that was never established, so there
// is no round to catch up and nothing should be queued. queueRewardPrecompute
// also drops an event without a nonce, so queueing one would spawn a worker
// that immediately does nothing.
func TestQueueStartupRewardPrecomputeSkipsUnestablishedEpoch(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		epoch models.Epoch
	}{
		{
			name: "no length",
			epoch: models.Epoch{
				EpochId: 655,
				Nonce:   []byte{0x01},
			},
		},
		{
			name: "no nonce",
			epoch: models.Epoch{
				EpochId:       655,
				LengthInSlots: 432000,
			},
		},
		{
			name:  "zero value",
			epoch: models.Epoch{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ls := &LedgerState{}
			ls.currentEpoch = tc.epoch

			queued := make(chan event.EpochTransitionEvent, 1)
			ls.queueStartupRewardPrecomputeWith(
				func(evt event.EpochTransitionEvent) error {
					queued <- evt
					return nil
				},
			)

			testutil.RequireNoReceive(
				t, queued, 100*time.Millisecond,
				"unestablished epoch must not queue a precompute",
			)
		})
	}
}

// Epoch 0 has no predecessor and starts at slot 0; neither derived field may
// underflow into a bogus epoch or slot.
func TestQueueStartupRewardPrecomputeHandlesEpochZero(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	ls.currentEpoch = models.Epoch{
		EpochId:       0,
		StartSlot:     0,
		LengthInSlots: 432000,
		Nonce:         []byte{0x01},
	}

	queued := make(chan event.EpochTransitionEvent, 1)
	ls.queueStartupRewardPrecomputeWith(
		func(evt event.EpochTransitionEvent) error {
			queued <- evt
			return nil
		},
	)

	evt := testutil.RequireReceive(
		t, queued, 2*time.Second, "startup precompute queued",
	)
	require.Equal(t, uint64(0), evt.NewEpoch)
	require.Equal(t, uint64(0), evt.PreviousEpoch)
	require.Equal(t, uint64(0), evt.SnapshotSlot)
	require.Equal(t, uint64(0), evt.BoundarySlot)
}

const (
	survivalSnapshotEpoch = uint64(1)
	survivalNewEpoch      = uint64(4)
	survivalCapturedSlot  = uint64(200)
	survivalBoundarySlot  = uint64(1_200)
)

func rewardOutputIDs(
	t *testing.T,
	db *database.Database,
) (map[string]uint, map[string]uint) {
	t.Helper()
	pools, err := db.Metadata().GetRewardPoolOutputs(survivalSnapshotEpoch, nil)
	require.NoError(t, err)
	accounts, err := db.Metadata().GetRewardAccountOutputs(
		survivalSnapshotEpoch, nil,
	)
	require.NoError(t, err)
	poolIDs := make(map[string]uint, len(pools))
	for _, output := range pools {
		poolIDs[string(output.PoolKeyHash)] = output.ID
	}
	accountIDs := make(map[string]uint, len(accounts))
	for _, output := range accounts {
		accountIDs[string(output.PoolKeyHash)+"/"+string(output.StakingKey)+
			"/"+output.RewardType] = output.ID
	}
	return poolIDs, accountIDs
}

func requirePrecomputeReusable(t *testing.T, ls *LedgerState) {
	t.Helper()
	txn := ls.db.Transaction(false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		_, ok, err := ls.precomputedStakeRewardApplication(
			txn, survivalNewEpoch, survivalBoundarySlot,
		)
		require.NoError(t, err)
		require.True(t, ok, "the boundary must find the precompute")
		return nil
	}))
}

// TestRewardPrecomputeSurvivesRollbackAboveCapturedSlot pins that a finished
// precompute outlives every rollback that stays above its captured slot: the
// rollback's reward-state sweep keeps its rows, and a re-run under the new
// rollback generation accepts them instead of recomputing the round.
func TestRewardPrecomputeSurvivesRollbackAboveCapturedSlot(t *testing.T) {
	t.Parallel()
	ls, db := seedMultiPoolRewardPrecomputeFixture(t, 7, 4, 7)
	ls.rewardPrecomputeChunkPoolsOverride = 2
	require.NoError(t, ls.runChunkedStakeRewardPrecompute(
		survivalNewEpoch, survivalCapturedSlot, survivalBoundarySlot,
	))
	poolIDs, accountIDs := rewardOutputIDs(t, db)
	require.Len(t, poolIDs, 7)

	// A rollback into the epoch the round is applied at, below its
	// application boundary.
	require.NoError(t, db.DeleteRewardStateAfterSlot(
		survivalCapturedSlot+300, nil,
	))
	ls.rewardInputGeneration.Add(2)
	gotPools, gotAccounts := rewardOutputIDs(t, db)
	require.Equal(t, poolIDs, gotPools, "the rollback must keep pool outputs")
	require.Equal(t, accountIDs, gotAccounts)

	require.NoError(t, ls.runChunkedStakeRewardPrecompute(
		survivalNewEpoch, survivalCapturedSlot, survivalBoundarySlot,
	))
	gotPools, gotAccounts = rewardOutputIDs(t, db)
	require.Equal(t, poolIDs, gotPools, "a re-run must not recompute the round")
	require.Equal(t, accountIDs, gotAccounts)
	requirePrecomputeReusable(t, ls)
}

// TestRewardPrecomputeRestartsWhenCommittedOutputsAreGone is the negative
// case of resumption: when the outputs a cursor claims are missing -- as after
// a rollback that reached the round's captured slot -- a re-run recomputes the
// whole round instead of resuming over the gap, and still matches an
// uninterrupted run.
func TestRewardPrecomputeRestartsWhenCommittedOutputsAreGone(t *testing.T) {
	t.Parallel()
	ls, db := seedMultiPoolRewardPrecomputeFixture(t, 7, 4, 7)
	ls.rewardPrecomputeChunkPoolsOverride = 2
	ls.rewardPrecomputeChunkHook = func(processed, total int) {
		if processed >= 4 {
			ls.rewardInputGeneration.Add(2)
		}
	}
	require.NoError(t, ls.runChunkedStakeRewardPrecompute(
		survivalNewEpoch, survivalCapturedSlot, survivalBoundarySlot,
	))
	partial, _ := rewardOutputIDs(t, db)
	require.Len(t, partial, 4)
	require.NoError(t, db.Metadata().DeleteRewardOutputsForEpoch(
		survivalSnapshotEpoch, nil,
	))

	ls.rewardPrecomputeChunkHook = nil
	require.NoError(t, ls.runChunkedStakeRewardPrecompute(
		survivalNewEpoch, survivalCapturedSlot, survivalBoundarySlot,
	))
	fresh, freshDB := seedMultiPoolRewardPrecomputeFixture(t, 7, 4, 7)
	fresh.rewardPrecomputeChunkPoolsOverride = 2
	require.NoError(t, fresh.runChunkedStakeRewardPrecompute(
		survivalNewEpoch, survivalCapturedSlot, survivalBoundarySlot,
	))
	require.Equal(
		t,
		snapshotRewardPrecomputeOutputs(t, freshDB, survivalSnapshotEpoch, 3),
		snapshotRewardPrecomputeOutputs(t, db, survivalSnapshotEpoch, 3),
	)
	requirePrecomputeReusable(t, ls)
}

// TestRewardPrecomputeResumesAcrossRollbackGeneration pins resumption: a run
// interrupted by a rollback that leaves its inputs untouched continues from
// its cursor under the new generation, keeping the committed chunks' rows.
func TestRewardPrecomputeResumesAcrossRollbackGeneration(t *testing.T) {
	t.Parallel()
	ls, db := seedMultiPoolRewardPrecomputeFixture(t, 7, 4, 7)
	ls.rewardPrecomputeChunkPoolsOverride = 2
	bumped := false
	ls.rewardPrecomputeChunkHook = func(processed, total int) {
		if !bumped {
			bumped = true
			ls.rewardInputGeneration.Add(2)
		}
	}
	require.NoError(t, ls.runChunkedStakeRewardPrecompute(
		survivalNewEpoch, survivalCapturedSlot, survivalBoundarySlot,
	))
	firstChunk, firstAccounts := rewardOutputIDs(t, db)
	require.Len(t, firstChunk, 2)

	ls.rewardPrecomputeChunkHook = nil
	require.NoError(t, ls.runChunkedStakeRewardPrecompute(
		survivalNewEpoch, survivalCapturedSlot, survivalBoundarySlot,
	))
	all, allAccounts := rewardOutputIDs(t, db)
	require.Len(t, all, 7)
	for key, id := range firstChunk {
		require.Equal(t, id, all[key], "a resumed run keeps committed chunks")
	}
	for key, id := range firstAccounts {
		require.Equal(t, id, allAccounts[key])
	}
	requirePrecomputeReusable(t, ls)

	// The resumed result equals an uninterrupted run's.
	fresh, freshDB := seedMultiPoolRewardPrecomputeFixture(t, 7, 4, 7)
	fresh.rewardPrecomputeChunkPoolsOverride = 2
	require.NoError(t, fresh.runChunkedStakeRewardPrecompute(
		survivalNewEpoch, survivalCapturedSlot, survivalBoundarySlot,
	))
	require.Equal(
		t,
		snapshotRewardPrecomputeOutputs(t, freshDB, survivalSnapshotEpoch, 3),
		snapshotRewardPrecomputeOutputs(t, db, survivalSnapshotEpoch, 3),
	)
}

// Preview's on-chain ADA pots, as reported by Koios
// (https://preview.koios.rest/api/v1/totals). Preview declares
// TestShelleyHardForkAtEpoch: 0, so epoch 0 is already Alonzo and every epoch
// boundary from 0->1 onward runs cardano-ledger's NEWEPOCH monetary expansion.
// Preview's genesis decentralisationParam is 1, so eta is 1 by definition and
// no stake rewards are distributed in these epochs: the whole reward pot is
// split between the treasury tax and the reserves refund.
const (
	previewGenesisReserves = uint64(15_000_000_000_000_000)
	previewMaxSupply       = uint64(45_000_000_000_000_000)

	// Epoch 0's fee pot is empty: nothing was collected before epoch 0.
	previewEpoch1Treasury = uint64(9_000_000_000_000)
	previewEpoch1Reserves = uint64(14_991_000_000_000_000)

	// Epoch 0 collected 437793 lovelace in fees, which the 1->2 boundary
	// folds into the reward pot.
	previewEpoch1Fees     = uint64(437_793)
	previewEpoch2Treasury = uint64(17_994_600_087_558)
	previewEpoch2Reserves = uint64(14_982_005_400_350_235)

	// Epoch 1 collected 206597 lovelace in fees, which the 2->3 boundary
	// folds into the reward pot. Preview's decentralisation is 1 at epochs 0
	// and 1 and 0 from epoch 2 onward, so the 2->3 round -- whose parameters
	// come from performance epoch 1 -- still takes the d >= 0.8 short circuit
	// and expands by the full rho * reserves.
	previewEpoch2Fees     = uint64(206_597)
	previewEpoch3Treasury = uint64(26_983_803_369_087)
	previewEpoch3Reserves = uint64(14_973_016_197_275_303)

	previewEpochLength = uint64(86_400)
)

// newPreviewRewardPotsTestLedger builds a LedgerState configured with
// Preview's Shelley genesis and seeds the epoch rows, protocol parameters and
// empty mark snapshots that the delayed reward calculation reads for the first
// two boundaries.
func newPreviewRewardPotsTestLedger(
	t *testing.T,
) (*LedgerState, *database.Database) {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.05,
		"epochLength": 86400,
		"maxLovelaceSupply": 45000000000000000,
		"securityParam": 432,
		"slotLength": 1,
		"systemStart": "2022-10-25T00:00:00Z"
	}`)))
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck

	ls := &LedgerState{
		db:         db,
		currentEra: eras.AlonzoEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}

	// Preview's genesis protocol parameters: rho 0.003, tau 0.2, d 1.
	pparams := &alonzo.AlonzoProtocolParameters{
		NOpt:             150,
		A0:               rewardCalcRat(3, 10),
		Rho:              rewardCalcRat(3, 1_000),
		Tau:              rewardCalcRat(1, 5),
		Decentralization: rewardCalcRat(1, 1),
		ProtocolMajor:    6,
		ProtocolMinor:    0,
	}
	pparamsCbor, err := cbor.Encode(pparams)
	require.NoError(t, err)

	// Preview's decentralisation drops from 1 to 0 at epoch 2, so each epoch
	// is seeded with its own protocol parameters.
	decentralizedPParams := *pparams
	decentralizedPParams.Decentralization = rewardCalcRat(0, 1)
	decentralizedCbor, err := cbor.Encode(&decentralizedPParams)
	require.NoError(t, err)

	meta := db.Metadata()
	for _, epoch := range []uint64{0, 1, 2} {
		epochPParamsCbor := pparamsCbor
		if epoch >= 2 {
			epochPParamsCbor = decentralizedCbor
		}
		startSlot := epoch * previewEpochLength
		require.NoError(t, meta.SetEpoch(
			startSlot,
			epoch,
			nil,
			nil,
			nil,
			nil,
			eras.AlonzoEraDesc.Id,
			1,
			uint(previewEpochLength),
			nil,
		))
		require.NoError(t, db.SetPParams(
			epochPParamsCbor,
			startSlot,
			epoch,
			eras.AlonzoEraDesc.Id,
			nil,
		))
		// Preview has no stake delegated to non-overlay pools in these
		// epochs, so the mark snapshot is empty. Epoch 0's is seeded at
		// startup by snapshot.Manager.CaptureGenesisSnapshot.
		require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
			Epoch:           epoch,
			SnapshotType:    "mark",
			CapturedSlot:    startSlot,
			BoundarySlot:    startSlot,
			ProtocolVersion: 6,
		}, nil))
	}
	return ls, db
}

// TestApplyStakeRewardsPreviewEpoch1Pots pins the 0->1 boundary. cardano-ledger
// applies monetary expansion and the treasury tax at the first boundary of a
// network whose epoch 0 is already Shelley-era, with an empty fee pot and no
// distribution. Skipping that round leaves the treasury at 0 and the reserves
// at their genesis value, which is what dingo #3381 observed on Preview.
func TestApplyStakeRewardsPreviewEpoch1Pots(t *testing.T) {
	t.Parallel()

	ls, db := newPreviewRewardPotsTestLedger(t)
	meta := db.Metadata()

	require.NoError(t, meta.SetNetworkState(0, previewGenesisReserves, 0, nil))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        0,
		Treasury:     0,
		Reserves:     types.Uint64(previewGenesisReserves),
		Fees:         0,
		CapturedSlot: 0,
	}, nil))

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, 1, previewEpochLength)
	}))

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, previewEpoch1Treasury, uint64(state.Treasury))
	require.Equal(t, previewEpoch1Reserves, uint64(state.Reserves))
}

// TestApplyStakeRewardsPreviewEpoch2Pots pins the 1->2 boundary against the
// same Koios reference. It is seeded with the epoch-1 pots the previous
// boundary must produce, so it isolates the epoch-2 arithmetic from the
// epoch-1 seeding defect.
func TestApplyStakeRewardsPreviewEpoch2Pots(t *testing.T) {
	t.Parallel()

	ls, db := newPreviewRewardPotsTestLedger(t)
	meta := db.Metadata()

	require.NoError(t, meta.SetNetworkState(
		previewEpoch1Treasury, previewEpoch1Reserves, previewEpochLength, nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        1,
		Treasury:     types.Uint64(previewEpoch1Treasury),
		Reserves:     types.Uint64(previewEpoch1Reserves),
		Fees:         types.Uint64(previewEpoch1Fees),
		CapturedSlot: previewEpochLength,
	}, nil))

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, 2, 2*previewEpochLength)
	}))

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, previewEpoch2Treasury, uint64(state.Treasury))
	require.Equal(t, previewEpoch2Reserves, uint64(state.Reserves))
}

// TestApplyStakeRewardsPreviewGenesisToEpoch2 chains both boundaries the way a
// genesis replay does: the 0->1 round, the epoch-1 ADA pots capture that
// records its result, then the 1->2 round that reads it back. Preview's epoch 0
// carries exactly two transactions, at slots 60 and 320, whose fees (200000 and
// 237793) are the 437793 the 1->2 boundary folds into the reward pot.
//
// This is the unit-level counterpart of dingo #3381's reproduction: the
// epoch-2 treasury and reserves must equal the Koios Preview reference values.
func TestApplyStakeRewardsPreviewGenesisToEpoch2(t *testing.T) {
	t.Parallel()

	ls, db := newPreviewRewardPotsTestLedger(t)
	meta := db.Metadata()

	require.NoError(t, meta.SetNetworkState(0, previewGenesisReserves, 0, nil))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        0,
		Treasury:     0,
		Reserves:     types.Uint64(previewGenesisReserves),
		Fees:         0,
		CapturedSlot: 0,
	}, nil))

	// Preview's two epoch-0 transactions.
	_, err := rewardCalcSQLDB(t, db).Exec(`
INSERT INTO "transaction" (
    id, hash, block_hash, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES
    (1, ?, ?, 60, 5, '200000', '0', '0', 0, TRUE),
    (2, ?, ?, 320, 5, '237793', '0', '0', 0, TRUE)`,
		[]byte("preview-tx-0"), []byte("preview-block-0"),
		[]byte("preview-tx-1"), []byte("preview-block-1"),
	)
	require.NoError(t, err)

	epoch0, err := meta.GetEpoch(0, nil)
	require.NoError(t, err)
	require.NotNil(t, epoch0)

	// Boundary into epoch 1: apply the reward round, then capture the epoch-1
	// ADA pots the way processEpochRollover does.
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		if err := ls.applyStakeRewards(
			txn, 1, previewEpochLength,
		); err != nil {
			return err
		}
		return ls.saveRewardAdaPotsForEpoch(
			txn, 1, *epoch0, previewEpochLength,
		)
	}))

	pots1, err := meta.GetRewardAdaPots(1, nil)
	require.NoError(t, err)
	require.NotNil(t, pots1)
	require.Equal(t, previewEpoch1Treasury, uint64(pots1.Treasury))
	require.Equal(t, previewEpoch1Reserves, uint64(pots1.Reserves))
	require.Equal(t, previewEpoch1Fees, uint64(pots1.Fees))

	// Boundary into epoch 2, reading the row the previous boundary wrote.
	txn = db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, 2, 2*previewEpochLength)
	}))

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, previewEpoch2Treasury, uint64(state.Treasury))
	require.Equal(t, previewEpoch2Reserves, uint64(state.Reserves))
}

// TestApplyStakeRewardsPreviewEpoch3Pots pins the 2->3 boundary, where
// preview's decentralisation differs between the round's performance epoch (1,
// d = 1) and its calculation epoch (2, d = 0).
//
// cardano-ledger's startStep builds the whole reward update from
// prevPParams -- the parameters in force during the epoch whose blocks are
// counted, which is dingo's performance epoch -- so d is 1 here and eta takes
// the d >= 0.8 short circuit. Reading d from the calculation epoch instead
// gives d = 0, no short circuit, and an eta of zero against an empty epoch-0
// mark snapshot, which drops the monetary expansion entirely and moves only
// the fee pot (dingo #3481).
func TestApplyStakeRewardsPreviewEpoch3Pots(t *testing.T) {
	t.Parallel()

	ls, db := newPreviewRewardPotsTestLedger(t)
	meta := db.Metadata()

	require.NoError(t, meta.SetNetworkState(
		previewEpoch2Treasury,
		previewEpoch2Reserves,
		2*previewEpochLength,
		nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        2,
		Treasury:     types.Uint64(previewEpoch2Treasury),
		Reserves:     types.Uint64(previewEpoch2Reserves),
		Fees:         types.Uint64(previewEpoch2Fees),
		CapturedSlot: 2 * previewEpochLength,
	}, nil))

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, 3, 3*previewEpochLength)
	}))

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, previewEpoch3Treasury, uint64(state.Treasury))
	require.Equal(t, previewEpoch3Reserves, uint64(state.Reserves))
}

// TestLedgerViewRewardWithdrawalValidation exercises the protocol rule against
// Dingo's database-backed LedgerView. The upstream rule is responsible for the
// era-specific amount policy; storage remains era-neutral and rejects only
// overdrafts before subtracting the accepted amount.
func TestLedgerViewRewardWithdrawalValidation(t *testing.T) {
	t.Parallel()

	const balance = uint64(100)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)

	key := bytes.Repeat([]byte{0xa1}, lcommon.AddressHashSize)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: key,
		Reward:     types.Uint64(balance),
		Active:     true,
	}))
	lv := &LedgerView{ls: &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}}
	rewardAddr, err := lcommon.NewAddressFromBytes(
		append([]byte{0xe1}, key...),
	)
	require.NoError(t, err)

	validateShelley := func(amount uint64) error {
		tx := mockledger.NewTransactionBuilder().WithWithdrawals(
			map[*lcommon.Address]uint64{&rewardAddr: amount},
		)
		return shelley.UtxoValidateWithdrawals(tx, 0, lv, nil)
	}
	require.NoError(t, validateShelley(balance))
	var incorrectAmount shelley.IncorrectWithdrawalAmountError
	require.ErrorAs(t, validateShelley(balance/2), &incorrectAmount)
	incorrectAmount = shelley.IncorrectWithdrawalAmountError{}
	require.ErrorAs(t, validateShelley(balance+1), &incorrectAmount)

	validateDijkstra := func(amount uint64) error {
		tx := mockledger.NewTransactionBuilder().WithWithdrawals(
			map[*lcommon.Address]uint64{&rewardAddr: amount},
		)
		pp := &conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: lcommon.ProtocolVersionDijkstra,
			},
		}
		return conway.UtxoValidateWithdrawals(tx, 0, lv, pp)
	}
	require.NoError(t, validateDijkstra(balance/2))
	incorrectAmount = shelley.IncorrectWithdrawalAmountError{}
	require.ErrorAs(t, validateDijkstra(balance+1), &incorrectAmount)
}

const (
	// Deliberately contestedSlot-1: the ancestor search range is half-open, so
	// an ancestor immediately below the contested slot is the boundary case.
	sameSlotAncestorSlot  = 19
	sameSlotContestedSlot = 20
)

// sameSlotCompetitorFixture holds a ledger whose applied tip is a block at
// sameSlotContestedSlot, with one UTxO produced at sameSlotAncestorSlot and
// consumed by that applied block.
type sameSlotCompetitorFixture struct {
	ls            *LedgerState
	db            *database.Database
	appliedTip    ochainsync.Tip
	ancestorPoint ocommon.Point
	survivingHash []byte
	spentTxId     []byte
}

func newSameSlotCompetitorFixture(
	t *testing.T,
) *sameSlotCompetitorFixture {
	t.Helper()
	return newSameSlotCompetitorFixtureOpts(t, true)
}

// newSameSlotCompetitorFixtureOpts builds the fixture, optionally omitting the
// ancestor's recorded nonce so that no applied ancestor exists below the
// contested slot.
func newSameSlotCompetitorFixtureOpts(
	t *testing.T,
	seedAncestorNonce bool,
) *sameSlotCompetitorFixture {
	t.Helper()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 2}),
	)

	ancestorHash := testHashBytes("3678-ancestor")
	survivingHash := testHashBytes("3678-surviving")
	competitorHash := testHashBytes("3678-competitor")

	// The primary chain holds the ancestor and the block at the contested slot
	// that survives chain selection. The ledger's applied tip is a *different*
	// block at that same slot -- an abandoned same-slot competitor whose effects
	// were applied to the UTxO set before chain selection moved off it. This is
	// the shape enforceDurableTipFloor repairs: it hands rollback the durable
	// applied floor while currentTip names the same-slot competitor.
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks([]chain.RawBlock{
			{
				Slot:        sameSlotAncestorSlot,
				Hash:        ancestorHash,
				BlockNumber: 1,
				Type:        1,
				Cbor:        []byte{0x80},
			},
			{
				Slot:        sameSlotContestedSlot,
				Hash:        survivingHash,
				BlockNumber: 2,
				Type:        1,
				PrevHash:    ancestorHash,
				Cbor:        []byte{0x80},
			},
		}),
	)

	ls, err := NewLedgerState(
		LedgerStateConfig{
			Database:          db,
			ChainManager:      cm,
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	)
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())

	// Block nonces record which blocks were applied.
	// latestLedgerPrimaryChainAncestor reads them to find the newest applied
	// ancestor below the contested slot.
	if seedAncestorNonce {
		require.NoError(
			t,
			db.SetBlockNonce(
				ancestorHash,
				sameSlotAncestorSlot,
				[]byte("nonce-3678-ancestor"),
				true,
				nil,
			),
		)
	}
	require.NoError(
		t,
		db.SetBlockNonce(
			survivingHash,
			sameSlotContestedSlot,
			[]byte("nonce-3678-surviving"),
			false,
			nil,
		),
	)

	// The competitor was applied before chain selection moved off it, so its
	// nonce is recorded. That recorded nonce is what distinguishes an applied
	// same-slot competitor, whose effects are in the UTxO set, from a merely
	// in-memory tip that was never applied.
	require.NoError(
		t,
		db.SetBlockNonce(
			competitorHash,
			sameSlotContestedSlot,
			[]byte("nonce-3678-competitor"),
			false,
			nil,
		),
	)

	appliedTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(sameSlotContestedSlot, competitorHash),
		BlockNumber: 2,
	}
	require.NoError(t, db.SetTip(appliedTip, nil))
	ls.currentTip = appliedTip
	ls.chainsyncState = SyncingChainsyncState
	ls.publishSnapshotsLocked()

	// One UTxO produced at the ancestor slot and consumed by the applied
	// block at the contested slot, exactly as a normal block application
	// leaves it: the row survives, soft-deleted with deleted_slot set to the
	// consuming block's slot.
	spentTxId := testHashBytes("3678-utxo-producer")
	mdTxn := db.MetadataTxn(true)
	require.NoError(t, mdTxn.Do(func(txn *database.Txn) error {
		return db.CreateUtxo(txn, &models.Utxo{
			TxId:        spentTxId,
			OutputIdx:   0,
			AddedSlot:   sameSlotAncestorSlot,
			DeletedSlot: sameSlotContestedSlot,
			Amount:      types.Uint64(1_000_000),
		})
	}))

	return &sameSlotCompetitorFixture{
		ls:            ls,
		db:            db,
		appliedTip:    appliedTip,
		ancestorPoint: ocommon.NewPoint(sameSlotAncestorSlot, ancestorHash),
		survivingHash: survivingHash,
		spentTxId:     spentTxId,
	}
}

// inputInLiveSet reports whether the consumed UTxO is present in the live UTxO
// set, using the same database.UtxoByRef lookup that LedgerView.UtxoById
// delegates to. That lookup applies the deleted_slot filter, so it is the
// predicate that decides Conway bad-inputs and, through it, the consumed term
// of value conservation.
//
// Presence is judged on ErrUtxoNotFound rather than on a nil error: a row
// seeded directly into metadata carries no blob CBOR (models.Utxo.Cbor is not
// persisted by CreateUtxo), so the decode step of UtxoById cannot succeed for a
// synthetic UTxO. Only ErrUtxoCborUnavailable, alongside a nil error, is read
// as the row being in the live set. Any other error -- including a genuine
// decode failure -- is a lookup failure rather than an answer about live-set
// membership, and is returned so the test fails loudly instead of counting as
// present.
func (f *sameSlotCompetitorFixture) inputInLiveSet(t *testing.T) bool {
	t.Helper()

	var live bool
	txn := f.db.Transaction(false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		_, err := f.db.UtxoByRef(f.spentTxId, 0, txn)
		switch {
		case err == nil,
			errors.Is(err, database.ErrUtxoCborUnavailable):
			live = true
			return nil
		case errors.Is(err, database.ErrUtxoNotFound):
			live = false
			return nil
		default:
			// Any other error is a real lookup failure, not an answer about
			// live-set membership. Return it so the test fails instead of
			// reading it as "present".
			return err
		}
	}))
	return live
}

// TestRollbackSameSlotCompetitorRestoresConsumedUtxo covers issue #3678.
//
// A rollback target that shares the applied tip's slot but carries a different
// hash used to fall through to database.TruncateAfterSlot's slot-only UTxO
// predicates (added_slot > slot, deleted_slot > slot). Nothing at the contested
// slot matched, so the UTxOs the abandoned block consumed stayed soft-deleted
// with no row left to restore them, while the tip was reported as repaired.
//
// The next block that legitimately spends such an input cannot resolve it,
// which Conway reports under the bad-inputs rule and, because value
// conservation sums consumed over only the inputs that did resolve, under the
// value-not-conserved rule in the same pass -- both from the one divergence.
// The numbers those rules carry in the diagnostic are upstream positions and
// move on a gouroboros bump, so they are named here rather than pinned.
//
// This drives LedgerState.rollback, the entry point every recovery path uses,
// and asserts live-set membership through the database.UtxoByRef lookup that
// LedgerView.UtxoById delegates to, rather than querying deleted_slot directly.
func TestRollbackSameSlotCompetitorRestoresConsumedUtxo(t *testing.T) {
	t.Parallel()

	fixture := newSameSlotCompetitorFixture(t)

	// While the applied block at the contested slot stands, its consumed
	// input is correctly unresolvable.
	require.False(
		t,
		fixture.inputInLiveSet(t),
		"consumed input should not be in the live set before the rollback",
	)

	require.NoError(
		t,
		fixture.ls.rollback(
			ocommon.NewPoint(
				sameSlotContestedSlot,
				fixture.survivingHash,
			),
		),
	)

	// The contested slot must be truncated whole, so the input the abandoned
	// block consumed is live again and resolvable at the validated point.
	require.True(
		t,
		fixture.inputInLiveSet(t),
		"consumed input must be restored to the live set after rolling back past the contested slot",
	)

	// The ledger must sit at an applied point, not at the competitor it was
	// handed, so the block at the contested slot can be re-applied.
	require.Equal(
		t,
		fixture.ancestorPoint,
		fixture.ls.currentTip.Point,
		"tip should be redirected to the applied ancestor below the contested slot",
	)
}

// TestRollbackSameSlotCompetitorWithoutAncestorFailsLoudly covers the other
// half of issue #3678's acceptance criteria: when the contested slot cannot be
// truncated because no applied ancestor below it can be found, the rollback
// must fail with a persistent diagnostic instead of reporting a repair that
// left the UTxO set diverged.
func TestRollbackSameSlotCompetitorWithoutAncestorFailsLoudly(t *testing.T) {
	t.Parallel()

	// No ancestor nonce, so no applied block exists below the contested slot.
	fixture := newSameSlotCompetitorFixtureOpts(t, false)

	err := fixture.ls.rollback(
		ocommon.NewPoint(sameSlotContestedSlot, fixture.survivingHash),
	)
	require.ErrorIs(t, err, ErrNoAppliedAncestorBelowContestedSlot)

	// The tip must not move, so the failure stays visible to the recovery
	// caller instead of being reported as a completed repair.
	require.Equal(
		t,
		fixture.appliedTip.Point,
		fixture.ls.currentTip.Point,
		"tip must not move when the contested slot cannot be truncated",
	)
}

// A skipped reward round is not a benign no-op. The reference node credits
// the round regardless, so every skip leaves this node's reward balances --
// and the leadership stake distribution derived from them -- permanently
// short by that epoch's rewards, with nothing to backfill it later.
//
// That shortfall is what rejects canonical blocks: leader eligibility
// compares a VRF value against a stake-derived threshold, so a sigma
// shortfall of eps flips a decision with probability about eps per block.
// Measured on preview for issue #3165, the shortfall was ~3 epochs of reward
// accrual, sigma was 0.042% short, and the rejected block's leader value sat
// between this node's threshold and the reference's.
//
// Both skip paths logged at Debug before this, invisible at the default
// level, which is why three separate field reports were investigated without
// anyone seeing the cause. The level is the fix: a node quietly diverging
// from the network has to say so before it wedges, not after.
func TestSkippedStakeRewardsIsReportedLoudly(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
				// Deliberately Warn: the point of the change is that this
				// survives the default level. A Debug-level report would
				// produce no output here.
				Level: slog.LevelWarn,
			})),
		},
	}

	ls.reportSkippedStakeRewards(1386, "missing ADA pots", "pots_epoch", 1385)

	logs := buf.String()
	require.NotEmpty(t, logs,
		"a skipped reward round must be visible at the default log level; "+
			"at Debug it stays hidden until the node rejects a block")
	assert.Contains(t, logs, "level=WARN")
	assert.Contains(t, logs, "missing ADA pots")
	assert.Contains(t, logs, "new_epoch=1386")
	assert.Contains(t, logs, "pots_epoch=1385")
	// The consequence, not just the event: whoever reads this needs to know
	// the balances stay short rather than catching up on their own.
	assert.Contains(t, logs, "permanently")
	assert.Contains(t, logs, "basis was never persisted")
	assert.Contains(t, logs, "ledgerstate import warnings")
	assert.NotContains(t, logs, "expected after a Mithril bootstrap",
		"a failed imported-basis seed must not be misreported as an "+
			"inherent bootstrap limitation")
}

// The reporting path must tolerate a LedgerState with no logger and no
// metrics, since it runs on the epoch-boundary hot path where a nil
// dereference would take down block application.
func TestSkippedStakeRewardsSurvivesNilDependencies(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	require.NotPanics(t, func() {
		ls.reportSkippedStakeRewards(
			1386,
			"missing reward snapshot",
			"reward_snapshot_epoch",
			1383,
		)
	})
}

func TestMissingRewardSnapshotReportsImportedSeedFailure(t *testing.T) {
	t.Parallel()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		failureReason       = "historical protocol parameters are unavailable"
	)

	for _, tc := range []struct {
		name        string
		seedFailure bool
		wantReason  string
		notReason   string
	}{
		{
			name:        "durable import failure",
			seedFailure: true,
			wantReason: "imported reward basis seeding failed: " +
				failureReason,
			notReason: "skipping stake rewards: missing reward snapshot;",
		},
		{
			name:       "genuinely missing import",
			wantReason: "skipping stake rewards: missing reward snapshot;",
			notReason:  "imported reward basis seeding failed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ls, db := newRewardCalculationTestLedger(t)
			var logs bytes.Buffer
			ls.config.Logger = slog.New(slog.NewTextHandler(&logs, nil))

			meta := db.Metadata()
			require.NoError(t, meta.SaveRewardAdaPots(
				&models.RewardAdaPots{
					Epoch:        potsEpoch,
					CapturedSlot: 300,
				},
				nil,
			))
			if tc.seedFailure {
				require.NoError(t, meta.SaveRewardSeedFailure(
					rewardSnapshotEpoch,
					"mark",
					failureReason,
					100,
					nil,
				))
			}

			txn := db.Transaction(false)
			defer func() { _ = txn.Rollback() }()
			app, ok, err := ls.calculateStakeRewardApplication(
				txn,
				newEpoch,
				400,
				400,
				true,
			)
			require.NoError(t, err)
			require.False(t, ok)
			require.Nil(t, app)
			assert.Contains(t, logs.String(), tc.wantReason)
			assert.NotContains(t, logs.String(), tc.notReason)
		})
	}
}

// BenchmarkTipSnapshotReadOnly and BenchmarkTipSnapshotReadUnderWriter are the
// regression sentinel for issue #2601: LedgerState.Tip, GetCurrentPParams,
// CurrentEpoch, and IsAtTip are read constantly from API handlers, chainsync,
// forging, and block validation, and used to take the embedded RWMutex's read
// lock. A concurrent writer stalled every reader (RWMutex with 1% concurrent
// writer measured ~591ns at 16 cores in #2601's prototype, versus ~133ns
// read-only), because each writer Lock blocks the shared reader counter. The
// fix (already landed, see LedgerState.consensus/tip atomic.Pointer fields
// and publishSnapshotsLocked) moved these fields behind immutable
// copy-on-write snapshots so reads never block on a concurrent writer.
//
// Comparing these two benchmarks' ns/op across -cpu=1,4,8,16 is the
// regression signal: on the current atomic.Pointer implementation both should
// stay flat and close to each other as core count rises. A reintroduced
// RWMutex (or any lock) on this read path would show
// BenchmarkTipSnapshotReadUnderWriter degrading sharply relative to
// BenchmarkTipSnapshotReadOnly as cores increase, exactly the negative
// scaling issue #1895 asks this framework to catch.
//
// BenchmarkConcurrentQueries (see tests_61443820_test.go) exercises database query
// load under concurrency; it is not a substitute for this benchmark, which
// targets the specific in-memory snapshot read/publish path #2601 describes.

func benchmarkTipSnapshotReaders(b *testing.B, ledgerState *LedgerState) {
	b.Helper()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = ledgerState.Tip()
			_ = ledgerState.GetCurrentPParams()
			_ = ledgerState.CurrentEpoch()
			_ = ledgerState.IsAtTip()
		}
	})
}

// BenchmarkTipSnapshotReadOnly is the baseline: readers only, no concurrent
// writer. Run with -cpu=1,4,8,16 to see the scaling curve.
func BenchmarkTipSnapshotReadOnly(b *testing.B) {
	db, ledgerState := newBatchBenchmarkLedgerState(b, nil)
	defer dbtest.CloseDatabase(db)

	benchmarkTipSnapshotReaders(b, ledgerState)
}

// BenchmarkTipSnapshotReadUnderWriter adds a background writer that
// continuously republishes the consensus/tip snapshots (the same
// publishSnapshotsLocked call a real per-block writer makes), while readers
// run concurrently. Run with -cpu=1,4,8,16 to see the scaling curve; per
// #2601's regression, an implementation using a plain RWMutex here would
// degrade sharply at higher core counts, while the atomic.Pointer
// implementation should stay close to BenchmarkTipSnapshotReadOnly.
//
// The writer runs as fast as possible (deliberately more aggressive than a
// real per-block cadence) so a reintroduced lock's contention shows up
// clearly rather than being diluted by a realistic, much lower write rate.
func BenchmarkTipSnapshotReadUnderWriter(b *testing.B) {
	db, ledgerState := newBatchBenchmarkLedgerState(b, nil)
	defer dbtest.CloseDatabase(db)

	done := make(chan struct{})
	writerStopped := make(chan struct{})
	go func() {
		defer close(writerStopped)
		for {
			select {
			case <-done:
				return
			default:
				ledgerState.Lock()
				ledgerState.publishSnapshotsLocked()
				ledgerState.Unlock()
			}
		}
	}()
	defer func() {
		close(done)
		<-writerStopped
	}()

	benchmarkTipSnapshotReaders(b, ledgerState)
}

// valueNotConservedSubstring is the message
// shelley.ValueNotConservedUtxoError renders. These tests match on that
// message and never on the rule index: the index is an offset into the
// upstream gouroboros slice and moves whenever upstream inserts or reorders a
// rule. It printed as 32 on v0.202.5 and prints as 33 on the currently pinned
// v0.202.6, which inserted UtxoValidateCurrentTreasuryValue at index 0.
const valueNotConservedSubstring = "value not conserved"

const stakeRefundTestKeyDeposit = 2_000_000

// stakeRefundTestPparams returns Conway protocol parameters whose KeyDeposit
// is the value a legacy stake deregistration falls back to when the ledger
// state cannot report the deposit recorded at registration.
func stakeRefundTestPparams() *conway.ConwayProtocolParameters {
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: 9,
		},
		KeyDeposit:           stakeRefundTestKeyDeposit,
		MaxTxSize:            16_384,
		MaxValueSize:         5_000,
		CollateralPercentage: 150,
		MaxCollateralInputs:  3,
	}
}

// stakeDeregistrationTx builds a real *conway.ConwayTransaction carrying a
// single legacy stake deregistration and no inputs or outputs, so value
// conservation reduces to "refund must equal fee". The refund is the only
// consumed value and the fee is the only produced value, which isolates the
// recorded-deposit lookup from every other term in the equation.
func stakeDeregistrationTx(
	cred lcommon.Credential,
	fee uint64,
) *conway.ConwayTransaction {
	cert := &lcommon.StakeDeregistrationCertificate{
		CertType:        uint(lcommon.CertificateTypeStakeDeregistration),
		StakeCredential: cred,
	}
	return &conway.ConwayTransaction{
		TxIsValid: true,
		Body: conway.ConwayTransactionBody{
			TxFee: fee,
			TxCertificates: []lcommon.CertificateWrapper{
				{
					Type: uint(
						lcommon.CertificateTypeStakeDeregistration,
					),
					Certificate: cert,
				},
			},
		},
	}
}

// seedStakeRegistration drives a stake registration through the production
// certificate write path, which is what decides whether the recorded deposit
// lands in the database as a value or as NULL. Passing a nil deposit omits the
// certificate index from the certDeposits map exactly as
// ledger.calculateCertificateDeposit and backfill.calculateCertDeposits do
// when the deposit cannot be computed.
func seedStakeRegistration(
	t *testing.T,
	db *database.Database,
	cred lcommon.Credential,
	deposit *uint64,
	slot uint64,
	seed byte,
) {
	t.Helper()
	builder := mockledger.NewTransactionBuilder()
	builder.WithId(bytes.Repeat([]byte{seed}, 32))
	builder.WithValid(true)
	input, err := mockledger.NewSimpleTransactionInput(
		bytes.Repeat([]byte{seed + 1}, 32),
		0,
	)
	require.NoError(t, err)
	builder.WithInputs(input)
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd").
		WithLovelace(1_000_000).
		Build()
	require.NoError(t, err)
	builder.WithOutputs(output)
	builder.WithCertificates(&lcommon.StakeRegistrationCertificate{
		StakeCredential: cred,
	})
	tx, err := builder.Build()
	require.NoError(t, err)
	certDeposits := map[int]uint64{}
	if deposit != nil {
		certDeposits[0] = *deposit
	}
	require.NoError(t, db.SetTransactionMetadataOnly(
		tx,
		ocommon.NewPoint(slot, bytes.Repeat([]byte{seed + 2}, 32)),
		0,
		certDeposits,
		nil,
	))
}

// newStakeRefundTestView returns a *LedgerView over a real database, built
// from the same *LedgerState the other end-to-end validation tests use so the
// Conway rules that read genesis configuration (network ids, slot
// conversion) run rather than panic.
func newStakeRefundTestView(
	t *testing.T,
) (*LedgerView, *database.Database) {
	t.Helper()
	ls, db := newRewardCalculationTestLedger(t)
	return &LedgerView{ls: ls}, db
}

func stakeRefundTestCredential(seed byte) lcommon.Credential {
	return lcommon.Credential{
		CredType: lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(
			bytes.Repeat([]byte{seed}, lcommon.AddressHashSize),
		),
	}
}

// requireValueConserved asserts the transaction clears value conservation
// through the production Conway validation path. Other rules error on these
// deliberately minimal transactions (the input set is empty by design), so the
// assertion is on the absence of the value-conservation failure specifically,
// which is what the recorded-deposit refund decides.
func requireValueConserved(
	t *testing.T,
	lv *LedgerView,
	tx *conway.ConwayTransaction,
) {
	t.Helper()
	pp := stakeRefundTestPparams()
	// Invoke the rule directly first. This assertion cannot pass vacuously:
	// a nil error means value conservation actually ran and balanced, rather
	// than merely that the substring was absent because some unrelated rule
	// failed first and short-circuited the message.
	require.NoError(
		t,
		conway.UtxoValidateValueNotConservedUtxo(tx, 200, lv, pp),
	)
	// Then assert the same outcome through the production path.
	if err := eras.ValidateTxConway(tx, 200, lv, pp); err != nil {
		require.NotContains(t, err.Error(), valueNotConservedSubstring)
	}
}

func requireValueNotConserved(
	t *testing.T,
	lv *LedgerView,
	tx *conway.ConwayTransaction,
) {
	t.Helper()
	pp := stakeRefundTestPparams()
	// The rule itself must reject, so the rejection is attributable to value
	// conservation rather than to any other rule the production path joins.
	require.ErrorContains(
		t,
		conway.UtxoValidateValueNotConservedUtxo(tx, 200, lv, pp),
		valueNotConservedSubstring,
	)
	err := eras.ValidateTxConway(tx, 200, lv, pp)
	require.Error(t, err)
	require.Contains(t, err.Error(), valueNotConservedSubstring)
}

// TestValueConservationRefundsUnknownStakeDepositAtKeyDeposit is the
// regression test for #3829. A registration ingested without a computable
// deposit records NULL, LedgerView.StakeCredentialDeposit reports absence, and
// gouroboros' UtxoValidateValueNotConservedUtxo falls back to the current
// KeyDeposit. Before the fix the three zero-reporting sites stored an
// authoritative 0, the rule refunded 0, and this otherwise valid transaction
// failed value conservation.
//
// The assertion is on acceptance through eras.ValidateTxConway with a real
// *LedgerView, not on the helper's return value, because the defect was that
// a plausible internal value became the wrong validation outcome.
func TestValueConservationRefundsUnknownStakeDepositAtKeyDeposit(
	t *testing.T,
) {
	t.Parallel()

	lv, db := newStakeRefundTestView(t)
	cred := stakeRefundTestCredential(0xc1)
	seedStakeRegistration(t, db, cred, nil, 100, 0xc1)

	// The refund falls back to KeyDeposit, so a fee of exactly KeyDeposit
	// conserves value. This acceptance is the assertion that carries the
	// regression: it is the validation outcome, one layer above the recorded
	// value that produces it.
	requireValueConserved(
		t,
		lv,
		stakeDeregistrationTx(cred, stakeRefundTestKeyDeposit),
	)

	// Supporting evidence for why the acceptance holds: the recorded deposit
	// is genuinely absent rather than a zero that happened to balance.
	recorded, err := lv.StakeCredentialDeposit(cred)
	require.NoError(t, err)
	assert.Nil(
		t,
		recorded,
		"an uncomputable registration deposit must be recorded as absent, not zero",
	)
}

// TestValueConservationRejectsUnbalancedUnknownStakeDeposit is the mandatory
// negative case: the KeyDeposit fallback must not become a licence to pass
// value conservation for any fee. A genuinely unbalanced transaction over the
// same absent-deposit registration is still rejected.
func TestValueConservationRejectsUnbalancedUnknownStakeDeposit(t *testing.T) {
	t.Parallel()

	lv, db := newStakeRefundTestView(t)
	cred := stakeRefundTestCredential(0xc2)
	seedStakeRegistration(t, db, cred, nil, 100, 0xc2)

	requireValueNotConserved(
		t,
		lv,
		stakeDeregistrationTx(cred, stakeRefundTestKeyDeposit+1_000_000),
	)
}

// TestValueConservationRefundsRecordedStakeDepositNotKeyDeposit is the second
// mandatory negative case: a correctly recorded non-zero deposit must be
// refunded at its recorded value, never at the current KeyDeposit. The
// recorded 5 ADA deliberately differs from the 2 ADA KeyDeposit, so the two
// possible refunds give opposite outcomes and the test cannot pass by
// accident.
func TestValueConservationRefundsRecordedStakeDepositNotKeyDeposit(
	t *testing.T,
) {
	t.Parallel()

	lv, db := newStakeRefundTestView(t)
	cred := stakeRefundTestCredential(0xc3)
	recordedDeposit := uint64(5_000_000)
	require.NotEqual(
		t,
		uint64(stakeRefundTestKeyDeposit),
		recordedDeposit,
		"the recorded deposit must differ from KeyDeposit for this test to discriminate",
	)
	seedStakeRegistration(t, db, cred, &recordedDeposit, 100, 0xc3)

	got, err := lv.StakeCredentialDeposit(cred)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, recordedDeposit, *got)

	// Balanced at the recorded deposit: accepted.
	requireValueConserved(t, lv, stakeDeregistrationTx(cred, recordedDeposit))
	// Balanced at the current KeyDeposit instead: rejected, which is what
	// proves the recorded value won.
	requireValueNotConserved(
		t,
		lv,
		stakeDeregistrationTx(cred, stakeRefundTestKeyDeposit),
	)
}

// TestValueConservationRefundsRecordedZeroStakeDepositAsZero pins the
// distinction the fix must preserve. A recorded zero is reachable and
// authoritative: config/cardano/devnet/shelley-genesis.json sets
// "keyDeposit": 0, so every stake registration on dingo's own devnet records
// a real zero deposit. Folding zero into the unknown case would refund
// KeyDeposit there and break value conservation on the devnet, which is why
// only the uncomputable case reports absence.
func TestValueConservationRefundsRecordedZeroStakeDepositAsZero(t *testing.T) {
	t.Parallel()

	lv, db := newStakeRefundTestView(t)
	cred := stakeRefundTestCredential(0xc4)
	recordedZero := uint64(0)
	seedStakeRegistration(t, db, cred, &recordedZero, 100, 0xc4)

	got, err := lv.StakeCredentialDeposit(cred)
	require.NoError(t, err)
	require.NotNil(
		t,
		got,
		"a recorded zero deposit must stay a value, not become absence",
	)
	require.Equal(t, uint64(0), *got)

	// Refunded as zero, so a zero fee conserves value.
	requireValueConserved(t, lv, stakeDeregistrationTx(cred, 0))
	// And the KeyDeposit fallback must not be taken.
	requireValueNotConserved(
		t,
		lv,
		stakeDeregistrationTx(cred, stakeRefundTestKeyDeposit),
	)
}

// Fixtures captured off the wire from cardano-cli 11.0.0.0
// `query stake-snapshot` against cardano-node 11.0.1 (devnet). See dingo
// issue #2917. The query payloads are LSQ MsgQuery (mini-protocol 7); the
// result fixtures are the CBOR carried inside the tag-24 Serialised wrapper
// of the MsgResult that cardano-node returns.
const (
	ssQueryOnePoolHex = "82038200820082068209821481" +
		"d9010281581c728ea45cc4888f97d1c3233956fd6abf9362854d880efd6e577807f2"
	ssQueryAllPoolsHex = "82038200820082068209821480"

	// [ {728e…: [mark 1e12, set 0, go 0]}, markTotal 2e12, setTotal 1, goTotal 1 ]
	ssResultOnePoolInnerHex = "84a1581c728ea45cc4888f97d1c3233956fd6abf9362854d880efd6e577807f2" +
		"831b000000e8d4a5100000001b000001d1a94a20000101"

	// [ {728e…: [1e12, 1e12, 0], ccfa…: [1e12, 1e12, 0]}, 2e12, 2e12, 1 ]
	ssResultAllPoolsInnerHex = "84a2" +
		"581c728ea45cc4888f97d1c3233956fd6abf9362854d880efd6e577807f2" +
		"831b000000e8d4a510001b000000e8d4a5100000" +
		"581cccfa09b0c1f3fe9650a11b4d23d5c461df76f6ff10eb95018940984f" +
		"831b000000e8d4a510001b000000e8d4a5100000" +
		"1b000001d1a94a20001b000001d1a94a200001"

	ssPool1Hex = "728ea45cc4888f97d1c3233956fd6abf9362854d880efd6e577807f2"
	ssPool2Hex = "ccfa09b0c1f3fe9650a11b4d23d5c461df76f6ff10eb95018940984f"

	oneTrillion = uint64(1_000_000_000_000)
	twoTrillion = uint64(2_000_000_000_000)
)

// blockQueryFromHex decodes a captured LSQ MsgQuery payload and returns the
// inner *BlockQuery, exactly the value dingo's LSQ server hands to
// LedgerState.Query.
func blockQueryFromHex(
	t *testing.T,
	payloadHex string,
) *olocalstatequery.BlockQuery {
	t.Helper()
	msg, err := olocalstatequery.NewMsgFromCbor(
		olocalstatequery.MessageTypeQuery,
		mustHex(t, payloadHex),
	)
	require.NoError(
		t,
		err,
		"captured stake-snapshot query must decode (issue #2917)",
	)
	msgQuery, ok := msg.(*olocalstatequery.MsgQuery)
	require.True(t, ok)
	blockQuery, ok := msgQuery.Query.Query.(*olocalstatequery.BlockQuery)
	require.True(t, ok)
	return blockQuery
}

func markSnapshot(
	t *testing.T,
	epoch uint64,
	poolHex string,
	stake uint64,
) *models.PoolStakeSnapshot {
	t.Helper()
	return &models.PoolStakeSnapshot{
		Epoch:          epoch,
		SnapshotType:   "mark",
		PoolKeyHash:    mustHex(t, poolHex),
		TotalStake:     types.Uint64(stake),
		DelegatorCount: 1,
		CapturedSlot:   epoch * 100,
	}
}

func epochSummary(epoch, total uint64) *models.EpochSummary {
	return &models.EpochSummary{
		Epoch:            epoch,
		TotalActiveStake: types.Uint64(total),
		SnapshotReady:    true,
	}
}

// serialisedInner runs the query through LedgerState.Query and returns the
// bytes inside the tag-24 (CBOR-in-CBOR) Serialised wrapper, asserting the
// GetCBOR result shape along the way.
func serialisedInner(t *testing.T, ls *LedgerState, payloadHex string) []byte {
	t.Helper()
	result, err := ls.Query(blockQueryFromHex(t, payloadHex), QueryPoint{})
	require.NoError(t, err)
	outer, ok := result.([]any)
	require.True(t, ok, "expected []any MsgResult wire form")
	require.Len(t, outer, 1)
	tag, ok := outer[0].(cbor.Tag)
	require.True(t, ok, "GetCBOR result must be a CBOR tag")
	require.Equal(
		t,
		uint64(cbor.CborTagCbor),
		tag.Number,
		"expected tag 24 (CBOR-in-CBOR)",
	)
	inner, ok := tag.Content.([]byte)
	require.True(t, ok, "tag-24 content must be a byte string")
	return inner
}

// TestQueryStakeSnapshotSpecificPool reproduces the exact single-pool
// scenario captured from cardano-node and asserts dingo emits byte-identical
// serialised CBOR. Reproduces and guards the fix for issue #2917.
func TestQueryStakeSnapshotSpecificPool(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	meta := db.Metadata()
	// Only pool1 has a current-epoch (mark) snapshot; set/go are empty.
	require.NoError(t, meta.SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{
			markSnapshot(t, 2, ssPool1Hex, oneTrillion),
		},
		nil,
	))
	// Only the mark total exists (2e12, both pools). The set/go epochs have
	// no data, so their totals are 0 and must be reported as the NonZero
	// minimum 1 -- matching cardano-node -- giving totals [2e12, 1, 1].
	require.NoError(t, meta.SaveEpochSummary(epochSummary(2, twoTrillion), nil))
	ls := &LedgerState{db: db}
	ls.consensus.Store(
		&consensusSnapshot{currentEpoch: models.Epoch{EpochId: 2}},
	)

	inner := serialisedInner(t, ls, ssQueryOnePoolHex)
	require.Equal(t,
		ssResultOnePoolInnerHex,
		hex.EncodeToString(inner),
		"serialised stake-snapshot must match cardano-node bytes",
	)
}

// TestQueryStakeSnapshotAllPools reproduces the all-pools scenario captured
// from cardano-node and asserts byte-identical serialised CBOR, including the
// canonical (sorted) pool-map key order.
func TestQueryStakeSnapshotAllPools(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	meta := db.Metadata()
	require.NoError(t, meta.SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{
			// mark (current epoch 2)
			markSnapshot(t, 2, ssPool1Hex, oneTrillion),
			markSnapshot(t, 2, ssPool2Hex, oneTrillion),
			// set (epoch 1)
			markSnapshot(t, 1, ssPool1Hex, oneTrillion),
			markSnapshot(t, 1, ssPool2Hex, oneTrillion),
			// go (epoch 0) intentionally absent -> per-pool go = 0
		},
		nil,
	))
	// mark and set totals are 2e12; the go epoch has no data, so goTotal is
	// clamped to the NonZero minimum 1, giving totals [2e12, 2e12, 1].
	require.NoError(t, meta.SaveEpochSummary(epochSummary(2, twoTrillion), nil))
	require.NoError(t, meta.SaveEpochSummary(epochSummary(1, twoTrillion), nil))
	ls := &LedgerState{db: db}
	ls.consensus.Store(
		&consensusSnapshot{currentEpoch: models.Epoch{EpochId: 2}},
	)

	inner := serialisedInner(t, ls, ssQueryAllPoolsHex)
	require.Equal(t,
		ssResultAllPoolsInnerHex,
		hex.EncodeToString(inner),
		"serialised all-pools stake-snapshot must match cardano-node bytes",
	)
}

// TestQueryStakeSnapshotEarlyEpochsNonZeroTotals covers the genesis case:
// at epoch 0 the set/go epochs would underflow, so they must resolve to zero
// stake without querying a bogus wrapped-around epoch, and the set/go totals
// must be reported as the NonZero minimum 1 rather than 0 (cardano clients
// decode the totals as NonZero). This mirrors cardano-node 11.0.1 on a fresh
// devnet, which reports total set/go = 1 while every pool's set/go stake is 0.
func TestQueryStakeSnapshotEarlyEpochsNonZeroTotals(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	meta := db.Metadata()
	require.NoError(t, meta.SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{
			markSnapshot(t, 0, ssPool1Hex, oneTrillion),
		},
		nil,
	))
	require.NoError(t, meta.SaveEpochSummary(epochSummary(0, oneTrillion), nil))
	ls := &LedgerState{db: db}
	ls.consensus.Store(
		&consensusSnapshot{currentEpoch: models.Epoch{EpochId: 0}},
	)

	inner := serialisedInner(t, ls, ssQueryOnePoolHex)
	// [ {728e…: [mark 1e12, set 0, go 0]}, markTotal 1e12, setTotal 1, goTotal 1 ]
	var decoded []any
	_, err := cbor.Decode(inner, &decoded)
	require.NoError(t, err)
	require.Len(t, decoded, 4)
	require.EqualValues(t, oneTrillion, decoded[1], "markTotal")
	require.EqualValues(
		t,
		1,
		decoded[2],
		"setTotal must be NonZero (1) at epoch 0",
	)
	require.EqualValues(
		t,
		1,
		decoded[3],
		"goTotal must be NonZero (1) at epoch 0",
	)
	// The per-pool set/go stakes remain plain zero (not clamped).
	pools, ok := decoded[0].(map[any]any)
	require.True(t, ok)
	for _, v := range pools {
		perPool, ok := v.([]any)
		require.True(t, ok)
		require.Len(t, perPool, 3)
		require.EqualValues(t, 0, perPool[1], "per-pool set stake stays 0")
		require.EqualValues(t, 0, perPool[2], "per-pool go stake stays 0")
	}
}

// TestQueryStakeSnapshotAllPoolsIncludesRetiredPool covers the all-pools path
// building the pool set from the union of the mark/set/go snapshots: a pool
// that has retired keeps historical set/go stake and must still be reported
// even though it has no current-epoch (mark) snapshot.
func TestQueryStakeSnapshotAllPoolsIncludesRetiredPool(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	meta := db.Metadata()
	// pool1 is active (mark@epoch 2). pool2 retired: it only appears in the
	// previous epoch's snapshot, which is the current epoch's "set".
	require.NoError(t, meta.SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{
			markSnapshot(t, 2, ssPool1Hex, oneTrillion),
			markSnapshot(t, 1, ssPool2Hex, oneTrillion),
		},
		nil,
	))
	require.NoError(t, meta.SaveEpochSummary(epochSummary(2, twoTrillion), nil))
	require.NoError(t, meta.SaveEpochSummary(epochSummary(1, oneTrillion), nil))
	ls := &LedgerState{db: db}
	ls.consensus.Store(
		&consensusSnapshot{currentEpoch: models.Epoch{EpochId: 2}},
	)

	inner := serialisedInner(t, ls, ssQueryAllPoolsHex)
	var result olocalstatequery.StakeSnapshotsResult
	_, err := cbor.Decode(inner, &result)
	require.NoError(t, err)
	require.Len(t, result.PoolSnapshots, 2, "both pools must be reported")

	pool1 := ledger.NewBlake2b224(mustHex(t, ssPool1Hex))
	pool2 := ledger.NewBlake2b224(mustHex(t, ssPool2Hex))
	require.Contains(t, result.PoolSnapshots, pool1)
	retired, ok := result.PoolSnapshots[pool2]
	require.True(t, ok, "retired pool with only set/go stake must be reported")
	assert.EqualValues(
		t,
		0,
		retired.StakeMark,
		"retired pool has no mark stake",
	)
	assert.EqualValues(
		t,
		oneTrillion,
		retired.StakeSet,
		"retired pool keeps its set stake",
	)
	assert.EqualValues(t, 0, retired.StakeGo)
}

// consensusAtVersion builds a consensus snapshot pinned to a protocol major
// version, so stake-snapshot tests can exercise the PV11 zero-pool filtering.
func consensusAtVersion(epoch uint64, major uint) *consensusSnapshot {
	return &consensusSnapshot{
		currentEpoch: models.Epoch{EpochId: epoch},
		currentPParams: &conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: major,
			},
		},
	}
}

// TestQueryStakeSnapshotNonexistentPoolPV10 verifies that below PV11 an
// explicitly requested pool with no stake is still returned, all-zero,
// matching pre-PV11 cardano-ledger GetStakeSnapshots semantics.
func TestQueryStakeSnapshotNonexistentPoolPV10(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := &LedgerState{db: db}
	ls.consensus.Store(consensusAtVersion(2, 10))

	inner := serialisedInner(t, ls, ssQueryOnePoolHex)
	var result olocalstatequery.StakeSnapshotsResult
	_, err := cbor.Decode(inner, &result)
	require.NoError(t, err)
	require.Len(
		t,
		result.PoolSnapshots,
		1,
		"below PV11 an explicitly requested pool is returned even with zero stake",
	)
	snap, ok := result.PoolSnapshots[ledger.NewBlake2b224(mustHex(t, ssPool1Hex))]
	require.True(t, ok)
	assert.EqualValues(t, 0, snap.StakeMark)
	assert.EqualValues(t, 0, snap.StakeSet)
	assert.EqualValues(t, 0, snap.StakeGo)
}

// TestQueryStakeSnapshotNonexistentPoolPV11 verifies that at PV11 an
// explicitly requested pool whose mark/set/go are all zero is omitted
// (cardano-ledger issue 5581).
func TestQueryStakeSnapshotNonexistentPoolPV11(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := &LedgerState{db: db}
	ls.consensus.Store(consensusAtVersion(2, 11))

	inner := serialisedInner(t, ls, ssQueryOnePoolHex)
	var result olocalstatequery.StakeSnapshotsResult
	_, err := cbor.Decode(inner, &result)
	require.NoError(t, err)
	assert.Empty(t, result.PoolSnapshots,
		"PV11 omits all-zero pools even when explicitly requested")
	// Totals are still reported, clamped to the NonZero minimum.
	assert.EqualValues(t, 1, result.TotalStakeMark)
}

// TestQueryStakeSnapshotAllPoolsPV11OmitsZeroStake verifies that at PV11 an
// all-pools query drops a pool whose snapshots are all zero while keeping
// pools that carry stake.
func TestQueryStakeSnapshotAllPoolsPV11OmitsZeroStake(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	meta := db.Metadata()
	require.NoError(t, meta.SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{
			markSnapshot(t, 2, ssPool1Hex, oneTrillion), // has stake
			markSnapshot(
				t,
				2,
				ssPool2Hex,
				0,
			), // explicit zero-stake row
		},
		nil,
	))
	require.NoError(t, meta.SaveEpochSummary(epochSummary(2, oneTrillion), nil))
	ls := &LedgerState{db: db}
	ls.consensus.Store(consensusAtVersion(2, 11))

	inner := serialisedInner(t, ls, ssQueryAllPoolsHex)
	var result olocalstatequery.StakeSnapshotsResult
	_, err := cbor.Decode(inner, &result)
	require.NoError(t, err)
	require.Len(t, result.PoolSnapshots, 1)
	assert.Contains(t, result.PoolSnapshots,
		ledger.NewBlake2b224(mustHex(t, ssPool1Hex)), "pool with stake is kept")
	assert.NotContains(
		t,
		result.PoolSnapshots,
		ledger.NewBlake2b224(
			mustHex(t, ssPool2Hex),
		),
		"all-zero pool is omitted",
	)
}

// protocolParamsQuery wraps GetCurrentProtocolParams the way the wire
// delivers it, matching poolDistr2Query/stakeDistributionQuery in the
// neighboring query test files.
func protocolParamsQuery() *olocalstatequery.BlockQuery {
	return &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyCurrentProtocolParamsQuery{},
		},
	}
}

// conwayPParamsWithCostModels builds a Conway pparams value with every
// cbor.Rat-bearing field populated, not just CostModels -- blinklabs-io/dingo#3825's
// PR review (wolf31o2): a fixture that only sets CostModels type-asserts fine
// but is not actually encodable, since cbor.Rat.MarshalCBOR panics on the nil
// *big.Rat a zero-value cbor.Rat (or a nil *cbor.Rat pointer field) carries,
// and PoolVotingThresholds/DRepVotingThresholds's value-typed cbor.Rat fields
// are always encoded (never skippable as CBOR null the way a nil *cbor.Rat
// pointer field is). This is what real cardano-node protocol-parameter data
// always has populated, so an end-to-end wire test should encode a value
// shaped like the real thing, not a partial struct that happens to satisfy a
// type assertion.
func conwayPParamsWithCostModels(
	costModels map[uint][]int64,
) *conway.ConwayProtocolParameters {
	rat := func(n, d int64) cbor.Rat { return cbor.Rat{Rat: big.NewRat(n, d)} }
	ratPtr := func(n, d int64) *cbor.Rat { return &cbor.Rat{Rat: big.NewRat(n, d)} }
	return &conway.ConwayProtocolParameters{
		CostModels:                 costModels,
		A0:                         ratPtr(3, 10),
		Rho:                        ratPtr(3, 1000),
		Tau:                        ratPtr(1, 5),
		MinFeeRefScriptCostPerByte: ratPtr(15, 1),
		ExecutionCosts: lcommon.ExUnitPrice{
			MemPrice:  ratPtr(577, 10000),
			StepPrice: ratPtr(721, 10000000),
		},
		PoolVotingThresholds: conway.PoolVotingThresholds{
			MotionNoConfidence:    rat(51, 100),
			CommitteeNormal:       rat(51, 100),
			CommitteeNoConfidence: rat(51, 100),
			HardForkInitiation:    rat(51, 100),
			PpSecurityGroup:       rat(51, 100),
		},
		DRepVotingThresholds: conway.DRepVotingThresholds{
			MotionNoConfidence:    rat(67, 100),
			CommitteeNormal:       rat(67, 100),
			CommitteeNoConfidence: rat(60, 100),
			UpdateToConstitution:  rat(75, 100),
			HardForkInitiation:    rat(60, 100),
			PpNetworkGroup:        rat(67, 100),
			PpEconomicGroup:       rat(67, 100),
			PpTechnicalGroup:      rat(67, 100),
			PpGovGroup:            rat(75, 100),
			TreasuryWithdrawal:    rat(67, 100),
		},
	}
}

// TestInjectedSyntheticV2CostModel_DetectsHardForkBabbagesDefault covers the
// actual code path this session found responsible for blinklabs-io/dingo#3825:
// HardForkBabbage fabricates a PlutusV2 cost model whenever the previous
// era's params don't have one -- real for any Alonzo genesis, since the
// AlonzoGenesisCostModels format predates PlutusV2 entirely and never has a
// slot for it.
func TestInjectedSyntheticV2CostModel_DetectsHardForkBabbagesDefault(
	t *testing.T,
) {
	t.Parallel()

	prev := &alonzo.AlonzoProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}},
	}
	after, err := eras.HardForkBabbage(nil, prev)
	require.NoError(t, err)

	assert.True(t, injectedSyntheticV2CostModel(prev, after))
}

// TestInjectedSyntheticV2CostModel_FalseWhenAlreadyPresent covers a pparams
// value that already carries a real (non-fabricated) PlutusV2 entry before
// the transition -- HardForkBabbage's own guard (`if _, hasV2 :=
// ret.CostModels[1]; !hasV2`) leaves it untouched, so nothing was injected.
func TestInjectedSyntheticV2CostModel_FalseWhenAlreadyPresent(t *testing.T) {
	t.Parallel()

	realV2 := []int64{9, 9, 9}
	prev := &alonzo.AlonzoProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}, 1: realV2},
	}
	after, err := eras.HardForkBabbage(nil, prev)
	require.NoError(t, err)

	assert.False(t, injectedSyntheticV2CostModel(prev, after))
}

// TestInjectedSyntheticV2CostModel_FalseWhenValueIsNotTheKnownDefault covers
// a hypothetical newly-added key 1 whose value does not match
// eras.DefaultPlutusV2CostModel -- only the exact known fabricated value
// counts as synthetic, not "any new key 1."
func TestInjectedSyntheticV2CostModel_FalseWhenValueIsNotTheKnownDefault(
	t *testing.T,
) {
	t.Parallel()

	before := &babbage.BabbageProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}},
	}
	after := &babbage.BabbageProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}, 1: {999}},
	}

	assert.False(t, injectedSyntheticV2CostModel(before, after))
}

// TestWithoutSyntheticV2CostModel_RemovesKeyWithoutMutatingOriginal covers
// the query-boundary filter: when synthetic is true, the returned value
// omits PlutusV2 while every other key survives, and the original pparams
// (still reachable from internal validation state) is never mutated.
func TestWithoutSyntheticV2CostModel_RemovesKeyWithoutMutatingOriginal(
	t *testing.T,
) {
	t.Parallel()

	original := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{
			0: {1, 1, 1},
			1: {2, 2, 2},
			2: {3, 3, 3},
		},
	}

	filtered := withoutSyntheticV2CostModel(original, true, nil)

	fp, ok := filtered.(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	assert.NotContains(t, fp.CostModels, uint(1))
	assert.Equal(t, []int64{1, 1, 1}, fp.CostModels[0])
	assert.Equal(t, []int64{3, 3, 3}, fp.CostModels[2])

	// The original, still reachable from ls.currentPParams / the published
	// snapshot for internal script validation, must be untouched.
	assert.Contains(t, original.CostModels, uint(1))
	assert.Equal(t, []int64{2, 2, 2}, original.CostModels[1])
}

// TestWithoutSyntheticV2CostModel_CoversEveryEraType covers
// blinklabs-io/dingo#3825's PR review: the filter's type switch must handle
// every era type ShelleyCurrentProtocolParamsQuery can actually return
// (Alonzo, Babbage, Conway, Dijkstra), not just Conway -- a regression in
// any branch would otherwise pass the suite silently.
func TestWithoutSyntheticV2CostModel_CoversEveryEraType(t *testing.T) {
	t.Parallel()

	costModels := map[uint][]int64{0: {1}, 1: {2}, 2: {3}}

	t.Run("Alonzo", func(t *testing.T) {
		pp := &alonzo.AlonzoProtocolParameters{CostModels: cloneMap(costModels)}
		got := withoutSyntheticV2CostModel(pp, true, nil)
		fp, ok := got.(*alonzo.AlonzoProtocolParameters)
		require.True(t, ok)
		assert.NotContains(t, fp.CostModels, uint(1))
		assert.Contains(t, pp.CostModels, uint(1), "original must be untouched")
	})
	t.Run("Babbage", func(t *testing.T) {
		pp := &babbage.BabbageProtocolParameters{
			CostModels: cloneMap(costModels),
		}
		got := withoutSyntheticV2CostModel(pp, true, nil)
		fp, ok := got.(*babbage.BabbageProtocolParameters)
		require.True(t, ok)
		assert.NotContains(t, fp.CostModels, uint(1))
		assert.Contains(t, pp.CostModels, uint(1), "original must be untouched")
	})
	t.Run("Conway", func(t *testing.T) {
		pp := &conway.ConwayProtocolParameters{CostModels: cloneMap(costModels)}
		got := withoutSyntheticV2CostModel(pp, true, nil)
		fp, ok := got.(*conway.ConwayProtocolParameters)
		require.True(t, ok)
		assert.NotContains(t, fp.CostModels, uint(1))
		assert.Contains(t, pp.CostModels, uint(1), "original must be untouched")
	})
	t.Run("Dijkstra", func(t *testing.T) {
		pp := &dijkstra.DijkstraProtocolParameters{
			ConwayProtocolParameters: conway.ConwayProtocolParameters{
				CostModels: cloneMap(costModels),
			},
		}
		got := withoutSyntheticV2CostModel(pp, true, nil)
		fp, ok := got.(*dijkstra.DijkstraProtocolParameters)
		require.True(t, ok)
		assert.NotContains(t, fp.CostModels, uint(1))
		assert.Contains(t, pp.CostModels, uint(1), "original must be untouched")
	})
}

func cloneMap(m map[uint][]int64) map[uint][]int64 {
	out := make(map[uint][]int64, len(m))
	for k, v := range m {
		out[k] = append([]int64(nil), v...)
	}
	return out
}

// TestWithoutSyntheticV2CostModel_NilPointerDoesNotPanic covers
// blinklabs-io/dingo#3825's PR review: a concrete-typed nil pointer
// (lcommon.ProtocolParameters holding e.g. a nil *conway.ConwayProtocolParameters)
// still matches its type's case in the switch, so each case must guard
// against nil before dereferencing rather than panicking.
func TestWithoutSyntheticV2CostModel_NilPointerDoesNotPanic(t *testing.T) {
	t.Parallel()

	var nilConway *conway.ConwayProtocolParameters
	var pp lcommon.ProtocolParameters = nilConway

	require.NotPanics(t, func() {
		got := withoutSyntheticV2CostModel(pp, true, nil)
		assert.Equal(t, pp, got)
	})
}

// TestWithoutSyntheticV2CostModel_NoOpWhenNotSynthetic covers the common
// case: once real data has been observed (or none was ever fabricated),
// the filter must return the value unchanged, identical pointer included,
// so a caller reading it sees the exact same struct internal validation
// uses.
func TestWithoutSyntheticV2CostModel_NoOpWhenNotSynthetic(t *testing.T) {
	t.Parallel()

	pp := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{0: {1}, 1: {2}, 2: {3}},
	}

	got := withoutSyntheticV2CostModel(pp, false, nil)

	assert.Same(t, pp, got)
}

// unknownProtocolParameters is a lcommon.ProtocolParameters implementation
// the withoutSyntheticV2CostModel switch has no case for -- standing in for
// a future era type this switch hasn't been taught yet.
type unknownProtocolParameters struct {
	lcommon.ProtocolParameters
}

// TestWithoutSyntheticV2CostModel_UnknownTypeLogsAndReturnsUnfiltered covers
// blinklabs-io/dingo#3825's PR review (wolf31o2): a protocol-parameters type
// the switch doesn't recognize falls to the default branch, which -- unlike
// every other branch -- returns pp unfiltered even though synthetic is true.
// That silently reintroduces #3825 for whatever type this is; the least this
// path can do is log so the gap is observable instead of invisible.
func TestWithoutSyntheticV2CostModel_UnknownTypeLogsAndReturnsUnfiltered(
	t *testing.T,
) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	pp := &unknownProtocolParameters{}

	got := withoutSyntheticV2CostModel(pp, true, logger)

	assert.Same(t, pp, got,
		"an unrecognized type must still be returned, unfiltered")
	assert.Contains(
		t,
		buf.String(),
		"does not recognize this protocol-parameters type",
	)
}

// TestExtractRawCostModels_CoversDijkstra covers blinklabs-io/dingo#3825's PR
// review (wolf31o2): extractRawCostModels' type switch lacked a Dijkstra
// case (falling to its own default: return nil), asymmetric with
// withoutSyntheticV2CostModel, which does handle Dijkstra -- meaning
// injectedSyntheticV2CostModel (built on extractRawCostModels) could never
// detect a Dijkstra-era injection even though the filter it feeds covers
// that era.
func TestExtractRawCostModels_CoversDijkstra(t *testing.T) {
	t.Parallel()

	pp := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			CostModels: map[uint][]int64{0: {1}, 1: {2}},
		},
	}

	got := extractRawCostModels(pp)

	assert.Equal(t, map[uint][]int64{0: {1}, 1: {2}}, got)
}

// TestExtractRawCostModels_NilPointerDoesNotPanic verifies that a concrete-typed
// nil pointer (lcommon.ProtocolParameters
// holding e.g. a nil *dijkstra.DijkstraProtocolParameters) still matches its
// type's case in the switch, so every case must guard against nil before
// dereferencing rather than panicking -- mirroring the guard
// withoutSyntheticV2CostModel already has for the identical hazard.
func TestExtractRawCostModels_NilPointerDoesNotPanic(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		pp   lcommon.ProtocolParameters
	}{
		{"Alonzo", (*alonzo.AlonzoProtocolParameters)(nil)},
		{"Babbage", (*babbage.BabbageProtocolParameters)(nil)},
		{"Conway", (*conway.ConwayProtocolParameters)(nil)},
		{"Dijkstra", (*dijkstra.DijkstraProtocolParameters)(nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				got := extractRawCostModels(tc.pp)
				assert.Nil(t, got)
			})
		})
	}
}

// TestQueryShelleyCurrentProtocolParams_OmitsSyntheticV2CostModel is the
// end-to-end regression test for blinklabs-io/dingo#3825: confirmed against
// a real cardano-node's raw wire bytes (captured via a temporary diagnostic,
// decoded with the real client-side type, independent of any display-layer
// bug) that on a chain which has never received a real PlutusV2
// cost-model update, a real cardano-node's GetCurrentProtocolParams reply
// has no PlutusV2 entry at all -- while Dingo's internal state always
// carries HardForkBabbage's fabricated one, needed for real script
// validation. The LocalStateQuery reply must match the real node's
// observable behavior; internal validation must not be affected.
func TestQueryShelleyCurrentProtocolParams_OmitsSyntheticV2CostModel(
	t *testing.T,
) {
	t.Parallel()

	ls := newPoolDistr2Ledger(t, newTestDB(t))
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(map[uint][]int64{
		0: {1, 1, 1},
		1: eras.DefaultPlutusV2CostModel,
		2: {3, 3, 3},
	})
	ls.syntheticV2CostModel = true
	ls.publishSnapshotsLocked()

	result, err := ls.Query(protocolParamsQuery(), QueryPoint{})
	require.NoError(t, err)

	arr, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)
	pp, ok := arr[0].(*conway.ConwayProtocolParameters)
	require.True(t, ok)

	assert.NotContains(t, pp.CostModels, uint(1),
		"the reply must omit the synthetic PlutusV2 cost model")
	assert.Contains(t, pp.CostModels, uint(0))
	assert.Contains(t, pp.CostModels, uint(2))

	// This is a wire-level regression test, not just a type-assertion check:
	// encode what the reply actually contains and decode it back with the
	// real client-side type, matching the raw-CBOR verification this issue's
	// original diagnosis relied on independent of any display-layer bug.
	encoded, err := cbor.Encode(pp)
	require.NoError(t, err)
	var decoded conway.ConwayProtocolParameters
	_, err = cbor.Decode(encoded, &decoded)
	require.NoError(t, err)
	assert.NotContains(
		t,
		decoded.CostModels,
		uint(1),
		"the encoded wire bytes must not carry the synthetic PlutusV2 cost model",
	)

	// Internal validation state must be completely unaffected by the query.
	internal, ok := ls.currentPParams.(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	assert.Contains(t, internal.CostModels, uint(1),
		"internal state must keep the default for real script validation")
}

// TestQueryShelleyCurrentProtocolParams_IncludesRealV2CostModel covers the
// other half: once real governance data has cleared the synthetic marker
// (LedgerState.syntheticV2CostModel == false), the reply must include
// whatever is actually in CostModels -- including a value that happens to
// equal the known synthetic default, since real governance re-affirming
// that exact value is still real data, not still a guess.
func TestQueryShelleyCurrentProtocolParams_IncludesRealV2CostModel(
	t *testing.T,
) {
	t.Parallel()

	ls := newPoolDistr2Ledger(t, newTestDB(t))
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(map[uint][]int64{
		0: {1, 1, 1},
		1: eras.DefaultPlutusV2CostModel,
		2: {3, 3, 3},
	})
	ls.syntheticV2CostModel = false
	ls.publishSnapshotsLocked()

	result, err := ls.Query(protocolParamsQuery(), QueryPoint{})
	require.NoError(t, err)

	arr, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)
	pp, ok := arr[0].(*conway.ConwayProtocolParameters)
	require.True(t, ok)

	assert.Contains(t, pp.CostModels, uint(1))
	assert.Equal(t, eras.DefaultPlutusV2CostModel, pp.CostModels[1])

	encoded, err := cbor.Encode(pp)
	require.NoError(t, err)
	var decoded conway.ConwayProtocolParameters
	_, err = cbor.Decode(encoded, &decoded)
	require.NoError(t, err)
	assert.Equal(
		t,
		eras.DefaultPlutusV2CostModel,
		decoded.CostModels[1],
		"real data equal to the known default must still round-trip on the wire",
	)
}

// TestGetCurrentPParamsForReporting_OmitsSyntheticV2CostModel covers
// blinklabs-io/dingo#3825's PR review (wolf31o2): withoutSyntheticV2CostModel
// originally had a single call site (queries.go's LocalStateQuery handler),
// while every other interface reporting current parameters --
// api/blockfrost, api/utxorpc, api/mesh -- read GetCurrentPParams()
// unfiltered and would still report a synthetic PlutusV2 entry a real
// cardano-node never has. GetCurrentPParamsForReporting is the shared
// accessor all of those now use; this proves its filtering behavior
// directly, independent of which specific API surface calls it.
func TestGetCurrentPParamsForReporting_OmitsSyntheticV2CostModel(t *testing.T) {
	t.Parallel()

	ls := newPoolDistr2Ledger(t, newTestDB(t))
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(map[uint][]int64{
		0: {1, 1, 1},
		1: eras.DefaultPlutusV2CostModel,
		2: {3, 3, 3},
	})
	ls.syntheticV2CostModel = true
	ls.publishSnapshotsLocked()

	reported := ls.GetCurrentPParamsForReporting()
	pp, ok := reported.(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	assert.NotContains(t, pp.CostModels, uint(1),
		"the reporting accessor must omit the synthetic PlutusV2 cost model,"+
			" matching every other reporting surface")

	// GetCurrentPParams (used by internal validation, block-building, Leios
	// committee parameters, and governance-action decoding) must be
	// completely unaffected.
	internal, ok := ls.GetCurrentPParams().(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	assert.Contains(t, internal.CostModels, uint(1),
		"GetCurrentPParams must keep the default for internal validation")
}

// TestGetCurrentPParamsForReporting_IncludesRealV2CostModel covers the other
// half: once the synthetic marker is cleared, the reporting accessor must
// return the same value GetCurrentPParams does.
func TestGetCurrentPParamsForReporting_IncludesRealV2CostModel(t *testing.T) {
	t.Parallel()

	ls := newPoolDistr2Ledger(t, newTestDB(t))
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(map[uint][]int64{
		0: {1, 1, 1},
		1: eras.DefaultPlutusV2CostModel,
		2: {3, 3, 3},
	})
	ls.syntheticV2CostModel = false
	ls.publishSnapshotsLocked()

	reported := ls.GetCurrentPParamsForReporting()
	pp, ok := reported.(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	assert.Contains(t, pp.CostModels, uint(1))
	assert.Equal(t, eras.DefaultPlutusV2CostModel, pp.CostModels[1])
}

// TestSyntheticV2CostModelPersistence_RoundTripsAcrossRestart covers
// blinklabs-io/dingo#3825's PR review: LedgerState.syntheticV2CostModel must
// survive a restart via persistSyntheticV2CostModel/loadSyntheticV2CostModel,
// not silently reconstruct as false (the zero value) regardless of the
// chain's real history.
func TestSyntheticV2CostModelPersistence_RoundTripsAcrossRestart(t *testing.T) {
	t.Parallel()

	ls := newPoolDistr2Ledger(t, newTestDB(t))

	// Not yet persisted: a fresh database reads back false, same as an
	// explicit false would.
	ls.loadSyntheticV2CostModel()
	assert.False(t, ls.syntheticV2CostModel)

	require.NoError(t, ls.persistSyntheticV2CostModel(true, nil))
	// Simulate a restart: a fresh in-memory value, restored from the same
	// database.
	ls.syntheticV2CostModel = false
	ls.loadSyntheticV2CostModel()
	assert.True(t, ls.syntheticV2CostModel,
		"restored value must survive the simulated restart")

	require.NoError(t, ls.persistSyntheticV2CostModel(false, nil))
	ls.syntheticV2CostModel = true
	ls.loadSyntheticV2CostModel()
	assert.False(t, ls.syntheticV2CostModel,
		"a later persisted false must also survive the simulated restart")
}

// TestResolveSyntheticV2CostModel_BootstrapsFromValueWhenMarkerAbsent covers
// blinklabs-io/dingo#3825's PR review (wolf31o2): a database that predates
// this marker (or one that was reset by
// database.RecomputeSyntheticV2CostModelMarkerAfterTruncate) must not
// silently behave as "not synthetic" -- it must compare the current PlutusV2
// cost model directly against the known synthetic default instead.
func TestResolveSyntheticV2CostModel_BootstrapsFromValueWhenMarkerAbsent(
	t *testing.T,
) {
	t.Parallel()

	stillSynthetic := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{1: eras.DefaultPlutusV2CostModel},
	}
	assert.True(t, resolveSyntheticV2CostModel("", stillSynthetic),
		"an absent marker with the exact synthetic default present must"+
			" resolve to still-synthetic")

	realData := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{1: {9, 9, 9}},
	}
	assert.False(t, resolveSyntheticV2CostModel("", realData),
		"an absent marker with a value that differs from the synthetic"+
			" default must resolve to real, not synthetic")

	noV2 := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}},
	}
	assert.False(t, resolveSyntheticV2CostModel("", noV2),
		"an absent marker with no PlutusV2 key at all must resolve to"+
			" not-synthetic")

	assert.False(t, resolveSyntheticV2CostModel("", nil),
		"an absent marker with nil pparams must resolve to not-synthetic")
}

// TestResolveSyntheticV2CostModel_ExplicitValueWins covers the common case:
// an explicitly persisted marker value is trusted directly, regardless of
// what pp happens to contain.
func TestResolveSyntheticV2CostModel_ExplicitValueWins(t *testing.T) {
	t.Parallel()

	realData := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{1: eras.DefaultPlutusV2CostModel},
	}
	assert.True(t, resolveSyntheticV2CostModel("true", realData))
	assert.False(t, resolveSyntheticV2CostModel("false", realData))
}

// TestMarkRealV2CostModelObserved_KeepsEarliestConfirmationAcrossMultipleUpdates
// verifies that a chain that enacts more than one real PlutusV2 cost-model
// update over its life does not have
// its cleared-epoch marker overwritten by the later update -- doing so would
// make RecomputeSyntheticV2CostModelMarkerAfterTruncate incorrectly reset
// the marker to synthetic on a rollback that crosses back past only the
// LATEST update but not an EARLIER one, even though the earlier real value
// still survives on the truncated chain.
func TestMarkRealV2CostModelObserved_KeepsEarliestConfirmationAcrossMultipleUpdates(
	t *testing.T,
) {
	t.Parallel()

	ls, db := newExpiryRollbackTestLedger(t, false, 0)

	// First real update confirmed at epoch 5 (slot 500).
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.markRealV2CostModelObserved(5, txn)
	}))

	// A second real update (e.g. a later governance-enacted cost-model
	// change) confirmed at epoch 10 (slot 1000) must not overwrite the
	// epoch-5 confirmation.
	txn = db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.markRealV2CostModelObserved(10, txn)
	}))

	clearedEpoch, cleared, err := database.SyntheticV2CostModelClearedEpoch(
		db, nil,
	)
	require.NoError(t, err)
	require.True(t, cleared)
	require.Equal(
		t,
		uint64(5),
		clearedEpoch,
		"the earliest confirmation must be kept, not overwritten by the later one",
	)

	// Roll back to slot 700 (epoch 7): after the first real update, before
	// the second. The surviving chain's PlutusV2 cost model is still real
	// (from the first update), so the marker must NOT be reset to synthetic.
	require.NoError(
		t,
		database.RecomputeSyntheticV2CostModelMarkerAfterTruncate(db, nil, 700),
	)

	value, err := db.GetSyncState(database.SyntheticV2CostModelSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, "false", value,
		"the surviving chain still has real data from the first update"+
			" and must not be reported as synthetic")
}

// TestRollbackRestore_LeavesRealPreExistingModelCorrectlyResolvedAsNotSynthetic
// covers blinklabs-io/dingo#3825's PR review (wolf31o2): on a database that
// predates these markers entirely, a real PlutusV2 cost model already in
// force (differing from the known synthetic default) can still pick up a
// clearedEpoch marker from the first update tracked AFTER these markers
// existed, even though the model was already real long before that epoch.
// A rollback crossing that epoch must not force the marker to "true" --
// doing so would misreport a real, already-in-force model as synthetic.
// Deleting it instead (RecomputeSyntheticV2CostModelMarkerAfterTruncate)
// lets resolveSyntheticV2CostModel's absent-marker fallback re-derive the
// correct answer from the live value.
func TestRollbackRestore_LeavesRealPreExistingModelCorrectlyResolvedAsNotSynthetic(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	// differs from eras.DefaultPlutusV2CostModel
	realNonDefaultV2 := []int64{1, 2, 3}

	// A persisted epoch table is needed for EpochBySlot to resolve the
	// rollback slot below (epochs 0-9, 100 slots each).
	for i := range uint64(10) {
		require.NoError(t, db.SetEpoch(
			i*100, i, nil, nil, nil, nil, 1, 1000, 100, nil,
		))
	}

	// A clearedEpoch marker exists (from the first tracked update after
	// these markers were introduced), even though the real model has
	// actually been in force since before that epoch.
	require.NoError(t, database.SetSyntheticV2CostModelClearedEpoch(db, nil, 5))
	require.NoError(
		t,
		db.SetSyncState(database.SyntheticV2CostModelSyncKey, "false", nil),
	)

	// Roll back to before epoch 5.
	require.NoError(
		t,
		database.RecomputeSyntheticV2CostModelMarkerAfterTruncate(db, nil, 0),
	)

	// The boolean marker must be absent, not forced to "true".
	value, err := db.GetSyncState(database.SyntheticV2CostModelSyncKey, nil)
	require.NoError(t, err)
	require.Empty(t, value)

	// A fresh load against the surviving (real, non-default) pparams value
	// must resolve to "not synthetic", not be misreported as synthetic.
	pp := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{1: realNonDefaultV2},
	}
	assert.False(t, resolveSyntheticV2CostModel(value, pp),
		"a real, non-default model already in force must not be reported"+
			" as synthetic just because a later marker briefly existed")
}

// TestTransitionToEraFrom_PersistsSyntheticMarkerInSameTransactionAsPParams
// verifies that the synthetic-cost-model marker is written in the same database
// transaction as the pparams update it describes, not committed
// separately afterward. If they were in different transactions, a crash
// between the two commits could leave a stale marker on restart. This is
// proven here by rolling the transaction back entirely: since both writes
// share one transaction, rollback must undo both together, and a fresh
// read must see neither.
func TestTransitionToEraFrom_PersistsSyntheticMarkerInSameTransactionAsPParams(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	prev := &alonzo.AlonzoProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}},
	}

	txn := db.Transaction(true)
	result, err := ls.transitionToEraFrom(
		txn,
		eras.BabbageEraDesc.Id,
		1,
		100,
		prev,
		eras.AlonzoEraDesc.Id,
	)
	require.NoError(t, err)
	require.True(t, result.InjectedSyntheticV2CostModel)
	require.NoError(t, txn.Rollback())

	// Nothing must be durable: neither the pparams write nor the marker,
	// since they shared one now-rolled-back transaction.
	ls.syntheticV2CostModel = false
	ls.loadSyntheticV2CostModel()
	assert.False(t, ls.syntheticV2CostModel,
		"a rolled-back transaction must not leave the marker persisted")
	rolledBackPParams, err := db.GetPParams(
		1, eras.BabbageEraDesc.Id, eras.DecodePParamsBabbage, nil,
	)
	require.NoError(t, err)
	assert.Nil(t, rolledBackPParams,
		"a rolled-back transaction must not leave the pparams write persisted")

	// The same sequence, committed instead, must persist both together.
	txn = db.Transaction(true)
	result, err = ls.transitionToEraFrom(
		txn,
		eras.BabbageEraDesc.Id,
		1,
		100,
		prev,
		eras.AlonzoEraDesc.Id,
	)
	require.NoError(t, err)
	require.True(t, result.InjectedSyntheticV2CostModel)
	require.NoError(t, txn.Commit())

	ls.syntheticV2CostModel = false
	ls.loadSyntheticV2CostModel()
	assert.True(t, ls.syntheticV2CostModel,
		"a committed transaction must persist the marker")
	committedPParams, err := db.GetPParams(
		1, eras.BabbageEraDesc.Id, eras.DecodePParamsBabbage, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, committedPParams,
		"a committed transaction must persist the pparams write")
}

// awaitTransitionInfo blocks until evaluateHardForkInitiationStability's
// async tally goroutine commits a new transitionInfo, then returns it
// under a read lock so the caller's assertions race-free against the
// goroutine's write. Use this whenever a test expects the helper to
// promote transitionInfo (Unknown/Impossible -> Known); paths that
// short-circuit before spawning the goroutine don't need it.
func awaitTransitionInfo(
	t *testing.T,
	ls *LedgerState,
	want hardfork.TransitionState,
) hardfork.TransitionInfo {
	t.Helper()
	require.Eventually(t, func() bool {
		ls.RLock()
		defer ls.RUnlock()
		return ls.transitionInfo.State == want
	}, testutil.AsyncWait, 5*time.Millisecond,
		"transitionInfo.State did not reach %v", want)
	ls.RLock()
	defer ls.RUnlock()
	return ls.transitionInfo
}

func awaitHFIEvalIdle(t *testing.T, ls *LedgerState) {
	t.Helper()
	require.Eventually(t, func() bool {
		return !ls.hfiStabilityEvalInFlight.Load()
	}, testutil.AsyncWait, 5*time.Millisecond,
		"HFI stability evaluation did not become idle")
}

// stabilityFixtureEpoch parameters: Shelley-style 432_000-slot epoch,
// safeZone = ceil(3*432/0.05) = 25_920, voting deadline distance from
// epoch end is 2 * safeZone = 51_840. So an epoch ending at slot
// 532_000 has its voting deadline at slot 480_160.
const (
	stabilityFixtureEpochID    uint64 = 500
	stabilityFixtureEpochStart uint64 = 100_000
	stabilityFixtureEpochLen   uint   = 432_000
	stabilityFixtureEpochEnd   uint64 = stabilityFixtureEpochStart +
		uint64(stabilityFixtureEpochLen)
	stabilityFixtureVotingDeadline uint64 = stabilityFixtureEpochEnd - 2*25_920
)

// stabilityFixtureLedgerState assembles a LedgerState wired with
// real-shaped Shelley genesis (so calculateStabilityWindowForEra returns
// the expected 25_920) and a file-backed SQLite DB. The caller seeds proposal /
// vote rows on db, sets currentTip and transitionInfo, and invokes
// evaluateHardForkInitiationStability.
//
// Setting currentPParams to Conway pparams with the supplied major
// version exercises the post-Conway code path inside the helper.
// Bootstrap (major 9) waives the DRep threshold but preserves the
// action-specific SPO and committee thresholds, so ratifiable fixtures must
// include both voting bodies.
func stabilityFixtureLedgerState(
	t *testing.T,
	major uint,
) (*LedgerState, *database.Database) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	pparams := mockledger.NewMockConwayProtocolParams()
	pparams.ProtocolVersion.Major = major
	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentEpoch: newTestEpoch(
			stabilityFixtureEpochID,
			stabilityFixtureEpochStart,
			stabilityFixtureEpochLen,
			eras.ConwayEraDesc.Id,
		),
		currentPParams: &pparams,
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	return ls, db
}

// seedRatifiableBootstrapHardForkInitiation primes the DB so that the
// governance ratifiability helper returns a non-nil result when the
// bootstrap (major 9) ratification rule is in effect — a HardForkInitiation
// proposal in the active set plus the required CC and SPO yes votes.
func seedRatifiableBootstrapHardForkInitiation(
	t *testing.T,
	db *database.Database,
	currentEpoch uint64,
	targetMajor uint,
) *models.GovernanceProposal {
	t.Helper()
	action := &lcommon.HardForkInitiationGovAction{Type: 1}
	action.ProtocolVersion.Major = targetMajor
	action.ProtocolVersion.Minor = 0
	cborBytes, err := cbor.Encode(action)
	require.NoError(t, err)

	proposal := &models.GovernanceProposal{
		TxHash:        repeatByte(32, 0xAA),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
		ProposedEpoch: currentEpoch - 1,
		ExpiresEpoch:  currentEpoch + 10,
		Deposit:       1_000,
		ReturnAddress: repeatByte(29, 0),
		AnchorURL:     "https://example.invalid/anchor",
		AnchorHash:    repeatByte(32, 0xEE),
		GovActionCbor: cborBytes,
		AddedSlot:     1,
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))
	loaded, err := db.GetGovernanceProposal(proposal.TxHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, loaded)

	drepCred := repeatByte(28, 0xBB)
	stakeCred := repeatByte(28, 0xCC)
	require.NoError(t, db.CreateDrep(nil, &models.Drep{
		Credential: drepCred,
		Active:     true,
		AddedSlot:  1,
	}))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Drep:       drepCred,
		DrepType:   models.DrepTypeAddrKeyHash,
		AddedSlot:  1,
		Active:     true,
	}))
	require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
		TxId:       repeatByte(32, 0x01),
		OutputIdx:  0,
		StakingKey: stakeCred,
		AddedSlot:  1,
		Amount:     types.Uint64(1_000),
	}))
	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      loaded.ID,
		VoterType:       models.VoterTypeDRep,
		VoterCredential: drepCred,
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))
	coldCred := repeatByte(28, 0xCE)
	hotCred := repeatByte(28, 0xCF)
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{
		{ColdCredHash: coldCred, ExpiresEpoch: currentEpoch + 10},
	}, nil))
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO auth_committee_hot (
    cold_credential, host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?)`, coldCred, hotCred, 1, 1)
	require.NoError(t, err)
	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      loaded.ID,
		VoterType:       models.VoterTypeCC,
		VoterCredential: hotCred,
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))
	poolCred := repeatByte(28, 0xDD)
	// governance.predictedBoundaryStakeEpochFor(currentEpoch) resolves to
	// currentEpoch itself (dingo#4441): the mid-epoch check tallies the SPO
	// vote against mark[currentEpoch], the last mark durably written at the
	// boundary that opened the currently active epoch. The boundary it
	// predicts will instead tally mark[currentEpoch+1], which SNAP does not
	// capture until that boundary runs.
	require.NoError(t, db.Metadata().SavePoolStakeSnapshot(
		&models.PoolStakeSnapshot{
			Epoch:        currentEpoch,
			SnapshotType: models.PoolStakeSnapshotTypeMark,
			PoolKeyHash:  poolCred,
			TotalStake:   types.Uint64(1_000),
		},
		nil,
	))
	require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      loaded.ID,
		VoterType:       models.VoterTypeSPO,
		VoterCredential: poolCred,
		Vote:            models.VoteYes,
		AddedSlot:       2,
	}, nil))
	return loaded
}

func repeatByte(length int, b byte) []byte {
	out := make([]byte, length)
	for i := range out {
		out[i] = b
	}
	return out
}

// TestEvaluateHardForkInitiationStability_PreDeadline_NoChange pins the
// "votes can still flip the outcome" guard: while the current tip is
// before the voting deadline (epochEnd - 2*stabilityWindow), the helper
// must not surface the upcoming transition even if the in-flight
// proposal currently meets thresholds — a yet-to-arrive No vote could
// still defeat it.
func TestEvaluateHardForkInitiationStability_PreDeadline_NoChange(
	t *testing.T,
) {
	t.Parallel()

	ls, db := stabilityFixtureLedgerState(t, 9 /* bootstrap */)
	seedRatifiableBootstrapHardForkInitiation(
		t, db, stabilityFixtureEpochID, 7,
	)
	// Tip one slot before the voting deadline.
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(
			stabilityFixtureVotingDeadline-1,
			[]byte("tip"),
		),
	}

	ls.evaluateHardForkInitiationStability()

	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"pre-deadline must not promote to TransitionKnown")
}

// TestEvaluateHardForkInitiationStability_PostDeadline_Ratifiable_SetsKnown
// is the core happy-path: after the voting deadline, with a ratifiable
// HardForkInitiation in flight, the helper must set TransitionKnown for
// the epoch the boundary will fire (currentEpoch + 1).
func TestEvaluateHardForkInitiationStability_PostDeadline_Ratifiable_SetsKnown(
	t *testing.T,
) {
	t.Parallel()

	ls, db := stabilityFixtureLedgerState(t, 9 /* bootstrap */)
	seedRatifiableBootstrapHardForkInitiation(
		t, db, stabilityFixtureEpochID, 7,
	)
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(stabilityFixtureVotingDeadline, []byte("tip")),
	}

	ls.evaluateHardForkInitiationStability()

	got := awaitTransitionInfo(t, ls, hardfork.TransitionKnown)
	assert.Equal(t, stabilityFixtureEpochID+1, got.KnownEpoch,
		"target epoch is the next epoch boundary")
}

// TestEvaluateHardForkInitiationStability_PostDeadline_NotRatifiable_NoChange
// pins the negative side: post-deadline without a ratifiable proposal,
// transitionInfo stays Unknown. (No proposal seeded; helper returns nil.)
func TestEvaluateHardForkInitiationStability_PostDeadline_NotRatifiable_NoChange(
	t *testing.T,
) {
	t.Parallel()

	ls, _ := stabilityFixtureLedgerState(t, 9)
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(
			stabilityFixtureVotingDeadline+5_000,
			[]byte("tip"),
		),
	}

	ls.evaluateHardForkInitiationStability()

	awaitHFIEvalIdle(t, ls)
	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"no ratifiable proposal means transitionInfo stays Unknown")
}

// TestEvaluateHardForkInitiationStability_PreConwayPParams_NoOp pins the
// short-circuit when the chain is pre-Conway: no governance state
// machine exists, so the helper must not even attempt the DB lookup
// (and certainly must not promote transitionInfo).
func TestEvaluateHardForkInitiationStability_PreConwayPParams_NoOp(
	t *testing.T,
) {
	t.Parallel()

	ls, db := stabilityFixtureLedgerState(t, 9)
	// Even with a "ratifiable" proposal seeded, swapping pparams to nil
	// (or any non-Conway type) makes the helper short-circuit before
	// it hits the proposal store.
	seedRatifiableBootstrapHardForkInitiation(
		t, db, stabilityFixtureEpochID, 7,
	)
	ls.currentPParams = nil
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(
			stabilityFixtureVotingDeadline+5_000,
			[]byte("tip"),
		),
	}

	ls.evaluateHardForkInitiationStability()

	awaitHFIEvalIdle(t, ls)
	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"pre-Conway pparams must short-circuit without promotion")
}

// TestEvaluateHardForkInitiationStability_AlreadyKnownForSameEpoch_Idempotent
// pins the short-circuit when transitionInfo already reports the same
// upcoming boundary. The function must not redundantly mutate state
// (a redundant mutation is harmless to behaviour but makes per-block
// invocations noisier than necessary).
func TestEvaluateHardForkInitiationStability_AlreadyKnownForSameEpoch_Idempotent(
	t *testing.T,
) {
	t.Parallel()

	ls, db := stabilityFixtureLedgerState(t, 9)
	seedRatifiableBootstrapHardForkInitiation(
		t, db, stabilityFixtureEpochID, 7,
	)
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(
			stabilityFixtureVotingDeadline+5_000,
			[]byte("tip"),
		),
	}
	ls.transitionInfo = hardfork.NewTransitionKnown(stabilityFixtureEpochID + 1)

	ls.evaluateHardForkInitiationStability()

	assert.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State)
	assert.Equal(t, stabilityFixtureEpochID+1, ls.transitionInfo.KnownEpoch)
}

// TestEvaluateHardForkInitiationStability_PreservesKnownFromOtherSource
// pins the deference to higher-priority sources of TransitionKnown.
// Both evaluateTriggerAtEpoch (test override) and reconstructTransitionInfo
// (pparams-bump detection) may set Known for a specific epoch. The
// mid-epoch governance detector must not clobber that decision even if
// it would otherwise fire, so on-chain HFI ratifiability cannot
// override an operator-configured TestXHardForkAtEpoch boundary.
func TestEvaluateHardForkInitiationStability_PreservesKnownFromOtherSource(
	t *testing.T,
) {
	t.Parallel()

	ls, db := stabilityFixtureLedgerState(t, 9)
	seedRatifiableBootstrapHardForkInitiation(
		t, db, stabilityFixtureEpochID, 7,
	)
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(
			stabilityFixtureVotingDeadline+5_000,
			[]byte("tip"),
		),
	}
	const externalTargetEpoch = stabilityFixtureEpochID + 7
	ls.transitionInfo = hardfork.NewTransitionKnown(externalTargetEpoch)

	ls.evaluateHardForkInitiationStability()

	assert.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State)
	assert.Equal(
		t,
		uint64(externalTargetEpoch),
		ls.transitionInfo.KnownEpoch,
		"a Known target set elsewhere must not be overwritten by mid-epoch detection",
	)
}

// TestEvaluateHardForkInitiationStability_IntraEraHFI_DoesNotSetKnown
// pins the era-boundary gate: TransitionKnown signals an upcoming era
// transition, not just any pparams bump. A HardForkInitiation that
// proposes a new ProtocolVersion still inside the current era's
// version range (e.g. Plomin's pv9 → pv10, both Conway) is an
// intra-era bump and must not be surfaced as TransitionKnown — clients
// would otherwise see era-history responses claiming the era ends at
// epoch+1 when in fact the era continues.
//
// The check matches the era-filter the boundary path's
// IsHardForkTransition applies, so mid-epoch detection and the
// boundary's enactment dispatch agree on what counts as a transition.
func TestEvaluateHardForkInitiationStability_IntraEraHFI_DoesNotSetKnown(
	t *testing.T,
) {
	t.Parallel()

	ls, db := stabilityFixtureLedgerState(t, 9 /* Conway, bootstrap */)
	// Target major 10 — still in Conway (Conway covers pv9-pv10).
	// A ratifiable proposal here represents an intra-era pparams
	// bump, not an era transition.
	seedRatifiableBootstrapHardForkInitiation(
		t, db, stabilityFixtureEpochID, 10,
	)
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(
			stabilityFixtureVotingDeadline+5_000,
			[]byte("tip"),
		),
	}

	ls.evaluateHardForkInitiationStability()

	awaitHFIEvalIdle(t, ls)
	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"intra-era HardForkInitiation must not be surfaced as TransitionKnown")
}

// TestEvaluateHardForkInitiationStability_UpgradesImpossibleToKnown pins
// the priority order: TransitionKnown is strictly more informative than
// TransitionImpossible (the latter only says "no transition this epoch
// before safe-zone end", the former says "transition will happen at
// epoch+1"). When both could apply, Known wins.
func TestEvaluateHardForkInitiationStability_UpgradesImpossibleToKnown(
	t *testing.T,
) {
	t.Parallel()

	ls, db := stabilityFixtureLedgerState(t, 9)
	seedRatifiableBootstrapHardForkInitiation(
		t, db, stabilityFixtureEpochID, 7,
	)
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(
			stabilityFixtureVotingDeadline+5_000,
			[]byte("tip"),
		),
	}
	ls.transitionInfo = hardfork.NewTransitionImpossible()

	ls.evaluateHardForkInitiationStability()

	got := awaitTransitionInfo(t, ls, hardfork.TransitionKnown)
	assert.Equal(t, stabilityFixtureEpochID+1, got.KnownEpoch)
}

// Slot grid for the prune-floor fixture. The Shelley genesis used here
// (k=432, f=0.05) gives a stability window W of 3k/f = 25920 slots, so with a
// tip at pruneFixtureTipSlot the consumed-UTxO sweep prunes everything with
// deleted_slot <= pruneFixtureFloorSlot (140000-W).
//
// The at-tip rewind schedule then steps the ledger tip 140000 -> 114080 ->
// 88160. Attempt 2 asks for W/2 below the tip (127040) and findRewindPoint
// resolves that to the nearest committed block, 114080; attempt 3 asks for a
// full W below the *new* tip, 88160. That recomputation from the lowered tip
// is what carries the descent past the floor -- the per-attempt cap is a
// stability window, but the cumulative descent is not.
const (
	pruneFixtureStabilityWindow = 25_920
	pruneFixtureRootSlot        = 10_000
	pruneFixtureProducerSlot    = 50_000
	pruneFixtureDeepRewindSlot  = 88_160
	pruneFixtureConsumerSlot    = 110_000
	pruneFixtureFloorSlot       = 114_080
	pruneFixtureRetainedSlot    = 120_000
	pruneFixtureTipSlot         = 140_000
)

type prunedUtxoFixture struct {
	ls *LedgerState
	db *database.Database
	// prunedTxId is consumed at pruneFixtureConsumerSlot, at or below the
	// prune floor, so its row is hard-deleted by the consumed-UTxO sweep.
	prunedTxId []byte
	// retainedTxId is consumed above the prune floor, so its row survives the
	// sweep and rollback can still restore it. It is the control that keeps a
	// failure of the pruned probe from being read as a dead fixture.
	retainedTxId []byte
}

func newPrunedUtxoFixture(t *testing.T, mithrilLedgerSlot uint64) *prunedUtxoFixture {
	t.Helper()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 1000}),
	)

	type fixtureBlock struct {
		slot   uint64
		number uint64
		hash   []byte
	}
	blocks := []fixtureBlock{
		{pruneFixtureRootSlot, 1, testHashBytes("3766-root")},
		{pruneFixtureProducerSlot, 2, testHashBytes("3766-producer")},
		{pruneFixtureDeepRewindSlot, 3, testHashBytes("3766-deep")},
		{pruneFixtureConsumerSlot, 4, testHashBytes("3766-consumer")},
		{pruneFixtureFloorSlot, 5, testHashBytes("3766-floor")},
		{pruneFixtureTipSlot, 6, testHashBytes("3766-tip")},
	}
	rawBlocks := make([]chain.RawBlock, 0, len(blocks))
	for i, b := range blocks {
		var prevHash []byte
		if i > 0 {
			prevHash = blocks[i-1].hash
		}
		rawBlocks = append(rawBlocks, chain.RawBlock{
			Slot:        b.slot,
			Hash:        b.hash,
			BlockNumber: b.number,
			Type:        1,
			PrevHash:    prevHash,
			Cbor:        []byte{0x80},
		})
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(rawBlocks))

	ls, err := NewLedgerState(
		LedgerStateConfig{
			Database:          db,
			ChainManager:      cm,
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	)
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())

	// Every fixture block was applied, so each carries a recorded nonce.
	for _, b := range blocks {
		require.NoError(
			t,
			db.SetBlockNonce(b.hash, b.slot, []byte("nonce-3766"), false, nil),
		)
	}

	// One epoch covering the whole fixture grid, so the era reload that
	// follows every rollback keeps the ledger in Conway and the stability
	// window at 3k/f rather than falling back to the Byron default.
	require.NoError(t, db.SetEpoch(
		0,
		1,
		[]byte("nonce-3766-epoch"),
		[]byte("evolving-3766"),
		[]byte("candidate-3766"),
		[]byte("last-3766"),
		eras.ConwayEraDesc.Id,
		1,
		1_000_000,
		nil,
	))

	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(pruneFixtureTipSlot, blocks[len(blocks)-1].hash),
		BlockNumber: blocks[len(blocks)-1].number,
	}
	require.NoError(t, db.SetTip(tip, nil))
	ls.currentTip = tip
	ls.currentEra = eras.ConwayEraDesc
	ls.mithrilLedgerSlot = mithrilLedgerSlot
	ls.chainsyncState = SyncingChainsyncState
	ls.publishSnapshotsLocked()
	ls.syncUpstreamTipSlot.Store(pruneFixtureTipSlot)

	f := &prunedUtxoFixture{
		ls:           ls,
		db:           db,
		prunedTxId:   testHashBytes("3766-utxo-pruned"),
		retainedTxId: testHashBytes("3766-utxo-retained"),
	}
	seed := func(txId []byte, addedSlot, deletedSlot uint64) {
		mdTxn := db.MetadataTxn(true)
		require.NoError(t, mdTxn.Do(func(txn *database.Txn) error {
			return db.CreateUtxo(txn, &models.Utxo{
				TxId:        txId,
				OutputIdx:   0,
				AddedSlot:   addedSlot,
				DeletedSlot: deletedSlot,
				Amount:      types.Uint64(1_000_000),
			})
		}))
	}
	seed(f.prunedTxId, pruneFixtureProducerSlot, pruneFixtureConsumerSlot)
	seed(f.retainedTxId, pruneFixtureProducerSlot, pruneFixtureRetainedSlot)
	return f
}

// inLiveSet mirrors the probe used by the issue #3678 rollback tests: it asks
// the database.UtxoByRef lookup that LedgerView.UtxoById delegates to, so it
// exercises the deleted_slot filter that decides Conway bad-inputs and, through
// it, the consumed term of value conservation. A row seeded straight into
// metadata carries no blob CBOR, so ErrUtxoCborUnavailable counts as present;
// any error other than ErrUtxoNotFound is a lookup failure and fails the test.
func (f *prunedUtxoFixture) inLiveSet(t *testing.T, txId []byte) bool {
	t.Helper()
	var live bool
	txn := f.db.Transaction(false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		_, err := f.db.UtxoByRef(txId, 0, txn)
		switch {
		case err == nil, errors.Is(err, database.ErrUtxoCborUnavailable):
			live = true
			return nil
		case errors.Is(err, database.ErrUtxoNotFound):
			live = false
			return nil
		default:
			return err
		}
	}))
	return live
}

// driveAtTipRecovery runs the production at-tip recovery entry point the
// requested number of times against one persistent failure identity, which is
// what escalates the rewind schedule.
func (f *prunedUtxoFixture) driveAtTipRecovery(t *testing.T, rounds int) {
	t.Helper()
	validationErr := &txValidationError{
		BlockPoint: ocommon.NewPoint(
			pruneFixtureTipSlot+1,
			testHashBytes("3766-failing"),
		),
		TxHash: testHashBytes("3766-failing-tx"),
		Cause:  errors.New("bad input(s)"),
	}
	for i := range rounds {
		handled, err := f.ls.recoverAtTipFromTxValidationError(validationErr)
		require.NoError(t, err, "recovery round %d", i+1)
		require.True(t, handled, "recovery round %d", i+1)
	}
}

// assertLiveSetConsistentAtTip checks the invariant a rollback must preserve:
// at the ledger tip recovery settled on, every seeded output produced at or
// below that tip and consumed above it is resolvable, and every output already
// consumed at or below it is not. The second half is what keeps the fix from
// being "make lookups more permissive": refusing the rewind must not resurrect
// a genuinely spent output.
func (f *prunedUtxoFixture) assertLiveSetConsistentAtTip(t *testing.T) {
	t.Helper()
	tipSlot := f.ls.currentTip.Point.Slot
	for _, probe := range []struct {
		name        string
		txId        []byte
		addedSlot   uint64
		deletedSlot uint64
	}{
		{"pruned", f.prunedTxId, pruneFixtureProducerSlot, pruneFixtureConsumerSlot},
		{"retained", f.retainedTxId, pruneFixtureProducerSlot, pruneFixtureRetainedSlot},
	} {
		live := f.inLiveSet(t, probe.txId)
		switch {
		case probe.deletedSlot <= tipSlot:
			require.False(
				t,
				live,
				"%s output was consumed at slot %d, at or below ledger tip %d, and must not be in the live set",
				probe.name,
				probe.deletedSlot,
				tipSlot,
			)
		case probe.addedSlot <= tipSlot:
			require.True(
				t,
				live,
				"%s output produced at slot %d and consumed at slot %d must be in the live set at ledger tip %d",
				probe.name,
				probe.addedSlot,
				probe.deletedSlot,
				tipSlot,
			)
		}
	}
}

// TestAtTipRecoveryRewindBelowConsumedUtxoPruneFloor covers issue #3766.
//
// cleanupConsumedUtxos hard-deletes consumed UTxO rows whose deleted_slot is at
// or below tip-stabilityWindow. database.TruncateAfterSlot restores consumed
// UTxOs with an UPDATE (deleted_slot > slot), so a rollback below that prune
// floor cannot restore anything the sweep already removed -- and used to report
// the ledger repaired anyway. The at-tip recovery rewind schedule reaches such
// a target because each escalating attempt rewinds a further stability window
// below the *current* tip while the prune floor stays fixed at the highest tip
// the node reached.
//
// Blocks the node applied cleanly then become unapplyable: their inputs resolve
// to nothing, which Conway reports as bad inputs and, because value
// conservation sums consumed over only the inputs that resolve, as value not
// conserved with consumed 0 in the same pass.
//
// The recovery schedule here walks 140000 -> 114080 -> 88160, and 88160 is
// below the 114080 sweep floor.
func TestAtTipRecoveryRewindBelowConsumedUtxoPruneFloor(t *testing.T) {
	f := newPrunedUtxoFixture(t, 0)

	require.Equal(
		t,
		uint64(pruneFixtureStabilityWindow),
		f.ls.calculateStabilityWindow(),
		"fixture slot grid assumes a 3k/f stability window",
	)

	// Production consumed-UTxO sweep at the highest tip the node reached.
	f.ls.cleanupConsumedUtxos()
	require.False(
		t,
		f.inLiveSet(t, f.prunedTxId),
		"output consumed at or below the prune floor must be hard-deleted by the sweep",
	)
	floor, err := f.db.ConsumedUtxoPruneFloor(nil)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(pruneFixtureFloorSlot),
		floor,
		"the sweep must record how deep it removed spent rows",
	)

	f.driveAtTipRecovery(t, 3)

	// The live UTxO set must agree with the point recovery settled on.
	f.assertLiveSetConsistentAtTip(t)
	// ...which it can only do if the tip was never rewound below the point
	// from which the consumed-UTxO sweep can still restore state.
	require.GreaterOrEqual(
		t,
		f.ls.currentTip.Point.Slot,
		uint64(pruneFixtureFloorSlot),
		"recovery rewound the ledger below the consumed UTxO prune floor",
	)
	require.Positive(
		t,
		promtestutil.ToFloat64(f.ls.metrics.atTipRecoveryPruneFloorClamped),
		"the refused rewind must be visible to an operator",
	)
}

// TestAtTipRecoveryPruneFloorBindsAboveMithrilAnchor covers the Mithril-
// bootstrapped shape reported in issue #3766. The Mithril anchor sits far below
// the consumed-UTxO prune floor, so the existing trust-boundary check admits
// every target the rewind schedule produces while the sweep has already made
// them unrestorable. The prune floor is the binding constraint, and the node
// halts at the anchor only after the descent has destroyed the UTxO set on the
// way down.
func TestAtTipRecoveryPruneFloorBindsAboveMithrilAnchor(t *testing.T) {
	const mithrilAnchorSlot = pruneFixtureRootSlot
	f := newPrunedUtxoFixture(t, mithrilAnchorSlot)

	f.ls.cleanupConsumedUtxos()
	floor, err := f.db.ConsumedUtxoPruneFloor(nil)
	require.NoError(t, err)
	require.Greater(
		t,
		floor,
		uint64(mithrilAnchorSlot),
		"the fixture must place the prune floor above the Mithril anchor",
	)

	f.driveAtTipRecovery(t, 3)

	// The Mithril check alone would have allowed the descent: every target
	// the schedule produced is above the anchor.
	require.False(
		t,
		f.ls.recoveryRollbackExceedsMithrilBoundary(
			ocommon.NewPoint(pruneFixtureDeepRewindSlot, nil),
		),
		"the descent target is above the Mithril anchor, so only the prune floor can refuse it",
	)
	f.assertLiveSetConsistentAtTip(t)
	require.GreaterOrEqual(
		t,
		f.ls.currentTip.Point.Slot,
		uint64(pruneFixtureFloorSlot),
	)
}

// TestRollbackBelowConsumedUtxoPruneFloorIsRefused pins the backstop every
// rewind path funnels through. Callers other than at-tip recovery -- a peer
// rollback, the durable-tip-floor repair, replay recovery -- reach
// LedgerState.rollback directly, and it must refuse before mutating anything
// rather than move the tip and report a repair it cannot perform.
func TestRollbackBelowConsumedUtxoPruneFloorIsRefused(t *testing.T) {
	f := newPrunedUtxoFixture(t, 0)
	f.ls.cleanupConsumedUtxos()
	tipBefore := f.ls.currentTip

	err := f.ls.rollback(
		ocommon.NewPoint(
			pruneFixtureDeepRewindSlot,
			testHashBytes("3766-deep"),
		),
	)
	require.ErrorIs(t, err, ErrRollbackBelowUtxoPruneFloor)
	require.Equal(
		t,
		tipBefore.Point,
		f.ls.currentTip.Point,
		"a refused rollback must leave the ledger tip where it was",
	)

	// A target at or above the floor is still allowed: the floor refuses the
	// rewinds it cannot restore, not every rewind.
	require.NoError(
		t,
		f.ls.rollback(
			ocommon.NewPoint(
				pruneFixtureFloorSlot,
				testHashBytes("3766-floor"),
			),
		),
	)
	require.Equal(
		t,
		uint64(pruneFixtureFloorSlot),
		f.ls.currentTip.Point.Slot,
	)
	require.True(
		t,
		f.inLiveSet(t, f.retainedTxId),
		"an output consumed above the prune floor must still be restored by an allowed rollback",
	)
}

// TestRollbackIsAppliableRejectsBelowConsumedUtxoPruneFloor keeps the loop
// detector's crossability predicate in step with the rollback it predicts.
// Reporting a target below the prune floor as crossable would make the detector
// insist on applying a rollback rollbackChainAndStateDeferred refuses.
func TestRollbackIsAppliableRejectsBelowConsumedUtxoPruneFloor(t *testing.T) {
	f := newPrunedUtxoFixture(t, 0)
	f.ls.cleanupConsumedUtxos()

	require.True(
		t,
		f.ls.rollbackIsAppliable(
			ocommon.NewPoint(
				pruneFixtureFloorSlot,
				testHashBytes("3766-floor"),
			),
		),
		"a target at the prune floor is still crossable",
	)
	require.False(
		t,
		f.ls.rollbackIsAppliable(
			ocommon.NewPoint(
				pruneFixtureDeepRewindSlot,
				testHashBytes("3766-deep"),
			),
		),
		"a target below the prune floor cannot be crossed",
	)
}

// TestConsumedUtxoPruneFloorIsReadFromTheDatabase pins where the floor comes
// from. It is deliberately not mirrored in memory: a mirror is only refreshed
// after the sweep's transaction commits, so between commit and refresh it
// reports a lower floor than the database holds, and a rollback admitted on
// that stale value is exactly the divergence the floor exists to refuse.
func TestConsumedUtxoPruneFloorIsReadFromTheDatabase(t *testing.T) {
	f := newPrunedUtxoFixture(t, 0)

	floor, err := f.db.ConsumedUtxoPruneFloor(nil)
	require.NoError(t, err)
	require.Zero(t, floor, "nothing has been swept yet")

	f.ls.cleanupConsumedUtxos()

	floor, err = f.db.ConsumedUtxoPruneFloor(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(pruneFixtureFloorSlot), floor)

	// A floor written by another writer -- a prior run, or the sweep's own
	// transaction before any in-process cache could observe it -- is honored
	// immediately, with no reload step.
	require.NoError(t, f.db.SetSyncState(
		database.ConsumedUtxoPruneFloorSyncKey,
		strconv.FormatUint(pruneFixtureRetainedSlot, 10),
		nil,
	))
	below, seen, err := f.ls.rollbackBelowConsumedUtxoPruneFloor(
		ocommon.NewPoint(pruneFixtureFloorSlot, nil),
	)
	require.NoError(t, err)
	require.Equal(t, uint64(pruneFixtureRetainedSlot), seen)
	require.True(
		t,
		below,
		"the check must read the persisted floor, not a cached copy",
	)

	// A malformed value fails closed rather than reading as "nothing swept".
	require.NoError(t, f.db.SetSyncState(
		database.ConsumedUtxoPruneFloorSyncKey, "not-a-slot", nil,
	))
	_, _, err = f.ls.rollbackBelowConsumedUtxoPruneFloor(
		ocommon.NewPoint(pruneFixtureFloorSlot, nil),
	)
	require.Error(t, err)
	require.ErrorIs(
		t,
		f.ls.rollback(
			ocommon.NewPoint(
				pruneFixtureDeepRewindSlot,
				testHashBytes("3766-deep"),
			),
		),
		strconv.ErrSyntax,
		"an unreadable floor must refuse the rollback",
	)
}

// TestRollbackChainAndStateRefusesRedirectBelowPruneFloor covers the ordering
// hazard between the same-slot competitor redirect (issue #3678) and the prune
// floor. rollbackChainAndStateDeferred truncates the primary chain and only then
// synchronizes the ledger. A target sitting exactly on the floor whose hash
// differs from the applied tip resolves to an applied ancestor strictly below
// the floor, so checking the unresolved point would admit it here and refuse it
// only after chain.Rollback had already run, splitting the chain from the
// ledger.
func TestRollbackChainAndStateRefusesRedirectBelowPruneFloor(t *testing.T) {
	f := newPrunedUtxoFixture(t, 0)
	f.ls.cleanupConsumedUtxos()

	// Put the applied ledger tip on a same-slot competitor at the floor, with
	// a recorded nonce so the redirect treats it as genuinely applied.
	competitorHash := testHashBytes("3766-floor-competitor")
	require.NoError(t, f.db.SetBlockNonce(
		competitorHash,
		pruneFixtureFloorSlot,
		[]byte("nonce-3766-competitor"),
		false,
		nil,
	))
	competitorTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(pruneFixtureFloorSlot, competitorHash),
		BlockNumber: 5,
	}
	require.NoError(t, f.db.SetTip(competitorTip, nil))
	f.ls.currentTip = competitorTip
	f.ls.publishSnapshotsLocked()

	chainTipBefore := f.ls.chain.Tip().Point

	// The target's slot equals the floor, so an unresolved check passes; the
	// redirect resolves it below the floor.
	target := ocommon.NewPoint(
		pruneFixtureFloorSlot,
		testHashBytes("3766-floor"),
	)
	resolved, err := f.ls.resolveRollbackTarget(target, competitorTip)
	require.NoError(t, err)
	require.Less(
		t,
		resolved.Slot,
		uint64(pruneFixtureFloorSlot),
		"fixture must produce a redirect below the floor",
	)

	require.ErrorIs(
		t,
		f.ls.rollbackChainAndStateDeferred(target, nil),
		ErrRollbackBelowUtxoPruneFloor,
	)
	require.Equal(
		t,
		chainTipBefore,
		f.ls.chain.Tip().Point,
		"the primary chain must not be truncated for a refused rollback",
	)
	require.Equal(
		t,
		competitorTip.Point,
		f.ls.currentTip.Point,
		"the ledger tip must not move for a refused rollback",
	)
}

// TestHandleEventChainsyncRollbackRejectsBelowPruneFloor pins that a peer
// rollback the prune floor refuses is handled as peer divergence, not as a
// local fault. handleEventChainsync routes any error returned by the rollback
// handler to FatalErrorFunc, so returning one here would let a peer's choice of
// rollback point terminate the node. The handler must instead reject the peer
// chain and ask for a fresh intersection, exactly as the Mithril boundary does.
func TestHandleEventChainsyncRollbackRejectsBelowPruneFloor(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus

	// A floor above the rollback target, as a sweep at a higher tip would
	// have left behind.
	require.NoError(t, fixture.ls.db.SetSyncState(
		database.ConsumedUtxoPruneFloorSyncKey,
		strconv.FormatUint(fixture.currentTip.Point.Slot, 10),
		nil,
	))

	fatalCalls := 0
	fixture.ls.config.FatalErrorFunc = func(error) { fatalCalls++ }

	resyncCh := make(chan event.ChainsyncResyncEvent, 1)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			select {
			case resyncCh <- e:
			default:
			}
		},
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, subId)
	})

	err := fixture.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        fixture.ancestorTip.Point,
		},
		nil,
	)
	require.NoError(
		t,
		err,
		"a refused rollback must not surface as an error the fatal path acts on",
	)
	require.Zero(t, fatalCalls)

	// Neither side moved: the chain was not truncated and the ledger tip
	// stands.
	require.Equal(t, fixture.currentTip, fixture.ls.chain.Tip())
	require.Equal(t, fixture.currentTip, fixture.ls.currentTip)

	e := testutil.RequireReceive(
		t,
		resyncCh,
		time.Second,
		"expected prune-floor rollback resync event",
	)
	require.Equal(
		t,
		event.ChainsyncResyncReasonRollbackBelowUtxoPruneFloor,
		e.Reason,
	)
	require.Equal(t, fixture.connId, e.ConnectionId)
}

// TestUtxoPruningDeferredForCatchup pins both of utxoPruningDeferredForCatchup's
// defer conditions, plus the two cases that must NOT defer: it is the one
// change here that widens what Acquire accepts, so a mirror that drifts
// from cleanupConsumedUtxos' own two defer conditions fails open rather
// than closed. checkUtxoRetentionWindow calls this to decide whether to
// accept a point below the ordinary stability-window floor, so a false
// positive here (deferring when it shouldn't) would let Acquire accept a
// point cleanupConsumedUtxos might have already pruned.
func TestUtxoPruningDeferredForCatchup(t *testing.T) {
	t.Parallel()

	t.Run("no upstream tracked at all: not deferred", func(t *testing.T) {
		t.Parallel()
		ls := &LedgerState{}
		require.False(t, ls.utxoPruningDeferredForCatchup(1000, 50))
	})

	t.Run("active upstream, target not yet known: deferred", func(t *testing.T) {
		t.Parallel()
		connA := testChainsyncConnId(6101, 3291)
		ls := &LedgerState{
			config: LedgerStateConfig{
				GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
					return &connA
				},
			},
		}
		// UpstreamSyncStatus is (0, true) here: a live active connection
		// with no admitted target yet -- "still syncing," per that
		// function's own doc comment.
		require.True(t, ls.utxoPruningDeferredForCatchup(1000, 50))
	})

	t.Run("active upstream, known target, far behind: deferred", func(t *testing.T) {
		t.Parallel()
		connA := testChainsyncConnId(6102, 3292)
		ls := &LedgerState{
			config: LedgerStateConfig{
				GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
					return &connA
				},
			},
		}
		ls.publishActiveUpstream(connA)
		ls.publishAdmittedUpstreamTarget(ChainsyncEvent{
			ConnectionId:      connA,
			SyncTarget:        ochainsync.Tip{Point: ocommon.NewPoint(1000, nil)},
			SyncTargetTrusted: true,
		})
		require.Equal(t, uint64(1000), ls.UpstreamTipSlot())
		// tipSlot 100 is 900 slots behind upstream's 1000, well outside a
		// 50-slot stability window.
		require.True(t, ls.utxoPruningDeferredForCatchup(100, 50))
	})

	t.Run("active upstream, known target, caught up: not deferred", func(t *testing.T) {
		t.Parallel()
		connA := testChainsyncConnId(6103, 3293)
		ls := &LedgerState{
			config: LedgerStateConfig{
				GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
					return &connA
				},
			},
		}
		ls.publishActiveUpstream(connA)
		ls.publishAdmittedUpstreamTarget(ChainsyncEvent{
			ConnectionId:      connA,
			SyncTarget:        ochainsync.Tip{Point: ocommon.NewPoint(1000, nil)},
			SyncTargetTrusted: true,
		})
		require.Equal(t, uint64(1000), ls.UpstreamTipSlot())
		// tipSlot 980 is within a 50-slot stability window of upstream's
		// 1000 -- caught up, pruning must proceed normally.
		require.False(t, ls.utxoPruningDeferredForCatchup(980, 50))
	})
}

// TestUtxoStorageAndRetrieval tests that UTxOs from regular blocks are stored
// and retrieved correctly using the offset-based storage system.
func TestUtxoStorageAndRetrieval(t *testing.T) {
	t.Parallel()

	// Create temp directory for database
	tmpDir, err := os.MkdirTemp("", "utxo_storage_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	logger := slog.New(
		slog.NewTextHandler(
			os.Stdout,
			&slog.HandlerOptions{Level: slog.LevelDebug},
		),
	)

	// Create database
	dbConfig := &database.Config{
		DataDir: tmpDir,
		Logger:  logger,
	}
	db, err := dbtest.NewDatabase(t, dbConfig)
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	// Load blocks from immutable testdata
	imm, err := immutable.New("../database/immutable/testdata")
	require.NoError(t, err)

	// Start from genesis and process a few blocks
	iter, err := imm.BlocksFromPoint(ocommon.Point{Slot: 0, Hash: nil})
	require.NoError(t, err)
	defer iter.Close()

	var storedUtxos []struct {
		txId      []byte
		outputIdx uint32
		slot      uint64
	}

	blocksProcessed := 0
	maxBlocks := 10 // Process a few blocks to find some UTxOs

	for blocksProcessed < maxBlocks {
		immBlock, err := iter.Next()
		if err != nil {
			// io.EOF or equivalent signals end of iteration
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
				break
			}
			t.Fatalf("unexpected iterator error: %v", err)
		}
		if immBlock == nil {
			break
		}

		// Decode block using gouroboros
		block, err := ledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		require.NoError(t, err)

		point := ocommon.Point{
			Slot: block.SlotNumber(),
			Hash: block.Hash().Bytes(),
		}

		t.Logf("Processing block at slot %d with %d transactions",
			point.Slot, len(block.Transactions()))

		// Skip blocks with no transactions
		if len(block.Transactions()) == 0 {
			blocksProcessed++
			continue
		}

		// First, store the block
		txn := db.Transaction(true)
		err = txn.Do(func(txn *database.Txn) error {
			// Store block CBOR
			blockRecord := models.Block{
				Slot:     point.Slot,
				Hash:     point.Hash,
				Number:   block.BlockNumber(),
				Type:     uint(block.Type()),
				PrevHash: block.PrevHash().Bytes(),
				Cbor:     block.Cbor(),
			}
			if err := db.BlockCreate(blockRecord, txn); err != nil {
				return err
			}

			// Compute offsets - offsets MUST be available
			indexer := database.NewBlockIndexer(point.Slot, point.Hash)
			offsets, err := indexer.ComputeOffsets(block.Cbor(), block)
			if err != nil {
				return fmt.Errorf(
					"compute offsets for block %d: %w",
					point.Slot,
					err,
				)
			}

			// Process each transaction
			for txIdx, tx := range block.Transactions() {
				txHash := tx.Hash()
				var txHashArray [32]byte
				copy(txHashArray[:], txHash.Bytes())

				t.Logf("  TX %d: %s with %d outputs",
					txIdx, txHash.String(), len(tx.Outputs()))

				// Verify offsets exist for this transaction
				if txOff, ok := offsets.TxOffsets[txHashArray]; ok {
					t.Logf("    TX offset: slot=%d, offset=%d, length=%d",
						txOff.BlockSlot, txOff.ByteOffset, txOff.ByteLength)
				} else {
					return fmt.Errorf("TX offset not found for %s", txHash.String())
				}

				// Store the transaction - offsets MUST be available
				err := db.SetTransaction(
					tx,
					point,
					uint32(txIdx),
					0,
					nil,
					nil,
					&database.BlockIngestionResult{
						TxOffsets:   offsets.TxOffsets,
						UtxoOffsets: offsets.UtxoOffsets,
					},
					txn,
				)
				if err != nil {
					return err
				}

				// Track outputs for later verification
				for _, utxo := range tx.Produced() {
					txId := utxo.Id.Id().Bytes()
					outputIdx := utxo.Id.Index()

					// Verify offset was computed
					ref := database.UtxoRef{
						TxId:      txHashArray,
						OutputIdx: outputIdx,
					}
					if utxoOff, ok := offsets.UtxoOffsets[ref]; ok {
						t.Logf(
							"    Output %d offset: slot=%d, offset=%d, length=%d",
							outputIdx,
							utxoOff.BlockSlot,
							utxoOff.ByteOffset,
							utxoOff.ByteLength,
						)
					} else {
						return fmt.Errorf("output %d offset not found", outputIdx)
					}

					storedUtxos = append(storedUtxos, struct {
						txId      []byte
						outputIdx uint32
						slot      uint64
					}{
						txId:      txId,
						outputIdx: outputIdx,
						slot:      point.Slot,
					})
				}
			}

			return nil
		})
		require.NoError(t, err)

		blocksProcessed++
	}

	t.Logf(
		"\n=== Stored %d UTxOs from %d blocks ===\n",
		len(storedUtxos),
		blocksProcessed,
	)

	// Now try to retrieve each stored UTxO
	var retrievalErrors int
	var metadataErrors int
	var blobErrors int
	var successCount int

	for _, utxoRef := range storedUtxos {
		txn := db.Transaction(false)

		// Step 1: Check if metadata exists
		metaTxn := txn.Metadata()
		utxoMeta, err := db.Metadata().
			GetUtxo(utxoRef.txId, utxoRef.outputIdx, metaTxn)
		if err != nil {
			t.Logf("Metadata error for %s#%d: %v",
				hex.EncodeToString(utxoRef.txId[:8]), utxoRef.outputIdx, err)
			metadataErrors++
			txn.Release()
			continue
		}
		if utxoMeta == nil {
			t.Logf(
				"Metadata MISSING for %s#%d (slot %d)",
				hex.EncodeToString(
					utxoRef.txId[:8],
				),
				utxoRef.outputIdx,
				utxoRef.slot,
			)
			metadataErrors++
			txn.Release()
			continue
		}

		// Step 2: Check if blob data exists
		blob := db.Blob()
		blobTxn := txn.Blob()
		// db.Blob() is non-nil: database.New rejects a nil or typed-nil blob
		// store (database/database.go), so the nil-receiver branch of
		// blobStoreRef.blobStore that nilaway traces is unreachable for any
		// constructed database.
		//nolint:nilaway // database.New requires a non-nil blob store
		blobData, err := blob.GetUtxo(blobTxn, utxoRef.txId, utxoRef.outputIdx)
		if err != nil {
			t.Logf("Blob error for %s#%d: %v",
				hex.EncodeToString(utxoRef.txId[:8]), utxoRef.outputIdx, err)
			blobErrors++
			txn.Release()
			continue
		}

		// Step 3: Check blob data type
		if database.IsUtxoOffsetStorage(blobData) {
			// Decode offset
			offset, err := database.DecodeUtxoOffset(blobData)
			if err != nil {
				t.Logf(
					"Offset decode error for %s#%d: %v",
					hex.EncodeToString(
						utxoRef.txId[:8],
					),
					utxoRef.outputIdx,
					err,
				)
				blobErrors++
				txn.Release()
				continue
			}

			// Try to get block CBOR
			blockCbor, _, err := blob.GetBlock(
				blobTxn,
				offset.BlockSlot,
				offset.BlockHash[:],
			)
			if err != nil {
				t.Logf(
					"Block retrieval error for %s#%d: slot=%d, hash=%x, err=%v",
					hex.EncodeToString(utxoRef.txId[:8]),
					utxoRef.outputIdx,
					offset.BlockSlot,
					offset.BlockHash[:8],
					err,
				)
				blobErrors++
				txn.Release()
				continue
			}

			// Extract UTxO CBOR
			end := uint64(offset.ByteOffset) + uint64(offset.ByteLength)
			if end > uint64(len(blockCbor)) {
				t.Logf(
					"Offset out of bounds for %s#%d: offset=%d, length=%d, block_size=%d",
					hex.EncodeToString(utxoRef.txId[:8]),
					utxoRef.outputIdx,
					offset.ByteOffset,
					offset.ByteLength,
					len(blockCbor),
				)
				blobErrors++
				txn.Release()
				continue
			}

			// Success!
			successCount++
		} else {
			// Raw CBOR storage
			if len(blobData) > 0 {
				successCount++
			} else {
				t.Logf("Empty blob data for %s#%d",
					hex.EncodeToString(utxoRef.txId[:8]), utxoRef.outputIdx)
				blobErrors++
			}
		}

		txn.Release()
	}

	retrievalErrors = metadataErrors + blobErrors

	t.Logf("\n=== RESULTS ===")
	t.Logf("Total UTxOs: %d", len(storedUtxos))
	t.Logf("Successful retrievals: %d", successCount)
	t.Logf("Metadata errors: %d", metadataErrors)
	t.Logf("Blob errors: %d", blobErrors)
	t.Logf("Total retrieval errors: %d", retrievalErrors)

	// Require all UTxOs to be retrievable
	require.Equal(t, 0, retrievalErrors, "Some UTxOs could not be retrieved")
	require.Equal(
		t,
		len(storedUtxos),
		successCount,
		"Not all UTxOs were retrieved successfully",
	)
}

// newUtxoStorageTestDB creates a temp-directory database for tests that
// load real blocks from the immutable testdata fixture.
func newUtxoStorageTestDB(t *testing.T) *database.Database {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "utxo_storage_test")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: tmpDir,
		Logger: slog.New(
			slog.NewTextHandler(
				io.Discard,
				&slog.HandlerOptions{Level: slog.LevelDebug},
			),
		),
	})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })
	return db
}

// newUtxoStorageTestIterator opens the shared immutable testdata fixture
// and returns an iterator positioned at genesis.
func newUtxoStorageTestIterator(t *testing.T) *immutable.BlockIterator {
	t.Helper()
	imm, err := immutable.New("../database/immutable/testdata")
	require.NoError(t, err)
	iter, err := imm.BlocksFromPoint(ocommon.Point{Slot: 0, Hash: nil})
	require.NoError(t, err)
	t.Cleanup(func() { iter.Close() })
	return iter
}

// errNextProducingBlockValidated is returned from the scratch transaction
// nextProducingBlock uses to check storability, forcing a rollback so the
// caller's own (real) store starts from a clean slate regardless of
// whether that check succeeded or failed.
var errNextProducingBlockValidated = errors.New(
	"nextProducingBlock: storability validated, rolling back",
)

// nextProducingBlock advances iter to the next block whose first
// transaction produces at least one UTxO and can actually be stored with
// this package's minimal SetTransaction call (nil pparamUpdates and
// certDeposits) — some fixture transactions carry a deposit-bearing
// certificate that needs certDeposits this helper doesn't supply, and
// those are skipped too, in a scratch transaction that is always rolled
// back. It decodes the block and returns it along with its raw CBOR. It
// skips the test if the fixture is exhausted before finding one, so
// callers never need to separately handle a non-producing or un-storable
// first transaction.
func nextProducingBlock(
	t *testing.T,
	db *database.Database,
	iter *immutable.BlockIterator,
) (lcommon.Block, []byte) {
	t.Helper()
	for {
		immBlock, err := iter.Next()
		require.NoError(t, err)
		if immBlock == nil {
			t.Skip(
				"no storable block with a producing first transaction found in testdata",
			)
		}

		block, err := ledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		require.NoError(t, err)

		if len(block.Transactions()) == 0 ||
			len(block.Transactions()[0].Produced()) == 0 {
			continue
		}

		txn := db.Transaction(true)
		err = txn.Do(func(txn *database.Txn) error {
			if _, err := tryStoreBlockFirstTx(db, txn, block, immBlock.Cbor); err != nil {
				return err
			}
			return errNextProducingBlockValidated
		})
		if !errors.Is(err, errNextProducingBlockValidated) {
			// A real storage error (e.g. a deposit-bearing certificate
			// needing certDeposits this helper doesn't supply); try the
			// next producing block instead.
			continue
		}
		return block, immBlock.Cbor
	}
}

// storeBlockFirstTx stores block (and its raw CBOR) plus its first
// transaction into db within txn, computing the offsets SetTransaction
// requires, and returns the stored transaction.
func storeBlockFirstTx(
	t *testing.T,
	db *database.Database,
	txn *database.Txn,
	block lcommon.Block,
	blockCbor []byte,
) lcommon.Transaction {
	t.Helper()
	tx, err := tryStoreBlockFirstTx(db, txn, block, blockCbor)
	require.NoError(t, err)
	return tx
}

// tryStoreBlockFirstTx is the non-asserting form of storeBlockFirstTx, for
// callers that want to skip a block that fails to store (e.g. one whose
// first transaction carries a deposit-bearing certificate, which needs
// certDeposits this minimal helper doesn't supply) rather than failing the
// test outright.
func tryStoreBlockFirstTx(
	db *database.Database,
	txn *database.Txn,
	block lcommon.Block,
	blockCbor []byte,
) (lcommon.Transaction, error) {
	point := ocommon.Point{
		Slot: block.SlotNumber(),
		Hash: block.Hash().Bytes(),
	}
	blockRecord := models.Block{
		Slot:     point.Slot,
		Hash:     point.Hash,
		Number:   block.BlockNumber(),
		Type:     uint(block.Type()),
		PrevHash: block.PrevHash().Bytes(),
		Cbor:     blockCbor,
	}
	if err := db.BlockCreate(blockRecord, txn); err != nil {
		return nil, err
	}

	indexer := database.NewBlockIndexer(point.Slot, point.Hash)
	offsets, err := indexer.ComputeOffsets(blockCbor, block)
	if err != nil {
		return nil, err
	}

	txs := block.Transactions()
	if len(txs) == 0 {
		return nil, errors.New("block has no transactions")
	}
	tx := txs[0]
	if err := db.SetTransaction(
		tx,
		point,
		0,
		0,
		nil,
		nil,
		&database.BlockIngestionResult{
			TxOffsets:   offsets.TxOffsets,
			UtxoOffsets: offsets.UtxoOffsets,
		},
		txn,
	); err != nil {
		return nil, err
	}
	return tx, nil
}

// TestUtxoByRefAfterSetTransaction verifies that UtxoByRef works immediately
// after SetTransaction within the same transaction.
func TestUtxoByRefAfterSetTransaction(t *testing.T) {
	t.Parallel()

	db := newUtxoStorageTestDB(t)
	iter := newUtxoStorageTestIterator(t)
	block, blockCbor := nextProducingBlock(t, db, iter)

	// Store block and verify UTxO retrieval in same transaction
	txn := db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		tx := storeBlockFirstTx(t, db, txn, block, blockCbor)

		// Try to retrieve UTxOs immediately (within same transaction)
		for _, utxo := range tx.Produced() {
			txId := utxo.Id.Id().Bytes()
			outputIdx := utxo.Id.Index()

			t.Logf("Attempting to retrieve %s#%d within same transaction...",
				hex.EncodeToString(txId[:8]), outputIdx)

			retrieved, err := db.UtxoByRef(txId, outputIdx, txn)
			if err != nil {
				t.Errorf("Failed to retrieve %s#%d: %v",
					hex.EncodeToString(txId[:8]), outputIdx, err)
				continue
			}

			t.Logf("Successfully retrieved %s#%d: CBOR len=%d",
				hex.EncodeToString(txId[:8]), outputIdx, len(retrieved.Cbor))
		}

		return nil
	})
	require.NoError(t, err)
}

// TestUtxosByRefsAfterSetTransaction verifies the batched UTxO lookup
// returns every produced UTxO for a transaction in one call, exactly once
// even when a ref is requested more than once, and silently omits a ref
// that doesn't correspond to any live UTxO rather than erroring the whole
// batch (see #392).
func TestUtxosByRefsAfterSetTransaction(t *testing.T) {
	t.Parallel()

	db := newUtxoStorageTestDB(t)
	iter := newUtxoStorageTestIterator(t)
	block, blockCbor := nextProducingBlock(t, db, iter)

	txn := db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		tx := storeBlockFirstTx(t, db, txn, block, blockCbor)
		produced := tx.Produced()

		refs := make([]models.UtxoId, 0, len(produced)+2)
		for _, utxo := range produced {
			refs = append(refs, models.UtxoId{
				Hash: utxo.Id.Id().Bytes(),
				Idx:  utxo.Id.Index(),
			})
		}
		// Requesting the first produced UTxO's ref a second time must not
		// duplicate it in the result.
		refs = append(refs, refs[0])
		// A ref with no matching live UTxO should be silently omitted,
		// not cause the whole batch to fail.
		bogusHash := make([]byte, 32)
		refs = append(refs, models.UtxoId{Hash: bogusHash, Idx: 9999})

		results, err := db.UtxosByRefs(refs, txn)
		if err != nil {
			return err
		}
		require.Len(
			t,
			results,
			len(produced),
			"duplicate ref should not duplicate its result row",
		)

		byRef := make(map[string]models.Utxo, len(results))
		for _, utxo := range results {
			key := hex.EncodeToString(utxo.TxId) + ":" +
				fmt.Sprint(utxo.OutputIdx)
			byRef[key] = utxo
		}
		for _, utxo := range produced {
			txId := utxo.Id.Id().Bytes()
			key := hex.EncodeToString(txId) + ":" +
				fmt.Sprint(utxo.Id.Index())
			got, ok := byRef[key]
			require.True(t, ok, "missing UTxO %s", key)
			require.NotEmpty(t, got.Cbor)
		}

		return nil
	})
	require.NoError(t, err)
}

func TestUtxoByRefRecoversMissingBlobFromProducerBlock(t *testing.T) {
	t.Parallel()

	for _, deleteTxBlob := range []bool{false, true} {
		t.Run(
			fmt.Sprintf("delete_tx_blob=%t", deleteTxBlob),
			func(t *testing.T) {
				tmpDir, err := os.MkdirTemp("", "utxo_byref_recover_test")
				require.NoError(t, err)
				defer os.RemoveAll(tmpDir)

				logger := slog.New(
					slog.NewTextHandler(
						io.Discard,
						&slog.HandlerOptions{Level: slog.LevelDebug},
					),
				)

				dbConfig := &database.Config{
					DataDir: tmpDir,
					Logger:  logger,
				}
				db, err := dbtest.NewDatabase(t, dbConfig)
				require.NoError(t, err)
				defer dbtest.CloseDatabase(db)

				imm, err := immutable.New("../database/immutable/testdata")
				require.NoError(t, err)

				iter, err := imm.BlocksFromPoint(
					ocommon.Point{Slot: 0, Hash: nil},
				)
				require.NoError(t, err)
				defer iter.Close()

				var block lcommon.Block
				var blockCbor []byte
				for {
					immBlock, err := iter.Next()
					require.NoError(t, err)
					if immBlock == nil {
						t.Fatal("no blocks with transactions found")
					}
					block, err = ledger.NewBlockFromCbor(
						immBlock.Type,
						immBlock.Cbor,
					)
					require.NoError(t, err)
					if len(block.Transactions()) == 0 {
						continue
					}
					if len(block.Transactions()[0].Produced()) == 0 {
						continue
					}
					blockCbor = immBlock.Cbor
					break
				}

				point := ocommon.Point{
					Slot: block.SlotNumber(),
					Hash: block.Hash().Bytes(),
				}
				tx := block.Transactions()[0]
				expectedUtxo := tx.Produced()[0]
				txId := tx.Hash().Bytes()
				outputIdx := expectedUtxo.Id.Index()

				txn := db.Transaction(true)
				err = txn.Do(func(txn *database.Txn) error {
					blockRecord := models.Block{
						Slot:     point.Slot,
						Hash:     point.Hash,
						Number:   block.BlockNumber(),
						Type:     uint(block.Type()),
						PrevHash: block.PrevHash().Bytes(),
						Cbor:     blockCbor,
					}
					if err := db.BlockCreate(blockRecord, txn); err != nil {
						return err
					}
					indexer := database.NewBlockIndexer(point.Slot, point.Hash)
					offsets, err := indexer.ComputeOffsets(blockCbor, block)
					if err != nil {
						return fmt.Errorf("compute offsets: %w", err)
					}
					return db.SetTransaction(
						tx,
						point,
						0,
						0,
						nil,
						nil,
						&database.BlockIngestionResult{
							TxOffsets:   offsets.TxOffsets,
							UtxoOffsets: offsets.UtxoOffsets,
						},
						txn,
					)
				})
				require.NoError(t, err)

				deleteTxn := db.Transaction(true)
				err = deleteTxn.Do(func(txn *database.Txn) error {
					if err := db.Blob().DeleteUtxo(txn.Blob(), txId, outputIdx); err != nil {
						return err
					}
					if deleteTxBlob {
						if err := db.Blob().DeleteTx(txn.Blob(), txId); err != nil {
							return err
						}
					}
					return nil
				})
				require.NoError(t, err)

				metaUtxo, err := db.Metadata().GetUtxo(txId, outputIdx, nil)
				require.NoError(t, err)
				require.NotNil(t, metaUtxo)

				lookupTxn := db.Transaction(true)
				err = lookupTxn.Do(func(txn *database.Txn) error {
					retrieved, err := db.UtxoByRef(txId, outputIdx, txn)
					require.NoError(t, err)
					require.Equal(t, expectedUtxo.Output.Cbor(), retrieved.Cbor)

					// The recovery path should heal the missing blob so future
					// lookups do not need to re-derive it from the producer block.
					// Verify by checking the blob key exists (the stored value is a
					// DOFF offset reference, not raw CBOR).
					_, err = db.Blob().GetUtxo(txn.Blob(), txId, outputIdx)
					require.NoError(t, err)

					// A second UtxoByRef should succeed without recovery.
					retrieved2, err := db.UtxoByRef(txId, outputIdx, txn)
					require.NoError(t, err)
					require.Equal(
						t,
						expectedUtxo.Output.Cbor(),
						retrieved2.Cbor,
					)
					return nil
				})
				require.NoError(t, err)
			},
		)
	}
}

func TestValidationReferenceSlotPrefersCurrentSlotWhenAhead(t *testing.T) {
	t.Parallel()

	got := validationReferenceSlot(100, 125, nil)
	if got != 125 {
		t.Fatalf("expected current slot 125, got %d", got)
	}
}

func TestValidationReferenceSlotKeepsCurrentWhenEqual(t *testing.T) {
	t.Parallel()

	got := validationReferenceSlot(125, 125, nil)
	if got != 125 {
		t.Fatalf("expected shared slot 125, got %d", got)
	}
}

func TestValidationReferenceSlotFallsBackToTipOnError(t *testing.T) {
	t.Parallel()

	got := validationReferenceSlot(100, 125, errors.New("clock unavailable"))
	if got != 100 {
		t.Fatalf("expected tip slot 100 on error, got %d", got)
	}
}

func TestValidationReferenceSlotKeepsTipWhenAhead(t *testing.T) {
	t.Parallel()

	got := validationReferenceSlot(125, 100, nil)
	if got != 125 {
		t.Fatalf("expected tip slot 125, got %d", got)
	}
}

func TestHistoricalBlockValidationSkipsMithrilCoveredBlocks(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name              string
		validationEnabled bool
		trustedReplay     bool
		chainsyncState    ChainsyncState
		blockSlot         uint64
		cutoffSlot        uint64
		mithrilLedgerSlot uint64
		shouldValidate    bool
		reachedTipRegion  bool
	}{
		{
			name:              "historical validation inside Mithril boundary",
			validationEnabled: true,
			chainsyncState:    SyncingChainsyncState,
			blockSlot:         100,
			cutoffSlot:        50,
			mithrilLedgerSlot: 100,
			reachedTipRegion:  true,
		},
		{
			name:              "historical validation outside Mithril boundary",
			validationEnabled: true,
			chainsyncState:    SyncingChainsyncState,
			blockSlot:         101,
			cutoffSlot:        50,
			mithrilLedgerSlot: 100,
			shouldValidate:    true,
			reachedTipRegion:  true,
		},
		{
			name:              "tip window inside Mithril boundary",
			chainsyncState:    SyncingChainsyncState,
			blockSlot:         100,
			cutoffSlot:        50,
			mithrilLedgerSlot: 100,
			reachedTipRegion:  true,
		},
		{
			name:              "tip window outside Mithril boundary",
			chainsyncState:    SyncingChainsyncState,
			blockSlot:         101,
			cutoffSlot:        50,
			mithrilLedgerSlot: 100,
			shouldValidate:    true,
			reachedTipRegion:  true,
		},
		{
			name:           "trusted replay",
			trustedReplay:  true,
			chainsyncState: SyncingChainsyncState,
			blockSlot:      100,
			cutoffSlot:     50,
		},
		{
			name:           "before tip window",
			chainsyncState: SyncingChainsyncState,
			blockSlot:      49,
			cutoffSlot:     50,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			shouldValidate, reachedTipRegion := historicalBlockValidationDecision(
				testCase.validationEnabled,
				testCase.trustedReplay,
				testCase.chainsyncState,
				testCase.blockSlot,
				testCase.cutoffSlot,
				testCase.mithrilLedgerSlot,
			)
			require.Equal(t, testCase.shouldValidate, shouldValidate)
			require.Equal(t, testCase.reachedTipRegion, reachedTipRegion)
		})
	}
}

// TestValidateChainSelectionHeaderCryptoAcceptsVerifiedHeader proves that a
// header whose crypto is valid and whose leader eligibility can already be
// checked against local ledger state passes with no error -- the baseline
// "fully verified" case chain selection must count toward Genesis density.
func TestValidateChainSelectionHeaderCryptoAcceptsVerifiedHeader(
	t *testing.T,
) {
	t.Parallel()

	tb := createTestBlock(t, [32]byte{70}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	seedBlockPoolRegistration(t, db, tb.block)
	poolKeyHash := tb.block.IssuerVkey().Hash()
	// Pool owns 100% of stake, matching createTestBlock's threshold
	// assumption, at the epoch-5 block's "mark" snapshot (epoch 4).
	seedPoolStakeSnapshot(t, db, 4, poolKeyHash[:], 1_000_000_000)
	ls.publishSnapshotsLocked()

	err := ls.ValidateChainSelectionHeaderCrypto(tb.block.Header())
	require.NoError(
		t,
		err,
		"a header with valid crypto and confirmed leader eligibility must verify",
	)
}

// TestValidateChainSelectionHeaderCryptoRejectsTamperedProof proves that a
// header with an internally-invalid VRF proof is a definite (non-deferred)
// failure, even while local ledger state has not caught up to the header's
// slot. An invalid header must never be counted toward Genesis density
// regardless of local sync state (dingo #3517).
func TestValidateChainSelectionHeaderCryptoRejectsTamperedProof(
	t *testing.T,
) {
	t.Parallel()

	tb := createTestBlock(t, [32]byte{71}, 0, tamperVRFProof)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	// Behind the block's slot, same as the deferred-eligibility fixtures
	// below -- proves a real crypto failure is not masked by state-defer
	// tolerance.
	ls.currentTip.Point.Slot = tb.block.SlotNumber() - 1
	ls.publishSnapshotsLocked()

	err := ls.ValidateChainSelectionHeaderCrypto(tb.block.Header())
	require.Error(t, err)
	assert.False(
		t,
		IsHeaderVerificationDeferred(err),
		"a tampered VRF proof must be a definite failure, not deferred",
	)
}

// TestValidateChainSelectionHeaderCryptoDefersAheadOfLocalState proves that a
// header this node cannot yet confirm leader eligibility for -- because local
// ledger application has not reached its slot -- is reported as deferred, not
// rejected. This is the fast-sync/Genesis-bootstrap case the fix must
// preserve: an honest peer legitimately racing ahead of local ledger apply
// must still be eligible for chain-selection density.
func TestValidateChainSelectionHeaderCryptoDefersAheadOfLocalState(
	t *testing.T,
) {
	t.Parallel()

	tb := createTestBlock(t, [32]byte{72}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	ls.currentTip.Point.Slot = tb.block.SlotNumber() - 1
	ls.publishSnapshotsLocked()

	err := ls.ValidateChainSelectionHeaderCrypto(tb.block.Header())
	require.Error(t, err)
	assert.True(
		t,
		IsHeaderVerificationDeferred(err),
		"a header ahead of local ledger state must defer, not fail closed",
	)
}

// TestValidateChainSelectionHeaderCryptoDoesNotAdvanceEpochCache proves that,
// like ValidateBlockHeaderCrypto, verifying a header for chain selection
// never mutates the shared epoch cache -- an unauthenticated peer header must
// not be able to influence shared ledger state as a side effect of being
// observed for density.
func TestValidateChainSelectionHeaderCryptoDoesNotAdvanceEpochCache(
	t *testing.T,
) {
	t.Parallel()

	const futureSlot = uint64(1001)
	ls := &LedgerState{
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{Point: ocommon.NewPoint(500, []byte("tip"))},
		epochCache: []models.Epoch{{
			EpochId:       500,
			StartSlot:     0,
			SlotLength:    1_000,
			LengthInSlots: 1_000,
			EraId:         eras.ConwayEraDesc.Id,
			Nonce:         []byte{0x01},
		}},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	err := ls.ValidateChainSelectionHeaderCrypto(
		&mockBabbageBlock{slot: futureSlot},
	)
	require.Error(t, err)
	assert.Len(
		t,
		ls.loadConsensusSnapshot().epochCache,
		1,
		"chain-selection header validation must not advance the shared epoch cache",
	)
}

// TestValidateChainSelectionHeaderCryptoDefersOnUnpublishedNonce is a
// regression test for a bot-review finding: a cached epoch entry that
// genuinely covers the header's slot but has no published nonce yet (a
// post-Byron epoch transiently, or Byron always) must defer, not hard-fail.
// ShouldVerifyChainSelectionHeaderCrypto now returns true for every
// non-Mithril slot (see TestShouldVerifyChainSelectionHeaderCryptoIgnoresMissingNonce),
// so this case is reachable in practice: without IsHeaderVerificationDeferred
// also recognizing errEpochNonceUnavailable, an honest peer whose header
// simply arrived before the local nonce was published would be treated as
// invalid and have its connection recycled.
func TestValidateChainSelectionHeaderCryptoDefersOnUnpublishedNonce(
	t *testing.T,
) {
	const targetSlot = uint64(500)
	ls := &LedgerState{
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{Point: ocommon.NewPoint(500, []byte("tip"))},
		epochCache: []models.Epoch{{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    1_000,
			LengthInSlots: 1_000,
			EraId:         eras.ConwayEraDesc.Id,
			Nonce:         nil,
		}},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	err := ls.ValidateChainSelectionHeaderCrypto(
		&mockBabbageBlock{slot: targetSlot},
	)
	require.Error(t, err)
	assert.True(
		t,
		IsHeaderVerificationDeferred(err),
		"a covered epoch with no published nonce yet must defer, not "+
			"hard-fail an honest header",
	)
}

// TestShouldVerifyChainSelectionHeaderCryptoMatchesChainsyncGate proves that
// ShouldVerifyChainSelectionHeaderCrypto shares the same Mithril exemption as
// the ledger's own chainsync header-queue gate (shouldEnforceBlockPipelineCrypto),
// so a competing peer's header is exempt under exactly the same condition the
// applied chain already is. Issue #3528: a coarse ValidateHistorical=false
// historical-sync toggle must not exempt header crypto -- only a slot a
// Mithril certificate already covers may skip it, regardless of
// validationEnabled.
func TestShouldVerifyChainSelectionHeaderCryptoMatchesChainsyncGate(
	t *testing.T,
) {
	t.Parallel()

	tb := createTestBlock(t, [32]byte{73}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	slot := tb.block.SlotNumber()

	assert.True(
		t,
		ls.ShouldVerifyChainSelectionHeaderCrypto(slot),
		"verification must run once the epoch nonce is cached, "+
			"independent of validationEnabled",
	)

	ls.validationEnabled = true
	ls.publishSnapshotsLocked()
	assert.True(
		t,
		ls.ShouldVerifyChainSelectionHeaderCrypto(slot),
		"verification must still run once live validation is enabled and the "+
			"epoch nonce is cached",
	)

	ls.mithrilLedgerSlot = slot
	ls.publishSnapshotsLocked()
	assert.False(
		t,
		ls.ShouldVerifyChainSelectionHeaderCrypto(slot),
		"a Mithril-covered slot must be exempt, matching the applied-chain gate",
	)
}

// TestShouldVerifyChainSelectionHeaderCryptoIgnoresMissingNonce is a
// regression test for a bot-review finding: ShouldVerifyChainSelectionHeaderCrypto
// used to delegate to shouldEnforceBlockPipelineCrypto, which also returns
// false when the epoch nonce isn't cached yet -- a condition the chainsync
// header-queue path can safely retry later, but chain selection cannot (a
// header it skips verifying is never re-verified). That let an unverified
// peer header influence Genesis density/corroboration silently, with no
// later check. It must instead return true for any non-Mithril slot and let
// ValidateChainSelectionHeaderCrypto's own deferred-error handling decide,
// which is safe to call unconditionally.
func TestShouldVerifyChainSelectionHeaderCryptoIgnoresMissingNonce(
	t *testing.T,
) {
	const futureSlot = uint64(999_999)
	ls := &LedgerState{
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{Point: ocommon.NewPoint(500, []byte("tip"))},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	require.False(
		t,
		ls.hasCachedEpochNonceForSlot(futureSlot),
		"test setup: this slot's epoch nonce must not be cached",
	)
	assert.True(
		t,
		ls.ShouldVerifyChainSelectionHeaderCrypto(futureSlot),
		"a missing epoch nonce must not exempt a non-Mithril slot from "+
			"verification -- ValidateChainSelectionHeaderCrypto must be given "+
			"the chance to return a deferred error instead",
	)

	err := ls.ValidateChainSelectionHeaderCrypto(
		&mockBabbageBlock{slot: futureSlot},
	)
	require.Error(t, err)
	assert.True(
		t,
		IsHeaderVerificationDeferred(err),
		"missing epoch data must defer, not silently pass or hard-reject",
	)
}

// cardanoNodeConfigWithMaxLovelaceSupply builds a *cardano.CardanoNodeConfig
// whose ShelleyGenesis().MaxLovelaceSupply is nonzero -- the exact condition
// circulatingSupplyGenesis (ledger/queries.go) gates
// verifyStakeDistributionRetentionOnly's network_state floor on. A fixture
// with no CardanoNodeConfig at all leaves that floor inactive, so a case
// built without this helper can pass for the wrong reason: pinning
// over-rejection when the gate it means to test was never active, not the
// real requirement.
func cardanoNodeConfigWithMaxLovelaceSupply(t *testing.T, maxLovelaceSupply uint64) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(
		fmt.Sprintf(`{"maxLovelaceSupply": %d}`, maxLovelaceSupply),
	)))
	return cfg
}

// TestVerifyPointQueryable_WithinAllFloors_Accepted covers the accept
// direction: a point inside every point-aware query type's own retention
// floor -- on chain, within the UTxO/stake/pparams/era windows -- must be
// accepted so a well-behaved client's Acquire actually succeeds, not just
// so a stale one is rejected.
func TestVerifyPointQueryable_WithinAllFloors_Accepted(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	// Activates verifyStakeDistributionRetentionOnly's network_state floor
	// (circulatingSupplyGenesis) -- see cardanoNodeConfigWithMaxLovelaceSupply's
	// doc comment for why this fixture must set it to genuinely prove the
	// accept direction, not just the case where the floor never runs at all.
	ls.config.CardanoNodeConfig = cardanoNodeConfigWithMaxLovelaceSupply(t, 45_000_000_000_000_000)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 3}
	ls.publishSnapshotsLocked()

	hash := repeatedBytes(32, 0x0B)
	seedBlockAtSlot(t, ls, 350, hash)
	seedEpochs(t, ls, map[uint64]uint64{300: 3})
	// verifyStakeDistributionRetentionOnly's second floor requires a
	// network_state row at or before the pinned slot, matching what
	// PoolStakeDistribution's own totalCirculatingSupply call separately
	// requires -- without this row, a point can be inside the epoch-based
	// retention window and still get rejected.
	require.NoError(t, db.Metadata().SetNetworkState(0, 1_000, 300, nil))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(350, hash),
	}, nil))

	err := ls.VerifyPointQueryable(nil, QueryPoint{Slot: 350, Hash: hash})
	require.NoError(t, err)
}

// TestVerifyPointQueryable_PastRetentionFloor_Rejected covers the reject
// direction: a point on-chain but past the stake-snapshot retention floor
// must fail with ErrHistoricalStateUnavailable, the sentinel
// localstatequeryServerAcquire maps to a clean wire-level
// AcquireFailurePointTooOld -- exactly mirroring
// TestPoolStakeDistribution_AsOfSlot_TooOldRejected's scenario, but through
// VerifyPointQueryable (which checks verifyPointOnChain first, unlike a
// bare PoolStakeDistribution call) to prove the whole upfront check
// rejects it, not just the one retention check it happens to hit first.
func TestVerifyPointQueryable_PastRetentionFloor_Rejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 10}
	ls.publishSnapshotsLocked()

	hash := repeatedBytes(32, 0x0B)
	seedBlockAtSlot(t, ls, 350, hash)
	seedEpochs(t, ls, map[uint64]uint64{300: 3, 1000: 10})
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1050, repeatedBytes(32, 0x0C)),
	}, nil))

	// Epoch 3 is 7 epochs behind the live epoch (10) -- outside the
	// 3-epoch stake-snapshot retention window.
	err := ls.VerifyPointQueryable(nil, QueryPoint{Slot: 350, Hash: hash})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// TestVerifyPointQueryable_APIStorageMode_PastRetentionFloor_Accepted covers
// checkAsOfEpochRecency's apiStorageMode early-return branch (ledger/pool_stake_distribution.go):
// pool-stake snapshots are never pruned when the database runs in API
// storage mode, so a point whose mark-snapshot epoch would be rejected under
// the default (core) retention window must still be accepted here, matching
// cleanupOldSnapshots' own API-mode carve-out that this check mirrors.
//
// Same shape as TestVerifyPointQueryable_PastRetentionFloor_Rejected -- an
// epoch 3 pinned point 7 epochs behind live epoch 10, well outside the
// 3-epoch stake-snapshot retention window -- except the database is opened
// in API storage mode instead of the default core mode, and epoch 3 carries
// its own persisted epoch row and pparams row (so the historical-epoch reads
// further down VerifyPointQueryable, which that rejected-in-core-mode test
// never reaches, succeed here on their own merits rather than accidentally
// masking the check under test). That test proves core mode must reject
// this point; this test proves API mode must accept the identical point
// instead. No-opping the apiStorageMode branch in checkAsOfEpochRecency
// (i.e. falling through to the pruning-window check regardless of storage
// mode) makes this test fail with ErrHistoricalStateUnavailable instead of
// the required nil.
func TestVerifyPointQueryable_APIStorageMode_PastRetentionFloor_Accepted(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDBForCleanup(t, types.StorageModeAPI)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {9, 9, 9}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 10}
	ls.publishSnapshotsLocked()

	conwayEraId := uint(eras.ConwayEraDesc.Id)
	hash := repeatedBytes(32, 0x0B)
	seedBlockAtSlot(t, ls, 350, hash)
	require.NoError(t, ls.db.SetEpoch(
		300, 3, nil, nil, nil, nil, conwayEraId, 1, 100, nil,
	))
	require.NoError(t, ls.db.SetEpoch(
		1000, 10, nil, nil, nil, nil, conwayEraId, 1, 100, nil,
	))
	historicalPParams := conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	historicalCbor, err := cbor.Encode(historicalPParams)
	require.NoError(t, err)
	require.NoError(t, ls.db.SetPParams(
		historicalCbor, 300, 3, conwayEraId, nil,
	))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1050, repeatedBytes(32, 0x0C)),
	}, nil))

	// Epoch 3 is 7 epochs behind the live epoch (10) -- outside the
	// 3-epoch stake-snapshot retention window that applies in core storage
	// mode (see TestVerifyPointQueryable_PastRetentionFloor_Rejected). In
	// API storage mode, pool-stake snapshots are never pruned, so this must
	// be accepted instead.
	verifyErr := ls.VerifyPointQueryable(nil, QueryPoint{Slot: 350, Hash: hash})
	require.NoError(t, verifyErr)
}

// TestVerifyPointQueryable_NoNetworkStateRow_Rejected covers a regression:
// checking only checkAsOfEpochRecency's mark-snapshot floor is not enough,
// since PoolStakeDistribution's own totalCirculatingSupply call separately
// requires a network_state row at or before the pinned slot (asOfSlot,
// non-nil for a pinned point) and rejects with ErrHistoricalStateUnavailable
// when missing. Before this second floor was added, VerifyPointQueryable
// accepted this exact point,
// and a client that then Acquired it and issued GetPoolDistr2 or
// GetStakeDistribution got that same error from a live query instead --
// handleQuery returns it bare, tearing the connection down, the identical
// failure this whole change exists to close at Acquire time instead.
//
// Identical to TestVerifyPointQueryable_WithinAllFloors_Accepted (pinned
// point's epoch equals the live epoch, so queryShelleyCurrentProtocolParams
// answers from the live snapshot rather than needing a historical pparams
// row, and CardanoNodeConfig is set so the network_state floor is actually
// active -- see cardanoNodeConfigWithMaxLovelaceSupply's doc comment; a
// fixture without it would pass here for the wrong reason, since the floor
// this test targets would never run at all) except for the one thing this
// test is about: no db.Metadata().SetNetworkState call, so no
// network_state row exists at any slot. Isolating every other floor this
// way means only the new check this test targets can be why this fails.
func TestVerifyPointQueryable_NoNetworkStateRow_Rejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.config.CardanoNodeConfig = cardanoNodeConfigWithMaxLovelaceSupply(t, 45_000_000_000_000_000)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 3}
	ls.publishSnapshotsLocked()

	hash := repeatedBytes(32, 0x0B)
	seedBlockAtSlot(t, ls, 350, hash)
	seedEpochs(t, ls, map[uint64]uint64{300: 3})
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(350, hash),
	}, nil))

	err := ls.VerifyPointQueryable(nil, QueryPoint{Slot: 350, Hash: hash})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// TestVerifyPointQueryable_NoNetworkStateRow_AcceptedWhenFloorInactive
// covers the companion case: an unconditional network_state floor would
// reject a point every real query would have answered whenever
// totalCirculatingSupply itself never reaches GetNetworkStateAsOfSlot --
// no CardanoNodeConfig (as here, and as every other ledger test in this
// repository already constructs a LedgerState), no ShelleyGenesis, or a
// genesis with no MaxLovelaceSupply. Identical to
// TestVerifyPointQueryable_NoNetworkStateRow_Rejected (same missing row)
// except CardanoNodeConfig is left nil, so this one must accept where that
// one must reject -- proving the floor is genuinely conditional, not just
// present or absent.
func TestVerifyPointQueryable_NoNetworkStateRow_AcceptedWhenFloorInactive(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 3}
	ls.publishSnapshotsLocked()

	hash := repeatedBytes(32, 0x0B)
	seedBlockAtSlot(t, ls, 350, hash)
	seedEpochs(t, ls, map[uint64]uint64{300: 3})
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(350, hash),
	}, nil))

	err := ls.VerifyPointQueryable(nil, QueryPoint{Slot: 350, Hash: hash})
	require.NoError(t, err)
}

// TestVerifyPointQueryable_UnknownEraId_Rejected covers a regression:
// VerifyPointQueryable's queryHardFork call (HardForkCurrentEraQuery) is
// the only one of its five checks that ever inspects an epoch row's era at
// all, so nothing else here
// would catch it being silently dropped. Both existing regression tests
// pass whether or not that call exists, because neither fixture gives it
// anything to reject on: WithinAllFloors_Accepted's epoch row names a real
// era, and PastRetentionFloor_Rejected is already rejected earlier by
// verifyStakeDistributionRetentionOnly.
//
// Identical to TestVerifyPointQueryable_WithinAllFloors_Accepted (every
// other floor passes cleanly: on chain, within the UTxO/stake/pparams
// windows, with a covering network_state row) except the epoch row itself
// names era 255, which eras.GetEraById cannot resolve. Deleting the
// queryHardFork call from VerifyPointQueryable would make this pass when it
// must fail.
func TestVerifyPointQueryable_UnknownEraId_Rejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 3}
	ls.publishSnapshotsLocked()

	hash := repeatedBytes(32, 0x0B)
	seedBlockAtSlot(t, ls, 350, hash)
	require.NoError(t, ls.db.SetEpoch(
		300, 3, nil, nil, nil, nil, 255, 1, 100, nil,
	))
	require.NoError(t, db.Metadata().SetNetworkState(0, 1_000, 300, nil))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(350, hash),
	}, nil))

	err := ls.VerifyPointQueryable(nil, QueryPoint{Slot: 350, Hash: hash})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// TestVerifyPointQueryable_UtxoFloorOnly_Rejected covers a gap the other
// TestVerifyPointQueryable* cases leave open: deleting the
// checkUtxoRetentionWindow call, or the queryShelleyCurrentProtocolParams
// call, from VerifyPointQueryable leaves every existing
// TestVerifyPointQueryable* case green --
// PastRetentionFloor_Rejected is rejected by the stake floor either way, and
// no fixture has a UTxO-floor rejection as its only failure. Identical to
// TestVerifyPointQueryable_WithinAllFloors_Accepted (on chain, within the
// stake/era windows, no CardanoNodeConfig so the network_state floor never
// runs) except for a durably persisted consumed-UTxO prune floor (400) above
// the pinned slot (350) -- only checkUtxoRetentionWindow can reject this
// point.
func TestVerifyPointQueryable_UtxoFloorOnly_Rejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 3}
	ls.publishSnapshotsLocked()

	hash := repeatedBytes(32, 0x0B)
	seedBlockAtSlot(t, ls, 350, hash)
	seedEpochs(t, ls, map[uint64]uint64{300: 3})
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(350, hash),
	}, nil))
	require.NoError(t, ls.persistConsumedUtxoPruneFloor(400, nil))

	err := ls.VerifyPointQueryable(nil, QueryPoint{Slot: 350, Hash: hash})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// TestVerifyPointQueryable_PParamsRowOnly_Rejected covers a gap the other
// TestVerifyPointQueryable* cases leave open: deleting the
// checkUtxoRetentionWindow call, or this queryShelleyCurrentProtocolParams
// call, from VerifyPointQueryable leaves every existing
// TestVerifyPointQueryable* case green -- neither deletion
// changes PastRetentionFloor_Rejected's outcome, since the stake floor
// already rejects that fixture, and no case has a missing pparams row as its
// only failure. The pinned point's epoch (3) sits exactly at the
// stake-retention window's edge relative to the live epoch (5) -- mark
// snapshot epoch 2 equals the floor 5-3=2, so checkAsOfEpochRecency accepts
// it (the same boundary TestQueryShelleyUtxoByTxIn_RetentionWindow_AtFloor_Succeeds
// covers for the UTxO floor) -- and no CardanoNodeConfig means the
// network_state floor never runs, so only the missing persisted pparams row
// for epoch 3 can reject this point.
func TestVerifyPointQueryable_PParamsRowOnly_Rejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(
		map[uint][]int64{0: {1, 1, 1}},
	)
	ls.currentEpoch = models.Epoch{EpochId: 5}
	ls.publishSnapshotsLocked()

	hash := repeatedBytes(32, 0x0B)
	seedBlockAtSlot(t, ls, 350, hash)
	seedEpochs(t, ls, map[uint64]uint64{300: 3, 700: 5})
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(750, repeatedBytes(32, 0x0C)),
	}, nil))

	err := ls.VerifyPointQueryable(nil, QueryPoint{Slot: 350, Hash: hash})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// prunedSnapshotFixture builds a ledger that has advanced to epoch 8 while the
// block's mark snapshot (epoch 4) has been pruned by the default 3-epoch
// pool-snapshot retention window, the way cleanupOldSnapshots leaves it.
func prunedSnapshotFixture(
	t *testing.T,
	tb *testBlockResult,
	withSummary bool,
	onChain bool,
) *LedgerState {
	t.Helper()
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	pool := tb.block.IssuerVkey().Hash()
	seedPoolStakeSnapshot(t, db, 4, pool[:], 1_000_000_000)
	if withSummary {
		require.NoError(t, db.Metadata().SaveEpochSummary(&models.EpochSummary{
			Epoch:            4,
			TotalActiveStake: types.Uint64(1_000_000_000),
			TotalPoolCount:   1,
			SnapshotReady:    true,
		}, nil))
	}
	seedBlockPoolRegistration(t, db, tb.block)
	require.NoError(
		t,
		db.Metadata().DeletePoolStakeSnapshotsBeforeEpoch(5, nil),
	)

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	hash := tb.block.Header().Hash().Bytes()
	if !onChain {
		// A different block at the same slot: the header is on a fork.
		hash = append([]byte{0xff}, hash[1:]...)
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks([]chain.RawBlock{{
		Slot:        tb.block.SlotNumber(),
		Hash:        hash,
		BlockNumber: 1,
		Type:        1,
		Cbor:        []byte{0x80},
	}}))
	ls.chain = cm.PrimaryChain()

	ls.currentEpoch = models.Epoch{EpochId: 8}
	ls.currentTip = ochainsync.Tip{Point: ocommon.Point{
		Slot: tb.block.SlotNumber() + 1_000,
	}}
	ls.publishSnapshotsLocked()
	return ls
}

// A header the node already applied must not be re-judged against pool
// snapshots the retention window has since pruned.
func TestValidateChainSelectionHeaderCryptoAppliedHeaderSurvivesPruning(
	t *testing.T,
) {
	t.Parallel()
	for _, withSummary := range []bool{false, true} {
		name := "no-summary"
		if withSummary {
			name = "summary"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tb := createTestBlock(t, [32]byte{30}, 0, tamperNone)
			ls := prunedSnapshotFixture(t, tb, withSummary, true)
			err := ls.ValidateChainSelectionHeaderCrypto(tb.block.Header())
			assert.NoError(t, err)
		})
	}
}

// A block can join the chain with its state checks deferred until the ledger
// applies it, so holding the point is not enough to skip verification while
// the ledger tip is still behind the header's slot.
func TestValidateChainSelectionHeaderCryptoChainHeldUnappliedHeaderStillVerified(
	t *testing.T,
) {
	t.Parallel()
	tb := createTestBlock(t, [32]byte{34}, 0, tamperVRFProof)
	ls := prunedSnapshotFixture(t, tb, true, true)
	ls.Lock()
	ls.currentTip = ochainsync.Tip{Point: ocommon.Point{
		Slot: tb.block.SlotNumber() - 1,
	}}
	ls.publishSnapshotsLocked()
	ls.Unlock()
	err := ls.ValidateChainSelectionHeaderCrypto(tb.block.Header())
	require.Error(t, err)
	assert.False(t, IsHeaderVerificationDeferred(err), "%v", err)
}

// A header on a fork is still verified, but pruned history is "cannot
// evaluate" (deferred), not proof the pool is absent.
func TestValidateChainSelectionHeaderCryptoForkHeaderPrunedSnapshotDefers(
	t *testing.T,
) {
	t.Parallel()
	for _, withSummary := range []bool{false, true} {
		name := "no-summary"
		if withSummary {
			name = "summary"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tb := createTestBlock(t, [32]byte{31}, 0, tamperNone)
			ls := prunedSnapshotFixture(t, tb, withSummary, false)
			err := ls.ValidateChainSelectionHeaderCrypto(tb.block.Header())
			require.Error(t, err)
			assert.True(
				t,
				IsHeaderVerificationDeferred(err),
				"pruned snapshot must defer, not reject: %v",
				err,
			)
		})
	}
}

// Header crypto is still checked for a header that is not on the chain, even
// when its stake snapshot is pruned.
func TestValidateChainSelectionHeaderCryptoForkHeaderStillVerified(
	t *testing.T,
) {
	t.Parallel()
	tb := createTestBlock(t, [32]byte{32}, 0, tamperVRFProof)
	ls := prunedSnapshotFixture(t, tb, true, false)
	err := ls.ValidateChainSelectionHeaderCrypto(tb.block.Header())
	require.Error(t, err)
	assert.False(t, IsHeaderVerificationDeferred(err), "%v", err)
}

// A pool absent from a populated, unpruned snapshot is still a hard
// rejection.
func TestValidateChainSelectionHeaderCryptoPoolAbsentFromPopulatedSnapshotRejects(
	t *testing.T,
) {
	t.Parallel()
	tb := createTestBlock(t, [32]byte{33}, 0, tamperNone)
	ls := prunedSnapshotFixture(t, tb, true, false)
	other := make([]byte, 28)
	other[0] = 0xee
	seedPoolStakeSnapshot(t, ls.db, 4, other, 1_000_000_000)
	err := ls.ValidateChainSelectionHeaderCrypto(tb.block.Header())
	require.Error(t, err)
	assert.False(t, IsHeaderVerificationDeferred(err), "%v", err)
	assert.Contains(t, err.Error(), "has no stake in epoch 4 snapshot")
}

// TestVerifyRegisteredVrfKey_RejectsUnregisteredOrMismatchedKey verifies the
// consensus-critical binding of a block's VRF verification key to the producing
// pool's on-chain registered VRF key. The VRF proof is validated only against
// the key carried in the header, so a block whose VRF key is not the one the
// pool registered must be rejected — otherwise an attacker can grind VRF keys
// offline and win slots regardless of stake. With no pool registration present
// for the block's issuer, the block's VRF key cannot match any registered key,
// so verification must fail (it must never pass by default).
func TestVerifyRegisteredVrfKey_RejectsUnregisteredOrMismatchedKey(
	t *testing.T,
) {
	t.Parallel()

	tb := createTestBlock(t, [32]byte{71}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)

	// No pool registration seeded for this block's issuer, so the header's VRF
	// key is not bound to any registered VRF key.
	err := ls.verifyRegisteredVrfKey(tb.block, blockEpochId(t, ls, tb.block))
	require.Error(
		t,
		err,
		"a block whose VRF key is not the issuer pool's registered VRF key must be rejected",
	)
	// The rejection is specifically about the VRF key / pool registration, not
	// some unrelated failure.
	msg := err.Error()
	assert.True(
		t,
		containsAny(
			msg,
			"VRF key",
			"registered VRF key",
			"registration lookup",
		),
		"rejection should cite the VRF-key registration binding, got: %s",
		msg,
	)

	vrfKey, ok, err := headerVrfKeyFromBodyCbor(tb.block.Header())
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, vrfKey)
	poolKeyHash := tb.block.IssuerVkey().Hash()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(
		"INSERT INTO pool (pool_key_hash, vrf_key_hash) VALUES (?, ?)",
		poolKeyHash[:],
		lcommon.Blake2b256Hash(vrfKey).Bytes(),
	)
	require.NoError(t, err)

	err = ls.verifyRegisteredVrfKey(tb.block, blockEpochId(t, ls, tb.block))
	require.Error(
		t,
		err,
		"a denormalized pool row without a registration must not bind "+
			"the header VRF key",
	)
	assert.Contains(t, err.Error(), "registered VRF key hash unavailable")
}

// TestVerifyRegisteredVrfKeyAcceptsAFirstRegistrationInsideTheCapturedEpoch
// pins the reference behaviour for a pool that has only ever registered once,
// inside the epoch the electing snapshot was captured in.
//
// cardano-ledger's POOL rule inserts a first registration into psStakePools
// immediately and defers only a re-registration through
// psFutureStakePoolParams (Shelley/Rules/Pool.hs), so such a pool is already
// in psStakePools when SNAP runs and the snapshot carries its VRF key. The
// parameter cutoff predates that registration, so resolving strictly at the
// cutoff finds nothing — and rejecting there would reject a canonical block
// from every pool for its first epochs, which is what this test previously
// asserted.
func TestVerifyRegisteredVrfKeyAcceptsAFirstRegistrationInsideTheCapturedEpoch(
	t *testing.T,
) {
	t.Parallel()

	tb := createTestBlock(t, [32]byte{76}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	blockSlot := tb.block.SlotNumber()
	ls.epochCache = []models.Epoch{
		{EpochId: 4, StartSlot: 0, LengthInSlots: uint(blockSlot)},
		{EpochId: 5, StartSlot: blockSlot, LengthInSlots: 1_000_000},
	}
	ls.publishSnapshotsLocked()

	vrfKey, ok, err := headerVrfKeyFromBodyCbor(tb.block.Header())
	require.NoError(t, err)
	require.True(t, ok)
	poolKeyHash := tb.block.IssuerVkey().Hash()
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash: poolKeyHash[:],
			VrfKeyHash:  lcommon.Blake2b256Hash(vrfKey).Bytes(),
		},
		&models.PoolRegistration{
			PoolKeyHash: poolKeyHash[:],
			VrfKeyHash:  lcommon.Blake2b256Hash(vrfKey).Bytes(),
			AddedSlot:   blockSlot,
		},
		nil,
	))
	require.NoError(t, db.Metadata().SavePoolStakeSnapshot(
		&models.PoolStakeSnapshot{
			Epoch:        4,
			SnapshotType: models.PoolStakeSnapshotTypeMark,
			PoolKeyHash:  poolKeyHash[:],
			TotalStake:   1,
			CapturedSlot: blockSlot,
		},
		nil,
	))

	require.NoError(t, ls.verifyRegisteredVrfKey(tb.block, 5),
		"a pool whose only registration lands inside the captured epoch is "+
			"in psStakePools when SNAP runs, so the snapshot carries its key")
}

// TestVerifyRegisteredVrfKey_AcceptsMatchingKeyRejectsMismatch is the
// positive-and-mismatch counterpart to the unregistered-pool case: it proves
// the binding accepts a block whose header VRF key hashes to the pool's
// registered VRF key hash, and rejects one whose does not. The VRF proof is
// only ever validated against the header-carried key, so this equality is the
// sole barrier preventing an attacker from registering with one VRF key and
// producing blocks with a different, offline-ground key.
func TestVerifyRegisteredVrfKey_AcceptsMatchingKeyRejectsMismatch(
	t *testing.T,
) {
	t.Parallel()

	// --- Matching registered VRF key is accepted ---
	tbMatch := createTestBlock(t, [32]byte{72}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tbMatch.epochNonce)

	matchVrfKey, ok, err := headerVrfKeyFromBodyCbor(tbMatch.block.Header())
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, matchVrfKey)

	matchPoolKeyHash := tbMatch.block.IssuerVkey().Hash()
	seedPoolRegistration(
		t,
		db,
		matchPoolKeyHash[:],
		lcommon.Blake2b256Hash(matchVrfKey).Bytes(),
	)

	require.NoError(
		t,
		ls.verifyRegisteredVrfKey(
			tbMatch.block,
			blockEpochId(t, ls, tbMatch.block),
		),
		"block whose header VRF key hashes to the pool's registered "+
			"VRF key hash must be accepted",
	)

	// --- Mismatched registered VRF key is rejected ---
	tbMismatch := createTestBlock(t, [32]byte{73}, 0, tamperNone)
	mismatchVrfKey, ok, err := headerVrfKeyFromBodyCbor(
		tbMismatch.block.Header(),
	)
	require.NoError(t, err)
	require.True(t, ok)

	wrongVrfKeyHash := make([]byte, len(lcommon.Blake2b256{}))
	for i := range wrongVrfKeyHash {
		wrongVrfKeyHash[i] = 0xAB
	}
	// Guard against an accidental collision with the block's real VRF key hash.
	require.NotEqual(
		t,
		lcommon.Blake2b256Hash(mismatchVrfKey).Bytes(),
		wrongVrfKeyHash,
	)

	mismatchPoolKeyHash := tbMismatch.block.IssuerVkey().Hash()
	seedPoolRegistration(t, db, mismatchPoolKeyHash[:], wrongVrfKeyHash)

	err = ls.verifyRegisteredVrfKey(
		tbMismatch.block,
		blockEpochId(t, ls, tbMismatch.block),
	)
	require.Error(
		t,
		err,
		"block whose header VRF key does not match the registered "+
			"VRF key must be rejected",
	)
	assert.Contains(t, err.Error(), "VRF key does not match")
}

func TestVerifyRegisteredVrfKey_AcceptsRetiredPoolRegistration(
	t *testing.T,
) {
	t.Parallel()

	tb := createTestBlock(t, [32]byte{74}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)

	vrfKey, ok, err := headerVrfKeyFromBodyCbor(tb.block.Header())
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, vrfKey)

	poolKeyHash := tb.block.IssuerVkey().Hash()
	seedPoolRegistration(
		t,
		db,
		poolKeyHash[:],
		lcommon.Blake2b256Hash(vrfKey).Bytes(),
	)

	require.NoError(t, db.SetEpoch(
		0,
		0,
		nil,
		nil,
		nil,
		nil,
		0,
		1,
		1_000,
		nil,
	))
	require.NoError(t, db.SetEpoch(
		2_000,
		2,
		nil,
		nil,
		nil,
		nil,
		0,
		1,
		1_000,
		nil,
	))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.Point{
			Slot: 2_000,
			Hash: make([]byte, 32),
		},
		BlockNumber: 2,
	}, nil))

	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	var poolID uint
	require.NoError(t, raw.QueryRow(
		"SELECT id FROM pool WHERE pool_key_hash = ?",
		poolKeyHash[:],
	).Scan(&poolID))
	_, err = raw.Exec(`
INSERT INTO pool_retirement (pool_id, pool_key_hash, epoch, added_slot)
VALUES (?, ?, 2, 2)`,
		poolID, poolKeyHash[:],
	)
	require.NoError(t, err)

	require.NoError(
		t,
		ls.verifyRegisteredVrfKey(tb.block, blockEpochId(t, ls, tb.block)),
		"registered VRF-key binding must not depend on current-tip "+
			"active pool filtering",
	)
}

func TestVerifyRegisteredVrfKey_UsesLatestRegistrationBeforePoolRowHash(
	t *testing.T,
) {
	t.Parallel()

	tb := createTestBlock(t, [32]byte{75}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)

	vrfKey, ok, err := headerVrfKeyFromBodyCbor(tb.block.Header())
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, vrfKey)
	registeredVrfHash := lcommon.Blake2b256Hash(vrfKey).Bytes()

	staleVrfHash := make([]byte, len(lcommon.Blake2b256{}))
	for i := range staleVrfHash {
		staleVrfHash[i] = 0xCD
	}
	require.NotEqual(t, registeredVrfHash, staleVrfHash)

	poolKeyHash := tb.block.IssuerVkey().Hash()
	err = db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash: poolKeyHash[:],
			VrfKeyHash:  staleVrfHash,
		},
		&models.PoolRegistration{
			PoolKeyHash: poolKeyHash[:],
			VrfKeyHash:  registeredVrfHash,
			AddedSlot:   1,
		},
		nil,
	)
	require.NoError(t, err)

	require.NoError(
		t,
		ls.verifyRegisteredVrfKey(tb.block, blockEpochId(t, ls, tb.block)),
		"latest registration VRF hash should take precedence over stale "+
			"denormalized pool VRF hash",
	)
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if len(sub) > 0 && len(s) >= len(sub) {
			for i := 0; i+len(sub) <= len(s); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}

// TestLedgerProcessBlockByronAdoptedFeePolicy covers #4419 through block
// application: a real, signed Byron transaction paying a 200 lovelace fee is
// judged by the fee policy adopted for the block, which ledgerProcessBlock
// receives as pparams, and not by the genesis policy. Each case changes the
// summand or the multiplier so that the adopted policy and the genesis one
// disagree about the same transaction.
func TestLedgerProcessBlockByronAdoptedFeePolicy(t *testing.T) {
	t.Parallel()
	const (
		protocolMagic = 764824073
		fee           = 200
		nanoPerUnit   = 1_000_000_000
	)
	keyA := newByronBlockTestKey(t, 0x71)
	payTo := newByronBlockTestKey(t, 0x72).address
	build := func(t *testing.T, db *database.Database) *byron.ByronTransaction {
		a := seedByronUtxoWithAmount(t, db, 0x01, keyA.address, 1_000)
		return buildByronBlockTestTx(t, protocolMagic,
			[]byronBlockTestInput{{a, 0}},
			[]byronBlockTestOutput{{payTo, 1_000 - fee}},
			nil, []byronBlockTestKey{keyA})
	}
	size := eras.TxSizeForFee(build(t, newTestDB(t)))
	require.Positive(t, size)
	// perByteNano makes the multiplier alone require exactly fee lovelace or
	// slightly less, so doubling it requires more than fee.
	perByteNano := int64(fee * nanoPerUnit / size)

	nodeConfigFor := func(
		t *testing.T,
		summand, multiplier int64,
	) *cardano.CardanoNodeConfig {
		t.Helper()
		nodeConfig := &cardano.CardanoNodeConfig{}
		genesisJSON := fmt.Sprintf(
			`{"blockVersionData": {"slotDuration": "20000", `+
				`"maxTxSize": "4096", "txFeePolicy": `+
				`{"summand": "%d", "multiplier": "%d"}}, `+
				`"protocolConsts": {"k": 2160, "protocolMagic": %d}}`,
			summand, multiplier, protocolMagic,
		)
		require.NoError(t, loadByronGenesisForTest(
			t, nodeConfig, strings.NewReader(genesisJSON),
		))
		return nodeConfig
	}
	adopt := func(
		t *testing.T,
		nodeConfig *cardano.CardanoNodeConfig,
		summandNano, multiplierNano int64,
	) *eras.ByronProtocolParameters {
		t.Helper()
		genesis, err := eras.NewByronProtocolParametersFromGenesis(
			nodeConfig.ByronGenesis(),
		)
		require.NoError(t, err)
		adopted, err := genesis.ApplyUpdate(
			byron.ByronUpdateProposalBlockVersionMod{
				TxFeePolicy: []byron.ByronTxFeePolicy{{
					SummandNano:    big.NewInt(summandNano),
					MultiplierNano: big.NewInt(multiplierNano),
				}},
			},
		)
		require.NoError(t, err)
		return adopted
	}

	tests := []struct {
		name string
		// genesis is the Byron genesis fee policy {summand, multiplier} in
		// nano-lovelace; adopted is the policy an update adopted.
		genesis, adopted [2]int64
		wantRejected     bool
	}{
		{
			name:    "lowered summand accepts what genesis rejects",
			genesis: [2]int64{(fee + 1) * nanoPerUnit, 0},
			adopted: [2]int64{fee * nanoPerUnit, 0},
		},
		{
			name:         "raised summand rejects what genesis accepts",
			genesis:      [2]int64{fee * nanoPerUnit, 0},
			adopted:      [2]int64{(fee + 1) * nanoPerUnit, 0},
			wantRejected: true,
		},
		{
			name:    "lowered multiplier accepts what genesis rejects",
			genesis: [2]int64{0, 2 * perByteNano},
			adopted: [2]int64{0, perByteNano},
		},
		{
			name:         "raised multiplier rejects what genesis accepts",
			genesis:      [2]int64{0, perByteNano},
			adopted:      [2]int64{0, 2 * perByteNano},
			wantRejected: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			nodeConfig := nodeConfigFor(t, test.genesis[0], test.genesis[1])
			genesis, err := eras.NewByronProtocolParametersFromGenesis(
				nodeConfig.ByronGenesis(),
			)
			require.NoError(t, err)
			adopted := adopt(t, nodeConfig, test.adopted[0], test.adopted[1])

			// Control: the genesis policy reaches the opposite verdict.
			db := newTestDB(t)
			err = processByronBlockWithPParams(
				t, db, nodeConfig, build(t, db), genesis,
			)
			var feeErr eras.FeeTooLowByronError
			if test.wantRejected {
				require.NoError(t, err, "genesis policy")
			} else {
				require.ErrorAs(t, err, &feeErr, "genesis policy")
			}

			db = newTestDB(t)
			err = processByronBlockWithPParams(
				t, db, nodeConfig, build(t, db), adopted,
			)
			if !test.wantRejected {
				require.NoError(t, err, "adopted policy")
				return
			}
			require.ErrorAs(t, err, &feeErr, "adopted policy")
			required, err := adopted.MinFee(size)
			require.NoError(t, err)
			require.Equal(t, required, feeErr.Required)
		})
	}
}
