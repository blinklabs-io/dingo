// Copyright 2025 Blink Labs Software
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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

var benchmarkDiscardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

const storageModeBenchmarkStartSlot = 10000
const storageModeBenchmarkSkippedInputHash = "e3ca57e8f323265742a8f4e79ff9af884c9ff8719bd4f7788adaea4c33ba07b6"
const storageModeBenchmarkSkippedInputIndex = 3

// Helper functions for benchmark seeding

// openImmutableTestDB opens the immutable test database
func openImmutableTestDB(b *testing.B) *immutable.ImmutableDb {
	immDb, err := immutable.New("../database/immutable/testdata")
	if err != nil {
		b.Fatal(err)
	}
	return immDb
}

// seedBlocksFromSlots seeds the database with blocks from the specified slots
func seedBlocksFromSlots(
	b *testing.B,
	db *database.Database,
	immDb *immutable.ImmutableDb,
	slots []uint64,
) int {
	seeded := 0
	for _, slot := range slots {
		point := ocommon.NewPoint(slot, nil)
		block, err := immDb.GetBlock(point)
		if err != nil {
			b.Logf("GetBlock error for slot %d: %v", slot, err)
			continue
		}
		if block == nil {
			// missing block is acceptable in some ranges; skip
			continue
		}

		// Store block in database
		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			continue // Skip problematic blocks
		}

		blockModel := models.Block{
			ID:       block.Slot, // Use slot as ID
			Slot:     block.Slot,
			Hash:     block.Hash,
			Number:   0,
			Type:     uint(ledgerBlock.Type()),
			PrevHash: ledgerBlock.PrevHash().Bytes(),
			Cbor:     ledgerBlock.Cbor(),
		}

		if err := db.BlockCreate(blockModel, nil); err != nil {
			continue // Skip if block already exists
		}
		seeded++
	}
	return seeded
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
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

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
		_, err := db.UtxosByAddress(testAddr, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUtxoLookupByAddressRealData benchmarks UTxO lookup by address against real seeded data
func BenchmarkUtxoLookupByAddressRealData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Seed database with real blocks (sample from different parts of chain)
	immDb := openImmutableTestDB(b)

	// Sample blocks from different slots to get diverse data (use slots that are more likely to exist)
	sampleSlots := []uint64{1000, 5000, 10000, 50000, 100000, 200000}
	seeded := seedBlocksFromSlots(b, db, immDb, sampleSlots)
	b.Logf("Seeded %d blocks for benchmark", seeded)

	// Create a test address for queries
	paymentKey := make([]byte, 28)
	stakeKey := make([]byte, 28)
	testAddr, err := ledger.NewAddressFromParts(0, 0, paymentKey, stakeKey)
	if err != nil {
		b.Fatal(err)
	}

	// Reset timer after seeding
	b.ResetTimer()

	// Benchmark lookup against real seeded data
	for b.Loop() {
		_, err := db.UtxosByAddress(testAddr, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUtxoLookupByRefNoData benchmarks UTxO lookup by reference on empty database
func BenchmarkUtxoLookupByRefNoData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

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

// BenchmarkUtxoLookupByRefRealData benchmarks UTxO lookup by reference against real seeded data
func BenchmarkUtxoLookupByRefRealData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Seed database with real blocks (sample from different parts of chain)
	immDb := openImmutableTestDB(b)

	// Sample blocks from different slots to get diverse data (use slots that are more likely to exist)
	sampleSlots := []uint64{1000, 5000, 10000, 50000, 100000, 200000}
	seeded := seedBlocksFromSlots(b, db, immDb, sampleSlots)
	b.Logf("Seeded %d blocks for benchmark", seeded)

	// Create a test transaction reference (use a hash from seeded data if possible, otherwise dummy)
	testTxId := make([]byte, 32) // dummy 32-byte tx ID for now
	for i := range testTxId {
		testTxId[i] = byte(i % 256)
	}
	testOutputIdx := uint32(0)

	// Reset timer after seeding
	b.ResetTimer()

	// Benchmark lookup against real seeded data
	for b.Loop() {
		_, err := db.UtxoByRef(testTxId, testOutputIdx, nil)
		// Don't fail on "not found" - this is expected for non-existent UTxOs
		if err != nil && !errors.Is(err, database.ErrUtxoNotFound) {
			b.Fatal(err)
		}
	}
}

// BenchmarkBlockRetrievalByIndexNoData benchmarks block retrieval by index on empty database
func BenchmarkBlockRetrievalByIndexNoData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

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
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Seed database with real blocks
	immDb, err := immutable.New("../database/immutable/testdata")
	if err != nil {
		b.Fatal(err)
	}

	// Sample blocks from different slots
	sampleSlots := []uint64{1000, 5000, 10000, 50000, 100000}

	for i, slot := range sampleSlots {
		point := ocommon.NewPoint(slot, nil)
		block, err := immDb.GetBlock(point)
		if err != nil {
			b.Logf("Could not get block at slot %d: %v", slot, err)
			continue
		}
		if block == nil {
			continue
		}

		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			b.Logf("Could not parse block: %v", err)
			continue
		}

		blockModel := models.Block{
			ID:       uint64(i + 1), // Sequential IDs for easy querying
			Slot:     block.Slot,
			Hash:     block.Hash,
			Number:   0,
			Type:     uint(ledgerBlock.Type()),
			PrevHash: ledgerBlock.PrevHash().Bytes(),
			Cbor:     ledgerBlock.Cbor(),
		}

		if err := db.BlockCreate(blockModel, nil); err != nil {
			b.Logf("Could not create block: %v", err)
			continue
		}
	}

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
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

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
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Seed database with real blocks that contain transactions
	immDb, err := immutable.New("../database/immutable/testdata")
	if err != nil {
		b.Fatal(err)
	}

	// Sample blocks from slots that are likely to have transactions
	sampleSlots := []uint64{10000, 50000, 100000, 150000, 200000}

	for i, slot := range sampleSlots {
		point := ocommon.NewPoint(slot, nil)
		block, err := immDb.GetBlock(point)
		if err != nil {
			b.Logf("Could not get block at slot %d: %v", slot, err)
			continue
		}
		if block == nil {
			continue
		}

		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			b.Logf("Could not parse block: %v", err)
			continue
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
			b.Logf("Could not create block: %v", err)
			continue
		}
	}

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
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create a test stake key
	testStakeKey := make([]byte, 28) // 28-byte stake key hash
	for i := range testStakeKey {
		testStakeKey[i] = byte(i % 256)
	}

	// Reset timer after setup
	b.ResetTimer()

	// Benchmark lookup (on empty database for now)
	for b.Loop() {
		_, err := db.Metadata().GetAccount(testStakeKey, false, nil)
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
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Seed database with real blocks
	immDb, err := immutable.New("../database/immutable/testdata")
	if err != nil {
		b.Fatal(err)
	}

	// Sample blocks from different slots
	sampleSlots := []uint64{1000, 5000, 10000, 50000, 100000}

	for i, slot := range sampleSlots {
		point := ocommon.NewPoint(slot, nil)
		block, err := immDb.GetBlock(point)
		if err != nil {
			b.Logf("Could not get block at slot %d: %v", slot, err)
			continue
		}
		if block == nil {
			continue
		}

		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			b.Logf("Could not parse block: %v", err)
			continue
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
			b.Logf("Could not create block: %v", err)
			continue
		}
	}

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
		_, err := db.Metadata().GetAccount(key, false, nil)
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
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

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
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Seed database with real blocks
	immDb, err := immutable.New("../database/immutable/testdata")
	if err != nil {
		b.Fatal(err)
	}

	// Sample blocks from different slots
	sampleSlots := []uint64{1000, 5000, 10000, 50000, 100000}

	for i, slot := range sampleSlots {
		point := ocommon.NewPoint(slot, nil)
		block, err := immDb.GetBlock(point)
		if err != nil {
			b.Logf("Could not get block at slot %d: %v", slot, err)
			continue
		}
		if block == nil {
			continue
		}

		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			b.Logf("Could not parse block: %v", err)
			continue
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
			b.Logf("Could not create block: %v", err)
			continue
		}
	}

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
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

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
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Seed database with real blocks
	immDb, err := immutable.New("../database/immutable/testdata")
	if err != nil {
		b.Fatal(err)
	}

	// Sample blocks from different slots
	sampleSlots := []uint64{1000, 5000, 10000, 50000, 100000}

	for i, slot := range sampleSlots {
		point := ocommon.NewPoint(slot, nil)
		block, err := immDb.GetBlock(point)
		if err != nil {
			b.Logf("Could not get block at slot %d: %v", slot, err)
			continue
		}
		if block == nil {
			continue
		}

		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			b.Logf("Could not parse block: %v", err)
			continue
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
			b.Logf("Could not create block: %v", err)
			continue
		}
	}

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
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

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
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Seed database with real blocks
	immDb, err := immutable.New("../database/immutable/testdata")
	if err != nil {
		b.Fatal(err)
	}

	// Sample blocks from different slots
	sampleSlots := []uint64{1000, 5000, 10000, 50000, 100000}

	for i, slot := range sampleSlots {
		point := ocommon.NewPoint(slot, nil)
		block, err := immDb.GetBlock(point)
		if err != nil {
			b.Logf("Could not get block at slot %d: %v", slot, err)
			continue
		}
		if block == nil {
			continue
		}

		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			b.Logf("Could not parse block: %v", err)
			continue
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
			b.Logf("Could not create block: %v", err)
			continue
		}
	}

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
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create test epochs
	testEpochs := []uint64{1, 10, 50, 100, 200}

	// Reset timer after setup
	b.ResetTimer()

	// Benchmark lookup (on empty database for now)
	for i := 0; b.Loop(); i++ {
		epoch := testEpochs[i%len(testEpochs)]
		_, err := db.Metadata().GetPParams(epoch, nil)
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
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Seed database with real blocks
	immDb, err := immutable.New("../database/immutable/testdata")
	if err != nil {
		b.Fatal(err)
	}

	// Sample blocks from different slots
	sampleSlots := []uint64{1000, 5000, 10000, 50000, 100000}

	for i, slot := range sampleSlots {
		point := ocommon.NewPoint(slot, nil)
		block, err := immDb.GetBlock(point)
		if err != nil {
			b.Logf("Could not get block at slot %d: %v", slot, err)
			continue
		}
		if block == nil {
			continue
		}

		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			b.Logf("Could not parse block: %v", err)
			continue
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
			b.Logf("Could not create block: %v", err)
			continue
		}
	}

	// Create test epochs
	testEpochs := []uint64{1, 10, 50, 100, 200}

	// Reset timer after seeding
	b.ResetTimer()

	// Benchmark lookup against real seeded data
	for i := 0; b.Loop(); i++ {
		epoch := testEpochs[i%len(testEpochs)]
		_, err := db.Metadata().GetPParams(epoch, nil)
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
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

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
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Seed database with real blocks
	immDb, err := immutable.New("../database/immutable/testdata")
	if err != nil {
		b.Fatal(err)
	}

	// Sample blocks from different slots
	sampleSlots := []uint64{1000, 5000, 10000, 50000, 100000}

	for i, slot := range sampleSlots {
		point := ocommon.NewPoint(slot, nil)
		block, err := immDb.GetBlock(point)
		if err != nil {
			b.Logf("Could not get block at slot %d: %v", slot, err)
			continue
		}
		if block == nil {
			continue
		}

		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			b.Logf("Could not parse block: %v", err)
			continue
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
			b.Logf("Could not create block: %v", err)
			continue
		}
	}

	// Create test points using real block hashes
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
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

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
		_, err := db.Metadata().GetStakeRegistrations(stakeKey, nil)
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
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Seed database with real blocks
	immDb, err := immutable.New("../database/immutable/testdata")
	if err != nil {
		b.Fatal(err)
	}

	// Sample blocks from different slots
	sampleSlots := []uint64{1000, 5000, 10000, 50000, 100000}

	for i, slot := range sampleSlots {
		point := ocommon.NewPoint(slot, nil)
		block, err := immDb.GetBlock(point)
		if err != nil {
			b.Logf("Could not get block at slot %d: %v", slot, err)
			continue
		}
		if block == nil {
			continue
		}

		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			b.Logf("Could not parse block: %v", err)
			continue
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
			b.Logf("Could not create block: %v", err)
			continue
		}
	}

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
		_, err := db.Metadata().GetStakeRegistrations(stakeKey, nil)
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
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

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
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Seed database with real blocks
	immDb, err := immutable.New("../database/immutable/testdata")
	if err != nil {
		b.Fatal(err)
	}

	// Sample blocks from different slots
	sampleSlots := []uint64{1000, 5000, 10000, 50000, 100000}

	for i, slot := range sampleSlots {
		point := ocommon.NewPoint(slot, nil)
		block, err := immDb.GetBlock(point)
		if err != nil {
			b.Logf("Could not get block at slot %d: %v", slot, err)
			continue
		}
		if block == nil {
			continue
		}

		ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			b.Logf("Could not parse block: %v", err)
			continue
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
			b.Logf("Could not create block: %v", err)
			continue
		}
	}

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
	immDb, err := immutable.New("../database/immutable/testdata")
	if err != nil {
		b.Fatal(err)
	}

	// Sample blocks from different eras by trying various slots
	// This will naturally include era transitions as we process blocks sequentially
	var blocks []*immutable.Block
	var currentEra uint = 999 // sentinel value

	// Try slots in order to get blocks from different eras
	for slot := uint64(1); slot <= 200000; slot += 1000 {
		point := ocommon.NewPoint(slot, nil)
		block, err := immDb.GetBlock(point)
		if err != nil {
			b.Logf("Could not get block at slot %d: %v", slot, err)
			continue
		}
		if block == nil {
			continue
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

// BenchmarkEraTransitionPerformanceRealData benchmarks processing blocks across era transitions with database seeding
func BenchmarkEraTransitionPerformanceRealData(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Open immutable database
	immDb, err := immutable.New("../database/immutable/testdata")
	if err != nil {
		b.Fatal(err)
	}

	// Sample blocks from different eras and seed database
	var blocks []*immutable.Block

	// Use iterator to get blocks from different eras
	originPoint := ocommon.NewPoint(0, nil)
	iterator, err := immDb.BlocksFromPoint(originPoint)
	if err != nil {
		b.Fatal(err)
	}
	defer iterator.Close()

	// Collect blocks from different parts of the chain to test era transitions
	blockCount := 0
	maxBlocks := 10 // Collect up to 10 blocks from different eras

	for blockCount < maxBlocks {
		block, err := iterator.Next()
		if err != nil {
			break
		}
		if block == nil {
			break
		}

		// Sample blocks at different intervals to get different eras
		if blockCount == 0 || blockCount == 3 || blockCount == 6 ||
			blockCount == 9 {
			blocks = append(blocks, block)

			// Also seed the database with this block
			ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
			if err != nil {
				b.Logf("Could not parse block: %v", err)
				continue
			}

			blockModel := models.Block{
				ID:       uint64(len(blocks)),
				Slot:     block.Slot,
				Hash:     block.Hash,
				Number:   0,
				Type:     uint(ledgerBlock.Type()),
				PrevHash: ledgerBlock.PrevHash().Bytes(),
				Cbor:     ledgerBlock.Cbor(),
			}

			if err := db.BlockCreate(blockModel, nil); err != nil {
				b.Logf("Could not create block: %v", err)
				continue
			}
		}

		blockCount++
		if blockCount >= 1000 { // Safety limit
			break
		}
	}

	if len(blocks) == 0 {
		b.Skip("No blocks found in test data")
	}

	// Reset timer after seeding
	b.ResetTimer()

	// Benchmark processing blocks across era transitions with database operations
	for b.Loop() {
		for _, block := range blocks {
			ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
			if err != nil {
				b.Fatal(err)
			}

			// Simulate block processing with database operations
			_ = ledgerBlock.Hash()
			_ = ledgerBlock.PrevHash()
			_ = ledgerBlock.Type()

			// Additional database operations that might happen during processing
			// GetPParams returns empty slice for missing params, not an error
			res, err := db.Metadata().GetPParams(1, nil)
			_ = res
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkIndexBuildingTime benchmarks the time to build indexes for new blocks
func BenchmarkIndexBuildingTime(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

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

	// Reset timer after setup
	b.ResetTimer()

	// Benchmark reading real blocks (sample a few different slots)
	testSlots := []uint64{
		tip.Slot - 4,
		tip.Slot - 3,
		tip.Slot - 2,
		tip.Slot - 1,
		tip.Slot,
	} // Last 5 blocks
	for i := 0; b.Loop(); i++ {
		slot := testSlots[i%len(testSlots)]
		point := ocommon.NewPoint(slot, nil) // nil hash means get by slot
		_, err := immDb.GetBlock(point)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRealBlockProcessing benchmarks end-to-end processing of real Cardano blocks
func BenchmarkRealBlockProcessing(b *testing.B) {
	// Set up in-memory database
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

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
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

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
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

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
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

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
	b.Cleanup(func() { db.Close() })

	b.Logf("Loaded %d blocks for throughput testing", len(blocks))

	b.ResetTimer()

	processedBlocks := 0
	blockIdx := 0
	for i := 0; b.Loop(); i++ {
		if blockIdx == len(blocks) {
			b.StopTimer()
			if err := db.Close(); err != nil {
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
	b.Cleanup(func() { db.Close() })

	b.Logf("Loaded %d blocks for near-tip blockfetch testing", len(blocks))

	b.ResetTimer()

	processedBlocks := 0
	blockIdx := 0
	for i := 0; b.Loop(); i++ {
		if blockIdx == len(blocks) {
			b.StopTimer()
			if err := db.Close(); err != nil {
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

		if err := ledgerState.handleEventBlockfetchBlock(evt); err != nil {
			b.Fatalf("handleEventBlockfetchBlock failed: %v", err)
		}
		// Flush after each block to simulate near-tip behavior where
		// blocks are committed individually rather than batched.
		if err := ledgerState.flushPendingBlockfetchBlocks(); err != nil {
			b.Fatalf("flushPendingBlockfetchBlocks failed: %v", err)
		}

		processedBlocks++
	}

	b.ReportMetric(float64(processedBlocks)/b.Elapsed().Seconds(), "blocks/sec")
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
	b.Cleanup(func() { db.Close() })

	b.Logf("Loaded %d predecoded blocks for throughput testing", len(blocks))

	b.ResetTimer()

	processedBlocks := 0
	blockIdx := 0
	for i := 0; b.Loop(); i++ {
		if blockIdx == len(blocks) {
			b.StopTimer()
			if err := db.Close(); err != nil {
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
			_ = db.Close()
			b.Fatal(err)
		}
		b.StopTimer()
		if err := db.Close(); err != nil {
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
			_ = db.Close()
			b.Fatal(err)
		}
		b.StopTimer()
		if err := db.Close(); err != nil {
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
	const blockCount = 100

	seedModels := make([]models.Block, 0, seedCount)
	for len(seedModels) < seedCount {
		block, err := iterator.Next()
		if err != nil {
			b.Fatal(err)
		}
		if block == nil {
			b.Skip("insufficient blocks available for throughput benchmark seed")
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

	blocks := make([]*immutable.Block, 0, blockCount)
	for len(blocks) < blockCount {
		block, err := iterator.Next()
		if err != nil {
			b.Fatal(err)
		}
		if block == nil {
			b.Skip("insufficient blocks available for throughput benchmark")
		}
		tmpBlock := &immutable.Block{
			Slot: block.Slot,
			Type: block.Type,
			Cbor: slices.Clone(block.Cbor),
			Hash: slices.Clone(block.Hash),
		}
		blocks = append(blocks, tmpBlock)
	}

	return seedModels, blocks
}

func newBatchBenchmarkLedgerState(
	b *testing.B,
	seedModels []models.Block,
) (*database.Database, *LedgerState) {
	b.Helper()

	db, err := database.New(&database.Config{DataDir: ""})
	if err != nil {
		b.Fatal(err)
	}
	for _, blockModel := range seedModels {
		tmpModel := blockModel
		if err := db.BlockCreate(tmpModel, nil); err != nil {
			_ = db.Close()
			b.Fatalf("seed batch benchmark block: %v", err)
		}
	}
	chainManager, err := chain.NewManager(db, nil)
	if err != nil {
		_ = db.Close()
		b.Fatal(err)
	}
	ledgerState, err := NewLedgerState(LedgerStateConfig{
		Database:     db,
		ChainManager: chainManager,
	})
	if err != nil {
		_ = db.Close()
		b.Fatal(err)
	}
	return db, ledgerState
}

func newBlockProcessingBenchmarkLedgerState(
	b *testing.B,
	seedModels []models.Block,
) (*database.Database, *LedgerState) {
	b.Helper()
	return newBatchBenchmarkLedgerState(b, seedModels)
}

// BenchmarkConcurrentQueries measures database performance under concurrent query load
// using real Cardano testnet data - simulates multiple clients querying simultaneously
func BenchmarkConcurrentQueries(b *testing.B) {
	// Set up database with real data
	config := &database.Config{
		DataDir: "", // in-memory
	}
	db, err := database.New(config)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Open immutable database
	immDb := openImmutableTestDB(b)

	// Seed database with substantial real data (blocks starting from slot 5)
	seeded := seedBlocksFromSlots(
		b,
		db,
		immDb,
		[]uint64{5, 6, 7, 8, 9, 10, 11, 12, 13, 14},
	)
	b.Logf("Seeded %d blocks for concurrent queries benchmark", seeded)

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

	// Run benchmark with concurrent queries
	b.RunParallel(func(pb *testing.PB) {
		workerID := 0
		for pb.Next() {
			queryType := queryTypes[workerID%len(queryTypes)]

			switch queryType {
			case "utxo_address":
				// Query UTxO by address (using test address)
				paymentKey := make([]byte, 28) // dummy 28-byte key hash
				stakeKey := make([]byte, 28)
				testAddr, err := ledger.NewAddressFromParts(
					0,
					0,
					paymentKey,
					stakeKey,
				)
				if err != nil {
					b.Fatal(err)
				}
				res, err := db.UtxosByAddress(testAddr, nil)
				_ = res
				_ = err // Ignore errors for benchmark

			case "utxo_ref":
				// Query UTxO by reference (using test hash)
				testTxId := []byte(
					"abcd1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab",
				)
				_, err := db.UtxoByRef(testTxId, 0, nil)
				_ = err // Ignore errors for benchmark

			case "block_retrieval":
				// Query block by index
				_, err := db.BlockByIndex(uint64(workerID%100), nil)
				_ = err // Ignore errors for benchmark
			}

			workerID++
		}
	})

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
				if input.Id().String() == storageModeBenchmarkSkippedInputHash &&
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
	fixtureDb, err := database.New(&database.Config{
		DataDir:     "",
		Logger:      benchmarkDiscardLogger,
		StorageMode: types.StorageModeCore,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer fixtureDb.Close()
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
				ID:       uint64(len(ret.seedBlocks) + len(ret.measureBlocks) + 1),
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
				db, err := database.New(&database.Config{
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
					_ = db.Close()
					b.Fatalf("seed storage mode benchmark blocks: %v", err)
				}

				b.StartTimer()
				txCount, err := ingestStorageModeBenchmarkBlocks(
					db,
					fixture.measureBlocks,
				)

				b.StopTimer()
				closeErr := db.Close()
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

			db, err := database.New(&database.Config{
				DataDir:     "",
				Logger:      benchmarkDiscardLogger,
				StorageMode: mode,
			})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			if _, err := ingestStorageModeBenchmarkBlocks(db, fixture.seedBlocks); err != nil {
				b.Fatalf("seed steady-state storage mode benchmark blocks: %v", err)
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
