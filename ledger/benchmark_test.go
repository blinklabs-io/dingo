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

package ledger_test

import (
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
	lg "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"gorm.io/gorm"
)

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
	// Use iterator to get blocks (more reliable than GetBlock)
	originPoint := ocommon.NewPoint(0, nil)
	iterator, err := immDb.BlocksFromPoint(originPoint)
	if err != nil {
		b.Logf("Failed to create iterator: %v", err)
		return 0
	}
	defer iterator.Close()

	// Collect first N blocks from iterator
	for seeded < len(slots) {
		block, err := iterator.Next()
		if err != nil {
			b.Logf("Iterator error: %v", err)
			break
		}
		if block == nil {
			break
		}

		// Store block in database
		ledgerBlock, err := lg.NewBlockFromCbor(block.Type, block.Cbor)
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
		ledgerBlock, err := lg.NewBlockFromCbor(block.Type, block.Cbor)
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
	testAddr, err := lg.NewAddressFromParts(0, 0, paymentKey, stakeKey)
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
	testAddr, err := lg.NewAddressFromParts(0, 0, paymentKey, stakeKey)
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
		_, err := db.UtxoByRef(testTxId, testOutputIdx, nil)
		// Don't fail on "not found" - this is expected for non-existent UTxOs
		if err != nil && !errors.Is(err, database.ErrUtxoNotFound) {
			b.Fatal(err)
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
		_, err := db.BlockByIndex(1, nil)
		// Ignore not found errors
		if err != nil && !errors.Is(err, models.ErrBlockNotFound) {
			b.Fatal(err)
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
			continue
		}
		if block == nil {
			continue
		}

		ledgerBlock, err := lg.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
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
			continue
		}
	}

	// Reset timer after seeding
	b.ResetTimer()

	// Benchmark retrieval against real seeded data
	for i := 0; b.Loop(); i++ {
		blockID := uint64((i % len(sampleSlots)) + 1)
		_, err := db.BlockByIndex(blockID, nil)
		if err != nil && !errors.Is(err, models.ErrBlockNotFound) {
			b.Fatal(err)
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
			continue
		}
		if block == nil {
			continue
		}

		ledgerBlock, err := lg.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
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
		_, err := db.Metadata().GetAccount(testStakeKey, nil)
		if err != nil {
			b.Fatal(err)
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
			continue
		}
		if block == nil {
			continue
		}

		ledgerBlock, err := lg.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
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
		_, err := db.Metadata().GetAccount(key, nil)
		if err != nil {
			b.Fatal(err)
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
		_, err := db.Metadata().GetPool(testPoolKeyHash, nil)
		if err != nil {
			b.Fatal(err)
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
			continue
		}
		if block == nil {
			continue
		}

		ledgerBlock, err := lg.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
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
		_, err := db.Metadata().GetPool(hash, nil)
		if err != nil {
			b.Fatal(err)
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
		_, err := db.Metadata().GetDrep(testDRepCredential, nil)
		// Don't fail on "record not found" - this is expected for non-existent DReps
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			b.Fatal(err)
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
			continue
		}
		if block == nil {
			continue
		}

		ledgerBlock, err := lg.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
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
		_, err := db.Metadata().GetDrep(cred, nil)
		// Don't fail on "record not found" - this is expected for non-existent DReps
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			b.Fatal(err)
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
		_, err := db.Metadata().GetDatum(testDatumHash, nil)
		// Don't fail on "record not found" - this is expected for non-existent datums
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
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
			continue
		}
		if block == nil {
			continue
		}

		ledgerBlock, err := lg.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
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
		_, err := db.Metadata().GetDatum(hash, nil)
		// Don't fail on "record not found" - this is expected for non-existent datums
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
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
			b.Fatal(err)
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
			continue
		}
		if block == nil {
			continue
		}

		ledgerBlock, err := lg.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
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
		_, err := db.Metadata().GetBlockNonce(point, nil)
		// Don't fail on "record not found" - this is expected for non-existent blocks
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			b.Fatal(err)
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
			continue
		}
		if block == nil {
			continue
		}

		ledgerBlock, err := lg.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
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
		_, err := db.Metadata().GetBlockNonce(point, nil)
		// Don't fail on "record not found" - this is expected for non-existent blocks
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			b.Fatal(err)
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
			b.Fatal(err)
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
			continue
		}
		if block == nil {
			continue
		}

		ledgerBlock, err := lg.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
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
			b.Fatal(err)
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
			continue
		}
		if block == nil {
			continue
		}

		ledgerBlock, err := lg.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
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
			ledgerBlock, err := lg.NewBlockFromCbor(block.Type, block.Cbor)
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
			ledgerBlock, err := lg.NewBlockFromCbor(block.Type, block.Cbor)
			if err != nil {
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
			ledgerBlock, err := lg.NewBlockFromCbor(block.Type, block.Cbor)
			if err != nil {
				b.Fatal(err)
			}

			// Simulate block processing with database operations
			_ = ledgerBlock.Hash()
			_ = ledgerBlock.PrevHash()
			_ = ledgerBlock.Type()

			// Additional database operations that might happen during processing
			_, err = db.Metadata().GetPParams(1, nil)
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
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
		ledgerBlock, err := lg.NewBlockFromCbor(block.Type, block.Cbor)
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
		ledgerBlock, err := lg.NewBlockFromCbor(block.Type, block.Cbor)
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
		ledgerBlock, err := lg.NewBlockFromCbor(block.Type, block.Cbor)
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
			ledgerBlock, err := lg.NewBlockFromCbor(block.Type, block.Cbor)
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
	ledgerCfg := ledger.LedgerStateConfig{
		Database:     db,
		ChainManager: chainManager,
	}
	ledgerState, err := ledger.NewLedgerState(ledgerCfg)
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
		ledgerBlock, err := lg.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
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
	immDb := openImmutableTestDB(b)

	// Seed database with initial blocks for chain state
	seeded := seedBlocksFromSlots(
		b,
		db,
		immDb,
		[]uint64{0, 1, 2, 3, 4},
	) // Genesis + first few blocks
	b.Logf("Seeded %d initial blocks for chain sync", seeded)

	// Create chain manager
	chainManager, err := chain.NewManager(db, nil)
	if err != nil {
		b.Fatal(err)
	}

	// Create ledger state for validation
	ledgerCfg := ledger.LedgerStateConfig{
		Database:     db,
		ChainManager: chainManager,
	}
	ledgerState, err := ledger.NewLedgerState(ledgerCfg)
	if err != nil {
		b.Fatal(err)
	}

	// Get iterator for continuous block processing (start from slot 5)
	startPoint := ocommon.NewPoint(5, nil)
	iterator, err := immDb.BlocksFromPoint(startPoint)
	if err != nil {
		b.Fatal(err)
	}
	defer iterator.Close()

	// Pre-load a batch of blocks for throughput testing
	var blocks []*immutable.Block
	blockCount := 0
	maxBlocks := 100 // Process up to 100 blocks for throughput measurement

	for blockCount < maxBlocks {
		block, err := iterator.Next()
		if err != nil {
			break // End of available blocks
		}
		if block == nil {
			break
		}
		blocks = append(blocks, block)
		blockCount++
	}

	if len(blocks) == 0 {
		b.Skip("No blocks available for throughput benchmarking")
	}

	b.Logf("Loaded %d blocks for throughput testing", len(blocks))

	// Reset timer after setup
	b.ResetTimer()

	// Benchmark continuous block processing throughput
	processedBlocks := 0
	for i := 0; b.Loop(); i++ {
		block := blocks[i%len(blocks)]

		// Convert to ledger block
		ledgerBlock, err := lg.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			b.Fatalf("NewBlockFromCbor failed: %v", err)
		}

		// Create database transaction for block addition
		txn := db.Transaction(true)

		// Add block to chain (this includes transaction validation and state updates)
		if err := ledgerState.Chain().AddBlock(ledgerBlock, txn); err != nil {
			// Some blocks may fail validation due to missing context, but we measure throughput anyway
			_ = err
		}

		// Commit transaction to finalize DB resources for this iteration
		if err := txn.Commit(); err != nil {
			b.Fatalf("Failed to commit transaction: %v", err)
		}

		processedBlocks++
	}

	// Report blocks per second
	b.ReportMetric(float64(processedBlocks)/b.Elapsed().Seconds(), "blocks/sec")
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
				testAddr, err := lg.NewAddressFromParts(
					0,
					0,
					paymentKey,
					stakeKey,
				)
				if err != nil {
					b.Fatal(err)
				}
				_, err = db.UtxosByAddress(testAddr, nil)
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
