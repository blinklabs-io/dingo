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

package chain_test

import (
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func decodeHex(hexData string) []byte {
	data, _ := hex.DecodeString(hexData)
	return data
}

type MockBlock struct {
	ledger.ConwayBlock
	MockHash        string
	MockSlot        uint64
	MockBlockNumber uint64
	MockPrevHash    string
}

func (b *MockBlock) Hash() common.Blake2b256 {
	hashBytes, err := hex.DecodeString(b.MockHash)
	if err != nil {
		panic("failed decoding hex: " + err.Error())
	}
	return common.NewBlake2b256(hashBytes)
}

func (b *MockBlock) PrevHash() common.Blake2b256 {
	prevHashBytes, err := hex.DecodeString(b.MockPrevHash)
	if err != nil {
		panic("failed decoding hex: " + err.Error())
	}
	return common.NewBlake2b256(prevHashBytes)
}

func (b *MockBlock) SlotNumber() uint64 {
	return b.MockSlot
}

func (b *MockBlock) BlockNumber() uint64 {
	return b.MockBlockNumber
}

var (
	// Mock hash prefix used when building mock hashes in test blocks below
	testHashPrefix = "000047442c8830c700ecb099064ee1b038ed6fd254133f582e906a4bc3fd"
	// Mock blocks
	testBlocks = []*MockBlock{
		{
			MockBlockNumber: 1,
			MockSlot:        0,
			MockHash:        testHashPrefix + "0001",
		},
		{
			MockBlockNumber: 2,
			MockSlot:        20,
			MockHash:        testHashPrefix + "0002",
			MockPrevHash:    testHashPrefix + "0001",
		},
		{
			MockBlockNumber: 3,
			MockSlot:        40,
			MockHash:        testHashPrefix + "0003",
			MockPrevHash:    testHashPrefix + "0002",
		},
		{
			MockBlockNumber: 4,
			MockSlot:        60,
			MockHash:        testHashPrefix + "0004",
			MockPrevHash:    testHashPrefix + "0003",
		},
		{
			MockBlockNumber: 5,
			MockSlot:        80,
			MockHash:        testHashPrefix + "0005",
			MockPrevHash:    testHashPrefix + "0004",
		},
		{
			MockBlockNumber: 6,
			MockSlot:        100,
			MockHash:        testHashPrefix + "0006",
			MockPrevHash:    testHashPrefix + "0005",
		},
	}
	dbConfig = &database.Config{
		Logger:         nil,
		PromRegistry:   nil,
		DataDir:        "",
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	}
)

func TestChainBasic(t *testing.T) {
	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	iter, err := c.FromPoint(ocommon.NewPointOrigin(), false)
	if err != nil {
		t.Fatalf("unexpected error creating chain iterator: %s", err)
	}
	// Iterate until hitting chain tip, and make sure we get blocks in the correct order with
	// all expected data
	testBlockIdx := 0
	for {
		next, err := iter.Next(false)
		if err != nil {
			if errors.Is(err, chain.ErrIteratorChainTip) {
				if testBlockIdx < len(testBlocks)-1 {
					t.Fatal("encountered chain tip before we expected to")
				}
				break
			}
			t.Fatalf(
				"unexpected error getting next block from chain iterator: %s",
				err,
			)
		}
		if next == nil {
			t.Fatal("unexpected nil result from chain iterator")
		}
		if testBlockIdx >= len(testBlocks) {
			t.Fatal("ran out of test blocks before reaching chain tip")
		}
		testBlock := testBlocks[testBlockIdx]
		if next.Rollback {
			t.Fatalf("unexpected rollback from chain iterator")
		}
		nextBlock := next.Block
		if nextBlock.ID != uint64(testBlockIdx+1) {
			t.Fatalf(
				"did not get expected block from iterator: got index %d, expected %d",
				nextBlock.ID,
				testBlockIdx+1,
			)
		}
		nextHashHex := hex.EncodeToString(nextBlock.Hash)
		if nextHashHex != testBlock.MockHash {
			t.Fatalf(
				"did not get expected block from iterator: got hash %s, expected %s",
				nextHashHex,
				testBlock.MockHash,
			)
		}
		if testBlock.MockPrevHash != "" {
			nextPrevHashHex := hex.EncodeToString(nextBlock.PrevHash)
			if nextPrevHashHex != testBlock.MockPrevHash {
				t.Fatalf(
					"did not get expected block from iterator: got prev hash %s, expected %s",
					nextPrevHashHex,
					testBlock.MockPrevHash,
				)
			}
		}
		if nextBlock.Slot != testBlock.MockSlot {
			t.Fatalf(
				"did not get expected block from iterator: got slot %d, expected %d",
				nextBlock.Slot,
				testBlock.MockSlot,
			)
		}
		if nextBlock.Number != testBlock.MockBlockNumber {
			t.Fatalf(
				"did not get expected block from iterator: got block number %d, expected %d",
				nextBlock.Number,
				testBlock.MockBlockNumber,
			)
		}
		nextPoint := next.Point
		if nextPoint.Slot != nextBlock.Slot {
			t.Fatalf(
				"did not get expected point from iterator: got slot %d, expected %d",
				nextPoint.Slot,
				nextBlock.Slot,
			)
		}
		if string(nextPoint.Hash) != string(nextBlock.Hash) {
			t.Fatalf(
				"did not get expected point from iterator: got hash %x, expected %x",
				nextPoint.Hash,
				nextBlock.Hash,
			)
		}
		testBlockIdx++
	}
}

func TestChainRollback(t *testing.T) {
	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	iter, err := c.FromPoint(ocommon.NewPointOrigin(), false)
	if err != nil {
		t.Fatalf("unexpected error creating chain iterator: %s", err)
	}
	// Iterate until hitting chain tip, and make sure we get blocks in the correct order
	testBlockIdx := 0
	for {
		next, err := iter.Next(false)
		if err != nil {
			if errors.Is(err, chain.ErrIteratorChainTip) {
				if testBlockIdx < len(testBlocks)-1 {
					t.Fatal("encountered chain tip before we expected to")
				}
				break
			}
			t.Fatalf(
				"unexpected error getting next block from chain iterator: %s",
				err,
			)
		}
		if next == nil {
			t.Fatal("unexpected nil result from chain iterator")
		}
		if testBlockIdx >= len(testBlocks) {
			t.Fatal("ran out of test blocks before reaching chain tip")
		}
		testBlock := testBlocks[testBlockIdx]
		if next.Rollback {
			t.Fatalf("unexpected rollback from chain iterator")
		}
		nextBlock := next.Block
		nextHashHex := hex.EncodeToString(nextBlock.Hash)
		if nextHashHex != testBlock.MockHash {
			t.Fatalf(
				"did not get expected block from iterator: got hash %s, expected %s",
				nextHashHex,
				testBlock.MockHash,
			)
		}
		testBlockIdx++
	}
	// Rollback to specific test block point
	testRollbackIdx := 2
	testRollbackBlock := testBlocks[testRollbackIdx]
	testRollbackPoint := ocommon.Point{
		Slot: testRollbackBlock.SlotNumber(),
		Hash: testRollbackBlock.Hash().Bytes(),
	}
	if err := c.Rollback(testRollbackPoint); err != nil {
		t.Fatalf("unexpected error while rolling back chain: %s", err)
	}
	// Compare chain iterator tip to test rollback point
	chainTip := c.Tip()
	if chainTip.Point.Slot != testRollbackPoint.Slot ||
		string(chainTip.Point.Hash) != string(testRollbackPoint.Hash) {
		t.Fatalf(
			"chain tip does not match expected point after rollback: got %d.%x, wanted %d.%x",
			chainTip.Point.Slot,
			chainTip.Point.Hash,
			testRollbackPoint.Slot,
			testRollbackPoint.Hash,
		)
	}
	// The chain iterator should give us a rollback
	next, err := iter.Next(false)
	if err != nil {
		t.Fatalf("unexpected error calling chain iterator next: %s", err)
	}
	if next == nil {
		t.Fatal("unexpected nil result from chain iterator")
	}
	if !next.Rollback {
		t.Fatalf(
			"did not get expected rollback from chain iterator: got %#v",
			next,
		)
	}
	if next.Point.Slot != testRollbackPoint.Slot ||
		string(next.Point.Hash) != string(testRollbackPoint.Hash) {
		t.Fatalf(
			"chain iterator rollback does not match expected point after rollback: got %d.%x, wanted %d.%x",
			next.Point.Slot,
			next.Point.Hash,
			testRollbackPoint.Slot,
			testRollbackPoint.Hash,
		)
	}
}

func TestChainHeaderRange(t *testing.T) {
	testBlockCount := 3
	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	c := cm.PrimaryChain()
	// Add blocks
	for _, testBlock := range testBlocks[0:testBlockCount] {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	// Add headers
	for _, testBlock := range testBlocks[testBlockCount:] {
		if err := c.AddBlockHeader(testBlock); err != nil {
			t.Fatalf("unexpected error adding header to chain: %s", err)
		}
	}
	// Compare header range
	start, end := c.HeaderRange(1000)
	testStartBlock := testBlocks[testBlockCount]
	if start.Slot != testStartBlock.SlotNumber() ||
		string(start.Hash) != string(testStartBlock.Hash().Bytes()) {
		t.Fatalf(
			"did not get expected start point: got %d.%x, wanted %d.%s",
			start.Slot,
			start.Hash,
			testStartBlock.SlotNumber(),
			testStartBlock.Hash().String(),
		)
	}
	testEndBlock := testBlocks[len(testBlocks)-1]
	if end.Slot != testEndBlock.SlotNumber() ||
		string(end.Hash) != string(testEndBlock.Hash().Bytes()) {
		t.Fatalf(
			"did not get expected end point: got %d.%x, wanted %d.%s",
			end.Slot,
			end.Hash,
			testEndBlock.SlotNumber(),
			testEndBlock.Hash().String(),
		)
	}
}

func TestChainHeaderBlock(t *testing.T) {
	testBlockCount := 3
	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	c := cm.PrimaryChain()
	// Add blocks
	for _, testBlock := range testBlocks[0:testBlockCount] {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	// Add headers
	for _, testBlock := range testBlocks[testBlockCount:] {
		if err := c.AddBlockHeader(testBlock); err != nil {
			t.Fatalf("unexpected error adding header to chain: %s", err)
		}
	}
	// Add blocks for headers
	for _, testBlock := range testBlocks[testBlockCount:] {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding header to chain: %s", err)
		}
	}
}

func TestChainHeaderWrongBlock(t *testing.T) {
	testBlockCount := 3
	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	c := cm.PrimaryChain()
	// Add blocks
	for _, testBlock := range testBlocks[0:testBlockCount] {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	// Add headers
	for _, testBlock := range testBlocks[testBlockCount:] {
		if err := c.AddBlockHeader(testBlock); err != nil {
			t.Fatalf("unexpected error adding header to chain: %s", err)
		}
	}
	// Add wrong next blocks for headers
	testFirstHeader := testBlocks[testBlockCount]
	testWrongBlock := testBlocks[testBlockCount-1]
	testExpectedErr := chain.NewBlockNotMatchHeaderError(
		testWrongBlock.Hash().String(),
		testFirstHeader.Hash().String(),
	)
	err = c.AddBlock(testWrongBlock, nil)
	if err == nil {
		t.Fatalf(
			"AddBlock should fail when adding block that doesn't match first header",
		)
	}
	if !errors.Is(err, testExpectedErr) {
		t.Fatalf(
			"did not get expected error: got %#v but wanted %#v",
			err,
			testExpectedErr,
		)
	}
}

func TestChainHeaderRollback(t *testing.T) {
	testBlockCount := 3
	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	c := cm.PrimaryChain()
	// Add blocks
	for _, testBlock := range testBlocks[0:testBlockCount] {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	// Add headers
	for _, testBlock := range testBlocks[testBlockCount:] {
		if err := c.AddBlockHeader(testBlock); err != nil {
			t.Fatalf("unexpected error adding header to chain: %s", err)
		}
	}
	// Rollback to first header point
	testFirstHeader := testBlocks[testBlockCount]
	testFirstHeaderPoint := ocommon.Point{
		Slot: testFirstHeader.SlotNumber(),
		Hash: testFirstHeader.Hash().Bytes(),
	}
	if err := c.Rollback(testFirstHeaderPoint); err != nil {
		t.Fatalf("unexpected error doing chain rollback: %s", err)
	}
	// Check header tip matches rollback point
	headerTip := c.HeaderTip()
	if headerTip.Point.Slot != testFirstHeaderPoint.Slot ||
		string(headerTip.Point.Hash) != string(testFirstHeaderPoint.Hash) {
		t.Fatalf(
			"did not get expected chain header tip after rollback: got %d.%x, wanted %d.%x",
			headerTip.Point.Slot,
			headerTip.Point.Hash,
			testFirstHeaderPoint.Slot,
			testFirstHeaderPoint.Hash,
		)
	}
}

// mockLedgerState implements the interface expected by ChainManager.SetLedger.
type mockLedgerState struct {
	securityParam int
}

func (m *mockLedgerState) SecurityParam() int {
	return m.securityParam
}

// makeLinkedHeaders builds n mock headers that chain together starting
// from prevHash at the given slot/block number offsets.
func makeLinkedHeaders(
	n int,
	startSlot uint64,
	startBlockNum uint64,
	prevHash string,
) []*MockBlock {
	headers := make([]*MockBlock, n)
	for i := range n {
		hash := fmt.Sprintf(
			"%s%04x",
			testHashPrefix,
			int(startBlockNum)+i,
		)
		headers[i] = &MockBlock{
			MockBlockNumber: startBlockNum + uint64(i),
			MockSlot:        startSlot + uint64(i)*20,
			MockHash:        hash,
			MockPrevHash:    prevHash,
		}
		prevHash = hash
	}
	return headers
}

func TestHeaderQueueLimitDefault(t *testing.T) {
	// Without securityParam the default limit applies
	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	c := cm.PrimaryChain()

	limit := chain.DefaultMaxQueuedHeaders
	// Build enough linked headers to fill the queue exactly
	headers := makeLinkedHeaders(limit+1, 0, 1, "")

	// Add headers up to the limit
	for i := range limit {
		if err := c.AddBlockHeader(headers[i]); err != nil {
			t.Fatalf(
				"unexpected error adding header %d: %s",
				i,
				err,
			)
		}
	}
	if c.HeaderCount() != limit {
		t.Fatalf(
			"expected %d headers, got %d",
			limit,
			c.HeaderCount(),
		)
	}
	// The next header must be rejected
	err = c.AddBlockHeader(headers[limit])
	if err == nil {
		t.Fatal("expected error when header queue is full")
	}
	if !errors.Is(err, chain.ErrHeaderQueueFull) {
		t.Fatalf(
			"expected ErrHeaderQueueFull, got: %s",
			err,
		)
	}
}

func TestHeaderQueueLimitFromSecurityParam(t *testing.T) {
	securityParam := 5
	expectedLimit := securityParam * 2

	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	cm.SetLedger(&mockLedgerState{securityParam: securityParam})
	c := cm.PrimaryChain()

	headers := makeLinkedHeaders(expectedLimit+1, 0, 1, "")

	// Add headers up to the limit
	for i := range expectedLimit {
		if err := c.AddBlockHeader(headers[i]); err != nil {
			t.Fatalf(
				"unexpected error adding header %d: %s",
				i,
				err,
			)
		}
	}
	if c.HeaderCount() != expectedLimit {
		t.Fatalf(
			"expected %d headers, got %d",
			expectedLimit,
			c.HeaderCount(),
		)
	}
	// The next header must be rejected
	err = c.AddBlockHeader(headers[expectedLimit])
	if err == nil {
		t.Fatal("expected error when header queue is full")
	}
	if !errors.Is(err, chain.ErrHeaderQueueFull) {
		t.Fatalf(
			"expected ErrHeaderQueueFull, got: %s",
			err,
		)
	}
}

func TestHeaderQueueAcceptsWithinLimit(t *testing.T) {
	securityParam := 10
	expectedLimit := securityParam * 2

	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	cm.SetLedger(&mockLedgerState{securityParam: securityParam})
	c := cm.PrimaryChain()

	// Add fewer headers than the limit -- all should succeed
	count := expectedLimit - 1
	headers := makeLinkedHeaders(count, 0, 1, "")
	for i, h := range headers {
		if err := c.AddBlockHeader(h); err != nil {
			t.Fatalf(
				"unexpected error adding header %d: %s",
				i,
				err,
			)
		}
	}
	if c.HeaderCount() != count {
		t.Fatalf(
			"expected %d headers, got %d",
			count,
			c.HeaderCount(),
		)
	}
}

func TestChainFromIntersect(t *testing.T) {
	testForkPointIndex := 2
	testIntersectPoints := []ocommon.Point{
		{
			Hash: decodeHex(testBlocks[testForkPointIndex].MockHash),
			Slot: testBlocks[testForkPointIndex].MockSlot,
		},
	}
	db, err := database.New(dbConfig)
	if err != nil {
		t.Fatalf("unexpected error creating database: %s", err)
	}
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	testChain, err := cm.NewChainFromIntersect(testIntersectPoints)
	if err != nil {
		t.Fatalf("unexpected error creating chain from intersect: %s", err)
	}
	testChainTip := testChain.Tip()
	if !reflect.DeepEqual(testChainTip.Point, testIntersectPoints[0]) {
		t.Fatalf(
			"did not get expected tip, got %d.%x, wanted %d.%x",
			testChainTip.Point.Slot,
			testChainTip.Point.Hash,
			testIntersectPoints[0].Slot,
			testIntersectPoints[0].Hash,
		)
	}
}

// mockLedger implements the interface{ SecurityParam() int }
// interface used by ChainManager.SetLedger.
type mockLedger struct {
	securityParam int
}

func (m *mockLedger) SecurityParam() int {
	return m.securityParam
}

// newTestDB creates an isolated database in a temporary
// directory so that tests do not share in-memory state.
func newTestDB(t *testing.T) *database.Database {
	t.Helper()
	cfg := &database.Config{
		DataDir:        t.TempDir(),
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	}
	db, err := database.New(cfg)
	if err != nil {
		t.Fatalf(
			"unexpected error creating database: %s",
			err,
		)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestChainRollbackExceedsSecurityParam(t *testing.T) {
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf(
			"unexpected error creating chain manager: %s",
			err,
		)
	}
	// Set security parameter to 2 so that rolling back
	// 3 blocks (from index 5 to index 2) exceeds it.
	cm.SetLedger(&mockLedger{securityParam: 2})
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf(
				"unexpected error adding block to chain: %s",
				err,
			)
		}
	}
	// Attempt rollback deeper than K (depth=3, K=2)
	shallowBlock := testBlocks[2]
	deepRollbackPoint := ocommon.Point{
		Slot: shallowBlock.SlotNumber(),
		Hash: shallowBlock.Hash().Bytes(),
	}
	err = c.Rollback(deepRollbackPoint)
	if err == nil {
		t.Fatal(
			"expected rollback to be rejected " +
				"when depth exceeds security param",
		)
	}
	if !errors.Is(err, chain.ErrRollbackExceedsSecurityParam) {
		t.Fatalf(
			"expected ErrRollbackExceedsSecurityParam, got: %s",
			err,
		)
	}
	// Verify the chain tip was NOT modified (rollback
	// was rejected before any state changes)
	tip := c.Tip()
	lastBlock := testBlocks[len(testBlocks)-1]
	if tip.Point.Slot != lastBlock.SlotNumber() {
		t.Fatalf(
			"chain tip should be unchanged after rejected "+
				"rollback: got slot %d, expected %d",
			tip.Point.Slot,
			lastBlock.SlotNumber(),
		)
	}
}

func TestChainRollbackWithinSecurityParam(t *testing.T) {
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf(
			"unexpected error creating chain manager: %s",
			err,
		)
	}
	// Set security parameter to 3. Rolling back 3 blocks
	// (from index 5 to index 2) should be allowed since
	// forkDepth == K is not strictly greater than K.
	cm.SetLedger(&mockLedger{securityParam: 3})
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf(
				"unexpected error adding block to chain: %s",
				err,
			)
		}
	}
	rollbackBlock := testBlocks[2]
	rollbackPoint := ocommon.Point{
		Slot: rollbackBlock.SlotNumber(),
		Hash: rollbackBlock.Hash().Bytes(),
	}
	if err := c.Rollback(rollbackPoint); err != nil {
		t.Fatalf(
			"rollback within security param should "+
				"succeed, got: %s",
			err,
		)
	}
	tip := c.Tip()
	if tip.Point.Slot != rollbackPoint.Slot ||
		string(tip.Point.Hash) != string(rollbackPoint.Hash) {
		t.Fatalf(
			"chain tip should match rollback point: "+
				"got %d.%x, wanted %d.%x",
			tip.Point.Slot,
			tip.Point.Hash,
			rollbackPoint.Slot,
			rollbackPoint.Hash,
		)
	}
}

func TestChainRollbackSecurityParamZeroAllowsAll(t *testing.T) {
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf(
			"unexpected error creating chain manager: %s",
			err,
		)
	}
	// securityParam defaults to 0 (ledger not initialized).
	// All rollbacks should be allowed.
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf(
				"unexpected error adding block to chain: %s",
				err,
			)
		}
	}
	// Roll back all the way to block index 0
	rollbackPoint := ocommon.Point{
		Slot: testBlocks[0].SlotNumber(),
		Hash: testBlocks[0].Hash().Bytes(),
	}
	if err := c.Rollback(rollbackPoint); err != nil {
		t.Fatalf(
			"rollback with securityParam=0 should "+
				"always succeed, got: %s",
			err,
		)
	}
}

func TestChainRollbackEphemeralChainNotRestricted(
	t *testing.T,
) {
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf(
			"unexpected error creating chain manager: %s",
			err,
		)
	}
	// Set a very small security param
	cm.SetLedger(&mockLedger{securityParam: 1})
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf(
				"unexpected error adding block to chain: %s",
				err,
			)
		}
	}
	// Create an ephemeral (non-persistent) fork chain
	forkPointIndex := 2
	forkPoint := ocommon.Point{
		Hash: decodeHex(
			testBlocks[forkPointIndex].MockHash,
		),
		Slot: testBlocks[forkPointIndex].MockSlot,
	}
	forkChain, err := cm.NewChainFromIntersect(
		[]ocommon.Point{forkPoint},
	)
	if err != nil {
		t.Fatalf(
			"unexpected error creating fork chain: %s",
			err,
		)
	}
	// Add blocks to the fork chain, then roll back
	forkBlocks := []*MockBlock{
		{
			MockBlockNumber: 4,
			MockSlot:        60,
			MockHash:        testHashPrefix + "00b4",
			MockPrevHash:    testHashPrefix + "0003",
		},
		{
			MockBlockNumber: 5,
			MockSlot:        80,
			MockHash:        testHashPrefix + "00b5",
			MockPrevHash:    testHashPrefix + "00b4",
		},
		{
			MockBlockNumber: 6,
			MockSlot:        100,
			MockHash:        testHashPrefix + "00b6",
			MockPrevHash:    testHashPrefix + "00b5",
		},
	}
	for _, blk := range forkBlocks {
		if err := forkChain.AddBlock(blk, nil); err != nil {
			t.Fatalf(
				"unexpected error adding block "+
					"to fork chain: %s",
				err,
			)
		}
	}
	// Roll back the ephemeral chain beyond K=1; this
	// should succeed because ephemeral chains are exempt.
	if err := forkChain.Rollback(forkPoint); err != nil {
		t.Fatalf(
			"ephemeral chain rollback should not be "+
				"restricted by security param, got: %s",
			err,
		)
	}
}

func TestChainFork(t *testing.T) {
	testForkPointIndex := 2
	testIntersectPoints := []ocommon.Point{
		{
			Hash: decodeHex(testBlocks[testForkPointIndex].MockHash),
			Slot: testBlocks[testForkPointIndex].MockSlot,
		},
	}
	testForkBlocks := []*MockBlock{
		{
			MockBlockNumber: 4,
			MockSlot:        60,
			MockHash:        testHashPrefix + "00a4",
			MockPrevHash:    testHashPrefix + "0003",
		},
		{
			MockBlockNumber: 5,
			MockSlot:        80,
			MockHash:        testHashPrefix + "00a5",
			MockPrevHash:    testHashPrefix + "00a4",
		},
		{
			MockBlockNumber: 6,
			MockSlot:        100,
			MockHash:        testHashPrefix + "00a6",
			MockPrevHash:    testHashPrefix + "00a5",
		},
	}
	db, err := database.New(dbConfig)
	if err != nil {
		t.Fatalf("unexpected error creating database: %s", err)
	}
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	testChain, err := cm.NewChainFromIntersect(testIntersectPoints)
	if err != nil {
		t.Fatalf("unexpected error creating chain from intersect: %s", err)
	}
	// Add additional blocks to forked test chain
	for _, testBlock := range testForkBlocks {
		if err := testChain.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	iter, err := testChain.FromPoint(ocommon.NewPointOrigin(), false)
	if err != nil {
		t.Fatalf("unexpected error creating chain iterator: %s", err)
	}
	// Iterate until hitting chain tip, and make sure we get blocks in the correct order with
	// all expected data
	testBlockIdx := 0
	testBlocks := slices.Concat(
		testBlocks[0:testForkPointIndex+1],
		testForkBlocks,
	)
	for {
		next, err := iter.Next(false)
		if err != nil {
			if errors.Is(err, chain.ErrIteratorChainTip) {
				if testBlockIdx < len(testBlocks)-1 {
					t.Fatal("encountered chain tip before we expected to")
				}
				break
			}
			t.Fatalf(
				"unexpected error getting next block from chain iterator: %s",
				err,
			)
		}
		if next == nil {
			t.Fatal("unexpected nil result from chain iterator")
		}
		if testBlockIdx >= len(testBlocks) {
			t.Fatal("ran out of test blocks before reaching chain tip")
		}
		testBlock := testBlocks[testBlockIdx]
		if next.Rollback {
			t.Fatalf("unexpected rollback from chain iterator")
		}
		nextBlock := next.Block
		if nextBlock.ID != uint64(testBlockIdx+1) {
			t.Fatalf(
				"did not get expected block from iterator: got index %d, expected %d",
				nextBlock.ID,
				testBlockIdx+1,
			)
		}
		nextHashHex := hex.EncodeToString(nextBlock.Hash)
		if nextHashHex != testBlock.MockHash {
			t.Fatalf(
				"did not get expected block from iterator: got hash %s, expected %s",
				nextHashHex,
				testBlock.MockHash,
			)
		}
		if testBlock.MockPrevHash != "" {
			nextPrevHashHex := hex.EncodeToString(nextBlock.PrevHash)
			if nextPrevHashHex != testBlock.MockPrevHash {
				t.Fatalf(
					"did not get expected block from iterator: got prev hash %s, expected %s",
					nextPrevHashHex,
					testBlock.MockPrevHash,
				)
			}
		}
		if nextBlock.Slot != testBlock.MockSlot {
			t.Fatalf(
				"did not get expected block from iterator: got slot %d, expected %d",
				nextBlock.Slot,
				testBlock.MockSlot,
			)
		}
		if nextBlock.Number != testBlock.MockBlockNumber {
			t.Fatalf(
				"did not get expected block from iterator: got block number %d, expected %d",
				nextBlock.Number,
				testBlock.MockBlockNumber,
			)
		}
		nextPoint := next.Point
		if nextPoint.Slot != nextBlock.Slot {
			t.Fatalf(
				"did not get expected point from iterator: got slot %d, expected %d",
				nextPoint.Slot,
				nextBlock.Slot,
			)
		}
		if string(nextPoint.Hash) != string(nextBlock.Hash) {
			t.Fatalf(
				"did not get expected point from iterator: got hash %x, expected %x",
				nextPoint.Hash,
				nextBlock.Hash,
			)
		}
		testBlockIdx++
	}
}
