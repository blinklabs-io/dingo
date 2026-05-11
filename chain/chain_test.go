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
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"golang.org/x/crypto/blake2b"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
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

func mustSetLedger(t *testing.T, cm *chain.ChainManager, securityParam int) {
	t.Helper()
	if err := cm.SetLedger(&mockLedgerState{securityParam: securityParam}); err != nil {
		t.Fatalf("SetLedger(%d): %v", securityParam, err)
	}
}

func TestSetLedgerRejectsNonPositiveSecurityParam(t *testing.T) {
	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	err = cm.SetLedger(&mockLedgerState{securityParam: 0})
	if err == nil {
		t.Fatal("expected error for K=0")
	}
	if !errors.Is(err, chain.ErrInvalidSecurityParam) {
		t.Fatalf("expected ErrInvalidSecurityParam, got %v", err)
	}
	err = cm.SetLedger(&mockLedgerState{securityParam: -1})
	if err == nil {
		t.Fatal("expected error for K=-1")
	}
	if !errors.Is(err, chain.ErrInvalidSecurityParam) {
		t.Fatalf("expected ErrInvalidSecurityParam, got %v", err)
	}
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
	// K=1 yields max(2, DefaultMaxQueuedHeaders) == DefaultMaxQueuedHeaders
	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	mustSetLedger(t, cm, 1)
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
	// securityParam must be large enough that sp*2 exceeds
	// DefaultMaxQueuedHeaders, otherwise the default floor applies.
	securityParam := chain.DefaultMaxQueuedHeaders/2 + 1
	expectedLimit := securityParam * 2

	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	mustSetLedger(t, cm, securityParam)
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
	mustSetLedger(t, cm, securityParam)
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

func TestRecentPointsNoDatabase(t *testing.T) {
	// Create a chain manager with no database. Blocks are stored
	// in memory only. RecentPoints must return the in-memory
	// chain points even though there is no blob store.
	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf(
			"unexpected error creating chain manager: %s",
			err,
		)
	}
	c := cm.PrimaryChain()

	// Empty chain should return no points
	points := c.RecentPoints(10)
	if len(points) != 0 {
		t.Fatalf(
			"expected 0 points on empty chain, got %d",
			len(points),
		)
	}

	// Add all test blocks
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf(
				"unexpected error adding block to chain: %s",
				err,
			)
		}
	}

	// Request more points than exist; should get all blocks
	points = c.RecentPoints(100)
	if len(points) != len(testBlocks) {
		t.Fatalf(
			"expected %d points, got %d",
			len(testBlocks),
			len(points),
		)
	}

	// Points should be in descending order (most recent first)
	for i, p := range points {
		expectedBlock := testBlocks[len(testBlocks)-1-i]
		expectedHash := decodeHex(expectedBlock.MockHash)
		if p.Slot != expectedBlock.MockSlot {
			t.Fatalf(
				"point %d: expected slot %d, got %d",
				i,
				expectedBlock.MockSlot,
				p.Slot,
			)
		}
		if string(p.Hash) != string(expectedHash) {
			t.Fatalf(
				"point %d: expected hash %x, got %x",
				i,
				expectedHash,
				p.Hash,
			)
		}
	}

	// Request fewer points than exist; should get exactly the
	// requested count, starting from the tip
	points = c.RecentPoints(2)
	if len(points) != 2 {
		t.Fatalf("expected 2 points, got %d", len(points))
	}
	lastBlock := testBlocks[len(testBlocks)-1]
	if points[0].Slot != lastBlock.MockSlot {
		t.Fatalf(
			"first point should be tip: expected slot %d, got %d",
			lastBlock.MockSlot,
			points[0].Slot,
		)
	}
	secondLastBlock := testBlocks[len(testBlocks)-2]
	if points[1].Slot != secondLastBlock.MockSlot {
		t.Fatalf(
			"second point should be tip-1: expected slot %d, got %d",
			secondLastBlock.MockSlot,
			points[1].Slot,
		)
	}
}

func TestRecentPointsWithDatabase(t *testing.T) {
	// Create a chain manager with a real database. RecentPoints
	// should still return the correct in-memory tip even though
	// block storage goes through the blob store.
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf(
			"unexpected error creating chain manager: %s",
			err,
		)
	}
	c := cm.PrimaryChain()

	// Add all test blocks
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf(
				"unexpected error adding block to chain: %s",
				err,
			)
		}
	}

	// RecentPoints should return points in descending order
	points := c.RecentPoints(3)
	if len(points) != 3 {
		t.Fatalf("expected 3 points, got %d", len(points))
	}

	// Verify descending order by slot
	for i := range len(points) - 1 {
		if points[i].Slot <= points[i+1].Slot {
			t.Fatalf(
				"points not in descending order: "+
					"point %d (slot %d) <= point %d (slot %d)",
				i, points[i].Slot,
				i+1, points[i+1].Slot,
			)
		}
	}

	// Tip should be the first point
	tip := c.Tip()
	if points[0].Slot != tip.Point.Slot ||
		string(points[0].Hash) != string(tip.Point.Hash) {
		t.Fatalf(
			"first point should match tip: got %d.%x, wanted %d.%x",
			points[0].Slot, points[0].Hash,
			tip.Point.Slot, tip.Point.Hash,
		)
	}
}

func TestIntersectPointsIncludesOlderSamples(t *testing.T) {
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf(
			"unexpected error creating chain manager: %s",
			err,
		)
	}
	c := cm.PrimaryChain()
	headers := makeLinkedHeaders(80, 0, 1, "")
	for _, header := range headers {
		if err := c.AddBlock(header, nil); err != nil {
			t.Fatalf(
				"unexpected error adding block to chain: %s",
				err,
			)
		}
	}

	points := c.IntersectPoints(40)
	if len(points) != 35 {
		t.Fatalf("expected 35 points, got %d", len(points))
	}

	for i := range 32 {
		expected := headers[len(headers)-1-i]
		if points[i].Slot != expected.MockSlot {
			t.Fatalf(
				"dense point %d: expected slot %d, got %d",
				i,
				expected.MockSlot,
				points[i].Slot,
			)
		}
	}

	expectedOlder := []struct {
		pointIdx  int
		headerIdx int
	}{
		{pointIdx: 32, headerIdx: 47},
		{pointIdx: 33, headerIdx: 15},
		{pointIdx: 34, headerIdx: 0},
	}
	for _, expected := range expectedOlder {
		header := headers[expected.headerIdx]
		point := points[expected.pointIdx]
		if point.Slot != header.MockSlot {
			t.Fatalf(
				"older point %d: expected slot %d, got %d",
				expected.pointIdx,
				header.MockSlot,
				point.Slot,
			)
		}
		if string(point.Hash) != string(decodeHex(header.MockHash)) {
			t.Fatalf(
				"older point %d: expected hash %x, got %x",
				expected.pointIdx,
				decodeHex(header.MockHash),
				point.Hash,
			)
		}
	}
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

// generateTestChain builds `count` Conway blocks that chain together via
// PrevHash and survive a CBOR round-trip — that is, after re-decoding
// each block's stored Cbor() via ledger.NewBlockFromCbor (the path used
// by models.Block.Decode in chain.reconcile), the decoded block's
// Hash() and PrevHash() match what the generator originally produced.
//
// The first block's PrevHash is set to prevHash. Each block's slot is
// startSlot + i*slotIncrement; block numbers run startBlockNumber..+count-1.
// All blocks have empty transactions/witnesses/auxiliary/invalid sets,
// so they share the same block body hash.
func generateTestChain(
	t testing.TB,
	startBlockNumber uint64,
	prevHash common.Blake2b256,
	startSlot, slotIncrement uint64,
	count int,
) []ledger.Block {
	t.Helper()
	if count <= 0 {
		return nil
	}
	// All generated blocks have identical empty bodies, so the four
	// component CBORs and the resulting block body hash are constant.
	emptyTxsCbor, err := cbor.Encode([]ledger.ConwayTransactionBody{})
	if err != nil {
		t.Fatalf("encode empty tx bodies: %s", err)
	}
	emptyWitsCbor, err := cbor.Encode([]ledger.ConwayTransactionWitnessSet{})
	if err != nil {
		t.Fatalf("encode empty witnesses: %s", err)
	}
	emptyAuxCbor, err := cbor.Encode(common.TransactionMetadataSet{})
	if err != nil {
		t.Fatalf("encode empty metadata set: %s", err)
	}
	emptyInvalidCbor, err := cbor.Encode([]uint{})
	if err != nil {
		t.Fatalf("encode empty invalid txs: %s", err)
	}
	bodyHash := computeBlockBodyHash(
		emptyTxsCbor, emptyWitsCbor, emptyAuxCbor, emptyInvalidCbor,
	)
	blocks := make([]ledger.Block, 0, count)
	currentPrev := prevHash
	for i := range count {
		body := babbage.BabbageBlockHeaderBody{
			BlockNumber: startBlockNumber + uint64(i),
			Slot:        startSlot + uint64(i)*slotIncrement,
			PrevHash:    currentPrev,
			IssuerVkey:  common.IssuerVkey{},
			VrfKey:      make([]byte, 32),
			VrfResult: common.VrfResult{
				Output: make([]byte, 64),
				Proof:  make([]byte, 80),
			},
			BlockBodySize: 0,
			BlockBodyHash: bodyHash,
			OpCert: babbage.BabbageOpCert{
				HotVkey:   make([]byte, 32),
				Signature: make([]byte, 64),
			},
			ProtoVersion: babbage.BabbageProtoVersion{Major: 9, Minor: 0},
		}
		block := &ledger.ConwayBlock{
			BlockHeader: &ledger.ConwayBlockHeader{
				BabbageBlockHeader: ledger.BabbageBlockHeader{
					Body:      body,
					Signature: make([]byte, 64),
				},
			},
		}
		blockCbor, err := cbor.Encode(block)
		if err != nil {
			t.Fatalf("encode block %d: %s", i, err)
		}
		// Re-decode so the returned block carries the canonical Cbor()
		// the reconcile path will observe, and so Hash() reads from the
		// post-round-trip header bytes.
		decoded, err := conway.NewConwayBlockFromCbor(blockCbor)
		if err != nil {
			t.Fatalf("decode generated block %d: %s", i, err)
		}
		if !bytes.Equal(decoded.Cbor(), blockCbor) {
			t.Fatalf("block %d Cbor mismatch after round-trip", i)
		}
		blocks = append(blocks, decoded)
		currentPrev = decoded.Hash()
	}
	return blocks
}

// computeBlockBodyHash returns blake2b256(blake2b256(p[0]) || ...) which
// matches common.ValidateBlockBodyHash's expected derivation.
func computeBlockBodyHash(parts ...[]byte) common.Blake2b256 {
	var combined []byte
	for _, p := range parts {
		h := blake2b.Sum256(p)
		combined = append(combined, h[:]...)
	}
	h := blake2b.Sum256(combined)
	return common.NewBlake2b256(h[:])
}

func TestGenerateTestChainRoundTrip(t *testing.T) {
	var origin common.Blake2b256
	gen := generateTestChain(t, 1, origin, 0, 20, 5)
	if len(gen) != 5 {
		t.Fatalf("expected 5 blocks, got %d", len(gen))
	}
	for i, b := range gen {
		decoded, err := ledger.NewBlockFromCbor(uint(b.Type()), b.Cbor())
		if err != nil {
			t.Fatalf("block %d decode failed: %s", i, err)
		}
		if decoded.Hash() != b.Hash() {
			t.Fatalf(
				"block %d hash changed after round-trip: %s -> %s",
				i, b.Hash(), decoded.Hash(),
			)
		}
		if decoded.PrevHash() != b.PrevHash() {
			t.Fatalf(
				"block %d prev hash changed after round-trip: %s -> %s",
				i, b.PrevHash(), decoded.PrevHash(),
			)
		}
		if decoded.BlockNumber() != uint64(i+1) {
			t.Fatalf(
				"block %d unexpected block number %d",
				i, decoded.BlockNumber(),
			)
		}
		if decoded.SlotNumber() != uint64(i)*20 {
			t.Fatalf(
				"block %d unexpected slot %d",
				i, decoded.SlotNumber(),
			)
		}
	}
	for i := 1; i < len(gen); i++ {
		if gen[i].PrevHash() != gen[i-1].Hash() {
			t.Fatalf(
				"chain link mismatch at index %d: prev=%s, want=%s",
				i, gen[i].PrevHash(), gen[i-1].Hash(),
			)
		}
	}
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
	mustSetLedger(t, cm, 2)
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
	mustSetLedger(t, cm, 3)
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

func TestRewindPrimaryChainToPointPrunesPersistentTail(t *testing.T) {
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf(
			"unexpected error creating chain manager: %s",
			err,
		)
	}
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf(
				"unexpected error adding block to chain: %s",
				err,
			)
		}
	}
	rewindBlock := testBlocks[2]
	rewindPoint := ocommon.Point{
		Slot: rewindBlock.SlotNumber(),
		Hash: rewindBlock.Hash().Bytes(),
	}
	if err := cm.RewindPrimaryChainToPoint(rewindPoint); err != nil {
		t.Fatalf(
			"unexpected error rewinding primary chain: %s",
			err,
		)
	}
	tip := c.Tip()
	if tip.Point.Slot != rewindPoint.Slot ||
		string(tip.Point.Hash) != string(rewindPoint.Hash) {
		t.Fatalf(
			"chain tip should match rewind point: got %d.%x, wanted %d.%x",
			tip.Point.Slot,
			tip.Point.Hash,
			rewindPoint.Slot,
			rewindPoint.Hash,
		)
	}
	for idx := uint64(1); idx <= 3; idx++ {
		if _, err := db.BlockByIndex(idx, nil); err != nil {
			t.Fatalf(
				"expected block index %d to remain after rewind: %s",
				idx,
				err,
			)
		}
	}
	for idx := uint64(4); idx <= 6; idx++ {
		if _, err := db.BlockByIndex(idx, nil); !errors.Is(err, models.ErrBlockNotFound) {
			t.Fatalf(
				"expected block index %d to be pruned after rewind, got: %v",
				idx,
				err,
			)
		}
	}
}

func TestChainRollbackRequiresSecurityParamConfigured(t *testing.T) {
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf(
			"unexpected error creating chain manager: %s",
			err,
		)
	}
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf(
				"unexpected error adding block to chain: %s",
				err,
			)
		}
	}
	rollbackPoint := ocommon.Point{
		Slot: testBlocks[0].SlotNumber(),
		Hash: testBlocks[0].Hash().Bytes(),
	}
	err = c.Rollback(rollbackPoint)
	if err == nil {
		t.Fatal("expected error when security parameter K is not configured")
	}
	if !errors.Is(err, chain.ErrSecurityParamNotConfigured) {
		t.Fatalf("expected ErrSecurityParamNotConfigured, got: %v", err)
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
	mustSetLedger(t, cm, 1)
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

// TestChainIterateNonPrimaryAfterPrimaryRollbackPastFork exercises the
// reconcile path on a non-primary chain when the primary chain has
// rolled back past the fork point.
//
// Setup:
//   - Primary chain has 6 blocks (block numbers 1..6, slots 0..100).
//   - Non-primary chain forks at primary block 3 and extends with three
//     divergent blocks F4', F5', F6' (block numbers 4..6, slots 60..100).
//   - Primary rolls back to block 2 (drops blocks 3..6, depth 4 with K=5).
//
// Expectation: iterating the non-primary chain from origin returns
// primary blocks 1..3 followed by F4', F5', F6'. Reconcile must walk
// back from the in-memory fork blocks through the rolled-back ancestor
// retained in the LRU cache to re-anchor the fork against the shorter
// primary chain.
func TestChainIterateNonPrimaryAfterPrimaryRollbackPastFork(t *testing.T) {
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	// K=5 allows rollback depth 4 (primary tip 6 -> rollback to 2).
	mustSetLedger(t, cm, 5)
	primaryChain := cm.PrimaryChain()

	var origin common.Blake2b256
	primaryBlocks := generateTestChain(t, 1, origin, 0, 20, 6)
	for i, b := range primaryBlocks {
		if err := primaryChain.AddBlock(b, nil); err != nil {
			t.Fatalf("AddBlock primary[%d]: %s", i, err)
		}
	}

	const forkIdx = 2 // primary block 3 (0-based index 2)
	forkPoint := ocommon.Point{
		Slot: primaryBlocks[forkIdx].SlotNumber(),
		Hash: primaryBlocks[forkIdx].Hash().Bytes(),
	}
	forkChain, err := cm.NewChainFromIntersect(
		[]ocommon.Point{forkPoint},
	)
	if err != nil {
		t.Fatalf("NewChainFromIntersect: %s", err)
	}

	forkBlocks := generateTestChain(
		t,
		uint64(forkIdx+2), // block number 4
		primaryBlocks[forkIdx].Hash(),
		primaryBlocks[forkIdx].SlotNumber()+20,
		20,
		3,
	)
	for i, b := range forkBlocks {
		if err := forkChain.AddBlock(b, nil); err != nil {
			t.Fatalf("AddBlock fork[%d]: %s", i, err)
		}
	}

	rollbackPoint := ocommon.Point{
		Slot: primaryBlocks[1].SlotNumber(),
		Hash: primaryBlocks[1].Hash().Bytes(),
	}
	if err := primaryChain.Rollback(rollbackPoint); err != nil {
		t.Fatalf("Rollback primary: %s", err)
	}

	iter, err := forkChain.FromPoint(ocommon.NewPointOrigin(), false)
	if err != nil {
		t.Fatalf("FromPoint: %s", err)
	}
	expected := append(
		[]ledger.Block{},
		primaryBlocks[0],
		primaryBlocks[1],
		primaryBlocks[2],
	)
	expected = append(expected, forkBlocks...)
	for i, want := range expected {
		next, err := iter.Next(false)
		if err != nil {
			t.Fatalf("iter.Next at idx %d: %s", i, err)
		}
		if next == nil {
			t.Fatalf("iter.Next at idx %d returned nil", i)
		}
		if next.Rollback {
			t.Fatalf("iter.Next at idx %d unexpected rollback", i)
		}
		if next.Block.Number != want.BlockNumber() {
			t.Fatalf(
				"idx %d block number: got %d, want %d",
				i, next.Block.Number, want.BlockNumber(),
			)
		}
		if !bytes.Equal(next.Block.Hash, want.Hash().Bytes()) {
			t.Fatalf(
				"idx %d block hash: got %x, want %x",
				i, next.Block.Hash, want.Hash().Bytes(),
			)
		}
	}
	if _, err := iter.Next(false); !errors.Is(err, chain.ErrIteratorChainTip) {
		t.Fatalf("expected ErrIteratorChainTip at fork tip, got: %v", err)
	}
}

// TestChainRollbackNonPrimaryAfterPrimaryRollback covers the case where
// the non-primary chain is itself rolled back after the primary has
// already rolled back past the fork point.
//
// Setup matches TestChainIterateNonPrimaryAfterPrimaryRollbackPastFork:
// primary blocks 1..6, fork chain branches at primary block 3 with
// divergent blocks F4', F5', F6', primary then rolls back to block 2.
//
// The test then iterates the fork chain to its tip (driving reconcile
// under the new primary), rolls back the fork chain itself to the
// original fork point (primary block 3, retained in the LRU cache),
// and verifies:
//   - the pre-existing iterator receives a rollback signal at the fork
//     point followed by ErrIteratorChainTip;
//   - a fresh iterator from origin delivers exactly P1, P2, P3.
func TestChainRollbackNonPrimaryAfterPrimaryRollback(t *testing.T) {
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	mustSetLedger(t, cm, 5)
	primaryChain := cm.PrimaryChain()

	var origin common.Blake2b256
	primaryBlocks := generateTestChain(t, 1, origin, 0, 20, 6)
	for i, b := range primaryBlocks {
		if err := primaryChain.AddBlock(b, nil); err != nil {
			t.Fatalf("AddBlock primary[%d]: %s", i, err)
		}
	}

	const forkIdx = 2 // primary block 3
	forkPoint := ocommon.Point{
		Slot: primaryBlocks[forkIdx].SlotNumber(),
		Hash: primaryBlocks[forkIdx].Hash().Bytes(),
	}
	forkChain, err := cm.NewChainFromIntersect(
		[]ocommon.Point{forkPoint},
	)
	if err != nil {
		t.Fatalf("NewChainFromIntersect: %s", err)
	}

	forkBlocks := generateTestChain(
		t,
		uint64(forkIdx+2),
		primaryBlocks[forkIdx].Hash(),
		primaryBlocks[forkIdx].SlotNumber()+20,
		20,
		3,
	)
	for i, b := range forkBlocks {
		if err := forkChain.AddBlock(b, nil); err != nil {
			t.Fatalf("AddBlock fork[%d]: %s", i, err)
		}
	}

	rbPrimary := ocommon.Point{
		Slot: primaryBlocks[1].SlotNumber(),
		Hash: primaryBlocks[1].Hash().Bytes(),
	}
	if err := primaryChain.Rollback(rbPrimary); err != nil {
		t.Fatalf("Rollback primary: %s", err)
	}

	// Drain the fork chain to its current tip; this triggers reconcile
	// and produces a known iterator position before the fork rollback.
	iter, err := forkChain.FromPoint(ocommon.NewPointOrigin(), false)
	if err != nil {
		t.Fatalf("FromPoint: %s", err)
	}
	preRollbackExpected := append(
		[]ledger.Block{},
		primaryBlocks[0],
		primaryBlocks[1],
		primaryBlocks[2],
	)
	preRollbackExpected = append(preRollbackExpected, forkBlocks...)
	for i, want := range preRollbackExpected {
		next, err := iter.Next(false)
		if err != nil {
			t.Fatalf("iter.Next pre-rollback idx %d: %s", i, err)
		}
		if next == nil || next.Rollback {
			t.Fatalf("iter.Next pre-rollback idx %d unexpected: %+v", i, next)
		}
		if next.Block.Number != want.BlockNumber() {
			t.Fatalf(
				"pre-rollback idx %d block number: got %d want %d",
				i, next.Block.Number, want.BlockNumber(),
			)
		}
	}

	// Roll back the fork chain itself to the original fork point (a
	// primary block that the primary has already rolled back away).
	rbFork := ocommon.Point{
		Slot: primaryBlocks[forkIdx].SlotNumber(),
		Hash: primaryBlocks[forkIdx].Hash().Bytes(),
	}
	if err := forkChain.Rollback(rbFork); err != nil {
		t.Fatalf("Rollback fork: %s", err)
	}

	// The pre-existing iterator's lastPoint (F6', slot 100) is past the
	// rollback point so it should observe a rollback signal at P3
	// followed by tip.
	next, err := iter.Next(false)
	if err != nil {
		t.Fatalf("iter.Next post-rollback: %s", err)
	}
	if next == nil || !next.Rollback {
		t.Fatalf("expected rollback signal, got: %+v", next)
	}
	if next.Point.Slot != rbFork.Slot ||
		!bytes.Equal(next.Point.Hash, rbFork.Hash) {
		t.Fatalf(
			"rollback point: got slot=%d hash=%x, want slot=%d hash=%x",
			next.Point.Slot, next.Point.Hash,
			rbFork.Slot, rbFork.Hash,
		)
	}
	if _, err := iter.Next(false); !errors.Is(err, chain.ErrIteratorChainTip) {
		t.Fatalf("expected ErrIteratorChainTip after rollback signal, got: %v", err)
	}

	// A fresh iterator should now reach exactly the rolled-back fork
	// tip (P1, P2, P3) before hitting tip.
	iter2, err := forkChain.FromPoint(ocommon.NewPointOrigin(), false)
	if err != nil {
		t.Fatalf("FromPoint 2: %s", err)
	}
	postRollbackExpected := []ledger.Block{
		primaryBlocks[0], primaryBlocks[1], primaryBlocks[2],
	}
	for i, want := range postRollbackExpected {
		next, err := iter2.Next(false)
		if err != nil {
			t.Fatalf("iter2.Next idx %d: %s", i, err)
		}
		if next == nil || next.Rollback {
			t.Fatalf("iter2.Next idx %d unexpected: %+v", i, next)
		}
		if next.Block.Number != want.BlockNumber() {
			t.Fatalf(
				"iter2 idx %d block number: got %d want %d",
				i, next.Block.Number, want.BlockNumber(),
			)
		}
		if !bytes.Equal(next.Block.Hash, want.Hash().Bytes()) {
			t.Fatalf(
				"iter2 idx %d block hash: got %x want %x",
				i, next.Block.Hash, want.Hash().Bytes(),
			)
		}
	}
	if _, err := iter2.Next(false); !errors.Is(err, chain.ErrIteratorChainTip) {
		t.Fatalf("expected ErrIteratorChainTip on iter2, got: %v", err)
	}
}

// TestChainMultipleNonPrimaryChainsIndependentRollback verifies that
// two non-primary chains rooted at different points on the primary
// chain reconcile independently after the primary chain rolls back
// past both fork points, and that rolling back one fork chain has no
// effect on the other.
//
// Setup:
//   - Primary chain: 8 blocks (P1..P8, slots 0..140).
//   - Fork A: branches at P3 with divergent A4', A5', A6'.
//   - Fork B: branches at P5 with divergent B6', B7', B8'.
//   - Primary rolls back to P2 (depth 6 with K=6).
//
// After rollback, iterating each fork from origin must deliver the
// retained primary prefix plus that fork's divergent tail. Rolling
// back fork A then leaves fork B's view unchanged.
func TestChainMultipleNonPrimaryChainsIndependentRollback(t *testing.T) {
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	mustSetLedger(t, cm, 6)
	primaryChain := cm.PrimaryChain()

	var origin common.Blake2b256
	primaryBlocks := generateTestChain(t, 1, origin, 0, 20, 8)
	for i, b := range primaryBlocks {
		if err := primaryChain.AddBlock(b, nil); err != nil {
			t.Fatalf("AddBlock primary[%d]: %s", i, err)
		}
	}

	const forkAIdx = 2 // primary block 3
	forkAPoint := ocommon.Point{
		Slot: primaryBlocks[forkAIdx].SlotNumber(),
		Hash: primaryBlocks[forkAIdx].Hash().Bytes(),
	}
	forkA, err := cm.NewChainFromIntersect(
		[]ocommon.Point{forkAPoint},
	)
	if err != nil {
		t.Fatalf("NewChainFromIntersect A: %s", err)
	}
	forkABlocks := generateTestChain(
		t,
		uint64(forkAIdx+2),
		primaryBlocks[forkAIdx].Hash(),
		primaryBlocks[forkAIdx].SlotNumber()+20,
		20,
		3,
	)
	for i, b := range forkABlocks {
		if err := forkA.AddBlock(b, nil); err != nil {
			t.Fatalf("AddBlock forkA[%d]: %s", i, err)
		}
	}

	const forkBIdx = 4 // primary block 5
	forkBPoint := ocommon.Point{
		Slot: primaryBlocks[forkBIdx].SlotNumber(),
		Hash: primaryBlocks[forkBIdx].Hash().Bytes(),
	}
	forkB, err := cm.NewChainFromIntersect(
		[]ocommon.Point{forkBPoint},
	)
	if err != nil {
		t.Fatalf("NewChainFromIntersect B: %s", err)
	}
	forkBBlocks := generateTestChain(
		t,
		uint64(forkBIdx+2),
		primaryBlocks[forkBIdx].Hash(),
		primaryBlocks[forkBIdx].SlotNumber()+20,
		20,
		3,
	)
	for i, b := range forkBBlocks {
		if err := forkB.AddBlock(b, nil); err != nil {
			t.Fatalf("AddBlock forkB[%d]: %s", i, err)
		}
	}

	rbPrimary := ocommon.Point{
		Slot: primaryBlocks[1].SlotNumber(),
		Hash: primaryBlocks[1].Hash().Bytes(),
	}
	if err := primaryChain.Rollback(rbPrimary); err != nil {
		t.Fatalf("Rollback primary: %s", err)
	}

	checkSequence := func(name string, c *chain.Chain, expected []ledger.Block) {
		t.Helper()
		iter, err := c.FromPoint(ocommon.NewPointOrigin(), false)
		if err != nil {
			t.Fatalf("%s FromPoint: %s", name, err)
		}
		for i, want := range expected {
			next, err := iter.Next(false)
			if err != nil {
				t.Fatalf("%s iter.Next idx %d: %s", name, i, err)
			}
			if next == nil || next.Rollback {
				t.Fatalf(
					"%s iter.Next idx %d unexpected: %+v",
					name, i, next,
				)
			}
			if next.Block.Number != want.BlockNumber() {
				t.Fatalf(
					"%s idx %d block number: got %d want %d",
					name, i, next.Block.Number, want.BlockNumber(),
				)
			}
			if !bytes.Equal(next.Block.Hash, want.Hash().Bytes()) {
				t.Fatalf(
					"%s idx %d block hash: got %x want %x",
					name, i, next.Block.Hash, want.Hash().Bytes(),
				)
			}
		}
		if _, err := iter.Next(false); !errors.Is(err, chain.ErrIteratorChainTip) {
			t.Fatalf(
				"%s expected ErrIteratorChainTip, got: %v",
				name, err,
			)
		}
	}

	expectedA := append(
		[]ledger.Block{},
		primaryBlocks[0],
		primaryBlocks[1],
		primaryBlocks[2],
	)
	expectedA = append(expectedA, forkABlocks...)
	checkSequence("forkA initial", forkA, expectedA)

	expectedB := append(
		[]ledger.Block{},
		primaryBlocks[0],
		primaryBlocks[1],
		primaryBlocks[2],
		primaryBlocks[3],
		primaryBlocks[4],
	)
	expectedB = append(expectedB, forkBBlocks...)
	checkSequence("forkB initial", forkB, expectedB)

	// Roll back fork A to its first divergent block; fork B must stay
	// unaffected.
	rbForkA := ocommon.Point{
		Slot: forkABlocks[0].SlotNumber(),
		Hash: forkABlocks[0].Hash().Bytes(),
	}
	if err := forkA.Rollback(rbForkA); err != nil {
		t.Fatalf("Rollback forkA: %s", err)
	}
	expectedAAfter := []ledger.Block{
		primaryBlocks[0],
		primaryBlocks[1],
		primaryBlocks[2],
		forkABlocks[0],
	}
	checkSequence("forkA after rollback", forkA, expectedAAfter)
	checkSequence("forkB unaffected", forkB, expectedB)
}
