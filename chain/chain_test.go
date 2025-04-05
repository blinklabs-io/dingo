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
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

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
	// Mock nonce value
	testNonce = []byte{0xab, 0xcd, 0xef, 0x01}
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
)

func TestChainBasic(t *testing.T) {
	c, err := chain.NewChain(
		nil,   // db
		nil,   // eventBus,
		false, // persistent
	)
	if err != nil {
		t.Fatalf("unexpected error creating chain: %s", err)
	}
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, testNonce, nil); err != nil {
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
			t.Fatalf("unexpected error getting next block from chain iterator: %s", err)
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
			t.Fatalf("did not get expected block from iterator: got index %d, expected %d", nextBlock.ID, testBlockIdx+1)
		}
		nextHashHex := hex.EncodeToString(nextBlock.Hash)
		if nextHashHex != testBlock.MockHash {
			t.Fatalf("did not get expected block from iterator: got hash %s, expected %s", nextHashHex, testBlock.MockHash)
		}
		if testBlock.MockPrevHash != "" {
			nextPrevHashHex := hex.EncodeToString(nextBlock.PrevHash)
			if nextPrevHashHex != testBlock.MockPrevHash {
				t.Fatalf("did not get expected block from iterator: got prev hash %s, expected %s", nextPrevHashHex, testBlock.MockPrevHash)
			}
		}
		if nextBlock.Slot != testBlock.MockSlot {
			t.Fatalf("did not get expected block from iterator: got slot %d, expected %d", nextBlock.Slot, testBlock.MockSlot)
		}
		if nextBlock.Number != testBlock.MockBlockNumber {
			t.Fatalf("did not get expected block from iterator: got block number %d, expected %d", nextBlock.Number, testBlock.MockBlockNumber)
		}
		nextPoint := next.Point
		if nextPoint.Slot != nextBlock.Slot {
			t.Fatalf("did not get expected point from iterator: got slot %d, expected %d", nextPoint.Slot, nextBlock.Slot)
		}
		if string(nextPoint.Hash) != string(nextBlock.Hash) {
			t.Fatalf("did not get expected point from iterator: got hash %x, expected %x", nextPoint.Hash, nextBlock.Hash)
		}
		testBlockIdx++
	}
}

func TestChainRollback(t *testing.T) {
	c, err := chain.NewChain(
		nil,   // db
		nil,   // eventBus,
		false, // persistent
	)
	if err != nil {
		t.Fatalf("unexpected error creating chain: %s", err)
	}
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, testNonce, nil); err != nil {
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
			t.Fatalf("unexpected error getting next block from chain iterator: %s", err)
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
			t.Fatalf("did not get expected block from iterator: got hash %s, expected %s", nextHashHex, testBlock.MockHash)
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
	if !next.Rollback {
		t.Fatalf("did not get expected rollback from chain iterator: got %#v", next)
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
	c, err := chain.NewChain(
		nil,   // db
		nil,   // eventBus,
		false, // persistent
	)
	if err != nil {
		t.Fatalf("unexpected error creating chain: %s", err)
	}
	// Add blocks
	for _, testBlock := range testBlocks[0:testBlockCount] {
		if err := c.AddBlock(testBlock, testNonce, nil); err != nil {
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
	c, err := chain.NewChain(
		nil,   // db
		nil,   // eventBus,
		false, // persistent
	)
	if err != nil {
		t.Fatalf("unexpected error creating chain: %s", err)
	}
	// Add blocks
	for _, testBlock := range testBlocks[0:testBlockCount] {
		if err := c.AddBlock(testBlock, testNonce, nil); err != nil {
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
		if err := c.AddBlock(testBlock, testNonce, nil); err != nil {
			t.Fatalf("unexpected error adding header to chain: %s", err)
		}
	}
}

func TestChainHeaderWrongBlock(t *testing.T) {
	testBlockCount := 3
	c, err := chain.NewChain(
		nil,   // db
		nil,   // eventBus,
		false, // persistent
	)
	if err != nil {
		t.Fatalf("unexpected error creating chain: %s", err)
	}
	// Add blocks
	for _, testBlock := range testBlocks[0:testBlockCount] {
		if err := c.AddBlock(testBlock, testNonce, nil); err != nil {
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
	err = c.AddBlock(testWrongBlock, testNonce, nil)
	if err == nil {
		t.Fatalf("AddBlock should fail when adding block that doesn't match first header")
	}
	if !errors.Is(err, testExpectedErr) {
		t.Fatalf("did not get expected error: got %#v but wanted %#v", err, testExpectedErr)
	}
}

func TestChainHeaderRollback(t *testing.T) {
	testBlockCount := 3
	c, err := chain.NewChain(
		nil,   // db
		nil,   // eventBus,
		false, // persistent
	)
	if err != nil {
		t.Fatalf("unexpected error creating chain: %s", err)
	}
	// Add blocks
	for _, testBlock := range testBlocks[0:testBlockCount] {
		if err := c.AddBlock(testBlock, testNonce, nil); err != nil {
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
