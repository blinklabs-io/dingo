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
	"github.com/blinklabs-io/dingo/event"
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

func TestChainBasic(t *testing.T) {
	testHashPrefix := "000047442c8830c700ecb099064ee1b038ed6fd254133f582e906a4bc3fd"
	testNonce := []byte{0xab, 0xcd, 0xef, 0x01}
	testBlocks := []*MockBlock{
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
	}
	eventBus := event.NewEventBus(nil)
	c, err := chain.NewChain(
		nil, // db
		eventBus,
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
