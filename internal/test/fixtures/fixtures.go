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

// Package fixtures contains small, valid protocol fixtures for Dingo tests.
package fixtures

import (
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	mockfixtures "github.com/blinklabs-io/ouroboros-mock/fixtures"
)

// GenerateConwayChain returns valid generated Conway blocks. The blocks have
// empty transaction sets and are linked through their header hashes.
func GenerateConwayChain(count int) ([]ledger.Block, error) {
	return mockfixtures.GenerateConwayChain(
		1,
		common.Blake2b256{},
		2,
		20,
		count,
	)
}

// GenerateConwayChainWithTransactions returns valid generated Conway blocks
// with transactions for tests that exercise transaction indexing.
func GenerateConwayChainWithTransactions(count int) ([]ledger.Block, error) {
	return mockfixtures.GenerateConwayChainWithTransactions(
		1,
		common.Blake2b256{},
		2,
		20,
		count,
	)
}

// GenerateConwayChainWithPeriodicTransactions returns a connected Conway
// chain with one transaction block after every emptyRun empty blocks.
func GenerateConwayChainWithPeriodicTransactions(
	count, emptyRun int,
) ([]ledger.Block, error) {
	if count <= 0 {
		return []ledger.Block{}, nil
	}
	if emptyRun < 0 {
		emptyRun = 0
	}
	blocks := make([]ledger.Block, 0, count)
	var previous common.Blake2b256
	for i := range count {
		withTransactions := i%(emptyRun+1) == emptyRun
		var block ledger.Block
		var err error
		if withTransactions {
			generated, generateErr := mockfixtures.GenerateConwayChainWithTransactions(
				uint64(i+1), previous, uint64(2+i*20), 20, 1,
			)
			err = generateErr
			if len(generated) > 0 {
				block = generated[0]
			}
		} else {
			generated, generateErr := mockfixtures.GenerateConwayChain(
				uint64(i+1), previous, uint64(2+i*20), 20, 1,
			)
			err = generateErr
			if len(generated) > 0 {
				block = generated[0]
			}
		}
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, block)
		previous = block.Hash()
	}
	return blocks, nil
}

// GenerateBabbageChain returns valid generated Babbage blocks for tests whose
// ledger configuration intentionally stops at the Babbage era.
func GenerateBabbageChain(count int) ([]ledger.Block, error) {
	return mockfixtures.GenerateBabbageChain(
		1,
		common.Blake2b256{},
		2,
		20,
		count,
	)
}

// GenerateConwayChainAt returns valid generated Conway blocks beginning at
// blockNumber and slot. It is useful when a test needs to preserve the
// numbers carried by a protocol trace while replacing malformed bytes.
func GenerateConwayChainAt(
	blockNumber, slot uint64,
	count int,
) ([]ledger.Block, error) {
	return mockfixtures.GenerateConwayChain(
		blockNumber,
		common.Blake2b256{},
		slot,
		1,
		count,
	)
}
