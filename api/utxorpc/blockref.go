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

package utxorpc

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	sync "github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync"
)

// blockRef is an internal helper representation that can be adapted to
// different protobuf BlockRef / ChainPoint types.
type blockRef struct {
	Slot   uint64
	Hash   []byte
	Height uint64
}

// blockRefFromModel builds a blockRef from a database block model.
// Height is derived from the block number.
func blockRefFromModel(b models.Block) blockRef {
	return blockRef{
		Slot:   b.Slot,
		Hash:   b.Hash,
		Height: b.Number,
	}
}

// anyChainBlockFromModel decodes a stored block via gouroboros ledger.Block and
// ledger.Block.Utxorpc, then attaches raw CBOR as NativeBytes.
func anyChainBlockFromModel(block models.Block) (*sync.AnyChainBlock, error) {
	ret, err := block.Decode()
	if err != nil {
		return nil, err
	}
	tmpBlock, err := ret.Utxorpc()
	if err != nil {
		return nil, fmt.Errorf("convert block: %w", err)
	}
	var acb sync.AnyChainBlock
	acb.Chain = &sync.AnyChainBlock_Cardano{Cardano: tmpBlock}
	acb.NativeBytes = block.Cbor
	return &acb, nil
}

// syncBlockRefFromModel builds sync.BlockRef for DumpHistory next_token cursors.
// Hash is copied so later mutations to the model slice do not affect the proto.
func syncBlockRefFromModel(b models.Block) *sync.BlockRef {
	r := blockRefFromModel(b)
	return &sync.BlockRef{
		Slot:   r.Slot,
		Hash:   append([]byte(nil), r.Hash...),
		Height: r.Height,
	}
}
