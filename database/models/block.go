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

package models

import (
	"errors"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
)

var ErrBlockNotFound = errors.New("block not found")

type Block struct {
	Hash     []byte
	PrevHash []byte
	Cbor     []byte
	ID       uint64
	Slot     uint64
	Number   uint64
	Type     uint
}

// Decode decodes b.Cbor with Dingo's era-aware storage compatibility paths.
// At most one verification config is meaningful, matching
// ledger.NewBlockFromCbor's variadic convention.
func (b Block) Decode(config ...common.VerifyConfig) (ledger.Block, error) {
	return DecodeBlockCbor(b.Type, b.Cbor, config...)
}

// DecodeBlockCbor applies Dingo's era-specific compatibility decoders before
// falling back to the strict Gouroboros block decoder.
func DecodeBlockCbor(
	blockType uint,
	blockCbor []byte,
	config ...common.VerifyConfig,
) (ledger.Block, error) {
	// Conway blocks may carry the Musashi/Leios extended header; route them
	// through the Leios-aware decoder. Stored blocks can also retain the early
	// Musashi Dijkstra layout while carrying Conway's wire type.
	if blockType == ledger.BlockTypeConway {
		block, err := DecodeConwayBlock(blockCbor)
		if err == nil {
			return block, nil
		}
		if hasDijkstraLeiosShape(blockCbor) {
			if dijkstraBlock, dijkstraErr := DecodeDijkstraBlock(
				blockCbor,
				config...,
			); dijkstraErr == nil {
				return dijkstraBlock, nil
			}
		}
		return nil, err
	}
	if blockType == ledger.BlockTypeDijkstra {
		return DecodeDijkstraBlock(blockCbor, config...)
	}
	return ledger.NewBlockFromCbor(blockType, blockCbor, config...)
}
