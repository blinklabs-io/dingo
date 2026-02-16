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

package chain

import (
	"errors"
	"fmt"
)

// DefaultMaxQueuedHeaders is the fallback header queue limit when the
// security parameter is not yet available (e.g. before ledger is wired).
const DefaultMaxQueuedHeaders = 10_000

var (
	ErrIntersectNotFound            = errors.New("chain intersect not found")
	ErrRollbackBeyondEphemeralChain = errors.New(
		"cannot rollback ephemeral chain beyond memory buffer",
	)
	ErrRollbackExceedsSecurityParam = errors.New(
		"rollback depth exceeds security parameter K",
	)
	ErrIteratorChainTip = errors.New(
		"chain iterator is at chain tip",
	)
	ErrHeaderQueueFull = errors.New(
		"header queue at maximum capacity",
	)
)

type BlockNotFitChainTipError struct {
	blockHash     string
	blockPrevHash string
	tipHash       string
}

func NewBlockNotFitChainTipError(
	blockHash string,
	blockPrevHash string,
	tipHash string,
) BlockNotFitChainTipError {
	return BlockNotFitChainTipError{
		blockHash:     blockHash,
		blockPrevHash: blockPrevHash,
		tipHash:       tipHash,
	}
}

func (e BlockNotFitChainTipError) BlockHash() string {
	return e.blockHash
}

func (e BlockNotFitChainTipError) BlockPrevHash() string {
	return e.blockPrevHash
}

func (e BlockNotFitChainTipError) TipHash() string {
	return e.tipHash
}

func (e BlockNotFitChainTipError) Error() string {
	return fmt.Sprintf(
		"block %s with prev hash %s does not fit on current chain tip %s",
		e.blockHash,
		e.blockPrevHash,
		e.tipHash,
	)
}

type BlockNotMatchHeaderError struct {
	blockHash  string
	headerHash string
}

func NewBlockNotMatchHeaderError(
	blockHash string,
	headerHash string,
) BlockNotMatchHeaderError {
	return BlockNotMatchHeaderError{
		blockHash:  blockHash,
		headerHash: headerHash,
	}
}

func (e BlockNotMatchHeaderError) Error() string {
	return fmt.Sprintf(
		"block hash %s does not match first pending header hash %s",
		e.blockHash,
		e.headerHash,
	)
}
