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

	"github.com/blinklabs-io/dingo/database/models"
)

// DefaultMaxQueuedHeaders is the minimum header queue capacity (floor).
// When the ledger security parameter K is configured, the limit is
// max(2*K, DefaultMaxQueuedHeaders).
const DefaultMaxQueuedHeaders = 10_000

var (
	ErrIntersectNotFound = errors.New("chain intersect not found")
	// ErrBlockAddNotAdmitted is returned by AddBlockWithPointDeferredIf when
	// the caller's admit predicate, evaluated under the chain mutex, declines
	// the block. It means the add was abandoned before any chain state was
	// read or written, not that the block was invalid.
	ErrBlockAddNotAdmitted = errors.New(
		"block add was not admitted by the caller's predicate",
	)
	ErrRollbackBeyondEphemeralChain = errors.New(
		"cannot rollback ephemeral chain beyond memory buffer",
	)
	// ErrInvalidSecurityParam is returned by ChainManager.SetLedger when the
	// ledger reports a non-positive Ouroboros security parameter K.
	ErrInvalidSecurityParam = errors.New(
		"ledger security parameter K must be positive",
	)
	// ErrSecurityParamNotConfigured is returned when an operation requires K
	// but ChainManager.SetLedger has not been called successfully.
	ErrSecurityParamNotConfigured = errors.New(
		"chain manager security parameter K is not configured; " +
			"call SetLedger with a ledger that returns a positive SecurityParam()",
	)
	ErrRollbackExceedsSecurityParam = errors.New(
		"rollback depth exceeds security parameter K",
	)
	// ErrRollbackPointNotOnChain is returned when a rollback target resolves
	// to a block that this chain no longer holds at that block index.
	// Rolled-back blocks stay resolvable through the manager's retained block
	// cache with their original index, so a point another fork has since
	// overwritten still looks valid; rolling back to it truncates to a stale
	// index and moves the tip to a block the chain does not have, splicing a
	// continuation onto a parent that is absent from the chain (issue #3005).
	// It wraps models.ErrBlockNotFound so existing callers keep treating an
	// unusable rollback target as "point not found" and re-intersect.
	ErrRollbackPointNotOnChain = fmt.Errorf(
		"%w: rollback point is not on this chain",
		models.ErrBlockNotFound,
	)
	ErrIteratorChainTip = errors.New(
		"chain iterator is at chain tip",
	)
	ErrIteratorChainOrigin = errors.New(
		"chain iterator is at chain origin",
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

// BlockNumberNotContiguousError is returned when a block/header's self-reported
// block number does not follow its parent's. The block number is a redundant
// header field that chain selection uses to pick the longer chain, so it must
// be bound to the actual chain length: a header that chains onto the tip
// (matching prev hash) but claims a non-contiguous block number is rejected so
// a forged (e.g. inflated) number cannot win chain selection.
type BlockNumberNotContiguousError struct {
	blockHash    string
	blockNumber  uint64
	parentNumber uint64
}

func NewBlockNumberNotContiguousError(
	blockHash string,
	blockNumber uint64,
	parentNumber uint64,
) BlockNumberNotContiguousError {
	return BlockNumberNotContiguousError{
		blockHash:    blockHash,
		blockNumber:  blockNumber,
		parentNumber: parentNumber,
	}
}

func (e BlockNumberNotContiguousError) BlockHash() string { return e.blockHash }

func (e BlockNumberNotContiguousError) BlockNumber() uint64 { return e.blockNumber }
func (e BlockNumberNotContiguousError) ParentNumber() uint64 {
	return e.parentNumber
}

func (e BlockNumberNotContiguousError) Error() string {
	return fmt.Sprintf(
		"block %s claims block number %d that is not contiguous with parent %d",
		e.blockHash,
		e.blockNumber,
		e.parentNumber,
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
