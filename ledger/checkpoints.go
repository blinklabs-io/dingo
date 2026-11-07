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

package ledger

import (
	"fmt"
	"strings"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
)

// CheckpointMismatchError is returned when a block at a configured
// checkpoint height has a hash that differs from the expected one. This
// is an envelope-validity failure: the block sits on a chain that
// diverges from the known-good chain at a checkpointed height, so it
// must be rejected regardless of any other validity it might have.
type CheckpointMismatchError struct {
	BlockNo  uint64
	Expected string
	Actual   string
}

func (e *CheckpointMismatchError) Error() string {
	return fmt.Sprintf(
		"block %d hash %s does not match configured checkpoint %s",
		e.BlockNo, e.Actual, e.Expected,
	)
}

// ValidateCheckpoint enforces a set of configured chain checkpoints
// against a single block. The checkpoints map is keyed by block number
// (height) with the expected block hash as a hex string.
//
// It returns nil when no checkpoints are configured, when there is no
// checkpoint at the given height, or when the block hash matches the
// configured checkpoint (case-insensitively). It returns a
// *CheckpointMismatchError when the height is checkpointed but the hash
// differs.
//
// Honest chains always agree with the shipped checkpoints, so this rule
// can only reject a block on a divergent chain; it never rejects a block
// on the canonical chain.
func ValidateCheckpoint(
	checkpoints map[uint64]string,
	blockNo uint64,
	hash string,
) error {
	if len(checkpoints) == 0 {
		return nil
	}
	expected, ok := checkpoints[blockNo]
	if !ok {
		return nil
	}
	if !strings.EqualFold(expected, hash) {
		return &CheckpointMismatchError{
			BlockNo:  blockNo,
			Expected: expected,
			Actual:   hash,
		}
	}
	return nil
}

// validateBlockCheckpoint applies the configured checkpoints to an
// inbound block. Byron epoch boundary blocks (EBBs) share the preceding
// block's number rather than incrementing it, so an EBB can collide with
// a checkpointed height that refers to the regular block at that height.
// EBBs are skipped to avoid a false mismatch; the regular block at the
// checkpointed height is still validated when it is processed.
//
// The block hash is only computed when a checkpoint actually exists at
// the block's height, keeping the common (non-checkpointed) case cheap.
func (ls *LedgerState) validateBlockCheckpoint(block ledger.Block) error {
	if len(ls.checkpoints) == 0 {
		return nil
	}
	if _, isEbb := block.(*byron.ByronEpochBoundaryBlock); isEbb {
		return nil
	}
	blockNo := block.BlockNumber()
	if _, ok := ls.checkpoints[blockNo]; !ok {
		return nil
	}
	return ValidateCheckpoint(ls.checkpoints, blockNo, block.Hash().String())
}

// ValidateHeaderCheckpoint applies configured checkpoints to a peer
// header, so a header on a chain that diverges at a checkpointed height is
// refused before it can join a candidate chain. Header validity does not
// depend on the validation mode or on Mithril coverage, matching the
// block-apply rule.
//
// Byron epoch boundary block headers share the preceding block's number and
// are skipped, as in validateBlockCheckpoint. The header hash is computed
// only when a checkpoint exists at the header's height.
func ValidateHeaderCheckpoint(
	checkpoints map[uint64]string,
	header ledger.BlockHeader,
) error {
	if len(checkpoints) == 0 {
		return nil
	}
	if _, isEbb := header.(*byron.ByronEpochBoundaryBlockHeader); isEbb {
		return nil
	}
	blockNo := header.BlockNumber()
	if _, ok := checkpoints[blockNo]; !ok {
		return nil
	}
	return ValidateCheckpoint(checkpoints, blockNo, header.Hash().String())
}

// ValidateHeaderCheckpoint applies the ledger's configured checkpoints to
// a peer header. It is safe for concurrent use: the checkpoint map is set
// at construction and never mutated.
func (ls *LedgerState) ValidateHeaderCheckpoint(
	header ledger.BlockHeader,
) error {
	return ValidateHeaderCheckpoint(ls.checkpoints, header)
}
