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
	"bytes"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// epochLabNonce returns the Praos lastEpochBlockNonce value to store on the new
// epoch record at a boundary (it becomes the carried lab used at the NEXT
// boundary). In cardano-ledger this is praosStateLastEpochBlockNonce, set at the
// epoch transition to praosStateLabNonce, which is updated per block to
// prevHashToNonce(block.prevHash). So when the closing epoch has at least one
// block, the value is the PARENT hash of that epoch's last block (the last
// block's prevHash), NOT the last block's own hash — a deliberate one-block lag.
//
// Using the last block's own hash shifts the carried lab by one block and
// diverges the computed epoch nonce from the network at every self-computed
// boundary, so every leader-VRF check in the following epoch fails with "issue
// verifying proof" (#2734, eta_1349 wedge). koios preview proof:
//
//	eta_1348 lab = prevHash(04f75b4e, last block of 1346) = 43edf2  (imported, correct)
//	eta_1349 lab = prevHash(94d3083a, last block of 1347) = 08a8dd12
//	blake2b256(candidate_1348 1aee9a8d || 08a8dd12) = 294f4731 = koios eta_1349
//
// If the closing epoch has no blocks of its own (or the boundary block has no
// parent — the genesis edge), the consensus state carries the previous value
// forward unchanged; that value is already a prevHash-shape nonce (or, at
// genesis, NeutralNonce/nil).
func (ls *LedgerState) epochLabNonce(
	txn *database.Txn,
	epochStartSlot uint64,
	epochEndSlot uint64,
	carriedLabNonce []byte,
) ([]byte, error) {
	lastBlock, err := ls.canonicalBlockBeforeSlot(txn, epochEndSlot)
	if err != nil {
		if !errors.Is(err, models.ErrBlockNotFound) {
			return nil, fmt.Errorf("lookup boundary block: %w", err)
		}
		return cloneNonce(carriedLabNonce), nil
	}
	if lastBlock.Slot < epochStartSlot {
		return cloneNonce(carriedLabNonce), nil
	}
	// Derive the parent hash, decoding the block CBOR when the stored
	// PrevHash is empty (legacy rows from the empty-PrevHash storage bug that
	// caused the Dijkstra at-tip wedge). Falling back to the carried value
	// here would silently shift the lab by one epoch, so a block whose parent
	// cannot be determined is an error, not a carry.
	prevHash, err := blockPrevHash(lastBlock)
	if err != nil {
		return nil, fmt.Errorf(
			"derive boundary block parent hash at slot %d: %w",
			lastBlock.Slot,
			err,
		)
	}
	if len(prevHash) == 0 {
		// The boundary block has no parent: it is the chain's first block,
		// so the carried value (NeutralNonce at genesis) is unchanged.
		return cloneNonce(carriedLabNonce), nil
	}
	if len(prevHash) != lcommon.Blake2b256Size {
		return nil, fmt.Errorf(
			"boundary block parent hash at slot %d has invalid length %d",
			lastBlock.Slot,
			len(prevHash),
		)
	}
	// cardano-ledger's prevHashToNonce maps GenesisHash to NeutralNonce: when
	// the closing epoch's last block is the chain's first block, the carried
	// value is used, not the genesis hash bytes.
	if genesisHash, gErr := GenesisBlockHash(ls.config.CardanoNodeConfig); gErr == nil &&
		bytes.Equal(prevHash, genesisHash[:]) {
		return cloneNonce(carriedLabNonce), nil
	}
	return cloneNonce(prevHash), nil
}

func (ls *LedgerState) canonicalBlockBeforeSlot(
	txn *database.Txn,
	slot uint64,
) (models.Block, error) {
	if ls == nil {
		return models.Block{}, errors.New("ledger state is nil")
	}
	if ls.chain != nil {
		return ls.chain.BlockBeforeSlot(slot)
	}
	if ls.db == nil && txn == nil {
		return models.Block{}, models.ErrBlockNotFound
	}
	return lookupBlockBeforeSlot(ls.db, txn, slot)
}

func cloneNonce(nonce []byte) []byte {
	return append([]byte(nil), nonce...)
}
