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
)

// epochLabNonce returns the Praos lastEpochBlockNonce value for an epoch
// boundary. If the epoch has at least one block, the nonce is the hash of the
// last block before the boundary. If the epoch has no blocks, the consensus
// state carries the previous value forward.
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
	if lastBlock.Slot >= epochStartSlot && len(lastBlock.Hash) > 0 {
		return cloneNonce(lastBlock.Hash), nil
	}
	return ls.normalizeCarriedLabNonce(txn, epochStartSlot, carriedLabNonce)
}

// normalizeCarriedLabNonce upgrades the old persisted shape that stored the
// boundary block's PrevHash instead of its Hash. This only applies when an
// epoch has no blocks of its own and must carry the previous boundary nonce.
func (ls *LedgerState) normalizeCarriedLabNonce(
	txn *database.Txn,
	epochStartSlot uint64,
	carriedLabNonce []byte,
) ([]byte, error) {
	if len(carriedLabNonce) == 0 || epochStartSlot == 0 {
		return cloneNonce(carriedLabNonce), nil
	}
	boundary, err := ls.canonicalBlockBeforeSlot(txn, epochStartSlot)
	if err != nil {
		if !errors.Is(err, models.ErrBlockNotFound) {
			return nil, fmt.Errorf("lookup carried boundary block: %w", err)
		}
		return cloneNonce(carriedLabNonce), nil
	}
	if len(boundary.Hash) > 0 &&
		len(boundary.PrevHash) > 0 &&
		bytes.Equal(carriedLabNonce, boundary.PrevHash) {
		return cloneNonce(boundary.Hash), nil
	}
	return cloneNonce(carriedLabNonce), nil
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
	return lookupBlockBeforeSlot(ls.db, txn, slot)
}

func cloneNonce(nonce []byte) []byte {
	return append([]byte(nil), nonce...)
}
