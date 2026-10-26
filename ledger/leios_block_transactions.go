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
	"context"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

type leiosBlockCertifierHeader interface {
	LeiosCertified() (certified bool, present bool)
	PrevHash() lcommon.Blake2b256
}

type leiosBlockAnnouncerHeader interface {
	LeiosAnnouncement() (lcommon.Blake2b256, uint64, bool)
}

// BlockTransactions returns the ledger transactions represented by a stored
// block. A Dijkstra certifying ranking block represents the transaction bodies
// in the endorser block announced by its parent; those bodies are retained
// separately from the ranking block CBOR.
func (ls *LedgerState) BlockTransactions(
	ctx context.Context,
	block models.Block,
) ([]lcommon.Transaction, error) {
	decoded, err := block.Decode()
	if err != nil {
		return nil, fmt.Errorf("decode block %x: %w", block.Hash, err)
	}
	transactions := decoded.Transactions()
	if _, ok := decoded.Header().(leiosBlockCertifierHeader); !ok &&
		block.Type == uint(dijkstra.BlockTypeDijkstra) {
		return nil, fmt.Errorf("dijkstra block %x has no Leios header", block.Hash)
	}
	closure, err := ls.CertifiedClosureTransactions(ctx, decoded)
	if err != nil {
		return nil, err
	}
	if closure == nil {
		return transactions, nil
	}
	if len(transactions) != 0 {
		return nil, fmt.Errorf(
			"certifying Dijkstra block %x also contains ranking-block transactions",
			block.Hash,
		)
	}
	return closure, nil
}

// CertifiedClosureTransactions returns the transactions a certifying ranking
// block applies from the certified closure: the endorser block its parent
// announced, in manifest order. It returns nil for a block that certifies
// nothing. The ledger applies these transactions with the ranking block, so
// they are as confirmed as the block's own transactions, and a rollback of the
// block undoes them; the mempool uses this to evict them and, on a rollback,
// to restore them, exactly as it does for the block's own transactions.
func (ls *LedgerState) CertifiedClosureTransactions(
	ctx context.Context,
	decoded ledger.Block,
) ([]lcommon.Transaction, error) {
	if decoded == nil {
		return nil, nil
	}
	header, ok := decoded.Header().(leiosBlockCertifierHeader)
	if !ok {
		return nil, nil
	}
	certified, present := header.LeiosCertified()
	if !present || !certified {
		return nil, nil
	}

	// The parent may itself have been rolled back, as when WatchTx rebuilds
	// the undo of a rolled-back chain, so it is also read from the rollback
	// cache.
	parent, err := ls.BlockByHashIncludingRolledBack(
		ctx,
		header.PrevHash().Bytes(),
	)
	if err != nil {
		return nil, fmt.Errorf("load certifying block parent: %w", err)
	}
	parentBlock, err := parent.Decode()
	if err != nil {
		return nil, fmt.Errorf("decode certifying block parent: %w", err)
	}
	announcer, ok := parentBlock.Header().(leiosBlockAnnouncerHeader)
	if !ok {
		return nil, errors.New("certifying block parent has no Leios announcement")
	}
	ebHash, _, announced := announcer.LeiosAnnouncement()
	if !announced {
		return nil, errors.New("certifying block parent announced no endorser block")
	}

	manifestRaw, manifestErr := ls.db.GetLeiosEBManifest(ebHash.Bytes(), parent.Slot)
	blockTransactions, txErr := ls.db.GetLeiosEBTxs(ebHash.Bytes(), parent.Slot)
	if manifestErr != nil && !errors.Is(manifestErr, types.ErrBlobKeyNotFound) {
		return nil, fmt.Errorf("load certified endorser-block manifest: %w", manifestErr)
	}
	if txErr != nil && !errors.Is(txErr, types.ErrBlobKeyNotFound) {
		return nil, fmt.Errorf("load certified endorser-block transactions: %w", txErr)
	}
	if errors.Is(manifestErr, types.ErrBlobKeyNotFound) ||
		errors.Is(txErr, types.ErrBlobKeyNotFound) {
		if ls.config.EndorserBlockDataProvider == nil {
			if manifestErr != nil {
				return nil, fmt.Errorf("load certified endorser-block manifest: %w", manifestErr)
			}
			return nil, fmt.Errorf("load certified endorser-block transactions: %w", txErr)
		}
		var ok bool
		manifestRaw, blockTransactions, ok = ls.config.EndorserBlockDataProvider(
			ebHash.Bytes(), parent.Slot,
		)
		if !ok {
			if manifestErr != nil {
				return nil, fmt.Errorf("load certified endorser-block manifest: %w", manifestErr)
			}
			return nil, fmt.Errorf("load certified endorser-block transactions: %w", txErr)
		}
	}
	manifest, err := lcommon.NewLeiosEndorserBlockFromCbor(manifestRaw)
	if err != nil {
		return nil, fmt.Errorf("decode certified endorser-block manifest: %w", err)
	}
	if err := manifest.Validate(); err != nil {
		return nil, fmt.Errorf("validate certified endorser-block manifest: %w", err)
	}
	if manifest.Hash() != ebHash {
		return nil, errors.New("certified endorser-block manifest hash mismatch")
	}

	if len(blockTransactions) != len(manifest.TransactionReferences) {
		return nil, fmt.Errorf(
			"certified endorser-block transaction count mismatch: got %d, want %d",
			len(blockTransactions),
			len(manifest.TransactionReferences),
		)
	}

	ret := make([]lcommon.Transaction, 0, len(blockTransactions))
	for i, raw := range blockTransactions {
		tx, err := safedecode.DijkstraBlockTransaction(raw)
		if err != nil {
			return nil, fmt.Errorf("decode certified endorser transaction %d: %w", i, err)
		}
		reference := manifest.TransactionReferences[i]
		// The manifest reference hashes the full block_transaction CBOR,
		// not the Cardano transaction body.
		if lcommon.Blake2b256Hash(raw) != reference.TransactionHash {
			return nil, fmt.Errorf("certified endorser transaction %d hash mismatch", i)
		}
		if uint64(len(raw)) != uint64(reference.TransactionSize) {
			return nil, fmt.Errorf("certified endorser transaction %d size mismatch", i)
		}
		ret = append(ret, tx)
	}
	return ret, nil
}
