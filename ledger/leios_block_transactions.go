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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// ErrCertifiedClosureUnavailable reports that the endorser-block closure a
// certifying ranking block applies is neither persisted nor held by the
// endorser-block data provider. It is not corruption: the ledger persists the
// closure when it applies the certifying block, so a stored block the ledger
// has not applied yet can lack it, as can any block read before the provider
// is wired at startup. A caller that can wait may retry once the ledger has
// applied the block; a closure that is present but invalid fails with a
// different error.
var ErrCertifiedClosureUnavailable = errors.New(
	"certified endorser-block closure unavailable",
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
		return nil, fmt.Errorf(
			"dijkstra block %x has no Leios header",
			block.Hash,
		)
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
	data, err := ls.loadCertifiedClosure(ctx, decoded)
	if err != nil || data == nil {
		return nil, err
	}
	return ls.decodeCertifiedClosure(data)
}

// CertifiedClosure is the endorser block a certifying ranking block applies,
// validated against the manifest its parent announced.
type CertifiedClosure struct {
	// EndorserBlockHash and EndorserBlockSlot identify the endorser block:
	// the parent's announcement, at the parent's slot.
	EndorserBlockHash lcommon.Blake2b256
	EndorserBlockSlot uint64
	manifest          *lcommon.LeiosEndorserBlock
	transactions      []cbor.RawMessage
}

// CertifiedClosureSource is what a certified closure is read from.
type CertifiedClosureSource interface {
	// ParentBlock returns the stored block with the given hash.
	ParentBlock(ctx context.Context, hash []byte) (models.Block, error)
	// EndorserBlockData returns the manifest and transaction bytes of the
	// endorser block announced at slot. The errors it returns are wrapped
	// as the caller reports them.
	EndorserBlockData(
		ctx context.Context,
		hash []byte,
		slot uint64,
	) ([]byte, []cbor.RawMessage, error)
}

// ledgerClosureSource reads through the ledger state: the parent may be a
// rolled-back block, and a closure missing from storage may still be
// available from the configured provider.
type ledgerClosureSource struct{ ls *LedgerState }

func (s ledgerClosureSource) ParentBlock(
	ctx context.Context,
	hash []byte,
) (models.Block, error) {
	// The parent may itself have been rolled back, as when WatchTx rebuilds
	// the undo of a rolled-back chain, so it is also read from the rollback
	// cache.
	return s.ls.BlockByHashIncludingRolledBack(ctx, hash)
}

func (s ledgerClosureSource) EndorserBlockData(
	_ context.Context,
	hash []byte,
	slot uint64,
) ([]byte, []cbor.RawMessage, error) {
	return readEndorserBlockData(
		s.ls.db, hash, slot, s.ls.config.EndorserBlockDataProvider,
	)
}

// databaseClosureSource reads a closure from a database alone, as a consumer
// that applies blocks the ledger already committed does.
type databaseClosureSource struct{ db *database.Database }

// NewDatabaseClosureSource reads certified closures from db's stored blocks
// and endorser-block data.
func NewDatabaseClosureSource(db *database.Database) CertifiedClosureSource {
	return databaseClosureSource{db: db}
}

func (s databaseClosureSource) ParentBlock(
	ctx context.Context,
	hash []byte,
) (models.Block, error) {
	return database.BlockByHash(ctx, s.db, hash)
}

func (s databaseClosureSource) EndorserBlockData(
	_ context.Context,
	hash []byte,
	slot uint64,
) ([]byte, []cbor.RawMessage, error) {
	return readEndorserBlockData(s.db, hash, slot, nil)
}

func readEndorserBlockData(
	db *database.Database,
	hash []byte,
	slot uint64,
	provider func([]byte, uint64) ([]byte, []cbor.RawMessage, bool),
) ([]byte, []cbor.RawMessage, error) {
	manifestRaw, manifestErr := db.GetLeiosEBManifest(hash, slot)
	blockTransactions, txErr := db.GetLeiosEBTxs(hash, slot)
	if manifestErr != nil && !errors.Is(manifestErr, types.ErrBlobKeyNotFound) {
		return nil, nil, fmt.Errorf(
			"load certified endorser-block manifest: %w",
			manifestErr,
		)
	}
	if txErr != nil && !errors.Is(txErr, types.ErrBlobKeyNotFound) {
		return nil, nil, fmt.Errorf(
			"load certified endorser-block transactions: %w",
			txErr,
		)
	}
	if !errors.Is(manifestErr, types.ErrBlobKeyNotFound) &&
		!errors.Is(txErr, types.ErrBlobKeyNotFound) {
		return manifestRaw, blockTransactions, nil
	}
	missing := func() error {
		if manifestErr != nil {
			return fmt.Errorf(
				"%w: load certified endorser-block manifest: %w",
				ErrCertifiedClosureUnavailable,
				manifestErr,
			)
		}
		return fmt.Errorf(
			"%w: load certified endorser-block transactions: %w",
			ErrCertifiedClosureUnavailable,
			txErr,
		)
	}
	if provider == nil {
		return nil, nil, missing()
	}
	manifestRaw, blockTransactions, ok := provider(hash, slot)
	if !ok {
		return nil, nil, missing()
	}
	return manifestRaw, blockTransactions, nil
}

func (ls *LedgerState) loadCertifiedClosure(
	ctx context.Context,
	decoded ledger.Block,
) (*CertifiedClosure, error) {
	return LoadCertifiedClosure(ctx, ledgerClosureSource{ls: ls}, decoded)
}

// LoadCertifiedClosure returns the closure a certifying ranking block
// applies, or nil for a block that certifies nothing. The manifest and the
// transaction count are validated; Decode checks each transaction.
func LoadCertifiedClosure(
	ctx context.Context,
	src CertifiedClosureSource,
	decoded ledger.Block,
) (*CertifiedClosure, error) {
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

	parent, err := src.ParentBlock(ctx, header.PrevHash().Bytes())
	if err != nil {
		return nil, fmt.Errorf("load certifying block parent: %w", err)
	}
	parentBlock, err := parent.Decode()
	if err != nil {
		return nil, fmt.Errorf("decode certifying block parent: %w", err)
	}
	announcer, ok := parentBlock.Header().(leiosBlockAnnouncerHeader)
	if !ok {
		return nil, errors.New(
			"certifying block parent has no Leios announcement",
		)
	}
	ebHash, _, announced := announcer.LeiosAnnouncement()
	if !announced {
		return nil, errors.New(
			"certifying block parent announced no endorser block",
		)
	}
	manifestRaw, blockTransactions, err := src.EndorserBlockData(
		ctx, ebHash.Bytes(), parent.Slot,
	)
	if err != nil {
		return nil, err
	}
	manifest, err := lcommon.NewLeiosEndorserBlockFromCbor(manifestRaw)
	if err != nil {
		return nil, fmt.Errorf(
			"decode certified endorser-block manifest: %w",
			err,
		)
	}
	if err := manifest.Validate(); err != nil {
		return nil, fmt.Errorf(
			"validate certified endorser-block manifest: %w",
			err,
		)
	}
	if manifest.Hash() != ebHash {
		return nil, errors.New(
			"certified endorser-block manifest hash mismatch",
		)
	}

	if len(blockTransactions) != len(manifest.TransactionReferences) {
		return nil, fmt.Errorf(
			"certified endorser-block transaction count mismatch: got %d, want %d",
			len(blockTransactions),
			len(manifest.TransactionReferences),
		)
	}

	return &CertifiedClosure{
		EndorserBlockHash: ebHash,
		EndorserBlockSlot: parent.Slot,
		manifest:          manifest,
		transactions:      blockTransactions,
	}, nil
}

// RawTransactions returns the closure's transactions as delivered, in
// manifest order.
func (c *CertifiedClosure) RawTransactions() []cbor.RawMessage {
	return c.transactions
}

// Decode returns the closure's transactions in manifest order, each checked
// against its manifest reference.
func (c *CertifiedClosure) Decode() ([]lcommon.Transaction, error) {
	ret := make([]lcommon.Transaction, 0, len(c.transactions))
	for i, raw := range c.transactions {
		tx, err := safedecode.DijkstraBlockTransaction(raw)
		if err != nil {
			return nil, fmt.Errorf(
				"decode certified endorser transaction %d: %w",
				i,
				err,
			)
		}
		reference := c.manifest.TransactionReferences[i]
		// The manifest reference hashes the full block_transaction CBOR,
		// not the Cardano transaction body.
		if lcommon.Blake2b256Hash(raw) != reference.TransactionHash {
			return nil, fmt.Errorf(
				"certified endorser transaction %d hash mismatch",
				i,
			)
		}
		if uint64(len(raw)) != uint64(reference.TransactionSize) {
			return nil, fmt.Errorf(
				"certified endorser transaction %d size mismatch",
				i,
			)
		}
		ret = append(ret, tx)
	}
	return ret, nil
}

func (ls *LedgerState) decodeCertifiedClosure(
	data *CertifiedClosure,
) ([]lcommon.Transaction, error) {
	ret, err := data.Decode()
	if err != nil {
		return nil, err
	}
	hash := data.manifest.Hash()
	ls.certifiedClosureReceipt.Store(&hash)
	return ret, nil
}

// BodyCbors returns the transaction body CBOR of each transaction, in
// manifest order: the payload of a transaction's offset entry.
func (c *CertifiedClosure) BodyCbors() ([][]byte, error) {
	bodies := make([][]byte, len(c.transactions))
	for i, raw := range c.transactions {
		_, elems, err := decodeEndorserTxEnvelope(raw)
		if err != nil {
			return nil, fmt.Errorf("endorser tx %d: %w", i, err)
		}
		bodies[i] = []byte(elems[0])
	}
	return bodies, nil
}

// BlockTransactionCount counts the top-level transactions represented by a
// stored block, retaining the integrity checks used by BlockTransactions.
func (ls *LedgerState) BlockTransactionCount(
	ctx context.Context,
	block models.Block,
) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	decoded, err := block.Decode()
	if err != nil {
		return 0, fmt.Errorf("decode block %x: %w", block.Hash, err)
	}
	transactions := decoded.Transactions()
	if _, ok := decoded.Header().(leiosBlockCertifierHeader); !ok &&
		block.Type == uint(dijkstra.BlockTypeDijkstra) {
		return 0, fmt.Errorf(
			"dijkstra block %x has no Leios header",
			block.Hash,
		)
	}
	data, err := ls.loadCertifiedClosure(ctx, decoded)
	if err != nil {
		return 0, err
	}
	if data == nil {
		return len(transactions), nil
	}
	receipt := ls.certifiedClosureReceipt.Load()
	verified := receipt != nil && *receipt == data.manifest.Hash()
	if verified {
		for i, raw := range data.transactions {
			reference := data.manifest.TransactionReferences[i]
			if uint64(len(raw)) != uint64(reference.TransactionSize) ||
				lcommon.Blake2b256Hash(raw) != reference.TransactionHash {
				verified = false
				break
			}
		}
	}
	if !verified {
		// Decode failures precede hash/size errors in the full reader. Reuse
		// that path on anomalies so callers observe the same error.
		if _, err := ls.decodeCertifiedClosure(data); err != nil {
			return 0, err
		}
	}
	if len(transactions) != 0 {
		return 0, fmt.Errorf(
			"certifying Dijkstra block %x also contains ranking-block transactions",
			block.Hash,
		)
	}
	return len(data.transactions), nil
}
