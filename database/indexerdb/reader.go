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

package indexerdb

import (
	"context"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

// Reader is the set of history and address queries that ledger state forwards
// for the APIs. It maps to the IndexerDB, not the LedgerDB.
type Reader interface {
	// CountTransactionsByAddress returns the total number of
	// transactions involving a given address within the inclusive
	// (slot, block index) range [from, to]. A nil bound is open.
	CountTransactionsByAddress(
		ctx context.Context,
		addr lcommon.Address,
		from *models.AddressTransactionPosition,
		to *models.AddressTransactionPosition,
		txn ledgerdb.Txn,
	) (int, error)
	// CountTransactionsByMetadataLabel returns the total number of transactions
	// that include metadata for a given label key.
	CountTransactionsByMetadataLabel(
		ctx context.Context,
		label uint64,
		txn ledgerdb.Txn,
	) (int, error)
	// GetDatums retrieves the datums for hashes inside one read transaction.
	// The result is aligned with hashes; an entry is nil when that datum is not
	// stored. ctx is checked before each lookup and also bounds the
	// transaction's statements, so a cancelled caller stops the batch.
	GetDatums(ctx context.Context, hashes [][]byte) ([]*models.Datum, error)
	// GetExistingTransactionHashes returns the subset of hashes that have a
	// transaction record, read in one metadata transaction that observes ctx.
	// The result is in no particular order.
	GetExistingTransactionHashes(
		ctx context.Context,
		hashes [][]byte,
	) ([][]byte, error)
	// GetTransactionsByAddress returns transactions that involve a given
	// address as either a sender (input) or receiver (output).
	// Results are returned in descending on-chain order.
	GetTransactionsByAddress(
		ctx context.Context,
		addr lcommon.Address,
		limit int,
		offset int,
		txn ledgerdb.Txn,
	) ([]models.Transaction, error)
	// GetTransactionsByAddressWithOrder returns transactions
	// involving a given address with explicit ordering, restricted to the
	// inclusive (slot, block index) range [from, to]. A nil bound is open.
	GetTransactionsByAddressWithOrder(
		ctx context.Context,
		addr lcommon.Address,
		limit int,
		offset int,
		order string,
		from *models.AddressTransactionPosition,
		to *models.AddressTransactionPosition,
		txn ledgerdb.Txn,
	) ([]models.Transaction, error)
	// GetTransactionsByBlockHash returns all transactions for a given
	// block hash, ordered by their position within the block.
	GetTransactionsByBlockHash(
		ctx context.Context,
		blockHash []byte,
		txn ledgerdb.Txn,
	) ([]models.Transaction, error)
	// ScanUtxosByAddress visits every UTxO matching q's exact address patterns
	// in ascending ledger order, one page at a time, and returns the tip stored
	// in the same read transaction. q.Limit is the page size and q.After must
	// be nil; maxCandidates and maxBytes bound each page as in
	// UtxosByAddressBoundedPage, so memory is bounded by one page however many
	// outputs match. Every page and the tip come from one snapshot, so a block
	// applied mid-scan neither duplicates nor drops an output, and the tip
	// names the state the outputs belong to. The transaction stays open until
	// the scan returns, including while visit runs. Native assets are not
	// loaded. A non-nil error from visit ends the scan and is returned.
	ScanUtxosByAddress(
		ctx context.Context,
		q *models.UtxoWithOrderingQuery,
		maxCandidates int,
		maxBytes int64,
		visit func([]models.UtxoWithOrdering) error,
	) (chainsync.Tip, error)
	// UtxoBalanceByAddressAtSlot returns the exact balance of address from
	// the outputs that existed at slot.
	UtxoBalanceByAddressAtSlot(
		ctx context.Context,
		address lcommon.Address,
		slot uint64,
		txn ledgerdb.Txn,
	) (models.AddressBalance, error)
	// UtxosByAddressBoundedPage scans an ordered page while bounding both the
	// number of candidate rows examined and the CBOR retained for matches. A
	// zero byte limit is invalid; callers that do not need a byte bound use
	// UtxosByAddressPage.
	UtxosByAddressBoundedPage(
		ctx context.Context,
		q *models.UtxoWithOrderingQuery,
		maxCandidates int,
		maxBytes int64,
	) (models.UtxoAddressPage, error)
}
