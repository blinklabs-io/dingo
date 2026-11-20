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

package utxorpc

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/apiready"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// QueryDatabase is the store the QueryService reads UTxOs, datums and
// transaction records from: the IndexerDB, whose tip and readiness the service
// reports and honors. Sync, watch, submit and evaluation stay on the ledger.
type QueryDatabase interface {
	Datums(ctx context.Context, hashes [][]byte) ([]*models.Datum, error)
	TransactionByHash(
		ctx context.Context,
		hash []byte,
	) (*models.Transaction, error)
	UtxosByRefsBounded(
		ctx context.Context,
		refs []models.UtxoId,
		maxBytes int64,
	) ([]models.Utxo, error)
	UtxosByAddressBoundedPage(
		ctx context.Context,
		q *models.UtxoWithOrderingQuery,
		maxCandidates int,
		maxBytes int64,
	) (models.UtxoAddressPage, error)
	// Tip is the chain position the stored rows cover.
	Tip() (ochainsync.Tip, error)
	// Ready reports whether endpoints of kind may answer.
	Ready(kind indexerdb.Kind) (bool, error)
}

// indexerQueryDatabase reads UTxOs, datums and transactions from the
// IndexerDB and reports its tip.
type indexerQueryDatabase struct {
	db    *database.Database
	store *indexerdb.Store
	ready apiready.Check
}

// NewIndexerQueryDatabase returns the QueryDatabase over the IndexerDB db,
// whose tip store reports and whose readiness ready decides.
func NewIndexerQueryDatabase(
	db *database.Database,
	store *indexerdb.Store,
	ready apiready.Check,
) QueryDatabase {
	return &indexerQueryDatabase{db: db, store: store, ready: ready}
}

func (q *indexerQueryDatabase) Datums(
	ctx context.Context,
	hashes [][]byte,
) ([]*models.Datum, error) {
	return q.db.GetDatums(ctx, hashes)
}

func (q *indexerQueryDatabase) TransactionByHash(
	ctx context.Context,
	hash []byte,
) (*models.Transaction, error) {
	return q.db.GetTransactionByHash(ctx, hash, nil)
}

func (q *indexerQueryDatabase) UtxosByRefsBounded(
	ctx context.Context,
	refs []models.UtxoId,
	maxBytes int64,
) ([]models.Utxo, error) {
	return q.db.UtxosByRefsBounded(ctx, refs, maxBytes, nil)
}

func (q *indexerQueryDatabase) UtxosByAddressBoundedPage(
	ctx context.Context,
	query *models.UtxoWithOrderingQuery,
	maxCandidates int,
	maxBytes int64,
) (models.UtxoAddressPage, error) {
	return q.db.UtxosByAddressBoundedPage(ctx, query, maxCandidates, maxBytes)
}

func (q *indexerQueryDatabase) Tip() (ochainsync.Tip, error) {
	tip, err := q.store.Tip(nil)
	if err != nil {
		return ochainsync.Tip{}, err
	}
	return ochainsync.Tip{
		Point:       ocommon.NewPoint(tip.Slot, tip.Hash),
		BlockNumber: tip.BlockNumber,
	}, nil
}

func (q *indexerQueryDatabase) Ready(kind indexerdb.Kind) (bool, error) {
	return q.ready(kind)
}

func (u *Utxorpc) queryDB() QueryDatabase {
	return u.config.QueryDatabase
}

// requireReady refuses a query the IndexerDB cannot yet answer. A store whose
// tip cannot be read is refused too: answering from rows the tip does not
// cover could report a position the data does not match.
func (s *queryServiceServer) requireReady(kind indexerdb.Kind) error {
	ok, err := s.utxorpc.queryDB().Ready(kind)
	if err != nil || !ok {
		return connect.NewError(
			connect.CodeUnavailable,
			errIndexerNotReady,
		)
	}
	return nil
}

// queryTip is the chain point query responses report.
func (s *queryServiceServer) queryTip() (ochainsync.Tip, error) {
	tip, err := s.utxorpc.queryDB().Tip()
	if err != nil {
		return ochainsync.Tip{}, connect.NewError(
			connect.CodeUnavailable,
			errIndexerNotReady,
		)
	}
	return tip, nil
}

var errIndexerNotReady = errors.New("indexer is not ready for this endpoint")
