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
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
	query "github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query/queryconnect"
)

// A core-tier node has no IndexerDB: the QueryService reads live outputs by
// reference from the ledger's UTxO set and refuses what only history can
// answer, rather than reading the ledger's database, which holds no UTxO,
// datum or transaction rows.
func TestCoreQueryServiceReadsTheLedgerAndRefusesHistory(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	// The api harness finds a live output of the shared fixture chain.
	api := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 20})
	apiCli := queryconnect.NewQueryServiceClient(
		api.Client, api.Server.URL, connect.WithGRPC(),
	)
	search, err := apiCli.SearchUtxos(
		ctx, connect.NewRequest(&query.SearchUtxosRequest{MaxItems: 1}),
	)
	require.NoError(t, err)
	require.NotEmpty(t, search.Msg.GetItems())
	ref := search.Msg.GetItems()[0].GetTxoRef()

	core := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{
		numBlocks: 20,
		tune: func(cfg *UtxorpcConfig) {
			ledger, ok := cfg.LedgerState.(QueryLedger)
			require.True(t, ok)
			cfg.QueryDatabase = NewLedgerQueryDatabase(ledger)
		},
	})
	cli := queryconnect.NewQueryServiceClient(
		core.Client, core.Server.URL, connect.WithGRPC(),
	)

	read, err := cli.ReadUtxos(ctx, connect.NewRequest(
		&query.ReadUtxosRequest{Keys: []*query.TxoRef{ref}},
	))
	require.NoError(t, err)
	require.Len(t, read.Msg.GetItems(), 1)
	require.Equal(t, ref.GetHash(), read.Msg.GetItems()[0].GetTxoRef().GetHash())
	require.Equal(t, ref.GetIndex(), read.Msg.GetItems()[0].GetTxoRef().GetIndex())
	tip := core.LS.Tip()
	require.Equal(t, tip.Point.Slot, read.Msg.GetLedgerTip().GetSlot())

	_, err = cli.SearchUtxos(
		ctx, connect.NewRequest(&query.SearchUtxosRequest{MaxItems: 1}),
	)
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	_, err = cli.ReadTx(ctx, connect.NewRequest(
		&query.ReadTxRequest{Hash: api.IndexedTxHashes[0]},
	))
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
}

// recordingQueryDB answers queries itself; calls counts what reached it.
type recordingQueryDB struct {
	ready    map[indexerdb.Kind]bool
	tip      ochainsync.Tip
	tipErr   error
	searches int
	datums   int
	txs      int
	refs     int
}

func (d *recordingQueryDB) Datums(
	context.Context, [][]byte,
) ([]*models.Datum, error) {
	d.datums++
	return nil, errors.New("unexpected datum read")
}

func (d *recordingQueryDB) TransactionByHash(
	context.Context, []byte,
) (*models.Transaction, error) {
	d.txs++
	return nil, nil
}

func (d *recordingQueryDB) UtxosByRefsBounded(
	context.Context, []models.UtxoId, int64,
) ([]models.Utxo, error) {
	d.refs++
	return nil, nil
}

func (d *recordingQueryDB) UtxosByAddressBoundedPage(
	context.Context, *models.UtxoWithOrderingQuery, int, int64,
) (models.UtxoAddressPage, error) {
	d.searches++
	return models.UtxoAddressPage{}, nil
}

func (d *recordingQueryDB) Tip() (ochainsync.Tip, error) { return d.tip, d.tipErr }

func (d *recordingQueryDB) Ready(kind indexerdb.Kind) (bool, error) {
	return d.ready[kind], nil
}

// stubQueryDatabase serves the QueryService reads from the rows a ledger stub
// answers itself, always ready and at the stub's tip.
type stubQueryDatabase struct{ ls UtxorpcLedgerState }

func stubQueryDB(ls UtxorpcLedgerState) QueryDatabase {
	return stubQueryDatabase{ls: ls}
}

func (s stubQueryDatabase) Datums(
	ctx context.Context, hashes [][]byte,
) ([]*models.Datum, error) {
	return s.ls.(interface {
		Datums(context.Context, [][]byte) ([]*models.Datum, error)
	}).Datums(ctx, hashes)
}

func (s stubQueryDatabase) TransactionByHash(
	ctx context.Context, hash []byte,
) (*models.Transaction, error) {
	return s.ls.(interface {
		TransactionByHash(context.Context, []byte) (*models.Transaction, error)
	}).TransactionByHash(ctx, hash)
}

func (s stubQueryDatabase) UtxosByRefsBounded(
	ctx context.Context, refs []models.UtxoId, maxBytes int64,
) ([]models.Utxo, error) {
	return s.ls.(interface {
		UtxosByRefsBounded(
			context.Context, []models.UtxoId, int64,
		) ([]models.Utxo, error)
	}).UtxosByRefsBounded(ctx, refs, maxBytes)
}

func (s stubQueryDatabase) UtxosByAddressBoundedPage(
	ctx context.Context,
	q *models.UtxoWithOrderingQuery,
	maxCandidates int,
	maxBytes int64,
) (models.UtxoAddressPage, error) {
	return s.ls.(interface {
		UtxosByAddressBoundedPage(
			context.Context, *models.UtxoWithOrderingQuery, int, int64,
		) (models.UtxoAddressPage, error)
	}).UtxosByAddressBoundedPage(ctx, q, maxCandidates, maxBytes)
}

func (s stubQueryDatabase) Tip() (ochainsync.Tip, error) {
	return s.ls.Tip(), nil
}

func (stubQueryDatabase) Ready(indexerdb.Kind) (bool, error) {
	return true, nil
}

// panicLedger fails the test, by nil dereference, if a query reaches the
// ledger instead of the query database.
type panicLedger struct{ UtxorpcLedgerState }

func newQuerySrv(db QueryDatabase) *queryServiceServer {
	return &queryServiceServer{utxorpc: NewUtxorpc(UtxorpcConfig{
		Logger:        discardLogger(),
		LedgerState:   panicLedger{},
		QueryDatabase: db,
	})}
}

func TestQueryServiceReadsTheQueryDatabase(t *testing.T) {
	t.Parallel()
	hash := make([]byte, 32)
	db := &recordingQueryDB{
		ready: map[indexerdb.Kind]bool{
			indexerdb.KindCurrent: true, indexerdb.KindHistory: true,
		},
		tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(77, []byte{0xaa}),
			BlockNumber: 9,
		},
	}
	srv := newQuerySrv(db)
	ctx := context.Background()

	search, err := srv.SearchUtxos(
		ctx,
		connect.NewRequest(&query.SearchUtxosRequest{MaxItems: 1}),
	)
	require.NoError(t, err)
	require.Equal(t, 1, db.searches)
	require.Equal(t, uint64(77), search.Msg.GetLedgerTip().GetSlot())
	require.Equal(t, uint64(9), search.Msg.GetLedgerTip().GetHeight())

	_, err = srv.ReadUtxos(ctx, connect.NewRequest(&query.ReadUtxosRequest{
		Keys: []*query.TxoRef{{Hash: hash, Index: 0}},
	}))
	require.Error(t, err, "the store holds no such UTxO")
	require.Equal(t, 1, db.refs)

	_, err = srv.ReadTx(
		ctx,
		connect.NewRequest(&query.ReadTxRequest{Hash: hash}),
	)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	require.Equal(t, 1, db.txs)
}

func TestQueryServiceRefusesUntilTheIndexerIsReady(t *testing.T) {
	t.Parallel()
	hash := make([]byte, 32)
	db := &recordingQueryDB{ready: map[indexerdb.Kind]bool{
		indexerdb.KindHistory: true,
	}}
	srv := newQuerySrv(db)
	ctx := context.Background()

	_, err := srv.SearchUtxos(
		ctx,
		connect.NewRequest(&query.SearchUtxosRequest{MaxItems: 1}),
	)
	require.Equal(t, connect.CodeUnavailable, connect.CodeOf(err))
	_, err = srv.ReadUtxos(ctx, connect.NewRequest(&query.ReadUtxosRequest{
		Keys: []*query.TxoRef{{Hash: hash}},
	}))
	require.Equal(t, connect.CodeUnavailable, connect.CodeOf(err))
	require.Zero(t, db.searches+db.refs, "a refused query reads nothing")

	// History is ready, so the same store now answers a history read.
	_, err = srv.ReadTx(
		ctx,
		connect.NewRequest(&query.ReadTxRequest{Hash: hash}),
	)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	db.ready[indexerdb.KindHistory] = false
	_, err = srv.ReadTx(
		ctx,
		connect.NewRequest(&query.ReadTxRequest{Hash: hash}),
	)
	require.Equal(t, connect.CodeUnavailable, connect.CodeOf(err))
	_, err = srv.ReadData(ctx, connect.NewRequest(&query.ReadDataRequest{
		Keys: [][]byte{hash},
	}))
	require.Equal(t, connect.CodeUnavailable, connect.CodeOf(err))
	require.Zero(t, db.datums)
}
