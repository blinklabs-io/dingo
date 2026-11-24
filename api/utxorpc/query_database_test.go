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
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/livesettest"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/require"
	utxorpcCardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
	query "github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query/queryconnect"
)

// A core-tier node has no IndexerDB: the QueryService reads live outputs by
// reference from the ledger's UTxO set and refuses what only history can
// answer, rather than reading the ledger's database, which holds no UTxO,
// datum or transaction rows.
func TestCoreQueryServiceReadsTheLedgerAndRefusesHistory(t *testing.T) {
	t.Parallel()

	// The api harness finds a live output of the shared fixture chain.
	api := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 20})
	apiCli := queryconnect.NewQueryServiceClient(
		api.Client, api.Server.URL, connect.WithGRPC(),
	)
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

	// The budget covers the RPCs only, not building the harnesses.
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	search, err := apiCli.SearchUtxos(
		ctx, connect.NewRequest(&query.SearchUtxosRequest{MaxItems: 1}),
	)
	require.NoError(t, err)
	require.NotEmpty(t, search.Msg.GetItems())
	ref := search.Msg.GetItems()[0].GetTxoRef()

	read, err := cli.ReadUtxos(ctx, connect.NewRequest(
		&query.ReadUtxosRequest{Keys: []*query.TxoRef{ref}},
	))
	require.NoError(t, err)
	require.Len(t, read.Msg.GetItems(), 1)
	require.Equal(t, ref.GetHash(), read.Msg.GetItems()[0].GetTxoRef().GetHash())
	require.Equal(t, ref.GetIndex(), read.Msg.GetItems()[0].GetTxoRef().GetIndex())
	tip := core.LS.Tip()
	require.Equal(t, tip.Point.Slot, read.Msg.GetLedgerTip().GetSlot())

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

func coreTestOutput(
	t *testing.T,
	payment, stake byte,
	datum *data.PlutusData,
) []byte {
	t.Helper()
	var stakeKey []byte
	addrType := uint8(lcommon.AddressTypeKeyNone)
	if stake != 0 {
		stakeKey = bytes.Repeat([]byte{stake}, lcommon.AddressHashSize)
		addrType = lcommon.AddressTypeKeyKey
	}
	addr, err := lcommon.NewAddressFromParts(
		addrType,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{payment}, lcommon.AddressHashSize),
		stakeKey,
	)
	require.NoError(t, err)
	out := babbage.BabbageTransactionOutput{
		OutputAddress: addr,
		OutputAmount:  mary.MaryTransactionOutputValue{Amount: 2_000_000},
	}
	if datum != nil {
		raw, err := data.Encode(*datum)
		require.NoError(t, err)
		optionCbor, err := cbor.Encode(
			[]any{babbage.DatumOptionTypeData, cbor.WrappedCbor(raw)},
		)
		require.NoError(t, err)
		var opt babbage.BabbageTransactionOutputDatumOption
		require.NoError(t, opt.UnmarshalCBOR(optionCbor))
		out.DatumOption = &opt
	}
	raw, err := cbor.Encode(&out)
	require.NoError(t, err)
	return raw
}

func addressPredicate(pattern *utxorpcCardano.AddressPattern) *query.UtxoPredicate {
	return &query.UtxoPredicate{Match: &query.AnyUtxoPattern{
		UtxoPattern: &query.AnyUtxoPattern_Cardano{
			Cardano: &utxorpcCardano.TxOutputPattern{Address: pattern},
		},
	}}
}

// The core-tier QueryService answers every UTxO read from the ledger's live
// set. The outputs here exist only in that set, which a core node's consensus
// database does not hold, so a read of any other store finds none of them.
func TestCoreQueryServiceServesLiveSetOnlyOutputs(t *testing.T) {
	t.Parallel()
	// A page examines at most maxHistoryItems+1 candidates.
	const maxHistoryItems = 3
	core := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{
		numBlocks:       20,
		maxHistoryItems: maxHistoryItems,
		tune: func(cfg *UtxorpcConfig) {
			ledger, ok := cfg.LedgerState.(QueryLedger)
			require.True(t, ok)
			cfg.QueryDatabase = NewLedgerQueryDatabase(ledger)
		},
	})
	cli := queryconnect.NewQueryServiceClient(
		core.Client, core.Server.URL, connect.WithGRPC(),
	)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	datum := data.NewInteger(big.NewInt(4242))
	targetA := coreTestOutput(t, 0xA1, 0xB1, nil)
	targetDatum := coreTestOutput(t, 0xA2, 0, &datum)
	other := coreTestOutput(t, 0xC1, 0, nil)
	txid := func(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }

	// Outputs of other addresses sort before every target by reference and
	// outnumber the candidates a page examines.
	var added []ledger.UtxoOutput
	for i := range 2 * (maxHistoryItems + 1) {
		added = append(added, ledger.UtxoOutput{
			Ref:  models.UtxoId{Hash: txid(0x01), Idx: uint32(i)}, //nolint:gosec // small test value
			Cbor: other,
		})
	}
	added = append(added,
		ledger.UtxoOutput{Ref: models.UtxoId{Hash: txid(0x10), Idx: 0}, Cbor: targetA},
		ledger.UtxoOutput{Ref: models.UtxoId{Hash: txid(0x10), Idx: 1}, Cbor: targetA},
		ledger.UtxoOutput{Ref: models.UtxoId{Hash: txid(0x11), Idx: 0}, Cbor: targetA},
		ledger.UtxoOutput{Ref: models.UtxoId{Hash: txid(0x12), Idx: 0}, Cbor: targetA},
		ledger.UtxoOutput{Ref: models.UtxoId{Hash: txid(0x20), Idx: 0}, Cbor: targetDatum},
	)
	set := livesettest.OpenLedgerDB(t, core.DB)
	tip := core.LS.Tip().Point
	require.NoError(t, set.ApplyUtxoChanges(nil, []ledger.UtxoEvent{{
		Point: tip, Added: added,
	}}))
	// 0x12#0 is spent after it was created.
	require.NoError(t, set.ApplyUtxoChanges(nil, []ledger.UtxoEvent{{
		Point:   tip,
		Removed: []models.UtxoId{{Hash: txid(0x12), Idx: 0}},
	}}))

	liveRef := &query.TxoRef{Hash: txid(0x10), Index: 1}
	spentRef := &query.TxoRef{Hash: txid(0x12), Index: 0}
	read, err := cli.ReadUtxos(ctx, connect.NewRequest(
		&query.ReadUtxosRequest{Keys: []*query.TxoRef{liveRef}},
	))
	require.NoError(t, err)
	require.Len(t, read.Msg.GetItems(), 1)
	require.Equal(t, liveRef.GetHash(), read.Msg.GetItems()[0].GetTxoRef().GetHash())
	_, err = cli.ReadUtxos(ctx, connect.NewRequest(
		&query.ReadUtxosRequest{Keys: []*query.TxoRef{spentRef}},
	))
	require.Error(t, err)

	// Paging over an address returns its live outputs in reference order,
	// each once, and never the spent one.
	pred := addressPredicate(&utxorpcCardano.AddressPattern{
		PaymentPart: bytes.Repeat([]byte{0xA1}, lcommon.AddressHashSize),
	})
	var got []string
	token := ""
	for pages := 0; ; pages++ {
		require.Less(t, pages, 10, "paging did not end")
		res, err := cli.SearchUtxos(ctx, connect.NewRequest(
			&query.SearchUtxosRequest{
				Predicate: pred, MaxItems: 1, StartToken: token,
			},
		))
		require.NoError(t, err)
		for _, item := range res.Msg.GetItems() {
			got = append(got, fmt.Sprintf(
				"%x#%d", item.GetTxoRef().GetHash()[:1], item.GetTxoRef().GetIndex(),
			))
		}
		token = res.Msg.GetNextToken()
		if token == "" {
			break
		}
	}
	require.Equal(t, []string{"10#0", "10#1", "11#0"}, got)

	// The stake credential and the exact address find the same outputs.
	addrA, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey, lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xA1}, lcommon.AddressHashSize),
		bytes.Repeat([]byte{0xB1}, lcommon.AddressHashSize),
	)
	require.NoError(t, err)
	rawA, err := addrA.Bytes()
	require.NoError(t, err)
	for _, p := range []*utxorpcCardano.AddressPattern{
		{ExactAddress: rawA},
		{DelegationPart: bytes.Repeat([]byte{0xB1}, lcommon.AddressHashSize)},
	} {
		res, err := cli.SearchUtxos(ctx, connect.NewRequest(
			&query.SearchUtxosRequest{Predicate: addressPredicate(p)},
		))
		require.NoError(t, err)
		require.Len(t, res.Msg.GetItems(), 3)
	}

	// A live page reports each output's lovelace, which the set stores only
	// inside the CBOR.
	livePage, err := core.LS.UtxosByAddressBoundedPage(
		ctx,
		&models.UtxoWithOrderingQuery{
			Limit: 10,
			AddressPatterns: []models.UtxoAddressPattern{{
				PaymentPart: bytes.Repeat([]byte{0xA1}, lcommon.AddressHashSize),
			}},
		},
		100, 1<<20,
	)
	require.NoError(t, err)
	require.Len(t, livePage.Utxos, 3)
	for _, u := range livePage.Utxos {
		require.EqualValues(t, 2_000_000, u.Amount)
	}

	// An address with nothing live finds nothing.
	res, err := cli.SearchUtxos(ctx, connect.NewRequest(
		&query.SearchUtxosRequest{Predicate: addressPredicate(
			&utxorpcCardano.AddressPattern{
				PaymentPart: bytes.Repeat([]byte{0xEE}, lcommon.AddressHashSize),
			},
		)},
	))
	require.NoError(t, err)
	require.Empty(t, res.Msg.GetItems())

	// An inline datum of a live output is found by its hash.
	decoded, err := gledger.NewTransactionOutputFromCbor(targetDatum)
	require.NoError(t, err)
	hash := decoded.DatumHash()
	require.NotNil(t, hash)
	rd, err := cli.ReadData(ctx, connect.NewRequest(
		&query.ReadDataRequest{Keys: [][]byte{hash.Bytes()}},
	))
	require.NoError(t, err)
	require.Len(t, rd.Msg.GetValues(), 1)
	_, err = cli.ReadData(ctx, connect.NewRequest(
		&query.ReadDataRequest{Keys: [][]byte{bytes.Repeat([]byte{0x77}, 32)}},
	))
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}
