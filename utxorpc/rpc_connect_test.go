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

// Connect RPC integration tests: in-memory preview ledger + httptest/h2c server,
// using the same generated Connect clients as production callers.

package utxorpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/mempool"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
	query "github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query/queryconnect"
	submit "github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit/submitconnect"
	sync "github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync/syncconnect"
	watch "github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch/watchconnect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// --- harness (preview fixture + h2c server) --------------------------------

type noopTxValidator struct{}

func (noopTxValidator) ValidateTx(gledger.Transaction) error { return nil }

func (noopTxValidator) ValidateTxWithOverlay(
	_ gledger.Transaction,
	_ map[string]struct{},
	_ map[string]lcommon.Utxo,
) error {
	return nil
}

// utxorpcConnectHarness wires preview genesis + immutable fixture blocks into an
// in-memory database, starts LedgerState (manual block processing), and serves
// the utxorpc Connect handlers over HTTP/2 cleartext (h2c) like production.
type utxorpcConnectHarness struct {
	EB     *event.EventBus
	DB     *database.Database
	LS     *ledger.LedgerState
	MP     *mempool.Mempool
	U      *Utxorpc
	Server *httptest.Server
	Client *http.Client
	// Tx hashes that were successfully indexed into metadata during harness setup.
	IndexedTxHashes [][]byte
}

type utxorpcHarnessOptions struct {
	numBlocks       int
	maxHistoryItems int
}

func newConnectH2CClient() *http.Client {
	return &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLS: func(network, addr string, _ *tls.Config) (net.Conn, error) {
				return net.Dial(network, addr)
			},
		},
	}
}

func testUtxorpcHTTPHandler(u *Utxorpc) http.Handler {
	mux := http.NewServeMux()
	compress1KB := connect.WithCompressMinBytes(1024)
	qp, qh := queryconnect.NewQueryServiceHandler(
		&queryServiceServer{utxorpc: u},
		compress1KB,
	)
	sp, sh := submitconnect.NewSubmitServiceHandler(
		&submitServiceServer{utxorpc: u},
		compress1KB,
	)
	yp, yh := syncconnect.NewSyncServiceHandler(
		&syncServiceServer{utxorpc: u},
		compress1KB,
	)
	wp, wh := watchconnect.NewWatchServiceHandler(
		&watchServiceServer{utxorpc: u},
		compress1KB,
	)
	mux.Handle(qp, qh)
	mux.Handle(sp, sh)
	mux.Handle(yp, yh)
	mux.Handle(wp, wh)
	return h2c.NewHandler(mux, &http2.Server{})
}

func newUtxorpcConnectHarness(t *testing.T, opts utxorpcHarnessOptions) *utxorpcConnectHarness {
	t.Helper()
	if opts.numBlocks < 2 {
		opts.numBlocks = 2
	}

	nodeCfg, err := cardano.NewCardanoNodeConfigFromEmbedFS(
		cardano.EmbeddedConfigFS,
		"preview/config.json",
	)
	require.NoError(t, err)

	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	blocks := loadTestChainBlocks(t, opts.numBlocks)
	for i := range blocks {
		require.NoError(t, db.BlockCreate(blocks[i], nil))
	}
	indexedTxHashes := indexFixtureTransactionsForReadTx(t, db, blocks)
	tip := blocks[len(blocks)-1]
	require.NoError(
		t,
		db.SetTip(
			ochainsync.Tip{
				Point:       ocommon.NewPoint(tip.Slot, tip.Hash),
				BlockNumber: tip.Number,
			},
			nil,
		),
	)

	// API-only bus: LedgerState must not subscribe here, otherwise its
	// Blockfetch handler can run before WaitForTx and stall Publish delivery.
	apiBus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { apiBus.Stop() })

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:              db,
		ChainManager:          cm,
		EventBus:              nil,
		Logger:                slog.New(slog.NewJSONHandler(io.Discard, nil)),
		CardanoNodeConfig:     nodeCfg,
		ManualBlockProcessing: true,
		DatabaseWorkerPoolConfig: ledger.DatabaseWorkerPoolConfig{
			Disabled: true,
		},
	})
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(ls))

	ledgerCtx, cancel := context.WithCancel(context.Background())
	require.NoError(t, ls.Start(ledgerCtx))
	t.Cleanup(func() {
		cancel()
		_ = ls.Close()
	})

	mp := mempool.NewMempool(mempool.MempoolConfig{
		Validator:       noopTxValidator{},
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:        apiBus,
		MempoolCapacity: 1 << 30,
	})
	t.Cleanup(func() { _ = mp.Stop(context.Background()) })

	maxHist := opts.maxHistoryItems
	if maxHist <= 0 {
		maxHist = DefaultMaxHistoryItems
	}
	u := NewUtxorpc(UtxorpcConfig{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:        apiBus,
		LedgerState:     ls,
		Mempool:         mp,
		MaxHistoryItems: maxHist,
	})

	srv := httptest.NewServer(testUtxorpcHTTPHandler(u))
	t.Cleanup(srv.Close)

	return &utxorpcConnectHarness{
		EB:              apiBus,
		DB:              db,
		LS:              ls,
		MP:              mp,
		U:               u,
		Server:          srv,
		Client:          newConnectH2CClient(),
		IndexedTxHashes: indexedTxHashes,
	}
}

func indexFixtureTransactionsForReadTx(
	t *testing.T,
	db *database.Database,
	blocks []models.Block,
) [][]byte {
	t.Helper()
	indexed := make([][]byte, 0, 64)
	for i := range blocks {
		mb := blocks[i]
		blk, err := gledger.NewBlockFromCbor(mb.Type, mb.Cbor)
		if err != nil {
			continue
		}
		txs := blk.Transactions()
		if len(txs) == 0 {
			continue
		}
		indexer := database.NewBlockIndexer(mb.Slot, mb.Hash)
		offsets, err := indexer.ComputeOffsets(mb.Cbor, blk)
		if err != nil {
			continue
		}
		point := ocommon.NewPoint(mb.Slot, mb.Hash)
		for j, tx := range txs {
			err := db.SetTransaction(
				tx,
				point,
				uint32(j),
				0,
				nil,
				nil,
				offsets,
				nil,
			)
			if err != nil {
				continue
			}
			indexed = append(indexed, append([]byte(nil), tx.Hash().Bytes()...))
		}
	}
	return indexed
}

// --- fixture helpers --------------------------------------------------------

func firstTxInFixtureBlocks(t *testing.T, numBlocks int) ([]byte, []byte, models.Block) {
	t.Helper()
	blocks := loadTestChainBlocks(t, numBlocks)
	for _, mb := range blocks {
		blk, err := gledger.NewBlockFromCbor(mb.Type, mb.Cbor)
		require.NoError(t, err)
		txs := blk.Transactions()
		if len(txs) == 0 {
			continue
		}
		tx := txs[0]
		return tx.Hash().Bytes(), tx.Cbor(), mb
	}
	t.Fatal("no transaction found in fixture blocks")
	return nil, nil, models.Block{}
}

// --- tests ------------------------------------------------------------------

func TestConnect_ReadParams(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 20})
	cli := queryconnect.NewQueryServiceClient(h.Client, h.Server.URL, connect.WithGRPC())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := cli.ReadParams(ctx, connect.NewRequest(&query.ReadParamsRequest{}))
	require.NoError(t, err)
	require.NotNil(t, out.Msg.GetValues())
	require.NotNil(t, out.Msg.GetValues().GetCardano())
	require.NotNil(t, out.Msg.GetLedgerTip())
	tip := h.LS.Tip()
	require.Equal(t, tip.Point.Slot, out.Msg.GetLedgerTip().GetSlot())
	require.Equal(t, tip.Point.Hash, out.Msg.GetLedgerTip().GetHash())
	require.Equal(t, tip.BlockNumber, out.Msg.GetLedgerTip().GetHeight())
}

func TestConnect_ReadEraSummary(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 20})
	cli := queryconnect.NewQueryServiceClient(h.Client, h.Server.URL, connect.WithGRPC())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := cli.ReadEraSummary(ctx, connect.NewRequest(&query.ReadEraSummaryRequest{}))
	require.NoError(t, err)
	s := out.Msg.GetCardano()
	require.NotNil(t, s)
	require.NotEmpty(t, s.GetSummaries())
	require.NotEmpty(t, s.GetSummaries()[0].GetName())
	require.NotNil(t, s.GetSummaries()[0].GetStart())
}

func TestConnect_ReadGenesis(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 5})
	cli := queryconnect.NewQueryServiceClient(h.Client, h.Server.URL, connect.WithGRPC())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := cli.ReadGenesis(ctx, connect.NewRequest(&query.ReadGenesisRequest{}))
	require.NoError(t, err)
	require.NotEmpty(t, out.Msg.GetCaip2())
	require.Equal(t, "cardano:preview", out.Msg.GetCaip2())
	require.NotNil(t, out.Msg.GetCardano())
}

func TestConnect_ReadTip(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 15})
	cli := syncconnect.NewSyncServiceClient(h.Client, h.Server.URL, connect.WithGRPC())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := cli.ReadTip(ctx, connect.NewRequest(&sync.ReadTipRequest{}))
	require.NoError(t, err)
	require.NotNil(t, out.Msg.GetTip())
	require.NotEmpty(t, out.Msg.GetTip().GetHash())
	tip := h.LS.Tip()
	require.Equal(t, tip.Point.Slot, out.Msg.GetTip().GetSlot())
	require.Equal(t, tip.Point.Hash, out.Msg.GetTip().GetHash())
	require.Equal(t, tip.BlockNumber, out.Msg.GetTip().GetHeight())
}

func TestConnect_FetchBlock(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 12})
	cli := syncconnect.NewSyncServiceClient(h.Client, h.Server.URL, connect.WithGRPC())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tip := h.LS.Tip()
	out, err := cli.FetchBlock(
		ctx,
		connect.NewRequest(&sync.FetchBlockRequest{
			Ref: []*sync.BlockRef{
				{Slot: tip.Point.Slot, Hash: tip.Point.Hash},
			},
		}),
	)
	require.NoError(t, err)
	require.Len(t, out.Msg.GetBlock(), 1)
	require.NotEmpty(t, out.Msg.GetBlock()[0].GetNativeBytes())
	blk, err := h.LS.GetBlock(tip.Point)
	require.NoError(t, err)
	require.Equal(t, blk.Cbor, out.Msg.GetBlock()[0].GetNativeBytes())
}

func TestConnect_DumpHistory(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 25})
	cli := syncconnect.NewSyncServiceClient(h.Client, h.Server.URL, connect.WithGRPC())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	blocks := loadTestChainBlocks(t, 25)
	out, err := cli.DumpHistory(
		ctx,
		connect.NewRequest(&sync.DumpHistoryRequest{
			StartToken: &sync.BlockRef{
				Slot:   blocks[0].Slot,
				Hash:   blocks[0].Hash,
				Height: blocks[0].Number,
			},
			MaxItems: 3,
		}),
	)
	require.NoError(t, err)
	require.Len(t, out.Msg.GetBlock(), 3)
	// start_token is exclusive in the server implementation, so the first page
	// begins at the block after the token.
	require.Equal(t, blocks[1].Cbor, out.Msg.GetBlock()[0].GetNativeBytes())
	require.Equal(t, blocks[2].Cbor, out.Msg.GetBlock()[1].GetNativeBytes())
	require.Equal(t, blocks[3].Cbor, out.Msg.GetBlock()[2].GetNativeBytes())
	require.NotNil(
		t,
		out.Msg.GetNextToken(),
		"history longer than maxItems must expose next_token for pagination",
	)
	require.Equal(t, blocks[3].Slot, out.Msg.GetNextToken().GetSlot())
	require.Equal(t, blocks[3].Hash, out.Msg.GetNextToken().GetHash())
	require.Equal(t, blocks[3].Number, out.Msg.GetNextToken().GetHeight())
	out2, err := cli.DumpHistory(
		ctx,
		connect.NewRequest(&sync.DumpHistoryRequest{
			StartToken: out.Msg.GetNextToken(),
			MaxItems:   10,
		}),
	)
	require.NoError(t, err)
	require.NotEmpty(t, out2.Msg.GetBlock())
	require.Equal(t, blocks[4].Cbor, out2.Msg.GetBlock()[0].GetNativeBytes())
}

func TestConnect_DumpHistory_StartTokenNotOnChain(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 10})
	cli := syncconnect.NewSyncServiceClient(h.Client, h.Server.URL, connect.WithGRPC())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err := cli.DumpHistory(
		ctx,
		connect.NewRequest(&sync.DumpHistoryRequest{
			StartToken: &sync.BlockRef{
				Slot:   999_999,
				Hash:   []byte{0xde, 0xad, 0xbe, 0xef},
				Height: 999_999,
			},
			MaxItems: 5,
		}),
	)
	require.Error(t, err)
	connErr, ok := err.(*connect.Error)
	require.True(t, ok, "expected connect.Error, got %T", err)
	// Depending on where validation happens (iterator lookup vs handler-level),
	// this can surface as InvalidArgument or Unknown wrapping ErrBlockNotFound.
	require.Contains(
		t,
		[]connect.Code{connect.CodeInvalidArgument, connect.CodeUnknown},
		connErr.Code(),
	)
}

func TestConnect_DumpHistory_MaxItemsExceeded(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{
		numBlocks:       10,
		maxHistoryItems: 50,
	})
	cli := syncconnect.NewSyncServiceClient(h.Client, h.Server.URL, connect.WithGRPC())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := cli.DumpHistory(
		ctx,
		connect.NewRequest(&sync.DumpHistoryRequest{
			MaxItems: 100,
		}),
	)
	require.Error(t, err)
	connErr, ok := err.(*connect.Error)
	require.True(t, ok, "expected connect.Error, got %T", err)
	require.Equal(t, connect.CodeInvalidArgument, connErr.Code())
	require.Contains(t, connErr.Message(), "maxItems 100 exceeds maximum of 50")
}

func TestConnect_SearchUtxos(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 20})
	cli := queryconnect.NewQueryServiceClient(h.Client, h.Server.URL, connect.WithGRPC())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := cli.SearchUtxos(
		ctx,
		connect.NewRequest(&query.SearchUtxosRequest{
			MaxItems: 5,
		}),
	)
	require.NoError(t, err)
	require.NotNil(t, out.Msg.GetLedgerTip())
	require.NotEmpty(t, out.Msg.GetItems(), "fixture should expose live UTxOs")
	require.LessOrEqual(t, len(out.Msg.GetItems()), 5)
	require.NotNil(t, out.Msg.GetItems()[0].GetTxoRef())
	require.NotEmpty(t, out.Msg.GetNextToken(), "pagination token expected for maxItems=5")

	out2, err := cli.SearchUtxos(
		ctx,
		connect.NewRequest(&query.SearchUtxosRequest{
			MaxItems:   5,
			StartToken: out.Msg.GetNextToken(),
		}),
	)
	require.NoError(t, err)
	require.NotEmpty(t, out2.Msg.GetItems())
	first1 := out.Msg.GetItems()[0].GetTxoRef()
	first2 := out2.Msg.GetItems()[0].GetTxoRef()
	require.False(
		t,
		bytes.Equal(first1.GetHash(), first2.GetHash()) &&
			first1.GetIndex() == first2.GetIndex(),
		"second page should advance past the first page",
	)
}

func TestConnect_ReadUtxos(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 20})
	cli := queryconnect.NewQueryServiceClient(h.Client, h.Server.URL, connect.WithGRPC())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	searchOut, err := cli.SearchUtxos(
		ctx,
		connect.NewRequest(&query.SearchUtxosRequest{MaxItems: 1}),
	)
	require.NoError(t, err)
	require.NotEmpty(
		t,
		searchOut.Msg.GetItems(),
		"fixture should expose at least one live UTxO",
	)
	ref := searchOut.Msg.GetItems()[0].GetTxoRef()
	out, err := cli.ReadUtxos(
		ctx,
		connect.NewRequest(&query.ReadUtxosRequest{
			Keys: []*query.TxoRef{ref},
		}),
	)
	require.NoError(t, err)
	require.Len(t, out.Msg.GetItems(), 1)
	gotRef := out.Msg.GetItems()[0].GetTxoRef()
	require.NotNil(t, gotRef)
	require.Equal(t, ref.GetHash(), gotRef.GetHash())
	require.Equal(t, ref.GetIndex(), gotRef.GetIndex())
	require.NotNil(t, out.Msg.GetLedgerTip())
}

func TestConnect_ReadData_EmptyKeys(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 5})
	cli := queryconnect.NewQueryServiceClient(h.Client, h.Server.URL, connect.WithGRPC())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := cli.ReadData(ctx, connect.NewRequest(&query.ReadDataRequest{}))
	require.NoError(t, err)
	require.Empty(t, out.Msg.GetValues())
	require.NotNil(t, out.Msg.GetLedgerTip())
	tip := h.LS.Tip()
	require.Equal(t, tip.Point.Slot, out.Msg.GetLedgerTip().GetSlot())
	require.Equal(t, tip.Point.Hash, out.Msg.GetLedgerTip().GetHash())
}

func TestConnect_ReadTx(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 40})
	cli := queryconnect.NewQueryServiceClient(h.Client, h.Server.URL, connect.WithGRPC())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NotEmpty(t, h.IndexedTxHashes, "harness must index fixture transactions")
	txHash := h.IndexedTxHashes[len(h.IndexedTxHashes)-1]
	out, err := cli.ReadTx(
		ctx,
		connect.NewRequest(&query.ReadTxRequest{Hash: txHash}),
	)
	require.NoError(t, err)
	require.NotNil(t, out.Msg.GetTx())
	require.NotNil(t, out.Msg.GetTx().GetCardano())
	require.NotEmpty(t, out.Msg.GetTx().GetNativeBytes())
	txType, err := gledger.DetermineTransactionType(out.Msg.GetTx().GetNativeBytes())
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(txType, out.Msg.GetTx().GetNativeBytes())
	require.NoError(t, err)
	require.Equal(t, txHash, tx.Hash().Bytes())
	rec, err := h.LS.TransactionByHash(txHash)
	require.NoError(t, err)
	require.NotNil(t, rec)
	require.NotNil(t, out.Msg.GetTx().GetBlockRef())
	require.Equal(t, rec.BlockHash, out.Msg.GetTx().GetBlockRef().GetHash())
	require.Equal(t, rec.Slot, out.Msg.GetTx().GetBlockRef().GetSlot())
	require.NotNil(t, out.Msg.GetLedgerTip())
}

func TestConnect_SubmitTx(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 40})
	_, txCbor, _ := firstTxInFixtureBlocks(t, 40)
	cli := submitconnect.NewSubmitServiceClient(h.Client, h.Server.URL, connect.WithGRPC())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := cli.SubmitTx(
		ctx,
		connect.NewRequest(&submit.SubmitTxRequest{
			Tx: &submit.AnyChainTx{
				Type: &submit.AnyChainTx_Raw{Raw: txCbor},
			},
		}),
	)
	require.NoError(t, err)
	require.NotEmpty(t, out.Msg.GetRef())
	txType, err := gledger.DetermineTransactionType(txCbor)
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(txType, txCbor)
	require.NoError(t, err)
	require.Equal(t, tx.Hash().Bytes(), out.Msg.GetRef())
	memTxs := h.MP.Transactions()
	found := false
	for _, mtx := range memTxs {
		if bytes.Equal(mtx.Cbor, txCbor) {
			found = true
			break
		}
	}
	require.True(t, found, "submitted tx must be present in mempool")
}

func TestConnect_ReadMempool(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 5})
	cli := submitconnect.NewSubmitServiceClient(h.Client, h.Server.URL, connect.WithGRPC())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := cli.ReadMempool(ctx, connect.NewRequest(&submit.ReadMempoolRequest{}))
	require.NoError(t, err)
	require.Empty(t, out.Msg.GetItems())
}

func TestConnect_WaitForTx_EmptyRefsClosesStream(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 5})
	cli := submitconnect.NewSubmitServiceClient(h.Client, h.Server.URL, connect.WithGRPC())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	stream, err := cli.WaitForTx(ctx, connect.NewRequest(&submit.WaitForTxRequest{}))
	require.NoError(t, err)
	require.False(t, stream.Receive(), "no refs means the handler returns without frames")
	require.NoError(t, stream.Err())
	cancel()
}

func TestConnect_EvalTx(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 40})
	_, txCbor, _ := firstTxInFixtureBlocks(t, 40)
	cli := submitconnect.NewSubmitServiceClient(h.Client, h.Server.URL, connect.WithGRPC())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := cli.EvalTx(
		ctx,
		connect.NewRequest(&submit.EvalTxRequest{
			Tx: &submit.AnyChainTx{
				Type: &submit.AnyChainTx_Raw{Raw: txCbor},
			},
		}),
	)
	require.NoError(t, err)
	report := out.Msg.GetReport().GetCardano()
	require.NotNil(t, report)
	if len(report.GetErrors()) > 0 {
		require.NotEmpty(t, report.GetErrors()[0].GetMsg())
	} else {
		require.NotNil(t, report.GetExUnits())
	}
}

func followTipStreamErr(t *testing.T, stream *connect.ServerStreamForClient[sync.FollowTipResponse]) string {
	t.Helper()
	if err := stream.Err(); err != nil {
		return err.Error()
	}
	return "stream closed without error"
}

func TestConnect_FollowTip_RollbackEmitsReset(t *testing.T) {
	const n = 8
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: n})
	blocks := loadTestChainBlocks(t, n)
	inter := blocks[5]
	roll := ocommon.NewPoint(inter.Slot, inter.Hash)
	require.NoError(t, h.LS.Chain().ValidateRollback(roll))

	cli := syncconnect.NewSyncServiceClient(h.Client, h.Server.URL, connect.WithGRPC())
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	stream, err := cli.FollowTip(ctx, connect.NewRequest(&sync.FollowTipRequest{
		Intersect: []*sync.BlockRef{
			{
				Slot:   inter.Slot,
				Hash:   append([]byte(nil), inter.Hash...),
				Height: inter.Number,
			},
		},
	}))
	require.NoError(t, err)

	for i := range 2 {
		require.True(
			t,
			stream.Receive(),
			"expected two Apply frames before rollback: %s",
			followTipStreamErr(t, stream),
		)
		apply, ok := stream.Msg().Action.(*sync.FollowTipResponse_Apply)
		require.True(t, ok, "expected Apply, got %T", stream.Msg().Action)
		require.NotNil(t, apply.Apply)
		require.NotEmpty(t, apply.Apply.GetNativeBytes())
		require.Equal(t, blocks[6+i].Cbor, apply.Apply.GetNativeBytes())
		require.NotNil(t, stream.Msg().GetTip())
	}

	require.NoError(t, h.LS.Chain().Rollback(roll))

	require.True(
		t,
		stream.Receive(),
		"expected Reset after chain rollback: %s",
		followTipStreamErr(t, stream),
	)
	reset, ok := stream.Msg().Action.(*sync.FollowTipResponse_Reset_)
	require.True(t, ok, "expected Reset action, got %T", stream.Msg().Action)
	require.Equal(t, roll.Slot, reset.Reset_.GetSlot())
	require.Equal(t, roll.Hash, reset.Reset_.GetHash())
	rollbackBlock, err := h.LS.GetBlock(roll)
	require.NoError(t, err)
	require.Equal(t, rollbackBlock.Number, reset.Reset_.GetHeight())
	rollbackTime, err := h.LS.SlotToTime(roll.Slot)
	require.NoError(t, err)
	require.Equal(t, uint64(rollbackTime.UnixMilli()), reset.Reset_.GetTimestamp())
	cancel()
}

func TestConnect_WatchTx_IdleEmptyForwardBlock(t *testing.T) {
	// Load a long prefix to locate an empty block, then trim the harness chain
	// so that empty block is the tip. Otherwise WatchTx keeps iterating forward
	// and may hit transactions that panic in gouroboros Utxorpc().
	scan := loadTestChainBlocks(t, 80)
	var cut int
	found := false
	for j := 6; j < len(scan); j++ {
		blk, err := gledger.NewBlockFromCbor(scan[j].Type, scan[j].Cbor)
		require.NoError(t, err)
		if len(blk.Transactions()) != 0 {
			continue
		}
		cut = j + 1
		found = true
		break
	}
	require.True(t, found, "no suitable empty block in fixture scan")
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: cut})
	blocks := loadTestChainBlocks(t, cut)
	parent := blocks[cut-2]
	emptyChild := blocks[cut-1]
	require.Equal(t, emptyChild.Slot, h.LS.Tip().Point.Slot)

	cli := watchconnect.NewWatchServiceClient(h.Client, h.Server.URL, connect.WithGRPC())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := cli.WatchTx(
		ctx,
		connect.NewRequest(&watch.WatchTxRequest{
			Intersect: []*watch.BlockRef{
				{
					Slot:   parent.Slot,
					Hash:   append([]byte(nil), parent.Hash...),
					Height: parent.Number,
				},
			},
		}),
	)
	require.NoError(t, err)
	if !stream.Receive() {
		t.Fatalf("WatchTx first frame: stream.Err()=%v", stream.Err())
	}
	idle, ok := stream.Msg().Action.(*watch.WatchTxResponse_Idle)
	require.True(t, ok, "expected Idle, got %T", stream.Msg().Action)
	require.NotNil(t, idle.Idle)
	require.Equal(t, emptyChild.Slot, idle.Idle.GetSlot())
	require.Equal(t, emptyChild.Number, idle.Idle.GetHeight())
	require.Equal(t, emptyChild.Hash, idle.Idle.GetHash())
	require.Equal(t, h.LS.Tip().Point.Hash, idle.Idle.GetHash())
	cancel()
}

func TestConnect_WaitForTx_ConfirmsOnBlockfetchEvent(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 40})
	txHash, _, mb := firstTxInFixtureBlocks(t, 40)
	blk, err := gledger.NewBlockFromCbor(mb.Type, mb.Cbor)
	require.NoError(t, err)

	cli := submitconnect.NewSubmitServiceClient(h.Client, h.Server.URL, connect.WithGRPC())
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	stopPublish := make(chan struct{})
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopPublish:
				return
			case <-ticker.C:
				h.EB.Publish(
					ledger.BlockfetchEventType,
					event.NewEvent(
						ledger.BlockfetchEventType,
						ledger.BlockfetchEvent{
							ConnectionId: ouroboros.ConnectionId{},
							Block:        blk,
							Point:        ocommon.NewPoint(mb.Slot, mb.Hash),
							Type:         uint(mb.Type),
							BatchDone:    false,
						},
					),
				)
			}
		}
	}()
	defer close(stopPublish)

	stream, err := cli.WaitForTx(
		ctx,
		connect.NewRequest(&submit.WaitForTxRequest{
			Ref: [][]byte{append([]byte(nil), txHash...)},
		}),
	)
	require.NoError(t, err)
	if !stream.Receive() {
		require.NoError(t, stream.Err())
		t.Fatal("WaitForTx stream closed without confirmation frame")
	}
	resp := stream.Msg()
	require.NotNil(t, resp)
	require.Equal(t, submit.Stage_STAGE_CONFIRMED, resp.GetStage())
	require.Equal(t, txHash, resp.GetRef())
	require.False(t, stream.Receive(), "handler returns after confirming all refs")
	require.NoError(t, stream.Err())
	cancel()
}

func TestConnect_WatchMempool_StreamsOnAddTransactionEvent(t *testing.T) {
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 40})
	txHash, txCbor, _ := firstTxInFixtureBlocks(t, 40)
	txType, err := gledger.DetermineTransactionType(txCbor)
	require.NoError(t, err)

	cli := submitconnect.NewSubmitServiceClient(h.Client, h.Server.URL, connect.WithGRPC())
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	stopPublish := make(chan struct{})
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopPublish:
				return
			case <-ticker.C:
				h.EB.Publish(
					mempool.AddTransactionEventType,
					event.NewEvent(
						mempool.AddTransactionEventType,
						mempool.AddTransactionEvent{
							Hash: hex.EncodeToString(txHash),
							Type: txType,
							Body: append([]byte(nil), txCbor...),
						},
					),
				)
			}
		}
	}()
	defer close(stopPublish)

	stream, err := cli.WatchMempool(
		ctx,
		connect.NewRequest(&submit.WatchMempoolRequest{}),
	)
	require.NoError(t, err)
	if !stream.Receive() {
		require.NoError(t, stream.Err())
		t.Fatal("WatchMempool stream closed without event frame")
	}
	resp := stream.Msg()
	require.NotNil(t, resp.GetTx())
	require.Equal(t, submit.Stage_STAGE_MEMPOOL, resp.GetTx().GetStage())
	require.True(t, bytes.Equal(txCbor, resp.GetTx().GetNativeBytes()))
	outTxType, err := gledger.DetermineTransactionType(resp.GetTx().GetNativeBytes())
	require.NoError(t, err)
	outTx, err := gledger.NewTransactionFromCbor(
		outTxType,
		resp.GetTx().GetNativeBytes(),
	)
	require.NoError(t, err)
	require.Equal(t, txHash, outTx.Hash().Bytes())
	cancel()
}
