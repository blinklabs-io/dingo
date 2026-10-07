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
	"fmt"
	"io"
	"log/slog"
	"strings"
	gosync "sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync"
)

// boundsRecordingMempool serves canned transactions and records the bounds a
// handler asked for, so a test can tell a bounded read from a full snapshot.
// It applies the bounds the same way the real mempool does for items only;
// byte semantics are covered in the mempool package.
type boundsRecordingMempool struct {
	mu       gosync.Mutex
	txs      []mempool.MempoolTransaction
	gotItems int
	gotBytes int64
	entered  chan struct{}
	release  chan struct{}
}

func newBoundsRecordingMempool(n int) *boundsRecordingMempool {
	txs := make([]mempool.MempoolTransaction, n)
	for i := range txs {
		txs[i] = mempool.MempoolTransaction{
			Hash: fmt.Sprintf("hash-%d", i),
			Cbor: fmt.Appendf(nil, "cbor-%d", i),
		}
	}
	return &boundsRecordingMempool{txs: txs}
}

func (m *boundsRecordingMempool) AddTransaction(uint, []byte) error {
	return nil
}

func (m *boundsRecordingMempool) TransactionsBounded(
	maxItems int,
	maxBytes int64,
) ([]mempool.MempoolTransaction, int) {
	m.mu.Lock()
	m.gotItems, m.gotBytes = maxItems, maxBytes
	m.mu.Unlock()
	if m.entered != nil {
		m.entered <- struct{}{}
		<-m.release
	}
	out := m.txs
	if maxItems > 0 && len(out) > maxItems {
		out = out[:maxItems]
	}
	return out, len(m.txs)
}

func (m *boundsRecordingMempool) bounds() (int, int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gotItems, m.gotBytes
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func TestReadMempool_BoundedByItemBudget(t *testing.T) {
	t.Parallel()
	const maxItems = 3
	tests := []struct {
		name     string
		poolSize int
		want     int
	}{
		{"below budget", maxItems - 1, maxItems - 1},
		{"at budget", maxItems, maxItems},
		{"just over budget", maxItems + 1, maxItems},
		{"far over budget", 50 * maxItems, maxItems},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mp := newBoundsRecordingMempool(tc.poolSize)
			u := NewUtxorpc(UtxorpcConfig{
				Logger:           discardLogger(),
				Mempool:          mp,
				MaxMempoolItems:  maxItems,
				MaxResponseBytes: 4096,
			})
			srv := &submitServiceServer{utxorpc: u}

			out, err := srv.ReadMempool(
				context.Background(),
				connect.NewRequest(&submit.ReadMempoolRequest{}),
			)
			require.NoError(t, err)
			require.Len(t, out.Msg.GetItems(), tc.want)

			gotItems, gotBytes := mp.bounds()
			require.Equal(
				t,
				maxItems,
				gotItems,
				"handler must ask the mempool for a bounded read, not a full snapshot",
			)
			require.Equal(t, int64(4096), gotBytes)
		})
	}
}

func TestBulkRequests_ConcurrencyBudget(t *testing.T) {
	t.Parallel()
	const budget = 2
	mp := newBoundsRecordingMempool(1)
	mp.entered = make(chan struct{}, 8)
	mp.release = make(chan struct{})
	var releaseOnce gosync.Once
	release := func() { releaseOnce.Do(func() { close(mp.release) }) }
	defer release()

	u := NewUtxorpc(UtxorpcConfig{
		Logger:                    discardLogger(),
		Mempool:                   mp,
		MaxConcurrentBulkRequests: budget,
	})
	submitSrv := &submitServiceServer{utxorpc: u}
	querySrv := &queryServiceServer{utxorpc: u}

	var wg gosync.WaitGroup
	errs := make(chan error, budget)
	for range budget {
		wg.Go(func() {
			_, err := submitSrv.ReadMempool(
				context.Background(),
				connect.NewRequest(&submit.ReadMempoolRequest{}),
			)
			errs <- err
		})
	}
	for range budget {
		testutil.RequireReceive(
			t, mp.entered, 5*time.Second, "handler entered the mempool read",
		)
	}

	// Every slot is held: any further bulk request, on any bulk handler,
	// is refused instead of queueing behind the held ones. The calls run in
	// goroutines so that a handler that queues surfaces as a timeout here
	// rather than a hung test.
	refused := make(chan error, 2)
	go func() {
		_, err := submitSrv.ReadMempool(
			context.Background(),
			connect.NewRequest(&submit.ReadMempoolRequest{}),
		)
		refused <- err
	}()
	go func() {
		_, err := querySrv.ReadData(
			context.Background(),
			connect.NewRequest(&query.ReadDataRequest{
				Keys: [][]byte{bytes.Repeat([]byte{0x01}, 32)},
			}),
		)
		refused <- err
	}()
	for range 2 {
		err := testutil.RequireReceive(
			t, refused, 5*time.Second,
			"over-budget bulk request must be refused, not queued",
		)
		require.Error(t, err)
		require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
	}

	release()
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	// Slots are returned: the same request now succeeds.
	_, err := submitSrv.ReadMempool(
		context.Background(),
		connect.NewRequest(&submit.ReadMempoolRequest{}),
	)
	require.NoError(t, err)
}

// TestBulkRequests_LogFixedSizeMetadata drives each unary bulk handler with a
// request whose caller-controlled fields are large and recognizable, and
// requires that none of it reaches the log.
func TestBulkRequests_LogFixedSizeMetadata(t *testing.T) {
	t.Parallel()
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 5})
	var logBuf bytes.Buffer
	var logMu gosync.Mutex
	u := NewUtxorpc(UtxorpcConfig{
		Logger: slog.New(slog.NewJSONHandler(
			writerFunc(func(p []byte) (int, error) {
				logMu.Lock()
				defer logMu.Unlock()
				return logBuf.Write(p)
			}), nil,
		)),
		EventBus:    h.EB,
		LedgerState: h.LS,
		Mempool:     h.MP,
	})
	querySrv := &queryServiceServer{utxorpc: u}
	syncSrv := &syncServiceServer{utxorpc: u}
	ctx := context.Background()

	marker := bytes.Repeat([]byte{0xAB}, 32)
	const markerHex = "abababababababab"
	const markerText = "MARKER-MARKER-MARKER-MARKER"
	keys := make([]*query.TxoRef, 100)
	for i := range keys {
		keys[i] = &query.TxoRef{Hash: marker, Index: uint32(i)} //nolint:gosec
	}
	dataKeys := [][]byte{marker, marker, marker, marker}

	_, _ = querySrv.ReadUtxos(ctx, connect.NewRequest(
		&query.ReadUtxosRequest{Keys: keys}))
	_, _ = querySrv.ReadData(ctx, connect.NewRequest(
		&query.ReadDataRequest{Keys: dataKeys}))
	_, _ = querySrv.SearchUtxos(ctx, connect.NewRequest(
		&query.SearchUtxosRequest{
			StartToken: strings.Repeat(markerText, 100),
		}))
	_, _ = querySrv.ReadTx(ctx, connect.NewRequest(
		&query.ReadTxRequest{Hash: marker}))
	_, _ = syncSrv.FetchBlock(ctx, connect.NewRequest(
		&sync.FetchBlockRequest{Ref: []*sync.BlockRef{
			{Slot: 1, Hash: marker}, {Slot: 2, Hash: marker},
		}}))
	_, _ = syncSrv.DumpHistory(ctx, connect.NewRequest(
		&sync.DumpHistoryRequest{
			StartToken: &sync.BlockRef{Slot: 1, Hash: marker},
		}))

	logMu.Lock()
	defer logMu.Unlock()
	logged := logBuf.String()
	require.NotEmpty(t, logged, "handlers should still log request metadata")
	require.NotContains(t, logged, markerHex)
	require.NotContains(t, logged, "MARKER")
	require.NotContains(t, logged, "xab")
	for line := range strings.SplitSeq(strings.TrimSpace(logged), "\n") {
		require.LessOrEqual(t, len(line), 512,
			"log record size must not scale with the request: %s", line)
	}
}

type writerFunc func(p []byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func TestReadUtxos_ResponseByteBudget(t *testing.T) {
	t.Parallel()
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 20})
	ctx := context.Background()

	open := &queryServiceServer{utxorpc: NewUtxorpc(UtxorpcConfig{
		Logger: discardLogger(), EventBus: h.EB, LedgerState: h.LS,
	})}
	search, err := open.SearchUtxos(ctx, connect.NewRequest(
		&query.SearchUtxosRequest{MaxItems: 3}))
	require.NoError(t, err)
	items := search.Msg.GetItems()
	require.Len(t, items, 3)
	keys := make([]*query.TxoRef, len(items))
	var total int64
	for i, it := range items {
		keys[i] = it.GetTxoRef()
		total += int64(len(it.GetNativeBytes()))
	}

	srvWith := func(budget int64) *queryServiceServer {
		return &queryServiceServer{utxorpc: NewUtxorpc(UtxorpcConfig{
			Logger: discardLogger(), EventBus: h.EB, LedgerState: h.LS,
			MaxResponseBytes: budget,
		})}
	}
	req := func() *connect.Request[query.ReadUtxosRequest] {
		return connect.NewRequest(&query.ReadUtxosRequest{Keys: keys})
	}

	out, err := srvWith(total).ReadUtxos(ctx, req())
	require.NoError(t, err, "a response exactly at the budget is allowed")
	require.Len(t, out.Msg.GetItems(), len(keys))

	_, err = srvWith(total-1).ReadUtxos(ctx, req())
	require.Error(t, err, "one byte over the budget is refused")
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
}

func TestSearchUtxos_PageByteBudget(t *testing.T) {
	t.Parallel()
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 20})
	ctx := context.Background()

	srvWith := func(budget int64) *queryServiceServer {
		return &queryServiceServer{utxorpc: NewUtxorpc(UtxorpcConfig{
			Logger: discardLogger(), EventBus: h.EB, LedgerState: h.LS,
			MaxResponseBytes: budget,
		})}
	}
	search := func(budget int64) *query.SearchUtxosResponse {
		out, err := srvWith(budget).SearchUtxos(ctx, connect.NewRequest(
			&query.SearchUtxosRequest{MaxItems: 5}))
		require.NoError(t, err)
		return out.Msg
	}

	full := search(0)
	require.Len(t, full.GetItems(), 5)
	sizeOf := func(n int) int64 {
		var total int64
		for _, it := range full.GetItems()[:n] {
			total += int64(len(it.GetNativeBytes()))
		}
		return total
	}

	atTwo := search(sizeOf(2))
	require.Len(t, atTwo.GetItems(), 2, "budget exactly fits two items")
	require.NotEmpty(t, atTwo.GetNextToken(),
		"a byte-truncated page must be resumable")

	underTwo := search(sizeOf(2) - 1)
	require.Len(t, underTwo.GetItems(), 1, "one byte short drops the second")
	require.NotEmpty(t, underTwo.GetNextToken())

	tiny := search(1)
	require.Len(t, tiny.GetItems(), 1,
		"a budget below one item still makes progress")
	require.NotEmpty(t, tiny.GetNextToken())

	resumed, err := srvWith(sizeOf(2)).SearchUtxos(ctx, connect.NewRequest(
		&query.SearchUtxosRequest{
			MaxItems: 5, StartToken: atTwo.GetNextToken(),
		}))
	require.NoError(t, err)
	require.Equal(t,
		full.GetItems()[2].GetTxoRef().GetHash(),
		resumed.Msg.GetItems()[0].GetTxoRef().GetHash(),
		"resuming from the token continues after the truncated page")
}

func TestNewUtxorpc_DefaultsBulkBudgets(t *testing.T) {
	t.Parallel()
	u := NewUtxorpc(UtxorpcConfig{})
	require.Equal(t, DefaultMaxMempoolItems, u.config.MaxMempoolItems)
	require.Equal(t, int64(DefaultMaxResponseBytes), u.config.MaxResponseBytes)
	require.Equal(t,
		DefaultMaxConcurrentBulkRequests, u.config.MaxConcurrentBulkRequests)
	require.Equal(t, DefaultMaxConcurrentBulkRequests, cap(u.bulkSlots))
}

// datumBudgetStub serves fixed datums so ReadData's byte budget can be driven
// without a chain.
type datumBudgetStub struct {
	tipHeightLedgerStub
	datums map[string]*models.Datum
}

func (s *datumBudgetStub) Datum(key []byte) (*models.Datum, error) {
	if d, ok := s.datums[string(key)]; ok {
		return d, nil
	}
	return nil, database.ErrDatumNotFound
}

func TestReadData_ResponseByteBudget(t *testing.T) {
	t.Parallel()
	stub := &datumBudgetStub{datums: map[string]*models.Datum{}}
	keys := make([][]byte, 3)
	var sizes []int64
	for i := range keys {
		keys[i] = bytes.Repeat([]byte{byte(i + 1)}, 32)
		// A Plutus bytes datum: CBOR byte string of 32+i bytes.
		raw := append(
			[]byte{0x58, byte(32 + i)},
			bytes.Repeat([]byte{0xCD}, 32+i)...,
		)
		stub.datums[string(keys[i])] = &models.Datum{
			Hash:     keys[i],
			RawDatum: raw,
		}
		sizes = append(sizes, int64(len(raw)))
	}
	total := sizes[0] + sizes[1] + sizes[2]

	readWith := func(budget int64, keys [][]byte) (*query.ReadDataResponse, error) {
		srv := &queryServiceServer{utxorpc: NewUtxorpc(UtxorpcConfig{
			Logger:           discardLogger(),
			LedgerState:      stub,
			MaxResponseBytes: budget,
		})}
		out, err := srv.ReadData(context.Background(),
			connect.NewRequest(&query.ReadDataRequest{Keys: keys}))
		if err != nil {
			return nil, err
		}
		return out.Msg, nil
	}

	out, err := readWith(total, keys)
	require.NoError(t, err, "a response exactly at the budget is allowed")
	require.Len(t, out.GetValues(), len(keys))

	_, err = readWith(total-1, keys)
	require.Error(t, err, "one byte over the budget is refused")
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))

	out, err = readWith(1, keys[:1])
	require.NoError(t, err, "a single item larger than the budget is kept")
	require.Len(t, out.GetValues(), 1)
}

func TestFetchBlock_ResponseByteBudget(t *testing.T) {
	t.Parallel()
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 5})
	blocks := loadTestChainBlocks(t, 5)
	refs := []*sync.BlockRef{
		{Slot: blocks[1].Slot, Hash: blocks[1].Hash},
		{Slot: blocks[2].Slot, Hash: blocks[2].Hash},
	}
	total := int64(len(blocks[1].Cbor) + len(blocks[2].Cbor))

	fetchWith := func(
		budget int64,
		refs []*sync.BlockRef,
	) (*sync.FetchBlockResponse, error) {
		srv := &syncServiceServer{utxorpc: NewUtxorpc(UtxorpcConfig{
			Logger:           discardLogger(),
			EventBus:         h.EB,
			LedgerState:      h.LS,
			MaxResponseBytes: budget,
		})}
		out, err := srv.FetchBlock(context.Background(),
			connect.NewRequest(&sync.FetchBlockRequest{Ref: refs}))
		if err != nil {
			return nil, err
		}
		return out.Msg, nil
	}

	out, err := fetchWith(total, refs)
	require.NoError(t, err, "a response exactly at the budget is allowed")
	require.Len(t, out.GetBlock(), len(refs))

	_, err = fetchWith(total-1, refs)
	require.Error(t, err, "one byte over the budget is refused")
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))

	out, err = fetchWith(1, refs[:1])
	require.NoError(t, err, "a single block larger than the budget is kept")
	require.Len(t, out.GetBlock(), 1)
}
