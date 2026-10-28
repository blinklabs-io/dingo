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
	"testing"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
	query "github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
)

// boundedUtxosStub scripts the bounded UTxO lookup and records the byte limit
// the handler passed to it.
type boundedUtxosStub struct {
	tipHeightLedgerStub
	gotBytes int64
	err      error
}

func (s *boundedUtxosStub) UtxosByRefsBounded(
	_ context.Context,
	_ []models.UtxoId,
	maxBytes int64,
) ([]models.Utxo, error) {
	s.gotBytes = maxBytes
	return nil, s.err
}

func readUtxosKeys() []*query.TxoRef {
	return []*query.TxoRef{{Hash: make([]byte, 32), Index: 0}}
}

// TestReadUtxos_PassesByteLimitToLookup covers the bound reaching the storage
// layer: the handler must hand the response limit to the lookup so CBOR
// loading stops there, not after every requested output is already in memory.
func TestReadUtxos_PassesByteLimitToLookup(t *testing.T) {
	t.Parallel()
	stub := &boundedUtxosStub{}
	srv := &queryServiceServer{utxorpc: NewUtxorpc(UtxorpcConfig{
		Logger:           discardLogger(),
		LedgerState:      stub,
		MaxResponseBytes: 4321,
	})}
	_, err := srv.ReadUtxos(
		context.Background(),
		connect.NewRequest(&query.ReadUtxosRequest{Keys: readUtxosKeys()}),
	)
	require.ErrorIs(t, err, database.ErrUtxoNotFound)
	require.Equal(t, int64(4321), stub.gotBytes)
}

// TestReadUtxos_LookupOverLimitIsResourceExhausted covers a lookup that stops
// because the loaded CBOR passed the limit. The caller must see the same
// ResourceExhausted a post-load budget check gives.
func TestReadUtxos_LookupOverLimitIsResourceExhausted(t *testing.T) {
	t.Parallel()
	stub := &boundedUtxosStub{err: database.ErrUtxoBytesExceeded}
	srv := &queryServiceServer{utxorpc: NewUtxorpc(UtxorpcConfig{
		Logger:      discardLogger(),
		LedgerState: stub,
	})}
	_, err := srv.ReadUtxos(
		context.Background(),
		connect.NewRequest(&query.ReadUtxosRequest{Keys: readUtxosKeys()}),
	)
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
}

// TestReadUtxos_CanceledAndDeadlineMapToConnectCodes covers a caller that has
// gone away: the lookup's context error must become Canceled or
// DeadlineExceeded rather than an internal error.
func TestReadUtxos_CanceledAndDeadlineMapToConnectCodes(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		ctx  func() (context.Context, context.CancelFunc)
		want connect.Code
	}{
		"canceled": {
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
			want: connect.CodeCanceled,
		},
		"deadline": {
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 0)
			},
			want: connect.CodeDeadlineExceeded,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := tc.ctx()
			defer cancel()
			stub := &boundedUtxosStub{err: ctx.Err()}
			srv := &queryServiceServer{utxorpc: NewUtxorpc(UtxorpcConfig{
				Logger:      discardLogger(),
				LedgerState: stub,
			})}
			_, err := srv.ReadUtxos(
				ctx,
				connect.NewRequest(
					&query.ReadUtxosRequest{Keys: readUtxosKeys()},
				),
			)
			require.Equal(t, tc.want, connect.CodeOf(err))
		})
	}
}

// TestUtxosByRefsBounded_StopsLoadingPastTheLimit runs the bounded lookup on
// the real ledger. The first output always fits; a second that pushes the
// running total past the limit stops the load with ErrUtxoBytesExceeded; no
// limit loads everything; a cancelled context stops before any load.
func TestUtxosByRefsBounded_StopsLoadingPastTheLimit(t *testing.T) {
	t.Parallel()
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 20})
	open := &queryServiceServer{utxorpc: NewUtxorpc(UtxorpcConfig{
		Logger: discardLogger(), EventBus: h.EB, LedgerState: h.LS,
	})}
	search, err := open.SearchUtxos(
		context.Background(),
		connect.NewRequest(&query.SearchUtxosRequest{MaxItems: 3}),
	)
	require.NoError(t, err)
	items := search.Msg.GetItems()
	require.Len(t, items, 3)
	refs := make([]models.UtxoId, len(items))
	var total int64
	for i, it := range items {
		refs[i] = models.UtxoId{
			Hash: it.GetTxoRef().GetHash(),
			Idx:  it.GetTxoRef().GetIndex(),
		}
		total += int64(len(it.GetNativeBytes()))
	}
	ctx := context.Background()

	got, err := h.LS.UtxosByRefsBounded(ctx, refs, 0)
	require.NoError(t, err, "no limit loads everything")
	require.Len(t, got, len(refs))

	got, err = h.LS.UtxosByRefsBounded(ctx, refs, total)
	require.NoError(t, err, "a total exactly at the limit is allowed")
	require.Len(t, got, len(refs))

	_, err = h.LS.UtxosByRefsBounded(ctx, refs, total-1)
	require.ErrorIs(t, err, database.ErrUtxoBytesExceeded)

	got, err = h.LS.UtxosByRefsBounded(ctx, refs[:1], 1)
	require.NoError(t, err, "a single output over the limit is kept")
	require.Len(t, got, 1)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = h.LS.UtxosByRefsBounded(canceled, refs, 0)
	require.ErrorIs(t, err, context.Canceled)
}
