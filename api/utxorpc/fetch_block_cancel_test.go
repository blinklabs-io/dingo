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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/database/models"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
	sync "github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync"
)

type fetchBlockCtxKey struct{}

// blockLookupStub scripts the context-aware block read FetchBlock uses.
type blockLookupStub struct {
	tipHeightLedgerStub
	lookup func(ctx context.Context, point ocommon.Point) (models.Block, error)
	calls  atomic.Int32
}

func (s *blockLookupStub) GetBlockContext(
	ctx context.Context,
	point ocommon.Point,
) (models.Block, error) {
	s.calls.Add(1)
	return s.lookup(ctx, point)
}

// fetchBlockWith runs one FetchBlock for a single block ref against a server
// whose only bulk slot is the one that request takes, and returns the server
// so the caller can check the slot afterwards.
func fetchBlockWith(
	ctx context.Context,
	stub *blockLookupStub,
) (*Utxorpc, error) {
	u := NewUtxorpc(UtxorpcConfig{
		Logger:                    discardLogger(),
		LedgerState:               stub,
		MaxConcurrentBulkRequests: 1,
	})
	srv := &syncServiceServer{utxorpc: u}
	_, err := srv.FetchBlock(ctx, connect.NewRequest(&sync.FetchBlockRequest{
		Ref: []*sync.BlockRef{{Slot: 1, Hash: make([]byte, 32)}},
	}))
	return u, err
}

// requireBulkSlotFree fails when a finished request still holds the only
// bulk slot.
func requireBulkSlotFree(t *testing.T, u *Utxorpc) {
	t.Helper()
	release, err := u.acquireBulk(context.Background())
	require.NoError(t, err, "bulk slot was not released")
	release()
}

// TestFetchBlock_LookupReceivesRequestContext covers the storage read being
// bound to the RPC's context instead of context.Background.
func TestFetchBlock_LookupReceivesRequestContext(t *testing.T) {
	t.Parallel()
	ctx := context.WithValue(context.Background(), fetchBlockCtxKey{}, "rpc")
	var got atomic.Value
	stub := &blockLookupStub{
		lookup: func(
			ctx context.Context,
			_ ocommon.Point,
		) (models.Block, error) {
			got.Store(ctx.Value(fetchBlockCtxKey{}))
			return models.Block{}, fmt.Errorf("stop here")
		},
	}
	_, err := fetchBlockWith(ctx, stub)
	require.Error(t, err)
	require.Equal(t, "rpc", got.Load())
}

// TestFetchBlock_CanceledBeforeLookupReadsNothing covers a caller that has
// already given up when its turn comes. FetchBlock must return Canceled
// without touching storage, so an abandoned request costs no backend work.
func TestFetchBlock_CanceledBeforeLookupReadsNothing(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stub := &blockLookupStub{
		lookup: func(context.Context, ocommon.Point) (models.Block, error) {
			return models.Block{}, nil
		},
	}
	u, err := fetchBlockWith(ctx, stub)
	require.Equal(t, connect.CodeCanceled, connect.CodeOf(err))
	require.Zero(t, stub.calls.Load(), "no block read for a canceled request")
	requireBulkSlotFree(t, u)
}

// TestFetchBlock_CancelDuringLookupMapsToCanceled covers a caller that cancels
// while the block read is running. The read's wrapped context error must
// still surface as Canceled, not as a generic internal error, and the bulk
// slot must be released.
func TestFetchBlock_CancelDuringLookupMapsToCanceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stub := &blockLookupStub{
		lookup: func(
			ctx context.Context,
			_ ocommon.Point,
		) (models.Block, error) {
			cancel()
			<-ctx.Done()
			return models.Block{}, fmt.Errorf("read: %w", ctx.Err())
		},
	}
	u, err := fetchBlockWith(ctx, stub)
	require.Equal(t, connect.CodeCanceled, connect.CodeOf(err))
	requireBulkSlotFree(t, u)
}

// TestFetchBlock_DeadlineDuringLookupMapsToDeadlineExceeded covers a request
// that times out while the block read is running. The deadline is a second so
// the handler reaches the read first, and the test waits for that signal
// before trusting the result.
func TestFetchBlock_DeadlineDuringLookupMapsToDeadlineExceeded(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	entered := make(chan struct{})
	stub := &blockLookupStub{
		lookup: func(
			ctx context.Context,
			_ ocommon.Point,
		) (models.Block, error) {
			close(entered)
			<-ctx.Done()
			return models.Block{}, fmt.Errorf("read: %w", ctx.Err())
		},
	}
	u, err := fetchBlockWith(ctx, stub)
	<-entered
	require.Equal(t, connect.CodeDeadlineExceeded, connect.CodeOf(err))
	requireBulkSlotFree(t, u)
}
