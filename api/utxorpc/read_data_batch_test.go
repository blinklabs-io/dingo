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
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
	query "github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
)

// batchDatumStub lets a test script the result of the batched datum lookup.
type batchDatumStub struct {
	tipHeightLedgerStub
	datums func(ctx context.Context, keys [][]byte) ([]*models.Datum, error)
}

func (s *batchDatumStub) Datums(
	ctx context.Context,
	keys [][]byte,
) ([]*models.Datum, error) {
	return s.datums(ctx, keys)
}

func readDataWith(
	ctx context.Context,
	stub *batchDatumStub,
	keys [][]byte,
) (*query.ReadDataResponse, error) {
	srv := &queryServiceServer{utxorpc: NewUtxorpc(UtxorpcConfig{
		Logger:      discardLogger(),
		LedgerState: stub,
	})}
	out, err := srv.ReadData(
		ctx,
		connect.NewRequest(&query.ReadDataRequest{Keys: keys}),
	)
	if err != nil {
		return nil, err
	}
	return out.Msg, nil
}

// TestReadData_LengthMismatchIsAnError covers a lookup that returns fewer
// results than hashes requested. ReadData must fail cleanly instead of
// indexing past the result.
func TestReadData_LengthMismatchIsAnError(t *testing.T) {
	t.Parallel()
	stub := &batchDatumStub{
		datums: func(context.Context, [][]byte) ([]*models.Datum, error) {
			return nil, nil
		},
	}
	keys := [][]byte{bytes.Repeat([]byte{0x01}, 32)}
	require.NotPanics(t, func() {
		_, err := readDataWith(context.Background(), stub, keys)
		require.ErrorContains(t, err, "got 0 results for 1 hashes")
	})
}

// TestReadData_MixedBatchNamesTheMissingKey covers a request with both stored
// and missing hashes. The NotFound error must name the missing hash, not a
// neighbouring one.
func TestReadData_MixedBatchNamesTheMissingKey(t *testing.T) {
	t.Parallel()
	found := bytes.Repeat([]byte{0x01}, 32)
	missing := bytes.Repeat([]byte{0x02}, 32)
	stub := &batchDatumStub{
		datums: func(_ context.Context, keys [][]byte) ([]*models.Datum, error) {
			out := make([]*models.Datum, len(keys))
			for i, key := range keys {
				if bytes.Equal(key, found) {
					out[i] = &models.Datum{Hash: key, RawDatum: []byte{0x01}}
				}
			}
			return out, nil
		},
	}
	_, err := readDataWith(
		context.Background(),
		stub,
		[][]byte{found, missing},
	)
	require.Error(t, err)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	require.ErrorContains(t, err, fmt.Sprintf("%x", missing))
	require.NotContains(t, err.Error(), fmt.Sprintf("%x", found))
}

// TestReadData_DeadlineExceeded covers a request whose deadline has passed.
// It must map to DeadlineExceeded, not Canceled.
func TestReadData_DeadlineExceeded(t *testing.T) {
	t.Parallel()
	stub := &batchDatumStub{
		datums: func(ctx context.Context, _ [][]byte) ([]*models.Datum, error) {
			return nil, ctx.Err()
		},
	}
	ctx, cancel := context.WithDeadline(
		context.Background(),
		time.Now().Add(-time.Second),
	)
	defer cancel()
	_, err := readDataWith(
		ctx,
		stub,
		[][]byte{bytes.Repeat([]byte{0x01}, 32)},
	)
	require.Equal(t, connect.CodeDeadlineExceeded, connect.CodeOf(err))
}

// TestReadData_CancelDuringLookup covers a caller that cancels while the batch
// is running, not before it starts. The lookup's error must still map to
// Canceled.
func TestReadData_CancelDuringLookup(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stub := &batchDatumStub{
		datums: func(ctx context.Context, _ [][]byte) ([]*models.Datum, error) {
			cancel()
			return nil, fmt.Errorf("read: %w", ctx.Err())
		},
	}
	_, err := readDataWith(
		ctx,
		stub,
		[][]byte{bytes.Repeat([]byte{0x01}, 32)},
	)
	require.Equal(t, connect.CodeCanceled, connect.CodeOf(err))
}

// TestReadData_DuplicateKeysAgainstRealDatabase runs ReadData over the real
// ledger and metadata store with a repeated hash and a second distinct one.
// Each distinct datum is returned once, in first-seen order.
func TestReadData_DuplicateKeysAgainstRealDatabase(t *testing.T) {
	t.Parallel()
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{numBlocks: 2})
	rawA, rawB := []byte{0x01}, []byte{0x02}
	require.NoError(t, h.DB.SetDatum(rawA, 1, nil))
	require.NoError(t, h.DB.SetDatum(rawB, 1, nil))
	hashA := lcommon.Blake2b256Hash(rawA).Bytes()
	hashB := lcommon.Blake2b256Hash(rawB).Bytes()

	srv := &queryServiceServer{utxorpc: h.U}
	out, err := srv.ReadData(
		context.Background(),
		connect.NewRequest(&query.ReadDataRequest{
			Keys: [][]byte{hashA, hashB, hashA, hashA},
		}),
	)
	require.NoError(t, err)
	values := out.Msg.GetValues()
	require.Len(t, values, 2)
	require.Equal(t, hashA, values[0].GetKey())
	require.Equal(t, hashB, values[1].GetKey())
}
