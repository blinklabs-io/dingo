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

package ouroboros

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	"github.com/stretchr/testify/require"
)

type wireLimitedLeiosRequester struct {
	txs   []cbor.RawMessage
	sizes []int
}

func (r *wireLimitedLeiosRequester) BlockTxsRequest(
	_ context.Context, _ ocommon.Point, bitmap map[uint16]uint64,
) (protocol.Message, error) {
	indices := leiosBitmapTxIndices(bitmap)
	txs := make([]cbor.RawMessage, 0, len(indices))
	for _, index := range indices {
		txs = append(txs, r.txs[index])
	}
	msg := leiosfetch.NewMsgBlockTxs(txs)
	raw, err := msg.MarshalCBOR()
	if err != nil {
		return nil, err
	}
	if len(raw) > leiosfetch.MaxBlockTxsMessageBytes {
		return nil, fmt.Errorf("reply exceeds wire limit: %d", len(raw))
	}
	r.sizes = append(r.sizes, len(raw))
	return msg, nil
}

func TestFetchLeiosEbTxsRespectsWireByteLimit(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		count, padding int
	}{
		{"many medium transactions", 1100, 1200},
		{"large byte string headers", 9, 80000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			txs := make([]cbor.RawMessage, tc.count)
			refs := make([]lcommon.LeiosTransactionReference, tc.count)
			for i := range txs {
				var parts []cbor.RawMessage
				_, err := cbor.Decode(testDijkstraTxIndex(t, i), &parts)
				require.NoError(t, err)
				parts[2], err = cbor.Encode(bytes.Repeat([]byte{0x42}, tc.padding))
				require.NoError(t, err)
				txs[i], err = cbor.Encode(parts)
				require.NoError(t, err)
				refs[i] = lcommon.LeiosTransactionReference{
					TransactionHash: lcommon.Blake2b256Hash(txs[i]),
					TransactionSize: uint32(len(txs[i])), //nolint:gosec // fixture size is bounded above
				}
			}
			manifest, err := (lcommon.LeiosEndorserBlock{TransactionReferences: refs}).MarshalCBOR()
			require.NoError(t, err)
			point := ocommon.NewPoint(100, lcommon.Blake2b256Hash(manifest).Bytes())
			node := newOuroboros(OuroborosConfig{EnableLeios: true})
			require.NoError(t, node.storeLeiosEndorserBlock(point, manifest, nil, leiosStoreAuthoritative))
			requester := &wireLimitedLeiosRequester{txs: txs}
			actual, err := node.fetchLeiosEbTxsBatchedUntil(context.Background(), requester, point, len(txs), manifest, time.Time{})
			require.NoError(t, err)
			require.Equal(t, txs, actual)
			require.Greater(t, len(requester.sizes), 1)
			for _, size := range requester.sizes {
				require.LessOrEqual(t, size, leiosfetch.MaxBlockTxsMessageBytes)
			}
			// The second fetch must reuse the complete retained prefix, not request it again.
			cachedRequester := &wireLimitedLeiosRequester{txs: txs}
			actual, err = node.fetchLeiosEbTxsBatchedUntil(context.Background(), cachedRequester, point, len(txs), manifest, time.Time{})
			require.NoError(t, err)
			require.Equal(t, txs, actual)
			require.Empty(t, cachedRequester.sizes)
		})
	}
}

func TestLeiosBoundFetchReplyRejectsUnfetchableTransaction(t *testing.T) {
	t.Parallel()
	_, err := leiosBoundFetchReply(map[uint16]uint64{0: 1 << 63}, []lcommon.LeiosTransactionReference{{TransactionSize: leiosfetch.MaxBlockTxsMessageBytes}})
	require.ErrorContains(t, err, "cannot fit")
	_, err = leiosBoundFetchReply(map[uint16]uint64{1: 1 << 63}, []lcommon.LeiosTransactionReference{{TransactionSize: 1}})
	require.ErrorContains(t, err, "exceeds manifest")
}
