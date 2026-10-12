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

package mempool

import (
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func parameterChangeCostModelTx(
	t *testing.T,
	txType uint,
	languageID uint,
) []byte {
	t.Helper()
	inputHash := make([]byte, 32)
	address := make([]byte, 29)
	address[0] = 0x60
	rewardAccount := make([]byte, 29)
	rewardAccount[0] = 0xe0
	body := map[uint]any{
		0: cbor.Tag{Number: 258, Content: []any{[]any{inputHash, uint64(0)}}},
		1: []any{[]any{address, uint64(1_000_000)}},
		2: uint64(200_000),
		20: []any{[]any{
			uint64(1_000_000_000),
			rewardAccount,
			[]any{
				uint64(lcommon.GovActionTypeParameterChange),
				nil,
				map[uint]any{18: map[uint][]int64{languageID: {1}}},
				nil,
			},
			[]any{"https://example.invalid/a", make([]byte, 32)},
		}},
	}
	var fields []any
	if txType == uint(gledger.TxTypeDijkstra) {
		fields = []any{body, map[uint]any{}, nil}
	} else {
		fields = []any{body, map[uint]any{}, true, nil}
	}
	raw, err := cbor.Encode(fields)
	require.NoError(t, err)
	return raw
}

func TestAddTransactionRejectsOverwideCostModelLanguageIDs(t *testing.T) {
	t.Parallel()
	for name, txType := range map[string]uint{
		"Conway":   uint(gledger.TxTypeConway),
		"Dijkstra": uint(gledger.TxTypeDijkstra),
	} {
		for _, languageID := range []uint{255, 256} {
			t.Run(fmt.Sprintf("%s/language-%d", name, languageID), func(t *testing.T) {
				t.Parallel()
				pool, err := NewMempool(MempoolConfig{
					Validator:       newMockValidator(),
					Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
					PromRegistry:    prometheus.NewRegistry(),
					MempoolCapacity: 1024 * 1024,
				})
				require.NoError(t, err)

				err = pool.AddTransaction(
					txType,
					parameterChangeCostModelTx(t, txType, languageID),
				)
				if languageID == 256 {
					require.ErrorContains(t, err, "exceeds Word8 maximum 255")
					require.Empty(t, pool.Transactions())
					return
				}
				require.NoError(t, err)
				require.Len(t, pool.Transactions(), 1)
			})
		}
	}
}
