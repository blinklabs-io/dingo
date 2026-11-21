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

package dingo

import (
	"testing"

	"github.com/blinklabs-io/dingo/mempool"
	"github.com/stretchr/testify/require"
)

// orderedMempoolSource reports one order through Transactions and another
// through BlockCandidates, so a test can tell which one a reader used.
type orderedMempoolSource struct {
	admission  []mempool.MempoolTransaction
	candidates []mempool.MempoolTransaction
}

func (s *orderedMempoolSource) Transactions() []mempool.MempoolTransaction {
	return s.admission
}

func (s *orderedMempoolSource) BlockCandidates() []mempool.MempoolTransaction {
	return s.candidates
}

func (s *orderedMempoolSource) RemoveTxsByHash([]string) {}

func TestForgingAdaptersUseBlockCandidateOrder(t *testing.T) {
	t.Parallel()
	first := mempool.MempoolTransaction{Hash: "aa", Cbor: []byte{0x01}, Type: 6}
	second := mempool.MempoolTransaction{Hash: "bb", Cbor: []byte{0x02}, Type: 6}
	source := &orderedMempoolSource{
		admission:  []mempool.MempoolTransaction{first, second},
		candidates: []mempool.MempoolTransaction{second, first},
	}

	forgingTxs := (&forgingMempoolAdapter{source: source}).Transactions()
	require.Len(t, forgingTxs, 2)
	require.Equal(t, []string{"bb", "aa"}, []string{forgingTxs[0].Hash, forgingTxs[1].Hash})

	ledgerTxs := (&ledgerMempoolAdapter{source: source}).Transactions()
	require.Len(t, ledgerTxs, 2)
	require.Equal(t, []string{"bb", "aa"}, []string{ledgerTxs[0].Hash, ledgerTxs[1].Hash})
}
