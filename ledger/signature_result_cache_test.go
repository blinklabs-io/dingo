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

package ledger

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/ledger/eras"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestBlockSignaturePreverifyReusesCachedResults checks that block
// application reuses the signature outcome an earlier validation, such as
// mempool admission, cached for the same transaction.
func TestBlockSignaturePreverifyReusesCachedResults(t *testing.T) {
	t.Parallel()
	tx := preverifyTestTransactionWithWitness(t, 11, 1)
	cache := eras.NewSignatureResultCache()
	admitted := cache.Verify(tx)
	require.NotNil(t, admitted)

	p := startSignaturePreverifyAhead(
		context.Background(),
		[]gledger.Block{signatureAheadBlock(tx)},
		[]bool{true},
		cache,
	)
	defer stopSignaturesWithin(t, p)
	results := p.forBlock(0)
	require.Len(t, results, 1)
	require.Same(t, admitted, results[0])

	set := preverifyTransactionSignatures([]lcommon.Transaction{tx}, cache)
	require.Same(t, admitted, set[tx.LeiosHash()])

	// Without a cache the block verifies afresh.
	uncached := preverifyTransactionSignatures([]lcommon.Transaction{tx}, nil)
	require.NotSame(t, admitted, uncached[tx.LeiosHash()])
}
