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
	"runtime"
	"sync"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// The transaction ID omits witnesses, so the full transaction distinguishes
// preverification results for identical bodies with different signature sets.
type preverifiedSignatureSet map[lcommon.Blake2b256]*lcommon.PreverifiedSignatures

func preverifyTransactionSignatures(
	transactions []lcommon.Transaction,
) preverifiedSignatureSet {
	if len(transactions) == 0 {
		return nil
	}
	results := make([]*lcommon.PreverifiedSignatures, len(transactions))
	workers := min(runtime.GOMAXPROCS(0), len(transactions))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for idx := range jobs {
				results[idx] = lcommon.PreverifySignatures(transactions[idx])
			}
		})
	}
	for idx := range transactions {
		jobs <- idx
	}
	close(jobs)
	wg.Wait()

	ret := make(preverifiedSignatureSet, len(transactions))
	for idx, transaction := range transactions {
		ret[transaction.LeiosHash()] = results[idx]
	}
	return ret
}

type preverifiedSignaturesContextKey struct{}

func withPreverifiedSignatures(
	ctx context.Context,
	signatures preverifiedSignatureSet,
) context.Context {
	if len(signatures) == 0 {
		return ctx
	}
	return context.WithValue(ctx, preverifiedSignaturesContextKey{}, signatures)
}

func preverifiedSignaturesFrom(
	ctx context.Context,
	transaction lcommon.Transaction,
) *lcommon.PreverifiedSignatures {
	signatures, _ := ctx.Value(preverifiedSignaturesContextKey{}).(preverifiedSignatureSet)
	return signatures[transaction.LeiosHash()]
}
