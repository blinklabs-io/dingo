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

package node

import (
	"context"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dledger "github.com/blinklabs-io/dingo/ledger"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

type backfillReadResult struct {
	raw              *database.BlobBlockResult
	parsed           gledger.Block
	readDecode       time.Duration
	inputPrefetch    time.Duration
	prefetchedInputs int
	errStage         string
	err              error
}

type backfillReadAhead struct {
	results <-chan backfillReadResult
	cancel  context.CancelFunc
	done    <-chan struct{}
}

func (r backfillReadAhead) Close() {
	r.cancel()
	<-r.done
}

func (b *Backfill) startBackfillReadAhead(
	ctx context.Context,
	next func() (*database.BlobBlockResult, error),
) backfillReadAhead {
	ctx, cancel := context.WithCancel(ctx)
	results := make(chan backfillReadResult, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(results)
		for {
			readStart := time.Now()
			raw, err := next()
			if err != nil || raw == nil {
				if err != nil {
					sendBackfillReadResult(ctx, results, backfillReadResult{
						errStage: "iterating blocks",
						err:      err,
					})
				}
				return
			}
			parsed, err := models.DecodeBlockCbor(
				raw.BlockType,
				raw.Cbor,
				lcommon.VerifyConfig{SkipBodyHashValidation: true},
			)
			result := backfillReadResult{
				raw:        raw,
				parsed:     parsed,
				readDecode: time.Since(readStart),
				errStage:   "parsing block",
				err:        err,
			}
			if err == nil {
				refs := backfillInputRefs(parsed.Transactions())
				result.prefetchedInputs = len(refs)
				prefetchStart := time.Now()
				if b.prefetchInputs != nil {
					result.err = b.prefetchInputs(ctx, refs)
				} else {
					result.err = b.db.PrefetchBackfillInputs(ctx, refs)
				}
				result.errStage = "prefetching block inputs"
				result.inputPrefetch = time.Since(prefetchStart)
			}
			if !sendBackfillReadResult(ctx, results, result) || result.err != nil {
				return
			}
		}
	}()
	return backfillReadAhead{results: results, cancel: cancel, done: done}
}

func sendBackfillReadResult(
	ctx context.Context,
	results chan<- backfillReadResult,
	result backfillReadResult,
) bool {
	select {
	case results <- result:
		return true
	case <-ctx.Done():
		return false
	}
}

func backfillInputRefs(txs []lcommon.Transaction) []models.UtxoId {
	type inputKey struct {
		hash lcommon.Blake2b256
		idx  uint32
	}
	seen := make(map[inputKey]struct{})
	var refs []models.UtxoId
	for _, tx := range txs {
		for _, level := range dledger.TransactionLevelsForApply(tx) {
			for _, input := range level.Consumed() {
				key := inputKey{hash: input.Id(), idx: input.Index()}
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				refs = append(refs, models.UtxoId{
					Hash: key.hash.Bytes(),
					Idx:  key.idx,
				})
			}
		}
	}
	return refs
}
