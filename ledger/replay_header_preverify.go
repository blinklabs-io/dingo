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
	"errors"
	"runtime"
	"sync"

	"github.com/blinklabs-io/dingo/database"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/fxamacker/cbor/v2"
	"golang.org/x/crypto/blake2b"
)

type replayHeaderVerification struct {
	identity      lcommon.Blake2b256
	epoch         uint64
	nonce         string
	kesSlots      uint64
	kesEvolutions uint64
	done          chan struct{}
	err           error
	panicked      bool
	panicValue    any
}

// The identity contains only integers, byte strings, and the plain OpCert
// struct, so none of the ledger codec's custom CBOR tags apply.
var replayHeaderIdentityEncoder = sync.OnceValues(func() (cbor.EncMode, error) {
	return (cbor.EncOptions{
		Sort:          cbor.SortCoreDeterministic,
		BigIntConvert: cbor.BigIntConvertShortest,
	}).EncMode()
})

// Preserved CBOR alone does not bind mutable decoded fields. Include the
// fields read directly by the KES and operational-certificate verifiers too.
func replayHeaderIdentity(block gledger.Block) (lcommon.Blake2b256, error) {
	header := block.Header()
	body, ok := headerBodyCbor(header)
	if !ok || len(body) == 0 {
		return lcommon.Blake2b256{}, errors.New("header body CBOR unavailable")
	}
	signature, hotKey, period, err := gledger.ExtractKesFields(header)
	if err != nil {
		return lcommon.Blake2b256{}, err
	}
	cert, err := gledger.ExtractOpCertFromHeader(header)
	if err != nil {
		return lcommon.Blake2b256{}, err
	}
	encoder, err := replayHeaderIdentityEncoder()
	if err != nil {
		return lcommon.Blake2b256{}, err
	}
	encoded, err := encoder.Marshal([]any{
		block.Era().Id, block.SlotNumber(), header.Era().Id,
		header.SlotNumber(), header.Cbor(), body, header.IssuerVkey(),
		signature, hotKey, period, cert,
	})
	if err != nil {
		return lcommon.Blake2b256{}, err
	}
	return lcommon.Blake2b256(blake2b.Sum256(encoded)), nil
}

func (ls *LedgerState) prepareReplayHeaders(
	ctx context.Context,
	blocks []gledger.Block,
) (results []*replayHeaderVerification, jobs <-chan int, retErr error) {
	// Preparation runs before apply opens its transaction. Keep malformed
	// implementations from moving a panic outside the rollback error boundary.
	defer func() {
		if value := recover(); value != nil {
			results = nil
			jobs = nil
			retErr = database.NewTxnPanicError("prepare replay headers", value)
		}
	}()
	if !ls.config.VerifyReplayHeaders || len(blocks) < 2 {
		return nil, nil, nil
	}
	snapshot := ls.loadConsensusSnapshot()
	if snapshot == nil {
		return nil, nil, nil
	}
	kesSlots, kesEvolutions := ls.SlotsPerKESPeriod(), ls.maxKESEvolutions()
	if kesSlots == 0 || kesEvolutions == 0 {
		return nil, nil, nil
	}
	results = make([]*replayHeaderVerification, len(blocks))
	pending := make(chan int, len(blocks))
	for idx, block := range blocks {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if block.Era().Id == byron.EraIdByron ||
			ls.slotCoveredByMithril(block.SlotNumber()) {
			continue
		}
		epoch, err := epochForSlotInCache(
			snapshot.epochCache,
			block.SlotNumber(),
		)
		if err != nil || len(epoch.Nonce) == 0 {
			continue
		}
		identity, err := replayHeaderIdentity(block)
		if err != nil {
			continue
		}
		// Normalization copies the header, including its lazy hash memo.
		// Initialize that memo before workers can copy it while apply hashes it.
		_ = block.Header().Hash()
		results[idx] = &replayHeaderVerification{
			identity: identity, epoch: epoch.EpochId,
			nonce:    ls.epochNonceHex(epoch.EpochId, epoch.Nonce),
			kesSlots: kesSlots, kesEvolutions: kesEvolutions,
			done: make(chan struct{}),
		}
		pending <- idx
	}
	close(pending)
	return results, pending, nil
}

type replayHeaderWorkers struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func startReplayHeaderJobs(
	ctx context.Context,
	blocks []gledger.Block,
	results []*replayHeaderVerification,
	jobs <-chan int,
	verify func(gledger.Block, *replayHeaderVerification),
) *replayHeaderWorkers {
	workers := min(runtime.GOMAXPROCS(0), blockPipelineMaxWorkers, len(jobs))
	if workers == 0 {
		return nil
	}
	workerCtx, cancel := context.WithCancel(ctx)
	group := &replayHeaderWorkers{ctx: ctx, cancel: cancel}
	for range workers {
		group.wg.Go(func() {
			for idx := range jobs {
				if err := workerCtx.Err(); err != nil {
					results[idx].err = err
				} else {
					verify(blocks[idx], results[idx])
				}
				close(results[idx].done)
			}
		})
	}
	return group
}

func (group *replayHeaderWorkers) wait() error {
	if group == nil {
		return nil
	}
	group.wg.Wait()
	return group.ctx.Err()
}

func (group *replayHeaderWorkers) stop() {
	if group == nil {
		return
	}
	group.cancel()
	group.wg.Wait()
}

func verifyReplayHeader(block gledger.Block, result *replayHeaderVerification) {
	// Deliver a verifier panic inside the ordered apply transaction, whose
	// existing panic recovery owns rollback. Never let it escape a worker.
	defer func() {
		if value := recover(); value != nil {
			result.panicked, result.panicValue = true, value
		}
	}()
	result.err = verifyHeaderCryptoParameters(
		block, result.nonce, result.kesSlots, result.kesEvolutions,
	)
}

type replayHeaderVerificationKey struct{}

func withReplayHeaderVerification(
	ctx context.Context,
	results []*replayHeaderVerification,
	index int,
) context.Context {
	if index >= len(results) || results[index] == nil {
		return ctx
	}
	return context.WithValue(ctx, replayHeaderVerificationKey{}, results[index])
}

func replayHeaderVerificationFrom(
	ctx context.Context,
	block gledger.Block,
	epoch uint64,
	nonce string,
	kesSlots, kesEvolutions uint64,
) (*replayHeaderVerification, bool) {
	result, _ := ctx.Value(replayHeaderVerificationKey{}).(*replayHeaderVerification)
	if result == nil {
		return nil, false
	}
	// The receipt's immutable input binding is prepared before workers start;
	// verdict fields are visible only after its completion signal.
	<-result.done
	if result.epoch != epoch || result.nonce != nonce ||
		result.kesSlots != kesSlots || result.kesEvolutions != kesEvolutions {
		return nil, false
	}
	identity, err := replayHeaderIdentity(block)
	return result, err == nil && result.identity == identity
}
