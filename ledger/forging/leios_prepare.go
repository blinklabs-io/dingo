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

package forging

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

const leiosPreparationWindow = 64

var errLeiosPreparationExpired = errors.New(
	"leios preparation deadline reached",
)

type preparedLeiosCandidate struct {
	mempool MempoolTransaction
	tx      lcommon.Transaction
	blockTx []byte
	err     error
}

type leiosCandidatePreparation struct {
	txs     []MempoolTransaction
	workers int
	index   int
	window  []preparedLeiosCandidate
	prepare func(MempoolTransaction) preparedLeiosCandidate
	expired func() bool
}

func newLeiosCandidatePreparation(
	txs []MempoolTransaction,
	workers int,
) *leiosCandidatePreparation {
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	return &leiosCandidatePreparation{
		txs:     txs,
		workers: min(8, workers),
		prepare: prepareLeiosCandidate,
	}
}

func prepareLeiosCandidate(tx MempoolTransaction) preparedLeiosCandidate {
	result := preparedLeiosCandidate{mempool: tx}
	if !validLeiosTransactionReference(tx) {
		result.err = errors.New("unrepresentable Leios transaction reference")
		return result
	}
	result.blockTx, result.err = leiosBlockTransactionCbor(tx.Cbor)
	if result.err != nil {
		return result
	}
	if !validLeiosTransactionSize(len(result.blockTx)) {
		result.err = errors.New("unrepresentable Leios block transaction size")
		return result
	}
	result.tx, result.err = decodeMempoolTx(tx)
	return result
}

// next joins each bounded window before exposing its decoded transactions.
// Ledger validation exclusively owns them afterward; no database session or
// mutable validation overlay is shared with preparation workers.
func (p *leiosCandidatePreparation) next(
	ctx context.Context,
) (preparedLeiosCandidate, error) {
	if err := ctx.Err(); err != nil {
		return preparedLeiosCandidate{}, err
	}
	if len(p.window) == 0 {
		size := min(leiosPreparationWindow, len(p.txs)-p.index)
		p.window = make([]preparedLeiosCandidate, size)
		if err := p.fill(ctx, p.txs[p.index:p.index+size]); err != nil {
			p.window = nil
			return preparedLeiosCandidate{}, err
		}
	}
	result := p.window[0]
	p.window[0] = preparedLeiosCandidate{}
	p.window = p.window[1:]
	p.index++
	return result, nil
}

func (p *leiosCandidatePreparation) fill(
	ctx context.Context,
	txs []MempoolTransaction,
) error {
	var next atomic.Int64
	var expired atomic.Bool
	var clock sync.Mutex
	var workers sync.WaitGroup
	run := func() {
		defer workers.Done()
		for ctx.Err() == nil {
			if p.expired != nil {
				clock.Lock()
				stop := p.expired()
				clock.Unlock()
				if stop {
					expired.Store(true)
					return
				}
			}
			i := int(next.Add(1) - 1)
			if i >= len(txs) {
				return
			}
			p.window[i] = p.prepare(txs[i])
		}
	}
	count := min(p.workers, len(txs))
	workers.Add(count)
	for i := 1; i < count; i++ {
		go run()
	}
	if count > 0 {
		run()
	}
	workers.Wait()
	if err := ctx.Err(); err != nil {
		return err
	}
	if expired.Load() {
		return errLeiosPreparationExpired
	}
	return nil
}
