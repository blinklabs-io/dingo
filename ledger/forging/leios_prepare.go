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
	"io"
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
	mempool  MempoolTransaction
	tx       lcommon.Transaction
	prepared PreparedLeiosTransaction
	blockTx  []byte
	err      error
}

type leiosCandidatePreparation struct {
	txs      []MempoolTransaction
	workers  int
	index    int
	batch    *leiosPreparationBatch
	position int
	prepare  func(MempoolTransaction) preparedLeiosCandidate
	expired  func() bool
	clock    sync.Mutex
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
	result := prepareLeiosCandidateBytes(tx)
	if result.err == nil {
		result.tx, result.err = decodeMempoolTx(tx)
	}
	return result
}

func prepareLeiosCandidateBytes(tx MempoolTransaction) preparedLeiosCandidate {
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
	return result
}

type leiosPreparationBatch struct {
	results []preparedLeiosCandidate
	ready   []chan struct{}
	done    chan struct{}
	cancel  context.CancelFunc
}

// next exposes the prepared prefix in mempool order without waiting for later
// candidates. Workers own distinct slots and never access the ledger overlay.
func (p *leiosCandidatePreparation) next(
	ctx context.Context,
) (preparedLeiosCandidate, error) {
	if err := ctx.Err(); err != nil {
		p.close()
		return preparedLeiosCandidate{}, err
	}
	if p.batch != nil && p.position == len(p.batch.results) {
		p.close()
	}
	if p.batch == nil {
		if p.index == len(p.txs) {
			return preparedLeiosCandidate{}, io.EOF
		}
		if p.isExpired() {
			return preparedLeiosCandidate{}, errLeiosPreparationExpired
		}
		p.start(ctx)
	}
	batch := p.batch
	ready := batch.ready[p.position]
	consume := func() (preparedLeiosCandidate, error) {
		if err := ctx.Err(); err != nil {
			p.close()
			return preparedLeiosCandidate{}, err
		}
		result := batch.results[p.position]
		batch.results[p.position] = preparedLeiosCandidate{}
		p.position++
		p.index++
		return result, nil
	}
	select {
	case <-ready:
		return consume()
	default:
	}
	if p.isExpired() {
		select {
		case <-ready:
			return consume()
		default:
		}
		p.close()
		return preparedLeiosCandidate{}, errLeiosPreparationExpired
	}
	select {
	case <-ctx.Done():
		p.close()
		return preparedLeiosCandidate{}, ctx.Err()
	case <-ready:
		return consume()
	case <-batch.done:
		select {
		case <-ready:
			return consume()
		default:
		}
		p.close()
		return preparedLeiosCandidate{}, errLeiosPreparationExpired
	}
}

func (p *leiosCandidatePreparation) isExpired() bool {
	p.clock.Lock()
	defer p.clock.Unlock()
	return p.expired != nil && p.expired()
}

func (p *leiosCandidatePreparation) close() {
	if p.batch == nil {
		return
	}
	p.batch.cancel()
	<-p.batch.done
	p.batch = nil
	p.position = 0
}

func (p *leiosCandidatePreparation) start(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	size := min(leiosPreparationWindow, len(p.txs)-p.index)
	batch := &leiosPreparationBatch{
		results: make(
			[]preparedLeiosCandidate,
			size,
		), ready: make([]chan struct{}, size),
		done: make(chan struct{}), cancel: cancel,
	}
	for i := range batch.ready {
		batch.ready[i] = make(chan struct{})
	}
	p.batch = batch
	txs := p.txs[p.index : p.index+size]
	var next atomic.Int64
	var workers sync.WaitGroup
	run := func() {
		defer workers.Done()
		for ctx.Err() == nil {
			if p.isExpired() {
				return
			}
			i := int(next.Add(1) - 1)
			if i >= len(txs) {
				return
			}
			batch.results[i] = p.prepare(txs[i])
			close(batch.ready[i])
		}
	}
	count := min(p.workers, size)
	workers.Add(count)
	for range count {
		go run()
	}
	go func() { workers.Wait(); close(batch.done) }()
}
