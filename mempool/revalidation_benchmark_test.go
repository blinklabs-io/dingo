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
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/prometheus/client_golang/prometheus"
)

const fifoRevalidationBenchmarkOccupancy = 1_000

type fifoRevalidationBenchmarkValidator struct {
	delayNS atomic.Int64
	started chan struct{}
	once    sync.Once
}

func (v *fifoRevalidationBenchmarkValidator) ValidateTx(
	tx gledger.Transaction,
) error {
	return v.ValidateTxWithOverlay(tx, nil, nil)
}

func (v *fifoRevalidationBenchmarkValidator) ValidateTxWithOverlay(
	gledger.Transaction,
	map[string]struct{},
	map[string]lcommon.Utxo,
) error {
	if delay := time.Duration(v.delayNS.Load()); delay > 0 {
		start := time.Now()
		for time.Since(start) < delay {
		}
	}
	return nil
}

func (v *fifoRevalidationBenchmarkValidator) WithTxValidationSession(
	fn func(
		func(
			gledger.Transaction,
			map[string]struct{},
			map[string]lcommon.Utxo,
		) error,
		func() bool,
	) error,
) error {
	v.once.Do(func() { close(v.started) })
	return fn(v.ValidateTxWithOverlay, func() bool { return true })
}

func BenchmarkFIFOAdmissionNoRevalidation(b *testing.B) {
	validator := &fifoRevalidationBenchmarkValidator{
		started: make(chan struct{}),
	}
	pool, _ := newFIFORevalidationBenchmarkPool(b, validator)
	generator := &fifoRevalidationTxGenerator{chainGap: 10}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		txBytes := generator.next(b)
		if err := pool.AddTransaction(
			uint(conway.EraIdConway),
			txBytes,
		); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFIFORevalidation(b *testing.B) {
	for _, scenario := range []struct {
		name     string
		chainGap uint64
	}{
		{name: "normal", chainGap: 10},
		{name: "degenerate-deep-chain", chainGap: 1},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			b.ReportAllocs()
			var admissionTotal, syncTotal, readTotal time.Duration
			for range b.N {
				b.StopTimer()
				validator := &fifoRevalidationBenchmarkValidator{
					started: make(chan struct{}),
				}
				pool, cleanup := newFIFORevalidationBenchmarkPool(b, validator)
				generator := &fifoRevalidationTxGenerator{
					chainGap: scenario.chainGap,
				}
				for range fifoRevalidationBenchmarkOccupancy {
					txBytes := generator.next(b)
					if err := pool.AddTransaction(
						uint(conway.EraIdConway),
						txBytes,
					); err != nil {
						b.Fatal(err)
					}
				}
				validator.delayNS.Store(int64(25 * time.Microsecond))
				b.StartTimer()

				syncStart := time.Now()
				rebuildDone := make(chan error, 1)
				go func() { rebuildDone <- pool.rebuildOverlay() }()
				waitCtx, cancelWait := context.WithTimeout(
					context.Background(),
					3*time.Second,
				)
				select {
				case <-validator.started:
				case <-waitCtx.Done():
					cancelWait()
					b.Fatal("timeout waiting for validation session start")
				}
				cancelWait()

				admissionStart := time.Now()
				if err := pool.AddTransaction(
					uint(conway.EraIdConway),
					generator.next(b),
				); err != nil {
					b.Fatal(err)
				}
				admissionTotal += time.Since(admissionStart)

				readStart := time.Now()
				_ = pool.Transactions()
				readTotal += time.Since(readStart)
				if err := <-rebuildDone; err != nil {
					b.Fatal(err)
				}
				syncTotal += time.Since(syncStart)
				b.StopTimer()
				cleanup()
			}
			b.ReportMetric(
				float64(admissionTotal.Nanoseconds())/float64(b.N),
				"admission-ns/avg",
			)
			b.ReportMetric(
				float64(syncTotal.Nanoseconds())/float64(b.N),
				"sync-ns/avg",
			)
			b.ReportMetric(
				float64(readTotal.Nanoseconds())/float64(b.N),
				"read-ns/avg",
			)
			b.ReportMetric(float64(b.N)/syncTotal.Seconds(), "attempts/s")
			b.ReportMetric(float64(b.N)/syncTotal.Seconds(), "tx/s")
			b.ReportMetric(
				float64(fifoRevalidationBenchmarkOccupancy+1),
				"txs-final",
			)
		})
	}
}

func newFIFORevalidationBenchmarkPool(
	b *testing.B,
	validator TxValidator,
) (*Mempool, func()) {
	b.Helper()
	eventBus := event.NewEventBus(nil, nil)
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(eventBus.Close)
	}
	b.Cleanup(cleanup)
	pool, err := NewMempool(MempoolConfig{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:        eventBus,
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       validator,
		MempoolCapacity: 1 << 30,
	})
	if err != nil {
		cleanup()
		b.Fatal(err)
	}
	return pool, cleanup
}

type fifoRevalidationTxGenerator struct {
	sequence uint64
	previous string
	chainGap uint64
}

func (g *fifoRevalidationTxGenerator) next(b *testing.B) []byte {
	b.Helper()
	inputHash := g.previous
	if inputHash == "" || g.chainGap == 0 || g.sequence%g.chainGap == 0 {
		inputHash = fifoRevalidationSeedHash(g.sequence)
	}
	template, err := hex.DecodeString(testTxHex)
	if err != nil {
		b.Fatal(err)
	}
	decoded, err := gledger.NewTransactionFromCbor(
		uint(conway.EraIdConway),
		template,
	)
	if err != nil {
		b.Fatal(err)
	}
	tx, ok := decoded.(*conway.ConwayTransaction)
	if !ok {
		b.Fatalf("transaction template decoded as %T", decoded)
	}
	tx.Body.TxInputs.SetItems([]shelley.ShelleyTransactionInput{
		shelley.NewShelleyTransactionInput(inputHash, 0),
	})
	tx.Body.TxFee = 1_000_000 + g.sequence
	tx.TxIsValid = true
	tx.Body.SetCbor(nil)
	tx.SetCbor(nil)
	txBytes, err := tx.MarshalCBOR()
	if err != nil {
		b.Fatal(err)
	}
	generated, err := gledger.NewTransactionFromCbor(
		uint(conway.EraIdConway),
		txBytes,
	)
	if err != nil {
		b.Fatal(err)
	}
	g.previous = generated.Hash().String()
	g.sequence++
	return txBytes
}

func fifoRevalidationSeedHash(sequence uint64) string {
	var seed [8]byte
	binary.BigEndian.PutUint64(seed[:], sequence)
	hash := sha256.Sum256(seed[:])
	return fmt.Sprintf("%x", hash[:])
}
