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

//go:build dingo_extra_plugins

package mysql

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
)

func TestBatchAccumulatorZeroValueProducedIndex(t *testing.T) {
	t.Parallel()
	var batch BatchAccumulator

	txID := bytes.Repeat([]byte{0xaa}, 32)
	batch.AddUtxoOutput(models.Utxo{
		TxId:      txID,
		OutputIdx: 2,
		Amount:    123,
	})
	amount, ok := batch.InFlightProducerAmount(txID, 2)
	if !ok {
		t.Fatal("expected zero-value accumulator to index produced output")
	}
	if amount != 123 {
		t.Fatalf("expected produced output amount 123, got %d", amount)
	}

	batch.Reset()
	if _, ok := batch.InFlightProducerAmount(txID, 2); ok {
		t.Fatal("expected reset to clear produced output index")
	}

	colRetTxID := bytes.Repeat([]byte{0xbb}, 32)
	batch.AddCollateralReturn(models.Utxo{
		TxId:      colRetTxID,
		OutputIdx: 0,
		Amount:    456,
	})
	amount, ok = batch.InFlightProducerAmount(colRetTxID, 0)
	if !ok {
		t.Fatal("expected zero-value accumulator to index collateral return")
	}
	if amount != 456 {
		t.Fatalf("expected collateral return amount 456, got %d", amount)
	}

	var dst BatchAccumulator
	var src BatchAccumulator
	srcTxID := bytes.Repeat([]byte{0xcc}, 32)
	src.AddUtxoOutput(models.Utxo{
		TxId:      srcTxID,
		OutputIdx: 1,
		Amount:    789,
	})
	dst.MergeFrom(&src)
	amount, ok = dst.InFlightProducerAmount(srcTxID, 1)
	if !ok {
		t.Fatal("expected zero-value merge destination to index source output")
	}
	if amount != 789 {
		t.Fatalf("expected merged output amount 789, got %d", amount)
	}
}
