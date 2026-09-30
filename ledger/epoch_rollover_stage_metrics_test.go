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
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/eras"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEpochRolloverStageRecordsBoundaryTransaction drives a real epoch
// boundary through ledgerProcessBlocksFromSource and requires the
// epoch_rollover stage to record it. The #4364 stall is the boundary
// transaction itself (reward application and the governance tally run inside
// processEpochRollover), which none of the per-block stages time.
func TestEpochRolloverStageRecordsBoundaryTransaction(t *testing.T) {
	t.Parallel()

	ls, _, firstShelley := newByronShelleyBoundaryLedger(t)

	results := make(chan readChainResult, 1)
	results <- readChainResult{blocks: []gledger.Block{firstShelley}}
	close(results)

	require.NoError(t, ls.ledgerProcessBlocksFromSource(
		context.Background(),
		results,
	))
	require.Equal(t, eras.ShelleyEraDesc.Id, ls.currentEra.Id)

	count, sum := blockStageSampleCount(
		t,
		&ls.metrics,
		blockStageEpochRollover,
	)
	assert.Equal(
		t,
		uint64(1),
		count,
		"one boundary crossing must record exactly one epoch_rollover sample",
	)
	assert.Positive(t, sum)
	assert.Equal(
		t,
		sum,
		math.Float64frombits(ls.metrics.blockStageEpochRolloverMax.Load()),
		"the running maximum must hold the only recorded rollover",
	)
}

// TestEpochRolloverStageRecordsFailedRollover requires a rollover that fails
// to be timed too: block application was blocked for its whole duration
// either way. It forces the failure the same way as
// TestByronShelleyBoundaryClosesReadResultDoneOnEpochRolloverFailure.
func TestEpochRolloverStageRecordsFailedRollover(t *testing.T) {
	t.Parallel()

	ls, _, firstShelley := newByronShelleyBoundaryLedger(t)

	results := make(chan readChainResult, 1)
	results <- readChainResult{
		blocks: []gledger.Block{firstShelley},
		done:   make(chan struct{}),
	}
	close(results)

	passReached := make(chan struct{})
	releasePass := make(chan struct{})
	ls.beforeReadResultDoneSignal = func() {
		passReached <- struct{}{}
		<-releasePass
	}

	processDone := make(chan error, 1)
	go func() {
		processDone <- ls.ledgerProcessBlocksFromSource(
			context.Background(),
			results,
		)
	}()

	testutil.RequireReceive(
		t, passReached, testutil.AsyncWait,
		"pass 1 (boundary discovery) never reached the done-signal hook",
	)
	count, _ := blockStageSampleCount(
		t,
		&ls.metrics,
		blockStageEpochRollover,
	)
	require.Zero(
		t,
		count,
		"no rollover has run before the boundary pass is released",
	)
	ls.config.CardanoNodeConfig = nil
	releasePass <- struct{}{}

	err := testutil.RequireReceive(
		t, processDone, testutil.AsyncWait,
		"ledgerProcessBlocksFromSource never returned after the forced "+
			"epoch-rollover failure",
	)
	require.ErrorContains(t, err, "process epoch rollover")

	count, _ = blockStageSampleCount(
		t,
		&ls.metrics,
		blockStageEpochRollover,
	)
	assert.Equal(
		t,
		uint64(1),
		count,
		"a failed rollover must still record its epoch_rollover sample",
	)
}
