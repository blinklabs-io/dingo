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
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// fallbackTestBuilder implements the package-private constrained builder
// path the production forger uses, so a test can distinguish an attempt
// that was allowed to carry transactions from the empty-body fallback.
type fallbackTestBuilder struct {
	block      ledger.Block
	cbor       []byte
	calls      int
	emptyCalls int
	// selectErr is returned for every attempt allowed to carry
	// transactions, simulating a ledger publication landing during each
	// selection pass.
	selectErr error
	// emptyErr, when set, also fails the empty-body attempt.
	emptyErr error
}

func (b *fallbackTestBuilder) BuildBlock(
	uint64,
	uint64,
) (ledger.Block, []byte, error) {
	b.calls++
	return nil, nil, b.selectErr
}

func (b *fallbackTestBuilder) buildBlockWithCredentialGeneration(
	_ uint64,
	_ uint64,
	_ LeiosBlockData,
	_ *credentialGeneration,
	constraints blockSelectionConstraints,
) (ledger.Block, []byte, error) {
	b.calls++
	if constraints.emptyBody {
		b.emptyCalls++
		if b.emptyErr != nil {
			return nil, nil, b.emptyErr
		}
		return b.block, b.cbor, nil
	}
	return nil, nil, b.selectErr
}

var _ credentialGenerationBlockBuilder = (*fallbackTestBuilder)(nil)

// TestForgeFallsBackToEmptyBlockWhenSelectionCannotComplete is the second
// half of the lost-slot fix: when no selection pass can complete against a
// stable snapshot before the slot runs out, forge a transaction-free block
// rather than nothing. A pool's reward for a slot does not depend on what
// the block carries, so an empty block is worth the whole slot.
func TestForgeFallsBackToEmptyBlockWhenSelectionCannotComplete(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &fallbackTestBuilder{
		block:     block,
		cbor:      block.cbor,
		selectErr: errTxValidationSnapshotChanged,
	}
	broadcaster := &forgerTestBroadcaster{}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		// No slot time left, so the retry path is exhausted immediately
		// and only the fallback can save the slot.
		slotEnd: time.Now(),
	}
	forger := newRetryForger(t, clock, builder, broadcaster)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, 1, builder.emptyCalls)
	require.Equal(t, 1, broadcaster.calls, "the empty block must be adopted")
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
		"a slot kept by the empty fallback is not a could-not-forge",
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.forgeSelectionFallback.WithLabelValues("empty"),
		),
	)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(
			forger.metrics.forgeSelectionFallback.WithLabelValues("lost"),
		),
	)
}

// TestForgeReportsLostSlotWhenEmptyFallbackAlsoFails keeps
// Forge_could_not_forge_int meaning what it always meant: the slot really
// produced nothing.
func TestForgeReportsLostSlotWhenEmptyFallbackAlsoFails(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &fallbackTestBuilder{
		block:     block,
		cbor:      block.cbor,
		selectErr: errTxValidationSnapshotChanged,
		emptyErr:  errors.New("VRF verification key not loaded"),
	}
	broadcaster := &forgerTestBroadcaster{}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now(),
	}
	forger := newRetryForger(t, clock, builder, broadcaster)

	err := forger.checkAndForgeProduction(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, errTxValidationSnapshotChanged)
	require.Equal(t, 1, builder.emptyCalls)
	require.Equal(t, 0, broadcaster.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.forgeSelectionFallback.WithLabelValues("lost"),
		),
	)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(
			forger.metrics.forgeSelectionFallback.WithLabelValues("empty"),
		),
	)
}

// TestForgeSkipsEmptyFallbackForNonSelectionFailures keeps the fallback
// scoped to the defect it exists for. A build that failed for an unrelated
// reason gets no second attempt.
func TestForgeSkipsEmptyFallbackForNonSelectionFailures(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &fallbackTestBuilder{
		block:     block,
		cbor:      block.cbor,
		selectErr: errors.New("epoch nonce not available"),
	}
	broadcaster := &forgerTestBroadcaster{}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now().Add(time.Hour),
	}
	forger := newRetryForger(t, clock, builder, broadcaster)

	require.Error(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, 0, builder.emptyCalls)
	require.Equal(t, 1, builder.calls)
}

// TestBuildBlockEmptyBodyConstraintDropsAllTransactions covers the builder
// half: the empty-body constraint must produce a block with no
// transactions and must not open a validation session at all, so a ledger
// publication cannot reject a candidate that contains nothing to validate.
func TestBuildBlockEmptyBodyConstraintDropsAllTransactions(t *testing.T) {
	validator := &sessionMockTxValidator{alwaysStale: true}
	builder := newSelectionTestBuilder(
		t,
		threeTxMempoolForSelection(t),
		selectionTestChainTip(),
		validator,
	)

	generation := builder.creds.acquireCredentialGeneration()
	defer generation.release()
	block, _, err := builder.buildBlockWithCredentialGeneration(
		1001,
		0,
		LeiosBlockData{},
		generation,
		blockSelectionConstraints{emptyBody: true},
	)
	require.NoError(t, err)
	require.Empty(t, block.Transactions())
	require.Zero(
		t,
		validator.sessions,
		"a transaction-free candidate has no snapshot to pin",
	)
	require.Zero(t, validator.validateCalls)
}

// TestBuildBlockWithNoCandidatesSkipsValidationSession pins the same
// property for a producer whose mempool is simply empty: before this, an
// unrelated ledger publication could reject a block that carried no
// transactions and cost the slot for nothing.
func TestBuildBlockWithNoCandidatesSkipsValidationSession(t *testing.T) {
	validator := &sessionMockTxValidator{alwaysStale: true}
	builder := newSelectionTestBuilder(
		t,
		&mockMempool{transactions: []MempoolTransaction{}},
		selectionTestChainTip(),
		validator,
	)

	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	require.Empty(t, block.Transactions())
	require.Zero(t, validator.sessions)
}
