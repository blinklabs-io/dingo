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
	"bytes"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// sessionMockTxValidator implements both TxValidator and
// TxValidationSessionProvider, so it exercises the same
// withTxValidationSession path DefaultBlockBuilder.buildBlock uses with a
// real LedgerState. It lets tests observe how many sessions were opened for
// one BuildBlock call, and lets a hook run synchronously inside a validate
// call to simulate a concurrent ledger or chain-tip mutation landing
// mid-selection (issue #3506).
type sessionMockTxValidator struct {
	sessions      int
	validateCalls int
	// staleAfterCalls, when non-zero, makes stillCurrent() report false
	// once validateCalls reaches this count. This simulates a ledger
	// publication (new block, rollback, epoch transition, or protocol
	// parameter change) landing while transaction selection is still in
	// progress.
	staleAfterCalls int
	// onValidate, when set, runs synchronously inside each validate call
	// with the 1-indexed call number. Tests use it to mutate shared state
	// (e.g. the chain tip) partway through selection, deterministically,
	// rather than racing real goroutines against a sleep.
	onValidate func(callNumber int)
}

func (v *sessionMockTxValidator) ValidateTx(tx ledger.Transaction) error {
	return v.ValidateTxWithOverlay(tx, nil, nil)
}

// ValidateTxWithOverlay is only reached if the builder fails to discover the
// TxValidationSessionProvider capability and falls back to unpinned,
// per-transaction validation. TestBuildBlockPinsOneValidationSessionPerBlock
// asserts that does not happen.
func (v *sessionMockTxValidator) ValidateTxWithOverlay(
	_ ledger.Transaction,
	_ map[string]struct{},
	_ map[string]lcommon.Utxo,
) error {
	return nil
}

func (v *sessionMockTxValidator) WithTxValidationSession(
	fn func(
		validate func(
			tx ledger.Transaction,
			consumed map[string]struct{},
			created map[string]lcommon.Utxo,
		) error,
		stillCurrent func() bool,
	) error,
) error {
	v.sessions++
	stale := false
	validate := func(
		_ ledger.Transaction,
		_ map[string]struct{},
		_ map[string]lcommon.Utxo,
	) error {
		v.validateCalls++
		if v.onValidate != nil {
			v.onValidate(v.validateCalls)
		}
		if v.staleAfterCalls > 0 && v.validateCalls >= v.staleAfterCalls {
			stale = true
		}
		return nil
	}
	stillCurrent := func() bool { return !stale }
	return fn(validate, stillCurrent)
}

var (
	_ TxValidator                 = (*sessionMockTxValidator)(nil)
	_ TxValidationSessionProvider = (*sessionMockTxValidator)(nil)
)

func threeTxMempoolForSelection(t *testing.T) *mockMempool {
	t.Helper()
	return &mockMempool{
		transactions: []MempoolTransaction{
			{
				Hash: "tx1",
				Cbor: makeMinimalTxCbor(t, 0x01, 0),
				Type: conway.TxTypeConway,
			},
			{
				Hash: "tx2",
				Cbor: makeMinimalTxCbor(t, 0x02, 0),
				Type: conway.TxTypeConway,
			},
			{
				Hash: "tx3",
				Cbor: makeMinimalTxCbor(t, 0x03, 0),
				Type: conway.TxTypeConway,
			},
		},
	}
}

func selectionTestChainTip() *mockChainTip {
	return &mockChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: bytes.Repeat([]byte{0xAA}, 32),
			},
			BlockNumber: 100,
		},
	}
}

func newSelectionTestBuilder(
	t *testing.T,
	mempool *mockMempool,
	chainTip *mockChainTip,
	validator TxValidator,
) *DefaultBlockBuilder {
	t.Helper()
	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        16384,
		MaxBlockBodySize: 90112,
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: &mockPParamsProvider{pparams: pparams},
		ChainTip:        chainTip,
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: setupTestCredentials(t),
		TxValidator: validator,
	})
	require.NoError(t, err)
	return builder
}

// TestBuildBlockPinsOneValidationSessionPerBlock verifies that a whole
// block's transaction selection runs inside a single validation session
// (the LedgerState-backed equivalent pins one ledger snapshot and one
// repeatable-read database transaction for it), rather than opening a fresh
// session per transaction. Before this, each transaction's
// ValidateTxWithOverlay call could observe a different ledger generation
// mid-selection (issue #3506).
func TestBuildBlockPinsOneValidationSessionPerBlock(t *testing.T) {
	validator := &sessionMockTxValidator{}
	builder := newSelectionTestBuilder(
		t,
		threeTxMempoolForSelection(t),
		selectionTestChainTip(),
		validator,
	)

	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	require.Len(t, block.Transactions(), 3)
	require.Equal(
		t,
		1,
		validator.sessions,
		"all transactions in one block must share a single pinned validation session",
	)
	require.Equal(t, 3, validator.validateCalls)
}

// TestBuildBlockRejectsWhenValidationSnapshotGoesStale verifies that a
// ledger publication observed partway through transaction selection (a new
// block, rollback, epoch transition, or protocol parameter change) rejects
// the whole candidate block instead of silently returning one assembled
// from transactions checked against different generations.
func TestBuildBlockRejectsWhenValidationSnapshotGoesStale(t *testing.T) {
	validator := &sessionMockTxValidator{staleAfterCalls: 1}
	builder := newSelectionTestBuilder(
		t,
		threeTxMempoolForSelection(t),
		selectionTestChainTip(),
		validator,
	)

	block, _, err := builder.BuildBlock(1001, 0)
	require.Error(t, err)
	require.Nil(t, block)
	require.ErrorIs(t, err, errTxValidationSnapshotChanged)
}

// TestBuildBlockRejectsWhenParentChangesDuringSelection simulates a peer
// block advancing the primary chain tip while a locally-forged block is
// still selecting mempool transactions against the previously-current
// parent. The builder must reject the stale candidate itself rather than
// binding VRF/KES signing to a parent that chain adoption will refuse
// anyway once the tip has moved (issue #3506).
func TestBuildBlockRejectsWhenParentChangesDuringSelection(t *testing.T) {
	chainTip := selectionTestChainTip()
	validator := &sessionMockTxValidator{}
	validator.onValidate = func(callNumber int) {
		if callNumber != 1 {
			return
		}
		// A concurrent peer block lands on the chain mid-selection: the
		// tip this forge attempt already committed to as its parent is
		// no longer current.
		chainTip.tip = ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1001,
				Hash: bytes.Repeat([]byte{0xBB}, 32),
			},
			BlockNumber: 101,
		}
	}
	builder := newSelectionTestBuilder(
		t,
		threeTxMempoolForSelection(t),
		chainTip,
		validator,
	)

	block, _, err := builder.BuildBlock(1002, 0)
	require.Error(t, err)
	require.Nil(t, block)
	require.ErrorIs(t, err, errParentChangedDuringBuild)
}

// TestBuildBlockAcceptsStableParentAcrossSelection is the negative case for
// TestBuildBlockRejectsWhenParentChangesDuringSelection: when nothing moves
// the tip during selection, the recheck must not itself reject a
// legitimately unchanged parent.
func TestBuildBlockAcceptsStableParentAcrossSelection(t *testing.T) {
	validator := &sessionMockTxValidator{}
	builder := newSelectionTestBuilder(
		t,
		threeTxMempoolForSelection(t),
		selectionTestChainTip(),
		validator,
	)

	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	require.Len(t, block.Transactions(), 3)
}
