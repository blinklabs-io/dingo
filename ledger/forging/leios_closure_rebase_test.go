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
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// leiosClosureRebaseFixture is a ranking block's parent state holding two
// outputs, a certified closure spending the first, and a mempool that still
// holds the closure's transaction, a different transaction spending the same
// output, and an unrelated transaction spending the second.
type leiosClosureRebaseFixture struct {
	closureRaw [][]byte
	mempool    []MempoolTransaction
	validator  *leiosOverlayValidator
	unrelated  MempoolTransaction
}

func newLeiosClosureRebaseFixture(t *testing.T) leiosClosureRebaseFixture {
	t.Helper()
	closureCbor := makeMinimalTxCbor(t, 0x11, 0)
	// Same input as the closure transaction, a different fee.
	closureInput := make([]byte, 32)
	closureInput[0] = 0x11
	conflictCbor, err := cbor.Encode([]any{
		map[uint]any{
			0: cbor.Tag{
				Number:  258,
				Content: []any{[]any{closureInput, uint64(0)}},
			},
			1: []any{
				[]any{
					append([]byte{0x61}, make([]byte, 28)...),
					uint64(900000),
				},
			},
			2: uint64(300000),
		},
		map[uint]any{},
		true,
		nil,
	})
	require.NoError(t, err)
	unrelatedCbor := makeMinimalTxCbor(t, 0x55, 0)
	closureTx, err := conway.NewConwayTransactionFromCbor(closureCbor)
	require.NoError(t, err)
	conflictTx, err := conway.NewConwayTransactionFromCbor(conflictCbor)
	require.NoError(t, err)
	unrelatedTx, err := conway.NewConwayTransactionFromCbor(unrelatedCbor)
	require.NoError(t, err)
	require.NotEqual(t, closureTx.Hash(), conflictTx.Hash())
	require.Equal(t, closureTx.Inputs()[0], conflictTx.Inputs()[0])

	unrelated := MempoolTransaction{
		Hash: unrelatedTx.Hash().String(),
		Cbor: unrelatedCbor,
		Type: conway.TxTypeConway,
	}
	return leiosClosureRebaseFixture{
		closureRaw: [][]byte{blockTxFromMempool(t, closureCbor)},
		mempool: []MempoolTransaction{
			{
				Hash: closureTx.Hash().String(),
				Cbor: closureCbor,
				Type: conway.TxTypeConway,
			},
			{
				Hash: conflictTx.Hash().String(),
				Cbor: conflictCbor,
				Type: conway.TxTypeConway,
			},
			unrelated,
		},
		validator: &leiosOverlayValidator{
			base: map[utxoref.Key]struct{}{
				utxoref.ForInput(closureTx.Inputs()[0]):   {},
				utxoref.ForInput(unrelatedTx.Inputs()[0]): {},
			},
		},
		unrelated: unrelated,
	}
}

// An endorser block announced by a certifying ranking block extends the state
// after the certified closure. The closure's transactions are already applied
// there, and so is every input they spend, so neither the closure transaction
// nor one conflicting with it may be selected, although both are still valid
// against the parent state the validator sees.
func TestSelectValidLeiosTransactionsRebasesOntoCertifiedClosure(t *testing.T) {
	fixture := newLeiosClosureRebaseFixture(t)

	// Without the closure the parent state accepts the closure transaction,
	// which is what re-packed an already-certified closure.
	selected, _, err := selectValidLeiosTransactions(context.Background(),
		fixture.mempool,
		fixture.validator,
		leiosSelectionLimits{},
		leiosSelectionBasis{},
	)
	require.NoError(t, err)
	require.Equal(t, fixture.mempool[0].Hash, selected[0].Hash)

	closure, err := newLeiosCertifiedClosure(fixture.closureRaw)
	require.NoError(t, err)
	selected, _, err = selectValidLeiosTransactions(context.Background(),
		fixture.mempool,
		fixture.validator,
		leiosSelectionLimits{},
		leiosSelectionBasis{closure: closure},
	)
	require.NoError(t, err)
	require.Equal(t, []MempoolTransaction{fixture.unrelated}, selected)
}

// The closure is served as block_transaction CBOR, whose blake2b-256 is the
// manifest reference. The mempool keys transactions by transaction id, so the
// closure must be matched by id; the reference hash never matches.
func TestNewLeiosCertifiedClosureKeysByTransactionID(t *testing.T) {
	fixture := newLeiosClosureRebaseFixture(t)
	closure, err := newLeiosCertifiedClosure(fixture.closureRaw)
	require.NoError(t, err)
	require.True(t, closure.applies(fixture.mempool[0].Hash))
	require.False(t, closure.applies(
		lcommon.Blake2b256Hash(fixture.closureRaw[0]).String(),
	))
	require.False(t, closure.applies(fixture.unrelated.Hash))

	_, err = newLeiosCertifiedClosure([][]byte{{0x01}})
	require.Error(t, err)
}

// The forge loop's end-to-end path: a ranking block that certifies its
// parent's endorser block and announces a new one must leave the certified
// closure, and anything spending its inputs, out of the new endorser block.
func TestCheckAndForgeLeiosEBExcludesCertifiedClosure(t *testing.T) {
	fixture := newLeiosClosureRebaseFixture(t)
	caster := &forgerTestLeiosCaster{}
	forger := &BlockForger{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:    time.Now,
		// No slot clock: the selection budget is this reserve from now.
		forgeEBSelectionReserve: time.Hour,
		leiosChecker:            &forgerTestLeiosChecker{allowed: true},
		leiosMempool:            forgerTestMempoolProvider{txs: fixture.mempool},
		leiosValidator:          fixture.validator,
		leiosEBCaster:           caster,
		leiosCerts: &forgerTestLeiosCerts{
			closureTxs:   fixture.closureRaw,
			closureTxsOK: true,
		},
	}
	closure, err := forger.certifiedClosureForRebase(
		lcommon.NewBlake2b256(bytes.Repeat([]byte{0x33}, 32)),
		9,
	)
	require.NoError(t, err)
	announcement, err := forger.checkAndForgeLeiosEB(
		context.Background(),
		10,
		leiosSelectionBasis{closure: closure},
	)
	require.NoError(t, err)
	require.NotNil(t, announcement)
	require.Equal(
		t,
		[][]byte{blockTxFromMempool(t, fixture.unrelated.Cbor)},
		caster.txBodies,
	)
}

// parentPinnedValidator is leiosOverlayValidator with the ledger's
// parent-pinned session: the session opens only when the ledger tip is the
// requested parent.
type parentPinnedValidator struct {
	*leiosOverlayValidator
	tip        []byte
	gotParents [][]byte
}

func (v *parentPinnedValidator) WithTxValidationSessionAtParent(
	_ context.Context,
	parentHash []byte,
	fn func(
		validate TxValidationFunc,
		stillCurrent func() bool,
		commitIfCurrent func(func() error) (bool, error),
	) error,
) error {
	v.gotParents = append(v.gotParents, parentHash)
	if !bytes.Equal(parentHash, v.tip) {
		return fmt.Errorf(
			"%w: tip %x, parent %x",
			ErrTxValidationParentMismatch,
			v.tip,
			parentHash,
		)
	}
	return fn(
		v.ValidateTxWithOverlay,
		func() bool { return true },
		func(commit func() error) (bool, error) { return true, commit() },
	)
}

var _ ParentPinnedTxValidationSessionProvider = (*parentPinnedValidator)(nil)

// The forger builds on the primary chain tip while the tip gates let the
// ledger trail it by a short apply backlog. Endorser-block selection must not
// validate against that trailing state, which has not applied the parent's
// blocks or any certified closure they carry: it is pinned to the parent, and
// skipped while the ledger is elsewhere.
func TestCheckAndForgeLeiosEBRequiresLedgerAtParent(t *testing.T) {
	fixture := newLeiosClosureRebaseFixture(t)
	parent := bytes.Repeat([]byte{0x77}, 32)
	validator := &parentPinnedValidator{
		leiosOverlayValidator: fixture.validator,
		tip:                   bytes.Repeat([]byte{0x66}, 32),
	}
	caster := &forgerTestLeiosCaster{}
	forger := &BlockForger{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:    time.Now,
		// No slot clock: the selection budget is this reserve from now.
		forgeEBSelectionReserve: time.Hour,
		leiosChecker:            &forgerTestLeiosChecker{allowed: true},
		leiosMempool:            forgerTestMempoolProvider{txs: fixture.mempool},
		leiosValidator:          validator,
		leiosEBCaster:           caster,
	}

	announcement, err := forger.checkAndForgeLeiosEB(
		context.Background(),
		10,
		leiosSelectionBasis{parentHash: parent},
	)
	require.NoError(t, err)
	require.Nil(t, announcement, "a trailing ledger must not select")
	require.Empty(t, caster.hash)
	require.Equal(t, [][]byte{parent}, validator.gotParents)

	validator.tip = parent
	announcement, err = forger.checkAndForgeLeiosEB(
		context.Background(),
		10,
		leiosSelectionBasis{parentHash: parent},
	)
	require.NoError(t, err)
	require.NotNil(t, announcement)
	require.NotEmpty(t, caster.hash)
}
