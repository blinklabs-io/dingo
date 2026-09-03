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

package database

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// erroringMetadata wraps a real MetadataStore and returns injectErr from
// SetTransaction and SetTransactionBatched. Every other method delegates
// to the embedded real store unchanged (via interface embedding), so the
// production paths' pre-metadata prerequisites (blob writes, offset
// lookups, consumed-input recovery, batch accumulator lifecycle) work
// exactly as they do at runtime — only the final metadata-write step is
// forced to fail with a known inner error.
//
// This is the harness that turns this test file into an integration test
// of the three real wrap sites, rather than a same-file duplication of
// the format strings (per PR #2982 review).
type erroringMetadata struct {
	metadata.MetadataStore
	injectErr error
}

func (e *erroringMetadata) SetTransaction(
	tx lcommon.Transaction,
	point ocommon.Point,
	idx uint32,
	certDeposits map[int]uint64,
	skipWithdrawalWitness bool,
	txn types.Txn,
) error {
	return e.injectErr
}

func (e *erroringMetadata) SetTransactionBatched(
	tx lcommon.Transaction,
	point ocommon.Point,
	idx uint32,
	certDeposits map[int]uint64,
	skipWithdrawalWitness bool,
	acc types.MetadataBatchAccumulator,
	txn types.Txn,
) error {
	return e.injectErr
}

// TestSetTransactionMetadataErrorWrap_ProductionPaths exercises the three
// public entry points whose error wraps this commit changed:
//
//   - database.SetTransactionBatched → wrap at batch.go
//     ("set transaction metadata for tx %s (batch idx %d, slot %d): %w")
//   - database.SetTransaction → wrap at transaction.go
//     ("set transaction metadata for tx %s (block idx %d, slot %d): %w")
//   - database.SetTransactionMetadataOnly → wrap at transaction.go
//     ("set transaction metadata only for tx %s (block idx %d, slot %d): %w")
//
// The metadata plugin is a real SQLite store from openTestDB, then wrapped
// with erroringMetadata so the final metadata write returns a known inner
// error. Everything upstream of the metadata call — blob offset writes,
// consumed-input recovery, batch accumulator lifecycle — runs against the
// real code as it does in production.
//
// If any production wrap loses the tx hash, index, slot, inner-error text,
// or the errors.Is chain, this test fails loudly. If a production message
// drifts (e.g. "batch idx" → "batch index"), this test fails loudly.
//
// (Addresses PR #2982 coderabbit review: prior version constructed
// fmt.Errorf calls that mirrored the production strings, so drift in the
// production strings could not be detected.)
func TestSetTransactionMetadataErrorWrap_ProductionPaths(t *testing.T) {
	// Inner error mimics the real #2976 failure that motivated the wrap.
	inner := errors.New(
		"pool reward account: pool cert reward_account: got 2 bytes, want 29",
	)

	db := openTestDB(t)
	// Swap in the erroring wrapper. Same-package access to the unexported
	// `metadata` field is intentional and follows the pattern used by
	// other database/*_test.go files (e.g. batch_skip_test.go) that
	// prod internal state directly.
	db.metadata = &erroringMetadata{
		MetadataStore: db.metadata,
		injectErr:     inner,
	}

	candidate := findBatchedCrossBlockSpendCandidate(t)
	// %s renders Blake2b256 exactly as the production sites will
	// (all three wraps use `tx.Hash()` as a `%s` arg).
	txHashStr := fmt.Sprintf("%s", candidate.producerTx.Hash())
	require.GreaterOrEqual(
		t,
		len(txHashStr),
		8,
		"Blake2b256 %%s output too short to be a valid hash: %q",
		txHashStr,
	)
	idx := candidate.producerIdx
	slot := candidate.producerPoint.Slot
	idxStr := fmt.Sprint(idx)
	slotStr := fmt.Sprint(slot)

	// --- Path 1: batch (batch.go: SetTransactionBatched) ---
	// The batched path stages the producer's UTxOs so that a real accumulator
	// can be built and the consumed-input step passes. Only the terminal
	// metadata.SetTransactionBatched call is forced to fail.
	_ = stagedProducer(t, db, candidate)
	acc := db.NewBatchAccumulator()
	txn := db.Transaction(true)
	batchErr := db.SetTransactionBatched(
		candidate.producerTx,
		candidate.producerPoint,
		candidate.producerIdx,
		0,   // updateEpoch
		nil, // pparamUpdates
		nil, // certDeposits
		mustBlockOffsets(t, candidate.producerBlock),
		acc,
		txn,
	)
	_ = txn.Rollback()
	txn.Release()
	assertProductionWrap(
		t,
		batchErr,
		inner,
		"batch idx",
		idxStr,
		slotStr,
		txHashStr,
	)

	// --- Path 2: block (transaction.go: SetTransaction) ---
	// Non-batched form uses the same setup, but the wrap-site prefix is
	// "block idx" instead of "batch idx".
	txn2 := db.Transaction(true)
	blockErr := db.SetTransaction(
		candidate.producerTx,
		candidate.producerPoint,
		candidate.producerIdx,
		0,   // updateEpoch
		nil, // pparamUpdates
		nil, // certDeposits
		mustBlockOffsets(t, candidate.producerBlock),
		txn2,
	)
	_ = txn2.Rollback()
	txn2.Release()
	assertProductionWrap(
		t,
		blockErr,
		inner,
		"block idx",
		idxStr,
		slotStr,
		txHashStr,
	)

	// --- Path 3: metadata-only (transaction.go: SetTransactionMetadataOnly) ---
	// Simpler path — no offsets or consumed-input recovery — but must still
	// produce the "metadata only" phrasing plus tx hash / block idx / slot.
	txn3 := db.Transaction(true)
	metaErr := db.SetTransactionMetadataOnly(
		candidate.producerTx,
		candidate.producerPoint,
		candidate.producerIdx,
		nil, // certDeposits
		txn3,
	)
	_ = txn3.Rollback()
	txn3.Release()
	require.Error(t, metaErr)
	assertProductionWrap(
		t,
		metaErr,
		inner,
		"block idx",
		idxStr,
		slotStr,
		txHashStr,
	)
	// The "only" wrap has a distinguishing marker in addition to the shared
	// tx/idx/slot fields — pin it too so the two block-idx sites can't
	// collapse into the same wording without failing this test.
	require.Contains(
		t,
		metaErr.Error(),
		"metadata only for tx",
		"metadata-only wrap must contain 'metadata only for tx' marker; got %q",
		metaErr.Error(),
	)
}

// assertProductionWrap validates a wrapped error from one of the three
// production wrap sites: (a) errors.Is unwrap chain preserved,
// (b) tx hash rendered via %s appears, (c) idx label ("batch idx" or
// "block idx") and its numeric value appear, (d) slot decimal appears,
// (e) inner error text preserved. It intentionally does NOT pin the
// entire format string so minor wording refinements (e.g. reordering)
// don't require churn — only field drift fails.
func assertProductionWrap(
	t *testing.T,
	wrapped, inner error,
	idxLabel, idxStr, slotStr, txHashStr string,
) {
	t.Helper()
	require.Error(
		t,
		wrapped,
		"expected non-nil error from production wrap site",
	)
	require.Truef(
		t,
		errors.Is(wrapped, inner),
		"wrap chain broken: errors.Is(wrapped, inner) == false; wrapped=%q",
		wrapped.Error(),
	)
	msg := wrapped.Error()
	require.Contains(
		t,
		msg,
		txHashStr,
		"wrap missing tx hash %q; got %q",
		txHashStr,
		msg,
	)
	require.Contains(
		t,
		msg,
		idxLabel,
		"wrap missing %q label; got %q",
		idxLabel,
		msg,
	)
	require.Contains(t, msg, idxLabel+" "+idxStr,
		"wrap missing %q with numeric value %q; got %q", idxLabel, idxStr, msg)
	require.Contains(t, msg, "slot "+slotStr,
		"wrap missing slot decimal %q; got %q", "slot "+slotStr, msg)
	require.Contains(t, msg, inner.Error(),
		"wrap dropped inner error text %q; got %q", inner.Error(), msg)
	// Also require the shared prefix to keep the two blocks-idx sites
	// grepable by operators; the exact phrase is the invariant.
	require.True(
		t,
		strings.Contains(msg, "set transaction metadata for tx ") ||
			strings.Contains(msg, "set transaction metadata only for tx "),
		"wrap missing 'set transaction metadata[ only] for tx ' prefix; got %q",
		msg,
	)
}
