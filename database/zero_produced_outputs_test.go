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
	"bytes"
	"fmt"
	"log/slog"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// collateralReturnAddress is an arbitrary mainnet payment address; the
// collateral-return fixtures only need a decodable one.
const collateralReturnAddress = "addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd"

// noOutputsTx is the legal zero-output shape from issue #3932: a valid
// transaction that declares no outputs, so Produced() is empty too. On chain
// this is e.g. a stake registration that spends its whole input on the deposit
// plus the fee and returns no change.
type noOutputsTx struct {
	lcommon.Transaction
}

func (t noOutputsTx) Outputs() []lcommon.TransactionOutput { return nil }

func (t noOutputsTx) Produced() []lcommon.Utxo { return nil }

// droppedOutputsTx is the unexpected shape: a valid transaction that declares
// outputs but produces no UTxOs. Produced() maps one-to-one onto Outputs() for
// a valid transaction, so this cannot occur on chain -- it would mean outputs
// were dropped before storage.
type droppedOutputsTx struct {
	lcommon.Transaction
}

func (t droppedOutputsTx) Produced() []lcommon.Utxo { return nil }

// invalidTx is the phase-2 failure shape, where Produced() is the collateral
// return alone. A nil collateralReturn is the legal zero-output case; a
// non-nil one that still produces nothing has lost the collateral return.
type invalidTx struct {
	lcommon.Transaction
	collateralReturn lcommon.TransactionOutput
}

func (t invalidTx) IsValid() bool { return false }

func (t invalidTx) CollateralReturn() lcommon.TransactionOutput {
	return t.collateralReturn
}

func (t invalidTx) Produced() []lcommon.Utxo { return nil }

// collateralReturnTx is the shape a real phase-2 failure takes in Babbage and
// later: invalid, a non-nil collateral return, and a Produced() that carries
// that return at index len(Outputs()). It is the true negative for the
// collateral branch of the warning -- the only shape where a non-nil
// CollateralReturn() coexists with a correctly stored UTxO, so it is the shape
// that would expose the branch firing on declaration alone rather than on loss.
type collateralReturnTx struct {
	lcommon.Transaction
	collateralReturn lcommon.TransactionOutput
}

func (t collateralReturnTx) IsValid() bool { return false }

func (t collateralReturnTx) CollateralReturn() lcommon.TransactionOutput {
	return t.collateralReturn
}

func (t collateralReturnTx) Produced() []lcommon.Utxo {
	return []lcommon.Utxo{
		{
			Id: shelley.NewShelleyTransactionInput(
				t.Hash().String(),
				len(t.Outputs()),
			),
			Output: t.collateralReturn,
		},
	}
}

// TestSetTransactionZeroProducedOutputsLogging covers the log emitted when a
// transaction stores no UTxOs. A zero-output transaction is a legal shape and
// must not warn; only losing declared outputs is worth an operator's
// attention.
func TestSetTransactionZeroProducedOutputsLogging(t *testing.T) {
	candidate := findGapConsumeCandidateWithoutCertificates(t)

	// newStagedDB stages the consumer's producers so the consumed inputs
	// resolve, leaving the produced-side logging as the only thing under test.
	newStagedDB := func(t *testing.T, logs *bytes.Buffer) *Database {
		t.Helper()
		db, err := newTestDatabase(t, &Config{
			DataDir: t.TempDir(),
			Logger: slog.New(slog.NewJSONHandler(
				logs,
				&slog.HandlerOptions{Level: slog.LevelDebug},
			)),
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		for _, p := range candidate.producers {
			storeBlockOffsetsOnly(t, db, p.block)
			metaTxn := db.MetadataTxn(true)
			producer := p
			require.NoError(t, metaTxn.Do(func(txn *Txn) error {
				return db.Metadata().SetGapBlockTransaction(
					producer.tx,
					producer.point,
					0,
					txn.Metadata(),
				)
			}))
			metaTxn.Release()
		}
		storeBlockOffsetsOnly(t, db, candidate.consumerBlock)
		return db
	}

	// setTx discards everything newStagedDB logged before calling the function
	// under test, so an assertion sees only that call's output. Staging writes
	// blocks and producer transactions, and a warning from that setup would
	// otherwise decide a "does not warn" subtest.
	setTx := func(
		t *testing.T,
		db *Database,
		logs *bytes.Buffer,
		tx lcommon.Transaction,
		withOffsets ...func(*BlockIngestionResult),
	) string {
		t.Helper()
		offsets := mustBlockOffsets(t, candidate.consumerBlock)
		for _, fn := range withOffsets {
			fn(offsets)
		}
		logs.Reset()
		require.NoError(t, db.SetTransactionWithOpts(
			tx,
			candidate.consumerPoint,
			0,
			0,
			nil,
			nil,
			offsets,
			nil,
			BatchedTxIngestOpts{},
		))
		return logs.String()
	}

	t.Run("zero-output transaction does not warn", func(t *testing.T) {
		var logs bytes.Buffer
		db := newStagedDB(t, &logs)
		out := setTx(
			t, db, &logs,
			noOutputsTx{Transaction: candidate.consumerTx},
		)
		require.NotContains(t, out, `"level":"WARN"`)
	})

	t.Run("transaction with outputs does not warn", func(t *testing.T) {
		var logs bytes.Buffer
		db := newStagedDB(t, &logs)
		out := setTx(t, db, &logs, candidate.consumerTx)
		require.NotContains(t, out, `"level":"WARN"`)
	})

	t.Run("dropped outputs warn", func(t *testing.T) {
		var logs bytes.Buffer
		db := newStagedDB(t, &logs)
		out := setTx(
			t, db, &logs,
			droppedOutputsTx{Transaction: candidate.consumerTx},
		)
		require.Contains(t, out, `"level":"WARN"`)
		require.Contains(
			t,
			out,
			"valid transaction produced no UTxOs despite declaring outputs",
		)
		// The count names what was dropped, so it must be the declared
		// outputs rather than the (empty) produced set.
		require.Contains(t, out, fmt.Sprintf(
			`"outputs":%d`,
			len(candidate.consumerTx.Outputs()),
		))
	})

	t.Run(
		"invalid transaction without collateral return does not warn",
		func(t *testing.T) {
			var logs bytes.Buffer
			db := newStagedDB(t, &logs)
			out := setTx(
				t, db, &logs,
				invalidTx{Transaction: candidate.consumerTx},
			)
			require.NotContains(t, out, `"level":"WARN"`)
		},
	)

	t.Run("dropped collateral return warns", func(t *testing.T) {
		const collateralLovelace = 1_000_000
		collateralReturn, err := mockledger.NewTransactionOutputBuilder().
			WithAddress(collateralReturnAddress).
			WithLovelace(collateralLovelace).
			Build()
		require.NoError(t, err)
		var logs bytes.Buffer
		db := newStagedDB(t, &logs)
		out := setTx(t, db, &logs, invalidTx{
			Transaction:      candidate.consumerTx,
			collateralReturn: collateralReturn,
		})
		require.Contains(t, out, `"level":"WARN"`)
		// The dropped declaration here is the collateral return, so the
		// message and the attribute must name it. Reporting the transaction's
		// outputs instead would point an operator at a field that is not what
		// went missing.
		require.Contains(
			t,
			out,
			"invalid transaction produced no UTxOs despite declaring "+
				"a collateral return",
		)
		require.Contains(t, out, fmt.Sprintf(
			`"collateralReturnLovelace":"%d"`,
			collateralLovelace,
		))
		require.NotContains(t, out, `"outputs":`)
		require.NotContains(t, out, "despite declaring outputs")
	})

	t.Run(
		"invalid transaction keeping its collateral return does not warn",
		func(t *testing.T) {
			collateralReturn, err := mockledger.NewTransactionOutputBuilder().
				WithAddress(collateralReturnAddress).
				WithLovelace(1_000_000).
				Build()
			require.NoError(t, err)
			var logs bytes.Buffer
			db := newStagedDB(t, &logs)
			var txHash [32]byte
			copy(txHash[:], ledgerHashBytes(candidate.consumerTx.Hash()))
			collateralIdx := uint32(len(candidate.consumerTx.Outputs()))
			out := setTx(
				t, db, &logs,
				collateralReturnTx{
					Transaction:      candidate.consumerTx,
					collateralReturn: collateralReturn,
				},
				func(offsets *BlockIngestionResult) {
					// The indexer only emits offsets for the outputs the
					// transaction declares, so the collateral return's index
					// has none and SetTransactionWithOpts would fail before
					// reaching the log. Nothing on this path decodes the
					// offset, so output 0's span stands in for it.
					offsets.UtxoOffsets[UtxoRef{
						TxId:      txHash,
						OutputIdx: collateralIdx,
					}] = offsets.UtxoOffsets[UtxoRef{
						TxId:      txHash,
						OutputIdx: 0,
					}]
				},
			)
			require.NotContains(t, out, `"level":"WARN"`)
		},
	)
}
