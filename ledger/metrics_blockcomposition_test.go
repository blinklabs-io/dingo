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
	"fmt"
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	omockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestConwayBlock builds a minimal in-memory Conway block from transaction
// bodies and witness sets, without going through CBOR decode. block.Era() is a
// fixed value on ConwayBlockHeader (EraConway) regardless of the header's
// other fields, so a zero-valued header is enough to exercise
// computeBlockComposition.
func newTestConwayBlock(
	bodies []conway.ConwayTransactionBody,
	witnessSets []conway.ConwayTransactionWitnessSet,
	invalid []uint,
) lcommon.Block {
	return &conway.ConwayBlock{
		BlockHeader:            &conway.ConwayBlockHeader{},
		TransactionBodies:      bodies,
		TransactionWitnessSets: witnessSets,
		InvalidTransactions:    invalid,
	}
}

// testInput returns a distinct ShelleyTransactionInput for the given fill
// byte, suitable as a fixture input/collateral reference. The composition
// counting under test never inspects the hash itself, so any well-formed
// 32-byte hex string is enough.
func testInput(fill byte, index int) shelley.ShelleyTransactionInput {
	return shelley.NewShelleyTransactionInput(
		strings.Repeat(fmt.Sprintf("%02x", fill), 32),
		index,
	)
}

// TestComputeBlockCompositionNoScripts is the regression test for the plain
// (no Plutus, no redeemer) case: era, transaction count, UTxO churn, and
// certificate count must all come from the fixture's shape, and hasScripts/
// redeemers must stay at their zero values.
func TestComputeBlockCompositionNoScripts(t *testing.T) {
	t.Parallel()

	body := conway.ConwayTransactionBody{
		TxInputs: conway.NewConwayTransactionInputSet(
			[]shelley.ShelleyTransactionInput{testInput('a', 0)},
		),
		TxOutputs: []babbage.BabbageTransactionOutput{{}, {}},
		TxCertificates: []lcommon.CertificateWrapper{
			{Certificate: &lcommon.StakeRegistrationCertificate{}},
		},
	}
	block := newTestConwayBlock(
		[]conway.ConwayTransactionBody{body},
		[]conway.ConwayTransactionWitnessSet{{}},
		nil,
	)

	c := computeBlockComposition(block)

	assert.Equal(t, "Conway", c.era)
	assert.Equal(t, 1, c.transactions)
	assert.False(t, c.hasScripts)
	assert.Equal(t, 0, c.redeemers)
	assert.Equal(t, 2, c.utxoCreated)
	assert.Equal(t, 1, c.utxoConsumed)
	assert.Equal(t, 1, c.certificates)
}

// TestComputeBlockCompositionWithPlutusScript proves a block whose only
// transaction carries a Plutus V2 script and a redeemer is reported as
// script-bearing, with the redeemer counted.
func TestComputeBlockCompositionWithPlutusScript(t *testing.T) {
	t.Parallel()

	body := conway.ConwayTransactionBody{
		TxInputs: conway.NewConwayTransactionInputSet(
			[]shelley.ShelleyTransactionInput{testInput('b', 0)},
		),
		TxOutputs: []babbage.BabbageTransactionOutput{{}},
	}
	witnesses := conway.ConwayTransactionWitnessSet{
		WsPlutusV2Scripts: cbor.NewSetType(
			[]lcommon.PlutusV2Script{{0x01, 0x02}},
			false,
		),
		WsRedeemers: conway.ConwayRedeemers{
			Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
				{Tag: lcommon.RedeemerTagSpend, Index: 0}: {},
			},
		},
	}
	block := newTestConwayBlock(
		[]conway.ConwayTransactionBody{body},
		[]conway.ConwayTransactionWitnessSet{witnesses},
		nil,
	)

	c := computeBlockComposition(block)

	assert.True(t, c.hasScripts)
	assert.Equal(t, 1, c.redeemers)
}

// TestComputeBlockCompositionRedeemerWithoutWitnessScript covers the
// reference-script case: the redeemer is present but the witness set carries
// no Plutus script at all (the script comes from a reference input instead).
// A redeemer alone still means phase-2 evaluation ran, so hasScripts must
// still be true -- this is the branch that would silently regress if the
// hasScripts check were narrowed to only the witness-set script lists.
func TestComputeBlockCompositionRedeemerWithoutWitnessScript(t *testing.T) {
	t.Parallel()

	body := conway.ConwayTransactionBody{
		TxInputs: conway.NewConwayTransactionInputSet(
			[]shelley.ShelleyTransactionInput{testInput('c', 0)},
		),
		TxOutputs: []babbage.BabbageTransactionOutput{{}},
	}
	witnesses := conway.ConwayTransactionWitnessSet{
		WsRedeemers: conway.ConwayRedeemers{
			Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
				{Tag: lcommon.RedeemerTagSpend, Index: 0}: {},
			},
		},
	}
	block := newTestConwayBlock(
		[]conway.ConwayTransactionBody{body},
		[]conway.ConwayTransactionWitnessSet{witnesses},
		nil,
	)

	c := computeBlockComposition(block)

	assert.True(
		t,
		c.hasScripts,
		"a redeemer with no witness-set script must still count as script-bearing",
	)
	assert.Equal(t, 1, c.redeemers)
}

// TestComputeBlockCompositionPhase2FailedTransactionUsesProducedNotOutputs is
// the regression test for the Produced()/Consumed() vs Outputs()/Inputs()
// distinction the issue calls out: for a phase-2-failed (invalid) transaction,
// Outputs() still reports the transaction's ordinary (never-realized)
// outputs and Inputs() the ordinary inputs, but only the collateral return
// and collateral inputs actually take effect. Reverting
// computeBlockComposition to raw Outputs()/Inputs() would overcount here
// without this test catching it.
func TestComputeBlockCompositionPhase2FailedTransactionUsesProducedNotOutputs(
	t *testing.T,
) {
	t.Parallel()

	body := conway.ConwayTransactionBody{
		// Ordinary inputs/outputs: never actually consumed/created, since
		// the transaction is marked invalid below.
		TxInputs: conway.NewConwayTransactionInputSet(
			[]shelley.ShelleyTransactionInput{testInput('d', 0)},
		),
		TxOutputs: []babbage.BabbageTransactionOutput{{}, {}},
		// Collateral: what actually gets consumed/produced for an invalid tx.
		TxCollateral: cbor.NewSetType(
			[]shelley.ShelleyTransactionInput{testInput('e', 0)},
			false,
		),
		TxCollateralReturn: &babbage.BabbageTransactionOutput{},
	}
	block := newTestConwayBlock(
		[]conway.ConwayTransactionBody{body},
		[]conway.ConwayTransactionWitnessSet{{}},
		[]uint{0}, // marks transaction 0 as invalid (phase-2 failed)
	)

	c := computeBlockComposition(block)

	assert.Equal(
		t,
		1,
		c.utxoCreated,
		"an invalid tx must count only its collateral return, not both ordinary outputs",
	)
	assert.Equal(
		t, 1, c.utxoConsumed,
		"an invalid tx must count its collateral input, not its ordinary input",
	)
}

// blockCompositionCounterValue reads one era-labelled sample of a
// *prometheus.CounterVec metric.
func blockCompositionCounterValue(
	vec *prometheus.CounterVec,
	era string,
) float64 {
	return testutil.ToFloat64(vec.WithLabelValues(era))
}

// TestObserveBlockCompositionRecordsUnderEraLabel is the regression test for
// the metric-recording wiring: each counter must accumulate under its own
// era label, with two distinct blocks under two distinct eras kept as two
// distinct series rather than conflated into one.
func TestObserveBlockCompositionRecordsUnderEraLabel(t *testing.T) {
	t.Parallel()

	var m stateMetrics
	m.init(prometheus.NewRegistry())

	m.observeBlockComposition(blockComposition{
		era:          "Babbage",
		transactions: 2,
		hasScripts:   true,
		redeemers:    3,
		utxoCreated:  4,
		utxoConsumed: 5,
		certificates: 1,
	})
	m.observeBlockComposition(blockComposition{
		era:          "Conway",
		transactions: 1,
		hasScripts:   false,
		redeemers:    0,
		utxoCreated:  1,
		utxoConsumed: 1,
		certificates: 0,
	})

	assert.Equal(
		t,
		float64(1),
		blockCompositionCounterValue(m.blocksTotal, "Babbage"),
	)
	assert.Equal(
		t,
		float64(1),
		blockCompositionCounterValue(m.blocksTotal, "Conway"),
	)
	assert.Equal(
		t,
		float64(2),
		blockCompositionCounterValue(m.blockTransactionsTotal, "Babbage"),
	)
	assert.Equal(
		t,
		float64(1),
		blockCompositionCounterValue(m.blockTransactionsTotal, "Conway"),
	)
	assert.Equal(
		t,
		float64(1),
		blockCompositionCounterValue(m.blocksWithScriptsTotal, "Babbage"),
	)
	assert.Equal(
		t,
		float64(0),
		blockCompositionCounterValue(m.blocksWithScriptsTotal, "Conway"),
	)
	assert.Equal(
		t,
		float64(3),
		blockCompositionCounterValue(m.redeemersTotal, "Babbage"),
	)
	assert.Equal(
		t,
		float64(4),
		blockCompositionCounterValue(m.utxoCreatedTotal, "Babbage"),
	)
	assert.Equal(
		t,
		float64(5),
		blockCompositionCounterValue(m.utxoConsumedTotal, "Babbage"),
	)
	assert.Equal(
		t,
		float64(1),
		blockCompositionCounterValue(m.certificatesTotal, "Babbage"),
	)
	assert.Equal(
		t,
		float64(0),
		blockCompositionCounterValue(m.certificatesTotal, "Conway"),
	)

	// Two distinct era labels, no more.
	assert.Equal(t, 2, testutil.CollectAndCount(m.blocksTotal))
}

// TestObserveBlockCompositionNoopWhenMetricsDisabled confirms the observe
// helper is safe to call on a *stateMetrics that was never initialized
// (metrics disabled), matching the other observe helpers in this file.
func TestObserveBlockCompositionNoopWhenMetricsDisabled(t *testing.T) {
	t.Parallel()

	var m stateMetrics
	// m.init is never called: every field stays nil.
	m.observeBlockComposition(blockComposition{era: "Conway", transactions: 1})
}

// compositionSpyTx counts how many times a transaction's produced UTxO set is
// materialized while its block's composition is computed.
type compositionSpyTx struct {
	lcommon.Transaction
	producedCalls *int
}

func (t compositionSpyTx) Produced() []lcommon.Utxo {
	*t.producedCalls++
	return t.Transaction.Produced()
}

// TestComputeBlockCompositionCountsValidOutputsWithoutBuildingUtxos pins the
// hot-path contract. computeBlockComposition runs inside the ledger's
// block-apply database transaction for every applied block, so counting the
// UTxOs a valid transaction creates must not build them: Produced() allocates
// one lcommon.Utxo per output and round-trips the transaction hash through hex
// to construct each UTxO's input reference, and the only use for that here is
// its length. Every era defines Produced() for a valid transaction as exactly
// one UTxO per output, so len(Outputs()) is the same number without the
// allocations. The phase-2-failed case, whose rule differs by era, is still
// read from Produced() -- see
// TestComputeBlockCompositionPhase2FailedTransactionUsesProducedNotOutputs.
func TestComputeBlockCompositionCountsValidOutputsWithoutBuildingUtxos(
	t *testing.T,
) {
	t.Parallel()

	const txCount, outputsPerTx = 3, 4
	producedCalls := 0
	txs := make([]lcommon.Transaction, 0, txCount)
	for range txCount {
		outputs := make([]lcommon.TransactionOutput, 0, outputsPerTx)
		for range outputsPerTx {
			outputs = append(outputs, &babbage.BabbageTransactionOutput{})
		}
		built, err := omockledger.NewTransactionBuilder().
			WithValid(true).
			WithInputs(testInput('f', 0)).
			WithOutputs(outputs...).
			Build()
		require.NoError(t, err)
		txs = append(txs, compositionSpyTx{
			Transaction:   built,
			producedCalls: &producedCalls,
		})
	}
	block := &validityOutcomeTestBlock{txs: txs, era: conway.EraConway}

	c := computeBlockComposition(block)

	assert.Equal(t, txCount*outputsPerTx, c.utxoCreated)
	assert.Equal(t, txCount, c.utxoConsumed)
	assert.Equal(
		t,
		0,
		producedCalls,
		"counting a valid transaction's created UTxOs must not build them",
	)
}
