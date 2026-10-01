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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	omockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
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

// blockStageSampleCount returns the sample count and sum recorded under the
// given stage label of dingo_ledger_block_stage_duration_seconds.
func blockStageSampleCount(
	t *testing.T,
	m *stateMetrics,
	stage string,
) (count uint64, sum float64) {
	t.Helper()
	metric := &dto.Metric{}
	obs, ok := m.blockStageDuration.WithLabelValues(stage).(prometheus.Histogram)
	require.True(t, ok, "stage observer must be a prometheus.Histogram")
	require.NoError(t, obs.Write(metric))
	return metric.GetHistogram().GetSampleCount(), metric.GetHistogram().
		GetSampleSum()
}

func namedMetricGaugeValue(
	t *testing.T,
	registry *prometheus.Registry,
	name string,
) float64 {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == name {
			require.Len(t, family.Metric, 1)
			return family.Metric[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("metric %q was not registered", name)
	return 0
}

func TestBlockfetchEventInProgressGaugeReportsStalls(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	var m stateMetrics
	m.init(registry)
	const metric = "dingo_ledger_blockfetch_event_in_progress_seconds"
	require.Zero(t, namedMetricGaugeValue(t, registry, metric))

	olderID := m.beginBlockfetchEvent()
	m.blockfetchEventMu.Lock()
	m.blockfetchEventStarts[olderID] = time.Now().Add(-14 * time.Minute)
	m.blockfetchEventMu.Unlock()
	require.GreaterOrEqual(
		t,
		namedMetricGaugeValue(t, registry, metric),
		14*60.0,
	)
	newerID := m.beginBlockfetchEvent()
	m.blockfetchEventMu.Lock()
	m.blockfetchEventStarts[newerID] = time.Now().Add(-7 * time.Minute)
	m.blockfetchEventMu.Unlock()
	m.endBlockfetchEvent(olderID)
	remaining := namedMetricGaugeValue(t, registry, metric)
	require.GreaterOrEqual(t, remaining, 7*60.0)
	require.Less(t, remaining, 14*60.0)
	m.endBlockfetchEvent(newerID)
	require.Zero(t, namedMetricGaugeValue(t, registry, metric))
}

// TestObserveBlockStageRecordsUnderEachLabel is the regression test for the
// per-block stage histogram wiring: each of the four known stages must
// record its own duration sample under its own label, so a dashboard can
// break down where per-block ledger time goes.
func TestObserveBlockStageRecordsUnderEachLabel(t *testing.T) {
	t.Parallel()

	var m stateMetrics
	m.init(prometheus.NewRegistry())

	m.observeBlockStage(blockStageHeaderVerify, 10*time.Millisecond)
	m.observeBlockStage(blockStageValidate, 20*time.Millisecond)
	m.observeBlockStage(blockStageApply, 30*time.Millisecond)
	m.observeBlockStage(blockStageValidate, 5*time.Millisecond)
	m.observeBlockStage(blockStageEpochRollover, 40*time.Second)

	count, _ := blockStageSampleCount(t, &m, blockStageHeaderVerify)
	assert.Equal(t, uint64(1), count)
	count, sum := blockStageSampleCount(t, &m, blockStageValidate)
	assert.Equal(t, uint64(2), count)
	assert.InDelta(t, 0.025, sum, 0.0001)
	count, _ = blockStageSampleCount(t, &m, blockStageApply)
	assert.Equal(t, uint64(1), count)
	count, _ = blockStageSampleCount(t, &m, blockStageEpochRollover)
	assert.Equal(t, uint64(1), count)

	// Four distinct label series, no more.
	assert.Equal(
		t,
		4,
		testutil.CollectAndCount(m.blockStageDuration),
	)
}

// TestObserveBlockStageIgnoresUnknownStage confirms an unrecognized stage
// label is silently dropped rather than panicking or creating a stray label
// series. init pre-materializes the four known stage series, so this
// checks their sample counts stay zero rather than the series count, which
// is already 4 regardless of any observation.
func TestObserveBlockStageIgnoresUnknownStage(t *testing.T) {
	t.Parallel()

	var m stateMetrics
	m.init(prometheus.NewRegistry())

	m.observeBlockStage("not_a_real_stage", time.Millisecond)

	assert.Equal(
		t,
		4,
		testutil.CollectAndCount(m.blockStageDuration),
		"init pre-materializes exactly the four known stage series",
	)
	for _, stage := range []string{
		blockStageHeaderVerify,
		blockStageValidate,
		blockStageApply,
		blockStageEpochRollover,
	} {
		count, _ := blockStageSampleCount(t, &m, stage)
		assert.Zerof(
			t, count,
			"stage %q must not have recorded an unknown-stage observation",
			stage,
		)
	}
}

// TestObserveBlockStageNoopWhenMetricsDisabled confirms the observe helper is
// safe to call on a *stateMetrics that was never initialized (metrics
// disabled), matching the other observe helpers in this file.
func TestObserveBlockStageNoopWhenMetricsDisabled(t *testing.T) {
	t.Parallel()

	var m stateMetrics
	// m.init is never called: blockStageDuration and friends stay nil.
	m.observeBlockStage(blockStageHeaderVerify, time.Millisecond)
}

// TestBlockStageDurationBucketsCoverTailStalls pins the histogram's upper
// range to the epoch-boundary stalls it has to resolve.
// blinklabs-io/dingo#4364 measured block application blocked for 25s to 318s
// across preview boundaries; with the old ExponentialBuckets(0.0001, 2, 16)
// ceiling of ~3.3s every one of those landed in +Inf, indistinguishable from
// each other.
func TestBlockStageDurationBucketsCoverTailStalls(t *testing.T) {
	t.Parallel()

	var m stateMetrics
	m.init(prometheus.NewRegistry())

	metric := &dto.Metric{}
	obs, ok := m.blockStageDuration.WithLabelValues(blockStageApply).(prometheus.Histogram)
	require.True(t, ok, "stage observer must be a prometheus.Histogram")
	require.NoError(t, obs.Write(metric))

	buckets := metric.GetHistogram().GetBucket()
	require.NotEmpty(
		t,
		buckets,
		"histogram must have at least one finite bucket boundary",
	)
	largest := buckets[len(buckets)-1].GetUpperBound()
	assert.GreaterOrEqual(
		t,
		largest,
		318.0,
		"largest finite bucket boundary (%vs) must resolve the 318s "+
			"epoch-boundary stall measured in #4364",
		largest,
	)
}

// blockStageMaxDurationValue returns the value reg exports for the given
// stage label of dingo_ledger_block_stage_max_duration_seconds. It reads the
// registry rather than a collector handle held on stateMetrics, because the
// metric deliberately keeps no exported-value state of its own to hold: the
// three GaugeFunc collectors read the running-maximum atomics at scrape time.
func blockStageMaxDurationValue(
	t *testing.T,
	reg *prometheus.Registry,
	stage string,
) float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "dingo_ledger_block_stage_max_duration_seconds" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "stage" &&
					label.GetValue() == stage {
					return metric.GetGauge().GetValue()
				}
			}
		}
	}
	t.Fatalf(
		"no dingo_ledger_block_stage_max_duration_seconds series for stage %q",
		stage,
	)
	return 0
}

// TestBlockStageMaxDurationExportsRunningMaximum drives the exported metric
// through observeBlockStage: it must start at zero, rise on a larger
// observation, hold on a smaller one, and move only the stage that was
// observed.
func TestBlockStageMaxDurationExportsRunningMaximum(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	var m stateMetrics
	m.init(reg)

	for _, stage := range []string{
		blockStageHeaderVerify,
		blockStageValidate,
		blockStageApply,
		blockStageEpochRollover,
	} {
		assert.Equal(
			t,
			0.0,
			blockStageMaxDurationValue(t, reg, stage),
			"stage %q must export zero before any observation",
			stage,
		)
	}

	m.observeBlockStage(blockStageApply, 5*time.Second)
	assert.Equal(t, 5.0, blockStageMaxDurationValue(t, reg, blockStageApply))

	m.observeBlockStage(blockStageApply, 2*time.Second)
	assert.Equal(
		t,
		5.0,
		blockStageMaxDurationValue(t, reg, blockStageApply),
		"a smaller observation after a larger one must not lower the record",
	)

	m.observeBlockStage(blockStageApply, 9*time.Second)
	assert.Equal(
		t,
		9.0,
		blockStageMaxDurationValue(t, reg, blockStageApply),
		"a larger observation must raise the record",
	)

	for _, stage := range []string{
		blockStageHeaderVerify,
		blockStageValidate,
		blockStageEpochRollover,
	} {
		assert.Equal(
			t,
			0.0,
			blockStageMaxDurationValue(t, reg, stage),
			"observing %q must not move stage %q",
			blockStageApply,
			stage,
		)
	}
}

// TestBlockStageMaxDurationExportedValueEqualsRecord hammers one stage from
// many goroutines at once and requires the exported value to equal the
// largest duration any writer submitted -- exactly, not merely to be bounded
// by it.
//
// Exact equality is the point. It is what distinguishes reading the
// running-maximum atomic at scrape time from pushing each new maximum into a
// Gauge: with a pushed Gauge two writers can each win the compare-and-swap
// and land their Set calls in the other order, leaving the exported value
// below the record with nothing to recover it until an observation beats the
// record itself (see updateMaxDuration). Run with -race, which also covers
// the compare-and-swap loop under concurrent writers.
func TestBlockStageMaxDurationExportedValueEqualsRecord(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	var m stateMetrics
	m.init(reg)

	const writers = 64
	var wg sync.WaitGroup
	wg.Add(writers)
	for i := range writers {
		// Each writer submits a larger value and then a smaller one, so
		// both directions race against every other writer.
		observed := time.Duration(i) * time.Millisecond
		go func() {
			defer wg.Done()
			m.observeBlockStage(blockStageApply, observed)
			m.observeBlockStage(blockStageApply, observed/2)
		}()
	}
	wg.Wait()

	want := (time.Duration(writers-1) * time.Millisecond).Seconds()
	assert.Equal(
		t,
		want,
		blockStageMaxDurationValue(t, reg, blockStageApply),
		"the exported value must equal the largest observed duration, "+
			"regardless of goroutine interleaving",
	)
}

// blockApplyBatchLatencySample returns the sample count and sum recorded by
// dingo_ledger_block_apply_batch_latency_seconds.
func blockApplyBatchLatencySample(
	t *testing.T,
	m *stateMetrics,
) (count uint64, sum float64) {
	t.Helper()
	metric := &dto.Metric{}
	require.NoError(t, m.blockApplyBatchLatency.Write(metric))
	return metric.GetHistogram().GetSampleCount(), metric.GetHistogram().
		GetSampleSum()
}

// blockApplyBatchSizeSample returns the sample count and sum recorded by
// dingo_ledger_block_apply_batch_size.
func blockApplyBatchSizeSample(
	t *testing.T,
	m *stateMetrics,
) (count uint64, sum float64) {
	t.Helper()
	metric := &dto.Metric{}
	require.NoError(t, m.blockApplyBatchSize.Write(metric))
	return metric.GetHistogram().GetSampleCount(), metric.GetHistogram().
		GetSampleSum()
}

// blockApplyBatchMaxLatencyValue returns the value reg exports for
// dingo_ledger_block_apply_batch_max_latency_seconds. It reads the registry
// rather than a collector handle held on stateMetrics, because the metric
// deliberately keeps no exported-value state of its own to hold: the
// GaugeFunc collector reads the running-maximum atomic at scrape time.
func blockApplyBatchMaxLatencyValue(
	t *testing.T,
	reg *prometheus.Registry,
) float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "dingo_ledger_block_apply_batch_max_latency_seconds" {
			continue
		}
		require.Len(t, family.GetMetric(), 1)
		return family.GetMetric()[0].GetGauge().GetValue()
	}
	t.Fatalf(
		"no dingo_ledger_block_apply_batch_max_latency_seconds series found",
	)
	return 0
}

// largestFiniteBucket returns the widest finite upper bound of a histogram.
func largestFiniteBucket(t *testing.T, h prometheus.Metric) float64 {
	t.Helper()
	metric := &dto.Metric{}
	require.NoError(t, h.Write(metric))
	buckets := metric.GetHistogram().GetBucket()
	require.NotEmpty(t, buckets, "histogram has no finite bucket boundary")
	return buckets[len(buckets)-1].GetUpperBound()
}

// TestBlockApplyBatchLatencyBucketsCoverBlockStageRange requires the batch
// histogram to reach at least as far as
// dingo_ledger_block_stage_duration_seconds. A batch window contains the
// validate and apply stages of every block in the chunk, so a range narrower
// than the per-stage one would put in +Inf a batch whose single stage the
// stage histogram still resolves.
func TestBlockApplyBatchLatencyBucketsCoverBlockStageRange(t *testing.T) {
	t.Parallel()

	var m stateMetrics
	m.init(prometheus.NewRegistry())

	stageObserver, ok := m.blockStageApply.(prometheus.Metric)
	require.True(t, ok, "stage observer must be a collectable histogram")
	stageLargest := largestFiniteBucket(t, stageObserver)
	batchLargest := largestFiniteBucket(t, m.blockApplyBatchLatency)
	assert.GreaterOrEqual(
		t,
		batchLargest,
		stageLargest,
		"batch latency range (%vs) is narrower than the block stage range (%vs)",
		batchLargest,
		stageLargest,
	)
}

// TestObserveBlockApplyBatchRecordsLatencyAndSize is the regression test for
// the observeBlockApplyBatch wiring: each call must record one sample under
// both the latency histogram and the companion batch-size histogram, using
// the values passed in.
func TestObserveBlockApplyBatchRecordsLatencyAndSize(t *testing.T) {
	t.Parallel()

	var m stateMetrics
	m.init(prometheus.NewRegistry())

	m.observeBlockApplyBatch(1, 10*time.Millisecond)
	m.observeBlockApplyBatch(50, 200*time.Millisecond)

	count, sum := blockApplyBatchLatencySample(t, &m)
	assert.Equal(t, uint64(2), count)
	assert.InDelta(t, 0.210, sum, 0.0001)

	sizeCount, sizeSum := blockApplyBatchSizeSample(t, &m)
	assert.Equal(t, uint64(2), sizeCount)
	assert.InDelta(t, 51.0, sizeSum, 0.0001, "1 + 50 blocks observed")
}

// TestObserveBlockApplyBatchNoopWhenMetricsDisabled confirms the observe
// helper is safe to call on a *stateMetrics that was never initialized
// (metrics disabled), matching the other observe helpers in this file.
func TestObserveBlockApplyBatchNoopWhenMetricsDisabled(t *testing.T) {
	t.Parallel()

	var m stateMetrics
	// m.init is never called: blockApplyBatchLatency and friends stay nil.
	m.observeBlockApplyBatch(1, time.Millisecond)
}

// TestBlockApplyBatchLatencyMaxDurationExportsRunningMaximum drives the
// exported max gauge through observeBlockApplyBatch: it must start at zero,
// rise on a larger observation, and hold (not fall) on a smaller one.
func TestBlockApplyBatchLatencyMaxDurationExportsRunningMaximum(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	var m stateMetrics
	m.init(reg)

	assert.Equal(
		t,
		0.0,
		blockApplyBatchMaxLatencyValue(t, reg),
		"must export zero before any observation",
	)

	m.observeBlockApplyBatch(1, 5*time.Second)
	assert.Equal(t, 5.0, blockApplyBatchMaxLatencyValue(t, reg))

	m.observeBlockApplyBatch(1, 2*time.Second)
	assert.Equal(
		t,
		5.0,
		blockApplyBatchMaxLatencyValue(t, reg),
		"a smaller observation after a larger one must not lower the record",
	)

	m.observeBlockApplyBatch(1, 9*time.Second)
	assert.Equal(
		t,
		9.0,
		blockApplyBatchMaxLatencyValue(t, reg),
		"a larger observation must raise the record",
	)
}

// TestBlockApplyBatchLatencyMaxDurationExportedValueEqualsRecord hammers the
// running maximum from many goroutines at once and requires the exported
// GaugeFunc value to equal the largest duration any writer submitted --
// exactly, not merely bounded by it. Exact equality is what distinguishes
// reading the running-maximum atomic at scrape time from pushing each new
// maximum into a separately-updated Gauge: with a pushed Gauge, two writers
// can each win the compare-and-swap in updateMaxDuration and then land their
// Set calls in the other order, leaving the exported value below the record
// with nothing to recover it (see updateMaxDuration's doc comment). Run with
// -race, which also covers the compare-and-swap loop under concurrent
// writers.
func TestBlockApplyBatchLatencyMaxDurationExportedValueEqualsRecord(
	t *testing.T,
) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	var m stateMetrics
	m.init(reg)

	const writers = 64
	var wg sync.WaitGroup
	wg.Add(writers)
	for i := range writers {
		// Each writer submits a larger value and then a smaller one, so
		// both directions race against every other writer.
		observed := time.Duration(i) * time.Millisecond
		go func() {
			defer wg.Done()
			m.observeBlockApplyBatch(1, observed)
			m.observeBlockApplyBatch(1, observed/2)
		}()
	}
	wg.Wait()

	want := (time.Duration(writers-1) * time.Millisecond).Seconds()
	assert.Equal(
		t,
		want,
		blockApplyBatchMaxLatencyValue(t, reg),
		"the exported value must equal the largest observed duration, "+
			"regardless of goroutine interleaving",
	)
}

// TestLedgerProcessBlocksFromSourceObservesBlockApplyBatchLatency drives one
// real block through the actual ledgerProcessBlocksFromSource batch-apply
// loop (the call site observeBlockApplyBatch was added to in state.go), not
// just the isolated observe helper, so this proves the instrumentation is
// actually wired into the production apply path rather than merely present
// as a metric type. Reuses newByronShelleyBoundaryLedger's harness (real
// LedgerState, real sqlite-backed database, a real decodable Shelley block)
// from byron_shelley_boundary_test.go: applying firstShelley there commits
// exactly one block through the same DB-transaction chunk this metric times.
func TestLedgerProcessBlocksFromSourceObservesBlockApplyBatchLatency(
	t *testing.T,
) {
	t.Parallel()

	ls, _, firstShelley := newByronShelleyBoundaryLedger(t)

	count, _ := blockApplyBatchLatencySample(t, &ls.metrics)
	require.Zero(t, count, "no batch applied yet")

	results := make(chan readChainResult, 1)
	results <- readChainResult{blocks: []gledger.Block{firstShelley}}
	close(results)

	require.NoError(t, ls.ledgerProcessBlocksFromSource(
		context.Background(),
		results,
	))
	require.Equal(
		t,
		firstShelley.SlotNumber(),
		ls.currentTip.Point.Slot,
		"the block must have actually committed and advanced the tip",
	)

	latencyCount, latencySum := blockApplyBatchLatencySample(t, &ls.metrics)
	require.Equal(
		t,
		uint64(1),
		latencyCount,
		"applying one batch must record exactly one latency observation",
	)
	assert.Greater(
		t,
		latencySum,
		0.0,
		"a real DB-backed apply must take measurable wall-clock time",
	)

	sizeCount, sizeSum := blockApplyBatchSizeSample(t, &ls.metrics)
	require.Equal(t, uint64(1), sizeCount)
	assert.Equal(
		t,
		1.0,
		sizeSum,
		"the committed chunk contained exactly one block",
	)
}
