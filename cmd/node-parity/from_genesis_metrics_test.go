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

package main

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/blinklabs-io/dingo/internal/nodeparity"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// countersWithMetrics returns a fromGenesisCounters wired to a metrics set
// registered into a private registry, so each test asserts on its own
// series rather than on process-global state.
func countersWithMetrics(
	t *testing.T,
) (*fromGenesisCounters, *fromGenesisMetrics) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m := newFromGenesisMetricsIn("preview", reg)
	return &fromGenesisCounters{metrics: m}, m
}

// TestRecordEpochRecordsDivergenceMetrics pins the wiring this file exists
// for: before it, from-genesis reported a divergence only through its own
// counters, the log and the exit code, so node_parity_divergence_total never
// moved and docs/dashboards/alerts.yaml's NodeParityDivergence rule could not
// fire for a from-genesis run.
//
// Proven by revert-and-test: removing any one recordDivergence call from
// recordEpoch leaves the corresponding subtest failing while the counter
// assertions stay green, which is exactly the gap this closes.
func TestRecordEpochRecordsDivergenceMetrics(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)

	t.Run("protocol params", func(t *testing.T) {
		t.Parallel()
		c, m := countersWithMetrics(t)
		c.recordEpoch(nodeparity.EpochResult{
			Epoch: 650,
			ProtocolParamsMismatches: []koiosparity.CheckMismatch{{
				Field: "minFeeA", DingoValue: "44", KoiosValue: "45",
			}},
			UTxOAttempted: true,
		}, logger)

		require.Equal(t, 1, c.ppMismatches)
		require.InDelta(t, 1.0, testutil.ToFloat64(
			m.divergenceTotal.WithLabelValues("protocol_params", ReferenceKoios),
		), 0.001, "a protocol-params divergence must move divergenceTotal")
	})

	t.Run("stake distribution", func(t *testing.T) {
		t.Parallel()
		c, m := countersWithMetrics(t)
		c.recordEpoch(nodeparity.EpochResult{
			Epoch: 650,
			StakeMismatches: []nodeparity.StakeMismatch{{
				PoolIDBech32: "pool1abc", DingoStake: 2, KoiosStake: "1",
				DiffLovelace: 1,
			}},
			UTxOAttempted: true,
		}, logger)

		require.Equal(t, 1, c.stakeMismatches)
		require.InDelta(t, 1.0, testutil.ToFloat64(
			m.divergenceTotal.WithLabelValues("stake_distribution", ReferenceKoios),
		), 0.001, "a stake divergence must move divergenceTotal")
	})

	t.Run("utxo", func(t *testing.T) {
		t.Parallel()
		c, m := countersWithMetrics(t)
		c.recordEpoch(nodeparity.EpochResult{
			Epoch:         650,
			UTxOAttempted: true,
			UTxOMissing:   []string{"txin#0"},
		}, logger)

		require.Equal(t, 1, c.utxoMismatches)
		require.InDelta(t, 1.0, testutil.ToFloat64(
			m.divergenceTotal.WithLabelValues("utxo", ReferenceKoios),
		), 0.001, "a UTxO divergence must move divergenceTotal")
	})
}

// TestRecordEpochRecordsIncompleteMetrics pins that a check which could not
// be trusted lands in epochChecksIncompleteTotal{field} rather than
// divergenceTotal. The distinction is the point: "Koios was unreachable" and
// "Dingo answered the wrong value" must not page identically.
func TestRecordEpochRecordsIncompleteMetrics(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)
	c, m := countersWithMetrics(t)

	c.recordEpoch(nodeparity.EpochResult{
		Epoch:             651,
		ProtocolParamsErr: errors.New("koios unreachable"),
		StakeErr:          errors.New("koios unreachable"),
		UTxOAttempted:     true,
		UTxOErr:           errors.New("tx_info fetch failed"),
	}, logger)

	for _, field := range fromGenesisFields {
		require.InDelta(t, 1.0, testutil.ToFloat64(
			m.epochChecksIncompleteTotal.WithLabelValues(field),
		), 0.001, "an untrusted %s check must count as incomplete", field)
		require.InDelta(t, 0.0, testutil.ToFloat64(
			m.divergenceTotal.WithLabelValues(field, ReferenceKoios),
		), 0.001, "an untrusted %s check must NOT count as a divergence", field)
	}
}

// TestRecordEpochCountsEveryEpoch pins that epochsTotal advances once per
// epoch that reached a verdict, so a liveness rule can tell a stalled replay
// from a quiet one.
func TestRecordEpochCountsEveryEpoch(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)
	c, m := countersWithMetrics(t)

	for epoch := uint64(1); epoch <= 3; epoch++ {
		c.recordEpoch(nodeparity.EpochResult{
			Epoch: epoch, UTxOAttempted: true,
		}, logger)
	}

	require.Equal(t, 3, c.epochsChecked)
	require.InDelta(t, 3.0, testutil.ToFloat64(m.epochsTotal), 0.001)
}

// TestRecordEpochWithoutMetricsDoesNotPanic pins the nil-metrics path: a
// run without --metrics-addr keeps working exactly as before, with no
// listening socket and no registered collectors.
func TestRecordEpochWithoutMetricsDoesNotPanic(t *testing.T) {
	t.Parallel()
	var c fromGenesisCounters
	require.Nil(t, c.metrics)

	c.recordEpoch(nodeparity.EpochResult{
		Epoch:         650,
		UTxOAttempted: true,
		UTxOMissing:   []string{"txin#0"},
		StakeMismatches: []nodeparity.StakeMismatch{{
			PoolIDBech32: "pool1abc", DingoStake: 2, KoiosStake: "1",
		}},
	}, slog.New(slog.DiscardHandler))

	require.Equal(t, 1, c.utxoMismatches)
	require.Equal(t, 1, c.stakeMismatches)
}

// TestRecordEpochDoesNotCountAWhollyIncompleteEpoch pins CodeRabbit's
// finding on #4771: an epoch whose every check was untrusted verified
// nothing, so folding it into epochsTotal would inflate the count an
// operator reads as "epochs actually validated" and make a wholly-degraded
// run look like a working one.
func TestRecordEpochDoesNotCountAWhollyIncompleteEpoch(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)
	c, m := countersWithMetrics(t)

	// Every check untrusted: nothing completed.
	c.recordEpoch(nodeparity.EpochResult{
		Epoch:             700,
		ProtocolParamsErr: errors.New("koios unreachable"),
		StakeErr:          errors.New("koios unreachable"),
		UTxOAttempted:     true,
		UTxOErr:           errors.New("tx_info fetch failed"),
	}, logger)
	require.InDelta(t, 0.0, testutil.ToFloat64(m.epochsTotal), 0.001,
		"an epoch with no trustworthy verdict must not count as validated")

	// A partially-degraded epoch still completed something, so it counts.
	c.recordEpoch(nodeparity.EpochResult{
		Epoch:         701,
		StakeErr:      errors.New("koios unreachable"),
		UTxOAttempted: true,
	}, logger)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.epochsTotal), 0.001,
		"an epoch where at least one check reached a verdict must count")
}

// TestRecordDivergenceIdentifiesKoiosAsTheReference pins the other half of
// that review: from-genesis compares against Koios, not cardano-node, and a
// responder reading NodeParityDivergence has to know which oracle
// disagreed. Without the label the alert sends them to the wrong side.
func TestRecordDivergenceIdentifiesKoiosAsTheReference(t *testing.T) {
	t.Parallel()
	c, m := countersWithMetrics(t)

	c.recordEpoch(nodeparity.EpochResult{
		Epoch:         702,
		UTxOAttempted: true,
		UTxOMissing:   []string{"txin#0"},
	}, slog.New(slog.DiscardHandler))

	require.InDelta(t, 1.0, testutil.ToFloat64(
		m.divergenceTotal.WithLabelValues("utxo", ReferenceKoios),
	), 0.001, "a from-genesis divergence must be tagged reference=koios")
	require.InDelta(t, 0.0, testutil.ToFloat64(
		m.divergenceTotal.WithLabelValues("utxo", ReferenceCardanoNode),
	), 0.001, "it must not be attributed to cardano-node")
}

// TestFromGenesisMetricsRegistersNoWatchCounters is the regression test for
// the review finding on #4771 that from-genesis would false-alert.
//
// NodeParityNotChecking fires when checks_total, checks_skipped_total and
// check_errors_total are all flat for 10 minutes. from-genesis records one
// epoch every few minutes to hours -- measured at a 10.5 min median past
// preview epoch 1100 and rising with the UTxO set -- so as long as it shares
// those series, a healthy replay reads as a dead tool. Exposing them at a
// constant zero is just as bad as incrementing them too slowly, since a zero
// series is exactly what that rule matches on.
//
// Reverting from-genesis to newParityMetricsIn fails this test on the first
// forbidden name.
func TestFromGenesisMetricsRegistersNoWatchCounters(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	newFromGenesisMetricsIn("preview", reg)

	families, err := reg.Gather()
	require.NoError(t, err)
	got := make(map[string]bool, len(families))
	for _, f := range families {
		got[f.GetName()] = true
	}

	for _, name := range []string{
		"node_parity_checks_total",
		"node_parity_checks_skipped_total",
		"node_parity_check_errors_total",
	} {
		require.False(t, got[name],
			"from-genesis must not register %s: NodeParityNotChecking is "+
				"sized for watch's per-block cadence and would fire on a "+
				"healthy replay", name)
	}
	for _, name := range []string{
		"node_parity_epochs_total",
		"node_parity_epoch_checks_incomplete_total",
		"node_parity_divergence_total",
	} {
		require.True(t, got[name], "from-genesis must register %s", name)
	}
}

// TestFromGenesisMetricsPreMaterializesZeroSeries pins that every field
// label exists from process start. A CounterVec exposes no series until
// something increments it, and NodeParityFromGenesisNotVerifying joins two
// vectors with "and" -- a label missing from either side drops the whole
// comparison, so an alert on a never-yet-incremented field could never fire.
func TestFromGenesisMetricsPreMaterializesZeroSeries(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newFromGenesisMetricsIn("preview", reg)

	for _, field := range fromGenesisFields {
		require.InDelta(t, 0.0, testutil.ToFloat64(
			m.epochChecksIncompleteTotal.WithLabelValues(field),
		), 0.001, "%s must expose a zero sample before anything increments it", field)
		require.InDelta(t, 0.0, testutil.ToFloat64(
			m.divergenceTotal.WithLabelValues(field, ReferenceKoios),
		), 0.001, "%s divergence must expose a zero sample too", field)
	}
}
