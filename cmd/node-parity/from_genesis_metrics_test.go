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
func countersWithMetrics(t *testing.T) (*fromGenesisCounters, *parityMetrics) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m := newParityMetricsIn("preview", reg)
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
			m.divergenceTotal.WithLabelValues("protocol_params"),
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
			m.divergenceTotal.WithLabelValues("stake_distribution"),
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
			m.divergenceTotal.WithLabelValues("utxo"),
		), 0.001, "a UTxO divergence must move divergenceTotal")
	})
}

// TestRecordEpochRecordsSkipMetrics pins that a check which could not be
// trusted lands in checksSkippedTotal rather than divergenceTotal. The
// distinction is the point: "Koios was unreachable" and "Dingo answered the
// wrong value" must not page identically.
func TestRecordEpochRecordsSkipMetrics(t *testing.T) {
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

	for _, reason := range []string{
		"protocol_params", "stake_distribution", "utxo",
	} {
		require.InDelta(t, 1.0, testutil.ToFloat64(
			m.checksSkippedTotal.WithLabelValues(reason),
		), 0.001, "an untrusted %s check must count as skipped", reason)
		require.InDelta(t, 0.0, testutil.ToFloat64(
			m.divergenceTotal.WithLabelValues(reason),
		), 0.001, "an untrusted %s check must NOT count as a divergence", reason)
	}
}

// TestRecordEpochCountsEveryEpoch pins that checksTotal advances once per
// epoch regardless of verdict, so NodeParityNotChecking can tell a stalled
// run from a quiet one.
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
	require.InDelta(t, 3.0, testutil.ToFloat64(m.checksTotal), 0.001)
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
