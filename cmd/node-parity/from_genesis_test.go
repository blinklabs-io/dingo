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
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/blinklabs-io/dingo/internal/nodeparity"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
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
// finding on an epoch whose every check was untrusted verified
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
// the review finding that from-genesis would false-alert.
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

// TestShouldServeMetrics pins the --metrics-addr opt-in, which was unpinned
// until review on replacing the condition with
// `globalFlags.metricsAddr != ""` left the whole cmd/node-parity suite
// green. The flag defaults to ":9464", so that mutation makes every
// from-genesis run bind a wildcard port nobody asked for, and fail outright
// when the port is taken -- on a replay that runs for days.
//
// The three cases are the whole contract: absent means off despite the
// non-empty default, present means on, and an explicit empty value is the
// documented way to say off.
func TestShouldServeMetrics(t *testing.T) {
	t.Parallel()

	newFlags := func() *pflag.FlagSet {
		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		fs.String("metrics-addr", defaultMetricsAddr, "")
		return fs
	}

	t.Run("absent means off even though the default is non-empty", func(t *testing.T) {
		t.Parallel()
		fs := newFlags()
		require.False(t, shouldServeMetrics(fs, defaultMetricsAddr),
			"an unset --metrics-addr must not bind the default port")
	})

	t.Run("explicitly passed means on", func(t *testing.T) {
		t.Parallel()
		fs := newFlags()
		require.NoError(t, fs.Parse([]string{"--metrics-addr=127.0.0.1:0"}))
		addr, err := fs.GetString("metrics-addr")
		require.NoError(t, err)
		require.True(t, shouldServeMetrics(fs, addr))
	})

	t.Run("explicitly empty means off", func(t *testing.T) {
		t.Parallel()
		fs := newFlags()
		require.NoError(t, fs.Parse([]string{"--metrics-addr="}))
		addr, err := fs.GetString("metrics-addr")
		require.NoError(t, err)
		require.Empty(t, addr)
		require.False(t, shouldServeMetrics(fs, addr),
			"--metrics-addr= is the documented way to turn metrics off")
	})
}

// TestSplitStakeMismatches pins the from-genesis report's separation of a
// real Dingo/Koios divergence from a KoiosFault entry (an unparseable koios
// active_stake value): counting the latter toward stakeMismatches would
// page on Koios's own data quality rather than a real Dingo bug, mirroring
// the wrong outcome DetermineStatus already prevents on the protocol-params
// side.
func TestSplitStakeMismatches(t *testing.T) {
	real := nodeparity.StakeMismatch{
		PoolIDBech32: "pool1real",
		DingoStake:   100,
		KoiosStake:   "50",
		DiffLovelace: 50,
	}
	fault := nodeparity.StakeMismatch{
		PoolIDBech32: "pool1fault",
		DingoStake:   100,
		KoiosStake:   "not-a-number",
		Reason:       "unparseable koios active_stake value",
		KoiosFault:   true,
	}

	t.Run("empty input", func(t *testing.T) {
		gotReal, gotFaults := splitStakeMismatches(nil)
		assert.Empty(t, gotReal)
		assert.Empty(t, gotFaults)
	})

	t.Run("real mismatch only", func(t *testing.T) {
		gotReal, gotFaults := splitStakeMismatches(
			[]nodeparity.StakeMismatch{real},
		)
		assert.Equal(t, []nodeparity.StakeMismatch{real}, gotReal)
		assert.Empty(t, gotFaults)
	})

	t.Run("koios fault only", func(t *testing.T) {
		gotReal, gotFaults := splitStakeMismatches(
			[]nodeparity.StakeMismatch{fault},
		)
		assert.Empty(t, gotReal)
		assert.Equal(t, []nodeparity.StakeMismatch{fault}, gotFaults)
	})

	t.Run("mixed keeps both, in order", func(t *testing.T) {
		gotReal, gotFaults := splitStakeMismatches(
			[]nodeparity.StakeMismatch{real, fault},
		)
		assert.Equal(t, []nodeparity.StakeMismatch{real}, gotReal)
		assert.Equal(t, []nodeparity.StakeMismatch{fault}, gotFaults)
	})
}

// TestFromGenesisCounters_RecordEpoch drives recordEpoch directly with a
// synthetic EpochResult and asserts on the resulting counters.
// TestSplitStakeMismatches above proves the partition helper is correct in
// isolation, but reverting recordEpoch's stake branch to the
// "stakeMismatches++ for any non-empty StakeMismatches" shape would leave
// that test green, since it never calls recordEpoch at all. Also covers the
// Incomplete counters: without them, a run whose every check was degraded
// reports 0 mismatches across the board, indistinguishable from a clean
// run.
func TestFromGenesisCounters_RecordEpoch(t *testing.T) {
	logger := discardLogger()

	t.Run("clean epoch: no mismatch/incomplete counters move, all three verified", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(nodeparity.EpochResult{Epoch: 1, UTxOAttempted: true}, logger)
		assert.Equal(t, fromGenesisCounters{
			epochsChecked: 1,
			ppVerified:    1, stakeVerified: 1, utxoVerified: 1,
		}, c)
	})

	t.Run("real stake mismatch counts as a mismatch, not incomplete, and is verified", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(nodeparity.EpochResult{
			Epoch: 1,
			StakeMismatches: []nodeparity.StakeMismatch{{
				PoolIDBech32: "pool1real", DingoStake: 100, KoiosStake: "50", DiffLovelace: 50,
			}},
			UTxOAttempted: true,
		}, logger)
		assert.Equal(t, 1, c.stakeMismatches)
		assert.Equal(t, 0, c.stakeIncomplete)
		assert.Equal(t, 1, c.stakeVerified,
			"a real mismatch is still a trustworthy result, not an incomplete one")
	})

	t.Run("koios-fault-only stake mismatch counts as incomplete, not a mismatch, and is not verified", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(nodeparity.EpochResult{
			Epoch: 1,
			StakeMismatches: []nodeparity.StakeMismatch{{
				PoolIDBech32: "pool1fault", DingoStake: 100, KoiosStake: "not-a-number",
				Reason: "unparseable koios active_stake value", KoiosFault: true,
			}},
			UTxOAttempted: true,
		}, logger)
		assert.Equal(t, 0, c.stakeMismatches,
			"a Koios data fault must not be counted as a Dingo divergence")
		assert.Equal(t, 1, c.stakeIncomplete)
		assert.Equal(t, 0, c.stakeVerified)
	})

	t.Run("StakeErr counts as incomplete, not verified", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(nodeparity.EpochResult{
			Epoch: 1, StakeErr: assert.AnError, UTxOAttempted: true,
		}, logger)
		assert.Equal(t, 0, c.stakeMismatches)
		assert.Equal(t, 1, c.stakeIncomplete)
		assert.Equal(t, 0, c.stakeVerified)
	})

	t.Run("ProtocolParamsErr counts as incomplete, not verified", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(nodeparity.EpochResult{
			Epoch: 1, ProtocolParamsErr: assert.AnError, UTxOAttempted: true,
		}, logger)
		assert.Equal(t, 0, c.ppMismatches)
		assert.Equal(t, 1, c.ppIncomplete)
		assert.Equal(t, 0, c.ppVerified)
	})

	t.Run("UTxOErr counts as incomplete, not verified", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(nodeparity.EpochResult{
			Epoch: 1, UTxOAttempted: true, UTxOErr: assert.AnError,
		}, logger)
		assert.Equal(t, 0, c.utxoMismatches)
		assert.Equal(t, 1, c.utxoIncomplete)
		assert.Equal(t, 0, c.utxoVerified)
	})

	t.Run("UTxO never attempted counts as incomplete, not verified", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(nodeparity.EpochResult{Epoch: 1, UTxOAttempted: false}, logger)
		assert.Equal(t, 0, c.utxoMismatches)
		assert.Equal(t, 1, c.utxoIncomplete)
		assert.Equal(t, 0, c.utxoVerified)
	})

	t.Run("real UTxO mismatch counts as a mismatch, not incomplete, and is verified", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(nodeparity.EpochResult{
			Epoch: 1, UTxOAttempted: true, UTxOMissing: []string{"abc#0"},
		}, logger)
		assert.Equal(t, 1, c.utxoMismatches)
		assert.Equal(t, 0, c.utxoIncomplete)
		assert.Equal(t, 1, c.utxoVerified,
			"a real mismatch is still a trustworthy result, not an incomplete one")
	})

	t.Run("epochsChecked increments once per call, across multiple epochs", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(nodeparity.EpochResult{Epoch: 1, UTxOAttempted: true}, logger)
		c.recordEpoch(nodeparity.EpochResult{Epoch: 2, UTxOAttempted: true}, logger)
		assert.Equal(t, 2, c.epochsChecked)
	})
}

// TestFromGenesisCounters_Result pins fromGenesisRun's actual exit-code
// decision: a Dingo-side query error after a successful Acquire, a
// Koios-side data fault, and an expected retention-floor Acquire rejection
// all land in the same *Incomplete counters recordEpoch fills in, with no
// mismatch counted for any of them -- so a run in which Dingo failed every
// single query, all epoch, would exit 0 without the "verified nothing"
// check below: every mismatch counter stays exactly 0, indistinguishable
// from a run that genuinely checked everything and found no divergence.
func TestFromGenesisCounters_Result(t *testing.T) {
	t.Run("no epochs reached: nil, not a false 'verified nothing' failure", func(t *testing.T) {
		var c fromGenesisCounters
		assert.NoError(t, c.result(),
			"a run that never reached an epoch boundary is reported through RunFromGenesis's own error return, not this check")
	})

	t.Run("epochs reached, everything verified, no mismatches: nil", func(t *testing.T) {
		c := fromGenesisCounters{
			epochsChecked: 3,
			ppVerified:    3, stakeVerified: 3, utxoVerified: 3,
		}
		assert.NoError(t, c.result())
	})

	t.Run("a real mismatch fails the run even if plenty was verified", func(t *testing.T) {
		c := fromGenesisCounters{
			epochsChecked: 3,
			ppVerified:    3, stakeVerified: 3, utxoVerified: 2,
			utxoMismatches: 1,
		}
		require.Error(t, c.result())
		assert.Contains(t, c.result().Error(), "diverged from Koios")
	})

	t.Run("every epoch's every check incomplete: fails, even with zero mismatches", func(t *testing.T) {
		c := fromGenesisCounters{
			epochsChecked:   5,
			ppIncomplete:    5,
			stakeIncomplete: 5,
			utxoIncomplete:  5,
		}
		require.Error(t, c.result(),
			"reverting this check in place would exit 0 for a run that verified nothing at all")
		assert.Contains(t, c.result().Error(), "verified nothing")
	})

	t.Run("at least one check verified in at least one epoch: not a 'verified nothing' failure", func(t *testing.T) {
		c := fromGenesisCounters{
			epochsChecked:   5,
			ppVerified:      1,
			ppIncomplete:    4,
			stakeIncomplete: 5,
			utxoIncomplete:  5,
		}
		assert.NoError(t, c.result(),
			"one genuinely verified check across the whole run is enough to not call this run a total loss")
	})
}

// TestRequireMatchingKoiosSource covers the guard that keeps a shared
// cache.db from mixing two Koios hosts' answers.
//
// OpenCache alone leaves assertClaimedSource with nothing claimed, so every
// later write succeeds regardless of which host produced the rows already
// there -- from-genesis would read one oracle's answers and write another's
// under the existing stamp. The cache this command is normally pointed at is
// a dingo instance's own, so the mismatch must be refused rather than
// recorded: RecordKoiosSource would discard every cached row for the network.
func TestRequireMatchingKoiosSource(t *testing.T) {
	const network = "preview"

	newCache := func(t *testing.T) (*koiosparity.Cache, string) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "cache.db")
		cache, err := koiosparity.OpenCache(path, slog.New(
			slog.NewTextHandler(io.Discard, nil),
		))
		require.NoError(t, err)
		t.Cleanup(func() { _ = cache.Close() })
		return cache, path
	}

	newClient := func(t *testing.T, baseURL string) *koiosparity.KoiosClient {
		t.Helper()
		client, err := nodeparity.NewKoiosClient(network, "", baseURL, true, true)
		require.NoError(t, err)
		return client
	}

	t.Run("matching host pins the source", func(t *testing.T) {
		cache, path := newCache(t)
		koiosFlags.cachePath = path
		t.Cleanup(func() { koiosFlags.cachePath = "" })

		// Stamped through a separate handle, so the handle under test has
		// claimed nothing of its own before requireMatchingKoiosSource runs
		// -- otherwise this would assert on RecordKoiosSource's own claim
		// rather than on the pin.
		other, err := koiosparity.OpenCache(path, slog.New(
			slog.NewTextHandler(io.Discard, nil),
		))
		require.NoError(t, err)
		t.Cleanup(func() { _ = other.Close() })

		client := newClient(t, "http://mirror.example/api/v1")
		_, err = other.RecordKoiosSource(
			network, client.ResolvedBaseURL(), time.Now().UTC(),
		)
		require.NoError(t, err)

		require.NoError(t, requireMatchingKoiosSource(cache, client, network))

		// Pinned: another process re-pointing the cache must now fail this
		// run's writes rather than let them land under a source its answers
		// never came from.
		_, err = other.RecordKoiosSource(
			network, "http://other.example/api/v1", time.Now().UTC(),
		)
		require.NoError(t, err)

		err = cache.UpsertTxInfos(
			network,
			[]koiosparity.KoiosTxInfoItem{{TxHash: "aa"}},
			time.Now().UTC(),
		)
		require.Error(t, err, "a pinned run must not write after a re-point")
	})

	t.Run("mismatched host is refused", func(t *testing.T) {
		cache, path := newCache(t)
		koiosFlags.cachePath = path
		t.Cleanup(func() { koiosFlags.cachePath = "" })

		_, err := cache.RecordKoiosSource(
			network, "http://recorded.example/api/v1", time.Now().UTC(),
		)
		require.NoError(t, err)

		client := newClient(t, "http://different.example/api/v1")
		err = requireMatchingKoiosSource(cache, client, network)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "http://recorded.example/api/v1")
		assert.Contains(t, err.Error(), "http://different.example/api/v1")

		// Refused, never recorded: the rows the cache already holds are
		// still there and still attributed to the host that produced them.
		recorded, ok, err := cache.GetKoiosSource(network)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "http://recorded.example/api/v1", recorded)
	})

	t.Run("unstamped cache is judged by its public-root attribution", func(t *testing.T) {
		cache, path := newCache(t)
		koiosFlags.cachePath = path
		t.Cleanup(func() { koiosFlags.cachePath = "" })

		// Nothing recorded: the rows are attributed to the public root for
		// the network, so a custom host disagrees with them.
		err := requireMatchingKoiosSource(
			cache, newClient(t, "http://mirror.example/api/v1"), network,
		)
		require.Error(t, err)

		// The default client resolves to that same public root, so it
		// matches and pins.
		require.NoError(
			t,
			requireMatchingKoiosSource(cache, newClient(t, ""), network),
		)
	})
}
