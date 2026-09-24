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
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/blinklabs-io/dingo/internal/nodeparity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		client, err := nodeparity.NewKoiosClient(network, "", baseURL, true)
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
