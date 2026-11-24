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

package nodeparity

import (
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLiveCheckResultErr(t *testing.T) {
	t.Parallel()
	t.Run("matched is nil", func(t *testing.T) {
		require.NoError(t, liveCheckResultErr(&CheckResult{
			Tip: Tip{Slot: 100, Hash: "aa"},
		}))
	})
	t.Run("skipped is an error, not a silent match", func(t *testing.T) {
		require.Error(t, liveCheckResultErr(&CheckResult{
			Skipped:    true,
			SkipReason: SkipTipMismatch,
			SkipDetail: "tips did not match",
		}))
	})
	// Lines() rolls truncated UTxO entries into one summary line, so the
	// error must state the true count (1 + 2 + 5), not the line count (4).
	t.Run("diverged reports the true count", func(t *testing.T) {
		err := liveCheckResultErr(&CheckResult{
			Tip: Tip{Slot: 100, Hash: "aa"},
			Diff: Diff{
				ProtocolParamsDiff: "protocol parameters differ",
				UTxO:               []string{"utxo a differs", "utxo b differs"},
				TruncatedUTxO:      5,
			},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "8 difference(s)")
	})
}

func TestLiveAtPoint(t *testing.T) {
	t.Parallel()
	hash := strings.Repeat("ab", 32)
	at, err := liveAtPoint("", "")
	require.NoError(t, err)
	assert.Nil(t, at, "neither set means the live tip")

	at, err = liveAtPoint("12345", hash)
	require.NoError(t, err)
	require.NotNil(t, at)
	assert.Equal(t, Tip{Slot: 12345, Hash: hash}, *at)

	for name, tc := range map[string][2]string{
		"slot without hash":  {"12345", ""},
		"hash without slot":  {"", hash},
		"zero slot":          {"0", hash},
		"non-numeric slot":   {"abc", hash},
		"invalid hex":        {"12345", "not-hex"},
		"wrong-length bytes": {"12345", "aabbcc"},
	} {
		_, err := liveAtPoint(tc[0], tc[1])
		require.Error(t, err, name)
	}
}

func TestLiveNetworkMagic(t *testing.T) {
	t.Parallel()
	preview, err := liveNetworkMagic("preview")
	require.NoError(t, err)
	preprod, err := liveNetworkMagic("preprod")
	require.NoError(t, err)
	assert.NotZero(t, preview)
	assert.NotEqual(t, preview, preprod)
	_, err = liveNetworkMagic("")
	require.Error(t, err)
	_, err = liveNetworkMagic("mainnet-typo")
	require.Error(t, err)
}

func TestResetFallbackTimerNotYetFiredGetsFullInterval(t *testing.T) {
	t.Parallel()
	const interval = 200 * time.Millisecond
	fallback := time.NewTimer(time.Hour)
	t.Cleanup(func() { fallback.Stop() })

	start := time.Now()
	resetFallbackTimer(fallback, interval)
	select {
	case <-fallback.C:
		elapsed := time.Since(start)
		assert.GreaterOrEqual(t, elapsed, interval)
		assert.Less(t, elapsed, interval+2*time.Second)
	case <-time.After(interval + 3*time.Second):
		t.Fatal("fallback timer never fired after being reset")
	}
}

func TestResetFallbackTimerAlreadyFiredSchedulesImmediately(t *testing.T) {
	t.Parallel()
	fallback := time.NewTimer(10 * time.Millisecond)
	t.Cleanup(func() { fallback.Stop() })
	// Stop reports "already fired" reliably only once the fire has been
	// delivered, so drain it first, as a check that overran the interval.
	<-fallback.C

	done := make(chan struct{})
	go func() {
		resetFallbackTimer(fallback, time.Hour)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("resetFallbackTimer blocked draining an already-fired timer")
	}
	select {
	case <-fallback.C:
	case <-time.After(time.Second):
		t.Fatal("an already-fired timer must be rescheduled immediately")
	}
}

func TestSplitStakeMismatches(t *testing.T) {
	t.Parallel()
	real := StakeMismatch{
		PoolIDBech32: "pool1real", DingoStake: 100, KoiosStake: "50",
		DiffLovelace: 50,
	}
	fault := StakeMismatch{
		PoolIDBech32: "pool1fault", DingoStake: 100, KoiosStake: "not-a-number",
		Reason: "unparseable koios active_stake value", KoiosFault: true,
	}
	gotReal, gotFaults := splitStakeMismatches(nil)
	assert.Empty(t, gotReal)
	assert.Empty(t, gotFaults)
	gotReal, gotFaults = splitStakeMismatches([]StakeMismatch{real, fault})
	assert.Equal(t, []StakeMismatch{real}, gotReal)
	assert.Equal(t, []StakeMismatch{fault}, gotFaults)
}

func TestFromGenesisCountersRecordEpoch(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)

	t.Run("clean epoch verifies all three", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(EpochResult{Epoch: 1, UTxOAttempted: true}, logger)
		assert.Equal(t, fromGenesisCounters{
			epochsChecked: 1,
			ppVerified:    1, stakeVerified: 1, utxoVerified: 1,
		}, c)
	})
	t.Run("real stake mismatch is a verified mismatch", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(EpochResult{
			Epoch: 1,
			StakeMismatches: []StakeMismatch{{
				PoolIDBech32: "pool1real", DingoStake: 100, KoiosStake: "50",
				DiffLovelace: 50,
			}},
			UTxOAttempted: true,
		}, logger)
		assert.Equal(t, 1, c.stakeMismatches)
		assert.Equal(t, 0, c.stakeIncomplete)
		assert.Equal(t, 1, c.stakeVerified)
	})
	t.Run("koios fault is incomplete, not a mismatch", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(EpochResult{
			Epoch: 1,
			StakeMismatches: []StakeMismatch{{
				PoolIDBech32: "pool1fault", KoiosStake: "not-a-number",
				KoiosFault: true,
			}},
			UTxOAttempted: true,
		}, logger)
		assert.Equal(t, 0, c.stakeMismatches)
		assert.Equal(t, 1, c.stakeIncomplete)
		assert.Equal(t, 0, c.stakeVerified)
	})
	t.Run("check errors are incomplete", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(EpochResult{
			Epoch:             1,
			ProtocolParamsErr: assert.AnError,
			StakeErr:          assert.AnError,
			UTxOAttempted:     true,
			UTxOErr:           assert.AnError,
		}, logger)
		assert.Equal(t, fromGenesisCounters{
			epochsChecked: 1,
			ppIncomplete:  1, stakeIncomplete: 1, utxoIncomplete: 1,
		}, c)
	})
	t.Run("skipped UTxO is neither incomplete nor verified", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(EpochResult{Epoch: 1, UTxOSkipped: true}, logger)
		assert.Equal(t, 0, c.utxoIncomplete)
		assert.Equal(t, 0, c.utxoVerified)
		assert.NoError(t, c.result())
	})
	t.Run("UTxO never attempted is incomplete", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(EpochResult{Epoch: 1}, logger)
		assert.Equal(t, 1, c.utxoIncomplete)
		assert.Equal(t, 0, c.utxoVerified)
	})
	t.Run("real UTxO mismatch is a verified mismatch", func(t *testing.T) {
		var c fromGenesisCounters
		c.recordEpoch(EpochResult{
			Epoch: 1, UTxOAttempted: true, UTxOMissing: []string{"abc#0"},
		}, logger)
		assert.Equal(t, 1, c.utxoMismatches)
		assert.Equal(t, 1, c.utxoVerified)
	})
}

func TestFromGenesisCountersResult(t *testing.T) {
	t.Parallel()
	var none fromGenesisCounters
	assert.NoError(t, none.result(), "no epoch reached is not a failure here")

	clean := fromGenesisCounters{
		epochsChecked: 3, ppVerified: 3, stakeVerified: 3, utxoVerified: 3,
	}
	assert.NoError(t, clean.result())

	mismatch := clean
	mismatch.utxoMismatches = 1
	require.ErrorContains(t, mismatch.result(), "diverged from Koios")

	// Every mismatch counter is zero here, so without this check a run that
	// verified nothing at all would pass.
	degraded := fromGenesisCounters{
		epochsChecked: 5, ppIncomplete: 5, stakeIncomplete: 5, utxoIncomplete: 5,
	}
	require.ErrorContains(t, degraded.result(), "verified nothing")

	partial := degraded
	partial.ppVerified, partial.ppIncomplete = 1, 4
	assert.NoError(t, partial.result())
}

func TestRequireMatchingKoiosSource(t *testing.T) {
	t.Parallel()
	const network = "preview"
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	newCache := func(t *testing.T) (*koiosparity.Cache, string) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "cache.db")
		cache, err := koiosparity.OpenCache(path, discard)
		require.NoError(t, err)
		t.Cleanup(func() { _ = cache.Close() })
		return cache, path
	}
	newClient := func(t *testing.T, baseURL string) *koiosparity.KoiosClient {
		t.Helper()
		client, err := NewKoiosClient(network, "", baseURL, true, true)
		require.NoError(t, err)
		return client
	}

	t.Run("matching host pins the source", func(t *testing.T) {
		cache, path := newCache(t)
		// Stamped through another handle, so the handle under test claims
		// nothing of its own before the pin.
		other, err := koiosparity.OpenCache(path, discard)
		require.NoError(t, err)
		t.Cleanup(func() { _ = other.Close() })
		client := newClient(t, "http://mirror.example/api/v1")
		_, err = other.RecordKoiosSource(
			network, client.ResolvedBaseURL(), time.Now().UTC(),
		)
		require.NoError(t, err)

		require.NoError(t, requireMatchingKoiosSource(cache, client, network, path))

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

	t.Run("mismatched host is refused, not recorded", func(t *testing.T) {
		cache, path := newCache(t)
		_, err := cache.RecordKoiosSource(
			network, "http://recorded.example/api/v1", time.Now().UTC(),
		)
		require.NoError(t, err)
		err = requireMatchingKoiosSource(
			cache, newClient(t, "http://different.example/api/v1"), network, path,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "http://recorded.example/api/v1")
		assert.Contains(t, err.Error(), "http://different.example/api/v1")
		recorded, ok, err := cache.GetKoiosSource(network)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "http://recorded.example/api/v1", recorded)
	})

	t.Run("unstamped cache is attributed to the public root", func(t *testing.T) {
		cache, path := newCache(t)
		require.Error(t, requireMatchingKoiosSource(
			cache, newClient(t, "http://mirror.example/api/v1"), network, path,
		))
		require.NoError(t, requireMatchingKoiosSource(
			cache, newClient(t, ""), network, path,
		))
	})
}
