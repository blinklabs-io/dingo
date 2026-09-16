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
	"context"
	"math"
	"net"
	"testing"

	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
)

// TestCheckProtocolParams_CurrentEraErrorFailsTheCheck is the regression a
// human reviewer found (Chris Guiney, dingo#4319): CheckProtocolParams used
// to ignore a GetCurrentEra error and fall back to
// ProtocolParamsFromNative's type-inferred era guess, which cannot tell
// Shelley and Allegra apart. queryHardFork's HardForkCurrentEraQuery case
// now returns errEpochNotResolved for a pinned point no epoch row covers
// (bcddd518) instead of silently answering era 0, so GetCurrentEra can
// newly fail here -- and swallowing that error in an Allegra epoch left
// the wrong guessed era in place, producing a false pparams_era mismatch
// (CompareEpochProtocolParams/DetermineStatus) reported as "ledger state
// diverged from Koios" for what was actually a failed query.
//
// koios is pointed at an unreachable address deliberately: a fixed
// GetCurrentEra error must fail this check before ever reaching Koios, so
// this test does not need a working Koios fake to prove it.
func TestCheckProtocolParams_CurrentEraErrorFailsTheCheck(t *testing.T) {
	t.Parallel()
	const magic = 42

	dingoState := newFakeLSQState()
	// GetCurrentProtocolParams issues a HardForkCurrentEraQuery of its own
	// first (to pick a decode target) -- that first call must still
	// succeed, or CheckProtocolParams fails at that unrelated, earlier
	// step instead of reaching the GetCurrentEra call this test targets.
	dingoState.failCurrentEraQueryAfter(1)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	dingoState.serveLSQOnly(t, listener, magic)

	ctx := context.Background()
	conn, lsq, err := acquireWithRetry(
		ctx, listener.Addr().String(), magic, pcommon.NewPointOrigin(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() }) //nolint:errcheck

	koios, err := NewKoiosClient("preview", "", "http://127.0.0.1:1/api/v1", true)
	require.NoError(t, err)

	mismatches, err := CheckProtocolParams(ctx, lsq.Client, koios, "preview", 1)
	require.Error(t, err,
		"a GetCurrentEra failure must fail the whole check, not silently "+
			"fall back to an ambiguous type-inferred era guess")
	assert.Nil(t, mismatches)
}

func TestNewKoiosClientRejectsMainnet(t *testing.T) {
	if _, err := NewKoiosClient("mainnet", "", "", false); err == nil {
		t.Fatal("expected an error for network \"mainnet\", got nil")
	}
	for _, network := range []string{"preview", "preprod"} {
		if _, err := NewKoiosClient(network, "", "", false); err != nil {
			t.Fatalf("network %q: unexpected error: %v", network, err)
		}
	}
}

// TestStakeDiffLovelaceIsExact proves stakeDiffLovelace has no tolerance at
// all: both GetPoolDistr2's TotalPoolStake and Koios's pool_history
// active_stake are exact integers with nothing to round between two
// independent computations, so even a 1-lovelace difference must be
// reported, not absorbed. Real observed Preview values (100 trillion
// lovelace per pool), not toy numbers, so a comparison that only worked by
// coincidence at small values would still be caught.
func TestStakeDiffLovelaceIsExact(t *testing.T) {
	const dingoStake = 100_000_000_000_000

	diff, kind := stakeDiffLovelace(dingoStake, "100000000000000")
	if kind != stakeDiffOK {
		t.Fatal("rejected a valid decimal lovelace string")
	}
	if diff != 0 {
		t.Fatalf("identical amounts produced a nonzero diff: %d", diff)
	}

	// A 1-lovelace difference is real and exact integers have nothing to
	// round -- it must be reported, not treated as noise.
	diff, kind = stakeDiffLovelace(dingoStake, "100000000000001")
	if kind != stakeDiffOK {
		t.Fatal("rejected a valid decimal lovelace string")
	}
	if diff != -1 {
		t.Fatalf("a real 1-lovelace difference was not reported exactly: got %d, want -1", diff)
	}

	// A real divergence -- Koios reporting a materially different value --
	// must be caught.
	diff, kind = stakeDiffLovelace(dingoStake, "190000000000000")
	if kind != stakeDiffOK {
		t.Fatal("rejected a valid decimal lovelace string")
	}
	if diff != -90_000_000_000_000 {
		t.Fatalf("a real ~90%% stake divergence was not reported exactly: got %d", diff)
	}

	if _, kind := stakeDiffLovelace(dingoStake, "not-a-number"); kind != stakeDiffUnparseableKoios {
		t.Fatalf("expected stakeDiffUnparseableKoios for an unparseable koios value, got %v", kind)
	}

	// Two independently-reported zero-stake pools must compare as an exact
	// match.
	diff, kind = stakeDiffLovelace(0, "0")
	if kind != stakeDiffOK || diff != 0 {
		t.Fatalf("zero vs zero: kind=%v diff=%v, want stakeDiffOK diff=0", kind, diff)
	}
}

// TestStakeDiffLovelaceOverflowIsNotAKoiosFault is the regression a human
// reviewer found (Chris Guiney, dingo#4319): stakeDiffLovelace's IsInt64
// branch fires when both values parse but their difference is too large to
// represent -- reachable not just from a corrupted koios string, but from
// Dingo itself reporting an impossible TotalPoolStake (Cardano's entire max
// supply fits comfortably inside int64's range, so this only happens if
// dingoStake itself is implausible). Conflating this with the
// unparseable-koios-string case labelled it a Koios fault and silently
// dropped it from the mismatch count, hiding a genuine Dingo-side bug as if
// it were unremarkable Koios noise. dingoStake is deliberately set well
// beyond Cardano's real max supply (45 billion ADA / 4.5e16 lovelace) to
// force the overflow while koiosStakeStr itself parses cleanly.
func TestStakeDiffLovelaceOverflowIsNotAKoiosFault(t *testing.T) {
	const implausibleDingoStake = math.MaxUint64

	diff, kind := stakeDiffLovelace(implausibleDingoStake, "0")
	if kind != stakeDiffOverflow {
		t.Fatalf("expected stakeDiffOverflow for a Dingo-side implausible "+
			"stake, got kind=%v diff=%v", kind, diff)
	}
}

// TestEvaluatePoolStake pins CheckStakeDistribution's actual per-pool
// decision -- the StakeMismatch (or nil) it returns for the pool's real
// caller, not just stakeDiffLovelace's own return values in isolation
// (human review, Chris Guiney, dingo#4319: TestStakeDiffLovelaceOverflowIsNotAKoiosFault
// alone doesn't prove this function still builds the right StakeMismatch
// for an overflowing dingo stake -- reverting its stakeDiffOverflow case in
// place to the old "same as unparseable, KoiosFault true" shape would have
// left that test green).
func TestEvaluatePoolStake(t *testing.T) {
	t.Run("no koios row, zero dingo stake: both sides agree, no mismatch", func(t *testing.T) {
		got := evaluatePoolStake("pool1new", 0, nil)
		assert.Nil(t, got)
	})

	t.Run("no koios row, nonzero dingo stake: a real mismatch", func(t *testing.T) {
		got := evaluatePoolStake("pool1missing", 100, nil)
		require.NotNil(t, got)
		assert.False(t, got.KoiosFault)
		assert.Equal(t, "no koios pool_history row for nonzero dingo stake", got.Reason)
	})

	t.Run("unparseable koios value: a koios fault, excluded from mismatch counting by callers", func(t *testing.T) {
		got := evaluatePoolStake("pool1fault", 100, &koiosparity.KoiosPoolHistoryItem{
			ActiveStake: "not-a-number",
		})
		require.NotNil(t, got)
		assert.True(t, got.KoiosFault)
		assert.Equal(t, "unparseable koios active_stake value", got.Reason)
	})

	t.Run("dingo-side overflow: a real mismatch, not a koios fault", func(t *testing.T) {
		got := evaluatePoolStake("pool1overflow", math.MaxUint64, &koiosparity.KoiosPoolHistoryItem{
			ActiveStake: "0",
		})
		require.NotNil(t, got,
			"an implausible dingo stake must still be reported as a mismatch")
		assert.False(t, got.KoiosFault,
			"a dingo-side overflow must not be excluded from the mismatch count as if it were koios noise")
		assert.Equal(
			t,
			"stake difference too large to represent -- dingo's reported stake is implausible",
			got.Reason,
		)
	})

	t.Run("real numeric divergence", func(t *testing.T) {
		got := evaluatePoolStake("pool1diverge", 100, &koiosparity.KoiosPoolHistoryItem{
			ActiveStake: "50",
		})
		require.NotNil(t, got)
		assert.False(t, got.KoiosFault)
		assert.Empty(t, got.Reason)
		assert.Equal(t, int64(50), got.DiffLovelace)
	})

	t.Run("exact match: no mismatch", func(t *testing.T) {
		got := evaluatePoolStake("pool1match", 100, &koiosparity.KoiosPoolHistoryItem{
			ActiveStake: "100",
		})
		assert.Nil(t, got)
	})
}

// TestUTxODiffDetectsRealMismatches proves UTxODiff in all three directions:
// identical sets report no difference, a deliberately injected missing/extra
// ref is caught precisely, and a ref present on both sides with disagreeing
// content is reported as a "differs" entry rather than being missed.
func TestUTxODiffDetectsRealMismatches(t *testing.T) {
	dingo := UTxOSet{
		"4843cf2e582b2f9ce37600e5ab4cc678991f988f8780fed05407f9537f7712bd#0": "addr1abc|1000000",
		"e3ca57e8f323265742a8f4e79ff9af884c9ff8719bd4f7788adaea4c33ba07b6#0": "addr1def|2000000",
	}
	identical := UTxOSet{
		"4843cf2e582b2f9ce37600e5ab4cc678991f988f8780fed05407f9537f7712bd#0": "addr1abc|1000000",
		"e3ca57e8f323265742a8f4e79ff9af884c9ff8719bd4f7788adaea4c33ba07b6#0": "addr1def|2000000",
	}
	if missing, extra, differs := UTxODiff(identical, dingo); len(missing) != 0 || len(extra) != 0 || len(differs) != 0 {
		t.Fatalf("identical sets reported a difference: missing=%v extra=%v differs=%v", missing, extra, differs)
	}

	koiosReconstruction := UTxOSet{
		"4843cf2e582b2f9ce37600e5ab4cc678991f988f8780fed05407f9537f7712bd#0": "addr1abc|9999999",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa#0": "addr1zzz|3000000",
	}
	missing, extra, differs := UTxODiff(koiosReconstruction, dingo)
	if len(missing) != 1 || missing[0] != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa#0" {
		t.Fatalf("expected exactly the injected missing ref, got %v", missing)
	}
	if len(extra) != 1 || extra[0] != "e3ca57e8f323265742a8f4e79ff9af884c9ff8719bd4f7788adaea4c33ba07b6#0" {
		t.Fatalf("expected exactly the injected extra ref, got %v", extra)
	}
	if len(differs) != 1 {
		t.Fatalf("expected exactly one content mismatch, got %v", differs)
	}
}

// TestUTxOChangesAppliesInputsAndOutputs proves the running reconstruction
// correctly adds new outputs (with their full canonical content, not just
// their ref) and removes spent inputs from Koios's own tx_info data.
func TestUTxOChangesAppliesInputsAndOutputs(t *testing.T) {
	set := UTxOSet{
		"spent0000000000000000000000000000000000000000000000000000000000#0": "addr1spent|500000",
	}
	txInfos := []koiosparity.KoiosTxInfoItem{
		{
			TxHash: "newtx000000000000000000000000000000000000000000000000000000000",
			Inputs: []koiosparity.KoiosTxInfoUtxoRef{
				{TxHash: "spent0000000000000000000000000000000000000000000000000000000000", TxIndex: 0},
			},
			Outputs: []koiosparity.KoiosTxInfoOutput{
				{
					TxHash:  "newtx000000000000000000000000000000000000000000000000000000000",
					TxIndex: 0,
					PaymentAddr: struct {
						Bech32 string `json:"bech32"`
					}{Bech32: "addr1new0"},
					Value: "1000000",
				},
				{
					TxHash:  "newtx000000000000000000000000000000000000000000000000000000000",
					TxIndex: 1,
					PaymentAddr: struct {
						Bech32 string `json:"bech32"`
					}{Bech32: "addr1new1"},
					Value: "2000000",
				},
			},
		},
	}
	UTxOChanges(set, txInfos)

	if _, ok := set["spent0000000000000000000000000000000000000000000000000000000000#0"]; ok {
		t.Fatal("spent input was not removed from the set")
	}
	if set["newtx000000000000000000000000000000000000000000000000000000000#0"] != "addr1new0|1000000" ||
		set["newtx000000000000000000000000000000000000000000000000000000000#1"] != "addr1new1|2000000" {
		t.Fatalf("new outputs were not added with correct canonical content: %v", set)
	}
	if len(set) != 2 {
		t.Fatalf("expected exactly 2 live refs after the change, got %d: %v", len(set), set)
	}
}
