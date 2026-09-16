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
	"testing"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
)

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

	diff, ok := stakeDiffLovelace(dingoStake, "100000000000000")
	if !ok {
		t.Fatal("rejected a valid decimal lovelace string")
	}
	if diff != 0 {
		t.Fatalf("identical amounts produced a nonzero diff: %d", diff)
	}

	// A 1-lovelace difference is real and exact integers have nothing to
	// round -- it must be reported, not treated as noise.
	diff, ok = stakeDiffLovelace(dingoStake, "100000000000001")
	if !ok {
		t.Fatal("rejected a valid decimal lovelace string")
	}
	if diff != -1 {
		t.Fatalf("a real 1-lovelace difference was not reported exactly: got %d, want -1", diff)
	}

	// A real divergence -- Koios reporting a materially different value --
	// must be caught.
	diff, ok = stakeDiffLovelace(dingoStake, "190000000000000")
	if !ok {
		t.Fatal("rejected a valid decimal lovelace string")
	}
	if diff != -90_000_000_000_000 {
		t.Fatalf("a real ~90%% stake divergence was not reported exactly: got %d", diff)
	}

	if _, ok := stakeDiffLovelace(dingoStake, "not-a-number"); ok {
		t.Fatal("accepted an unparseable koios value")
	}

	// Two independently-reported zero-stake pools must compare as an exact
	// match.
	diff, ok = stakeDiffLovelace(0, "0")
	if !ok || diff != 0 {
		t.Fatalf("zero vs zero: ok=%v diff=%v, want ok=true diff=0", ok, diff)
	}
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
