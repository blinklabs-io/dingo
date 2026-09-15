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

// TestStakeRelDiffDetectsRealMismatches proves stakeRelDiff both directions:
// it must not flag identical or negligibly-different amounts, and it must
// flag a real divergence. Real observed Preview values (100 trillion
// lovelace per pool), not toy numbers, so a comparison that only worked by
// coincidence at small values would still be caught.
func TestStakeRelDiffDetectsRealMismatches(t *testing.T) {
	const dingoStake = 100_000_000_000_000

	relDiff, ok := stakeRelDiff(dingoStake, "100000000000000")
	if !ok {
		t.Fatal("rejected a valid decimal lovelace string")
	}
	if relDiff > stakeRelativeTolerance {
		t.Fatalf("identical amounts produced relDiff=%v > tolerance %v", relDiff, stakeRelativeTolerance)
	}

	// A genuinely negligible 1-lovelace difference must not false-positive.
	relDiff, ok = stakeRelDiff(dingoStake, "100000000000001")
	if !ok {
		t.Fatal("rejected a valid decimal lovelace string")
	}
	if relDiff > stakeRelativeTolerance {
		t.Fatalf("a negligible 1-lovelace difference (relDiff=%v) exceeded tolerance %v", relDiff, stakeRelativeTolerance)
	}

	// A real divergence -- Koios reporting a materially different value --
	// must be caught.
	relDiff, ok = stakeRelDiff(dingoStake, "190000000000000")
	if !ok {
		t.Fatal("rejected a valid decimal lovelace string")
	}
	if relDiff <= stakeRelativeTolerance {
		t.Fatalf("a real ~90%% stake divergence was not detected: relDiff=%v", relDiff)
	}

	if _, ok := stakeRelDiff(dingoStake, "not-a-number"); ok {
		t.Fatal("accepted an unparseable koios value")
	}

	// Two independently-reported zero-stake pools must compare as an exact
	// match, not an undefined 0/0.
	relDiff, ok = stakeRelDiff(0, "0")
	if !ok || relDiff != 0 {
		t.Fatalf("zero vs zero: ok=%v relDiff=%v, want ok=true relDiff=0", ok, relDiff)
	}
}

// TestUTxORefDiffDetectsRealMismatches proves UTxORefDiff both directions:
// identical sets report no difference, and a deliberately injected
// missing/extra ref is caught precisely.
func TestUTxORefDiffDetectsRealMismatches(t *testing.T) {
	dingo := UTxORefSet{
		"4843cf2e582b2f9ce37600e5ab4cc678991f988f8780fed05407f9537f7712bd#0": true,
		"e3ca57e8f323265742a8f4e79ff9af884c9ff8719bd4f7788adaea4c33ba07b6#0": true,
	}
	identical := UTxORefSet{
		"4843cf2e582b2f9ce37600e5ab4cc678991f988f8780fed05407f9537f7712bd#0": true,
		"e3ca57e8f323265742a8f4e79ff9af884c9ff8719bd4f7788adaea4c33ba07b6#0": true,
	}
	if missing, extra := UTxORefDiff(identical, dingo); len(missing) != 0 || len(extra) != 0 {
		t.Fatalf("identical sets reported a difference: missing=%v extra=%v", missing, extra)
	}

	koiosReconstruction := UTxORefSet{
		"4843cf2e582b2f9ce37600e5ab4cc678991f988f8780fed05407f9537f7712bd#0": true,
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa#0": true,
	}
	missing, extra := UTxORefDiff(koiosReconstruction, dingo)
	if len(missing) != 1 || missing[0] != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa#0" {
		t.Fatalf("expected exactly the injected missing ref, got %v", missing)
	}
	if len(extra) != 1 || extra[0] != "e3ca57e8f323265742a8f4e79ff9af884c9ff8719bd4f7788adaea4c33ba07b6#0" {
		t.Fatalf("expected exactly the injected extra ref, got %v", extra)
	}
}

// TestUTxOChangesAppliesInputsAndOutputs proves the running reconstruction
// correctly adds new outputs and removes spent inputs from Koios's own
// tx_info data.
func TestUTxOChangesAppliesInputsAndOutputs(t *testing.T) {
	refs := UTxORefSet{
		"spent0000000000000000000000000000000000000000000000000000000000#0": true,
	}
	txInfos := []koiosparity.KoiosTxInfoItem{
		{
			TxHash: "newtx000000000000000000000000000000000000000000000000000000000",
			Inputs: []koiosparity.KoiosTxInfoUtxoRef{
				{TxHash: "spent0000000000000000000000000000000000000000000000000000000000", TxIndex: 0},
			},
			Outputs: []koiosparity.KoiosTxInfoUtxoRef{
				{TxHash: "newtx000000000000000000000000000000000000000000000000000000000", TxIndex: 0},
				{TxHash: "newtx000000000000000000000000000000000000000000000000000000000", TxIndex: 1},
			},
		},
	}
	UTxOChanges(refs, txInfos)

	if refs["spent0000000000000000000000000000000000000000000000000000000000#0"] {
		t.Fatal("spent input was not removed from the ref set")
	}
	if !refs["newtx000000000000000000000000000000000000000000000000000000000#0"] ||
		!refs["newtx000000000000000000000000000000000000000000000000000000000#1"] {
		t.Fatal("new outputs were not added to the ref set")
	}
	if len(refs) != 2 {
		t.Fatalf("expected exactly 2 live refs after the change, got %d: %v", len(refs), refs)
	}
}
