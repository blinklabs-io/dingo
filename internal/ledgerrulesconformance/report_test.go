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

package ledgerrulesconformance

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
)

func TestOutsideForecastMatchesExpectedMessage(t *testing.T) {
	t.Parallel()
	for _, message := range []string{
		"outside forecast",
		"OutsideForecast (Slot 500000)",
	} {
		t.Run(message, func(t *testing.T) {
			t.Parallel()
			if !predicateMatches(
				errors.New(message),
				expectedDoc{Predicate: "OutsideForecast"},
			) {
				t.Fatalf("OutsideForecast did not match %q", message)
			}
		})
	}
}

func TestVotersDoNotExistMatchesUnelectedCommitteeVoter(t *testing.T) {
	t.Parallel()
	if !predicateMatches(
		errors.New("conway utxo validation rule 60: committee voter is not elected"),
		expectedDoc{Predicate: "VotersDoNotExist"},
	) {
		t.Fatal("VotersDoNotExist did not match an unelected committee voter")
	}
}

func TestResolveReferencedObjectAppliesSiblingOverrides(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "common.json"), []byte(`{"slot":1,"epoch":2}`), 0o600); err != nil {
		t.Fatal(err)
	}
	resolved, err := resolveReferencedObject(
		root,
		json.RawMessage(`{"$ref":"common.json","$override":{"epoch":3}}`),
	)
	if err != nil {
		t.Fatalf("resolve referenced object: %v", err)
	}
	if string(resolved["slot"]) != "1" || string(resolved["epoch"]) != "3" {
		t.Fatalf(
			"resolved object = %s, want slot 1 and epoch 3",
			mustJSON(t, resolved),
		)
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal JSON: %v", err)
	}
	return string(encoded)
}

// A conformance transaction body carrying a reference script; the override
// guard only inspects IsValid() and the supplied validation error.
const referenceScriptOverrideTxHex = "84A500D9010281825820A1C4425C08922D444E4FAD7AD6E73796AC118E27165F0688AC049858938ED028000181BF00581D60A1C146C212ACFE02048AFBF8D2E5E9C806165934D9935C696F85C3DC011A004C4B40FF021A00E4E1C00F0012D9010281825820111111111111111111111111111111111111111111111111111111111111111100A101D9010281820180F5F6"

func TestApplyReferenceScriptSizeOverridePreservesSiblingErrors(t *testing.T) {
	t.Parallel()
	raw, err := hex.DecodeString(referenceScriptOverrideTxHex)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := conway.NewConwayTransactionFromCbor(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !tx.IsValid() {
		t.Fatal("fixture transaction should be valid")
	}
	siblingErr := errors.New("fee too small")

	t.Run("override limit above transaction size", func(t *testing.T) {
		t.Parallel()
		joined := errors.Join(
			common.RefScriptSizePerTxTooLargeError{
				TxSize:  300_000,
				MaxSize: conway.MaxRefScriptSizePerTx,
			},
			siblingErr,
		)
		limit := uint64(400_000)
		got := applyReferenceScriptSizeOverride(tx, nil, &limit, joined)
		if got == nil {
			t.Fatalf(
				"override accepted a transaction whose validation also failed: %v",
				joined,
			)
		}
		if !errors.Is(got, siblingErr) {
			t.Fatalf("override dropped the sibling validation error: %v", got)
		}
	})

	t.Run("override limit below transaction size", func(t *testing.T) {
		t.Parallel()
		joined := errors.Join(
			common.RefScriptSizePerTxTooLargeError{
				TxSize:  60,
				MaxSize: conway.MaxRefScriptSizePerTx,
			},
			siblingErr,
		)
		limit := uint64(50)
		got := applyReferenceScriptSizeOverride(tx, nil, &limit, joined)
		if got == nil {
			t.Fatal("override should reject over-limit reference scripts")
		}
		if !errors.Is(got, siblingErr) {
			t.Fatalf("override dropped the sibling validation error: %v", got)
		}
	})
}

func TestObservedLedgerResult(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		observed string
		want     string
		ok       bool
	}{
		{"accepted", "transaction accepted", "pass", true},
		{"decoded", "transaction decoded successfully", "pass", true},
		{
			"decoding failure",
			"transaction decoding failure: bad cbor",
			"DecodingFailure",
			true,
		},
		{
			"mapped predicate",
			"rejected: output too small",
			"BabbageOutputTooSmallUTxO",
			true,
		},
		{
			"unelected committee voter renamed",
			"rejected: conway utxo validation rule 60: committee voter is not elected",
			"VotersDoNotExist",
			true,
		},
		{"unmapped rejection", "rejected: something novel", "", false},
		{"unknown observation", "weird", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := observedLedgerResult(tc.observed)
			if got != tc.want || ok != tc.ok {
				t.Fatalf(
					"observedLedgerResult(%q) = %q, %v; want %q, %v",
					tc.observed, got, ok, tc.want, tc.ok,
				)
			}
		})
	}
}

func TestMismatchReportsUnmappedRejectionWithoutAborting(t *testing.T) {
	t.Parallel()
	outcome, failure, err := mismatch(
		"00001",
		expectedDoc{Predicate: "BadInputsUTxO"},
		"rejected: something novel",
	)
	if err != nil {
		t.Fatalf("mismatch returned error for unmapped rejection: %v", err)
	}
	if outcome.ID != "00001" || outcome.Result != unmappedLedgerResult {
		t.Fatalf("unexpected outcome: %+v", outcome)
	}
	if failure == nil || failure.Observed != "rejected: something novel" ||
		failure.Expected != "BadInputsUTxO" {
		t.Fatalf("unexpected failure: %+v", failure)
	}
}

func TestReportJSONFields(t *testing.T) {
	t.Parallel()
	data, err := json.Marshal(Report{
		SchemaVersion:  schemaVersion,
		Implementation: map[string]string{"name": "dingo"},
		Scenarios:      []CaseOutcome{{ID: "00001", Result: "pass"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got["version"] != schemaVersion {
		t.Fatalf("version = %v", got["version"])
	}
	impl, _ := got["implementation"].(map[string]any)
	if impl["name"] != "dingo" {
		t.Fatalf("implementation = %v", got["implementation"])
	}
}
