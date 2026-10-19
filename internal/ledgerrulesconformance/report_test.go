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
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
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

func TestUnelectedCommitteeVoterIsNotUnknownVoter(t *testing.T) {
	t.Parallel()
	err := errors.New("conway utxo validation rule 60: committee voter is not elected")
	if predicateMatches(err, expectedDoc{Predicate: "VotersDoNotExist"}) {
		t.Fatal("an unelected committee voter matched VotersDoNotExist")
	}
	if !predicateMatches(err, expectedDoc{Predicate: "UnelectedCommitteeVoters"}) {
		t.Fatal("an unelected committee voter did not match UnelectedCommitteeVoters")
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
			"unelected committee voter",
			"rejected: conway utxo validation rule 60: committee voter is not elected",
			"UnelectedCommitteeVoters",
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
