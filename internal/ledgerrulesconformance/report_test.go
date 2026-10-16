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
