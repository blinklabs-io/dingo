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

package conformance

import (
	"testing"
)

// TestRulesConformanceVectors runs the Amaru ledger rules conformance test
// vectors using Dingo's ledger implementation via the shared harness from
// ouroboros-mock/conformance.
//
// The test vectors exercise Conway era ledger rules including:
// - UTxO validation (inputs, outputs, fees, collateral)
// - Certificate processing (stake, pool, DRep, committee)
// - Governance (proposals, voting, enactment)
// - Script execution (native scripts, Plutus V1/V2/V3)
//
// Test vectors are embedded in the ouroboros-mock module and extracted at test
// time. This asserts and reports from a single corpus replay; see
// corpus_test.go for why the replay is memoized per backend and what the
// previous separate statistics pass cost.
func TestRulesConformanceVectors(t *testing.T) {
	results := sqliteCorpusResults(t)
	reportCorpus(t, "sqlite", results)
	assertCorpus(t, "sqlite", results)
}
