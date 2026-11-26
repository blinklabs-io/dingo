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
	"os"
	"regexp"
	"testing"
	"time"
)

var datasetCommitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

// TestWriteConformanceReport writes the ledger-rules report the release
// workflow publishes. It does nothing unless DINGO_CONFORMANCE_OUTPUT is set,
// and the report is kept even when it records failures. The report names the
// Amaru commit of the corpus it ran against in DINGO_CONFORMANCE_REVISION.
func TestWriteConformanceReport(t *testing.T) {
	t.Parallel()
	output := os.Getenv("DINGO_CONFORMANCE_OUTPUT")
	if output == "" {
		t.Skip("DINGO_CONFORMANCE_OUTPUT is not set")
	}
	fixtures := os.Getenv("DINGO_CONFORMANCE_INPUT")
	if fixtures == "" {
		t.Fatal("DINGO_CONFORMANCE_INPUT is required")
	}
	revision := os.Getenv("DINGO_CONFORMANCE_REVISION")
	if !datasetCommitPattern.MatchString(revision) {
		t.Fatalf(
			"DINGO_CONFORMANCE_REVISION must be the 40-character Amaru commit "+
				"that last changed the scenario corpus, got %q",
			revision,
		)
	}
	report, failures, err := GenerateReport(fixtures, time.Now())
	if err != nil {
		t.Fatalf("generate report: %v", err)
	}
	report.DatasetCommit = revision
	if err := WriteReport(output, report); err != nil {
		t.Fatalf("write report: %v", err)
	}
	for _, failure := range failures {
		t.Errorf(
			"fixture %s: expected %s; observed %s",
			failure.ID,
			failure.Expected,
			failure.Observed,
		)
	}
}
