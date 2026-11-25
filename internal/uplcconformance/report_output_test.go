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

package uplcconformance

import (
	"os"
	"strconv"
	"testing"
	"time"
)

// TestWriteConformanceReport writes the UPLC conformance report the release
// workflow publishes. It does nothing unless DINGO_CONFORMANCE_OUTPUT is set,
// and the report is kept even when it records failures.
func TestWriteConformanceReport(t *testing.T) {
	t.Parallel()
	output := os.Getenv("DINGO_CONFORMANCE_OUTPUT")
	if output == "" {
		t.Skip("DINGO_CONFORMANCE_OUTPUT is not set")
	}
	corpus := os.Getenv("DINGO_CONFORMANCE_INPUT")
	if corpus == "" {
		t.Fatal("DINGO_CONFORMANCE_INPUT is required")
	}
	report, err := GenerateReport(corpus, time.Now())
	if err != nil {
		t.Fatalf("generate report: %v", err)
	}
	if err := WriteReport(output, report); err != nil {
		t.Fatalf("write report: %v", err)
	}
	if report.Totals.Failed > 0 {
		t.Errorf(
			"%d failures across %d vectors",
			report.Totals.Failed,
			report.Totals.Total,
		)
	}
}

// TestWriteTurboReport writes the UPLC turbo report the release workflow
// publishes. It does nothing unless DINGO_CONFORMANCE_OUTPUT is set. A
// partial report is written before a benchmark error is returned.
func TestWriteTurboReport(t *testing.T) {
	t.Parallel()
	output := os.Getenv("DINGO_CONFORMANCE_OUTPUT")
	if output == "" {
		t.Skip("DINGO_CONFORMANCE_OUTPUT is not set")
	}
	corpus := os.Getenv("DINGO_CONFORMANCE_INPUT")
	if corpus == "" {
		t.Fatal("DINGO_CONFORMANCE_INPUT is required")
	}
	samples, err := strconv.Atoi(os.Getenv("DINGO_CONFORMANCE_SAMPLES"))
	if err != nil {
		t.Fatalf("DINGO_CONFORMANCE_SAMPLES: %v", err)
	}
	maxPrograms, err := strconv.Atoi(os.Getenv("DINGO_CONFORMANCE_MAX_PROGRAMS"))
	if err != nil {
		t.Fatalf("DINGO_CONFORMANCE_MAX_PROGRAMS: %v", err)
	}
	if err := generateTurboReportFile(t, corpus, output, samples, maxPrograms); err != nil {
		t.Fatalf("generate report: %v", err)
	}
}

func generateTurboReportFile(
	t *testing.T,
	corpusPath, outputPath string,
	samples, maxPrograms int,
) error {
	t.Helper()
	report, err := GenerateTurboReport(
		corpusPath,
		samples,
		maxPrograms,
		time.Now(),
	)
	if report.Scenario != "" {
		if writeErr := WriteTurboReport(outputPath, report); writeErr != nil {
			t.Fatalf("write report: %v", writeErr)
		}
	}
	return err
}
