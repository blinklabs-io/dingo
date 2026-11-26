// Copyright 2026 Blink Labs Software

package cborconformance

import (
	"os"
	"testing"
)

// TestWriteConformanceReport writes the CBOR decoding report the release
// workflow publishes. It does nothing unless DINGO_CONFORMANCE_OUTPUT is set,
// and the report is kept even when it records failures.
func TestWriteConformanceReport(t *testing.T) {
	t.Parallel()
	output := os.Getenv("DINGO_CONFORMANCE_OUTPUT")
	if output == "" {
		t.Skip("DINGO_CONFORMANCE_OUTPUT is not set")
	}
	dataset := os.Getenv("DINGO_CONFORMANCE_INPUT")
	revision := os.Getenv("DINGO_CONFORMANCE_REVISION")
	if dataset == "" || revision == "" {
		t.Fatal("DINGO_CONFORMANCE_INPUT and DINGO_CONFORMANCE_REVISION are required")
	}
	report, err := GenerateReport(dataset, revision)
	if err != nil {
		t.Fatalf("generate report: %v", err)
	}
	if err := WriteReport(output, report); err != nil {
		t.Fatalf("write report: %v", err)
	}
	if !report.Successful {
		t.Errorf(
			"%d failures across %d generated samples",
			len(report.Failures),
			report.Totals.GeneratedTotal,
		)
	}
}
