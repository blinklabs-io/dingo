// Copyright 2026 Blink Labs Software

package cborconformance

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerateReport(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	corpus := filepath.Join(root, "conway-123-100")
	validDir := filepath.Join(corpus, "credential", "valid")
	expectedDir := filepath.Join(corpus, "credential", "expected")
	zapZeroDir := filepath.Join(corpus, "credential", "invalid", "zap-0")
	zapOneDir := filepath.Join(corpus, "credential", "invalid", "zap-1")
	for _, dir := range []string{validDir, expectedDir, zapZeroDir, zapOneDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	valid, err := hex.DecodeString(
		"8200581c00000000000000000000000000000000000000000000000000000000",
	)
	if err != nil {
		t.Fatal(err)
	}
	write := func(path string, data []byte) {
		t.Helper()
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(validDir, "valid.cbor"), valid)
	write(filepath.Join(expectedDir, "valid.cbor"), valid)
	write(filepath.Join(zapZeroDir, "invalid.cbor"), []byte{0xf6})
	write(filepath.Join(zapOneDir, "mutation.cbor"), []byte{0x80})

	report, err := GenerateReport(corpus)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Successful {
		t.Fatalf("report has failures: %+v", report.Failures)
	}
	want := Outcome{
		GeneratedTotal:                    2,
		GeneratedDecodedReencodedExpected: 1,
		GeneratedDecodedReencodedActual:   1,
		GeneratedMustBeRejectedExpected:   1,
		GeneratedMustBeRejectedActual:     1,
		ZapMustBeRejectedExpected:         1,
		ZapMustBeRejectedActual:           1,
	}
	if report.Totals != want {
		t.Fatalf("totals = %+v, want %+v", report.Totals, want)
	}
}

func TestGenerateReportPrimaryDatasetLayout(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	corpus := filepath.Join(root, "conway")
	validDir := filepath.Join(corpus, "credential", "valid")
	zapZeroDir := filepath.Join(corpus, "credential", "invalid-zap-0")
	zapOneDir := filepath.Join(corpus, "credential", "invalid-zap-1")
	for _, dir := range []string{validDir, zapZeroDir, zapOneDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	valid, err := hex.DecodeString(
		"8200581c00000000000000000000000000000000000000000000000000000000",
	)
	if err != nil {
		t.Fatal(err)
	}
	write := func(path string, data []byte) {
		t.Helper()
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(validDir, "00001-valid.input.cbor"), valid)
	write(filepath.Join(validDir, "00001-valid.expected.cbor"), valid)
	write(filepath.Join(zapZeroDir, "00001-invalid.input.cbor"), []byte{0xf6})
	write(filepath.Join(zapOneDir, "00001-mutation.input.cbor"), []byte{0x80})

	report, err := GenerateReport(corpus)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Successful {
		t.Fatalf("report has failures: %+v", report.Failures)
	}
	want := Outcome{
		GeneratedTotal:                    2,
		GeneratedDecodedReencodedExpected: 1,
		GeneratedDecodedReencodedActual:   1,
		GeneratedMustBeRejectedExpected:   1,
		GeneratedMustBeRejectedActual:     1,
		ZapMustBeRejectedExpected:         1,
		ZapMustBeRejectedActual:           1,
	}
	if report.Totals != want {
		t.Fatalf("totals = %+v, want %+v", report.Totals, want)
	}
}

func TestGenerateReportRejectsOrphanExpectedOutput(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	corpus := filepath.Join(root, "conway")
	validDir := filepath.Join(corpus, "credential", "valid")
	if err := os.MkdirAll(validDir, 0o755); err != nil {
		t.Fatal(err)
	}
	valid, err := hex.DecodeString(
		"8200581c00000000000000000000000000000000000000000000000000000000",
	)
	if err != nil {
		t.Fatal(err)
	}
	for filename, data := range map[string][]byte{
		"paired.input.cbor":    valid,
		"paired.expected.cbor": valid,
		"orphan.expected.cbor": valid,
	} {
		if err := os.WriteFile(filepath.Join(validDir, filename), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	_, err = GenerateReport(corpus)
	if err == nil || !strings.Contains(err.Error(), "orphan.expected.cbor") {
		t.Fatalf(
			"GenerateReport error = %v, want orphan expected output error",
			err,
		)
	}
}

func TestGenerateReportClassifiesAcceptedMutation(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	corpus := filepath.Join(root, "conway-123-100")
	mutationDir := filepath.Join(corpus, "credential", "invalid", "zap-1")
	if err := os.MkdirAll(mutationDir, 0o755); err != nil {
		t.Fatal(err)
	}
	valid, err := hex.DecodeString(
		"8200581c00000000000000000000000000000000000000000000000000000000",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mutationDir, "accepted.cbor"), valid, 0o644); err != nil {
		t.Fatal(err)
	}

	report, err := GenerateReport(corpus)
	if err != nil {
		t.Fatal(err)
	}
	if report.Successful {
		t.Fatal("report marked an accepted invalid sample successful")
	}
	if len(report.Failures) != 1 {
		t.Fatalf("failure count = %d, want 1", len(report.Failures))
	}
	failure := report.Failures[0]
	if failure.Class != decodeSucceeded {
		t.Fatalf("failure class = %q, want %q", failure.Class, decodeSucceeded)
	}
	if failure.Sample != "credential/invalid/zap-1/accepted" {
		t.Fatalf("failure sample = %q", failure.Sample)
	}
}
