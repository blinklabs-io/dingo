// Copyright 2026 Blink Labs Software

package cborconformance

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testCorpusRevision = "0123456789abcdef0123456789abcdef01234567"

func makeCompleteCorpus(t *testing.T, corpus string) {
	t.Helper()
	if err := os.MkdirAll(corpus, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(corpus, "corpus.json"), []byte(`{"seed":123}`), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, rule := range ruleNames() {
		dir := filepath.Join(corpus, rule, "invalid-zap-1")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(
			filepath.Join(dir, "complete.input.cbor"),
			[]byte{0xff},
			0o644,
		); err != nil {
			t.Fatal(err)
		}
	}
}

func TestGenerateReport(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	corpus := filepath.Join(root, "conway-123-100")
	makeCompleteCorpus(t, corpus)
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

	report, err := generateReport(corpus, testCorpusRevision, "")
	if err != nil {
		t.Fatal(err)
	}
	if !report.Successful {
		t.Fatalf("report has failures: %+v", report.Failures)
	}
	if report.CorpusRevision != testCorpusRevision {
		t.Fatalf(
			"corpus revision = %q, want %q",
			report.CorpusRevision,
			testCorpusRevision,
		)
	}
	want := Outcome{
		GeneratedTotal:                    2,
		GeneratedDecodedReencodedExpected: 1,
		GeneratedDecodedReencodedActual:   1,
		GeneratedMustBeRejectedExpected:   1,
		GeneratedMustBeRejectedActual:     1,
		ZapMustBeRejectedExpected:         len(ruleNames()) + 1,
		ZapMustBeRejectedActual:           len(ruleNames()) + 1,
	}
	if report.Totals != want {
		t.Fatalf("totals = %+v, want %+v", report.Totals, want)
	}
}

func TestGenerateReportAcceptsByteExactRules(t *testing.T) {
	t.Parallel()
	corpus := filepath.Join(t.TempDir(), "conway")
	validDir := filepath.Join(corpus, "native_script", "valid")
	if err := os.MkdirAll(validDir, 0o755); err != nil {
		t.Fatal(err)
	}
	valid, err := hex.DecodeString(
		"8200581c00000000000000000000000000000000000000000000000000000000",
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"input", "expected"} {
		if err := os.WriteFile(
			filepath.Join(validDir, "valid."+suffix+".cbor"),
			valid,
			0o644,
		); err != nil {
			t.Fatal(err)
		}
	}

	report, err := generateReport(corpus, testCorpusRevision, "")
	if err != nil {
		t.Fatal(err)
	}
	if !report.Successful {
		t.Fatalf("report has failures: %+v", report.Failures)
	}
	if report.Totals.GeneratedDecodedReencodedActual != 1 {
		t.Fatalf("exact re-encoding count = %d, want 1", report.Totals.GeneratedDecodedReencodedActual)
	}
}

func TestGenerateReportComparesByteExactReferenceWithoutNormalization(t *testing.T) {
	t.Parallel()
	corpus := filepath.Join(t.TempDir(), "conway")
	validDir := filepath.Join(corpus, "native_script", "valid")
	if err := os.MkdirAll(validDir, 0o755); err != nil {
		t.Fatal(err)
	}
	input := append([]byte{0x9f, 0x00, 0x58, 0x1c}, make([]byte, 28)...)
	input = append(input, 0xff)
	expected := append([]byte{0x82, 0x00, 0x58, 0x1c}, make([]byte, 28)...)
	if err := os.WriteFile(
		filepath.Join(validDir, "valid.input.cbor"),
		input,
		0o644,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(validDir, "valid.expected.cbor"),
		expected,
		0o644,
	); err != nil {
		t.Fatal(err)
	}

	report, err := generateReport(corpus, testCorpusRevision, "")
	if err != nil {
		t.Fatal(err)
	}
	if report.Successful {
		t.Fatal("report succeeded after normalizing a byte-exact reference")
	}
	if report.Totals.GeneratedDecodedReencodedActual != 0 {
		t.Fatalf("byte-exact reference count = %d, want 0", report.Totals.GeneratedDecodedReencodedActual)
	}
	if len(report.Failures) != 1 {
		t.Fatalf("failure count = %d, want 1: %+v", len(report.Failures), report.Failures)
	}
	if got := report.Failures[0].Class; got != referenceMismatch {
		t.Fatalf("failure class = %q, want %q", got, referenceMismatch)
	}
	if got := report.Failures[0].Reason; got != "byte-exact reserialization differs from the expected output" {
		t.Fatalf("failure reason = %q", got)
	}
}

func TestGenerateReportAcceptsMatchingRawByteExactReference(t *testing.T) {
	t.Parallel()
	corpus := filepath.Join(t.TempDir(), "conway")
	validDir := filepath.Join(corpus, "native_script", "valid")
	if err := os.MkdirAll(validDir, 0o755); err != nil {
		t.Fatal(err)
	}
	input := append([]byte{0x9f, 0x00, 0x58, 0x1c}, make([]byte, 28)...)
	input = append(input, 0xff)
	for _, suffix := range []string{"input", "expected"} {
		if err := os.WriteFile(
			filepath.Join(validDir, "valid."+suffix+".cbor"),
			input,
			0o644,
		); err != nil {
			t.Fatal(err)
		}
	}

	report, err := generateReport(corpus, testCorpusRevision, "")
	if err != nil {
		t.Fatal(err)
	}
	if !report.Successful {
		t.Fatalf("report has failures: %+v", report.Failures)
	}
	if report.Totals.GeneratedDecodedReencodedActual != 1 {
		t.Fatalf("raw byte-exact reference count = %d, want 1", report.Totals.GeneratedDecodedReencodedActual)
	}
}

func TestGenerateReportNormalizesNonByteExactReference(t *testing.T) {
	t.Parallel()
	corpus := filepath.Join(t.TempDir(), "conway")
	validDir := filepath.Join(corpus, "credential", "valid")
	if err := os.MkdirAll(validDir, 0o755); err != nil {
		t.Fatal(err)
	}
	canonical := append([]byte{0x82, 0x00, 0x58, 0x1c}, make([]byte, 28)...)
	indefinite := append([]byte{0x9f, 0x00, 0x58, 0x1c}, make([]byte, 28)...)
	indefinite = append(indefinite, 0xff)
	if err := os.WriteFile(
		filepath.Join(validDir, "valid.input.cbor"),
		canonical,
		0o644,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(validDir, "valid.expected.cbor"),
		indefinite,
		0o644,
	); err != nil {
		t.Fatal(err)
	}

	report, err := generateReport(corpus, testCorpusRevision, "")
	if err != nil {
		t.Fatal(err)
	}
	if !report.Successful {
		t.Fatalf("report has failures: %+v", report.Failures)
	}
	if report.Totals.GeneratedDecodedReencodedActual != 1 {
		t.Fatalf("normalized reference count = %d, want 1", report.Totals.GeneratedDecodedReencodedActual)
	}
}

func TestGenerateReportRejectsIncompleteCorpusInventory(t *testing.T) {
	t.Parallel()
	for _, mutation := range []string{"missing sample", "altered sample"} {
		t.Run(mutation, func(t *testing.T) {
			t.Parallel()
			corpus := filepath.Join(t.TempDir(), "conway")
			makeCompleteCorpus(t, corpus)
			expectedInventory, err := corpusInventorySHA256(os.DirFS(corpus))
			if err != nil {
				t.Fatal(err)
			}
			samplePath := filepath.Join(corpus, "credential", "invalid-zap-1", "complete.input.cbor")
			if mutation == "missing sample" {
				err = os.Remove(samplePath)
			} else {
				err = os.WriteFile(samplePath, []byte{0x01}, 0o644)
			}
			if err != nil {
				t.Fatal(err)
			}
			_, err = generateReport(corpus, testCorpusRevision, expectedInventory)
			if err == nil || !strings.Contains(err.Error(), "inventory mismatch") {
				t.Fatalf("generateReport error = %v, want inventory mismatch", err)
			}
		})
	}
}

func TestGenerateReportAppliesExplicitSampleWaivers(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	corpus := filepath.Join(root, "conway")
	makeCompleteCorpus(t, corpus)
	metadata := `{"waivers":[` +
		`{"rule":"credential","kind":"verified-by-parent","reason":"checked by parent rule","samples":["credential/invalid/zap-1/complete"]},` +
		`{"rule":"credential","kind":"verified-by-parent","reason":"checked by parent rule","samples":["credential/invalid/zap-1/complete"]}` +
		`]}`
	if err := os.WriteFile(filepath.Join(corpus, "corpus.json"), []byte(metadata), 0o644); err != nil {
		t.Fatal(err)
	}

	report, err := generateReport(corpus, testCorpusRevision, "")
	if err != nil {
		t.Fatal(err)
	}
	if !report.Successful {
		t.Fatalf("report has failures: %+v", report.Failures)
	}
	if len(report.WaivedSamples) != 1 {
		t.Fatalf("waived samples = %+v, want one deduplicated sample", report.WaivedSamples)
	}
	waived := report.WaivedSamples[0]
	if waived.Sample != "credential/invalid/zap-1/complete" ||
		waived.Rule != "credential" ||
		waived.Kind != "verified-by-parent" ||
		waived.Reason != "checked by parent rule" {
		t.Fatalf("waived sample metadata = %+v", waived)
	}
	wantZapCount := len(ruleNames()) - 1
	if report.Totals.ZapMustBeRejectedExpected != wantZapCount ||
		report.Totals.ZapMustBeRejectedActual != wantZapCount {
		t.Fatalf("zap totals = %+v, want %d expected and actual", report.Totals, wantZapCount)
	}
}

func TestGenerateReportRejectsInvalidSampleWaivers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		metadata string
		wantErr  string
	}{
		{
			name:     "unknown sample",
			metadata: `{"waivers":[{"rule":"credential","kind":"verified-by-parent","reason":"reason","samples":["credential/manual-invalid/missing"]}]}`,
			wantErr:  "references unknown sample",
		},
		{
			name:     "wrong rule",
			metadata: `{"waivers":[{"rule":"address","kind":"verified-by-parent","reason":"reason","samples":["credential/invalid/zap-1/complete"]}]}`,
			wantErr:  "names rule",
		},
		{
			name:     "missing reason",
			metadata: `{"waivers":[{"rule":"credential","kind":"verified-by-parent","samples":["credential/invalid/zap-1/complete"]}]}`,
			wantErr:  "must include rule, kind, and reason",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			corpus := filepath.Join(root, "conway")
			makeCompleteCorpus(t, corpus)
			if err := os.WriteFile(filepath.Join(corpus, "corpus.json"), []byte(test.metadata), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := generateReport(corpus, testCorpusRevision, ""); err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("GenerateReport error = %v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestReencodeAcceptsIndefiniteCostModelMap(t *testing.T) {
	t.Parallel()
	if _, err := reencode("cost_models", []byte{0xbf, 0x03, 0x80, 0xff}); err != nil {
		t.Fatalf("reencode indefinite-length cost-model map: %v", err)
	}
}

func TestReencodeRejectsIndefiniteBoundedBytes(t *testing.T) {
	t.Parallel()
	if _, err := reencode("asset_name", []byte{0x5f, 0x41, 'a', 0xff}); err == nil {
		t.Fatal("accepted an indefinite byte string for a bounded asset name")
	}
}

func TestValidateConwayCostModelLanguageIDs(t *testing.T) {
	t.Parallel()
	for _, language := range []uint{0, 1, 2, 3, 255} {
		if err := validateConwayCostModelLanguageIDs(map[uint][]int64{language: {}}); err != nil {
			t.Errorf("language ID %d: %v", language, err)
		}
	}
	if err := validateConwayCostModelLanguageIDs(map[uint][]int64{256: {}}); err == nil {
		t.Error("language ID 256 was accepted, want uint8 range enforcement")
	}
	if _, err := reencode("cost_models", []byte{0xa1, 0x00, 0xf6}); err == nil {
		t.Error("cost model with a null parameter list was accepted")
	}
}

func TestGenerateReportPrimaryDatasetLayout(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	corpus := filepath.Join(root, "conway")
	makeCompleteCorpus(t, corpus)
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

	report, err := generateReport(corpus, testCorpusRevision, "")
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
		ZapMustBeRejectedExpected:         len(ruleNames()) + 1,
		ZapMustBeRejectedActual:           len(ruleNames()) + 1,
	}
	if report.Totals != want {
		t.Fatalf("totals = %+v, want %+v", report.Totals, want)
	}
}

func TestGenerateReportRejectsOrphanExpectedOutput(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	corpus := filepath.Join(root, "conway")
	makeCompleteCorpus(t, corpus)
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

	_, err = generateReport(corpus, testCorpusRevision, "")
	if err == nil || !strings.Contains(err.Error(), "orphan.expected.cbor") {
		t.Fatalf(
			"GenerateReport error = %v, want orphan expected output error",
			err,
		)
	}
}

func TestGenerateReportAcceptsManualSamplesAndEmitsScenarioSchema(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	corpus := filepath.Join(root, "conway")
	makeCompleteCorpus(t, corpus)
	manualValidDir := filepath.Join(corpus, "credential", "manual-valid")
	manualInvalidDir := filepath.Join(corpus, "credential", "manual-invalid")
	for _, dir := range []string{manualValidDir, manualInvalidDir} {
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
	for _, suffix := range []string{"input", "expected"} {
		if err := os.WriteFile(filepath.Join(manualValidDir, "only."+suffix+".cbor"), valid, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(manualInvalidDir, "invalid.input.cbor"), []byte{0xf6}, 0o644); err != nil {
		t.Fatal(err)
	}

	report, err := generateReport(corpus, testCorpusRevision, "")
	if err != nil {
		t.Fatal(err)
	}
	if !report.Successful {
		t.Fatalf("manual sample report has failures: %+v", report.Failures)
	}
	want := Outcome{
		GeneratedTotal:                    2,
		GeneratedDecodedReencodedExpected: 1,
		GeneratedDecodedReencodedActual:   1,
		GeneratedMustBeRejectedExpected:   1,
		GeneratedMustBeRejectedActual:     1,
		ZapMustBeRejectedExpected:         len(ruleNames()),
		ZapMustBeRejectedActual:           len(ruleNames()),
	}
	if report.Totals != want {
		t.Fatalf("totals = %+v, want %+v", report.Totals, want)
	}
	if report.SchemaVersion != schemaVersion || report.Scenario != scenario || report.GeneratedAt.IsZero() {
		t.Fatalf("report schema metadata is incomplete: %+v", report)
	}
	if report.GeneratedAt.Location() != time.UTC {
		t.Fatalf("generated_at location = %s, want UTC", report.GeneratedAt.Location())
	}
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"version", "scenario", "generated_at", "corpus", "protocol_version", "totals", "rules"} {
		if _, ok := fields[field]; !ok {
			t.Errorf("serialized report has no required field %q", field)
		}
	}
}

func TestGenerateReportRejectsUnknownConwayRule(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	corpus := filepath.Join(root, "conway")
	makeCompleteCorpus(t, corpus)
	unknownSample := filepath.Join(corpus, "unknown_rule", "invalid-zap-1", "input.input.cbor")
	if err := os.MkdirAll(filepath.Dir(unknownSample), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(unknownSample, []byte{0xff}, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := generateReport(corpus, testCorpusRevision, "")
	if err == nil || !strings.Contains(err.Error(), "unknown Conway rule directory") {
		t.Fatalf("GenerateReport error = %v, want unknown rule directory", err)
	}
}

func TestGenerateReportRejectsEmptyConwayRule(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	corpus := filepath.Join(root, "conway")
	makeCompleteCorpus(t, corpus)
	emptySample := filepath.Join(
		corpus,
		"credential",
		"invalid-zap-1",
		"complete.input.cbor",
	)
	if err := os.Remove(emptySample); err != nil {
		t.Fatal(err)
	}

	_, err := generateReport(corpus, testCorpusRevision, "")
	if err == nil || !strings.Contains(err.Error(), "contains no CBOR samples") {
		t.Fatalf("GenerateReport error = %v, want empty rule directory", err)
	}
}

func TestGenerateReportRejectsSampleSymlink(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	corpus := filepath.Join(root, "conway")
	makeCompleteCorpus(t, corpus)
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
	outside := filepath.Join(root, "outside.cbor")
	if err := os.WriteFile(outside, valid, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(validDir, "escape.input.cbor")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(validDir, "escape.expected.cbor"),
		valid,
		0o644,
	); err != nil {
		t.Fatal(err)
	}

	report, err := generateReport(corpus, testCorpusRevision, "")
	if err != nil {
		t.Fatal(err)
	}
	if report.Successful {
		t.Fatal("report accepted a sample symlink escaping the corpus")
	}
	if len(report.Failures) != 1 || report.Failures[0].Class != sampleUnreadable {
		t.Fatalf("failures = %+v, want one unreadable sample", report.Failures)
	}
}

func TestGenerateReportRejectsOversizedSample(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	corpus := filepath.Join(root, "conway")
	makeCompleteCorpus(t, corpus)
	validDir := filepath.Join(corpus, "credential", "valid")
	if err := os.MkdirAll(validDir, 0o755); err != nil {
		t.Fatal(err)
	}
	inputPath := filepath.Join(validDir, "oversized.input.cbor")
	if err := os.WriteFile(inputPath, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(inputPath, maxSampleBytes+1); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(validDir, "oversized.expected.cbor"),
		[]byte{0xf6},
		0o644,
	); err != nil {
		t.Fatal(err)
	}

	report, err := generateReport(corpus, testCorpusRevision, "")
	if err != nil {
		t.Fatal(err)
	}
	if report.Successful {
		t.Fatal("report accepted an oversized sample")
	}
	if len(report.Failures) != 1 ||
		report.Failures[0].Class != sampleUnreadable ||
		!strings.Contains(report.Failures[0].Reason, "sample limit") {
		t.Fatalf("failures = %+v, want one sample-size failure", report.Failures)
	}
}

func TestGenerateReportRejectsInvalidCorpusRevision(t *testing.T) {
	t.Parallel()
	_, err := GenerateReport(t.TempDir(), "main")
	if err == nil || !strings.Contains(err.Error(), "full commit SHA") {
		t.Fatalf("GenerateReport error = %v, want full commit SHA error", err)
	}
}

func TestGenerateReportRequiresRecordedCorpusInventory(t *testing.T) {
	t.Parallel()
	_, err := GenerateReport(t.TempDir(), testCorpusRevision)
	if err == nil || !strings.Contains(err.Error(), "no Conway corpus inventory is recorded") {
		t.Fatalf("GenerateReport error = %v, want missing corpus inventory error", err)
	}
}

func TestGenerateReportClassifiesAcceptedMutation(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	corpus := filepath.Join(root, "conway-123-100")
	makeCompleteCorpus(t, corpus)
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

	report, err := generateReport(corpus, testCorpusRevision, "")
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
