// Copyright 2026 Blink Labs Software

package cborconformance

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	schemaVersion    = "1"
	scenario         = "cbor-decoding"
	protocolVersion  = "11.0"
	maxCorpusSamples = 100_000
	maxSampleBytes   = int64(16 << 20)

	// Update this inventory together with the dataset revision in both conformance workflows.
	primaryConwayCorpusRevision  = "6cd8c928665bbafe90e02066ebb4620e38f051de"
	primaryConwayCorpusInventory = "9a7ce28ed79198ed30e09a9d13032728d00e0b70e501891965aa126c4ab7a93f"

	decodeFailed        = "decoding failed"
	decodeSucceeded     = "decoding succeeded but the sample must be rejected"
	referenceMismatch   = "re-encoding differs from the cbor reference"
	byteExactMismatch   = "re-encoding differs from the original bytes"
	referenceUnreadable = "the cbor reference is missing or unreadable"
	sampleUnreadable    = "the sample is unreadable"
)

type Outcome struct {
	GeneratedTotal                    int `json:"generated_total"`
	GeneratedDecodedReencodedExpected int `json:"generated_decoded_reencoded_expected"`
	GeneratedDecodedReencodedActual   int `json:"generated_decoded_reencoded_actual"`
	GeneratedMustBeRejectedExpected   int `json:"generated_must_be_rejected_expected"`
	GeneratedMustBeRejectedActual     int `json:"generated_must_be_rejected_actual"`
	ZapMustBeRejectedExpected         int `json:"zap_must_be_rejected_expected"`
	ZapMustBeRejectedActual           int `json:"zap_must_be_rejected_actual"`
}

type Failure struct {
	Sample string `json:"sample"`
	Rule   string `json:"rule"`
	Class  string `json:"class"`
	Reason string `json:"reason"`
}

type WaivedSample struct {
	Sample string `json:"sample"`
	Rule   string `json:"rule"`
	Kind   string `json:"kind"`
	Reason string `json:"reason"`
}

type Report struct {
	SchemaVersion   string             `json:"version"`
	Scenario        string             `json:"scenario"`
	GeneratedAt     time.Time          `json:"generated_at"`
	Corpus          string             `json:"corpus"`
	CorpusRevision  string             `json:"corpus_revision"`
	ProtocolVersion string             `json:"protocol_version"`
	Successful      bool               `json:"successful"`
	Totals          Outcome            `json:"totals"`
	Rules           map[string]Outcome `json:"rules"`
	Failures        []Failure          `json:"failures"`
	WaivedSamples   []WaivedSample     `json:"waived_samples"`
}

type corpusMetadata struct {
	Waivers []corpusWaiver `json:"waivers"`
}

type corpusWaiver struct {
	Rule    string   `json:"rule"`
	Kind    string   `json:"kind"`
	Reason  string   `json:"reason"`
	Samples []string `json:"samples"`
}

type expectation uint8

const (
	mustDecode expectation = iota
	mustReject
)

type sample struct {
	rule        string
	category    string
	name        string
	path        string
	expectPath  string
	expectation expectation
	mutation    bool
}

// GenerateReport evaluates the Conway CBOR corpus using Dingo's ledger
// decoders and encoders. The report follows the cardano-cbor-dataset schema.
func GenerateReport(datasetDir, corpusRevision string) (Report, error) {
	if err := validateCorpusRevision(corpusRevision); err != nil {
		return Report{}, err
	}
	if corpusRevision != primaryConwayCorpusRevision {
		return Report{}, fmt.Errorf("no Conway corpus inventory is recorded for revision %s", corpusRevision)
	}
	return generateReport(datasetDir, corpusRevision, primaryConwayCorpusInventory)
}

func generateReport(datasetDir, corpusRevision, expectedInventory string) (Report, error) {
	if err := validateCorpusRevision(corpusRevision); err != nil {
		return Report{}, err
	}
	root, err := filepath.Abs(datasetDir)
	if err != nil {
		return Report{}, fmt.Errorf("resolve dataset directory: %w", err)
	}
	corpusRoot, err := os.OpenRoot(root)
	if err != nil {
		return Report{}, fmt.Errorf("open dataset directory: %w", err)
	}
	defer func() { _ = corpusRoot.Close() }()
	info, err := corpusRoot.Stat(".")
	if err != nil {
		return Report{}, fmt.Errorf("stat dataset directory: %w", err)
	}
	if !info.IsDir() {
		return Report{}, fmt.Errorf("dataset path is not a directory: %s", root)
	}
	if expectedInventory != "" {
		actualInventory, err := corpusInventorySHA256(corpusRoot.FS())
		if err != nil {
			return Report{}, fmt.Errorf("hash Conway corpus inventory: %w", err)
		}
		if actualInventory != expectedInventory {
			return Report{}, fmt.Errorf(
				"conway corpus inventory mismatch for revision %s: got %s, want %s",
				corpusRevision,
				actualInventory,
				expectedInventory,
			)
		}
	}

	entries, err := fs.ReadDir(corpusRoot.FS(), ".")
	if err != nil {
		return Report{}, fmt.Errorf("read dataset directory: %w", err)
	}
	if len(entries) == 0 {
		return Report{}, errors.New("dataset contains no rule directories")
	}

	knownRules := ruleNames()
	known := make(map[string]struct{}, len(knownRules))
	for _, rule := range knownRules {
		known[rule] = struct{}{}
	}

	report := Report{
		SchemaVersion:   schemaVersion,
		Scenario:        scenario,
		GeneratedAt:     time.Now().UTC(),
		Corpus:          filepath.Base(root),
		CorpusRevision:  corpusRevision,
		ProtocolVersion: protocolVersion,
		Rules:           make(map[string]Outcome, len(entries)),
		Failures:        []Failure{},
	}

	var files []sample
	metadataPresent := false
	for _, entry := range entries {
		if !entry.IsDir() {
			if entry.Name() == "corpus.json" {
				metadataPresent = true
				info, err := corpusRoot.Lstat(entry.Name())
				if err != nil {
					return Report{}, fmt.Errorf("stat corpus metadata: %w", err)
				}
				if !info.Mode().IsRegular() {
					return Report{}, errors.New("corpus.json is not a regular file")
				}
				continue
			}
			return Report{}, fmt.Errorf(
				"unexpected file in dataset root: %s",
				entry.Name(),
			)
		}
		if _, ok := known[entry.Name()]; !ok {
			return Report{}, fmt.Errorf(
				"unknown Conway rule directory: %s",
				entry.Name(),
			)
		}
		report.Rules[entry.Name()] = Outcome{}
		ruleFiles, err := listRuleSamples(corpusRoot, entry.Name())
		if err != nil {
			return Report{}, err
		}
		if len(ruleFiles) == 0 {
			return Report{}, fmt.Errorf(
				"conway rule directory %s contains no CBOR samples",
				entry.Name(),
			)
		}
		files = append(files, ruleFiles...)
		if len(files) > maxCorpusSamples {
			return Report{}, fmt.Errorf(
				"dataset exceeds %d CBOR samples",
				maxCorpusSamples,
			)
		}
	}
	waivedSamples, err := loadSampleWaivers(corpusRoot, metadataPresent, files)
	if err != nil {
		return Report{}, err
	}
	report.WaivedSamples = waivedSamples
	if len(files) == 0 {
		return Report{}, errors.New("dataset contains no CBOR samples")
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].name < files[j].name
	})

	waived := make(map[string]struct{}, len(waivedSamples))
	for _, sample := range waivedSamples {
		waived[sample.Sample] = struct{}{}
	}
	for _, file := range files {
		if _, ok := waived[file.name]; ok {
			continue
		}
		outcome := report.Rules[file.rule]
		if file.mutation {
			outcome.ZapMustBeRejectedExpected++
		} else {
			outcome.GeneratedTotal++
		}
		switch {
		case file.expectation == mustDecode:
			outcome.GeneratedDecodedReencodedExpected++
		case !file.mutation:
			outcome.GeneratedMustBeRejectedExpected++
		}

		input, err := readCorpusFile(corpusRoot, file.path)
		if err != nil {
			report.addFailure(
				file,
				sampleUnreadable,
				"cannot read sample: "+err.Error(),
			)
			report.Rules[file.rule] = outcome
			continue
		}

		reencoded, err := reencode(file.rule, input)
		if file.expectation == mustReject {
			if err != nil {
				if file.mutation {
					outcome.ZapMustBeRejectedActual++
				} else {
					outcome.GeneratedMustBeRejectedActual++
				}
			} else {
				report.addFailure(file, decodeSucceeded, "deserialization unexpectedly succeeded")
			}
			report.Rules[file.rule] = outcome
			continue
		}
		if err != nil {
			report.addFailure(
				file,
				decodeFailed,
				"deserialization or re-encoding failed: "+err.Error(),
			)
			report.Rules[file.rule] = outcome
			continue
		}

		byteExact := isByteExactRule(file.rule)
		if byteExact && !bytes.Equal(reencoded, input) {
			report.addFailure(
				file,
				byteExactMismatch,
				"reserialization differs from the original bytes, which this rule hashes",
			)
			report.Rules[file.rule] = outcome
			continue
		}
		if !byteExact {
			reencoded, err = NormalizeCBOR(reencoded)
			if err != nil {
				report.addFailure(
					file,
					decodeFailed,
					"normalize re-encoding failed: "+err.Error(),
				)
				report.Rules[file.rule] = outcome
				continue
			}
		}
		if file.expectPath == "" {
			report.addFailure(
				file,
				referenceUnreadable,
				"sample was accepted but has no reference encoding",
			)
			report.Rules[file.rule] = outcome
			continue
		}
		reference, err := readCorpusFile(corpusRoot, file.expectPath)
		if err != nil {
			report.addFailure(
				file,
				referenceUnreadable,
				"cannot read expected output: "+err.Error(),
			)
			report.Rules[file.rule] = outcome
			continue
		}
		if !byteExact {
			reference, err = NormalizeCBOR(reference)
			if err != nil {
				report.addFailure(
					file,
					referenceUnreadable,
					"cannot normalize expected output: "+err.Error(),
				)
				report.Rules[file.rule] = outcome
				continue
			}
		}
		if !bytes.Equal(reencoded, reference) {
			reason := "normalized reserialization differs from the expected output"
			if byteExact {
				reason = "byte-exact reserialization differs from the expected output"
			}
			report.addFailure(
				file,
				referenceMismatch,
				reason,
			)
		} else {
			outcome.GeneratedDecodedReencodedActual++
		}
		report.Rules[file.rule] = outcome
	}

	for _, outcome := range report.Rules {
		report.Totals.add(outcome)
	}
	report.Successful = len(report.Failures) == 0
	return report, nil
}

func corpusInventorySHA256(corpus fs.FS) (string, error) {
	var files []string
	if err := fs.WalkDir(corpus, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if name == "." || entry.IsDir() {
			return nil
		}
		if entry.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("corpus contains a symbolic link: %s", name)
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("stat corpus file %s: %w", name, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("corpus entry is not a regular file: %s", name)
		}
		files = append(files, name)
		return nil
	}); err != nil {
		return "", fmt.Errorf("walk corpus files: %w", err)
	}
	sort.Strings(files)

	digest := sha256.New()
	for _, name := range files {
		file, err := corpus.Open(name)
		if err != nil {
			return "", fmt.Errorf("open corpus file %s: %w", name, err)
		}
		fileDigest := sha256.New()
		if _, err := io.Copy(fileDigest, file); err != nil {
			_ = file.Close()
			return "", fmt.Errorf("read corpus file %s: %w", name, err)
		}
		if err := file.Close(); err != nil {
			return "", fmt.Errorf("close corpus file %s: %w", name, err)
		}
		_, _ = io.WriteString(digest, name)
		_, _ = digest.Write([]byte{0})
		_, _ = digest.Write(fileDigest.Sum(nil))
		_, _ = digest.Write([]byte{'\n'})
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func loadSampleWaivers(
	root *os.Root,
	metadataPresent bool,
	files []sample,
) ([]WaivedSample, error) {
	if !metadataPresent {
		return nil, nil
	}
	data, err := readCorpusFile(root, "corpus.json")
	if err != nil {
		return nil, fmt.Errorf("read corpus metadata: %w", err)
	}
	var metadata corpusMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("decode corpus metadata: %w", err)
	}

	sampleRules := make(map[string]string, len(files))
	for _, file := range files {
		sampleRules[file.name] = file.rule
	}
	waived := make(map[string]WaivedSample)
	for _, waiver := range metadata.Waivers {
		if len(waiver.Samples) == 0 {
			continue
		}
		if waiver.Rule == "" || waiver.Kind == "" || strings.TrimSpace(waiver.Reason) == "" {
			return nil, errors.New("sample waiver must include rule, kind, and reason")
		}
		for _, sampleName := range waiver.Samples {
			rule, ok := sampleRules[sampleName]
			if !ok {
				return nil, fmt.Errorf("sample waiver references unknown sample %q", sampleName)
			}
			if rule != waiver.Rule {
				return nil, fmt.Errorf(
					"sample waiver for %q names rule %q, sample belongs to %q",
					sampleName,
					waiver.Rule,
					rule,
				)
			}
			entry := WaivedSample{
				Sample: sampleName,
				Rule:   waiver.Rule,
				Kind:   waiver.Kind,
				Reason: waiver.Reason,
			}
			if previous, exists := waived[sampleName]; exists {
				if previous.Rule != entry.Rule || previous.Kind != entry.Kind || previous.Reason != entry.Reason {
					return nil, fmt.Errorf("sample %q has conflicting waiver metadata", sampleName)
				}
				continue
			}
			waived[sampleName] = entry
		}
	}

	result := make([]WaivedSample, 0, len(waived))
	for _, sample := range waived {
		result = append(result, sample)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Sample < result[j].Sample
	})
	return result, nil
}

func validateCorpusRevision(revision string) error {
	if len(revision) != 40 || strings.ToLower(revision) != revision {
		return errors.New("corpus revision must be a lowercase full commit SHA")
	}
	if _, err := hex.DecodeString(revision); err != nil {
		return errors.New("corpus revision must be a lowercase full commit SHA")
	}
	return nil
}

func listRuleSamples(root *os.Root, rule string) ([]sample, error) {
	ruleDir := rule
	entries, err := fs.ReadDir(root.FS(), ruleDir)
	if err != nil {
		return nil, fmt.Errorf("read rule directory %s: %w", rule, err)
	}
	var files []sample
	for _, entry := range entries {
		switch entry.Name() {
		case "expected":
			if !entry.IsDir() {
				return nil, fmt.Errorf(
					"expected output path is not a directory: %s",
					path.Join(ruleDir, entry.Name()),
				)
			}
		case "valid":
			if !entry.IsDir() {
				return nil, fmt.Errorf(
					"valid sample path is not a directory: %s",
					path.Join(ruleDir, entry.Name()),
				)
			}
			validFiles, err := listCategoryFiles(root, rule, "valid", "valid")
			if err != nil {
				return nil, err
			}
			files = append(files, validFiles...)
		case "invalid":
			if !entry.IsDir() {
				return nil, fmt.Errorf(
					"invalid sample path is not a directory: %s",
					path.Join(ruleDir, entry.Name()),
				)
			}
			zapEntries, err := fs.ReadDir(root.FS(), path.Join(ruleDir, "invalid"))
			if err != nil {
				return nil, fmt.Errorf(
					"read invalid samples for %s: %w",
					rule,
					err,
				)
			}
			for _, zapEntry := range zapEntries {
				if !zapEntry.IsDir() || !validZapCategory(zapEntry.Name()) {
					return nil, fmt.Errorf(
						"unexpected invalid category for %s: %s",
						rule,
						zapEntry.Name(),
					)
				}
				category := path.Join("invalid", zapEntry.Name())
				zapFiles, err := listCategoryFiles(
					root,
					rule,
					category,
					category,
				)
				if err != nil {
					return nil, err
				}
				files = append(files, zapFiles...)
			}
		case "manual-valid", "manual-invalid":
			if !entry.IsDir() {
				return nil, fmt.Errorf(
					"manual sample path is not a directory: %s",
					path.Join(ruleDir, entry.Name()),
				)
			}
			manualFiles, err := listCategoryFiles(
				root,
				rule,
				entry.Name(),
				entry.Name(),
			)
			if err != nil {
				return nil, err
			}
			files = append(files, manualFiles...)
		default:
			if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "invalid-") {
				return nil, fmt.Errorf(
					"unexpected entry in rule directory %s: %s",
					rule,
					entry.Name(),
				)
			}
			zapCategory := strings.TrimPrefix(entry.Name(), "invalid-")
			if !validZapCategory(zapCategory) {
				return nil, fmt.Errorf(
					"unexpected invalid category for %s: %s",
					rule,
					entry.Name(),
				)
			}
			category := path.Join("invalid", zapCategory)
			zapFiles, err := listCategoryFiles(
				root,
				rule,
				entry.Name(),
				category,
			)
			if err != nil {
				return nil, err
			}
			files = append(files, zapFiles...)
		}
	}
	return files, nil
}

func validZapCategory(name string) bool {
	return name == "zap-0" || name == "zap-1" || name == "zap-2" ||
		name == "zap-3"
}

func listCategoryFiles(
	root *os.Root,
	rule, sourceCategory, category string,
) ([]sample, error) {
	expectation, mutation, err := classifyCategory(category)
	if err != nil {
		return nil, err
	}
	categoryDir := path.Join(rule, sourceCategory)
	entries, err := fs.ReadDir(root.FS(), categoryDir)
	if err != nil {
		return nil, fmt.Errorf(
			"read sample category %s/%s: %w",
			rule,
			category,
			err,
		)
	}
	files := make([]sample, 0, len(entries))
	inputNames := make(map[string]struct{}, len(entries))
	if expectation == mustDecode {
		for _, entry := range entries {
			if entry.IsDir() ||
				!strings.HasSuffix(entry.Name(), ".input.cbor") {
				continue
			}
			inputNames[strings.TrimSuffix(entry.Name(), ".input.cbor")] = struct{}{}
		}
	}
	for _, entry := range entries {
		if entry.IsDir() {
			return nil, fmt.Errorf(
				"unexpected sample entry in %s/%s: %s",
				rule,
				category,
				entry.Name(),
			)
		}
		filename := entry.Name()
		var sampleName string
		var expectPath string
		switch {
		case strings.HasSuffix(filename, ".expected.cbor"):
			if expectation != mustDecode {
				return nil, fmt.Errorf(
					"unexpected sample entry in %s/%s: %s",
					rule,
					category,
					filename,
				)
			}
			sampleName := strings.TrimSuffix(filename, ".expected.cbor")
			if _, ok := inputNames[sampleName]; !ok {
				return nil, fmt.Errorf(
					"expected output has no matching input: %s",
					path.Join(categoryDir, filename),
				)
			}
			continue
		case strings.HasSuffix(filename, ".input.cbor"):
			sampleName = strings.TrimSuffix(filename, ".input.cbor")
			if expectation == mustDecode {
				expectPath = path.Join(
					categoryDir,
					sampleName+".expected.cbor",
				)
			}
		case strings.HasSuffix(filename, ".cbor"):
			sampleName = strings.TrimSuffix(filename, ".cbor")
			if expectation == mustDecode {
				expectPath = path.Join(rule, "expected", filename)
			}
		default:
			return nil, fmt.Errorf(
				"unexpected sample entry in %s/%s: %s",
				rule,
				category,
				filename,
			)
		}
		name := path.Join(rule, category, sampleName)
		file := sample{
			rule:        rule,
			category:    filepath.ToSlash(category),
			name:        name,
			path:        path.Join(categoryDir, filename),
			expectPath:  expectPath,
			expectation: expectation,
			mutation:    mutation,
		}
		files = append(files, file)
	}
	return files, nil
}

func classifyCategory(category string) (expectation, bool, error) {
	switch category {
	case "valid", "manual-valid":
		return mustDecode, false, nil
	case "manual-invalid", "invalid/zap-0":
		return mustReject, false, nil
	case "invalid/zap-1", "invalid/zap-2", "invalid/zap-3":
		return mustReject, true, nil
	default:
		return mustReject, false, fmt.Errorf("unknown Conway sample category: %s", category)
	}
}

func readCorpusFile(root *os.Root, name string) ([]byte, error) {
	info, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%s is a symbolic link", name)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", name)
	}
	if info.Size() > maxSampleBytes {
		return nil, fmt.Errorf(
			"%s exceeds the %d-byte sample limit",
			name,
			maxSampleBytes,
		)
	}
	file, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	openedInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !openedInfo.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", name)
	}
	if !os.SameFile(info, openedInfo) {
		return nil, fmt.Errorf("%s changed while it was opened", name)
	}
	data, err := io.ReadAll(io.LimitReader(file, maxSampleBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxSampleBytes {
		return nil, fmt.Errorf(
			"%s exceeds the %d-byte sample limit",
			name,
			maxSampleBytes,
		)
	}
	return data, nil
}

func (r *Report) addFailure(file sample, class, reason string) {
	r.Failures = append(r.Failures, Failure{
		Sample: file.name,
		Rule:   file.rule,
		Class:  class,
		Reason: reason,
	})
}

func (o *Outcome) add(other Outcome) {
	o.GeneratedTotal += other.GeneratedTotal
	o.GeneratedDecodedReencodedExpected += other.GeneratedDecodedReencodedExpected
	o.GeneratedDecodedReencodedActual += other.GeneratedDecodedReencodedActual
	o.GeneratedMustBeRejectedExpected += other.GeneratedMustBeRejectedExpected
	o.GeneratedMustBeRejectedActual += other.GeneratedMustBeRejectedActual
	o.ZapMustBeRejectedExpected += other.ZapMustBeRejectedExpected
	o.ZapMustBeRejectedActual += other.ZapMustBeRejectedActual
}

// WriteReport writes an indented JSON report with a trailing newline.
func WriteReport(path string, report Report) error {
	if report.Failures == nil {
		report.Failures = []Failure{}
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create report directory: %w", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	return nil
}
