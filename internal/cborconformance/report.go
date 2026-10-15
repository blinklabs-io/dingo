// Copyright 2026 Blink Labs Software

package cborconformance

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	protocolVersion = "11.0"

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

type Report struct {
	Corpus          string             `json:"corpus"`
	ProtocolVersion string             `json:"protocol_version"`
	Successful      bool               `json:"successful"`
	Totals          Outcome            `json:"totals"`
	Rules           map[string]Outcome `json:"rules"`
	Failures        []Failure          `json:"failures"`
}

type expectation uint8

const (
	mustDecode expectation = iota
	mustReject
)

type sample struct {
	rule       string
	category   string
	name       string
	path       string
	expectPath string
}

// GenerateReport evaluates the Conway CBOR corpus using Dingo's ledger
// decoders and encoders. The report follows the cardano-cbor-dataset schema.
func GenerateReport(datasetDir string) (Report, error) {
	root, err := filepath.Abs(datasetDir)
	if err != nil {
		return Report{}, fmt.Errorf("resolve dataset directory: %w", err)
	}
	info, err := os.Stat(root)
	if err != nil {
		return Report{}, fmt.Errorf("stat dataset directory: %w", err)
	}
	if !info.IsDir() {
		return Report{}, fmt.Errorf("dataset path is not a directory: %s", root)
	}

	entries, err := os.ReadDir(root)
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
		Corpus:          filepath.Base(root),
		ProtocolVersion: protocolVersion,
		Rules:           make(map[string]Outcome, len(knownRules)),
		Failures:        []Failure{},
	}
	for _, rule := range knownRules {
		report.Rules[rule] = Outcome{}
	}

	var files []sample
	for _, entry := range entries {
		if !entry.IsDir() {
			return Report{}, fmt.Errorf("unexpected file in dataset root: %s", entry.Name())
		}
		if _, ok := known[entry.Name()]; !ok {
			return Report{}, fmt.Errorf("unknown Conway rule directory: %s", entry.Name())
		}
		ruleFiles, err := listRuleSamples(root, entry.Name())
		if err != nil {
			return Report{}, err
		}
		files = append(files, ruleFiles...)
	}
	if len(files) == 0 {
		return Report{}, errors.New("dataset contains no CBOR samples")
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].name < files[j].name
	})

	for _, file := range files {
		outcome := report.Rules[file.rule]
		want := mustDecode
		if file.category == "invalid/zap-0" {
			want = mustReject
		}
		if want == mustDecode || want == mustReject {
			if file.category == "valid" || file.category == "invalid/zap-0" {
				outcome.GeneratedTotal++
			}
		}
		switch file.category {
		case "valid":
			outcome.GeneratedDecodedReencodedExpected++
		case "invalid/zap-0":
			outcome.GeneratedMustBeRejectedExpected++
		default:
			outcome.ZapMustBeRejectedExpected++
		}

		input, err := os.ReadFile(file.path)
		if err != nil {
			report.addFailure(file, sampleUnreadable, "cannot read sample: "+err.Error())
			report.Rules[file.rule] = outcome
			continue
		}

		reencoded, err := reencode(file.rule, input)
		if want == mustReject {
			if err != nil {
				outcome.GeneratedMustBeRejectedActual++
			} else {
				report.addFailure(file, decodeSucceeded, "deserialization unexpectedly succeeded")
			}
			report.Rules[file.rule] = outcome
			continue
		}
		if file.category != "valid" {
			if err != nil {
				outcome.ZapMustBeRejectedActual++
			} else {
				report.addFailure(file, decodeSucceeded, "deserialization unexpectedly succeeded")
			}
			report.Rules[file.rule] = outcome
			continue
		}
		if err != nil {
			report.addFailure(file, decodeFailed, "deserialization or re-encoding failed: "+err.Error())
			report.Rules[file.rule] = outcome
			continue
		}

		if isByteExactRule(file.rule) && !bytes.Equal(reencoded, input) {
			report.addFailure(file, byteExactMismatch, "reserialization differs from the original bytes, which this rule hashes")
			report.Rules[file.rule] = outcome
			continue
		}
		if file.expectPath == "" {
			report.addFailure(file, referenceUnreadable, "sample was accepted but has no reference encoding")
			report.Rules[file.rule] = outcome
			continue
		}
		reference, err := os.ReadFile(file.expectPath)
		if err != nil {
			report.addFailure(file, referenceUnreadable, "cannot read expected output: "+err.Error())
			report.Rules[file.rule] = outcome
			continue
		}
		if !isByteExactRule(file.rule) {
			reencoded, err = NormalizeCBOR(reencoded)
			if err != nil {
				report.addFailure(file, decodeFailed, "normalize re-encoding failed: "+err.Error())
				report.Rules[file.rule] = outcome
				continue
			}
		}
		if !bytes.Equal(reencoded, reference) {
			report.addFailure(file, referenceMismatch, "normalized reserialization differs from the expected output")
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

func listRuleSamples(root, rule string) ([]sample, error) {
	ruleDir := filepath.Join(root, rule)
	entries, err := os.ReadDir(ruleDir)
	if err != nil {
		return nil, fmt.Errorf("read rule directory %s: %w", rule, err)
	}
	var files []sample
	for _, entry := range entries {
		if entry.Name() == "expected" {
			if !entry.IsDir() {
				return nil, fmt.Errorf("expected output path is not a directory: %s", filepath.Join(ruleDir, entry.Name()))
			}
			continue
		}
		if entry.Name() == "valid" {
			if !entry.IsDir() {
				return nil, fmt.Errorf("valid sample path is not a directory: %s", filepath.Join(ruleDir, entry.Name()))
			}
			validFiles, err := listCategoryFiles(root, rule, "valid")
			if err != nil {
				return nil, err
			}
			files = append(files, validFiles...)
			continue
		}
		if entry.Name() == "invalid" {
			if !entry.IsDir() {
				return nil, fmt.Errorf("invalid sample path is not a directory: %s", filepath.Join(ruleDir, entry.Name()))
			}
			zapEntries, err := os.ReadDir(filepath.Join(ruleDir, "invalid"))
			if err != nil {
				return nil, fmt.Errorf("read invalid samples for %s: %w", rule, err)
			}
			for _, zapEntry := range zapEntries {
				if !zapEntry.IsDir() || !validZapCategory(zapEntry.Name()) {
					return nil, fmt.Errorf("unexpected invalid category for %s: %s", rule, zapEntry.Name())
				}
				zapFiles, err := listCategoryFiles(root, rule, filepath.Join("invalid", zapEntry.Name()))
				if err != nil {
					return nil, err
				}
				files = append(files, zapFiles...)
			}
			continue
		}
		return nil, fmt.Errorf("unexpected entry in rule directory %s: %s", rule, entry.Name())
	}
	return files, nil
}

func validZapCategory(name string) bool {
	return name == "zap-0" || name == "zap-1" || name == "zap-2" || name == "zap-3"
}

func listCategoryFiles(root, rule, category string) ([]sample, error) {
	categoryDir := filepath.Join(root, rule, category)
	entries, err := os.ReadDir(categoryDir)
	if err != nil {
		return nil, fmt.Errorf("read sample category %s/%s: %w", rule, category, err)
	}
	files := make([]sample, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".cbor" {
			return nil, fmt.Errorf("unexpected sample entry in %s/%s: %s", rule, category, entry.Name())
		}
		name := filepath.ToSlash(filepath.Join(rule, category, strings.TrimSuffix(entry.Name(), ".cbor")))
		file := sample{
			rule:     rule,
			category: filepath.ToSlash(category),
			name:     name,
			path:     filepath.Join(categoryDir, entry.Name()),
		}
		if category == "valid" {
			file.expectPath = filepath.Join(root, rule, "expected", entry.Name())
		}
		files = append(files, file)
	}
	return files, nil
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
