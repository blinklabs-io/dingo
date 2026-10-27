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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blinklabs-io/plutigo/cek"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/fxamacker/cbor/v2"
)

const (
	turboSchemaVersion = "1"
	turboBestSamples   = 30
)

var turboVersionSuffix = regexp.MustCompile(`(?i)-v([123])(-p([0-9]+))?\.flat$`)

type TurboReport struct {
	SchemaVersion  string            `json:"version"`
	Scenario       string            `json:"scenario"`
	GeneratedAt    string            `json:"generated_at"`
	Implementation map[string]string `json:"implementation"`
	Machine        TurboMachine      `json:"machine"`
	Benchmarks     []TurboBenchmark  `json:"benchmarks"`
}

type TurboMachine struct {
	OS           string `json:"os,omitempty"`
	Architecture string `json:"architecture,omitempty"`
	LogicalCores int    `json:"logical_cores,omitempty"`
}

type TurboBenchmark struct {
	Bench           string   `json:"bench"`
	PlutusVersion   string   `json:"plutus_version"`
	ProtocolVersion string   `json:"protocol_version"`
	BudgetCPU       *int64   `json:"budget_cpu"`
	BudgetMem       *int64   `json:"budget_mem"`
	CPUUS           *float64 `json:"cpu_us"`
	MemBytes        *float64 `json:"mem_bytes"`
	FlatUS          *float64 `json:"-"`
	SizeBytes       int      `json:"-"`
	Error           string   `json:"-"`
}

type turboSample struct {
	duration           time.Duration
	evaluationDuration time.Duration
	flatDuration       time.Duration
	memBytes           uint64
	budget             cek.ExBudget
}

type turboCase struct {
	path          string
	name          string
	version       lang.LanguageVersion
	group         string
	protocolMajor int
}

// GenerateTurboReport measures flat UPLC programs. Memory is measured as Go
// heap allocation during evaluation; this captures the evaluator's arena
// growth and other evaluation allocations while excluding Flat decoding.
//
// A positive maxPrograms measures an evenly spaced, name-ordered subset of
// that size, so the selection is the same on every run; zero measures the
// whole corpus.
func GenerateTurboReport(
	root string,
	samples, maxPrograms int,
	generatedAt time.Time,
) (TurboReport, error) {
	if samples < 1 {
		return TurboReport{}, errors.New("sample count must be positive")
	}
	if maxPrograms < 0 {
		return TurboReport{}, errors.New("program limit must not be negative")
	}
	if runtime.GOOS != "linux" {
		return TurboReport{}, errors.New(
			"UPLC turbo reports can only be generated on Linux",
		)
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return TurboReport{}, fmt.Errorf(
			"resolve turbo corpus directory: %w",
			err,
		)
	}
	info, err := os.Stat(root)
	if err != nil {
		return TurboReport{}, fmt.Errorf("stat turbo corpus directory: %w", err)
	}
	if !info.IsDir() {
		return TurboReport{}, fmt.Errorf(
			"turbo corpus path is not a directory: %s",
			root,
		)
	}
	cases, err := discoverTurboCases(root)
	if err != nil {
		return TurboReport{}, err
	}
	if len(cases) == 0 {
		return TurboReport{}, errors.New(
			"turbo corpus contains no versioned Flat programs",
		)
	}
	if maxPrograms > 0 && len(cases) > maxPrograms {
		cases = evenlySpaced(cases, maxPrograms)
	}

	report := TurboReport{
		SchemaVersion: turboSchemaVersion,
		Scenario:      "uplc-turbo",
		GeneratedAt:   generatedAt.UTC().Format(time.RFC3339),
		Implementation: map[string]string{
			"name": "dingo",
		},
		Machine: TurboMachine{
			OS:           runtime.GOOS,
			Architecture: runtime.GOARCH,
			LogicalCores: runtime.NumCPU(),
		},
		Benchmarks: make([]TurboBenchmark, 0, len(cases)),
	}

	var benchmarkErrors []error
	for _, tc := range cases {
		programBytes, err := readTurboProgram(tc.path)
		if err != nil {
			if _, ok := errors.AsType[*os.PathError](err); ok {
				return TurboReport{}, fmt.Errorf("read turbo program %s: %w", tc.name, err)
			}
			failure := turboBenchmarkError(tc, fmt.Errorf("read CBOR-wrapped Flat program: %w", err))
			benchmarkErrors = append(benchmarkErrors, failure)
			continue
		}
		measurements, err := measureTurboCase(tc, programBytes, samples)
		if err != nil {
			failure := turboBenchmarkError(tc, err)
			benchmarkErrors = append(benchmarkErrors, failure)
			continue
		}
		benchmark, err := makeTurboBenchmark(
			tc,
			len(programBytes),
			measurements,
		)
		if err != nil {
			failure := turboBenchmarkError(tc, err)
			benchmarkErrors = append(benchmarkErrors, failure)
			continue
		}
		report.Benchmarks = append(report.Benchmarks, benchmark)
	}
	return report, errors.Join(benchmarkErrors...)
}

// evenlySpaced returns count cases spread across the ordered input.
func evenlySpaced(cases []turboCase, count int) []turboCase {
	selected := make([]turboCase, 0, count)
	for i := range count {
		selected = append(selected, cases[i*len(cases)/count])
	}
	return selected
}

func readTurboProgram(path string) ([]byte, error) {
	encoded, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var program []byte
	if err := cbor.Unmarshal(encoded, &program); err != nil {
		return nil, fmt.Errorf("decode CBOR byte string: %w", err)
	}
	return program, nil
}

func turboBenchmarkError(tc turboCase, err error) error {
	if pathErr, ok := errors.AsType[*os.PathError](err); ok {
		err = pathErr.Err
	}
	return fmt.Errorf("benchmark %s: %w", tc.name, err)
}

func protocolMajorForEpoch(epoch int) int {
	switch {
	case epoch >= 644:
		return 11
	case epoch >= 537:
		return 10
	case epoch >= 507:
		return 9
	default:
		return 8
	}
}

func discoverTurboCases(root string) ([]turboCase, error) {
	var cases []turboCase
	benchmarkPaths := map[string]string{}
	err := filepath.WalkDir(
		root,
		func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return fmt.Errorf("walk %q: %w", path, walkErr)
			}
			if entry.IsDir() || filepath.Ext(path) != ".flat" {
				return nil
			}
			match := turboVersionSuffix.FindStringSubmatch(filepath.Base(path))
			if len(match) != 4 {
				return fmt.Errorf(
					"flat program name has no Plutus version suffix: %s",
					path,
				)
			}
			versionNumber := match[1]
			relativePath, err := filepath.Rel(root, path)
			if err != nil {
				return fmt.Errorf("resolve turbo corpus path %q: %w", path, err)
			}
			pathParts := strings.Split(relativePath, string(filepath.Separator))
			if len(pathParts) < 2 {
				return fmt.Errorf("turbo program is not inside an epoch directory: %s", path)
			}
			epoch, err := strconv.Atoi(pathParts[0])
			if err != nil || epoch < 0 {
				return fmt.Errorf("turbo program has an invalid epoch directory: %s", path)
			}
			protocolMajor := protocolMajorForEpoch(epoch)
			if match[3] != "" {
				protocolMajor, err = strconv.Atoi(match[3])
				if err != nil || protocolMajor < 1 {
					return fmt.Errorf("turbo program has an invalid protocol version suffix: %s", path)
				}
			}
			name := filepath.Base(path)
			if previous, ok := benchmarkPaths[name]; ok {
				return fmt.Errorf(
					"duplicate benchmark ID %q in %q and %q",
					name,
					previous,
					relativePath,
				)
			}
			benchmarkPaths[name] = relativePath
			version := lang.LanguageVersionV1
			switch versionNumber {
			case "2":
				version = lang.LanguageVersionV2
			case "3":
				version = lang.LanguageVersionV3
			}
			cases = append(cases, turboCase{
				path:          path,
				name:          name,
				version:       version,
				group:         "V" + versionNumber,
				protocolMajor: protocolMajor,
			})
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	sort.Slice(
		cases,
		func(i, j int) bool { return cases[i].name < cases[j].name },
	)
	return cases, nil
}

func measureTurboCase(
	tc turboCase,
	programBytes []byte,
	samples int,
) ([]turboSample, error) {
	machine := cek.NewMachine[syn.DeBruijn](
		tc.version,
		200,
		cek.NewDefaultEvalContext(tc.version, cek.ProtoVersion{Major: uint(tc.protocolMajor)}),
	)
	measurements := make([]turboSample, 0, samples)
	for range samples {
		decodeStart := time.Now()
		program, err := syn.Decode[syn.DeBruijn](programBytes)
		if err != nil {
			return nil, fmt.Errorf("decode Flat program: %w", err)
		}
		decodeDuration := time.Since(decodeStart)
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		initial := cek.ExBudget{Cpu: math.MaxInt64, Mem: math.MaxInt64}
		machine.ExBudget = initial
		evaluationStart := time.Now()
		if _, err := machine.Run(program.Term); err != nil {
			return nil, fmt.Errorf("evaluate Flat program: %w", err)
		}
		evaluationDuration := time.Since(evaluationStart)
		duration := decodeDuration + evaluationDuration
		runtime.ReadMemStats(&after)
		measurements = append(measurements, turboSample{
			duration:           duration,
			evaluationDuration: evaluationDuration,
			flatDuration:       decodeDuration,
			memBytes:           after.TotalAlloc - before.TotalAlloc,
			budget:             initial.Sub(&machine.ExBudget),
		})
	}
	return measurements, nil
}

func makeTurboBenchmark(
	tc turboCase,
	size int,
	samples []turboSample,
) (TurboBenchmark, error) {
	if len(samples) == 0 {
		return TurboBenchmark{}, errors.New("no samples measured")
	}
	firstBudget := samples[0].budget
	for _, sample := range samples[1:] {
		if sample.budget != firstBudget {
			return TurboBenchmark{}, fmt.Errorf(
				"execution budget changed between samples: first %v, got %v",
				firstBudget,
				sample.budget,
			)
		}
	}
	samples = bestTurboSamples(samples)
	var totalEvaluation time.Duration
	var totalFlat time.Duration
	var totalMemory uint64
	for _, sample := range samples {
		totalEvaluation += sample.evaluationDuration
		totalFlat += sample.flatDuration
		totalMemory += sample.memBytes
	}
	flatUS := float64(totalFlat.Nanoseconds()) / 1_000 / float64(len(samples))
	memBytes := float64(totalMemory) / float64(len(samples))
	cpuUS := float64(totalEvaluation.Nanoseconds()) / 1_000 / float64(len(samples))
	return TurboBenchmark{
		Bench:           tc.name,
		PlutusVersion:   tc.group,
		ProtocolVersion: fmt.Sprintf("%d.0", tc.protocolMajor),
		BudgetCPU:       new(firstBudget.Cpu),
		BudgetMem:       new(firstBudget.Mem),
		CPUUS:           &cpuUS,
		FlatUS:          &flatUS,
		MemBytes:        &memBytes,
		SizeBytes:       size,
	}, nil
}

func bestTurboSamples(samples []turboSample) []turboSample {
	if len(samples) <= turboBestSamples {
		return samples
	}
	best := append([]turboSample(nil), samples...)
	sort.SliceStable(best, func(i, j int) bool {
		return best[i].duration < best[j].duration
	})
	return best[:turboBestSamples]
}

// WriteTurboReport writes the turbo report as indented JSON.
func WriteTurboReport(path string, report TurboReport) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("encode UPLC turbo report: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create report directory: %w", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write UPLC turbo report: %w", err)
	}
	return nil
}
