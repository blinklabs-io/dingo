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
	"container/heap"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/blinklabs-io/plutigo/cek"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/fxamacker/cbor/v2"
)

const (
	turboSchemaVersion     = "1"
	turboProtocolVersion   = "10.0"
	turboDefaultSamples    = 10
	turboDeviationLimit    = 1.5
	turboCPUUnitNanosecond = 0.001 // one picosecond per execution unit
)

var turboVersionSuffix = regexp.MustCompile(`(?i)-v([123])\.flat$`)

type TurboReport struct {
	SchemaVersion   string            `json:"schema_version"`
	Scenario        string            `json:"scenario"`
	GeneratedAt     string            `json:"generated_at"`
	Implementation  map[string]string `json:"implementation"`
	Corpus          string            `json:"corpus"`
	ProtocolVersion string            `json:"protocol_version"`
	Setup           TurboSetup        `json:"setup"`
	Summary         []TurboSummary    `json:"summary"`
	Benchmarks      []TurboBenchmark  `json:"benchmarks"`
	Representatives []TurboBenchmark  `json:"representatives"`
	WorstCases      []TurboBenchmark  `json:"worst_cases"`
	Skipped         []TurboSkipped    `json:"skipped,omitempty"`
}

// TurboSkipped names a program that could not be measured, with the decode or
// evaluation error that excluded it.
type TurboSkipped struct {
	Bench  string `json:"bench"`
	Reason string `json:"reason"`
}

type TurboSetup struct {
	SampleCount        int     `json:"sample_count"`
	Measurement        string  `json:"measurement"`
	IncludesReset      bool    `json:"includes_reset"`
	CostModel          string  `json:"cost_model"`
	CorpusDigest       string  `json:"corpus_digest,omitempty"`
	DeviationTolerance float64 `json:"deviation_tolerance"`
	DeviationUnits     string  `json:"deviation_units"`
	MemoryMeasurement  string  `json:"memory_measurement"`
}

type TurboSummary struct {
	Group        string  `json:"group"`
	Scripts      int     `json:"scripts"`
	Samples      int     `json:"samples"`
	CPUMeanNS    float64 `json:"cpu_mean_ns"`
	CPUMaxNS     float64 `json:"cpu_max_ns"`
	MemMeanBytes float64 `json:"mem_mean_bytes"`
	MemMaxBytes  float64 `json:"mem_max_bytes"`
}

type TurboBenchmark struct {
	Bench         string  `json:"bench"`
	PlutusVersion string  `json:"plutus_version"`
	BudgetCPU     *int64  `json:"budget_cpu"`
	BudgetMem     *int64  `json:"budget_mem"`
	Group         string  `json:"group"`
	CPUMaxNS      float64 `json:"cpu_max_ns"`
	CPUMeanNS     float64 `json:"cpu_mean_ns"`
	MemMeanBytes  float64 `json:"mem_mean_bytes"`
	MemMaxBytes   float64 `json:"mem_max_bytes"`
	SizeBytes     int     `json:"size_bytes"`
}

type turboSample struct {
	duration time.Duration
	memBytes uint64
	budget   cek.ExBudget
}

type turboCase struct {
	path    string
	name    string
	version lang.LanguageVersion
	group   string
}

const turboMinHashCount = 16

type turboRepresentativeCandidate struct {
	benchmark TurboBenchmark
	path      string
	size      int
	hash      [sha256.Size]byte
	minHashes [turboMinHashCount]uint64
	indexed   bool
	medianCPU float64
	medianMem float64
}

type turboBandKey struct {
	group  string
	band   uint8
	first  uint64
	second uint64
}

type turboRepresentativeIndex struct {
	representatives []turboRepresentativeCandidate
	bands           map[turboBandKey][]int
	groupIndexes    map[string][]int
	unindexed       map[string][]int
}

type maxUint64Heap []uint64

func (h *maxUint64Heap) Len() int           { return len(*h) }
func (h *maxUint64Heap) Less(i, j int) bool { return (*h)[i] > (*h)[j] }

func (h *maxUint64Heap) Swap(
	i, j int,
) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}
func (h *maxUint64Heap) Push(value any) { *h = append(*h, value.(uint64)) }
func (h *maxUint64Heap) Pop() any {
	last := len(*h) - 1
	value := (*h)[last]
	*h = (*h)[:last]
	return value
}

// GenerateTurboReport measures flat UPLC programs. Memory is measured as Go
// heap allocation during evaluation; this captures the evaluator's arena
// growth and other evaluation allocations while excluding Flat decoding.
//
// A positive maxPrograms measures an evenly spaced, name-ordered subset of
// that size, so the selection is the same on every run; zero measures the
// whole corpus.
func GenerateTurboReport(
	root, corpusID, corpusDigest, implementationVersion string,
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
	measurement := "Flat decode + evaluation + evaluator arena reset wall-clock time"
	if maxPrograms > 0 && len(cases) > maxPrograms {
		measurement += fmt.Sprintf(
			"; %d of %d programs, evenly spaced in name order",
			maxPrograms,
			len(cases),
		)
		cases = evenlySpaced(cases, maxPrograms)
	}

	if corpusID == "" {
		corpusID = "amaru-uplc/turbo"
	}
	if implementationVersion == "" {
		implementationVersion = "unknown"
	}
	report := TurboReport{
		SchemaVersion: turboSchemaVersion,
		Scenario:      "uplc-turbo",
		GeneratedAt:   generatedAt.UTC().Format(time.RFC3339),
		Implementation: map[string]string{
			"name":    "dingo",
			"version": implementationVersion,
		},
		Corpus:          corpusID,
		ProtocolVersion: turboProtocolVersion,
		Setup: TurboSetup{
			SampleCount:        samples,
			Measurement:        measurement,
			IncludesReset:      true,
			CostModel:          "Default Plutus cost model for each script language version at protocol version 10",
			CorpusDigest:       corpusDigest,
			DeviationTolerance: turboDeviationLimit,
			DeviationUnits:     "CPU: 1 unit = 1 picosecond; memory: 1 unit = 1 byte",
			MemoryMeasurement:  "Go heap allocation bytes during evaluation, excluding Flat decoding",
		},
		Summary:         []TurboSummary{},
		Benchmarks:      make([]TurboBenchmark, 0, len(cases)),
		Representatives: make([]TurboBenchmark, 0, len(cases)),
		WorstCases:      []TurboBenchmark{},
	}

	representativeIndex := turboRepresentativeIndex{
		bands:        make(map[turboBandKey][]int),
		groupIndexes: make(map[string][]int),
		unindexed:    make(map[string][]int),
	}
	for _, tc := range cases {
		programBytes, err := readTurboProgram(tc.path)
		if err != nil {
			return TurboReport{}, fmt.Errorf(
				"read CBOR-wrapped Flat program %q: %w",
				tc.path,
				err,
			)
		}
		measurements, err := measureTurboCase(tc, programBytes, samples)
		if err != nil {
			// The corpus can hold programs a conforming decoder rejects. They
			// have no timing to report, so list them rather than losing the
			// whole run to one.
			report.Skipped = append(
				report.Skipped,
				TurboSkipped{Bench: tc.name, Reason: err.Error()},
			)
			continue
		}
		benchmark, err := makeTurboBenchmark(
			tc,
			len(programBytes),
			measurements,
		)
		if err != nil {
			return TurboReport{}, fmt.Errorf("benchmark %s: %w", tc.name, err)
		}
		report.Benchmarks = append(report.Benchmarks, benchmark)
		minHashes, indexed := turboMinHashes(programBytes)
		candidate := turboRepresentativeCandidate{
			benchmark: benchmark,
			path:      tc.path,
			size:      len(programBytes),
			hash:      sha256.Sum256(programBytes),
			minHashes: minHashes,
			indexed:   indexed,
			medianCPU: medianTurboCPU(measurements),
			medianMem: medianTurboMemory(measurements),
		}
		if err := representativeIndex.add(candidate, programBytes); err != nil {
			return TurboReport{}, fmt.Errorf(
				"classify representative %s: %w",
				tc.name,
				err,
			)
		}
	}
	if len(report.Benchmarks) == 0 {
		return TurboReport{}, fmt.Errorf(
			"no turbo program could be measured; first skipped: %s: %s",
			report.Skipped[0].Bench,
			report.Skipped[0].Reason,
		)
	}
	report.Summary = summarizeTurbo(report.Benchmarks, samples)
	report.Representatives = representativeIndex.benchmarks()
	report.WorstCases = turboWorstCases(report.Benchmarks, 10)
	return report, nil
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

func discoverTurboCases(root string) ([]turboCase, error) {
	var cases []turboCase
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
			if len(match) != 2 {
				return fmt.Errorf(
					"flat program name has no Plutus version suffix: %s",
					path,
				)
			}
			versionNumber := match[1]
			version := lang.LanguageVersionV1
			switch versionNumber {
			case "2":
				version = lang.LanguageVersionV2
			case "3":
				version = lang.LanguageVersionV3
			}
			cases = append(cases, turboCase{
				path: path,
				name: filepath.ToSlash(
					strings.TrimPrefix(path, root+string(filepath.Separator)),
				),
				version: version,
				group:   "V" + versionNumber,
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
		cek.NewDefaultEvalContext(tc.version, cek.ProtoVersion{Major: 10}),
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
		duration := decodeDuration + time.Since(evaluationStart)
		runtime.ReadMemStats(&after)
		measurements = append(measurements, turboSample{
			duration: duration,
			memBytes: after.TotalAlloc - before.TotalAlloc,
			budget:   initial.Sub(&machine.ExBudget),
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
	var totalDuration time.Duration
	var totalMemory uint64
	var maxDuration time.Duration
	var maxMemory uint64
	for _, sample := range samples {
		if sample.budget != firstBudget {
			return TurboBenchmark{}, fmt.Errorf(
				"execution budget changed between samples: first %v, got %v",
				firstBudget,
				sample.budget,
			)
		}
		totalDuration += sample.duration
		totalMemory += sample.memBytes
		if sample.duration > maxDuration {
			maxDuration = sample.duration
		}
		if sample.memBytes > maxMemory {
			maxMemory = sample.memBytes
		}
	}
	return TurboBenchmark{
		Bench:         tc.name,
		PlutusVersion: tc.group,
		BudgetCPU:     new(firstBudget.Cpu),
		BudgetMem:     new(firstBudget.Mem),
		Group:         tc.group,
		CPUMaxNS:      float64(maxDuration.Nanoseconds()),
		CPUMeanNS: float64(
			totalDuration.Nanoseconds(),
		) / float64(
			len(samples),
		),
		MemMeanBytes: float64(totalMemory) / float64(len(samples)),
		MemMaxBytes:  float64(maxMemory),
		SizeBytes:    size,
	}, nil
}

func summarizeTurbo(
	benchmarks []TurboBenchmark,
	sampleCount int,
) []TurboSummary {
	byGroup := make(map[string]TurboSummary)
	for _, benchmark := range benchmarks {
		summary := byGroup[benchmark.Group]
		summary.Group = benchmark.Group
		summary.Scripts++
		summary.Samples += sampleCount
		summary.CPUMeanNS += benchmark.CPUMeanNS
		if benchmark.CPUMaxNS > summary.CPUMaxNS {
			summary.CPUMaxNS = benchmark.CPUMaxNS
		}
		summary.MemMeanBytes += benchmark.MemMeanBytes
		if benchmark.MemMaxBytes > summary.MemMaxBytes {
			summary.MemMaxBytes = benchmark.MemMaxBytes
		}
		byGroup[benchmark.Group] = summary
	}
	groups := make([]string, 0, len(byGroup))
	for group := range byGroup {
		groups = append(groups, group)
	}
	sort.Strings(groups)
	result := make([]TurboSummary, 0, len(groups))
	for _, group := range groups {
		summary := byGroup[group]
		summary.CPUMeanNS /= float64(summary.Scripts)
		summary.MemMeanBytes /= float64(summary.Scripts)
		result = append(result, summary)
	}
	return result
}

func (index *turboRepresentativeIndex) add(
	candidate turboRepresentativeCandidate,
	program []byte,
) error {
	candidateIndexes := make(map[int]struct{})
	if candidate.indexed {
		for band := range turboMinHashCount / 2 {
			key := turboBandKey{
				group:  candidate.benchmark.Group,
				band:   uint8(band),
				first:  candidate.minHashes[band*2],
				second: candidate.minHashes[band*2+1],
			}
			for _, candidateIndex := range index.bands[key] {
				candidateIndexes[candidateIndex] = struct{}{}
			}
		}
		for _, candidateIndex := range index.unindexed[candidate.benchmark.Group] {
			candidateIndexes[candidateIndex] = struct{}{}
		}
	} else {
		for _, candidateIndex := range index.groupIndexes[candidate.benchmark.Group] {
			candidateIndexes[candidateIndex] = struct{}{}
		}
	}
	orderedIndexes := make([]int, 0, len(candidateIndexes))
	for candidateIndex := range candidateIndexes {
		orderedIndexes = append(orderedIndexes, candidateIndex)
	}
	sort.Ints(orderedIndexes)

	for _, candidateIndex := range orderedIndexes {
		representative := index.representatives[candidateIndex]
		if !withinNinetyPercentSize(candidate.size, representative.size) {
			continue
		}
		if !withinTenPercent(candidate.medianCPU, representative.medianCPU) ||
			!withinTenPercent(candidate.medianMem, representative.medianMem) {
			continue
		}
		if candidate.hash == representative.hash {
			return nil
		}
		representativeProgram, err := readTurboProgram(representative.path)
		if err != nil {
			return fmt.Errorf(
				"read representative Flat program %q: %w",
				representative.path,
				err,
			)
		}
		if byteSimilarity(program, representativeProgram) >= 0.90 {
			return nil
		}
	}

	candidateIndex := len(index.representatives)
	index.representatives = append(index.representatives, candidate)
	index.groupIndexes[candidate.benchmark.Group] = append(
		index.groupIndexes[candidate.benchmark.Group], candidateIndex,
	)
	if candidate.indexed {
		for band := range turboMinHashCount / 2 {
			key := turboBandKey{
				group:  candidate.benchmark.Group,
				band:   uint8(band),
				first:  candidate.minHashes[band*2],
				second: candidate.minHashes[band*2+1],
			}
			index.bands[key] = append(index.bands[key], candidateIndex)
		}
	} else {
		index.unindexed[candidate.benchmark.Group] = append(
			index.unindexed[candidate.benchmark.Group], candidateIndex,
		)
	}
	return nil
}

func (index *turboRepresentativeIndex) benchmarks() []TurboBenchmark {
	result := make([]TurboBenchmark, 0, len(index.representatives))
	for _, representative := range index.representatives {
		result = append(result, representative.benchmark)
	}
	return result
}

func turboMinHashes(program []byte) ([turboMinHashCount]uint64, bool) {
	var result [turboMinHashCount]uint64
	for i := range result {
		result[i] = math.MaxUint64
	}
	if len(program) < 2 {
		return result, false
	}

	values := make(maxUint64Heap, 0, turboMinHashCount)
	seen := make(map[uint64]struct{}, turboMinHashCount)
	for offset := 0; offset <= len(program)-2; offset++ {
		value := mixTurboHash(
			uint64(binary.LittleEndian.Uint16(program[offset : offset+2])),
		)
		if _, exists := seen[value]; exists {
			continue
		}
		if len(values) == turboMinHashCount && value >= values[0] {
			continue
		}
		if len(values) == turboMinHashCount {
			removed := heap.Pop(&values).(uint64)
			delete(seen, removed)
		}
		heap.Push(&values, value)
		seen[value] = struct{}{}
	}
	copy(result[:], values)
	sort.Slice(result[:len(values)], func(i, j int) bool {
		return result[i] < result[j]
	})
	return result, len(values) == turboMinHashCount
}

func mixTurboHash(value uint64) uint64 {
	value ^= value >> 30
	value *= 0xbf58476d1ce4e5b9
	value ^= value >> 27
	value *= 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func withinNinetyPercentSize(a, b int) bool {
	if a == 0 || b == 0 {
		return a == b
	}
	smaller, larger := min(a, b), max(a, b)
	return float64(smaller)/float64(larger) >= 0.90
}

func byteSimilarity(a, b []byte) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	if len(a) > len(b) {
		a, b = b, a
	}
	wordCount := (len(a) + 63) / 64
	positions := make([][]uint64, 256)
	for character := range positions {
		positions[character] = make([]uint64, wordCount)
	}
	for position, character := range a {
		positions[character][position/64] |= uint64(1) << uint(position%64)
	}
	state := make([]uint64, wordCount)
	for _, character := range b {
		carryShift := uint64(1)
		borrow := uint64(0)
		for wordIndex, stateWord := range state {
			x := positions[character][wordIndex] | stateWord
			y := stateWord<<1 | carryShift
			carryShift = stateWord >> 63
			difference := x - y
			borrowFromY := uint64(0)
			if x < y {
				borrowFromY = 1
			}
			differenceWithBorrow := difference - borrow
			borrowFromPrior := uint64(0)
			if difference < borrow {
				borrowFromPrior = 1
			}
			borrow = borrowFromY | borrowFromPrior
			state[wordIndex] = x &^ differenceWithBorrow
		}
	}
	var lcsLength int
	for _, word := range state {
		lcsLength += bits.OnesCount64(word)
	}
	return float64(lcsLength) / float64(max(len(a), len(b)))
}

func medianTurboCPU(samples []turboSample) float64 {
	values := make([]float64, len(samples))
	for i, sample := range samples {
		values[i] = float64(sample.duration.Nanoseconds())
	}
	return medianFloat64(values)
}

func medianTurboMemory(samples []turboSample) float64 {
	values := make([]float64, len(samples))
	for i, sample := range samples {
		values[i] = float64(sample.memBytes)
	}
	return medianFloat64(values)
}

func medianFloat64(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sortedValues := append([]float64(nil), values...)
	sort.Float64s(sortedValues)
	middle := len(sortedValues) / 2
	if len(sortedValues)%2 != 0 {
		return sortedValues[middle]
	}
	return (sortedValues[middle-1] + sortedValues[middle]) / 2
}

func withinTenPercent(a, b float64) bool {
	if a == 0 || b == 0 {
		return a == b
	}
	return math.Abs(a-b)/math.Max(a, b) <= 0.10
}

func turboWorstCases(benchmarks []TurboBenchmark, limit int) []TurboBenchmark {
	result := append([]TurboBenchmark(nil), benchmarks...)
	sort.Slice(result, func(i, j int) bool {
		return turboDeviationScore(result[i]) > turboDeviationScore(result[j])
	})
	if len(result) > limit {
		result = result[:limit]
	}
	return result
}

func turboDeviationScore(benchmark TurboBenchmark) float64 {
	score := 0.0
	if benchmark.BudgetCPU != nil && *benchmark.BudgetCPU > 0 {
		score = benchmark.CPUMaxNS / (float64(*benchmark.BudgetCPU) * turboCPUUnitNanosecond)
	}
	if benchmark.BudgetMem != nil && *benchmark.BudgetMem > 0 {
		memoryScore := benchmark.MemMaxBytes / float64(*benchmark.BudgetMem)
		if memoryScore > score {
			score = memoryScore
		}
	}
	return score
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
