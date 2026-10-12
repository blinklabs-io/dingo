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

package integration

import (
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Scale-benchmark environment knobs. Every benchmark falls back to a small
// default so a plain "go test -bench=." stays fast; the bench-storage-scale
// target and the runbook in DATABASE.md raise them.
const (
	envBenchScale        = "DINGO_BENCH_SCALE"
	envBenchBlockBytes   = "DINGO_BENCH_BLOCK_BYTES"
	envBenchBlocks       = "DINGO_BENCH_BLOCKS"
	envBenchDataDir      = "DINGO_BENCH_DATADIR"
	envBenchLatencySamps = "DINGO_BENCH_LATENCY_SAMPLES"
)

// parseScales parses a comma-separated list of positive counts. Each entry
// may carry a k, m or b suffix (thousand, million, billion).
func parseScales(raw string) ([]int, error) {
	var out []int
	for part := range strings.SplitSeq(raw, ",") {
		part = strings.TrimSpace(part)
		mult := 1
		switch {
		case strings.HasSuffix(part, "k"):
			mult = 1_000
		case strings.HasSuffix(part, "m"):
			mult = 1_000_000
		case strings.HasSuffix(part, "b"):
			mult = 1_000_000_000
		}
		if mult != 1 {
			part = part[:len(part)-1]
		}
		n, err := strconv.Atoi(part)
		if err != nil || n <= 0 || n > math.MaxInt/mult {
			return nil, fmt.Errorf("invalid scale %q in %q", part, raw)
		}
		out = append(out, n*mult)
	}
	return out, nil
}

// envScales returns the scales in the named variable, or def when unset.
// A set but invalid value is an error rather than a silent fall back, so a
// mistyped mainnet-scale run cannot pass as a small one.
func envScales(
	getenv func(string) string,
	name string,
	def []int,
) ([]int, error) {
	raw := getenv(name)
	if raw == "" {
		return def, nil
	}
	return parseScales(raw)
}

// envInt returns the positive integer in the named variable, or def when
// unset.
func envInt(getenv func(string) string, name string, def int) (int, error) {
	raw := getenv(name)
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid %s=%q", name, raw)
	}
	return n, nil
}

func envIntAtLeast(
	getenv func(string) string,
	name string,
	def, minimum int,
) (int, error) {
	n, err := envInt(getenv, name, def)
	if err != nil {
		return 0, err
	}
	if n < minimum {
		return 0, fmt.Errorf("invalid %s=%q: must be >= %d", name, getenv(name), minimum)
	}
	return n, nil
}

// percentile returns the nearest-rank p-th percentile (0 < p <= 100) of
// samples, which it sorts in place. It returns 0 for no samples.
func percentile(samples []time.Duration, p float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	slices.Sort(samples)
	rank := int(p/100*float64(len(samples)) + 0.999999999)
	rank = min(max(rank, 1), len(samples))
	return samples[rank-1]
}

// latencyRecorder keeps at most limit samples so a long run cannot grow
// without bound; later samples are dropped, not averaged in.
type latencyRecorder struct {
	limit   int
	samples []time.Duration
}

func newLatencyRecorder(limit int) *latencyRecorder {
	return &latencyRecorder{limit: limit}
}

func (r *latencyRecorder) record(d time.Duration) {
	if len(r.samples) < r.limit {
		r.samples = append(r.samples, d)
	}
}

// report publishes p50, p95, p99 and max as benchmark metrics.
func (r *latencyRecorder) report(b *testing.B, prefix string) {
	b.Helper()
	for _, q := range []struct {
		name string
		p    float64
	}{{"p50", 50}, {"p95", 95}, {"p99", 99}, {"max", 100}} {
		b.ReportMetric(
			float64(percentile(r.samples, q.p).Nanoseconds()),
			prefix+"-"+q.name+"-ns",
		)
	}
}

// rssBytes returns the process resident set size. ok is false where the
// platform does not expose it.
func rssBytes() (n uint64, ok bool) {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, false
	}
	return parseVmRSS(string(data))
}

// parseVmRSS extracts the VmRSS line of a /proc/<pid>/status document.
func parseVmRSS(status string) (uint64, bool) {
	for line := range strings.SplitSeq(status, "\n") {
		rest, found := strings.CutPrefix(line, "VmRSS:")
		if !found {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) != 2 || fields[1] != "kB" {
			return 0, false
		}
		kb, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return 0, false
		}
		return kb * 1024, true
	}
	return 0, false
}
