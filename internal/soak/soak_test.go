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

package soak_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/soak"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// series builds n samples one minute apart with goroutines and RSS given by
// the supplied functions of the sample index.
func series(n int, g, rss func(i int) float64) []soak.Sample {
	out := make([]soak.Sample, n)
	for i := range out {
		out[i] = soak.Sample{
			Time:       t0.Add(time.Duration(i) * time.Minute),
			Goroutines: g(i),
			RSSBytes:   rss(i),
			GCCount:    float64(i * 2),
			GCSeconds:  float64(i) * 0.002,
		}
	}
	return out
}

func TestAnalyseFlatPlateauWithWarmupPasses(t *testing.T) {
	t.Parallel()
	// Steep growth during the first half, then flat: the warmup is excluded.
	grow := func(i int) float64 {
		if i < 50 {
			return 100 + float64(i)*40
		}
		return 2060
	}
	rep, err := soak.Analyse(series(100, grow, grow), soak.Options{WarmupFraction: 0.5})
	require.NoError(t, err)
	assert.False(t, rep.Failed(), "%+v", rep.Trends)
}

func TestAnalyseSustainedGoroutineGrowthFails(t *testing.T) {
	t.Parallel()
	rep, err := soak.Analyse(series(100,
		func(i int) float64 { return 1000 + float64(i)*5 },
		func(int) float64 { return 1e9 },
	), soak.Options{})
	require.NoError(t, err)
	require.True(t, rep.Failed())
	assert.True(t, rep.Trends[0].Sustained, "goroutines should be flagged")
	assert.False(t, rep.Trends[1].Sustained, "flat RSS must not be flagged")
	assert.Equal(t, "goroutines", rep.Trends[0].Name)
}

func TestAnalyseSustainedRSSGrowthFails(t *testing.T) {
	t.Parallel()
	rep, err := soak.Analyse(series(100,
		func(int) float64 { return 500 },
		func(i int) float64 { return 1e9 + float64(i)*5e7 },
	), soak.Options{})
	require.NoError(t, err)
	require.True(t, rep.Failed())
	assert.True(t, rep.Trends[1].Sustained)
	assert.False(t, rep.Trends[0].Sustained)
}

func TestAnalyseSawtoothIsNotSustained(t *testing.T) {
	t.Parallel()
	// Repeating ramp: the fitted slope is not meaningfully positive and the
	// fit is poor, so it is not a leak.
	saw := func(i int) float64 { return 1000 + float64(i%10)*100 }
	rep, err := soak.Analyse(series(200, saw, saw), soak.Options{})
	require.NoError(t, err)
	assert.False(t, rep.Failed(), "%+v", rep.Trends)
}

func TestAnalyseNoisyPositiveSlopeBelowR2IsNotSustained(t *testing.T) {
	t.Parallel()
	// Slope is above the limit but the fit is dominated by noise.
	noisy := func(i int) float64 {
		return 1000 + float64(i)*0.3 + float64((i*7919)%400)
	}
	rep, err := soak.Analyse(series(100, noisy, func(int) float64 { return 1 }),
		soak.Options{MaxGrowthPercentPerHour: 0.5})
	require.NoError(t, err)
	assert.Greater(t, rep.Trends[0].GrowthPercentPerHour, 0.5)
	assert.Less(t, rep.Trends[0].R2, 0.5)
	assert.False(t, rep.Trends[0].Sustained)
}

func TestAnalyseTooFewSamples(t *testing.T) {
	t.Parallel()
	_, err := soak.Analyse(series(5,
		func(int) float64 { return 1 }, func(int) float64 { return 1 }),
		soak.Options{})
	require.Error(t, err)
	assert.True(t, errors.Is(err, soak.ErrInsufficientData))
}

func TestAnalyseRestartFails(t *testing.T) {
	t.Parallel()
	s := series(100, func(int) float64 { return 1 }, func(int) float64 { return 1 })
	for i := 60; i < len(s); i++ {
		s[i].GCCount = float64(i - 60)
	}
	rep, err := soak.Analyse(s, soak.Options{})
	require.NoError(t, err)
	assert.Equal(t, 1, rep.Restarts)
	assert.True(t, rep.Failed())
}

func TestAnalyseReportsGCPressure(t *testing.T) {
	t.Parallel()
	rep, err := soak.Analyse(series(100,
		func(int) float64 { return 1 }, func(int) float64 { return 1 }),
		soak.Options{})
	require.NoError(t, err)
	// Two cycles per minute, one millisecond mean pause.
	assert.InDelta(t, 120, rep.GCCyclesPerHour, 0.001)
	assert.InDelta(t, 0.001, rep.GCMeanPauseSeconds, 1e-9)
}

const exposition = `# TYPE go_goroutines gauge
go_goroutines 42
# TYPE process_resident_memory_bytes gauge
process_resident_memory_bytes 1.048576e+08
# TYPE go_memstats_heap_alloc_bytes gauge
go_memstats_heap_alloc_bytes 5e+07
# TYPE go_memstats_heap_inuse_bytes gauge
go_memstats_heap_inuse_bytes 6e+07
# TYPE go_gc_duration_seconds summary
go_gc_duration_seconds{quantile="0"} 0.0001
go_gc_duration_seconds{quantile="1"} 0.003
go_gc_duration_seconds_sum 0.75
go_gc_duration_seconds_count 300
`

func TestParseMetrics(t *testing.T) {
	t.Parallel()
	s, err := soak.ParseMetrics(strings.NewReader(exposition), t0)
	require.NoError(t, err)
	assert.Equal(t, soak.Sample{
		Time: t0, Goroutines: 42, RSSBytes: 104857600, HeapAlloc: 5e7,
		HeapInuse: 6e7, GCCount: 300, GCSeconds: 0.75,
	}, s)
}

func TestParseMetricsMissingSeriesFails(t *testing.T) {
	t.Parallel()
	_, err := soak.ParseMetrics(strings.NewReader("go_goroutines 1\n"), t0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestCSVRoundTrip(t *testing.T) {
	t.Parallel()
	in := series(3, func(i int) float64 { return float64(i) + 0.5 },
		func(i int) float64 { return 1e9 + float64(i) })
	var buf bytes.Buffer
	require.NoError(t, soak.WriteCSVHeader(&buf))
	for _, s := range in {
		require.NoError(t, soak.WriteCSVRow(&buf, s))
	}
	out, err := soak.ReadCSV(&buf)
	require.NoError(t, err)
	assert.Equal(t, in, out)
}

func TestReadCSVRejectsForeignHeader(t *testing.T) {
	t.Parallel()
	_, err := soak.ReadCSV(strings.NewReader("a,b\n1,2\n"))
	require.Error(t, err)
}

func TestScrape(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, exposition)
		}))
	defer srv.Close()
	s, err := soak.Scrape(context.Background(), srv.Client(), srv.URL, t0)
	require.NoError(t, err)
	assert.Equal(t, 42.0, s.Goroutines)

	bad := httptest.NewServer(http.NotFoundHandler())
	defer bad.Close()
	_, err = soak.Scrape(context.Background(), bad.Client(), bad.URL, t0)
	require.Error(t, err)
}

func TestSummariseLogCollapsesVariableFragments(t *testing.T) {
	t.Parallel()
	log := strings.Join([]string{
		`{"time":"x","level":"WARN","msg":"peer 10.0.0.1:3001 timed out after 5s"}`,
		`{"time":"x","level":"WARN","msg":"peer 10.0.0.9:3001 timed out after 12s"}`,
		`time=x level=WARN msg="peer 10.1.1.1:3001 timed out after 7s"`,
		`{"time":"x","level":"ERROR","msg":"block abcdef0123456789abcdef0123456789 rejected"}`,
		`{"time":"x","level":"ERROR","msg":"block 0123456789abcdef0123456789abcdef rejected"}`,
		`{"time":"x","level":"INFO","msg":"peer connected"}`,
		`{"time":"x","level":"INFO","msg":"peer connected"}`,
		`{"time":"x","level":"WARN","msg":"seen once"}`,
		`not a log line`,
	}, "\n")
	got, err := soak.SummariseLog(strings.NewReader(log), 2)
	require.NoError(t, err)
	assert.Equal(t, []soak.Repeated{
		{Level: "WARN", Message: "peer <addr> timed out after <n>", Count: 3},
		{Level: "ERROR", Message: "block <hex> rejected", Count: 2},
	}, got)
}

func TestSnapshotWritesGoroutineAndHeapProfiles(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, "profile for "+r.URL.Path)
		}))
	defer srv.Close()
	dir := t.TempDir()
	require.NoError(t, soak.Snapshot(context.Background(), srv.Client(), srv.URL, dir, t0))
	g, err := os.ReadFile(filepath.Join(dir, "goroutine-20260101T000000Z.pprof"))
	require.NoError(t, err)
	assert.Equal(t, "profile for /debug/pprof/goroutine", string(g))
	_, err = os.Stat(filepath.Join(dir, "heap-20260101T000000Z.pprof"))
	require.NoError(t, err)
}
