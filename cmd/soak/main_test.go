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

package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/soak"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeCSV(t *testing.T, goroutines func(i int) float64) string {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, soak.WriteCSVHeader(&buf))
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range 100 {
		require.NoError(t, soak.WriteCSVRow(&buf, soak.Sample{
			Time:       base.Add(time.Duration(i) * time.Minute),
			Goroutines: goroutines(i),
			RSSBytes:   1e9,
			GCCount:    float64(i),
		}))
	}
	p := filepath.Join(t.TempDir(), "soak.csv")
	require.NoError(t, os.WriteFile(p, buf.Bytes(), 0o600))
	return p
}

func TestAnalyseExitCodes(t *testing.T) {
	t.Parallel()
	flat := writeCSV(t, func(int) float64 { return 500 })
	leak := writeCSV(t, func(i int) float64 { return 500 + float64(i)*5 })

	var out, errb bytes.Buffer
	assert.Equal(
		t,
		0,
		run(
			context.Background(),
			[]string{"analyse", "-csv", flat},
			&out,
			&errb,
		),
	)
	assert.Contains(t, out.String(), "goroutines")

	out.Reset()
	errb.Reset()
	assert.Equal(
		t,
		1,
		run(
			context.Background(),
			[]string{"analyse", "-csv", leak},
			&out,
			&errb,
		),
	)
	assert.Contains(t, out.String(), "GROWING")

	assert.Equal(
		t,
		2,
		run(context.Background(), []string{"analyse"}, &out, &errb),
	)
	assert.Equal(t, 2, run(context.Background(), nil, &out, &errb))
}

func TestLogsSubcommand(t *testing.T) {
	t.Parallel()
	p := filepath.Join(t.TempDir(), "node.log")
	require.NoError(t, os.WriteFile(p, []byte(strings.Repeat(
		`{"level":"WARN","msg":"slow peer 10.0.0.1"}`+"\n", 3)), 0o600))
	var out, errb bytes.Buffer
	assert.Equal(
		t,
		0,
		run(
			context.Background(),
			[]string{"logs", "-min-count", "2", p},
			&out,
			&errb,
		),
	)
	assert.Contains(t, out.String(), "slow peer <addr>")
}

func TestAnalyseRejectsOutOfRangeFlags(t *testing.T) {
	t.Parallel()
	flat := writeCSV(t, func(int) float64 { return 500 })
	for _, args := range [][]string{
		{"-warmup", "0"},
		{"-warmup", "1"},
		{"-min-r2", "0"},
		{"-min-r2", "1.5"},
		{"-max-growth-percent-per-hour", "0"},
	} {
		var out, errb bytes.Buffer
		code := run(context.Background(),
			append([]string{"analyse", "-csv", flat}, args...), &out, &errb)
		assert.Equal(t, 2, code, "%v: %s", args, errb.String())
	}
}

const exposition = `go_goroutines 42
process_resident_memory_bytes 1e+08
go_memstats_heap_alloc_bytes 5e+07
go_memstats_heap_inuse_bytes 6e+07
# TYPE go_gc_duration_seconds summary
go_gc_duration_seconds_sum 0.75
go_gc_duration_seconds_count 300
`

func TestSampleContinuesAfterScrapeFailure(t *testing.T) {
	t.Parallel()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if hits.Add(1) == 1 {
				http.Error(w, "down", http.StatusServiceUnavailable)
				return
			}
			fmt.Fprint(w, exposition)
		}))
	defer srv.Close()
	var out, errb bytes.Buffer
	code := run(context.Background(), []string{"sample",
		"-metrics-url", srv.URL, "-interval", "10ms", "-duration", "300ms",
	}, &out, &errb)
	require.Equal(t, 0, code, errb.String())
	assert.Contains(t, errb.String(), "sample failed")
	samples, err := soak.ReadCSV(&out)
	require.NoError(t, err)
	assert.NotEmpty(t, samples, "scrapes after the failure must be recorded")
}

func TestSampleSnapshotsEveryNthSample(t *testing.T) {
	t.Parallel()
	var scrapes, snaps atomic.Int64
	metrics := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			scrapes.Add(1)
			fmt.Fprint(w, exposition)
		}))
	defer metrics.Close()
	debug := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/debug/pprof/goroutine") {
				snaps.Add(1)
			}
			fmt.Fprint(w, "profile")
		}))
	defer debug.Close()
	var out, errb bytes.Buffer
	code := run(context.Background(), []string{"sample",
		"-metrics-url", metrics.URL, "-debug-url", debug.URL,
		"-snapshot-dir", t.TempDir(), "-snapshot-every", "3",
		"-interval", "10ms", "-duration", "300ms",
	}, &out, &errb)
	require.Equal(t, 0, code, errb.String())
	n := scrapes.Load()
	require.GreaterOrEqual(t, n, int64(4))
	// Samples 0, 3, 6, ... snapshot; the last may be cut off by the deadline.
	want := (n + 2) / 3
	assert.GreaterOrEqual(t, snaps.Load(), want-1)
	assert.LessOrEqual(t, snaps.Load(), want)
	assert.NotContains(t, errb.String(), "snapshot failed")
}
