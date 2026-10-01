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

// Package soak samples a running node's Prometheus metrics, fits growth
// trends over the plateau of a long run, and summarises repeated log
// messages, so a multi-day run can be judged without reading it by eye.
package soak

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// Metric names read from the node's /metrics endpoint. The node registers on
// the default Prometheus registerer, which carries the Go and process
// collectors, so these are present without any Dingo-specific wiring.
const (
	metricGoroutines = "go_goroutines"
	metricRSS        = "process_resident_memory_bytes"
	metricHeapAlloc  = "go_memstats_heap_alloc_bytes"
	metricHeapInuse  = "go_memstats_heap_inuse_bytes"
	metricGC         = "go_gc_duration_seconds"
	metricStart      = "process_start_time_seconds"
)

// Sample is one observation of the node's runtime metrics.
type Sample struct {
	Time       time.Time
	Goroutines float64
	RSSBytes   float64
	HeapAlloc  float64
	HeapInuse  float64
	// GCCount and GCSeconds are the cumulative GC cycle count and total pause
	// time since process start.
	GCCount   float64
	GCSeconds float64
	// ProcessStart is the process start time in Unix seconds, or 0 when the
	// endpoint does not expose it. A change between samples is a restart.
	ProcessStart float64
}

var csvHeader = []string{
	"time", "goroutines", "rss_bytes", "heap_alloc_bytes",
	"heap_inuse_bytes", "gc_count", "gc_seconds", "process_start_seconds",
}

// ParseMetrics extracts a Sample from Prometheus text exposition. It returns
// an error when a required series is missing, so a misconfigured target fails
// the first sample rather than producing a CSV of zeroes.
func ParseMetrics(r io.Reader, at time.Time) (Sample, error) {
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(r)
	if err != nil {
		return Sample{}, fmt.Errorf("parse metrics: %w", err)
	}
	s := Sample{Time: at}
	gauge := func(name string, dst *float64) error {
		fam, ok := families[name]
		if !ok || len(fam.GetMetric()) == 0 {
			return fmt.Errorf("metric %s not found", name)
		}
		m := fam.GetMetric()[0]
		switch {
		case m.GetGauge() != nil:
			*dst = m.GetGauge().GetValue()
		case m.GetCounter() != nil:
			*dst = m.GetCounter().GetValue()
		default:
			*dst = m.GetUntyped().GetValue()
		}
		return nil
	}
	for name, dst := range map[string]*float64{
		metricGoroutines: &s.Goroutines,
		metricRSS:        &s.RSSBytes,
		metricHeapAlloc:  &s.HeapAlloc,
		metricHeapInuse:  &s.HeapInuse,
	} {
		if err := gauge(name, dst); err != nil {
			return Sample{}, err
		}
	}
	gc, ok := families[metricGC]
	if !ok || len(gc.GetMetric()) == 0 ||
		gc.GetMetric()[0].GetSummary() == nil {
		return Sample{}, fmt.Errorf("metric %s not found", metricGC)
	}
	sum := gc.GetMetric()[0].GetSummary()
	s.GCCount = float64(sum.GetSampleCount())
	s.GCSeconds = sum.GetSampleSum()
	// Optional: without it, Analyse still sees a restart whose GC count drops.
	if _, ok := families[metricStart]; ok {
		if err := gauge(metricStart, &s.ProcessStart); err != nil {
			return Sample{}, err
		}
	}
	return s, nil
}

// Scrape fetches metricsURL and parses it into a Sample stamped with now.
func Scrape(
	ctx context.Context,
	client *http.Client,
	metricsURL string,
	now time.Time,
) (Sample, error) {
	body, err := get(ctx, client, metricsURL)
	if err != nil {
		return Sample{}, err
	}
	defer body.Close()
	return ParseMetrics(body, now)
}

func get(
	ctx context.Context,
	client *http.Client,
	url string,
) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	return resp.Body, nil
}

// WriteCSVHeader writes the column header. Call it once per file.
func WriteCSVHeader(w io.Writer) error {
	cw := csv.NewWriter(w)
	if err := cw.Write(csvHeader); err != nil {
		return err
	}
	cw.Flush()
	return cw.Error()
}

// WriteCSVRow appends one sample to a CSV started with WriteCSVHeader.
func WriteCSVRow(w io.Writer, s Sample) error {
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
	cw := csv.NewWriter(w)
	if err := cw.Write([]string{
		s.Time.UTC().Format(time.RFC3339Nano),
		f(s.Goroutines), f(s.RSSBytes), f(s.HeapAlloc), f(s.HeapInuse),
		f(s.GCCount), f(s.GCSeconds), f(s.ProcessStart),
	}); err != nil {
		return err
	}
	cw.Flush()
	return cw.Error()
}

// ReadCSV parses a file written by WriteCSVHeader and WriteCSVRow.
func ReadCSV(r io.Reader) ([]Sample, error) {
	cr := csv.NewReader(r)
	rows, err := cr.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 ||
		strings.Join(rows[0], ",") != strings.Join(csvHeader, ",") {
		return nil, errors.New("soak csv: missing or unexpected header")
	}
	out := make([]Sample, 0, len(rows)-1)
	for i, row := range rows[1:] {
		ts, err := time.Parse(time.RFC3339Nano, row[0])
		if err != nil {
			return nil, fmt.Errorf("soak csv row %d: %w", i+2, err)
		}
		s := Sample{Time: ts}
		for j, dst := range []*float64{
			&s.Goroutines, &s.RSSBytes, &s.HeapAlloc, &s.HeapInuse,
			&s.GCCount, &s.GCSeconds, &s.ProcessStart,
		} {
			v, err := strconv.ParseFloat(row[j+1], 64)
			if err != nil {
				return nil, fmt.Errorf("soak csv row %d: %w", i+2, err)
			}
			*dst = v
		}
		out = append(out, s)
	}
	return out, nil
}
