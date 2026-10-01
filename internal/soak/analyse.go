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

package soak

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

// Options tunes the plateau analysis. The zero value selects the defaults.
type Options struct {
	// WarmupFraction of the run, counted from the first sample, is excluded
	// from the fit: sync and cache fill legitimately grow memory and
	// goroutines. Default 0.25.
	WarmupFraction float64
	// MaxGrowthPercentPerHour is the largest tolerated fitted growth of
	// goroutines or RSS, as a percentage of the fitted value at the start of
	// the plateau per hour. Default 1.
	MaxGrowthPercentPerHour float64
	// MinR2 is the least coefficient of determination for growth to count as
	// sustained. A noisy series with a positive slope but a poor fit is a
	// sawtooth, not a leak. Default 0.5.
	MinR2 float64
	// MinSamples is the fewest plateau samples needed to judge. Default 10.
	MinSamples int
}

func (o Options) withDefaults() Options {
	if o.WarmupFraction <= 0 {
		o.WarmupFraction = 0.25
	}
	if o.MaxGrowthPercentPerHour <= 0 {
		o.MaxGrowthPercentPerHour = 1
	}
	if o.MinR2 <= 0 {
		o.MinR2 = 0.5
	}
	if o.MinSamples <= 0 {
		o.MinSamples = 10
	}
	return o
}

// Trend is the least-squares fit of one metric over the plateau window.
type Trend struct {
	Name string
	// SlopePerHour is the fitted change per hour in the metric's own unit.
	SlopePerHour float64
	// GrowthPercentPerHour is SlopePerHour relative to the fitted start value.
	GrowthPercentPerHour float64
	R2                   float64
	// Sustained is true when growth exceeds the limit with a fit good enough
	// to call it a trend.
	Sustained bool
}

// Report is the outcome of Analyse.
type Report struct {
	PlateauSamples int
	PlateauHours   float64
	Trends         []Trend
	// GCCyclesPerHour and GCMeanPauseSeconds describe GC pressure across the
	// plateau; they are reported but do not gate the result.
	GCCyclesPerHour    float64
	GCMeanPauseSeconds float64
	// Restarts counts decreases of the cumulative GC counter, which mean the
	// process restarted and the run is not one continuous soak.
	Restarts int
}

// Failed reports whether any gated trend shows sustained growth or the
// process restarted during the run.
func (r Report) Failed() bool {
	if r.Restarts > 0 {
		return true
	}
	for _, t := range r.Trends {
		if t.Sustained {
			return true
		}
	}
	return false
}

// ErrInsufficientData is returned when the plateau holds too few samples.
var ErrInsufficientData = errors.New("soak: not enough plateau samples")

// Analyse fits goroutine and RSS growth over the plateau of samples, which
// must be in time order.
func Analyse(samples []Sample, opts Options) (Report, error) {
	opts = opts.withDefaults()
	if len(samples) < 2 {
		return Report{}, ErrInsufficientData
	}
	if !sort.SliceIsSorted(samples, func(i, j int) bool {
		return samples[i].Time.Before(samples[j].Time)
	}) {
		return Report{}, errors.New("soak: samples are not in time order")
	}
	var rep Report
	for i := 1; i < len(samples); i++ {
		if samples[i].GCCount < samples[i-1].GCCount {
			rep.Restarts++
		}
	}
	first, last := samples[0].Time, samples[len(samples)-1].Time
	cut := first.Add(time.Duration(float64(last.Sub(first)) * opts.WarmupFraction))
	start := sort.Search(len(samples), func(i int) bool {
		return !samples[i].Time.Before(cut)
	})
	plateau := samples[start:]
	if len(plateau) < opts.MinSamples {
		return rep, fmt.Errorf(
			"%w: have %d, need %d", ErrInsufficientData, len(plateau), opts.MinSamples,
		)
	}
	rep.PlateauSamples = len(plateau)
	rep.PlateauHours = plateau[len(plateau)-1].Time.Sub(plateau[0].Time).Hours()
	if rep.PlateauHours <= 0 {
		return rep, ErrInsufficientData
	}
	for _, m := range []struct {
		name string
		get  func(Sample) float64
	}{
		{"goroutines", func(s Sample) float64 { return s.Goroutines }},
		{"rss_bytes", func(s Sample) float64 { return s.RSSBytes }},
	} {
		rep.Trends = append(rep.Trends, fit(m.name, plateau, m.get, opts))
	}
	// Deltas are taken within the plateau only; a restart inside it makes the
	// figures meaningless but Restarts already fails the report.
	dc := plateau[len(plateau)-1].GCCount - plateau[0].GCCount
	ds := plateau[len(plateau)-1].GCSeconds - plateau[0].GCSeconds
	if dc > 0 {
		rep.GCCyclesPerHour = dc / rep.PlateauHours
		rep.GCMeanPauseSeconds = ds / dc
	}
	return rep, nil
}

func fit(
	name string,
	plateau []Sample,
	get func(Sample) float64,
	opts Options,
) Trend {
	t0 := plateau[0].Time
	n := float64(len(plateau))
	var sx, sy, sxx, sxy, syy float64
	for _, s := range plateau {
		x := s.Time.Sub(t0).Hours()
		y := get(s)
		sx += x
		sy += y
		sxx += x * x
		sxy += x * y
		syy += y * y
	}
	tr := Trend{Name: name}
	den := n*sxx - sx*sx
	if den == 0 {
		return tr
	}
	slope := (n*sxy - sx*sy) / den
	intercept := (sy - slope*sx) / n
	tr.SlopePerHour = slope
	if intercept > 0 {
		tr.GrowthPercentPerHour = slope / intercept * 100
	}
	if vy := n*syy - sy*sy; vy > 0 {
		cov := n*sxy - sx*sy
		tr.R2 = cov * cov / (den * vy)
	}
	tr.Sustained = tr.GrowthPercentPerHour > opts.MaxGrowthPercentPerHour &&
		tr.R2 >= opts.MinR2
	return tr
}
