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

// soak samples a running Dingo node's runtime metrics, analyses the samples
// for sustained goroutine or RSS growth, and summarises repeated WARN and
// ERROR log messages. See docs/soak.md.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/blinklabs-io/dingo/internal/soak"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

const usage = "usage: soak sample|analyse|logs [flags]"

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	var err error
	switch args[0] {
	case "sample":
		err = runSample(ctx, args[1:], stdout)
	case "analyse":
		err = runAnalyse(args[1:], stdout)
	case "logs":
		err = runLogs(args[1:], stdout)
	default:
		fmt.Fprintln(stderr, usage)
		return 2
	}
	switch {
	case err == nil:
		return 0
	case errors.Is(err, errSoakFailed):
		fmt.Fprintln(stderr, err)
		return 1
	case errors.Is(err, flag.ErrHelp):
		return 2
	default:
		fmt.Fprintln(stderr, "soak:", err)
		return 2
	}
}

var errSoakFailed = errors.New("soak: sustained growth or restart detected")

func runSample(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("sample", flag.ContinueOnError)
	metrics := fs.String("metrics-url", "http://127.0.0.1:12798/metrics", "node /metrics URL")
	debug := fs.String("debug-url", "", "pprof listener base URL; enables profile snapshots")
	snapDir := fs.String("snapshot-dir", "soak-pprof", "directory for pprof snapshots")
	snapEvery := fs.Int("snapshot-every", 60, "take a snapshot every N samples")
	interval := fs.Duration("interval", time.Minute, "sampling interval")
	duration := fs.Duration("duration", 0, "stop after this long (0 runs until interrupted)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *interval <= 0 || *snapEvery <= 0 {
		return errors.New("interval and snapshot-every must be positive")
	}
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}
	client := &http.Client{Timeout: 30 * time.Second}
	if err := soak.WriteCSVHeader(stdout); err != nil {
		return err
	}
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	for n := 0; ; n++ {
		now := time.Now()
		s, err := soak.Scrape(ctx, client, *metrics, now)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// A transient scrape failure must not end a multi-day run; the
			// gap shows in the CSV timestamps.
			fmt.Fprintln(os.Stderr, "soak: sample failed:", err)
		} else if err := soak.WriteCSVRow(stdout, s); err != nil {
			return err
		}
		if *debug != "" && n%*snapEvery == 0 {
			if err := soak.Snapshot(ctx, client, *debug, *snapDir, now); err != nil && ctx.Err() == nil {
				fmt.Fprintln(os.Stderr, "soak: snapshot failed:", err)
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func runAnalyse(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("analyse", flag.ContinueOnError)
	file := fs.String("csv", "", "CSV written by `soak sample`")
	warmup := fs.Float64("warmup", 0.25, "fraction of the run excluded as warmup")
	maxGrowth := fs.Float64("max-growth-percent-per-hour", 1, "tolerated fitted growth per hour")
	minR2 := fs.Float64("min-r2", 0.5, "least R^2 for growth to count as sustained")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *file == "" {
		return errors.New("analyse: -csv is required")
	}
	f, err := os.Open(*file)
	if err != nil {
		return err
	}
	defer f.Close()
	samples, err := soak.ReadCSV(f)
	if err != nil {
		return err
	}
	rep, err := soak.Analyse(samples, soak.Options{
		WarmupFraction:          *warmup,
		MaxGrowthPercentPerHour: *maxGrowth,
		MinR2:                   *minR2,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "plateau: %d samples over %.2fh\n", rep.PlateauSamples, rep.PlateauHours)
	for _, t := range rep.Trends {
		verdict := "ok"
		if t.Sustained {
			verdict = "GROWING"
		}
		fmt.Fprintf(stdout, "%-12s slope=%.4g/h growth=%.3f%%/h r2=%.2f %s\n",
			t.Name, t.SlopePerHour, t.GrowthPercentPerHour, t.R2, verdict)
	}
	fmt.Fprintf(stdout, "gc: %.1f cycles/h, mean pause %.6fs\n",
		rep.GCCyclesPerHour, rep.GCMeanPauseSeconds)
	fmt.Fprintf(stdout, "restarts: %d\n", rep.Restarts)
	if rep.Failed() {
		return errSoakFailed
	}
	return nil
}

func runLogs(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("logs", flag.ContinueOnError)
	min := fs.Int("min-count", 10, "report messages seen at least this often")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var in io.Reader = os.Stdin
	if fs.NArg() > 0 {
		f, err := os.Open(fs.Arg(0))
		if err != nil {
			return err
		}
		defer f.Close()
		in = f
	}
	rep, err := soak.SummariseLog(in, *min)
	if err != nil {
		return err
	}
	for _, r := range rep {
		fmt.Fprintf(stdout, "%7d %-5s %s\n", r.Count, r.Level, r.Message)
	}
	return nil
}
