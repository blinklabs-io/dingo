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

// Command benchcheck compares two `go test -bench` result files (produced
// by `make bench-ci`) via internal/benchci and writes a markdown regression
// report. It is invoked by the weekly benchmark.yml workflow.
//
// Usage:
//
//	benchcheck <old-file> <new-file> <report-output-path>
//
// benchcheck always exits 0 when it successfully compares the two files and
// writes the report, regardless of whether a regression was found --
// regression is data for the caller to act on, not a program failure. It
// prints exactly one of "regression=true" or "regression=false" as its
// final stdout line so the calling shell/workflow step can capture it. A
// genuine I/O or parse error is reported on stderr with a non-zero exit.
package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/blinklabs-io/dingo/internal/benchci"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "benchcheck: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 3 {
		return errors.New(
			"usage: benchcheck <old-file> <new-file> <report-output-path>",
		)
	}
	oldFile, newFile, reportPath := args[0], args[1], args[2]

	report, regressed, err := benchci.Compare(
		oldFile,
		newFile,
		benchci.TrackedBenchmarks,
	)
	if err != nil {
		return fmt.Errorf("comparing %s to %s: %w", oldFile, newFile, err)
	}

	// reportPath is a CLI argument supplied by the trusted benchmark
	// workflow invoking this command, not attacker-controlled input.
	if err := os.WriteFile(reportPath, []byte(report), 0o600); err != nil { //nolint:gosec // G703: reportPath is a trusted CLI argument, not attacker input
		return fmt.Errorf("writing report to %s: %w", reportPath, err)
	}

	fmt.Printf("regression=%t\n", regressed)
	return nil
}
