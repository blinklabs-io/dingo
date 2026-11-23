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

// Command benchgate compares the base-revision and head-revision result
// files written by .github/scripts/bench-gate.sh and writes a markdown
// report. It is the per-pull-request benchmark gate.
//
// Usage:
//
//	benchgate <base-file> <head-file> <report-output-path>
//
// Exit status is 0 when no gate benchmark regressed, 1 when at least one did
// or is missing from the head file, and 2 on a usage, I/O, or comparison error. The report is written before
// exiting 1 so the workflow can publish it.
package main

import (
	"fmt"
	"os"

	"github.com/blinklabs-io/dingo/internal/benchci"
)

const (
	exitRegression = 1
	exitError      = 2
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) != 3 {
		fmt.Fprintln(
			os.Stderr,
			"usage: benchgate <base-file> <head-file> <report-output-path>",
		)
		return exitError
	}
	baseFile, headFile, reportPath := args[0], args[1], args[2]

	report, regressed, err := benchci.CompareGate(baseFile, headFile)
	if err != nil {
		fmt.Fprintf(
			os.Stderr,
			"benchgate: comparing %s to %s: %v\n",
			baseFile, headFile, err,
		)
		return exitError
	}
	// reportPath is a CLI argument supplied by the trusted gate workflow, not
	// attacker-controlled input.
	if err := os.WriteFile(reportPath, []byte(report), 0o600); err != nil { //nolint:gosec // G703: reportPath is a trusted CLI argument
		fmt.Fprintf(os.Stderr, "benchgate: writing %s: %v\n", reportPath, err)
		return exitError
	}
	if regressed {
		fmt.Printf(
			"regression: a gate benchmark is significantly slower by more than %.0f%%, or missing from head\n",
			benchci.GateRegressionThresholdPercent,
		)
		return exitRegression
	}
	fmt.Println("no gate regression")
	return 0
}
