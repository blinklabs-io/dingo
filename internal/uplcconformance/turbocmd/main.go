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
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/blinklabs-io/dingo/internal/uplcconformance"
)

func main() {
	corpusPath := flag.String("corpus", "", "path to the UPLC turbo corpus")
	outputPath := flag.String("output", "", "path for the JSON report")
	samples := flag.Int("samples", 50, "number of timed samples per program")
	maxPrograms := flag.Int(
		"max-programs",
		0,
		"measure only this many evenly spaced programs; 0 measures all",
	)
	flag.Parse()
	if *corpusPath == "" || *outputPath == "" {
		flag.Usage()
		os.Exit(2)
	}

	if err := generateReport(*corpusPath, *outputPath, *samples, *maxPrograms); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func generateReport(corpusPath, outputPath string, samples, maxPrograms int) error {
	report, err := uplcconformance.GenerateTurboReport(
		corpusPath,
		samples,
		maxPrograms,
		time.Now(),
	)
	if report.Scenario != "" {
		if writeErr := uplcconformance.WriteTurboReport(outputPath, report); writeErr != nil {
			return writeErr
		}
	}
	if err != nil {
		return err
	}
	fmt.Fprintf(
		os.Stderr,
		"wrote %s: %d programs across %d groups\n",
		outputPath,
		len(report.Benchmarks),
		len(report.Summary),
	)
	return nil
}
