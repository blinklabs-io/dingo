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
	corpusID := flag.String(
		"corpus-id",
		"amaru-uplc/turbo",
		"identifier for the benchmark corpus",
	)
	corpusHash := flag.String(
		"corpus-digest",
		"",
		"SHA-256 digest of the downloaded corpus archive",
	)
	implementationVersion := flag.String(
		"version",
		"unknown",
		"Dingo implementation version",
	)
	outputPath := flag.String("output", "", "path for the JSON report")
	samples := flag.Int("samples", 10, "number of timed samples per program")
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

	report, err := uplcconformance.GenerateTurboReport(
		*corpusPath,
		*corpusID,
		*corpusHash,
		*implementationVersion,
		*samples,
		*maxPrograms,
		time.Now(),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := uplcconformance.WriteTurboReport(*outputPath, report); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Fprintf(
		os.Stderr,
		"wrote %s: %d programs across %d groups\n",
		*outputPath,
		len(report.Benchmarks),
		len(report.Summary),
	)
}
