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
	corpusPath := flag.String("corpus", "", "path to the UPLC conformance corpus")
	outputPath := flag.String("output", "", "path for the JSON report")
	flag.Parse()
	if *corpusPath == "" || *outputPath == "" {
		flag.Usage()
		os.Exit(2)
	}

	report, err := uplcconformance.GenerateReport(*corpusPath, time.Now())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := uplcconformance.WriteReport(*outputPath, report); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Fprintf(
		os.Stderr,
		"wrote %s: %d failures across %d vectors (evaluation %d/%d, budget %d/%d)\n",
		*outputPath,
		report.Totals.Failed,
		report.Totals.Total,
		report.Totals.Evaluation.Passed,
		report.Totals.Evaluation.Total,
		report.Totals.Budget.Passed,
		report.Totals.Budget.Total,
	)
	if report.Totals.Failed > 0 {
		os.Exit(1)
	}
}
