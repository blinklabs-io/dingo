// Copyright 2026 Blink Labs Software

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/blinklabs-io/dingo/internal/cborconformance"
)

func main() {
	datasetDir := flag.String("dataset", "", "path to the Conway corpus directory")
	outputPath := flag.String("output", "", "path for the JSON report")
	flag.Parse()

	if *datasetDir == "" || *outputPath == "" {
		flag.Usage()
		os.Exit(2)
	}

	report, err := cborconformance.GenerateReport(*datasetDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := cborconformance.WriteReport(*outputPath, report); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Fprintf(
		os.Stderr,
		"wrote %s: %d failures across %d generated samples\n",
		*outputPath,
		len(report.Failures),
		report.Totals.GeneratedTotal,
	)
	if !report.Successful {
		os.Exit(1)
	}
}
