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

// convert-amaru-vector walks the Amaru ledger-conformance corpus
// shipped by ouroboros-mock and emits new-format JSON equivalents
// under internal/test/consensus/testdata/converted/. Mechanical
// rewrapping only — no semantic translation. See plan §3 for the
// per-event mapping table.
//
// Usage:
//
//	convert-amaru-vector [-in <path>] [-out <path>] [-fail-on-mismatch]
//
// Defaults: -in extracts ouroboros-mock's embedded corpus to a temp
// dir (the dependency is linked into this binary at build time, so
// no external setup is needed); -out defaults to
// internal/test/consensus/testdata/converted/.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/blinklabs-io/dingo/internal/test/consensus"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
)

const (
	defaultOutDir = "internal/test/consensus/testdata/converted"
	// amaruCorpusSubpath is appended to the resolved ouroboros-mock
	// testdata root to form the default -in path.
	amaruCorpusSubpath = "eras/conway"
)

func main() {
	in := flag.String("in", "",
		"Amaru corpus root (default: ouroboros-mock embedded corpus)")
	out := flag.String("out", defaultOutDir,
		"output root for converted vectors")
	failOnMismatch := flag.Bool("fail-on-mismatch", false,
		"treat any per-vector conversion error as fatal (default: log and continue)")
	flag.Parse()

	inPath := *in
	if inPath == "" {
		resolved, err := resolveDefaultInputPath()
		if err != nil {
			log.Fatalf(
				"convert-amaru-vector: -in not set and default could not be resolved: %v",
				err,
			)
		}
		inPath = resolved
	}

	if _, err := os.Stat(inPath); err != nil {
		log.Fatalf("convert-amaru-vector: -in %s: %v", inPath, err)
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		log.Fatalf("convert-amaru-vector: -out %s: %v", *out, err)
	}

	vectors, err := conformance.CollectVectorFiles(inPath)
	if err != nil {
		log.Fatalf("convert-amaru-vector: collect vectors: %v", err)
	}
	if len(vectors) == 0 {
		log.Fatalf("convert-amaru-vector: no vector files under %s", inPath)
	}

	var ok, failed int
	for _, srcPath := range vectors {
		rel, relErr := filepath.Rel(inPath, srcPath)
		if relErr != nil {
			rel = srcPath
		}
		if err := convertOne(srcPath, *out, rel); err != nil {
			failed++
			log.Printf("convert-amaru-vector: %s: %v", rel, err)
			if *failOnMismatch {
				log.Fatalf(
					"convert-amaru-vector: aborting on first failure (-fail-on-mismatch)",
				)
			}
			continue
		}
		ok++
	}

	fmt.Printf(
		"convert-amaru-vector: %d converted, %d failed (input=%s, output=%s)\n",
		ok, failed, inPath, *out,
	)
	if failed > 0 {
		os.Exit(1)
	}
}

// convertOne decodes one Amaru source vector, applies the structural
// conversion, and writes the JSON output mirroring the source's
// relative path under outRoot (with `.json` appended). The encoder
// also runs the new-format validator, so any field-set violation
// surfaces here rather than at first replay.
func convertOne(srcPath, outRoot, rel string) error {
	src, err := conformance.DecodeTestVector(srcPath)
	if err != nil {
		return fmt.Errorf("decode source: %w", err)
	}
	dst, err := convertVector(src)
	if err != nil {
		return fmt.Errorf("convert: %w", err)
	}
	outPath := filepath.Join(outRoot, rel+".json")
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(outPath), err)
	}
	if err := consensus.WriteVector(outPath, dst); err != nil {
		return fmt.Errorf("write %s: %w", outPath, err)
	}
	return nil
}

// resolveDefaultInputPath extracts the embedded ouroboros-mock
// testdata into a process-lifetime temp dir and returns the path
// pointing at the Conway era subtree (where the conformance vectors
// live). The extraction is leaked on process exit — fine for a
// one-shot binary that converts the corpus and exits.
//
// Using the embedded extractor instead of a go.mod lookup means the
// binary works regardless of GOMODCACHE state and doesn't need to
// shell out to `go list`.
func resolveDefaultInputPath() (string, error) {
	dir, err := os.MkdirTemp("", "convert-amaru-vector-*")
	if err != nil {
		return "", fmt.Errorf("mkdtemp: %w", err)
	}
	root, err := conformance.ExtractEmbeddedTestdata(dir)
	if err != nil {
		_ = os.RemoveAll(dir)
		return "", fmt.Errorf("ExtractEmbeddedTestdata: %w", err)
	}
	return filepath.Join(root, amaruCorpusSubpath), nil
}
