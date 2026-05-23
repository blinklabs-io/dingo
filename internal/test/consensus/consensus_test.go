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

package consensus_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/consensus"
)

// TestConsensusConformanceVectors walks every committed
// consensus-category vector under testdata/captured/ and replays it
// through the dingo-side runner. Each vector becomes a subtest;
// pass/fail is reported per file.
//
// Empty corpus is a soft skip: the test surfaces when no vectors are
// committed (a vacuous pass would silently hide a regression that
// removes the corpus).
func TestConsensusConformanceVectors(t *testing.T) {
	runVectorDir(t, filepath.Join("testdata", "captured"))
}

// TestLedgerConformanceVectorsNewFormat walks ledger-category
// vectors under testdata/converted/. Today the directory is empty
// (the Amaru-corpus conversion tool isn't built yet); when it
// populates the directory the test loop picks the new vectors up
// automatically. The existing internal/test/conformance/
// TestRulesConformanceVectors continues to run against the
// un-converted Amaru corpus in the meantime.
func TestLedgerConformanceVectorsNewFormat(t *testing.T) {
	runVectorDir(t, filepath.Join("testdata", "converted"))
}

func runVectorDir(t *testing.T, dir string) {
	t.Helper()
	paths, err := collectVectorPaths(dir)
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	if len(paths) == 0 {
		t.Skipf("no vectors under %s", dir)
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			v, err := consensus.LoadVector(path)
			if err != nil {
				t.Fatalf("load %s: %v", path, err)
			}
			if err := consensus.RunVector(t, v); err != nil {
				t.Fatalf("%s: %v", v.Title, err)
			}
		})
	}
}

// collectVectorPaths returns every .json file beneath root,
// recursively. Returns nil if root doesn't exist (so the caller can
// skip cleanly when a corpus dir hasn't been populated yet).
func collectVectorPaths(root string) ([]string, error) {
	if _, err := os.Stat(root); os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	var paths []string
	err := filepath.Walk(root, func(
		path string, info os.FileInfo, walkErr error,
	) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".json") {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	return paths, err
}
