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

package conformance

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/blinklabs-io/ouroboros-mock/conformance"
)

// Every vector resets the state manager and loads its own initial state, so
// vectors are independent of one another and the corpus can be replayed by
// several state managers at once. The replay is single-goroutine CPU work, so
// the wall clock of the SQLite replay drops roughly with the worker count.

// corpusReplayWorkers returns how many state managers replay the corpus
// concurrently.
func corpusReplayWorkers() int {
	return max(1, min(runtime.GOMAXPROCS(0), 8))
}

// shardCorpusRoot mirrors the extracted corpus under dst, linking the vectors
// of this shard, copying supporting files and omitting the vectors that
// belong to another shard. The shared harness only
// discovers vectors by walking a root, so a root that holds one shard's
// vectors makes it replay exactly that shard. The extracted corpus itself is
// never modified.
func shardCorpusRoot(
	root, dst string,
	vectorShard map[string]int,
	shard int,
) error {
	return filepath.WalkDir(
		root,
		func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			target := filepath.Join(dst, rel)
			if entry.IsDir() {
				return os.MkdirAll(target, 0o755)
			}
			if owner, isVector := vectorShard[path]; isVector {
				if owner != shard {
					return nil
				}
				return os.Symlink(path, target)
			}
			// Supporting files (protocol parameters) are copied: the
			// parameter loader refuses a path that resolves outside its
			// directory, which a link would.
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			return os.WriteFile(target, data, 0o600)
		},
	)
}

// replayCorpusSharded replays the corpus through the shared harness with
// workers state managers and returns the results in the harness's own order,
// carrying the paths under root exactly as a single replay reports them.
func replayCorpusSharded(
	root string,
	workers int,
	newStateManager func() (*DingoStateManager, error),
) corpusRun {
	vectors, err := collectEntryPointVectors(root)
	if err != nil {
		return corpusRun{err: err}
	}
	order := make(map[string]int, len(vectors))
	vectorShard := make(map[string]int, len(vectors))
	for i, path := range vectors {
		order[path] = i
		vectorShard[path] = i % workers
	}
	scratch, err := os.MkdirTemp("", "dingo-conformance-shards-")
	if err != nil {
		return corpusRun{err: fmt.Errorf("create shard dir: %w", err)}
	}
	defer os.RemoveAll(scratch)
	roots := make([]string, workers)
	for shard := range workers {
		roots[shard] = filepath.Join(scratch, fmt.Sprintf("shard-%d", shard))
		if err := shardCorpusRoot(
			root, roots[shard], vectorShard, shard,
		); err != nil {
			return corpusRun{err: fmt.Errorf("build shard %d: %w", shard, err)}
		}
	}
	runs := make([]corpusRun, workers)
	var wg sync.WaitGroup
	for shard := range workers {
		wg.Go(func() {
			sm, err := newStateManager()
			if err != nil {
				runs[shard] = corpusRun{err: fmt.Errorf(
					"new state manager for shard %d: %w", shard, err,
				)}
				return
			}
			defer sm.Close()
			runs[shard] = replayCorpusRoot(sm, roots[shard])
		})
	}
	wg.Wait()
	var merged []conformance.VectorResult
	for shard, run := range runs {
		if run.err != nil {
			return corpusRun{err: fmt.Errorf("shard %d: %w", shard, run.err)}
		}
		for _, result := range run.results {
			rel, err := filepath.Rel(roots[shard], result.Path)
			if err != nil {
				return corpusRun{err: err}
			}
			result.Path = filepath.Join(root, rel)
			merged = append(merged, result)
		}
	}
	for _, result := range merged {
		if _, ok := order[result.Path]; !ok {
			return corpusRun{err: fmt.Errorf(
				"shard replay reported unknown vector %s", result.Path,
			)}
		}
	}
	sort.Slice(merged, func(i, j int) bool {
		return order[merged[i].Path] < order[merged[j].Path]
	})
	return corpusRun{results: merged}
}

// replayEntryPointsSharded is replayEntryPoints with workers state managers,
// each replaying every workers-th vector. The evidence keeps the vector order
// of a single replay.
func replayEntryPointsSharded(
	testdataRoot string,
	entries []eraEntryPoint,
	workers int,
) ([]vectorEntryPointEvidence, error) {
	paths, err := collectEntryPointVectors(testdataRoot)
	if err != nil {
		return nil, err
	}
	evidence := make([]vectorEntryPointEvidence, len(paths))
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Go(func() {
			sm, err := NewDingoStateManager()
			if err != nil {
				errs[worker] = fmt.Errorf("new sqlite state manager: %w", err)
				return
			}
			defer sm.Close()
			loader := conformance.NewPParamsLoaderFromTestdata(testdataRoot)
			observer := newObservedLedgerState(NewDingoStateProvider(sm))
			for i := worker; i < len(paths); i += workers {
				ev := replayVectorEntryPoints(
					sm, loader, observer, entries, paths[i],
				)
				if rel, err := filepath.Rel(testdataRoot, paths[i]); err == nil {
					ev.Path = strings.TrimPrefix(rel, string(filepath.Separator))
				}
				evidence[i] = ev
			}
		})
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return evidence, nil
}
