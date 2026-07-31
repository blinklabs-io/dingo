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

// Package ci_test guards the CI configuration that cannot be exercised by
// running the pipeline itself. The DevNet gate
// (.github/workflows/devnet.yml) decides whether to run the DevNet suites from
// the path patterns in .github/devnet-paths.txt; a pattern that matches nothing
// looks like coverage while providing none, so the pattern list is checked
// against the real tree here and fails the normal test run when it drifts.
package ci_test

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const (
	patternFile    = ".github/devnet-paths.txt"
	runTestsScript = "internal/test/devnet/run-tests.sh"
)

// consensusSensitiveFiles are real files whose change must select the DevNet
// suites. They are spot checks on the classification contract in the issue:
// consensus, ledger epoch/nonce logic, forging and credentials, chain
// selection, mempool, tx submission, NtN/NtC protocols, block/header
// validation, DevNet infrastructure, and shared dependencies.
var consensusSensitiveFiles = []string{
	"chain/chain.go",
	"chainselection/selector.go",
	"chainselection/vrf.go",
	"chainsync/chainsync.go",
	"consensus/praos/view.go",
	"ledger/chainsync.go",
	"ledger/epoch_lab_nonce.go",
	"ledger/verify_header.go",
	"ledger/verify_opcert.go",
	"ledger/forging/builder.go",
	"ledger/leader/election.go",
	"keystore/keystore.go",
	"mempool/mempool.go",
	"ouroboros/txsubmission.go",
	"ouroboros/localtxsubmission.go",
	"ouroboros/localstatequery.go",
	"ouroboros/blockfetch.go",
	"connmanager/connection_manager.go",
	"peergov/peergov.go",
	"database/database.go",
	"node.go",
	"node_forging.go",
	"config.go",
	"cmd/dingo/main.go",
	"internal/node/node.go",
	"internal/test/devnet/run-tests.sh",
	"internal/test/devnet/docker-compose.yml",
	"internal/test/antithesis/Dockerfile.txpump",
	"Dockerfile",
	"Makefile",
	"go.mod",
	"go.sum",
	".github/workflows/devnet.yml",
	".github/workflows/devnet-suite.yml",
	".github/devnet-paths.txt",
	".github/scripts/devnet-path-filter.sh",
}

// unrelatedFiles are real files whose change must not select the DevNet suites.
// They keep the filter from degenerating into "always run", which would make
// the gate useless as a signal and block unrelated work once it is required.
var unrelatedFiles = []string{
	"README.md",
	"ARCHITECTURE.md",
	"DATABASE.md",
	".github/workflows/go-test.yml",
	".github/dependabot.yml",
	"examples/README.md",
}

func TestDevNetPathFilterPatternsMatchRealFiles(t *testing.T) {
	repoRoot := findRepoRoot(t)
	patterns := loadPatterns(t, filepath.Join(repoRoot, patternFile))
	if len(patterns) == 0 {
		t.Fatalf("%s contains no patterns", patternFile)
	}
	paths := repoPaths(t, repoRoot)

	for _, pattern := range patterns {
		compiled, err := regexp.Compile(pattern)
		if err != nil {
			t.Errorf("pattern %q does not compile: %v", pattern, err)
			continue
		}
		matched := false
		for _, path := range paths {
			if compiled.MatchString(path) {
				matched = true
				break
			}
		}
		if !matched {
			t.Errorf(
				"pattern %q in %s matches no file in the tree; a filter that matches nothing looks like coverage without providing any",
				pattern,
				patternFile,
			)
		}
	}
}

func TestDevNetPathFilterClassifiesKnownFiles(t *testing.T) {
	repoRoot := findRepoRoot(t)
	patterns := loadPatterns(t, filepath.Join(repoRoot, patternFile))
	compiled := make([]*regexp.Regexp, 0, len(patterns))
	for _, pattern := range patterns {
		expr, err := regexp.Compile(pattern)
		if err != nil {
			t.Fatalf("pattern %q does not compile: %v", pattern, err)
		}
		compiled = append(compiled, expr)
	}

	matches := func(path string) bool {
		for _, expr := range compiled {
			if expr.MatchString(path) {
				return true
			}
		}
		return false
	}

	for _, path := range consensusSensitiveFiles {
		if _, err := os.Stat(filepath.Join(repoRoot, path)); err != nil {
			t.Errorf(
				"consensus-sensitive spot check %q does not exist; update the list and %s together: %v",
				path,
				patternFile,
				err,
			)
			continue
		}
		if !matches(path) {
			t.Errorf(
				"%q is consensus-sensitive but no pattern in %s matches it",
				path,
				patternFile,
			)
		}
	}

	for _, path := range unrelatedFiles {
		if _, err := os.Stat(filepath.Join(repoRoot, path)); err != nil {
			t.Errorf("unrelated spot check %q does not exist: %v", path, err)
			continue
		}
		if matches(path) {
			t.Errorf(
				"%q is not consensus-sensitive but a pattern in %s matches it; the gate would run the DevNet suites for unrelated changes",
				path,
				patternFile,
			)
		}
	}
}

// TestDevNetRunTestsExitStatusContract guards the teardown exit-status contract
// of run-tests.sh. Copying the genesis stake keys out of the utxo-keys volume
// runs as root inside a container, so the copied tree was root-owned while the
// host temp dir was not; the host "rm -rf" in the EXIT trap then failed with
// "Permission denied", and because the script runs under "set -e" that unguarded
// failure aborted the trap and the shell exited 1 even when every scenario
// passed. The gate can only be honest if a green run exits 0, so teardown
// problems are warnings and the trap ends by exiting the status that triggered
// it.
func TestDevNetRunTestsExitStatusContract(t *testing.T) {
	repoRoot := findRepoRoot(t)
	script, err := os.ReadFile(filepath.Join(repoRoot, runTestsScript))
	if err != nil {
		t.Fatalf("read %s: %v", runTestsScript, err)
	}
	body := string(script)

	if !strings.Contains(body, `exit "${exit_code}"`) {
		t.Errorf(
			"%s must end its EXIT trap with exit \"${exit_code}\" so teardown noise cannot change the run's exit status",
			runTestsScript,
		)
	}
	if strings.Contains(body, `rm -rf "${STAKE_KEYS_HOST_DIR}"`) {
		t.Errorf(
			"%s must not remove the stake-keys temp dir with a bare host rm -rf; the copied tree is container-owned and the failure becomes the script's exit status",
			runTestsScript,
		)
	}
}

// TestDevNetSystemStartDelayIsPlumbed guards the genesis start budget wiring.
// configurator.sh applies systemStart after key generation because
// genesis-cli.py's own 5s delay is too short, and the budget has to be
// adjustable per host without editing the script. The variable is only useful if
// the compose file passes it to both profiles' configurator services; a dropped
// pass-through silently falls back to the default instead of failing.
func TestDevNetSystemStartDelayIsPlumbed(t *testing.T) {
	repoRoot := findRepoRoot(t)

	configurator, err := os.ReadFile(
		filepath.Join(repoRoot, "internal/test/devnet/configurator.sh"),
	)
	if err != nil {
		t.Fatalf("read configurator.sh: %v", err)
	}
	script := string(configurator)
	if !strings.Contains(script, `"${DEVNET_SYSTEM_START_DELAY:-30}"`) {
		t.Error(
			"configurator.sh must read DEVNET_SYSTEM_START_DELAY with a default of 30 seconds",
		)
	}
	if !strings.Contains(script, "must be a non-negative integer") {
		t.Error(
			"configurator.sh must reject a non-integer DEVNET_SYSTEM_START_DELAY before it reaches arithmetic and jq",
		)
	}

	compose, err := os.ReadFile(
		filepath.Join(repoRoot, "internal/test/devnet/docker-compose.yml"),
	)
	if err != nil {
		t.Fatalf("read docker-compose.yml: %v", err)
	}
	const passThrough = `DEVNET_SYSTEM_START_DELAY: "${DEVNET_SYSTEM_START_DELAY:-30}"`
	if got := strings.Count(string(compose), passThrough); got != 2 {
		t.Errorf(
			"docker-compose.yml passes DEVNET_SYSTEM_START_DELAY to %d services, want 2 (configurator and configurator-dingo)",
			got,
		)
	}
}

// loadPatterns reads the pattern file, dropping comments and blank lines exactly
// as .github/scripts/devnet-path-filter.sh does.
func loadPatterns(t *testing.T, path string) []string {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer file.Close()

	var patterns []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		patterns = append(patterns, line)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return patterns
}

// repoPaths returns every tracked-looking repository-relative path, skipping
// version-control and build output directories.
func repoPaths(t *testing.T, repoRoot string) []string {
	t.Helper()

	var paths []string
	err := filepath.WalkDir(
		repoRoot,
		func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			rel, relErr := filepath.Rel(repoRoot, path)
			if relErr != nil {
				return relErr
			}
			rel = filepath.ToSlash(rel)
			if entry.IsDir() {
				switch rel {
				case ".", ".github":
					return nil
				}
				switch entry.Name() {
				case "bin", "build", "coverage", "dist", "out":
					return filepath.SkipDir
				}
				if strings.HasPrefix(entry.Name(), ".") {
					return filepath.SkipDir
				}
				return nil
			}
			paths = append(paths, rel)
			return nil
		},
	)
	if err != nil {
		t.Fatalf("walk %s: %v", repoRoot, err)
	}
	return paths
}

// findRepoRoot walks upward from the test working directory until it finds the
// module root.
func findRepoRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find repository root")
		}
		dir = parent
	}
}
