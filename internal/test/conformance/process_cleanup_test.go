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
	"log"
	"os"
	"testing"
)

// cleanupCorpusTestdata removes the shared extraction. Safe to call when the
// corpus was never extracted, which is the case for a run whose tests all
// skipped.
func cleanupCorpusTestdata() error {
	if testdataDir == "" {
		return nil
	}
	return os.RemoveAll(testdataDir)
}

// processCleanups holds teardown that must run once after every test in the
// process, in registration order. A build configuration contributes to it from
// an init function, so TestMain itself needs no build tags and cannot drift
// between configurations.
var processCleanups []func() error

// registerProcessCleanup adds fn to the process teardown chain. Call it from an
// init function in a build-tagged file.
func registerProcessCleanup(fn func() error) {
	processCleanups = append(processCleanups, fn)
}

// runProcessCleanups runs every registered process teardown in registration
// order and reports whether any of them failed.
//
// Every step always runs: a failure is logged and recorded rather than
// returning early, so one failed drop or removal never leaves a sibling
// resource uncleaned as a side effect -- for example the blob directory paired
// with a schema that failed to drop.
func runProcessCleanups() bool {
	cleanupFailed := false
	for _, cleanup := range processCleanups {
		if err := cleanup(); err != nil {
			log.Printf("conformance: process cleanup: %v", err)
			cleanupFailed = true
		}
	}
	return cleanupFailed
}

// TestMain runs every registered process teardown after the tests finish, then
// removes the shared vector extraction (see corpusTestdataRoot). The vector
// extraction removal runs even when a registered cleanup failed, and any
// failure is reflected in the exit code.
func TestMain(m *testing.M) {
	code := m.Run()

	cleanupFailed := runProcessCleanups()
	if err := cleanupCorpusTestdata(); err != nil {
		log.Printf("conformance: remove shared vector extraction: %v", err)
		cleanupFailed = true
	}

	os.Exit(processCleanupExitCode(code, cleanupFailed))
}

// processCleanupExitCode folds a process-cleanup failure into the test run's
// own exit code. A cleanup failure must fail the run even when every test
// passed -- otherwise the leaked schema, database, or directory this cleanup
// exists to remove is invisible to anything that only checks the exit code (CI,
// a local `go test && echo ok`). A real test failure's exit code is never
// downgraded, only upgraded from 0.
func processCleanupExitCode(testExitCode int, cleanupFailed bool) int {
	if cleanupFailed && testExitCode == 0 {
		return 1
	}
	return testExitCode
}
