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

	"github.com/stretchr/testify/require"
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

// TestMain removes the shared vector extraction (see corpusTestdataRoot) after
// the tests finish, and fails the run when that removal fails.
func TestMain(m *testing.M) {
	code := m.Run()

	cleanupFailed := false
	if err := cleanupCorpusTestdata(); err != nil {
		log.Printf("conformance: remove shared vector extraction: %v", err)
		cleanupFailed = true
	}

	os.Exit(processCleanupExitCode(code, cleanupFailed))
}

// processCleanupExitCode folds a process-cleanup failure into the test run's
// own exit code. A cleanup failure must fail the run even when every test
// passed, or the leaked directory is invisible to anything that only checks
// the exit code. A real test failure's exit code is never downgraded, only
// upgraded from 0.
func processCleanupExitCode(testExitCode int, cleanupFailed bool) int {
	if cleanupFailed && testExitCode == 0 {
		return 1
	}
	return testExitCode
}

// TestProcessCleanupExitCodeFailsOnCleanupFailure proves a process-cleanup
// failure makes TestMain report a nonzero exit code even when every test in the
// process passed.
func TestProcessCleanupExitCodeFailsOnCleanupFailure(t *testing.T) {
	require.Equal(t, 0, processCleanupExitCode(0, false))
	require.Equal(t, 1, processCleanupExitCode(0, true))
	require.Equal(t, 2, processCleanupExitCode(2, true))
	require.Equal(t, 2, processCleanupExitCode(2, false))
}
