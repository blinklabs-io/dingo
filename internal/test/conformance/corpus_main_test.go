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

//go:build !dingo_extra_plugins

package conformance

import (
	"log"
	"os"
	"testing"
)

// TestMain removes the shared vector extraction (see corpusTestdataRoot) after
// every test in this process has finished.
//
// The build constraint is what keeps this from colliding with the TestMain in
// conformance_main_test.go, which is tagged dingo_extra_plugins and performs
// the same cleanup alongside its Postgres and MySQL teardown. Exactly one
// TestMain exists in either build configuration; do not drop either
// constraint.
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
// own exit code, matching the tagged build's helper of the same name so both
// configurations treat a leaked resource identically. A real test failure's
// exit code is never downgraded, only upgraded from 0.
func processCleanupExitCode(testExitCode int, cleanupFailed bool) int {
	if cleanupFailed && testExitCode == 0 {
		return 1
	}
	return testExitCode
}
