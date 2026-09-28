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

// This package builds in two configurations, and process-level teardown differs
// between them: the dingo_extra_plugins build additionally owns a Postgres
// schema, a MySQL database, and their paired blob directories. That previously
// meant two TestMain functions selected by build tag, each with its own cleanup
// chain and its own copy of processCleanupExitCode.
//
// That split is what this replaces. Two entry points had to be kept in step by
// hand, a change to one silently did not apply to the other, and only the
// tagged copy of the exit-code helper had a test -- so the untagged build could
// have regressed without any run noticing. There is now one TestMain, compiled
// in both configurations, and the tag-specific teardown registers itself.

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
// with a schema that failed to drop. TestMain is the only caller that runs it
// against the real chain; TestProcessCleanupChainRunsEveryStepAfterAFailure
// calls this same function against a substituted chain so the guarantee is
// tested where it is implemented rather than in a copy of the loop.
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

// TestProcessCleanupExitCodeFailsOnCleanupFailure proves a process-cleanup
// failure makes TestMain report a nonzero exit code even when every test in the
// process passed -- a reviewer's forced RemoveAll permission failure otherwise
// logged "permission denied" but left `go test` exiting 0, silently leaking the
// per-run schema, database, or directory this cleanup exists to remove.
//
// This test is deliberately untagged, so it covers both build configurations.
// Previously it lived beside the tagged TestMain and the untagged build carried
// an uncovered copy of the helper.
func TestProcessCleanupExitCodeFailsOnCleanupFailure(t *testing.T) {
	require.Equal(t, 0, processCleanupExitCode(0, false))
	require.Equal(t, 1, processCleanupExitCode(0, true))
	require.Equal(t, 2, processCleanupExitCode(2, true))
	require.Equal(t, 2, processCleanupExitCode(2, false))
}

// TestProcessCleanupChainRunsEveryStepAfterAFailure proves one failing cleanup
// does not skip the ones registered after it, and that the failure still
// reaches the exit code. The ordering guarantee is the point: the Postgres and
// MySQL teardowns each drop a remote resource and then remove the local blob
// directory paired with it.
//
// It calls runProcessCleanups, the function TestMain calls, against a
// substituted chain, so a future early return on the first cleanup error fails
// this test.
func TestProcessCleanupChainRunsEveryStepAfterAFailure(t *testing.T) {
	before := processCleanups
	t.Cleanup(func() { processCleanups = before })
	processCleanups = nil

	var ran []string
	registerProcessCleanup(func() error {
		ran = append(ran, "first")
		return os.ErrPermission
	})
	registerProcessCleanup(func() error {
		ran = append(ran, "second")
		return nil
	})

	cleanupFailed := runProcessCleanups()

	require.Equal(t, []string{"first", "second"}, ran)
	require.True(t, cleanupFailed)
	require.Equal(t, 1, processCleanupExitCode(0, cleanupFailed))
}
