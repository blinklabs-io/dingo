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

// Package load holds the guards for the test-load gate in run-tests.sh.
//
// The script detects Plutus accounting drift in two directions: a Go test
// covers under-accounting, and a grep over the load log covers
// over-accounting. Both halves refer to something defined elsewhere by
// name, and both fail open — a stale test name makes `go test -run` match
// nothing and exit 0, and a reworded log line makes the grep find nothing.
// Either way the gate reports success while measuring nothing, which is the
// exact failure it exists to prevent. These tests pin both references.
package load

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	gateTestRe = regexp.MustCompile(`GATE_TEST="([^"]+)"`)
	markerRe   = regexp.MustCompile(`DISAGREEMENT_MARKER="([^"]+)"`)
	// The Warn call the marker has to match, in ledger/state.go.
	ledgerWarnRe = regexp.MustCompile(
		`Logger\.Warn\(\s*\n\s*"([^"]*Plutus evaluation disagrees[^"]*)"`,
	)
)

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for range 10 {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, dir, parent, "reached the filesystem root")
		dir = parent
	}
	t.Fatal("could not locate the module root")
	return ""
}

func readFile(t *testing.T, parts ...string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(parts...))
	require.NoError(t, err)
	return string(body)
}

func captureOne(t *testing.T, re *regexp.Regexp, text, what string) string {
	t.Helper()
	m := re.FindStringSubmatch(text)
	require.Len(t, m, 2, "could not find %s", what)
	return m[1]
}

// TestLoadGateGuardTestExists keeps the test name the script filters on in
// step with the test itself. `go test -run` exits 0 when its pattern matches
// nothing, so a rename would leave the under-accounting half of the gate
// passing without executing anything.
func TestLoadGateGuardTestExists(t *testing.T) {
	root := repoRoot(t)
	script := readFile(t, root, "internal", "test", "load", "run-tests.sh")
	name := captureOne(t, gateTestRe, script, "GATE_TEST in run-tests.sh")

	source := readFile(t, root, "ledger", "eras", "validation_test.go")
	require.Contains(
		t, source, "func "+name+"(t *testing.T)",
		"run-tests.sh gates on %q, which does not exist in "+
			"ledger/eras/validation_test.go; the under-accounting gate "+
			"would match no tests and silently pass", name,
	)
}

// TestLoadGateWarningMatchesLedger keeps the grep marker in step with the
// producer-disagreement warning it looks for. The script greps the load log
// for prose emitted by ledger/state.go; rewording the log line without the
// script would make the over-accounting half of the gate find nothing and
// report success.
func TestLoadGateWarningMatchesLedger(t *testing.T) {
	root := repoRoot(t)
	script := readFile(t, root, "internal", "test", "load", "run-tests.sh")
	marker := captureOne(
		t, markerRe, script, "DISAGREEMENT_MARKER in run-tests.sh",
	)

	state := readFile(t, root, "ledger", "state.go")
	logged := captureOne(
		t, ledgerWarnRe, state, "the producer-disagreement Warn call",
	)

	require.True(
		t, strings.Contains(logged, marker),
		"run-tests.sh greps for %q but ledger/state.go logs %q; the "+
			"over-accounting gate would never fire", marker, logged,
	)
}
