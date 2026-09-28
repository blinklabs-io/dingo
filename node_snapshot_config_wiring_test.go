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

package dingo

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// snapshotMgrSetterCall matches a snapshot-manager configuration call on the
// node, capturing the setter name.
var snapshotMgrSetterCall = regexp.MustCompile(
	`n\.snapshotMgr\.(Set[A-Za-z0-9_]+)\(`,
)

// koiosParityRetentionWiring matches the retention setter called with the
// operator's own koios-parity enablement. It binds the argument, not just the
// setter name: a call wired from any other field would leave the operator's
// setting ignored exactly as silently as no call at all.
var koiosParityRetentionWiring = regexp.MustCompile(
	`n\.snapshotMgr\.SetRewardAccountOutputRetentionUnbounded\(\s*` +
		`n\.config\.koiosParity\.Enabled,?\s*\)`,
)

// nodeFuncBodyForSnapshotWiring returns the source of the named function, from
// its declaration to the next top-level declaration.
func nodeFuncBodyForSnapshotWiring(
	t *testing.T,
	path string,
	decl string,
) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	s := string(b)
	start := strings.Index(s, decl)
	if start < 0 {
		t.Fatalf("%s not found in %s", decl, path)
	}
	body := s[start+len(decl):]
	if end := strings.Index(body, "\nfunc "); end >= 0 {
		body = body[:end]
	}
	return body
}

// TestReinitializeBackgroundManagersMirrorsRunSnapshotConfig pins that a live
// Restore/Truncate rebuilds the snapshot manager with every option Run()
// configures. reinitializeBackgroundManagers constructs a second
// snapshot.Manager by hand, so an option added to Run() alone is silently
// dropped after a live lifecycle operation and the node keeps running with
// package defaults instead of the operator's configuration -- which is
// exactly what happened to SetDelegatorInactivity (see
// TestLiveTruncateReinitializationPreservesSnapshotManagerDelegatorInactivityConfig)
// and is the same gap dingo #4188's retention setter would leave.
func TestReinitializeBackgroundManagersMirrorsRunSnapshotConfig(t *testing.T) {
	t.Parallel()

	runBody := nodeFuncBodyForSnapshotWiring(
		t,
		"node.go",
		"func (n *Node) Run(ctx context.Context) (runErr error) {",
	)
	reinitBody := nodeFuncBodyForSnapshotWiring(
		t,
		"node_lifecycle.go",
		"func (n *Node) reinitializeBackgroundManagers(",
	)

	runSetters := snapshotMgrSetterCall.FindAllStringSubmatch(runBody, -1)
	if len(runSetters) == 0 {
		t.Fatal("no n.snapshotMgr setter calls found in Run")
	}
	reinitSetters := map[string]struct{}{}
	for _, m := range snapshotMgrSetterCall.FindAllStringSubmatch(
		reinitBody,
		-1,
	) {
		reinitSetters[m[1]] = struct{}{}
	}
	for _, m := range runSetters {
		if _, ok := reinitSetters[m[1]]; !ok {
			t.Errorf(
				"Run configures the snapshot manager with %s but "+
					"reinitializeBackgroundManagers does not; a live "+
					"restore/truncate would silently drop that setting",
				m[1],
			)
		}
	}
}

// TestKoiosParityRetentionWiredFromConfigInBothStartupPaths pins dingo #4188's
// wiring itself: both node startup paths must widen reward_account_output
// retention from the operator's koios-parity enablement. Without the call the
// node keeps CORE mode's 4-epoch window, and the observer -- whose network
// -bound epoch validation routinely runs many epochs behind chain progression
// during a catch-up sync -- reads an epoch's rows only after
// cleanupOldSnapshots has already deleted them.
func TestKoiosParityRetentionWiredFromConfigInBothStartupPaths(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		path string
		decl string
	}{
		{
			path: "node.go",
			decl: "func (n *Node) Run(ctx context.Context) (runErr error) {",
		},
		{
			path: "node_lifecycle.go",
			decl: "func (n *Node) reinitializeBackgroundManagers(",
		},
	} {
		body := nodeFuncBodyForSnapshotWiring(t, tc.path, tc.decl)
		if !koiosParityRetentionWiring.MatchString(body) {
			t.Errorf(
				"%s does not call "+
					"n.snapshotMgr.SetRewardAccountOutputRetentionUnbounded("+
					"n.config.koiosParity.Enabled); reward_account_output "+
					"would be pruned to the 4-epoch window with the "+
					"koios-parity observer enabled",
				tc.decl,
			)
		}
	}
}
