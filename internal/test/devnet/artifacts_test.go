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

package devnet

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeArtifactSource stands in for NodeControl so the capture path can be
// exercised without a running DevNet or a Docker daemon.
type fakeArtifactSource struct {
	status    string
	statusErr error
	logs      map[string]string
	logErr    map[string]error
	asked     []string
	tails     []int
}

func (f *fakeArtifactSource) ContainerStatus(
	context.Context,
) (string, error) {
	return f.status, f.statusErr
}

func (f *fakeArtifactSource) Logs(
	_ context.Context,
	service string,
	tailLines int,
) (string, error) {
	f.asked = append(f.asked, service)
	f.tails = append(f.tails, tailLines)
	if err := f.logErr[service]; err != nil {
		return "", err
	}
	return f.logs[service], nil
}

// requireSinglePathSegment asserts that name addresses exactly one entry
// directly inside root, and does so without depending on the platform's
// separator. A literal would: filepath.Join normalizes through Clean,
// whose documented last step replaces every slash with filepath.Separator,
// so on Windows filepath.Dir(filepath.Join("/root", name)) is `\root` and
// never equals a "/root" literal. This file carries no build tag, so it
// runs under the untagged `go test ./...` that release validation runs on
// windows-latest; an assertion that only holds on one separator fails
// there and blocks the release. Both sides are built with filepath here,
// so they normalize identically on every platform.
func requireSinglePathSegment(
	t *testing.T,
	root, name string,
	msgAndArgs ...any,
) {
	t.Helper()
	joined := filepath.Join(root, name)
	require.Equal(t, filepath.Clean(root), filepath.Dir(joined), msgAndArgs...)
	require.Equal(t, name, filepath.Base(joined), msgAndArgs...)
}

func testEndpoints() []NodeEndpoint {
	return []NodeEndpoint{
		{Name: "dingo-1", Role: "producer", Container: "dingo-1"},
		{Name: "dingo-2", Role: "producer", Container: "dingo-2"},
		{Name: "dingo-relay", Role: "relay", Container: "dingo-relay"},
	}
}

func TestPlanFailureCaptureNamesServicesFromEndpoints(t *testing.T) {
	plan, ok := PlanFailureCapture(
		"/artifacts", "TestSustainedConsensus", testEndpoints(),
	)

	require.True(t, ok, "an artifact dir plus containers enables capture")
	require.Equal(t, "/artifacts", plan.Root)
	require.Equal(t, "TestSustainedConsensus", plan.Name)
	require.Equal(t,
		[]string{"dingo-1", "dingo-2", "dingo-relay"}, plan.Services,
		"every endpoint with a container contributes its service",
	)
}

func TestPlanFailureCaptureDisabledWithoutArtifactDir(t *testing.T) {
	_, ok := PlanFailureCapture("", "TestSustainedConsensus", testEndpoints())

	require.False(t, ok,
		"without DEVNET_ARTIFACT_DIR there is nowhere to preserve evidence",
	)
}

func TestPlanFailureCaptureDisabledWithoutContainers(t *testing.T) {
	// The unit-style harness tests construct endpoints that describe no
	// container. Capturing for them would dial addresses that do not
	// exist and ask Docker for services it does not have.
	endpoints := []NodeEndpoint{
		{Name: "cardano-producer", Role: "producer"},
		{Name: "cardano-relay", Role: "relay"},
	}

	_, ok := PlanFailureCapture("/artifacts", "TestX", endpoints)

	require.False(t, ok, "no container means nothing to capture")
}

func TestPlanFailureCaptureSkipsEndpointsWithoutContainers(t *testing.T) {
	endpoints := append(
		testEndpoints(),
		NodeEndpoint{Name: "observer-only", Role: "relay"},
	)

	plan, ok := PlanFailureCapture("/artifacts", "TestX", endpoints)

	require.True(t, ok)
	require.NotContains(t, plan.Services, "",
		"an endpoint with no container must not become an empty service",
	)
	require.Len(t, plan.Services, 3)
}

func TestPlanFailureCaptureKeepsTheNameInsideTheRoot(t *testing.T) {
	// t.Name() renders a subtest as parent/child. Left alone that
	// scatters one scenario's evidence across nested directories, and a
	// name that walked upward would escape the artifact root entirely.
	plan, ok := PlanFailureCapture(
		"/artifacts", "TestEpochBoundary/../../etc", testEndpoints(),
	)

	require.True(t, ok)
	requireSinglePathSegment(t, "/artifacts", plan.Name,
		"the artifact directory must be a direct child of the root",
	)
}

func TestWriteFailureArtifactsPreservesChainsStatusAndLogs(t *testing.T) {
	root := t.TempDir()
	src := &fakeArtifactSource{
		status: "NAME       STATUS\ndingo-1    Up 4 minutes\n",
		logs: map[string]string{
			"dingo-1":     "dingo-1 forged block 77\n",
			"dingo-relay": "dingo-relay received block 77\n",
		},
	}
	snapshots := []ChainSnapshot{
		{
			Node:         "dingo-1",
			Tip:          ChainTip{SlotNumber: 241, BlockNumber: 77},
			RollForwards: 77,
		},
		{
			Node:         "dingo-relay",
			Tip:          ChainTip{SlotNumber: 241, BlockNumber: 77},
			RollForwards: 77,
		},
	}
	plan := FailureCapturePlan{
		Root:     root,
		Name:     "TestSustainedConsensus",
		Services: []string{"dingo-1", "dingo-relay"},
	}

	WriteFailureArtifacts(
		context.Background(), src, plan, snapshots, t.Logf,
	)

	dir := filepath.Join(root, "TestSustainedConsensus")

	// The observed chain events are the evidence that distinguishes a
	// missed forge from a propagation stall, so they have to survive
	// teardown in a form something else can read.
	raw, err := os.ReadFile(filepath.Join(dir, "observed-chains.json"))
	require.NoError(t, err)
	var got []ChainSnapshot
	require.NoError(t, json.Unmarshal(raw, &got))
	require.Equal(t, snapshots, got)

	status, err := os.ReadFile(filepath.Join(dir, "container-status.txt"))
	require.NoError(t, err)
	require.Equal(t, src.status, string(status))

	for svc, want := range src.logs {
		logs, err := os.ReadFile(filepath.Join(dir, svc+".log"))
		require.NoError(t, err, "logs for %s", svc)
		require.Equal(t, want, string(logs))
	}
}

func TestWriteFailureArtifactsWithoutRootWritesNothing(t *testing.T) {
	root := t.TempDir()
	src := &fakeArtifactSource{status: "status"}
	plan := FailureCapturePlan{
		Name:     "TestSustainedConsensus",
		Services: []string{"dingo-1"},
	}

	WriteFailureArtifacts(
		context.Background(), src, plan, nil, t.Logf,
	)

	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Empty(t, entries, "an unset root must not write anywhere")
	require.Empty(t, src.asked, "and must not shell out to Docker either")
}

func TestWriteFailureArtifactsContinuesAfterOneServiceFails(t *testing.T) {
	// Capture is best-effort: it runs because the test already failed,
	// so losing one service's logs must not cost the rest of the
	// evidence.
	root := t.TempDir()
	src := &fakeArtifactSource{
		statusErr: errors.New("docker compose ps: daemon gone"),
		logs:      map[string]string{"dingo-relay": "relay logs\n"},
		logErr: map[string]error{
			"dingo-1": errors.New("no such service"),
		},
	}
	plan := FailureCapturePlan{
		Root:     root,
		Name:     "TestSustainedConsensus",
		Services: []string{"dingo-1", "dingo-relay"},
	}

	WriteFailureArtifacts(
		context.Background(), src, plan,
		[]ChainSnapshot{{Node: "dingo-1"}}, t.Logf,
	)

	dir := filepath.Join(root, "TestSustainedConsensus")
	require.FileExists(t, filepath.Join(dir, "observed-chains.json"),
		"the chain record does not depend on Docker",
	)
	require.NoFileExists(t, filepath.Join(dir, "container-status.txt"),
		"a failed status read writes no half-truth",
	)
	require.NoFileExists(t, filepath.Join(dir, "dingo-1.log"))
	require.FileExists(t, filepath.Join(dir, "dingo-relay.log"),
		"the service that answered is still preserved",
	)
	require.Equal(t, []string{"dingo-1", "dingo-relay"}, src.asked,
		"one service failing must not stop the walk",
	)
}

func TestWriteFailureArtifactsWithoutSourceStillWritesChains(t *testing.T) {
	// The observed chains are recorded in-process, so an unreachable
	// Docker daemon must not cost the one artifact that does not depend
	// on it.
	root := t.TempDir()
	plan := FailureCapturePlan{
		Root:     root,
		Name:     "TestSustainedConsensus",
		Services: []string{"dingo-1"},
	}

	WriteFailureArtifacts(
		context.Background(), nil, plan,
		[]ChainSnapshot{{Node: "dingo-1", RollForwards: 77}}, t.Logf,
	)

	dir := filepath.Join(root, "TestSustainedConsensus")
	raw, err := os.ReadFile(filepath.Join(dir, "observed-chains.json"))
	require.NoError(t, err)
	var got []ChainSnapshot
	require.NoError(t, json.Unmarshal(raw, &got))
	require.Len(t, got, 1)
	require.Equal(t, 77, got[0].RollForwards)
	require.NoFileExists(t, filepath.Join(dir, "container-status.txt"))
	require.NoFileExists(t, filepath.Join(dir, "dingo-1.log"))
}

func TestWriteFailureArtifactsBoundsCapturedLogs(t *testing.T) {
	// A DevNet node at debug level emits tens of megabytes a minute, and
	// run-tests.sh already preserves the complete compose log for the
	// whole run. The per-scenario copy only has to carry the window
	// around the failure, so it asks for a bounded tail rather than
	// everything the daemon still holds.
	src := &fakeArtifactSource{logs: map[string]string{"dingo-1": "x"}}
	plan := FailureCapturePlan{
		Root:     t.TempDir(),
		Name:     "TestSustainedConsensus",
		Services: []string{"dingo-1"},
	}

	WriteFailureArtifacts(context.Background(), src, plan, nil, t.Logf)

	require.Equal(t, []int{CapturedLogTailLines}, src.tails)
	require.Positive(t, CapturedLogTailLines,
		"an unbounded tail is what this guards against",
	)
}

// artifactNameCorpus collects the test names that probe the ways an
// encoding can lose information: the subtest separator against an
// ordinary character, an escape sequence written literally, both
// separators, and names made only of dots.
//
// The last group is what Windows treats specially. Go rewrites
// whitespace to '_' before t.Name() returns, so a trailing space cannot
// arrive that way, but ArtifactName is exported and NodeControl's
// caller passes a name of its own, so it is covered too; ':' '*' '?' '"'
// '<' '>' '|' and a trailing dot all survive t.Run untouched. They are
// here to hold the encoding to its contract on every platform the
// untagged tests run on: distinct names stay distinct, each one stays a
// single path segment, and each one names a directory the filesystem
// will actually create.
var artifactNameCorpus = []string{
	"TestSustainedConsensus",
	"accelerated-timeline",
	"TestX/a-b",
	"TestX/a/b",
	"TestX-a-b",
	"TestX%2Fa",
	"TestX/a",
	"TestX%252Fa",
	`TestX\a`,
	"TestX/",
	"/TestX",
	"TestEpochBoundary/../../etc",
	".",
	"..",
	"...",
	"TestX/..",
	"TestX/a:b",
	"TestX/a*b",
	"TestX/a?b",
	`TestX/a"b`,
	"TestX/a<b",
	"TestX/a>b",
	"TestX/a|b",
	"TestX/a.",
	"TestX/a ",
}

func TestArtifactNameIsInjective(t *testing.T) {
	// Replacing the separator with an ordinary character is lossy:
	// TestX/a-b and TestX/a/b would both become TestX-a-b, and two
	// failing subtests would mix their evidence in one directory.
	// Escaping keeps the mapping one-to-one, so this is a property of
	// the encoding rather than a probability.
	seen := make(map[string]string, len(artifactNameCorpus))
	for _, name := range artifactNameCorpus {
		got := ArtifactName(name)
		if prev, dup := seen[got]; dup {
			t.Fatalf(
				"%q and %q both encode to %q", prev, name, got,
			)
		}
		seen[got] = name
	}
}

func TestArtifactNameStaysASinglePathSegment(t *testing.T) {
	for _, name := range artifactNameCorpus {
		got := ArtifactName(name)
		require.NotEmpty(t, got, "%q encoded to nothing", name)
		requireSinglePathSegment(t, "/root", got,
			"%q encoded to %q, which leaves the root", name, got,
		)
	}
}

func TestArtifactNameLeavesPlainScenarioNamesAlone(t *testing.T) {
	// Every scenario in the canonical suite is a top-level test, and
	// the directory is found by the name in the failure output, so a
	// name that needs no rewriting must come back untouched.
	require.Equal(t, "TestSustainedConsensus",
		ArtifactName("TestSustainedConsensus"),
	)
}

// TestArtifactNamesCreateDistinctDirectories checks the guarantee
// ArtifactName actually makes -- distinct test names get distinct
// directories -- against a filesystem instead of against string
// equality. Lexical injectivity is not the same guarantee: a filesystem
// that rewrites a name stores two distinct encodings in one directory,
// and a filesystem that rejects one stores neither. Windows does both,
// so this is where that shows up rather than in
// TestArtifactNameIsInjective.
func TestArtifactNamesCreateDistinctDirectories(t *testing.T) {
	root := t.TempDir()
	// Report every name rather than stopping at the first, because the
	// two ways this breaks look nothing alike: a rejected name is a
	// loud error, while a rewritten one silently joins another
	// scenario's directory. Seeing both at once is the point.
	encoded := make([]string, 0, len(artifactNameCorpus))
	source := make([]string, 0, len(artifactNameCorpus))
	for _, name := range artifactNameCorpus {
		got := ArtifactName(name)
		if err := os.MkdirAll(filepath.Join(root, got), 0o755); err != nil {
			t.Errorf(
				"%q encoded to %q, which the filesystem rejected: %v",
				name, got, err,
			)
			continue
		}
		encoded = append(encoded, got)
		source = append(source, name)
	}

	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	made := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		made[entry.Name()] = struct{}{}
	}
	for i, got := range encoded {
		if _, ok := made[got]; !ok {
			t.Errorf(
				"%q encoded to %q, which the filesystem stored as "+
					"something else",
				source[i], got,
			)
		}
	}
	// The corpus encodes injectively, so one directory per entry is the
	// only count that leaves every scenario's evidence separable.
	require.Len(t, entries, len(encoded),
		"%d names produced %d directories", len(encoded), len(entries),
	)
}

// TestArtifactNameNeverEndsInAStrippedCharacter guards the trailing-dot
// and trailing-space escapes directly. Windows removes both from the end
// of a name, so an encoding that ended in either would be stored under a
// shorter name -- and two encodings that differ only there would become
// one directory. The filesystem test above only catches this when it runs
// on Windows; this one holds everywhere.
func TestArtifactNameNeverEndsInAStrippedCharacter(t *testing.T) {
	for _, name := range artifactNameCorpus {
		got := ArtifactName(name)
		last := got[len(got)-1]
		require.NotContains(t, ". ", string(last),
			"%q encoded to %q, which Windows would store as %q",
			name, got, strings.TrimRight(got, ". "),
		)
	}
}
