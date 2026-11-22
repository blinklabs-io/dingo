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

package architecture_test

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// sdkSourceAssertions pairs each source interface in sdk/cardano with the node
// types it is written for. It is compiled, never run.
const sdkSourceAssertions = `package main

import (
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/leader"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/sdk/cardano"
)

var (
	_ cardano.LedgerSource         = (*ledger.LedgerState)(nil)
	_ cardano.ForgedBlockValidator = (*ledger.LedgerState)(nil)
	_ cardano.ForgedBlockValidator = (forging.BlockValidator)(nil)
	_ cardano.LeaderSource         = (*leader.Election)(nil)
	_ cardano.LeaderSource         = (forging.LeaderChecker)(nil)
	_ cardano.NetworkSink          = (forging.BlockBroadcaster)(nil)
	_ cardano.MempoolSource        = (*mempool.Mempool)(nil)
)

func main() {}
`

// TestSDKCardanoSourcesMatchNode keeps the node's types satisfying the source
// interfaces the Cardano roles in sdk/cardano wrap. The node module does not
// import the sdk, which would put a replace directive in the node's go.mod and
// break go install of the node, so no build compiles the two together and a
// changed node signature would go unnoticed. This compiles them in a temporary
// workspace that holds both modules.
func TestSDKCardanoSourcesMatchNode(t *testing.T) {
	t.Parallel()

	root := findRepoRoot(t)
	goVersion := goDirective(t, filepath.Join(root, "go.mod"))
	dir := t.TempDir()
	checkDir := filepath.Join(dir, "check")
	if err := os.Mkdir(checkDir, 0o755); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		filepath.Join(checkDir, "go.mod"): "module sdksourcecheck\n\ngo " +
			goVersion + "\n",
		filepath.Join(checkDir, "main.go"): sdkSourceAssertions,
		filepath.Join(dir, "go.work"): "go " + goVersion + "\n\nuse (\n\t" +
			strings.Join([]string{
				goWorkPath(root),
				goWorkPath(filepath.Join(root, sdkDir)),
				"./check",
			}, "\n\t") + "\n)\n",
	}
	for path, content := range files {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Fatalf("find go: %v", err)
	}
	cmd := exec.CommandContext(t.Context(), goBin, "vet", "./check")
	cmd.Dir = dir
	// GOFLAGS is cleared because a -mod flag is rejected in workspace mode.
	cmd.Env = append(
		os.Environ(),
		"GOWORK="+filepath.Join(dir, "go.work"),
		"GOFLAGS=",
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf(
			"node types no longer satisfy the sdk/cardano sources: %v\n%s",
			err,
			out,
		)
	}
}

// goDirective returns the go version a go.mod declares.
func goDirective(t *testing.T, goMod string) string {
	t.Helper()

	f, err := os.Open(goMod)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if v, ok := strings.CutPrefix(scanner.Text(), "go "); ok {
			return strings.TrimSpace(v)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	t.Fatalf("%s declares no go version", goMod)
	return ""
}

// goWorkPath quotes a directory for a go.work use directive.
func goWorkPath(path string) string {
	return `"` + filepath.ToSlash(path) + `"`
}
