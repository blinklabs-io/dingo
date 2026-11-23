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

// Package pluginfixture builds the out-of-process test plugin used by the
// plugin host and node tests.
package pluginfixture

import (
	"bufio"
	_ "embed"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

//go:embed testplugin.go.txt
var testPluginSource string

// Capability is the contract the test plugin implements by default.
const Capability = "example.widget/v0"

// Build compiles the test plugin from a module that requires nothing but the
// sdk, found through a temporary workspace, and returns the executable path.
// The plugin serves the gRPC health service as its contract. Flags:
// -capability, -exit-at-start and -ignore-term. Checking the health service
// "crash" makes it exit with status 7.
func Build(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..", "..")
	sdkDir := filepath.Join(repoRoot, "sdk")
	goVersion := goModDirective(t, filepath.Join(repoRoot, "go.mod"))
	// The go command matches a module against go.work's use paths through its
	// resolved working directory, so a symlinked temporary directory (macOS
	// /var -> /private/var) would leave the plugin module outside the
	// workspace.
	dir, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	pluginDir := filepath.Join(dir, "tp")
	require.NoError(t, os.Mkdir(pluginDir, 0o755))
	files := map[string]string{
		filepath.Join(pluginDir, "go.mod"): "module testplugin\n\ngo " +
			goVersion + "\n",
		filepath.Join(pluginDir, "main.go"): testPluginSource,
		filepath.Join(dir, "go.work"): "go " + goVersion + "\n\nuse (\n\t\"" +
			filepath.ToSlash(sdkDir) + "\"\n\t./tp\n)\n",
	}
	for path, content := range files {
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	}
	goBin, err := exec.LookPath("go")
	require.NoError(t, err)
	out := filepath.Join(dir, "testplugin")
	cmd := exec.CommandContext(
		t.Context(), goBin, "build", "-buildvcs=false", "-o", out, "./tp",
	)
	cmd.Dir = dir
	// GOFLAGS is cleared because a -mod flag is rejected in workspace mode.
	cmd.Env = append(
		os.Environ(),
		"GOWORK="+filepath.Join(dir, "go.work"),
		"GOFLAGS=",
	)
	if combined, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build test plugin: %v\n%s", err, combined)
	}
	return out
}

func goModDirective(t *testing.T, goMod string) string {
	t.Helper()
	f, err := os.Open(goMod)
	require.NoError(t, err)
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if v, ok := strings.CutPrefix(scanner.Text(), "go "); ok {
			return strings.TrimSpace(v)
		}
	}
	require.NoError(t, scanner.Err())
	t.Fatalf("%s declares no go version", goMod)
	return ""
}
