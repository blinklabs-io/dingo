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

package httpuseragent

import (
	"net/http"
	"runtime"
	"testing"

	"github.com/blinklabs-io/dingo/internal/version"
)

func TestDefaultIncludesVersionAndPlatform(t *testing.T) {
	t.Setenv("DINGO_HTTP_USER_AGENT", "")
	previousVersion, previousCommit := version.Version, version.CommitHash
	version.Version = "v0.87.1"
	version.CommitHash = "abcdef0"
	t.Cleanup(func() {
		version.Version = previousVersion
		version.CommitHash = previousCommit
	})

	want := "dingo/v0.87.1 (commit abcdef0) (" + runtime.GOOS + "/" + runtime.GOARCH + ")"
	if got := Default(); got != want {
		t.Fatalf("Default() = %q, want %q", got, want)
	}
}

func TestDefaultUsesConfiguredEnvironmentValue(t *testing.T) {
	t.Setenv("DINGO_HTTP_USER_AGENT", "operator-agent/2")
	if got := Default(); got != "operator-agent/2" {
		t.Fatalf("Default() = %q, want operator-agent/2", got)
	}
}

func TestSetPreservesRequestUserAgent(t *testing.T) {
	t.Setenv("DINGO_HTTP_USER_AGENT", "shared-agent/1")
	req, err := http.NewRequest(http.MethodGet, "https://example.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("User-Agent", "request-agent/3")

	Set(req)

	if got := req.Header.Get("User-Agent"); got != "request-agent/3" {
		t.Fatalf("User-Agent = %q, want request-agent/3", got)
	}
}

func TestSetUsesDefaultForEmptyRequestUserAgent(t *testing.T) {
	t.Setenv("DINGO_HTTP_USER_AGENT", "shared-agent/1")
	req, err := http.NewRequest(http.MethodGet, "https://example.test", nil)
	if err != nil {
		t.Fatal(err)
	}

	Set(req)

	if got := req.Header.Get("User-Agent"); got != "shared-agent/1" {
		t.Fatalf("User-Agent = %q, want shared-agent/1", got)
	}
}

func TestDefaultIgnoresInvalidEnvironmentValue(t *testing.T) {
	t.Setenv("DINGO_HTTP_USER_AGENT", "bad\r\nX-Injected: 1")
	got := Default()
	want := "dingo/" + version.GetVersionString() +
		" (" + runtime.GOOS + "/" + runtime.GOARCH + ")"
	if got != want {
		t.Fatalf("Default() = %q, want %q", got, want)
	}
}

func TestSetPreservesExplicitlyEmptyUserAgent(t *testing.T) {
	t.Setenv("DINGO_HTTP_USER_AGENT", "shared-agent/1")
	req, err := http.NewRequest(http.MethodGet, "https://example.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	// net/http sends no User-Agent at all for a present, empty value.
	req.Header["User-Agent"] = []string{""}

	Set(req)

	got, ok := req.Header["User-Agent"]
	if !ok || len(got) != 1 || got[0] != "" {
		t.Fatalf("User-Agent = %q (present %v), want present and empty", got, ok)
	}
}
