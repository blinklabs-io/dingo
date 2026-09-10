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

package main

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/internal/config"
)

// A profile file that fails to close must be reported to stderr, not
// silently dropped -- a truncated CPU/memory profile is otherwise
// indistinguishable from a clean one until someone tries to load it. The
// second Close() is forced to fail by closing f once already, ahead of the
// call under test, which deterministically exhausts the file descriptor.
func TestCloseProfileFileReportsCloseError(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "profile-*")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("unexpected error on first close: %v", err)
	}

	var buf bytes.Buffer
	closeProfileFile(&buf, f, "CPU")

	out := buf.String()
	if !strings.Contains(out, "could not close CPU profile file") {
		t.Fatalf("expected close error to be reported, got: %q", out)
	}
}

// The common case -- a clean close -- must stay silent. Reporting on every
// successful profile write would bury the genuine failures this exists to
// surface.
func TestCloseProfileFileStaysQuietOnSuccess(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "profile-*")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}

	var buf bytes.Buffer
	closeProfileFile(&buf, f, "memory")

	if buf.Len() != 0 {
		t.Fatalf(
			"expected no output on successful close, got: %q",
			buf.String(),
		)
	}
}

func TestMithrilPprofServerUsesDedicatedBindAddress(t *testing.T) {
	cfg := &config.Config{
		BindAddr:      "0.0.0.0",
		DebugBindAddr: "127.0.0.1",
		DebugPort:     6060,
	}
	srv := newDebugPprofHTTPServer(cfg)
	if srv == nil {
		t.Fatal("expected enabled pprof server")
	}
	if got, want := srv.Addr, "127.0.0.1:6060"; got != want {
		t.Fatalf("pprof address = %q, want %q", got, want)
	}

	cfg.DebugBindAddr = "0.0.0.0"
	srv = newDebugPprofHTTPServer(cfg)
	if srv == nil {
		t.Fatal("expected explicitly exposed pprof server")
	}
	if got, want := srv.Addr, "0.0.0.0:6060"; got != want {
		t.Fatalf("explicit wildcard pprof address = %q, want %q", got, want)
	}
}
