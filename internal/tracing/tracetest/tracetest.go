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

// Package tracetest captures runtime/trace execution traces in tests.
package tracetest

import (
	"bytes"
	rtrace "runtime/trace"
	"testing"
)

// SkipIfTracing skips t when an execution trace is already running, as it is
// under go test -trace. Only one trace can run per process, so a test cannot
// capture its own, and a running trace also defeats a test of the
// tracing-off path.
func SkipIfTracing(t testing.TB) {
	t.Helper()
	if rtrace.IsEnabled() {
		t.Skip("an execution trace is already running")
	}
}

// Capture runs fn under an execution trace and returns the trace. It skips t
// when a trace is already running. The trace is stopped even when fn panics
// or calls t.FailNow.
func Capture(t testing.TB, fn func()) []byte {
	t.Helper()
	SkipIfTracing(t)
	var buf bytes.Buffer
	if err := rtrace.Start(&buf); err != nil {
		t.Fatalf("start execution trace: %v", err)
	}
	func() {
		defer rtrace.Stop()
		fn()
	}()
	return buf.Bytes()
}

// RequireRegions fails t unless trace names every region in names.
func RequireRegions(t testing.TB, trace []byte, names ...string) {
	t.Helper()
	for _, name := range names {
		if !bytes.Contains(trace, []byte(name)) {
			t.Fatalf("execution trace has no region %q", name)
		}
	}
}

// RequireRegionOrder fails t unless trace names every region in names, first
// used in that order. The runtime writes each string to the trace when it is
// first used, so the order of the names' first occurrences is the order in
// which their regions first started.
func RequireRegionOrder(t testing.TB, trace []byte, names ...string) {
	t.Helper()
	last := -1
	for i, name := range names {
		at := bytes.Index(trace, []byte(name))
		if at < 0 {
			t.Fatalf("execution trace has no region %q", name)
		}
		if at < last {
			t.Fatalf("region %q first starts before %q", name, names[i-1])
		}
		last = at
	}
}
