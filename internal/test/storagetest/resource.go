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

package storagetest

import (
	"runtime"
	"testing"
	"time"
)

// AssertNoGoroutineLeak runs run twice -- which must construct, use, and
// fully stop/close its own store each time -- and asserts the goroutine
// count after the second run is no higher than after the first.
//
// The first run is a deliberate, unmeasured warm-up: a plugin's underlying
// library can start persistent, package-level machinery the first time it
// is ever opened in this process (a shared metrics goroutine, a driver's
// connection janitor), and that one-time initialization is not a
// per-instance leak. Measuring the baseline only after that warm-up isolates
// what this check actually cares about: does closing one store instance
// leave behind a goroutine that closing it correctly should have stopped.
//
// This is deliberately a standalone helper rather than a subtest inside
// RunBlobStoreConformance/RunMetadataStoreConformance: those reuse one store
// across every subtest and only stop it via t.Cleanup after the whole suite
// returns, so there is no point mid-suite where "just stopped, nothing else
// running yet" is true. A leak check needs exactly that point, so callers
// construct a store dedicated to this check inside run and stop it before
// returning.
func AssertNoGoroutineLeak(t *testing.T, run func(t *testing.T)) {
	t.Helper()
	run(t)
	runtime.GC()
	baseline := runtime.NumGoroutine()

	run(t)

	deadline := time.Now().Add(5 * time.Second)
	for {
		count := runtime.NumGoroutine()
		if count <= baseline {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"goroutine count did not return to baseline (%d) after "+
					"Stop/Close: now %d",
				baseline,
				count,
			)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// AssertRepeatedLifecycleIsSafe runs run n times in a row, requiring each
// cycle to leave t un-failed before starting the next one.
//
// Use this instead of AssertNoGoroutineLeak for a plugin backed by an
// HTTP or gRPC client the underlying SDK gives no way to force-close: Go's
// net/http keeps a persistConn read/write loop goroutine alive per pooled
// connection until its own idle timeout, regardless of how many client
// values get constructed and abandoned, and neither the AWS SDK's
// *s3.Client nor Google's storage.Client exposes a method to close that
// pool early (this repository's aws.BlobStoreS3.Stop already documents
// this: "S3 client doesn't need explicit closing"). A strict before/after
// goroutine-count diff would misreport that expected, bounded behavior as a
// leak. What this check asserts instead -- repeated construct/use/stop
// cycles neither error nor deadlock nor panic -- is the failure mode that
// actually matters for a long-running node cycling through plugin
// instances (for example across repeated migration or backup runs).
func AssertRepeatedLifecycleIsSafe(
	t *testing.T,
	iterations int,
	run func(t *testing.T),
) {
	t.Helper()
	for i := 1; i <= iterations; i++ {
		run(t)
		if t.Failed() {
			t.Fatalf("lifecycle cycle %d/%d failed", i, iterations)
		}
	}
}
