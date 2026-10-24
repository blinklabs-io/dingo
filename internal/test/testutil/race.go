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

package testutil

import "testing"

// SkipUnderRace skips t when the binary is built with -race. Use it only for a
// test whose cost is single-goroutine computation or SQLite query planning,
// where the detector has nothing to observe and multiplies the runtime several
// times over. The same test must still run un-instrumented in CI's check job
// (and, for a service-backed package, gate (services)), and reason must say
// what that test covers, so a reader can tell the skip is not hiding a
// concurrency check.
func SkipUnderRace(t testing.TB, reason string) {
	t.Helper()
	if raceEnabled {
		t.Skip("skipped under -race: " + reason)
	}
}
