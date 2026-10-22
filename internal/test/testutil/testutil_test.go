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

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRequireNeverLeavesNoCheckRunning pins why RequireNever exists. A check
// still running after the assertion returns races whatever the test does
// next: one that held a database write transaction outlived the test that
// started it and failed on its cancelled context.
func TestRequireNeverLeavesNoCheckRunning(t *testing.T) {
	t.Parallel()
	var inFlight, calls atomic.Int32
	RequireNever(t, func() bool {
		inFlight.Add(1)
		defer inFlight.Add(-1)
		calls.Add(1)
		// Long enough that a check is under way when the wait ends.
		<-time.After(20 * time.Millisecond)
		return false
	}, 50*time.Millisecond, time.Millisecond, "never satisfied")
	require.Zero(t, inFlight.Load(), "a check was still running on return")
	require.GreaterOrEqual(t, calls.Load(), int32(2))
}
