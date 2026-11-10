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

package antithesis_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/internal/antithesis"
)

// TestShimAPI exercises every shim function. In the default build they are
// no-ops; with the antithesis tag they reach the SDK, which falls back to a
// local no-op handler outside the Antithesis environment.
func TestShimAPI(t *testing.T) {
	t.Parallel()
	details := map[string]any{"k": "v"}
	antithesis.Always(true, "shim always", details)
	antithesis.AlwaysOrUnreachable(true, "shim always-or-unreachable", details)
	antithesis.Sometimes(true, "shim sometimes", details)
	antithesis.Reachable("shim reachable", details)
	antithesis.Always(true, "shim nil details", nil)
	antithesis.SetupComplete(nil)
}
