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

// Package antithesis holds the build-tag switch for Antithesis instrumentation.
//
// Assertions must NOT be wrapped. The Antithesis SDK requires the message
// argument of every assert call to be a string literal at the call site: the
// instrumentor builds its assertion catalog from those literals, and the
// message names the test property. A forwarding wrapper passes a variable, so
// its callers would never be cataloged. Call sites therefore import the SDK's
// assert package directly and gate on Enabled, which is a constant, so the
// compiler drops the block in default builds:
//
//	if antithesis.Enabled {
//		assert.Always(ok, "ledger tip never moves backwards", details)
//	}
//
// Built with the "antithesis" tag, Enabled is true and SetupComplete reaches
// the SDK's lifecycle package. Without the tag, Enabled is false and
// SetupComplete does nothing.
package antithesis
