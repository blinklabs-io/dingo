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
// its callers would never be cataloged.
//
// Importing the SDK also initializes it, and that must not happen in default
// builds. Put each assertion in a small file that carries the antithesis build
// tag and calls the SDK assert package directly with a literal message, and
// give it a counterpart with the opposite tag that does nothing:
//
//	// tip_antithesis.go
//	//go:build antithesis
//
//	func assertTipMonotonic(ok bool, d map[string]any) {
//		assert.Always(ok, "ledger tip never moves backwards", d)
//	}
//
//	// tip_noop.go
//	//go:build !antithesis
//
//	func assertTipMonotonic(bool, map[string]any) {}
//
// Built with the "antithesis" tag, Enabled is true and SetupComplete reaches
// the SDK's lifecycle package. Without the tag, Enabled is false, SetupComplete
// does nothing, and the SDK is not imported.
package antithesis
