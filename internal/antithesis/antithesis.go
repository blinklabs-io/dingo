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

//go:build antithesis

package antithesis

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
)

// Enabled reports whether the Antithesis SDK is compiled in.
const Enabled = true

// Always asserts cond holds every time this call is reached.
func Always(cond bool, message string, details map[string]any) {
	assert.Always(cond, message, details)
}

// AlwaysOrUnreachable asserts cond holds every time this call is reached,
// without requiring that it is reached at all.
func AlwaysOrUnreachable(cond bool, message string, details map[string]any) {
	assert.AlwaysOrUnreachable(cond, message, details)
}

// Sometimes asserts cond holds at least once across the whole test run.
func Sometimes(cond bool, message string, details map[string]any) {
	assert.Sometimes(cond, message, details)
}

// Reachable asserts this call is reached at least once.
func Reachable(message string, details map[string]any) {
	assert.Reachable(message, details)
}

// Unreachable asserts this call is never reached.
func Unreachable(message string, details map[string]any) {
	assert.Unreachable(message, details)
}

// SetupComplete tells Antithesis the system is ready for fault injection.
func SetupComplete(details map[string]any) {
	lifecycle.SetupComplete(details)
}
