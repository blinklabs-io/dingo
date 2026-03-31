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

// Package analysis provides assertion helpers that wrap the Antithesis SDK
// when built with the "antithesis" build tag.
package analysis

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
)

// Always asserts that condition holds on every evaluation.
func Always(condition bool, name string, details map[string]interface{}) {
	assert.Always(condition, name, details)
}

// Sometimes asserts that condition is true at least once during the run.
func Sometimes(condition bool, name string, details map[string]interface{}) {
	assert.Sometimes(condition, name, details)
}

// Reachable marks a program point that must be visited at least once.
func Reachable(name string, details map[string]interface{}) {
	assert.Reachable(name, details)
}

// SetupComplete signals that setup is done and fault injection may begin.
func SetupComplete() {
	lifecycle.SetupComplete(map[string]interface{}{})
}
