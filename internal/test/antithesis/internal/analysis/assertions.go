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

//go:build !antithesis

// Package analysis provides assertion helpers that wrap Antithesis SDK calls
// (or fall back to structured logging when the antithesis build tag is absent).
package analysis

import "log/slog"

// Always logs an assertion that must hold every time it is evaluated.
// When condition is false the assertion is treated as a failure.
func Always(condition bool, name string, details map[string]interface{}) {
	args := buildLogArgs("always", condition, name, details)
	if condition {
		slog.Info("assertion:always:pass", args...)
	} else {
		slog.Error("assertion:always:fail", args...)
	}
}

// Sometimes logs an assertion that should be true at least once across the
// whole test run.  A single true observation is sufficient to satisfy it.
func Sometimes(condition bool, name string, details map[string]interface{}) {
	args := buildLogArgs("sometimes", condition, name, details)
	if condition {
		slog.Info("assertion:sometimes:pass", args...)
	} else {
		slog.Debug("assertion:sometimes:pending", args...)
	}
}

// Reachable marks a point in the program that must be visited at least once
// during the test run.
func Reachable(name string, details map[string]interface{}) {
	args := buildLogArgs("reachable", true, name, details)
	slog.Info("assertion:reachable", args...)
}

// SetupComplete signals that system setup has finished and the Antithesis
// fuzzer may begin injecting faults.
func SetupComplete() {
	slog.Info("assertion:setup_complete")
}

// buildLogArgs converts the assertion parameters into a flat slog argument
// list suitable for passing to slog.*() variadic functions.
func buildLogArgs(
	kind string,
	condition bool,
	name string,
	details map[string]interface{},
) []any {
	args := make([]any, 0, 6+2*len(details))
	args = append(args,
		"assertion_kind", kind,
		"assertion_name", name,
		"condition", condition,
	)
	for k, v := range details {
		args = append(args, k, v)
	}
	return args
}
