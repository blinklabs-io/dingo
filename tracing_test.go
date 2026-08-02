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

package dingo

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWarnIfTracingMisconfigured covers the tracingStdout-without-tracing
// combination: setupTracing only runs when tracing is enabled, so stdout
// export alone exports nothing and must not fail silently.
func TestWarnIfTracingMisconfigured(t *testing.T) {
	tests := []struct {
		name          string
		tracing       bool
		tracingStdout bool
		wantWarning   bool
	}{
		{name: "both disabled"},
		{name: "both enabled", tracing: true, tracingStdout: true},
		{name: "tracing only", tracing: true},
		{
			name:          "stdout without tracing",
			tracingStdout: true,
			wantWarning:   true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			n := &Node{
				config: Config{
					logger:        slog.New(slog.NewJSONHandler(&logs, nil)),
					tracing:       test.tracing,
					tracingStdout: test.tracingStdout,
				},
			}
			n.warnIfTracingMisconfigured()
			if !test.wantWarning {
				require.Empty(t, logs.String())
				return
			}
			require.Contains(t, logs.String(), `"level":"WARN"`)
			require.Contains(t, logs.String(), `"component":"tracing"`)
			require.Contains(
				t,
				logs.String(),
				"tracing stdout export is enabled but tracing is disabled",
			)
			// The operator must be told how to fix it, not just that it is wrong.
			require.Contains(t, logs.String(), "--tracing")
			require.Contains(t, logs.String(), "DINGO_TRACING_ENABLED=true")
		})
	}
}
