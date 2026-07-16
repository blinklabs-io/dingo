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

package connmanager

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// TestCreateOutboundConn_InvalidSourcePortLogsWarning verifies that an
// out-of-range OutboundSourcePort (which makes net.ResolveTCPAddr fail)
// is logged instead of silently disabling source-port reuse and dialing
// without a source-port bind.
func TestCreateOutboundConn_InvalidSourcePortLogsWarning(t *testing.T) {
	var logBuf bytes.Buffer
	cm := NewConnectionManager(ConnectionManagerConfig{
		Logger: slog.New(
			slog.NewTextHandler(&logBuf, nil),
		),
		// Out of the valid 16-bit TCP port range, so
		// net.ResolveTCPAddr fails.
		OutboundSourcePort: 100000,
	})

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	// The dial itself is expected to fail (nothing listening), but the
	// resolve failure must be logged rather than silently ignored.
	_, _ = cm.CreateOutboundConn(ctx, "127.0.0.1:1")

	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "failed to resolve source port") {
		t.Errorf(
			"expected a source-port resolve warning in logs, got: %s",
			logOutput,
		)
	}
}
