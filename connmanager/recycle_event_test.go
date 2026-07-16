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
	"log/slog"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/event"
)

// TestHandleConnectionRecycleRequestedEvent_UnexpectedTypeLogged verifies
// that a mismatched event payload is logged rather than silently dropped:
// a recycle request lost with no trace is exactly the kind of swallowed
// failure this handler used to allow.
func TestHandleConnectionRecycleRequestedEvent_UnexpectedTypeLogged(t *testing.T) {
	var logBuf bytes.Buffer
	cm := NewConnectionManager(ConnectionManagerConfig{
		Logger: slog.New(slog.NewTextHandler(&logBuf, nil)),
	})

	// Wrong payload type: the handler asserts ConnectionRecycleRequestedEvent.
	cm.HandleConnectionRecycleRequestedEvent(event.Event{Data: "not-a-recycle-event"})

	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "unexpected event data type") {
		t.Errorf(
			"expected an unexpected-event-type warning in logs, got: %s",
			logOutput,
		)
	}
}
