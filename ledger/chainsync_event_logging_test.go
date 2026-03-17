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

package ledger

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
)

func newTestLedgerStateWithBuffer() (*LedgerState, *bytes.Buffer) {
	var logBuf bytes.Buffer
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(
				slog.NewJSONHandler(
					&logBuf,
					&slog.HandlerOptions{Level: slog.LevelWarn},
				),
			),
		},
	}
	return ls, &logBuf
}

func assertLogContains(t *testing.T, buf *bytes.Buffer, wants []string) {
	t.Helper()
	logOutput := buf.String()
	for _, want := range wants {
		if !strings.Contains(logOutput, want) {
			t.Fatalf("expected log output to contain %q, got %s", want, logOutput)
		}
	}
}

func TestHandleEventChainsync_WarnsOnUnexpectedEventDataType(t *testing.T) {
	ls, logBuf := newTestLedgerStateWithBuffer()
	evt := event.Event{
		Type:      event.EventType("chainsync.test"),
		Timestamp: time.Date(2026, 3, 16, 12, 0, 0, 0, time.UTC),
		Data:      "unexpected",
	}

	ls.handleEventChainsync(evt)

	assertLogContains(t, logBuf, []string{
		`"msg":"received unexpected event data type"`,
		`"expected":"ChainsyncEvent"`,
		`"data_type":"string"`,
		`"event_type":"chainsync.test"`,
		`"event_timestamp":"2026-03-16T12:00:00Z"`,
		`"event":{"Timestamp":"2026-03-16T12:00:00Z","Data":"unexpected","Type":"chainsync.test"}`,
	})
}

func TestHandleEventBlockfetch_WarnsOnUnexpectedEventDataType(t *testing.T) {
	ls, logBuf := newTestLedgerStateWithBuffer()
	evt := event.Event{
		Type:      event.EventType("blockfetch.test"),
		Timestamp: time.Date(2026, 3, 16, 12, 5, 0, 0, time.UTC),
		Data:      123,
	}

	ls.handleEventBlockfetch(evt)

	assertLogContains(t, logBuf, []string{
		`"msg":"received unexpected event data type"`,
		`"expected":"BlockfetchEvent"`,
		`"data_type":"int"`,
		`"event_type":"blockfetch.test"`,
		`"event_timestamp":"2026-03-16T12:05:00Z"`,
		`"event":{"Timestamp":"2026-03-16T12:05:00Z","Data":123,"Type":"blockfetch.test"}`,
	})
}
