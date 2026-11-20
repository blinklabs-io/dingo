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

package node

import (
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo"
)

// warnLogger sends a node's warnings to the test log.
func warnLogger(t *testing.T) dingo.ConfigOptionFunc {
	return dingo.WithLogger(slog.New(slog.NewTextHandler(
		testLogWriter{t}, &slog.HandlerOptions{Level: slog.LevelWarn},
	)))
}

// testLogWriter writes each log line to the test log.
type testLogWriter struct{ t *testing.T }

func (w testLogWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// maskLiveStakeUpdatedSlot drops reward_live_stake.updated_slot from the
// dumps. It records the slot of the last write to a credential's row, and the
// ledger's own write path and the IndexerDB's patch of the delegations it
// copies write a row at different slots; no read orders or filters by it.
func maskLiveStakeUpdatedSlot(tables ...map[string][]string) {
	for _, t := range tables {
		rows := t["reward_live_stake"]
		for i, row := range rows {
			rows[i] = liveStakeUpdatedSlot.ReplaceAllString(row, "")
		}
		slices.Sort(rows)
	}
}

var liveStakeUpdatedSlot = regexp.MustCompile(`updated_slot=[^|]*\|`)
