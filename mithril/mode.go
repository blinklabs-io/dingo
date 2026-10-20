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

package mithril

// Mode is how a serving node uses Mithril while it is behind the certified
// immutable tip.
type Mode int

const (
	// ModeManual leaves Mithril to the explicit `dingo mithril sync` command.
	ModeManual Mode = iota
	// ModeStream replays certified blocks through the ledger for effects.
	ModeStream
	// ModeSnapshot restores a snapshot and backfills block history.
	ModeSnapshot
)

// SelectMode picks the node-managed Mithril mode from mithril.enabled and
// historyExpiry.enabled. Stream mode keeps all history, so a node that
// expires history restores a snapshot instead of replaying it.
func SelectMode(enabled, historyExpiry bool) Mode {
	switch {
	case !enabled:
		return ModeManual
	case historyExpiry:
		return ModeSnapshot
	default:
		return ModeStream
	}
}
