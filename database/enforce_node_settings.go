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

package database

import (
	"github.com/blinklabs-io/dingo/database/nodesettings"
)

// EnforceNodeSettings validates and persists the gates that only a full node
// startup can know: the era genesis hashes and the ledger-semantics gates
// (history expiry, pledge leverage, full-pot rewards, delegator inactivity,
// minimum pool margin, and the two validation taints). It is called once
// during node startup, after the cardano config has been parsed and before
// the ledger applies its first block; by that point phase 1
// (database.New's CheckNodeSettings) has already validated everything a bare
// database open can know.
//
// Every value in values is treated as explicit, the same rule
// CheckNodeSettings applies to phase 1's gates: the caller here is node.go,
// which assembles values from the fully-resolved node configuration rather
// than from a partial Config, so there is no "not yet known" case to leave
// room for.
//
// The read/evaluate/persist/verify body is shared with CheckNodeSettings via
// evaluateAndPersistGates (database/commit_timestamp.go); this is just that
// call with phase 2's configured map.
func (d *Database) EnforceNodeSettings(values nodesettings.Values) error {
	return d.evaluateAndPersistGates(values)
}
