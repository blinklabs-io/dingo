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
	"encoding/hex"
	"fmt"
	"strconv"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// LedgerReimportSyncKey marks an import of ledger state the node started on
// its own to recover its live UTxO set, as opposed to one a Mithril sync
// command began. A start that finds it with the import unfinished runs the
// import again.
const LedgerReimportSyncKey = "ledger.state_reimport.active"

// LedgerReimportSwappedSyncKey records, as `<slot>:<hash>`, the point of the
// imported live UTxO set a re-import has made current. A start that finds the
// re-import marks set finishes the run without importing again only when this
// names the ledger's tip: a set at the ledger's tip says nothing by itself,
// since a crash before the import wrote anything leaves the set as it was.
const LedgerReimportSwappedSyncKey = "ledger.state_reimport.swapped"

// LedgerReimportPointMark is the value of LedgerReimportSwappedSyncKey for the
// imported point.
func LedgerReimportPointMark(point ocommon.Point) string {
	return strconv.FormatUint(point.Slot, 10) + ":" + hex.EncodeToString(point.Hash)
}

// DeleteFamilySyncState deletes a sync_state key a ledger-state family owns.
// The migration that moves the family into the LedgerDB calls it for the
// family's keys, which DeleteSyncState refuses once the family is latched.
func (d *Database) DeleteFamilySyncState(key string) error {
	if err := d.metadata.DeleteSyncState(key, nil); err != nil {
		return fmt.Errorf("Database.DeleteFamilySyncState(%q): %w", key, err)
	}
	return nil
}
