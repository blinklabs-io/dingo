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

import (
	"fmt"
	"strconv"

	"github.com/blinklabs-io/dingo/database"
)

// syncKeyImmutableMax records the highest immutable file number imported into
// the blob store by a Mithril sync. A later catch-up reads it to skip
// downloading immutable archives already present and to bound its
// chain-intersection check.
//
// It lives under sync_state but is NOT part of the ephemeral sync_status
// lifecycle: ClearSyncState (run on sync completion) wipes every sync_state
// entry, so this marker must be written AFTER the completion clear so it
// survives across runs. Only the Mithril completion path clears sync_state, so
// nothing in normal `dingo serve` operation removes it.
const syncKeyImmutableMax = "mithril_immutable_max"

// setImmutableImportMarker records num as the highest immutable file number
// imported by a Mithril sync.
func setImmutableImportMarker(db *database.Database, num uint64) error {
	if err := db.SetSyncState(
		syncKeyImmutableMax, strconv.FormatUint(num, 10), nil,
	); err != nil {
		return fmt.Errorf("setting mithril immutable import marker: %w", err)
	}
	return nil
}

// getImmutableImportMarker returns the recorded highest imported immutable file
// number. ok is false when no marker is present (a database bootstrapped before
// this feature, or one that never completed a Mithril sync).
func getImmutableImportMarker(
	db *database.Database,
) (num uint64, ok bool, err error) {
	val, err := db.GetSyncState(syncKeyImmutableMax, nil)
	if err != nil {
		return 0, false, fmt.Errorf(
			"reading mithril immutable import marker: %w", err,
		)
	}
	if val == "" {
		return 0, false, nil
	}
	num, err = strconv.ParseUint(val, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf(
			"parsing mithril immutable import marker %q: %w", val, err,
		)
	}
	return num, true, nil
}
