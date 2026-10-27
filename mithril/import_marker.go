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
// lifecycle: Mithril completion cleanup removes transient sync_state
// entries, so this marker must be written AFTER the completion clear so it
// survives across runs. Only the Mithril completion path clears sync_state, so
// nothing in normal `dingo serve` operation removes it.
const syncKeyImmutableMax = "mithril_immutable_max"

// RewardStateRepairPendingKey marks a legacy imported database whose reward
// state must be reconciled against a newer certified ledger snapshot before
// it can serve. The metadata migration writes it only when it finds the old
// imported reward-pot row at the recorded Mithril anchor.
const RewardStateRepairPendingKey = "mithril_reward_repair_pending"

// RewardStateRepairActiveKey marks a reward-state repair whose first database
// write has begun. It is ephemeral and is cleared with sync_status when the
// import completes.
const RewardStateRepairActiveKey = "mithril_reward_repair_active"

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

// syncKeyImmutableStreamOnly marks an immutable-import marker that stream
// catch-up created on a database no Mithril snapshot ever populated. Stream
// catch-up applies every block from genesis, so such a database holds the
// full ledger state and WasBootstrapped must not report it. Durable like the
// marker; a later snapshot import's completion clear removes it, and that
// import records the trust boundary WasBootstrapped then keys on.
const syncKeyImmutableStreamOnly = "mithril_immutable_stream_only"

// setStreamImportMarker records num as the highest immutable file whose blocks
// stream catch-up has committed. On a database without a marker that Mithril
// never bootstrapped it first records that the marker is stream-only; the
// flag goes first so an interruption between the writes leaves a flag with no
// marker, which WasBootstrapped ignores.
func setStreamImportMarker(db *database.Database, num uint64) error {
	_, ok, err := getImmutableImportMarker(db)
	if err != nil {
		return err
	}
	if !ok {
		bootstrapped, err := WasBootstrapped(db)
		if err != nil {
			return err
		}
		if !bootstrapped {
			if err := db.SetSyncState(
				syncKeyImmutableStreamOnly, "1", nil,
			); err != nil {
				return fmt.Errorf(
					"marking mithril immutable import marker stream-only: %w",
					err,
				)
			}
		}
	}
	return setImmutableImportMarker(db, num)
}

// WasBootstrapped reports whether db was populated by a Mithril bootstrap. It
// checks the durable immutable-import marker a completed Mithril sync leaves in
// sync_state (written after the completion clear, and never removed by normal
// serve operation), so it stays true for the life of a bootstrapped database.
// Consensus features whose ledger state cannot be reconstructed from a Mithril
// snapshot use it to refuse serving such a database -- notably CIP-0163
// delegator inactivity, whose per-account expiration state is absent from the
// cardano-ledger snapshot and cannot be recovered after import.
//
// Databases bootstrapped before the immutable-import marker existed
// (pre-v0.62.0) carry only the older mithril_ledger_slot trust boundary, so we
// fall back to it. That boundary is written solely by the Mithril import
// completion path (updateMithrilReadyState, sync_import.go) and never by a
// genesis sync, so keying on its presence cannot misclassify a genesis-synced
// database as bootstrapped.
//
// A marker that stream catch-up created (syncKeyImmutableStreamOnly) does not
// count: that database replayed every block and imported no snapshot state.
func WasBootstrapped(db *database.Database) (bool, error) {
	_, ok, err := getImmutableImportMarker(db)
	if err != nil {
		return false, err
	}
	if ok {
		streamOnly, err := db.GetSyncState(syncKeyImmutableStreamOnly, nil)
		if err != nil {
			return false, fmt.Errorf(
				"reading mithril stream-only marker flag: %w", err,
			)
		}
		if streamOnly == "" {
			return true, nil
		}
	}
	// Legacy fallback: the durable trust boundary written by every completed
	// Mithril import before the immutable-import marker was introduced.
	val, err := db.GetSyncState(mithrilLedgerSlotSyncKey, nil)
	if err != nil {
		return false, fmt.Errorf(
			"reading mithril ledger-slot boundary: %w", err,
		)
	}
	return val != "", nil
}

// RewardStateRepairPending reports whether startup must reconcile a legacy
// imported reward state before serving the database.
func RewardStateRepairPending(db *database.Database) (bool, error) {
	value, err := db.GetSyncState(RewardStateRepairPendingKey, nil)
	if err != nil {
		return false, fmt.Errorf(
			"reading Mithril reward repair marker: %w", err,
		)
	}
	return value == "1", nil
}

// RewardStateRepairActive reports whether an interrupted reward-state repair
// is the operation represented by the current in-progress sync status.
func RewardStateRepairActive(db *database.Database) (bool, error) {
	value, err := db.GetSyncState(RewardStateRepairActiveKey, nil)
	if err != nil {
		return false, fmt.Errorf(
			"reading Mithril reward repair activity marker: %w", err,
		)
	}
	return value == "1", nil
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

// syncKeyCatchUpActive marks a catch-up import as having begun writing to the
// database. Unlike syncKeyImmutableMax it IS part of the ephemeral sync
// lifecycle: set alongside the in-progress sync_status when a catch-up starts
// mutating, deleted by completion cleanup. Its only purpose is to
// route an interrupted catch-up back through catch-up semantics on the next
// run — a markerless catch-up (pre-marker database) leaves no other trace, and
// resuming it as a plain bootstrap would skip the reconcile pass and leave
// stale live rows in place forever.
const syncKeyCatchUpActive = "mithril_catchup_active"

// setCatchUpActive records that a catch-up import has started mutating the
// database.
func setCatchUpActive(db *database.Database) error {
	if err := db.SetSyncState(syncKeyCatchUpActive, "1", nil); err != nil {
		return fmt.Errorf("marking mithril catch-up active: %w", err)
	}
	return nil
}

// getCatchUpActive reports whether an interrupted catch-up import was mutating
// this database.
func getCatchUpActive(db *database.Database) (bool, error) {
	val, err := db.GetSyncState(syncKeyCatchUpActive, nil)
	if err != nil {
		return false, fmt.Errorf(
			"reading mithril catch-up active flag: %w", err,
		)
	}
	return val != "", nil
}
