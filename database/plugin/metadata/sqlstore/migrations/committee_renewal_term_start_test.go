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

package migrations

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

type committeeTermFixture struct {
	tag           int64
	hash          []byte
	addedSlot     int64
	deletedSlot   sql.NullInt64
	termStartSlot int64
	wantTermStart int64
}

type enactedProposalFixture struct {
	slot          int64
	actionType    int64
	govActionCbor []byte
}

func deletedAt(slot int64) sql.NullInt64 {
	return sql.NullInt64{Int64: slot, Valid: true}
}

func TestCommitteeRenewalTermStartBackfill(t *testing.T) {
	t.Parallel()
	renewed := bytes.Repeat([]byte{0xaa}, 28)
	reelected := bytes.Repeat([]byte{0xbb}, 28)
	noConfidenceA := bytes.Repeat([]byte{0xcc}, 28)
	noConfidenceB := bytes.Repeat([]byte{0xdd}, 28)
	importedRoot := bytes.Repeat([]byte{0xee}, 28)
	unenacted := bytes.Repeat([]byte{0xf0}, 28)
	parameterChange := bytes.Repeat([]byte{0xf1}, 28)
	importedEnactment := bytes.Repeat([]byte{0xf2}, 28)
	cbor := []byte{0x80}
	proposals := []enactedProposalFixture{
		{1000, updateCommitteeActionType, cbor},
		{2000, updateCommitteeActionType, cbor},
		{3000, updateCommitteeActionType, cbor},
		{4000, int64(lcommon.GovActionTypeNoConfidence), cbor},
		{4500, updateCommitteeActionType, cbor},
		{5000, updateCommitteeActionType, cbor},
		{6000, updateCommitteeActionType, cbor},
		{7500, int64(lcommon.GovActionTypeParameterChange), cbor},
		// The Mithril import's synthetic committee root.
		{8000, updateCommitteeActionType, nil},
		// An imported UpdateCommittee the import records as enacted at its
		// anchor, with the proposal's action CBOR.
		{9000, updateCommitteeActionType, cbor},
	}
	fixtures := []committeeTermFixture{
		// Two renewals each stamped a fresh term start; both inherit the
		// genesis term through the already-repaired middle row.
		{0, renewed, 0, deletedAt(1000), 0, 0},
		{0, renewed, 1000, deletedAt(5000), 900, 0},
		{0, renewed, 5000, sql.NullInt64{}, 4800, 0},
		// A script credential sharing the hash bytes has its own history.
		{1, renewed, 200, deletedAt(1000), 50, 50},
		{1, renewed, 1000, sql.NullInt64{}, 950, 50},
		// Removal at 2000 and re-election at 3000 starts a new term, which a
		// later renewal then carries forward.
		{0, reelected, 100, deletedAt(2000), 100, 100},
		{0, reelected, 3000, deletedAt(6000), 2900, 2900},
		{0, reelected, 6000, sql.NullInt64{}, 5900, 2900},
		// NoConfidence at 4000 removes every member; re-election at 4500
		// starts new terms.
		{0, noConfidenceA, 0, deletedAt(4000), 0, 0},
		{0, noConfidenceA, 4500, sql.NullInt64{}, 4400, 4400},
		{0, noConfidenceB, 0, deletedAt(4000), 0, 0},
		{0, noConfidenceB, 4500, sql.NullInt64{}, 4400, 4400},
		// A Mithril catch-up import restamps at its anchor on purpose, both
		// beside its synthetic root and beside an imported enactment.
		{0, importedRoot, 0, deletedAt(8000), 0, 0},
		{0, importedRoot, 8000, sql.NullInt64{}, 8000, 8000},
		{0, importedEnactment, 0, deletedAt(9000), 0, 0},
		{0, importedEnactment, 9000, sql.NullInt64{}, 9000, 9000},
		// Replacement in place with no enactment behind it is not a renewal.
		{0, unenacted, 0, deletedAt(7000), 0, 0},
		{0, unenacted, 7000, sql.NullInt64{}, 7000, 7000},
		// Only a non-committee action was enacted at the replacement slot.
		{0, parameterChange, 0, deletedAt(7500), 0, 0},
		{0, parameterChange, 7500, sql.NullInt64{}, 7500, 7500},
	}
	runCommitteeRenewalTermStartBackfill(t, proposals, fixtures, true)
}

// Migration v8 backfilled term_start_slot from added_slot, so a renewal
// enacted before it carries a term start equal to its added_slot, the shape
// an import writes. A database that never imported a snapshot cannot hold an
// import row, so such a renewal is still repaired there.
func TestCommitteeRenewalTermStartBackfillRepairsLegacyRenewal(t *testing.T) {
	t.Parallel()
	renewed := bytes.Repeat([]byte{0xaa}, 28)
	proposals := []enactedProposalFixture{
		{1000, updateCommitteeActionType, []byte{0x80}},
	}
	fixtures := []committeeTermFixture{
		{0, renewed, 0, deletedAt(1000), 0, 0},
		{0, renewed, 1000, sql.NullInt64{}, 1000, 0},
	}
	runCommitteeRenewalTermStartBackfill(t, proposals, fixtures, false)
}

func runCommitteeRenewalTermStartBackfill(
	t *testing.T,
	proposals []enactedProposalFixture,
	fixtures []committeeTermFixture,
	mithrilImported bool,
) {
	t.Helper()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := SQLiteRegistry()
	require.NoError(t, err)
	index := len(registry) - 1
	require.Equal(t, committeeRenewalTermStartSchemaRelease, registry[index].Name)
	// One credential per batch, so the cursor is resumed between every
	// credential and a renewal chain is never split.
	registry[index].BatchSize = 1
	locker := NewFileLocker(databasePath + ".migrate.lock")
	runTo := func(versions []Migration) {
		t.Helper()
		runner := Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: versions,
			Locker:   locker,
		}
		require.NoError(t, runner.Run(context.Background()))
	}
	runTo(registry[:index])

	if mithrilImported {
		_, err := db.Exec(
			`INSERT INTO sync_state (sync_key, value) VALUES (?, ?)`,
			"mithril_ledger_slot", "9000",
		)
		require.NoError(t, err)
	}
	for i, proposal := range proposals {
		_, err := db.Exec(`
INSERT INTO governance_proposal (
    tx_hash, action_index, action_type, proposed_epoch, expires_epoch,
    enacted_epoch, enacted_slot, deposit, gov_action_cbor, added_slot
) VALUES (?, 0, ?, 0, 0, 1, ?, 0, ?, 0)`,
			bytes.Repeat([]byte{byte(i + 1)}, 32),
			proposal.actionType, proposal.slot, proposal.govActionCbor,
		)
		require.NoError(t, err)
	}
	ids := make([]int64, len(fixtures))
	for i, fixture := range fixtures {
		require.NoError(t, db.QueryRow(`
INSERT INTO committee_member (
    cold_credential_tag, cold_cred_hash, expires_epoch, term_start_slot,
    term_start_slot_set, added_slot, deleted_slot
) VALUES (?, ?, 100, ?, TRUE, ?, ?) RETURNING id`,
			fixture.tag, fixture.hash, fixture.termStartSlot,
			fixture.addedSlot, fixture.deletedSlot,
		).Scan(&ids[i]))
	}

	runTo(registry)

	for i, fixture := range fixtures {
		var got int64
		require.NoError(t, db.QueryRow(
			`SELECT term_start_slot FROM committee_member WHERE id = ?`,
			ids[i],
		).Scan(&got))
		require.Equal(
			t,
			fixture.wantTermStart,
			got,
			"row %d (tag %d, added_slot %d)",
			i, fixture.tag, fixture.addedSlot,
		)
	}
}

// The backfill matches enacted proposals by the action_type the ledger stores,
// a direct cast of the gouroboros enum, so a wrong copy would silently match
// nothing and repair no row.
func TestUpdateCommitteeActionTypeMatchesLedger(t *testing.T) {
	t.Parallel()
	require.EqualValues(
		t,
		lcommon.GovActionTypeUpdateCommittee,
		updateCommitteeActionType,
	)
}

func TestCommitteeCredentialCursorRoundTrip(t *testing.T) {
	t.Parallel()
	credential := committeeColdCredential{
		tag:  1,
		hash: []byte{0x00, 0x01, 0xfe, 0xff},
	}
	tag, hash, err := parseCommitteeCredentialCursor(
		formatCommitteeCredentialCursor(credential),
	)
	require.NoError(t, err)
	require.Equal(t, credential.tag, tag)
	require.Equal(t, credential.hash, hash)

	for _, cursor := range []string{"1", "x:00", "1:zz"} {
		_, _, err := parseCommitteeCredentialCursor(cursor)
		require.Error(t, err, "cursor %q", cursor)
	}
}
