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

package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	_ "github.com/glebarez/go-sqlite"
	"github.com/stretchr/testify/require"
)

// preprod tip and shape from issue #2756: 648,758 auth_committee_hot rows for
// 35 distinct cold credentials at slot ~79.48M.
const (
	preprodTipSlot         = uint64(79_480_000)
	preprodColdCredentials = 35
	// Scaled down from preprod's ~18,500 authorizations per member to keep
	// the test near ten seconds. The shape is what matters, and the full
	// 35 x 18,500 = 647,500-row dataset was measured with this same code:
	// 647,500 rows before pruning, 1,085 after (99.83% removed), with
	// GetActiveCommitteeMembers identical either side.
	preprodAuthsPerMember = 2_000
)

func credentialHash(seed byte) []byte {
	hash := make([]byte, lcommon.AddressHashSize)
	for i := range hash {
		hash[i] = seed
	}
	return hash
}

func hotHash(seed byte, generation int) []byte {
	hash := credentialHash(seed)
	hash[0] = byte(generation)
	hash[1] = byte(generation >> 8)
	hash[2] = byte(generation >> 16)
	return hash
}

// seedAuthorization writes an auth_committee_hot row directly. Building a
// preprod-sized backlog through the certificate write path would take longer
// than the behavior under test is worth; the write path is exercised
// separately by TestAuthCommitteeHotWritePathPrunesSupersededAuthorizations.
func seedAuthorization(
	t *testing.T,
	store *Store,
	coldTag uint8,
	cold []byte,
	hotTag uint8,
	hot []byte,
	certificateID uint64,
	slot uint64,
) {
	t.Helper()
	_, err := store.writeDB.Exec(`
INSERT INTO auth_committee_hot (
    cold_credential_tag, cold_credential, hot_credential_tag,
    host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?, ?, ?)`,
		coldTag, cold, hotTag, hot, certificateID, slot,
	)
	require.NoError(t, err)
}

func seedResignation(
	t *testing.T,
	store *Store,
	coldTag uint8,
	cold []byte,
	certificateID uint64,
	slot uint64,
) {
	t.Helper()
	_, err := store.writeDB.Exec(`
INSERT INTO resign_committee_cold (
    anchor_url, cold_credential_tag, cold_credential, anchor_hash,
    certificate_id, added_slot
) VALUES ('', ?, ?, NULL, ?, ?)`,
		coldTag, cold, certificateID, slot,
	)
	require.NoError(t, err)
}

func seatMember(
	t *testing.T,
	store *Store,
	coldTag uint8,
	cold []byte,
	termStartSlot uint64,
) {
	t.Helper()
	require.NoError(t, store.SetCommitteeMembers([]*models.CommitteeMember{{
		ColdCredentialTag: coldTag,
		ColdCredHash:      cold,
		ExpiresEpoch:      10_000,
		TermStartSlot:     termStartSlot,
		TermStartSlotSet:  true,
		AddedSlot:         termStartSlot,
	}}, nil))
}

func authRowCount(t *testing.T, store *Store) int {
	t.Helper()
	var count int
	require.NoError(
		t,
		store.writeDB.QueryRow("SELECT COUNT(*) FROM auth_committee_hot").
			Scan(&count),
	)
	return count
}

func authRowCountFor(
	t *testing.T,
	store *Store,
	coldTag uint8,
	cold []byte,
) int {
	t.Helper()
	var count int
	require.NoError(t, store.writeDB.QueryRow(`
SELECT COUNT(*) FROM auth_committee_hot
WHERE cold_credential_tag = ? AND cold_credential = ?`,
		coldTag, cold,
	).Scan(&count))
	return count
}

// applyAuthCertificate drives the production certificate write path --
// applyTransactionCertificates is what Store.SetTransaction calls for every
// applied block -- rather than calling the pruning helper directly.
func applyAuthCertificate(
	t *testing.T,
	store *Store,
	transactionID int64,
	coldTag uint,
	cold []byte,
	hotTag uint,
	hot []byte,
	slot uint64,
) {
	t.Helper()
	certificate := &lcommon.AuthCommitteeHotCertificate{
		CertType: uint(lcommon.CertificateTypeAuthCommitteeHot),
		ColdCredential: lcommon.Credential{
			CredType:   coldTag,
			Credential: lcommon.NewBlake2b224(cold),
		},
		HotCredential: lcommon.Credential{
			CredType:   hotTag,
			Credential: lcommon.NewBlake2b224(hot),
		},
	}
	_, err := store.applyTransactionCertificates(
		context.Background(),
		newDialectQueryer(store.writeDB, store.dialect.Name()),
		transactionID,
		[]lcommon.Certificate{certificate},
		ocommon.Point{Slot: slot, Hash: credentialHash(0x99)},
		0,
		nil,
	)
	require.NoError(t, err)
}

func TestNewRejectsUnsafeCommitteeAuthRetention(t *testing.T) {
	t.Parallel()
	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:sqlstore_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	_, err = New(Config{
		WriteDB:                     db,
		Dialect:                     SQLiteDialect(),
		CommitteeAuthRetentionSlots: DefaultCommitteeAuthRetentionSlots - 1,
	})
	require.ErrorContains(t, err, "below the safe rollback window")
}

func TestAuthCommitteeHotTransactionDrainsMultipleBatches(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	const coldTag = uint8(lcommon.CredentialTypeAddrKeyHash)
	cold := credentialHash(0xc5)
	for i := 1; i <= committeeAuthPruneBatch*2; i++ {
		seedAuthorization(
			t, store, coldTag, cold,
			uint8(lcommon.CredentialTypeAddrKeyHash), hotHash(0x73, i),
			uint64(i), uint64(i), // #nosec G115
		)
	}

	certificates := []lcommon.Certificate{
		&lcommon.AuthCommitteeHotCertificate{
			CertType: uint(lcommon.CertificateTypeAuthCommitteeHot),
			ColdCredential: lcommon.Credential{
				CredType: uint(coldTag), Credential: lcommon.NewBlake2b224(cold),
			},
			HotCredential: lcommon.Credential{
				CredType:   lcommon.CredentialTypeAddrKeyHash,
				Credential: lcommon.NewBlake2b224(hotHash(0x73, 2*committeeAuthPruneBatch+1)),
			},
		},
		&lcommon.AuthCommitteeHotCertificate{
			CertType: uint(lcommon.CertificateTypeAuthCommitteeHot),
			ColdCredential: lcommon.Credential{
				CredType: uint(coldTag), Credential: lcommon.NewBlake2b224(cold),
			},
			HotCredential: lcommon.Credential{
				CredType:   lcommon.CredentialTypeAddrKeyHash,
				Credential: lcommon.NewBlake2b224(hotHash(0x73, 2*committeeAuthPruneBatch+2)),
			},
		},
	}
	_, err := store.applyTransactionCertificates(
		context.Background(),
		newDialectQueryer(store.writeDB, store.dialect.Name()),
		1, certificates,
		ocommon.Point{Slot: preprodTipSlot, Hash: credentialHash(0x99)},
		0, nil,
	)
	require.NoError(t, err)
	// Two certificate calls remove two bounded batches and retain the newest
	// pre-horizon row plus both newly applied authorizations.
	require.Equal(t, 3, authRowCountFor(t, store, coldTag, cold))
}

// TestAuthCommitteeHotWritePathPrunesSupersededAuthorizations is the
// fails-without-fix case: applying a new hot-key authorization must drop the
// credential's older rows that have fallen outside the rollback window,
// instead of leaving every certificate ever seen in the table.
func TestAuthCommitteeHotWritePathPrunesSupersededAuthorizations(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	cold := credentialHash(0xc0)
	const coldTag = uint8(lcommon.CredentialTypeAddrKeyHash)

	// 20 superseded authorizations, all far below the rollback horizon.
	for i := 1; i <= 20; i++ {
		seedAuthorization(
			t, store, coldTag, cold,
			uint8(lcommon.CredentialTypeAddrKeyHash), hotHash(0x40, i),
			uint64(i), uint64(1_000*i), // #nosec G115
		)
	}
	require.Equal(t, 20, authRowCount(t, store))

	applyAuthCertificate(
		t, store, 1,
		uint(coldTag), cold,
		lcommon.CredentialTypeAddrKeyHash, hotHash(0x40, 21),
		preprodTipSlot,
	)

	// Retained: the newest row at or below the horizon (slot 20000) plus the
	// row just written (slot 79480000, inside the rollback window). The other
	// 19 are unreachable by any legal rollback and must be gone.
	require.Equal(t, 2, authRowCount(t, store))
	var retainedSlots []uint64
	rows, err := store.writeDB.Query(
		"SELECT added_slot FROM auth_committee_hot ORDER BY added_slot",
	)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var slot uint64
		require.NoError(t, rows.Scan(&slot))
		retainedSlots = append(retainedSlots, slot)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []uint64{20_000, preprodTipSlot}, retainedSlots)
}

func TestCommitteeHotMaintenancePrunesInactiveCredentialBacklog(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	const coldTag = uint8(lcommon.CredentialTypeAddrKeyHash)
	cold := credentialHash(0xc4)
	for i := 1; i <= committeeAuthPruneBatch*2; i++ {
		seedAuthorization(
			t, store, coldTag, cold,
			uint8(lcommon.CredentialTypeAddrKeyHash), hotHash(0x72, i),
			uint64(i), uint64(i), // #nosec G115
		)
	}
	require.NoError(t, store.SetTip(ochainsync.Tip{
		Point: ocommon.Point{Slot: preprodTipSlot, Hash: []byte("tip")},
	}, nil))

	// No new certificate is applied for this credential. One maintenance cycle
	// must drain successive bounded batches and retain its newest pre-horizon
	// authorization.
	require.NoError(t, store.pruneCommitteeHotAuthorizationsMaintenance(
		context.Background(),
	))
	require.Equal(t, 1, authRowCountFor(t, store, coldTag, cold))
	var retainedSlot uint64
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT added_slot FROM auth_committee_hot WHERE cold_credential = ?",
		cold,
	).Scan(&retainedSlot))
	require.Equal(t, uint64(committeeAuthPruneBatch*2), retainedSlot)
}

func TestCommitteeHotPruningBoundsEachDeleteCall(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	const coldTag = uint8(lcommon.CredentialTypeAddrKeyHash)
	cold := credentialHash(0xc5)
	for i := 1; i <= committeeAuthPruneBatch*2; i++ {
		seedAuthorization(
			t, store, coldTag, cold,
			uint8(lcommon.CredentialTypeAddrKeyHash), hotHash(0x73, i),
			uint64(i), uint64(i), // #nosec G115
		)
	}

	queryer := newDialectQueryer(store.writeDB, store.dialect.Name())
	pruned, err := store.pruneCommitteeHotAuthorizations(
		context.Background(), queryer, coldTag, cold, preprodTipSlot,
	)
	require.NoError(t, err)
	require.Equal(t, int64(committeeAuthPruneBatch), pruned)
	require.Equal(t, committeeAuthPruneBatch, authRowCountFor(t, store, coldTag, cold))
}

// TestAuthCommitteeHotPruningKeepsTallyIdenticalAtPreprodScale builds the
// dataset shape from issue #2756 -- 35 cold credentials each with a long run
// of authorizations -- and proves GetActiveCommitteeMembers returns exactly
// the same tally after pruning as before it. See preprodAuthsPerMember for
// the full-size measurement.
func TestAuthCommitteeHotPruningKeepsTallyIdenticalAtPreprodScale(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	const coldTag = uint8(lcommon.CredentialTypeAddrKeyHash)

	// Multi-row inserts in one transaction: 647,500 single-row round trips
	// through the pure-Go SQLite driver dominate the test otherwise.
	const rowsPerInsert = 500
	values := strings.TrimSuffix(
		strings.Repeat("(?,?,?,?,?,?),", rowsPerInsert), ",",
	)
	insert := `
INSERT INTO auth_committee_hot (
    cold_credential_tag, cold_credential, hot_credential_tag,
    host_credential, certificate_id, added_slot
) VALUES ` + values
	tx, err := store.writeDB.Begin()
	require.NoError(t, err)
	stmt, err := tx.Prepare(insert)
	require.NoError(t, err)
	certificateID := uint64(0)
	spacing := preprodTipSlot / preprodAuthsPerMember
	args := make([]any, 0, rowsPerInsert*6)
	for member := range preprodColdCredentials {
		cold := credentialHash(byte(0x10 + member))
		for generation := 1; generation <= preprodAuthsPerMember; generation++ {
			certificateID++
			args = append(args,
				coldTag, cold,
				uint8(lcommon.CredentialTypeAddrKeyHash),
				hotHash(byte(0x80+member), generation),
				certificateID,
				uint64(generation)*spacing, // #nosec G115
			)
			if len(args) == rowsPerInsert*6 {
				if _, err := stmt.Exec(args...); err != nil {
					require.NoError(t, err)
				}
				args = args[:0]
			}
		}
	}
	require.Empty(t, args, "seed size must divide evenly into insert batches")
	require.NoError(t, stmt.Close())
	require.NoError(t, tx.Commit())

	for member := range preprodColdCredentials {
		seatMember(t, store, coldTag, credentialHash(byte(0x10+member)), 1)
	}

	before := authRowCount(t, store)
	t.Logf("auth_committee_hot rows before pruning: %d", before)
	require.Equal(t, preprodColdCredentials*preprodAuthsPerMember, before)

	tallyBefore, err := store.GetActiveCommitteeMembers(nil)
	require.NoError(t, err)
	require.Len(t, tallyBefore, preprodColdCredentials)

	// GetCommitteeMember is the other reader, reached from
	// LedgerView.CommitteeCredentialMember and so from block validation.
	memberBefore := make([]*models.AuthCommitteeHot, 0, preprodColdCredentials)
	for member := range preprodColdCredentials {
		found, err := store.GetCommitteeMember(
			coldTag, credentialHash(byte(0x10+member)), 1, nil,
		)
		require.NoError(t, err)
		require.NotNil(t, found)
		memberBefore = append(memberBefore, found)
	}

	// Drive the same production pruning call the certificate write path makes,
	// repeatedly, the way a run of real authorization certificates would.
	queryer := newDialectQueryer(store.writeDB, store.dialect.Name())
	total := int64(0)
	for member := range preprodColdCredentials {
		cold := credentialHash(byte(0x10 + member))
		for {
			pruned, err := store.pruneCommitteeHotAuthorizations(
				context.Background(), queryer, coldTag, cold, preprodTipSlot,
			)
			require.NoError(t, err)
			total += pruned
			if pruned == 0 {
				break
			}
		}
	}

	after := authRowCount(t, store)
	t.Logf(
		"auth_committee_hot rows after pruning: %d (deleted %d, %.2f%% removed)",
		after, total, 100*float64(before-after)/float64(before),
	)
	require.Less(t, after, before/100, "pruning must bound the table")

	tallyAfter, err := store.GetActiveCommitteeMembers(nil)
	require.NoError(t, err)
	require.Equal(t, tallyBefore, tallyAfter,
		"GetActiveCommitteeMembers must be identical before and after pruning")

	memberAfter := make([]*models.AuthCommitteeHot, 0, preprodColdCredentials)
	for member := range preprodColdCredentials {
		found, err := store.GetCommitteeMember(
			coldTag, credentialHash(byte(0x10+member)), 1, nil,
		)
		require.NoError(t, err)
		memberAfter = append(memberAfter, found)
	}
	require.Equal(t, memberBefore, memberAfter,
		"GetCommitteeMember must be identical before and after pruning")

	// Every retained row is either inside the rollback window or the single
	// newest row below it, for its own credential.
	horizon := preprodTipSlot - DefaultCommitteeAuthRetentionSlots
	for member := range preprodColdCredentials {
		cold := credentialHash(byte(0x10 + member))
		var belowHorizon int
		require.NoError(t, store.writeDB.QueryRow(`
SELECT COUNT(*) FROM auth_committee_hot
WHERE cold_credential_tag = ? AND cold_credential = ? AND added_slot <= ?`,
			coldTag, cold, horizon,
		).Scan(&belowHorizon))
		require.Equal(t, 1, belowHorizon,
			"exactly one pre-horizon row must survive per credential")
	}
}

// TestAuthCommitteeHotPruningKeepsResignationSuppression covers the negative
// case where a resignation is later than the authorization that pruning
// retains: the member must still be suppressed.
func TestAuthCommitteeHotPruningKeepsResignationSuppression(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	const coldTag = uint8(lcommon.CredentialTypeAddrKeyHash)
	cold := credentialHash(0xc1)
	seatMember(t, store, coldTag, cold, 1)

	for i := 1; i <= 5; i++ {
		seedAuthorization(
			t, store, coldTag, cold,
			uint8(lcommon.CredentialTypeAddrKeyHash), hotHash(0x50, i),
			uint64(i), uint64(1_000*i), // #nosec G115
		)
	}
	// Resignation after every retained authorization but still below the
	// horizon, so pruning cannot be "rescued" by the resignation being recent.
	seedResignation(t, store, coldTag, cold, 100, 6_000)

	membersBefore, err := store.GetActiveCommitteeMembers(nil)
	require.NoError(t, err)
	require.Empty(t, membersBefore, "resigned member must not be in the tally")

	applyAuthCertificate(
		t, store, 1,
		uint(coldTag), cold,
		lcommon.CredentialTypeAddrKeyHash, hotHash(0x50, 6),
		preprodTipSlot,
	)
	// The new authorization is inside the window and the resignation predates
	// it, so the current suppression rule still excludes the member. Pruning
	// must not have changed that.
	membersAfter, err := store.GetActiveCommitteeMembers(nil)
	require.NoError(t, err)
	require.Empty(t, membersAfter,
		"pruning must not resurrect a resigned member")
}

// TestAuthCommitteeHotPruningSurvivesRollbackAcrossPrunedBoundary is the
// reason the retention rule is not simply "keep the latest row". A rollback
// undoes the authorization that superseded the older ones, and the row that
// becomes current again must still be there.
func TestAuthCommitteeHotPruningSurvivesRollbackAcrossPrunedBoundary(t *testing.T) {
	t.Parallel()
	const coldTag = uint8(lcommon.CredentialTypeAddrKeyHash)
	cold := credentialHash(0xc2)
	horizon := preprodTipSlot - DefaultCommitteeAuthRetentionSlots
	// A legal rollback target: inside the rollback window, so at or above the
	// horizon, and below the newest authorization so that authorization is
	// undone.
	rollbackTo := horizon + 100
	newestSlot := preprodTipSlot - 1_000

	build := func(t *testing.T, prune bool) []*models.AuthCommitteeHot {
		t.Helper()
		store := newManagementTestStore(t)
		seatMember(t, store, coldTag, cold, 1)
		for i := 1; i <= 3; i++ {
			seedAuthorization(
				t, store, coldTag, cold,
				uint8(lcommon.CredentialTypeAddrKeyHash), hotHash(0x60, i),
				uint64(i), uint64(1_000*i), // #nosec G115
			)
		}
		seedAuthorization(
			t, store, coldTag, cold,
			uint8(lcommon.CredentialTypeAddrKeyHash), hotHash(0x60, 4),
			4, newestSlot,
		)
		if prune {
			queryer := newDialectQueryer(store.writeDB, store.dialect.Name())
			for {
				pruned, err := store.pruneCommitteeHotAuthorizations(
					context.Background(), queryer, coldTag, cold,
					preprodTipSlot,
				)
				require.NoError(t, err)
				if pruned == 0 {
					break
				}
			}
			require.Equal(t, 2, authRowCountFor(t, store, coldTag, cold),
				"the newest pre-horizon row must be retained alongside the "+
					"in-window row")
		} else {
			require.Equal(t, 4, authRowCountFor(t, store, coldTag, cold))
		}
		require.NoError(t, store.DeleteCertificatesAfterSlot(rollbackTo, nil))
		members, err := store.GetActiveCommitteeMembers(nil)
		require.NoError(t, err)
		return members
	}

	unpruned := build(t, false)
	pruned := build(t, true)
	require.Len(t, unpruned, 1,
		"the pre-rollback-window authorization becomes current again")
	require.Equal(t, hotHash(0x60, 3), unpruned[0].HotCredential)
	require.Equal(t, unpruned, pruned,
		"the post-rollback tally must be identical with and without pruning")
}

// TestAuthCommitteeHotPruningIsPerTaggedCredential covers the case where a
// key-hash and a script-hash cold credential share the same 28 bytes. They are
// different identities, so each credential is pruned only within its own
// (tag, hash) partition and neither loses its newest row.
func TestAuthCommitteeHotPruningIsPerTaggedCredential(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	shared := credentialHash(0xc3)
	const (
		keyTag    = uint8(lcommon.CredentialTypeAddrKeyHash)
		scriptTag = uint8(lcommon.CredentialTypeScriptHash)
	)
	require.NotEqual(t, keyTag, scriptTag)

	for i := 1; i <= 4; i++ {
		seedAuthorization(
			t, store, keyTag, shared,
			uint8(lcommon.CredentialTypeAddrKeyHash), hotHash(0x70, i),
			uint64(i), uint64(1_000*i), // #nosec G115
		)
		seedAuthorization(
			t, store, scriptTag, shared,
			uint8(lcommon.CredentialTypeScriptHash), hotHash(0x71, i),
			uint64(100+i), uint64(1_000*i), // #nosec G115
		)
	}

	// A script-hash authorization at the tip must prune only script-hash rows.
	applyAuthCertificate(
		t, store, 1,
		uint(scriptTag), shared,
		lcommon.CredentialTypeScriptHash, hotHash(0x71, 5),
		preprodTipSlot,
	)
	require.Equal(t, 4, authRowCountFor(t, store, keyTag, shared),
		"a script-hash credential must not prune a key-hash credential "+
			"sharing its hash")
	require.Equal(t, 2, authRowCountFor(t, store, scriptTag, shared))

	applyAuthCertificate(
		t, store, 2,
		uint(keyTag), shared,
		lcommon.CredentialTypeAddrKeyHash, hotHash(0x70, 5),
		preprodTipSlot,
	)
	require.Equal(t, 2, authRowCountFor(t, store, keyTag, shared))
	require.Equal(t, 2, authRowCountFor(t, store, scriptTag, shared))
}
