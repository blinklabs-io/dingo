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

package utxoset_test

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func baselineRef(b byte) models.StakeCredentialRef {
	return models.NewStakeCredentialRef(0, bytes.Repeat([]byte{b}, 28))
}

// insertSQLBaseline records an import baseline with delegations in the
// metadata store of db.
func insertSQLBaseline(
	t *testing.T,
	raw *sql.DB,
	ref models.StakeCredentialRef,
	pool, drep []byte,
	drepType, added int,
) {
	t.Helper()
	_, err := raw.Exec(
		`INSERT INTO account_import_baseline
		(credential_tag, staking_key, pool, drep, drep_type, active, added_slot, deposit_amount)
		VALUES (?, ?, ?, ?, ?, 1, ?, '2000000')`,
		ref.Tag, ref.Key, pool, drep, drepType, added,
	)
	require.NoError(t, err)
}

// legacySet opens a set whose import baselines were written before it kept
// their delegations: each row holds the active flag, the slot and the
// deposit only, and the baselines are marked loaded.
func legacySet(
	t *testing.T,
	refs ...models.StakeCredentialRef,
) *utxoset.Set {
	t.Helper()
	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		for _, ref := range refs {
			v := []byte{1}
			v = binary.BigEndian.AppendUint64(v, 9)
			v = append(v, 1)
			v = binary.BigEndian.AppendUint64(v, 2_000_000)
			key := append([]byte{'B', ref.Tag}, ref.Key...)
			if err := txn.Set(key, v); err != nil {
				return err
			}
		}
		return txn.Set([]byte("mbaseline"), []byte{1})
	}))
	require.NoError(t, db.Close())
	set, err := utxoset.Open(utxoset.Config{Dir: dir})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	return set
}

func loadBaselines(
	t *testing.T,
	set *utxoset.Set,
	db *database.Database,
) {
	t.Helper()
	require.NoError(t, set.LoadLedgerState(t.Context(), dbadapter.New(db)))
}

func allBaselines(t *testing.T, set *utxoset.Set) []ledger.ImportBaseline {
	t.Helper()
	var out []ledger.ImportBaseline
	var after *models.StakeCredentialRef
	for {
		page, next, err := set.ImportBaselinesAfter(after, 2)
		require.NoError(t, err)
		out = append(out, page...)
		if next == nil {
			return out
		}
		after = next
	}
}

// A set that first loads the import baselines takes the delegations the
// import recorded along with the registration, so an imported account's
// delegation at the anchor survives the metadata store drain.
func TestSetLoadsTheDelegationsOfItsImportBaselines(t *testing.T) {
	t.Parallel()
	set := openRecoverySet(t, utxoset.Config{})
	applySlots(t, set, 8)
	db := ledgerAt(t, slotPoint(8))
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	pool, drep := bytes.Repeat([]byte{0xaa}, 28), bytes.Repeat([]byte{0xbb}, 28)
	insertSQLBaseline(t, raw, baselineRef(1), pool, drep, 1, 9)
	insertSQLBaseline(t, raw, baselineRef(2), nil, nil, 0, 9)
	insertSQLBaseline(t, raw, baselineRef(3), pool, nil, 2, 9)

	loadBaselines(t, set, db)

	lost, err := set.ImportBaselineDelegationsLost()
	require.NoError(t, err)
	require.False(t, lost)
	deposit := uint64(2_000_000)
	require.Equal(t, []ledger.ImportBaseline{
		{Ref: baselineRef(1), Active: true, AddedSlot: 9, Deposit: &deposit,
			Pool: pool, Drep: drep, DrepType: 1},
		{Ref: baselineRef(2), Active: true, AddedSlot: 9, Deposit: &deposit},
		{Ref: baselineRef(3), Active: true, AddedSlot: 9, Deposit: &deposit,
			Pool: pool, DrepType: 2},
	}, allBaselines(t, set), "read in credential order across pages")
}

// A set that holds import baselines written before it kept their delegations
// reloads them while the metadata store still has the rows.
func TestSetReloadsLegacyImportBaselinesWhileTheRowsExist(t *testing.T) {
	t.Parallel()
	set := legacySet(t, baselineRef(1))
	applySlots(t, set, 8)
	db := ledgerAt(t, slotPoint(8))
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	pool := bytes.Repeat([]byte{0xaa}, 28)
	insertSQLBaseline(t, raw, baselineRef(1), pool, nil, 0, 9)

	loadBaselines(t, set, db)

	lost, err := set.ImportBaselineDelegationsLost()
	require.NoError(t, err)
	require.False(t, lost)
	got := allBaselines(t, set)
	require.Len(t, got, 1)
	require.Equal(t, pool, got[0].Pool)
}

// Once the metadata store has drained the rows, the delegations of a legacy
// baseline are in neither store: the set keeps the baseline it has and marks
// the delegations lost rather than reading them as none.
func TestSetMarksLegacyImportBaselineDelegationsLostOnceDrained(t *testing.T) {
	t.Parallel()
	set := legacySet(t, baselineRef(1))
	applySlots(t, set, 8)
	db := ledgerAt(t, slotPoint(8))
	set.SetDrainedFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyAccounts),
	)

	loadBaselines(t, set, db)

	lost, err := set.ImportBaselineDelegationsLost()
	require.NoError(t, err)
	require.True(t, lost)
	b, err := set.ImportBaseline(baselineRef(1))
	require.NoError(t, err)
	deposit := uint64(2_000_000)
	require.Equal(t, &ledger.ImportBaseline{
		Ref: baselineRef(1), Active: true, AddedSlot: 9, Deposit: &deposit,
	}, b, "the registration the set held is still served")
}
