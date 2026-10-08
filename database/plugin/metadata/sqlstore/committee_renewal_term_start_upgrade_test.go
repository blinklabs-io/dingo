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
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestCommitteeRenewalTermStartUpgradeRestoresHotKey writes a renewal the way
// enactment did before renewals preserved the term start, and a Mithril
// catch-up import over an existing member, both through the store's own
// writers. The renewed member's hot-key authorization predates the stamped
// term start and stays hidden until the v38 upgrade repairs it; the imported
// member's fresh anchor term is left in place.
func TestCommitteeRenewalTermStartUpgradeRestoresHotKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	index := len(registry) - 1
	require.Equal(
		t,
		"committee-renewal-term-start-repair",
		registry[index].Name,
	)
	locker := migrations.NewFileLocker(path + ".migrate.lock")
	openStore := func(versions []migrations.Migration) (*Store, *sql.DB) {
		t.Helper()
		db, err := sql.Open(
			"sqlite",
			"file:"+path+"?_pragma=journal_mode(MEMORY)&_pragma=synchronous(OFF)",
		)
		require.NoError(t, err)
		store, err := New(Config{
			WriteDB:         db,
			Dialect:         SQLiteDialect(),
			Migrations:      versions,
			MigrationLocker: locker,
		})
		require.NoError(t, err)
		require.NoError(t, store.Start(ctx))
		return store, db
	}
	activeColdCredentials := func(store *Store) [][]byte {
		t.Helper()
		active, err := store.GetActiveCommitteeMembers(nil)
		require.NoError(t, err)
		ret := make([][]byte, 0, len(active))
		for _, member := range active {
			ret = append(ret, member.ColdCredential)
		}
		return ret
	}

	renewed := bytes.Repeat([]byte{0xaa}, 28)
	imported := bytes.Repeat([]byte{0xbb}, 28)
	const (
		renewalSlot = 1000
		anchorSlot  = 8000
	)
	store, db := openStore(registry[:index])
	for _, cold := range [][]byte{renewed, imported} {
		require.NoError(t, store.SetCommitteeMembers(
			[]*models.CommitteeMember{{
				ColdCredHash:     cold,
				ExpiresEpoch:     100,
				TermStartSlotSet: true,
				AddedSlot:        0,
			}},
			nil,
		))
		_, err := db.ExecContext(ctx, `
INSERT INTO auth_committee_hot (
    cold_credential_tag, cold_credential, hot_credential_tag,
    host_credential, certificate_id, added_slot
) VALUES (0, ?, 0, ?, 1, 500)`, cold, bytes.Repeat([]byte{0xcc}, 28))
		require.NoError(t, err)
	}
	// The pre-fix enactment stamped the proposal's slot on the renewal.
	require.NoError(t, store.SetCommitteeMembers(
		[]*models.CommitteeMember{{
			ColdCredHash:     renewed,
			ExpiresEpoch:     200,
			TermStartSlot:    900,
			TermStartSlotSet: true,
			AddedSlot:        renewalSlot,
		}},
		nil,
	))
	setEnactedUpdateCommittee(t, store, 1, renewalSlot, []byte{0x80})
	// A catch-up import restamps the member at the snapshot anchor and seeds
	// a synthetic committee root without action CBOR.
	require.NoError(t, store.SetCommitteeMembers(
		[]*models.CommitteeMember{{
			ColdCredHash:     imported,
			ExpiresEpoch:     300,
			TermStartSlot:    anchorSlot,
			TermStartSlotSet: true,
			AddedSlot:        anchorSlot,
		}},
		nil,
	))
	setEnactedUpdateCommittee(t, store, 2, anchorSlot, nil)
	require.Empty(
		t,
		activeColdCredentials(store),
		"both authorizations predate the stored term starts before the upgrade",
	)
	require.NoError(t, store.Close())

	store, _ = openStore(registry)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.Equal(t, [][]byte{renewed}, activeColdCredentials(store))
}

func setEnactedUpdateCommittee(
	t *testing.T,
	store *Store,
	seed byte,
	slot uint64,
	govActionCbor []byte,
) {
	t.Helper()
	epoch := uint64(1)
	require.NoError(t, store.SetGovernanceProposal(
		&models.GovernanceProposal{
			TxHash:        bytes.Repeat([]byte{seed}, 32),
			ActionType:    uint8(lcommon.GovActionTypeUpdateCommittee),
			EnactedEpoch:  &epoch,
			EnactedSlot:   &slot,
			AnchorHash:    make([]byte, 32),
			ReturnAddress: make([]byte, 29),
			GovActionCbor: govActionCbor,
		},
		nil,
	))
}
