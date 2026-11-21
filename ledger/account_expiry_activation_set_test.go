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

package ledger

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func inactivityMembership(
	t *testing.T,
	ls *LedgerState,
	creds ...[]byte,
) map[string]struct{} {
	t.Helper()
	refs := make([]models.StakeCredentialRef, 0, len(creds))
	for _, c := range creds {
		refs = append(refs, models.NewStakeCredentialRef(0, c))
	}
	in, err := ls.ledgerDB().AccountInactivityActivationMembership(
		context.Background(), refs, nil,
	)
	require.NoError(t, err)
	return in
}

// The activation epoch and the accounts it stamped are journaled at the
// activation's boundary slot: a rollback that keeps the boundary keeps both,
// and one below it leaves neither, so the boundary activates again.
func TestInactivityActivationRollsBackWithItsBoundary(t *testing.T) {
	t.Parallel()

	const inactivity = uint64(2)
	ls, db := newExpiryRollbackTestLedger(t, true, inactivity)
	cred := renewTestCred(0x63)
	expiryRollbackAccount(t, ls, db, 150, models.Account{
		StakingKey: cred, Active: true, CreatedSlot: 150,
	})
	expiryRollbackActivate(t, ls, db, 500, 5)
	expiryRollbackEmptyBlock(t, ls, db, 650)
	expiryRollbackEmptyBlock(t, ls, db, 700)
	key := models.NewStakeCredentialRef(0, cred).MapKey()

	runRollbackRecompute(t, ls, db, 650)
	epoch, activated, err := ls.delegatorInactivityActivationEpoch(nil)
	require.NoError(t, err)
	require.True(t, activated)
	require.Equal(t, uint64(5), epoch)
	require.Contains(t, inactivityMembership(t, ls, cred), key)

	runRollbackRecompute(t, ls, db, 150)
	_, activated, err = ls.delegatorInactivityActivationEpoch(nil)
	require.NoError(t, err)
	require.False(t, activated)
	require.Empty(t, inactivityMembership(t, ls, cred))
	require.Zero(t, expiryRollbackAccountAt(t, ls, cred).ExpirationEpoch)

	expiryRollbackActivate(t, ls, db, 500, 5)
	require.Contains(t, inactivityMembership(t, ls, cred), key)
	require.Equal(
		t, 5+inactivity, expiryRollbackAccountAt(t, ls, cred).ExpirationEpoch,
	)
}

// A store written by a release that kept ledger state in SQL holds the
// activation epoch in sync_state and the stamped credentials in
// account_inactivity_activation. Startup moves both into the set before it
// deletes them, so the set answers as a node that ran the activation itself.
func TestMigratedInactivityActivationKeepsItsMembers(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	meta := db.Metadata()
	stamped := renewTestCred(0x64)
	later := renewTestCred(0x65)
	require.NoError(t, db.SetEpoch(
		500, 5, nil, nil, nil, nil, 1, 1000, 100, nil,
	))
	require.NoError(t, db.CreateAccount(context.Background(), nil, &models.Account{
		StakingKey: stamped, Active: true,
	}))
	_, err = db.StampAllActiveAccountExpirations(context.Background(), 7, nil)
	require.NoError(t, err)
	require.NoError(t, db.CreateAccount(context.Background(), nil, &models.Account{
		StakingKey: later, Active: true, AddedSlot: 510,
	}))
	require.NoError(t, meta.SetSyncState(
		database.DelegatorInactivityActivatedSyncKey, "5", nil,
	))
	// The store's tip is past the activation, so the set's history starts
	// after it and the stamped credentials come from the store's record.
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(600, bytes.Repeat([]byte{0x66}, 32)),
	}, nil))

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger:                     slog.New(slog.NewTextHandler(io.Discard, nil)),
			DelegatorInactivityEnabled: true,
			DelegatorInactivity:        2,
		},
	}
	attachTestLiveSet(t, ls)

	marker, err := meta.GetSyncState(
		database.DelegatorInactivityActivatedSyncKey, nil,
	)
	require.NoError(t, err)
	require.Empty(t, marker, "startup must delete the SQL marker")
	epoch, activated, err := ls.delegatorInactivityActivationEpoch(nil)
	require.NoError(t, err)
	require.True(t, activated)
	require.Equal(t, uint64(5), epoch)
	in := inactivityMembership(t, ls, stamped, later)
	require.Contains(t, in, models.NewStakeCredentialRef(0, stamped).MapKey())
	require.NotContains(t, in, models.NewStakeCredentialRef(0, later).MapKey())
}
