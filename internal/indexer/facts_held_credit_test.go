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

package indexer

import (
	"context"
	"database/sql"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

var (
	heldKeyPresent = []byte("stake-credential-present-0123456")
	heldKeyMissing = []byte("stake-credential-missing-0123456")
	heldTxPresent  = []byte("deposit-to-a-registered-account!")
	heldTxMissing  = []byte("deposit-to-a-missing-account!!!!")
)

// heldCreditFixture is a ledger journal with a credit for an account the
// IndexerDB holds and one for an account it does not.
type heldCreditFixture struct {
	f      *MetadataFacts
	dstRaw *sql.DB
}

func newHeldCreditFixture(t *testing.T) *heldCreditFixture {
	t.Helper()
	src, srcRaw := newFactsDB(t)
	dst, dstRaw := newFactsDB(t)
	f := newStoreFacts(t, src, dst)
	_, err := dstRaw.Exec(`INSERT INTO account
		(staking_key, credential_tag, added_slot, created_slot, reward, active)
		VALUES (?, 0, 10, 10, '5', 1)`, heldKeyPresent)
	require.NoError(t, err)
	for _, c := range []struct{ key, tx []byte }{
		{heldKeyPresent, heldTxPresent}, {heldKeyMissing, heldTxMissing},
	} {
		_, err = srcRaw.Exec(`INSERT INTO account_reward_delta
			(staking_key, credential_tag, tx_hash, amount, added_slot, withdrawal, post_snapshot)
			VALUES (?, 0, ?, '20', 120, 0, 0)`, c.key, c.tx)
		require.NoError(t, err)
	}
	return &heldCreditFixture{f: f, dstRaw: dstRaw}
}

func (h *heldCreditFixture) copyFacts(t *testing.T) error {
	t.Helper()
	txn := h.f.dst.Transaction(context.Background(), true)
	return txn.Do(func(txn *database.Txn) error {
		return h.f.CopyFacts(context.Background(), txn, 100, 200)
	})
}

func (h *heldCreditFixture) patchFacts(t *testing.T) error {
	t.Helper()
	txn := h.f.dst.Transaction(context.Background(), true)
	return txn.Do(func(txn *database.Txn) error {
		return h.f.PatchFacts(context.Background(), txn, 100, 200)
	})
}

func (h *heldCreditFixture) registerMissing(t *testing.T) {
	t.Helper()
	_, err := h.dstRaw.Exec(`INSERT INTO account
		(staking_key, credential_tag, added_slot, created_slot, reward, active)
		VALUES (?, 0, 110, 110, '0', 1)`, heldKeyMissing)
	require.NoError(t, err)
}

func (h *heldCreditFixture) reward(t *testing.T, key []byte) string {
	t.Helper()
	return scalar[string](
		t, h.dstRaw, `SELECT reward FROM account WHERE staking_key = ?`, key,
	)
}

// A credit to an account the segment's blocks register has no account to
// credit while the copy runs before them. The copy applies the credits whose
// accounts exist, and the patch step applies the held one once the blocks have
// registered the account.
func TestMetadataFactsHoldsCreditForAccountRegisteredByTheSegment(
	t *testing.T,
) {
	t.Parallel()
	h := newHeldCreditFixture(t)

	require.NoError(t, h.copyFacts(t))
	require.Equal(t, "25", h.reward(t, heldKeyPresent))
	require.Equal(
		t, 1, scalar[int](t, h.dstRaw, `SELECT COUNT(*) FROM account_reward_delta`),
		"only the credit to the registered account is journaled",
	)

	h.registerMissing(t)
	require.NoError(t, h.patchFacts(t))
	require.Equal(t, "20", h.reward(t, heldKeyMissing))
	require.Equal(t, "25", h.reward(t, heldKeyPresent))

	// The held credit is spent: another patch moves nothing.
	require.NoError(t, h.patchFacts(t))
	require.Equal(t, "20", h.reward(t, heldKeyMissing))
}

// The blocks of a direct-deposit transaction credit the account themselves, in
// the journal, ahead of the patch step; the held replay must not credit it
// again.
func TestMetadataFactsHeldCreditAlreadyAppliedByBlocksIsNotRepeated(
	t *testing.T,
) {
	t.Parallel()
	h := newHeldCreditFixture(t)
	require.NoError(t, h.copyFacts(t))
	h.registerMissing(t)

	txn := h.f.dst.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return h.f.dst.AddAccountRewardByCredential(
			context.Background(), 0, heldKeyMissing, 20, 120, heldTxMissing, txn,
		)
	}))
	require.Equal(t, "20", h.reward(t, heldKeyMissing))

	require.NoError(t, h.patchFacts(t))
	require.Equal(t, "20", h.reward(t, heldKeyMissing))
}

// A held credit whose account the segment's blocks did not register is an
// error, not a credit to drop.
func TestMetadataFactsHeldCreditWithoutAccountFailsPatch(t *testing.T) {
	t.Parallel()
	h := newHeldCreditFixture(t)
	require.NoError(t, h.copyFacts(t))

	require.ErrorIs(t, h.patchFacts(t), models.ErrAccountNotFound)
}
