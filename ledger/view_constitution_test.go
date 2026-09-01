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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/governance"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// The guardrails rule reaches the enacted constitution through
// common.LedgerState, so drift that stopped *LedgerView from satisfying that
// interface would silently remove the constitution from validation instead
// of failing to build.
var _ lcommon.LedgerState = (*LedgerView)(nil)

// constitutionTestView builds a LedgerView over a file-backed test database,
// so tests that must make the constitution row unreadable can reach the
// underlying SQLite file.
func constitutionTestView(t *testing.T) (*LedgerView, *database.Database) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	ls := &LedgerState{
		db:             db,
		currentPParams: &conway.ConwayProtocolParameters{},
	}
	ls.publishSnapshotsLocked()
	return &LedgerView{ls: ls}, db
}

// constitutionTestWithdrawalTx builds a valid transaction carrying a single
// treasury-withdrawal proposal with the given optional guardrails policy
// hash. Treasury withdrawal is one of the two action types the guardrails
// rule checks, and it needs no ledger state of its own to construct.
func constitutionTestWithdrawalTx(
	t *testing.T,
	policyHash []byte,
) *conway.ConwayTransaction {
	t.Helper()
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		bytes.Repeat([]byte{0x77}, 28),
	)
	require.NoError(t, err)
	action, err := lcommon.NewTreasuryWithdrawalGovAction(
		map[*lcommon.Address]uint64{&address: 1_000_000},
		policyHash,
	)
	require.NoError(t, err)
	wrapper, err := conway.NewConwayGovAction(action)
	require.NoError(t, err)
	return &conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxProposalProcedures: []conway.ConwayProposalProcedure{{
				PPGovAction: wrapper,
			}},
		},
		TxIsValid: true,
	}
}

func constitutionTestGuardrails(
	t *testing.T,
	lv *LedgerView,
	policyHash []byte,
) error {
	t.Helper()
	return conway.UtxoValidateGuardrailsScriptHash(
		constitutionTestWithdrawalTx(t, policyHash),
		1,
		lv,
		&conway.ConwayProtocolParameters{},
	)
}

// TestLedgerViewConstitutionWithPolicyHash proves the stored anchor and
// guardrails policy hash reach the shared ledger-state contract, and that
// guardrails validation consumes them: a proposal carrying the enacted
// policy hash is accepted and one carrying none is rejected. Before the
// mapping existed the view returned an empty common.Constitution, which
// inverted both verdicts.
func TestLedgerViewConstitutionWithPolicyHash(t *testing.T) {
	lv, db := constitutionTestView(t)
	anchorHash := bytes.Repeat([]byte{0xa1}, lcommon.Blake2b256Size)
	policyHash := bytes.Repeat([]byte{0xa2}, lcommon.Blake2b224Size)
	require.NoError(t, db.SetConstitution(&models.Constitution{
		AnchorURL:  "https://example.invalid/with-guardrails",
		AnchorHash: anchorHash,
		PolicyHash: policyHash,
		AddedSlot:  10,
	}, nil))

	got, err := lv.Constitution()
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(
		t,
		"https://example.invalid/with-guardrails",
		got.Anchor.Url,
	)
	require.Equal(t, anchorHash, got.Anchor.DataHash[:])
	require.Equal(t, policyHash, got.ScriptHash)

	require.NoError(t, constitutionTestGuardrails(t, lv, policyHash))

	err = constitutionTestGuardrails(t, lv, nil)
	require.Error(t, err)
	var mismatch conway.InvalidGuardrailsScriptHashError
	require.ErrorAs(t, err, &mismatch)
	require.Equal(t, policyHash, mismatch.Expected)
}

// TestLedgerViewConstitutionWithoutPolicyHash proves a constitution with no
// guardrails script maps to a nil ScriptHash, so guardrails validation
// accepts a proposal that carries no policy hash and rejects one that does.
func TestLedgerViewConstitutionWithoutPolicyHash(t *testing.T) {
	lv, db := constitutionTestView(t)
	anchorHash := bytes.Repeat([]byte{0xb1}, lcommon.Blake2b256Size)
	require.NoError(t, db.SetConstitution(&models.Constitution{
		AnchorURL:  "https://example.invalid/no-guardrails",
		AnchorHash: anchorHash,
		AddedSlot:  10,
	}, nil))

	got, err := lv.Constitution()
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(
		t,
		"https://example.invalid/no-guardrails",
		got.Anchor.Url,
	)
	require.Equal(t, anchorHash, got.Anchor.DataHash[:])
	require.Nil(t, got.ScriptHash)

	require.NoError(t, constitutionTestGuardrails(t, lv, nil))

	err = constitutionTestGuardrails(
		t,
		lv,
		bytes.Repeat([]byte{0xb2}, lcommon.Blake2b224Size),
	)
	require.Error(t, err)
	var mismatch conway.InvalidGuardrailsScriptHashError
	require.ErrorAs(t, err, &mismatch)
	require.Nil(t, mismatch.Expected)
}

// TestLedgerViewConstitutionLatestEnactedWins proves the view reports the
// most recently enacted constitution, including one that drops the
// guardrails script a previous constitution carried.
func TestLedgerViewConstitutionLatestEnactedWins(t *testing.T) {
	lv, db := constitutionTestView(t)
	require.NoError(t, db.SetConstitution(&models.Constitution{
		AnchorURL:  "https://example.invalid/first",
		AnchorHash: bytes.Repeat([]byte{0xc1}, lcommon.Blake2b256Size),
		PolicyHash: bytes.Repeat([]byte{0xc2}, lcommon.Blake2b224Size),
		AddedSlot:  10,
	}, nil))
	secondAnchor := bytes.Repeat([]byte{0xc3}, lcommon.Blake2b256Size)
	require.NoError(t, db.SetConstitution(&models.Constitution{
		AnchorURL:  "https://example.invalid/second",
		AnchorHash: secondAnchor,
		AddedSlot:  20,
	}, nil))

	got, err := lv.Constitution()
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "https://example.invalid/second", got.Anchor.Url)
	require.Equal(t, secondAnchor, got.Anchor.DataHash[:])
	require.Nil(t, got.ScriptHash)
}

// TestLedgerViewConstitutionMissingFailsClosed proves an unpopulated
// constitution store is reported as unavailable, and that guardrails
// validation therefore rejects the proposal with a ConstitutionLookupError
// instead of treating "no constitution recorded" as "no guardrails script
// required".
func TestLedgerViewConstitutionMissingFailsClosed(t *testing.T) {
	lv, _ := constitutionTestView(t)

	got, err := lv.Constitution()
	require.ErrorIs(t, err, governance.ErrConstitutionUnavailable)
	require.Nil(t, got)

	guardrailsErr := constitutionTestGuardrails(t, lv, nil)
	require.Error(t, guardrailsErr)
	var lookup conway.ConstitutionLookupError
	require.ErrorAs(t, guardrailsErr, &lookup)
	require.ErrorIs(
		t,
		guardrailsErr,
		governance.ErrConstitutionUnavailable,
	)
}

// TestLedgerViewConstitutionUnreadableFailsClosed proves a constitution
// store that cannot be read at all fails closed the same way a missing one
// does: the read error is propagated, never flattened into a valid-looking
// constitution with no guardrails script. The propagated error is the
// wrapped store error and not ErrConstitutionUnavailable, which is reserved
// for state that was read and found missing or malformed.
func TestLedgerViewConstitutionUnreadableFailsClosed(t *testing.T) {
	lv, db := constitutionTestView(t)
	require.NoError(t, db.SetConstitution(&models.Constitution{
		AnchorURL:  "https://example.invalid/unreadable",
		AnchorHash: bytes.Repeat([]byte{0xd1}, lcommon.Blake2b256Size),
		PolicyHash: bytes.Repeat([]byte{0xd2}, lcommon.Blake2b224Size),
		AddedSlot:  10,
	}, nil))

	// Confirm the row is readable first, so the assertions below cannot
	// pass because the fixture never had a constitution to lose.
	got, err := lv.Constitution()
	require.NoError(t, err)
	require.NotNil(t, got)

	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec("DROP TABLE constitution")
	require.NoError(t, err)

	got, err = lv.Constitution()
	require.Error(t, err)
	require.Nil(t, got)
	require.NotErrorIs(t, err, governance.ErrConstitutionUnavailable)

	guardrailsErr := constitutionTestGuardrails(t, lv, nil)
	require.Error(t, guardrailsErr)
	var lookup conway.ConstitutionLookupError
	require.ErrorAs(t, guardrailsErr, &lookup)
}
