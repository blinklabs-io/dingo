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
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newDonationTestDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() }) //nolint:errcheck
	return db
}

func networkState(t *testing.T, db *database.Database) (treasury, reserves, slot uint64) {
	t.Helper()
	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	return uint64(state.Treasury), uint64(state.Reserves), state.Slot
}

// TestApplyEpochDonations verifies that the ending epoch's donations are added
// to the treasury at the boundary slot, leaving reserves untouched, and that
// only the ended epoch's donations are moved.
func TestApplyEpochDonations(t *testing.T) {
	db := newDonationTestDB(t)
	ls := &LedgerState{db: db}

	// Post-withdrawal treasury/reserves baseline at an earlier slot.
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))
	// Donations for the ending epoch (7) and a later epoch (8) that must not move.
	require.NoError(t, db.Metadata().AddNetworkDonation(60, 7, 100, nil))
	require.NoError(t, db.Metadata().AddNetworkDonation(70, 7, 200, nil))
	require.NoError(t, db.Metadata().AddNetworkDonation(600, 8, 999, nil))

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyEpochDonations(txn, 7, 80)
	}))

	treasury, reserves, slot := networkState(t, db)
	assert.Equal(t, uint64(1_300), treasury, "treasury += epoch-7 donations (100+200)")
	assert.Equal(t, uint64(5_000), reserves, "reserves untouched by donations")
	assert.Equal(t, uint64(80), slot, "updated at the boundary slot")
}

// TestApplyEpochDonations_NoDonations is a no-op when the ended epoch had none.
func TestApplyEpochDonations_NoDonations(t *testing.T) {
	db := newDonationTestDB(t)
	ls := &LedgerState{db: db}
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyEpochDonations(txn, 7, 80)
	}))

	treasury, reserves, slot := networkState(t, db)
	assert.Equal(t, uint64(1_000), treasury)
	assert.Equal(t, uint64(5_000), reserves)
	assert.Equal(t, uint64(50), slot, "no boundary row written when no donations")
}

// TestEpochDonationWithdrawalRollback exercises the acceptance scenario: a
// treasury withdrawal (modelled as a debited treasury) followed by a donation
// at the boundary, then a rollback past the boundary that restores the prior
// treasury and drops the donation rows so re-application is deterministic.
func TestEpochDonationWithdrawalRollback(t *testing.T) {
	db := newDonationTestDB(t)
	ls := &LedgerState{db: db}

	// Epoch 7 starts with treasury 1_000 (slot 50).
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))
	// A donation block lands mid-epoch.
	require.NoError(t, db.Metadata().AddNetworkDonation(70, 7, 300, nil))
	// At the 7->8 boundary (slot 80) a treasury withdrawal of 400 is enacted
	// first (checked against the pre-donation treasury of 1_000), debiting the
	// treasury to 600...
	require.NoError(t, db.Metadata().SetNetworkState(600, 5_000, 80, nil))
	// ...then the epoch's donations are added on top.
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyEpochDonations(txn, 7, 80)
	}))
	treasury, reserves, slot := networkState(t, db)
	require.Equal(t, uint64(900), treasury, "1_000 - 400 withdrawal + 300 donation")
	require.Equal(t, uint64(5_000), reserves)
	require.Equal(t, uint64(80), slot)

	// Roll back past the boundary (to slot 60): the boundary NetworkState row
	// and the donation row are dropped, restoring epoch 7's starting treasury.
	require.NoError(t, db.DeleteNetworkStateAfterSlot(60, nil))
	require.NoError(t, db.DeleteNetworkDonationsAfterSlot(60, nil))

	treasury, reserves, slot = networkState(t, db)
	assert.Equal(t, uint64(1_000), treasury, "treasury restored to pre-boundary value")
	assert.Equal(t, uint64(5_000), reserves)
	assert.Equal(t, uint64(50), slot)
	sum, err := db.Metadata().SumNetworkDonationsForEpoch(7, nil)
	require.NoError(t, err)
	assert.Zero(t, sum, "rolled-back donation rows are gone")
}

// donationTestConwayPParams builds Conway pparams with voting thresholds so
// governance.ProcessEpoch's ratification phase has the fields it reads.
func donationTestConwayPParams(major uint) *conway.ConwayProtocolParameters {
	rat := func(n, d int64) cbor.Rat { return cbor.Rat{Rat: big.NewRat(n, d)} }
	p := &conway.ConwayProtocolParameters{}
	p.ProtocolVersion.Major = major
	p.MinCommitteeSize = 3
	p.DRepVotingThresholds = conway.DRepVotingThresholds{
		MotionNoConfidence:    rat(67, 100),
		CommitteeNormal:       rat(67, 100),
		CommitteeNoConfidence: rat(60, 100),
		UpdateToConstitution:  rat(75, 100),
		HardForkInitiation:    rat(60, 100),
		PpNetworkGroup:        rat(67, 100),
		PpEconomicGroup:       rat(67, 100),
		PpTechnicalGroup:      rat(67, 100),
		PpGovGroup:            rat(75, 100),
		TreasuryWithdrawal:    rat(67, 100),
	}
	p.PoolVotingThresholds = conway.PoolVotingThresholds{
		MotionNoConfidence:    rat(51, 100),
		CommitteeNormal:       rat(51, 100),
		CommitteeNoConfidence: rat(51, 100),
		HardForkInitiation:    rat(51, 100),
		PpSecurityGroup:       rat(51, 100),
	}
	return p
}

// TestEpochProcessWithdrawalThenDonation drives the real Conway enactment path:
// a ratified treasury withdrawal is enacted by governance.ProcessEpoch and then
// the ending epoch's donation is applied, exactly as processEpochRollover
// sequences them. It proves the withdrawal is checked/applied against the
// pre-donation treasury (the value the ledger uses at the boundary) and the
// donation is added afterwards.
func TestEpochProcessWithdrawalThenDonation(t *testing.T) {
	db := newDonationTestDB(t)
	ls := &LedgerState{db: db}

	const (
		initialTreasury = uint64(1_000)
		initialReserves = uint64(200)
		withdrawal      = uint64(400)
		donation        = uint64(300)
		endedEpoch      = uint64(4)
		boundarySlot    = uint64(500)
	)

	// Registered reward account that the withdrawal pays out to.
	stakeCred := make([]byte, 28)
	for i := range stakeCred {
		stakeCred[i] = 0x42
	}
	withdrawAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	withdrawAddrBytes, err := withdrawAddr.Bytes()
	require.NoError(t, err)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}))

	// A ratified treasury-withdrawal proposal so ProcessEpoch enacts it.
	withdrawalCbor, err := cbor.Encode(&lcommon.TreasuryWithdrawalGovAction{
		Type:        2,
		Withdrawals: map[*lcommon.Address]uint64{&withdrawAddr: withdrawal},
	})
	require.NoError(t, err)
	ratifiedEpoch := endedEpoch
	ratifiedSlot := uint64(400)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        make([]byte, 32),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: 3,
		ExpiresEpoch:  10,
		RatifiedEpoch: &ratifiedEpoch,
		RatifiedSlot:  &ratifiedSlot,
		AnchorURL:     "https://example.invalid/withdrawal",
		AnchorHash:    make([]byte, 32),
		Deposit:       0,
		ReturnAddress: withdrawAddrBytes,
		GovActionCbor: withdrawalCbor,
		AddedSlot:     101,
	}, nil))

	// Initial treasury/reserves and the ending epoch's donation.
	require.NoError(t, db.Metadata().SetNetworkState(
		initialTreasury, initialReserves, 1, nil,
	))
	require.NoError(t, db.Metadata().AddNetworkDonation(
		70, endedEpoch, donation, nil,
	))

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		if _, err := governance.ProcessEpoch(&governance.EpochInput{
			DB:           db,
			Txn:          txn,
			PrevEpoch:    endedEpoch,
			NewEpoch:     endedEpoch + 1,
			BoundarySlot: boundarySlot,
			PParams:      donationTestConwayPParams(10),
			UpdateFn: func(
				p lcommon.ProtocolParameters, _ any,
			) (lcommon.ProtocolParameters, error) {
				return p, nil
			},
		}); err != nil {
			return err
		}
		return ls.applyEpochDonations(txn, endedEpoch, boundarySlot)
	}))

	// Withdrawal (400) was applied against the pre-donation treasury (1000),
	// then the donation (300) was added: 1000 - 400 + 300 = 900.
	treasury, reserves, _ := networkState(t, db)
	assert.Equal(t, uint64(900), treasury,
		"treasury = initial - withdrawal + donation")
	assert.Equal(t, initialReserves, reserves)

	// The withdrawal credited the registered reward account.
	account, err := db.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, withdrawal, uint64(account.Reward),
		"withdrawal paid to the reward account")
}
