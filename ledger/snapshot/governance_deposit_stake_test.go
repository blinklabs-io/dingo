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

package snapshot

import (
	"bytes"
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// seedPendingGovernanceDepositFixture builds a pool and one delegator
// registered and delegated through certificate history, holding a UTxO worth
// utxoStake, who has also submitted a still-open governance-action proposal
// with the given deposit. Nothing enacts, expires, or drops the proposal, so
// its deposit stays outstanding: already spent out of the delegator's UTxO
// (real Preview evidence: a proposal-submission transaction is what produced
// the only UTxO left for such a delegator) but not yet returned to their
// reward account either.
//
// This is the shape behind the confirmed Preview incident: pool
// 897d60f7c4f8915f807b944c351cc202a953e0ffea5c578086673108's mark[995]
// snapshot undercounted one delegator by exactly two open governance-action
// deposits (100,000 ADA each) still outstanding at the boundary, because
// nothing in reward_live_stake or the historical UTxO/certificate
// reconstruction ever added a pending deposit back onto its depositor.
func seedPendingGovernanceDepositFixture(
	t *testing.T,
	db *database.Database,
	poolHash []byte,
	utxoStake uint64,
	deposit uint64,
) []byte {
	t.Helper()
	require.NoError(t, db.ImportPool(nil, &models.Pool{
		PoolKeyHash: poolHash,
		VrfKeyHash:  make([]byte, 32),
		Pledge:      1_000_000,
		Cost:        340_000_000,
		Margin:      &types.Rat{Rat: big.NewRat(1, 100)},
	}, &models.PoolRegistration{
		PoolKeyHash: poolHash,
		AddedSlot:   50,
		Pledge:      1_000_000,
		Cost:        340_000_000,
		Margin:      &types.Rat{Rat: big.NewRat(1, 100)},
		VrfKeyHash:  make([]byte, 32),
	}), "import pool")

	raw := snapshotSQLDB(t, db)
	stakeKey := bytes.Repeat([]byte{0x7a}, 28)
	regCertID := seedCertificate(
		t, raw, 100, 0, 0, lcommon.CertificateTypeStakeRegistration,
	)
	seedStakeRegistration(t, raw, models.StakeRegistration{
		StakingKey:    stakeKey,
		AddedSlot:     100,
		CertificateID: regCertID,
	})
	delCertID := seedCertificate(
		t, raw, 100, 0, 1, lcommon.CertificateTypeStakeDelegation,
	)
	seedStakeDelegation(t, raw, models.StakeDelegation{
		StakingKey:    stakeKey,
		PoolKeyHash:   poolHash,
		AddedSlot:     100,
		CertificateID: delCertID,
	})

	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: stakeKey,
		Pool:       poolHash,
		AddedSlot:  100,
		Active:     true,
	}), "create account")
	require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
		TxId:       bytes.Repeat([]byte{0x03}, 32),
		OutputIdx:  0,
		StakingKey: stakeKey,
		Amount:     types.Uint64(utxoStake),
		AddedSlot:  150,
	}), "create remaining utxo after the deposit-paying transaction")

	// A reward-account address is a one-byte header (key-hash, testnet) plus
	// the 28-byte credential hash -- see database/models/governance.go's
	// ReturnAddress doc and ledger/governance's rewardAccountStakeCredential,
	// which decodes the refund side of this same column.
	returnAddress := append([]byte{0xE0}, stakeKey...)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        bytes.Repeat([]byte{0x04}, 32),
		ActionIndex:   0,
		ActionType:    4, // InfoAction-shaped placeholder; type is irrelevant here
		ProposedEpoch: 0,
		ExpiresEpoch:  10,
		Deposit:       deposit,
		ReturnAddress: returnAddress,
		AddedSlot:     150,
		// EnactedEpoch/EnactedSlot, ExpiredEpoch/ExpiredSlot, and
		// DroppedEpoch/DroppedSlot are deliberately left nil: the proposal is
		// still open, and its deposit has not been refunded by any route.
	}, nil), "seed still-open governance proposal")

	return stakeKey
}

// TestCaptureEpochBoundaryCountsPendingGovernanceProposalDeposit reproduces
// the confirmed Preview incident: a delegator with an open governance-action
// proposal must still have their outstanding deposit counted as part of
// their pool's active stake, on both capture routes, or the mark snapshot
// permanently understates the pool by the deposit amount for as long as the
// proposal stays open -- silently tightening that pool's Praos leader
// threshold below what the real chain (and any other correctly-implemented
// node) computes.
func TestCaptureEpochBoundaryCountsPendingGovernanceProposalDeposit(t *testing.T) {
	const (
		utxoStake = uint64(700)
		deposit   = uint64(300)
	)
	for _, tc := range []struct {
		name        string
		computeSnap bool
	}{
		{
			name:        "authoritative SNAP-point path (live aggregate)",
			computeSnap: true,
		},
		{
			name:        "event-driven fallback (historical reconstruction)",
			computeSnap: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := setupTestDB(t)
			seedEpochs(t, db, []models.Epoch{
				{EpochId: 0, StartSlot: 0, LengthInSlots: 432_000},
			})
			poolHash := bytes.Repeat([]byte{0xc3}, 28)
			seedPendingGovernanceDepositFixture(
				t, db, poolHash, utxoStake, deposit,
			)

			mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
			evt := event.EpochTransitionEvent{
				PreviousEpoch:   0,
				NewEpoch:        1,
				BoundarySlot:    432_000,
				EpochNonce:      []byte{0x0a, 0x0b},
				ProtocolVersion: 8,
				SnapshotSlot:    431_999,
			}

			txn := db.Transaction(true)
			if tc.computeSnap {
				require.NoError(t, mgr.ComputeEpochBoundarySnapshot(
					context.Background(), txn, evt,
				))
			}
			require.NoError(t, mgr.CaptureEpochBoundarySnapshot(
				context.Background(), txn, evt,
			))
			require.NoError(t, txn.Commit())

			poolSnapshot, err := db.Metadata().GetPoolStakeSnapshot(
				1, "mark", poolHash, nil,
			)
			require.NoError(t, err)
			require.NotNil(t, poolSnapshot)
			require.Equal(
				t,
				utxoStake+deposit,
				uint64(poolSnapshot.TotalStake),
				"the mark snapshot must add the delegator's still-open "+
					"governance-action deposit back onto their stake; "+
					"omitting it understates the pool exactly as the "+
					"confirmed Preview pool 897d60f7... mark[995] incident did",
			)
		})
	}
}
