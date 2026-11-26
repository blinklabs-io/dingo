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

package governance

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// RATIFY reads DRep expiries as they were before the boundary's dormancy
// bump (Epoch.lagda.md hands RATIFIES the pre-bump DRep state; the bump only
// reaches the output state). The boundary into epoch 5 is dormant because the
// sole proposal's final chance is epoch 4. Expiry 4 therefore means expired
// for RATIFY, while the bump still moves it to 5 afterwards.
func TestProcessEpochRatifyReadsPreBumpDRepExpiry(t *testing.T) {
	t.Parallel()

	const (
		prevEpoch = uint64(4)
		newEpoch  = uint64(5)
	)
	tests := []struct {
		name string
		// expiredYes: the 70-stake DRep votes Yes and the 30-stake DRep
		// votes No; otherwise the 30-stake DRep votes Yes and the 70-stake
		// DRep does not vote.
		expiredYes bool
		bigExpiry  uint64
		wantRatify bool
	}{
		{
			name:       "expired yes vote does not enact",
			expiredYes: true,
			bigExpiry:  prevEpoch,
			wantRatify: false,
		},
		{
			name:       "expired absent vote leaves the denominator",
			expiredYes: false,
			bigExpiry:  prevEpoch,
			wantRatify: true,
		},
		{
			name:       "control: active yes vote enacts",
			expiredYes: true,
			bigExpiry:  newEpoch,
			wantRatify: true,
		},
		{
			name:       "control: active absent vote stays in the denominator",
			expiredYes: false,
			bigExpiry:  newEpoch,
			wantRatify: false,
		},
	}
	for _, tt := range tests {
		for _, deferred := range []bool{false, true} {
			name := tt.name
			if deferred {
				name += "/deferred"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				db, store := newTallyTestDB(t)
				// Final-chance proposal: expiresAfter = prevEpoch, so no
				// proposal is active in newEpoch and the boundary is dormant.
				proposal := seedNoConfidenceProposal(
					t, db, prevEpoch-2, prevEpoch,
				)
				bigCred := testBytes(28, 0x81)
				smallCred := testBytes(28, 0x82)
				require.NoError(t, store.CreateDrep(nil, &models.Drep{
					Credential:  bigCred,
					Active:      true,
					ExpiryEpoch: tt.bigExpiry,
				}))
				require.NoError(t, store.CreateDrep(nil, &models.Drep{
					Credential:  smallCred,
					Active:      true,
					ExpiryEpoch: 20,
				}))
				seedDRepStake(
					t, store, testBytes(28, 0x83), bigCred,
					models.DrepTypeAddrKeyHash, 70, 0x84,
				)
				seedDRepStake(
					t, store, testBytes(28, 0x85), smallCred,
					models.DrepTypeAddrKeyHash, 30, 0x86,
				)
				pool := testBytes(28, 0x87)
				seedPoolWithStake(
					t, store, pool, testBytes(29, 0x88), 10_000, newEpoch,
				)
				vote := func(cred []byte, v uint8, voterType uint8) {
					require.NoError(t, db.SetGovernanceVote(
						context.Background(),
						&models.GovernanceVote{
							ProposalID:      proposal.ID,
							VoterType:       voterType,
							VoterCredential: cred,
							Vote:            v,
							AddedSlot:       2,
						},
						nil,
					))
				}
				vote(pool, models.VoteYes, models.VoterTypeSPO)
				if tt.expiredYes {
					vote(bigCred, models.VoteYes, models.VoterTypeDRep)
					vote(smallCred, models.VoteNo, models.VoterTypeDRep)
				} else {
					vote(smallCred, models.VoteYes, models.VoterTypeDRep)
				}

				txn := db.MetadataTxn(context.Background(), true)
				defer txn.Release()
				out, err := ProcessEpoch(context.Background(), &EpochInput{
					DB:                       dbadapter.New(db),
					Txn:                      txn,
					PrevEpoch:                prevEpoch,
					NewEpoch:                 newEpoch,
					BoundarySlot:             newEpoch * 100,
					PParams:                  conwayPParamsFixture(10),
					DeferRatification:        deferred,
					BoundarySPOStateDeferred: deferred,
					UpdateFn: func(
						pparams lcommon.ProtocolParameters,
						_ any,
					) (lcommon.ProtocolParameters, error) {
						return pparams, nil
					},
				})
				require.NoError(t, err)
				var ratified int
				if deferred {
					require.NotNil(t, out.Ratification)
					spoState, err := LoadSPOVotingState(
						dbadapter.New(db), txn, newEpoch,
					)
					require.NoError(t, err)
					out.Ratification.SetBoundarySPOState(spoState)
					require.NoError(t, txn.Commit())
					read := db.MetadataTxn(context.Background(), false)
					defer read.Release()
					decision, err := out.Ratification.Decide(
						context.Background(), read,
					)
					require.NoError(t, err)
					ratified = len(decision.Ratified)
				} else {
					require.NoError(t, txn.Commit())
					ratified = out.RatifiedCount
				}
				if tt.wantRatify {
					require.Equal(t, 1, ratified)
				} else {
					require.Equal(t, 0, ratified)
				}

				// The bump itself is kept: stored expiry moves past RATIFY.
				drep, err := db.GetDrepByCredential(
					context.Background(), 0, bigCred, true, nil,
				)
				require.NoError(t, err)
				require.NotNil(t, drep)
				require.Equal(t, tt.bigExpiry+1, drep.ExpiryEpoch)
			})
		}
	}
}
