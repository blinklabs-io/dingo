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
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// runBoundaries applies n boundaries through the cycle.
func runBoundaries(
	t *testing.T,
	c *rollbackCycle,
	n int,
) []cycleResult {
	t.Helper()
	epoch, pparams := c.s.epoch, c.s.pparams
	results := make([]cycleResult, 0, n)
	for range n {
		r := c.run(t, epoch, pparams)
		results = append(results, r)
		epoch, pparams = r.epoch, r.pparams
	}
	return results
}

// requireChanged fails unless some boundary changed each table: a rollback
// restoring a table no boundary touched proves nothing about its rollback step.
func requireChanged(t *testing.T, results []cycleResult, tables ...string) {
	t.Helper()
	for _, table := range tables {
		changed := false
		for _, r := range results {
			changed = changed || r.changed[table]
		}
		require.True(t, changed, "no boundary changed %s", table)
	}
}

// addAccount registers a stake account the scenario's account-certificate
// seeding then backs with registration, delegation and vote-delegation rows.
func (s *govDiffScenario) addAccount(
	t *testing.T,
	marker byte,
	drep []byte,
	pool string,
) []byte {
	t.Helper()
	cred := repeatByte(28, marker)
	acct := &models.Account{
		StakingKey: cred,
		Drep:       drep,
		AddedSlot:  s.epoch.StartSlot,
		Active:     true,
	}
	if pool != "" {
		acct.Pool = []byte(pool)
	}
	require.NoError(t, s.db.CreateAccount(context.Background(), nil, acct))
	s.credentials = append(s.credentials, cred)
	return cred
}

func (s *govDiffScenario) enacted(t *testing.T, marker byte) bool {
	t.Helper()
	return s.proposalByMarker(t, marker).EnactedEpoch != nil
}

func constitutionAction(url string) *lcommon.NewConstitutionGovAction {
	a := &lcommon.NewConstitutionGovAction{
		Type: uint(lcommon.GovActionTypeNewConstitution),
	}
	a.Constitution.Anchor.Url = url
	a.Constitution.ScriptHash = repeatByte(28, 0xc7)
	return a
}

// voteAll casts the DRep, SPO and CC yes votes that carry any action.
func (s *govDiffScenario) voteAll(
	t *testing.T,
	p *models.GovernanceProposal,
	cc bool,
) {
	t.Helper()
	slot := s.epoch.StartSlot - 5
	s.castVote(t, p, models.VoterTypeDRep, s.drep, models.VoteYes, slot)
	s.castVote(t, p, models.VoterTypeSPO, []byte(hfrLiveYesPool), models.VoteYes, slot)
	if cc {
		s.castVote(t, p, models.VoterTypeCC, s.hotCredential, models.VoteYes, slot)
	}
}

func (s *govDiffScenario) propose(
	t *testing.T,
	marker byte,
	typ lcommon.GovActionType,
	action any,
) *models.GovernanceProposal {
	t.Helper()
	return s.addProposal(
		t, marker, typ, action, s.epoch.EpochId-1, s.epoch.EpochId+20, nil,
		s.epoch.StartSlot-10,
	)
}

// Enacting a new constitution, a motion of no confidence or a hard fork
// changes state that the proposal's own row does not carry. Each rollback
// entry point must restore it, and the replay must enact it identically.
func TestRollbackAcrossGovernanceEnactmentsRestoresEveryTable(t *testing.T) {
	t.Parallel()
	for _, entry := range []struct {
		name   string
		ledger bool
	}{{"database", false}, {"ledger", true}} {
		t.Run("constitution/"+entry.name, func(t *testing.T) {
			t.Parallel()
			s := newGovDiffScenarioWithOptions(t, govDiffScenarioOptions{noProposals: true})
			prior := &models.Constitution{
				AnchorURL:  "https://example.invalid/genesis-constitution",
				AnchorHash: repeatByte(32, 0xc1),
				AddedSlot:  1,
			}
			require.NoError(t, s.db.SetConstitution(prior, nil))
			p := s.propose(t, 0x81, lcommon.GovActionTypeNewConstitution,
				constitutionAction("https://example.invalid/new-constitution"))
			s.voteAll(t, p, true)
			c := newRollbackCycle(t, s, entry.ledger, false, 1, 3)
			results := runBoundaries(t, c, 3)
			require.True(t, s.enacted(t, 0x81))
			requireChanged(t, results, "constitution")
			got, err := s.db.GetConstitution(nil)
			require.NoError(t, err)
			require.Equal(t, "https://example.invalid/new-constitution", got.AnchorURL)
		})
		t.Run("no confidence/"+entry.name, func(t *testing.T) {
			t.Parallel()
			s := newGovDiffScenarioWithOptions(t, govDiffScenarioOptions{noProposals: true})
			p := s.propose(t, 0x83, lcommon.GovActionTypeNoConfidence,
				&lcommon.NoConfidenceGovAction{
					Type: uint(lcommon.GovActionTypeNoConfidence),
				})
			s.voteAll(t, p, false)
			c := newRollbackCycle(t, s, entry.ledger, false, 1, 3)
			results := runBoundaries(t, c, 3)
			require.True(t, s.enacted(t, 0x83))
			requireChanged(t, results, "committee_member", "committee_quorum")
			members, err := s.db.GetCommitteeMembers(context.Background(), nil)
			require.NoError(t, err)
			require.Empty(t, members, "no confidence must remove the committee")
		})
		t.Run("hard fork/"+entry.name, func(t *testing.T) {
			t.Parallel()
			s := newGovDiffScenarioWithOptions(t, govDiffScenarioOptions{noProposals: true})
			hf := &lcommon.HardForkInitiationGovAction{
				Type: uint(lcommon.GovActionTypeHardForkInitiation),
			}
			hf.ProtocolVersion.Major = 11
			p := s.propose(t, 0x85, lcommon.GovActionTypeHardForkInitiation, hf)
			s.voteAll(t, p, true)
			c := newRollbackCycle(t, s, entry.ledger, false, 1, 3)
			before := s.pparams
			results := runBoundaries(t, c, 3)
			require.True(t, s.enacted(t, 0x85))
			requireChanged(t, results, "pparams")
			require.NotEqual(t, before, s.pparams)
			require.Equal(t, uint(11), pparamsMajor(t, s.pparams))
		})
	}
}

func pparamsMajor(t *testing.T, pp lcommon.ProtocolParameters) uint {
	t.Helper()
	v, err := GetProtocolVersion(pp)
	require.NoError(t, err)
	return v.Major
}

// The ledger-level rollback must restore every table the database sweep does,
// and reload the in-memory protocol parameters and epoch.
func TestLedgerRollbackAcrossGovernanceBoundariesRestoresEveryTable(
	t *testing.T,
) {
	t.Parallel()
	for _, deferred := range []bool{false, true} {
		name := "ratified at boundary"
		if deferred {
			name = "ratification deferred"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := newGovDiffScenario(t)
			c := newRollbackCycle(t, s, true, deferred, 1, 4)
			results := runBoundaries(t, c, 4)
			requireChanged(t, results,
				"governance_proposal", "committee_member", "pparams",
				"network_state", "reward_ada_pots")
		})
	}
}

// The Plomin hard fork clears a vote delegation to a DRep that is not
// registered. Nothing but the boundary writes that, so a rollback has to
// restore the delegation from the vote-delegation certificate, and the replay
// has to clear it again.
func TestRollbackAcrossPlominHardForkRestoresDRepDelegations(t *testing.T) {
	t.Parallel()
	for _, entry := range []struct {
		name   string
		ledger bool
	}{{"database", false}, {"ledger", true}} {
		t.Run(entry.name, func(t *testing.T) {
			t.Parallel()
			s := newGovDiffScenarioWithOptions(t, govDiffScenarioOptions{noProposals: true})
			bootstrap := donationTestConwayPParams(9)
			bootstrap.MinCommitteeSize = 1
			bootstrap.CommitteeTermLimit = 100
			s.pparams = bootstrap
			s.ls.currentPParams = bootstrap
			dangling := s.addAccount(t, 0xf1, repeatByte(28, 0xf2), hfrLiveYesPool)
			hf := &lcommon.HardForkInitiationGovAction{
				Type: uint(lcommon.GovActionTypeHardForkInitiation),
			}
			hf.ProtocolVersion.Major = 10
			p := s.propose(t, 0x87, lcommon.GovActionTypeHardForkInitiation, hf)
			s.voteAll(t, p, true)
			c := newRollbackCycle(t, s, entry.ledger, false, 1, 3)
			results := runBoundaries(t, c, 3)
			require.True(t, s.enacted(t, 0x87))
			require.Equal(t, uint(10), pparamsMajor(t, s.pparams))
			requireChanged(t, results, "pparams", "account")
			account, err := s.db.GetAccountByCredential(
				context.Background(), 0, dangling, true, nil,
			)
			require.NoError(t, err)
			require.Empty(t, account.Drep,
				"the dangling delegation must be cleared by the hard fork")
		})
	}
}

// Votes and treasury donations in blocks above the rollback target are
// rolled back with them. A vote there is decisive: the proposal ratifies only
// because of it, so a rollback that left the vote behind, or a replay that
// dropped it, changes the ledger's governance outcome.
func TestRollbackRestoresVotesAndDonationsAboveTheTarget(t *testing.T) {
	t.Parallel()
	const gap = 50
	type variant struct {
		name string
		// fork replays a chain without the post-target votes and donation.
		fork bool
	}
	variants := []variant{{"same chain", false}, {"fork without the votes", true}}
	for _, entry := range []struct {
		name   string
		ledger bool
	}{{"database", false}, {"ledger", true}} {
		for _, v := range variants {
			name := entry.name + "/" + v.name
			t.Run("new vote/"+name, func(t *testing.T) {
				t.Parallel()
				s := newGovDiffScenarioWithOptions(t, govDiffScenarioOptions{noProposals: true})
				hf := &lcommon.HardForkInitiationGovAction{
					Type: uint(lcommon.GovActionTypeHardForkInitiation),
				}
				hf.ProtocolVersion.Major = 11
				p := s.propose(t, 0x85, lcommon.GovActionTypeHardForkInitiation, hf)
				slot := s.epoch.StartSlot - 5
				s.castVote(t, p, models.VoterTypeDRep, s.drep, models.VoteYes, slot)
				s.castVote(t, p, models.VoterTypeCC, s.hotCredential, models.VoteYes, slot)
				first := s.epoch.EpochId + 1
				boundaries := 3
				rp := s.addRewardPool(t)
				c := newRollbackCycle(t, s, entry.ledger, false, gap, boundaries)
				c.postTarget = func(t *testing.T, next models.Epoch, _ bool) {
					target := next.StartSlot - gap
					if next.EpochId == first {
						// The pool re-registers with new terms above the
						// target; the rollback must restore the old ones.
						margin := &types.Rat{Rat: big.NewRat(1, 4)}
						require.NoError(t, s.db.ImportPool(
							context.Background(), nil,
							&models.Pool{
								PoolKeyHash: rp.key, VrfKeyHash: make([]byte, 32),
								Pledge: 700, Cost: 2_000, Margin: margin,
								RewardAccount: rp.owner,
							},
							&models.PoolRegistration{
								PoolKeyHash: rp.key, AddedSlot: target + 3,
								Pledge: 700, Cost: 2_000, Margin: margin,
								VrfKeyHash: make([]byte, 32), RewardAccount: rp.owner,
							},
						))
						// Without the SPO vote the action cannot ratify.
						s.castVote(t, p, models.VoterTypeSPO,
							[]byte(hfrLiveYesPool), models.VoteYes, target+5)
					}
					require.NoError(t, s.db.Metadata().AddNetworkDonation(
						target+10, next.EpochId-1, 7_000, nil,
					))
				}
				if v.fork {
					boundaries = 1
					c.forkPostTarget = func(*testing.T, models.Epoch) {}
					c.forkCheck = func(t *testing.T, a, b govDiffEpochDump) {
						require.NotNil(t, a.Proposals[0].RatifiedIn)
						require.Nil(t, b.Proposals[0].RatifiedIn,
							"the fork has no SPO vote and must not ratify")
						require.Greater(t, a.Treasury, b.Treasury,
							"the fork has no donation")
					}
				}
				results := runBoundaries(t, c, boundaries)
				requireChanged(t, results,
					"governance_vote", "governance_vote_history", "network_donation",
					"pool_registration")
				if !v.fork {
					require.True(t, s.enacted(t, 0x85))
				}
			})
			t.Run("changed vote/"+name, func(t *testing.T) {
				t.Parallel()
				s := newGovDiffScenarioWithOptions(t, govDiffScenarioOptions{noProposals: true})
				minFee := uint(88)
				change := &conway.ConwayParameterChangeGovAction{
					Type: uint(lcommon.GovActionTypeParameterChange),
					ParamUpdate: conway.ConwayProtocolParameterUpdate{
						MinFeeA: &minFee,
					},
				}
				p := s.propose(t, 0x89, lcommon.GovActionTypeParameterChange, change)
				slot := s.epoch.StartSlot - 5
				s.castVote(t, p, models.VoterTypeDRep, s.drep, models.VoteNo, slot)
				// A fee change is a security parameter, so pools vote too.
				s.castVote(t, p, models.VoterTypeSPO,
					[]byte(hfrLiveYesPool), models.VoteYes, slot)
				s.castVote(t, p, models.VoterTypeCC, s.hotCredential, models.VoteYes, slot)
				first := s.epoch.EpochId + 1
				boundaries := 3
				c := newRollbackCycle(t, s, entry.ledger, false, gap, boundaries)
				c.postTarget = func(t *testing.T, next models.Epoch, _ bool) {
					target := next.StartSlot - gap
					if next.EpochId != first {
						return
					}
					// The DRep changes its vote after the target.
					updated := target + 5
					require.NoError(t, s.db.SetGovernanceVote(
						context.Background(), &models.GovernanceVote{
							ProposalID:      p.ID,
							VoterType:       models.VoterTypeDRep,
							VoterCredential: s.drep,
							Vote:            models.VoteYes,
							AddedSlot:       slot,
							VoteUpdatedSlot: &updated,
						}, nil))
				}
				if v.fork {
					boundaries = 1
					c.forkPostTarget = func(*testing.T, models.Epoch) {}
					c.forkCheck = func(t *testing.T, a, b govDiffEpochDump) {
						require.NotNil(t, a.Proposals[0].RatifiedIn)
						require.Nil(t, b.Proposals[0].RatifiedIn,
							"without the changed vote the DRep votes no")
					}
				}
				results := runBoundaries(t, c, boundaries)
				requireChanged(t, results, "governance_vote", "governance_vote_history")
				if !v.fork {
					require.True(t, s.enacted(t, 0x89))
				}
			})
		}
	}
}

// rewardUpdateNodeConfig is the scenario's genesis with a stability window
// (4k/f = 8 slots) that fits the scenario's 100-slot epoch, and a supply small
// enough that the pool's stake earns a nonzero reward.
func rewardUpdateNodeConfig(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("5a", 32),
	}
	require.NoError(t, completeTestShelleyGenesis(cfg).LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.5,
		"epochLength": 432000,
		"maxLovelaceSupply": 100000000,
		"securityParam": 1,
		"slotLength": 1,
		"updateQuorum": 5,
		"systemStart": "2017-09-23T21:44:51Z"
	}`)))
	return cfg
}

// rewardPool is a pool whose members the reward round pays.
type rewardPool struct {
	key, owner, member []byte
}

// addRewardPool registers a pool with an owner and a member, both delegated
// and registered, whose stake the seeded reward rounds distribute.
func (s *govDiffScenario) addRewardPool(t *testing.T) rewardPool {
	t.Helper()
	rp := rewardPool{
		key:    repeatByte(28, 0xc1),
		owner:  s.addAccount(t, 0xc2, nil, ""),
		member: s.addAccount(t, 0xc3, nil, ""),
	}
	margin := &types.Rat{Rat: big.NewRat(1, 10)}
	require.NoError(t, s.db.ImportPool(context.Background(), nil, &models.Pool{
		PoolKeyHash:   rp.key,
		VrfKeyHash:    make([]byte, 32),
		Pledge:        500,
		Cost:          1_000,
		Margin:        margin,
		RewardAccount: rp.owner,
	}, &models.PoolRegistration{
		PoolKeyHash:   rp.key,
		AddedSlot:     s.epoch.StartSlot - 500,
		Pledge:        500,
		Cost:          1_000,
		Margin:        margin,
		VrfKeyHash:    make([]byte, 32),
		RewardAccount: rp.owner,
	}))
	raw, err := dbtest.RawSQLiteMetadata(t, s.db)
	require.NoError(t, err)
	for _, cred := range [][]byte{rp.owner, rp.member} {
		_, err := raw.Exec(
			`UPDATE account SET pool = ? WHERE staking_key = ?`, rp.key, cred,
		)
		require.NoError(t, err)
	}
	return rp
}

// seedRewardRound writes the reward round the boundary after epoch applies:
// the snapshot's pool and stake inputs, and the blocks the pool made in the
// performance epoch.
func (s *govDiffScenario) seedRewardRound(
	t *testing.T,
	rp rewardPool,
	epoch models.Epoch,
	blocks int,
) {
	t.Helper()
	epochs, ok := stakeRewardEpochsForApplication(epoch.EpochId + 1)
	require.True(t, ok)
	meta := s.db.Metadata()
	perf, err := meta.GetEpoch(epochs.performance, nil)
	require.NoError(t, err)
	require.NotNil(t, perf)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], rp.key)
	for i := range blocks {
		require.NoError(t, s.db.UpdatePoolOpCertSequence(
			context.Background(), poolID, epoch.EpochId*100+uint64(i)+1, perf.StartSlot+uint64(i), nil,
		))
	}
	captured, boundary := epoch.StartSlot, epoch.StartSlot
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            epochs.snapshot,
		SnapshotType:     "mark",
		TotalActiveStake: 60_000_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     captured,
		BoundarySlot:     boundary,
		ProtocolVersion:  10,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{{
		Epoch:                      epochs.snapshot,
		PoolKeyHash:                rp.key,
		RewardAccount:              rp.owner,
		RewardAccountCredentialTag: 0,
		Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
		Pledge:                     500,
		Cost:                       1_000,
		DelegatedStake:             60_000_000,
		OwnerStake:                 10_000_000,
		DelegatorCount:             2,
		CapturedSlot:               captured,
		BoundarySlot:               boundary,
	}}, nil))
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{
			Epoch: epochs.snapshot, PoolKeyHash: rp.key, StakingKey: rp.owner,
			Stake: 10_000_000, Owner: true, Registered: true,
			CapturedSlot: captured, BoundarySlot: boundary,
		},
		{
			Epoch: epochs.snapshot, PoolKeyHash: rp.key, StakingKey: rp.member,
			Stake: 50_000_000, Registered: true,
			CapturedSlot: captured, BoundarySlot: boundary,
		},
	}, nil))
}

// A boundary that pays rewards moves reserves and the treasury, credits
// accounts and journals each credit. Rolling it back must undo all of it, and
// replaying it must pay the same amounts.
func TestRollbackAcrossRewardPayoutRestoresEveryTable(t *testing.T) {
	t.Parallel()
	for _, entry := range []struct {
		name   string
		ledger bool
	}{{"database", false}, {"ledger", true}} {
		t.Run(entry.name, func(t *testing.T) {
			t.Parallel()
			s := newGovDiffScenarioWithOptions(t, govDiffScenarioOptions{noProposals: true})
			rp := s.addRewardPool(t)
			// One desired pool lets the pool's stake earn its full share.
			s.pparams.(*conway.ConwayProtocolParameters).NOpt = 1
			// The reward update applies only once the tip is past the
			// stability window of the ended epoch, which must fit in it.
			s.ls.config.CardanoNodeConfig = rewardUpdateNodeConfig(t)
			c := newRollbackCycle(t, s, entry.ledger, false, 1, 2)
			c.prepare = func(t *testing.T, epoch models.Epoch) {
				s.seedRewardRound(t, rp, epoch, 30)
			}
			startTreasury, startReserves, _ := networkState(t, s.db)
			results := runBoundaries(t, c, 2)
			treasury, reserves, _ := networkState(t, s.db)
			require.Less(t, reserves, startReserves,
				"the reward round must draw on the reserves")
			require.NotEqual(t, startTreasury, treasury)
			outputs, err := s.db.Metadata().GetRewardPoolOutputs(739, nil)
			require.NoError(t, err)
			require.Len(t, outputs, 1)
			require.NotZero(t, uint64(outputs[0].LeaderReward))
			require.NotZero(t, uint64(outputs[0].MemberRewardTotal),
				"the pool's members must be paid")
			requireChanged(t, results,
				"network_state", "reward_ada_pots", "reward_pool_output", "reward_account_output",
				"reward_credit_round", "account_reward_delta")
		})
	}
}

// A boundary whose deferred RATIFY has decided but not written leaves a
// pending record and a running job. Rolling the boundary back through the
// ledger must discard both, so that the job's own write finds nothing to do and
// no ratification of the removed boundary survives; the replayed boundary then
// ratifies as it did the first time.
func TestLedgerRollbackDiscardsUndeliveredRatification(t *testing.T) {
	t.Parallel()
	s := newGovDiffScenario(t)
	c := newRollbackCycle(t, s, true, true, 1, 1)
	epoch, pparams := s.epoch, s.pparams
	s.prepareRollover(t, epoch, pparams)
	next := epoch.StartSlot + uint64(epoch.LengthInSlots)
	target := next - 1
	c.setTip(t, target)
	before := dumpComparableTables(t, c.raw)

	held, release := holdRatificationApply(t, s)
	_, _ = s.commitRollover(t, epoch, pparams, func(*LedgerState) {})
	c.setTip(t, next)
	requireHeld(t, held, epoch.EpochId+1)
	rec, err := loadPendingRatification(s.db, nil)
	require.NoError(t, err)
	require.NotNil(t, rec, "the boundary must leave its decision pending")
	require.Nil(t, s.proposalByMarker(t, 0x71).RatifiedEpoch)

	require.NoError(t, s.ls.rollbackWithBlocksAndIntent(
		context.Background(), rollbackPoint(target), nil, false, false, false,
	))
	rec, err = loadPendingRatification(s.db, nil)
	require.NoError(t, err)
	require.Nil(t, rec, "the rollback must discard the pending decision")
	s.ls.ratificationMu.Lock()
	job := s.ls.ratificationJob
	s.ls.ratificationMu.Unlock()
	require.Nil(t, job, "the rollback must drop the removed boundary's job")

	release()
	s.ls.ratificationWG.Wait()
	s.ls.rewardPrecomputeWG.Wait()
	require.Empty(t, diffMetadataTables(before, dumpComparableTables(t, c.raw)))
	require.Nil(t, s.proposalByMarker(t, 0x71).RatifiedEpoch,
		"the removed boundary's decision must not be written")

	s.ls.currentEpoch = epoch
	s.ls.currentPParams = pparams
	s.commitRollover(t, epoch, pparams, c.settle())
	require.NotNil(t, s.proposalByMarker(t, 0x71).RatifiedEpoch,
		"the replayed boundary must ratify")
	rec, err = loadPendingRatification(s.db, nil)
	require.NoError(t, err)
	require.Nil(t, rec)
}
