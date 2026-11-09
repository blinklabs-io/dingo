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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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
	s := newGovDiffScenario(t)
	c := newRollbackCycle(t, s, true, false, 1, 4)
	epoch, pparams := s.epoch, s.pparams
	for range 4 {
		r := c.run(t, epoch, pparams)
		epoch, pparams = r.epoch, r.pparams
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
