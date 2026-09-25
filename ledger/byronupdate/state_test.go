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

package byronupdate

import (
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	v0 = ProtocolVersion{}
	v1 = ProtocolVersion{Minor: 1}
	v2 = ProtocolVersion{Major: 1}
)

func TestRegisterProposal(t *testing.T) {
	t.Parallel()
	delegates := newDelegates(t, 7)
	env := testEnv(delegates)
	issuer := delegates[0]
	start := NewState(testGenesisParams())
	u16 := func(v uint16) *uint16 { return &v }

	tests := []struct {
		name    string
		spec    proposalSpec
		issuer  delegate
		corrupt bool
		check   func(t *testing.T, err error)
	}{
		{
			"valid protocol and software update",
			proposalSpec{version: v1, appVer: 1},
			issuer,
			false,
			func(t *testing.T, err error) { require.NoError(t, err) },
		},
		{
			"issuer is not a delegate",
			proposalSpec{version: v1, appVer: 1},
			newDelegate(t, 0x99),
			false,
			func(t *testing.T, err error) {
				require.ErrorAs(t, err, &ProposalInvalidProposerError{})
			},
		},
		{"bad signature", proposalSpec{version: v1, appVer: 1}, issuer, true,
			func(t *testing.T, err error) {
				require.ErrorAs(t, err, &ProposalInvalidSignatureError{})
			}},
		{"changes nothing", proposalSpec{version: v0, appVer: 0}, issuer, false,
			func(t *testing.T, err error) { require.NoError(t, err) }},
		{
			"version cannot follow",
			proposalSpec{version: ProtocolVersion{Minor: 2}},
			issuer,
			false,
			func(t *testing.T, err error) {
				require.ErrorAs(t, err, &ProposalInvalidProtocolVersionError{})
			},
		},
		{
			"block size more than doubled",
			proposalSpec{version: v1, maxBlock: big.NewInt(4_000_001)},
			issuer,
			false,
			func(t *testing.T, err error) {
				require.ErrorAs(t, err, &ProposalMaxBlockSizeTooLargeError{})
			},
		},
		{
			"block size exactly doubled",
			proposalSpec{version: v1, maxBlock: big.NewInt(4_000_000)},
			issuer,
			false,
			func(t *testing.T, err error) { require.NoError(t, err) },
		},
		{
			"tx size not below block size",
			proposalSpec{version: v1, maxTx: big.NewInt(2_000_000)},
			issuer,
			false,
			func(t *testing.T, err error) {
				require.ErrorAs(t, err, &ProposalMaxTxSizeTooLargeError{})
			},
		},
		{
			"script version jumps by two",
			proposalSpec{version: v1, script: u16(2)},
			issuer,
			false,
			func(t *testing.T, err error) {
				require.ErrorAs(t, err, &ProposalInvalidScriptVersionError{})
			},
		},
		{
			"script version rises by one",
			proposalSpec{version: v1, script: u16(1)},
			issuer,
			false,
			func(t *testing.T, err error) { require.NoError(t, err) },
		},
		{
			"proposal too large",
			proposalSpec{version: v1, padding: 700},
			issuer,
			false,
			func(t *testing.T, err error) {
				require.ErrorAs(t, err, &ProposalTooLargeError{})
			},
		},
		{
			"software version skips",
			proposalSpec{version: v0, appVer: 2},
			issuer,
			false,
			func(t *testing.T, err error) {
				require.ErrorAs(t, err, &ProposalInvalidSoftwareVersionError{})
			},
		},
		{
			"application name too long",
			proposalSpec{version: v1, appName: "thirteen-char", appVer: 1},
			issuer,
			false,
			func(t *testing.T, err error) {
				require.ErrorAs(t, err, &ProposalInvalidApplicationNameError{})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			proposal := test.spec.build(t, test.issuer, test.corrupt)
			_, err := apply(t, start, env, 1, issuer, v0, proposal)
			test.check(t, err)
		})
	}

	// A proposal that changes neither the protocol nor a known
	// application's version is a null update.
	proposal := proposalSpec{version: v0, appVer: 0}.build(t, issuer, false)
	next, err := apply(t, start, env, 1, issuer, v0, proposal)
	require.NoError(t, err)
	next = confirm(t, next, env, delegates, upIdOf(proposal), 2)
	again := proposalSpec{version: v0, appVer: 0}.build(t, delegates[1], false)
	_, err = apply(t, next, env, 3, issuer, v0, again)
	require.ErrorAs(t, err, &ProposalNullUpdateError{})

	// A second proposal for a registered version is a duplicate.
	first := proposalSpec{version: v1}.build(t, issuer, false)
	next, err = apply(t, start, env, 1, issuer, v0, first)
	require.NoError(t, err)
	second := proposalSpec{
		version: v1,
		maxTx:   big.NewInt(5_000),
	}.build(
		t,
		delegates[1],
		false,
	)
	_, err = apply(t, next, env, 2, issuer, v0, second)
	require.ErrorAs(t, err, &ProposalDuplicateProtocolVersionError{})
}

// confirm has the first four delegates vote for upId at slot.
func confirm(
	t *testing.T,
	state State,
	env Environment,
	delegates []delegate,
	upId UpId,
	slot uint64,
) State {
	t.Helper()
	votes := make([]*byron.UpdateVote, 0, 4)
	for _, voter := range delegates[:4] {
		votes = append(votes, newVote(t, voter, upId, false))
	}
	next, err := apply(t, state, env, slot, delegates[0], v0, nil, votes...)
	require.NoError(t, err)
	_, confirmed := next.confirmed[upId]
	require.True(t, confirmed)
	return next
}

func TestRegisterVotes(t *testing.T) {
	t.Parallel()
	delegates := newDelegates(t, 7)
	env := testEnv(delegates)
	proposal := proposalSpec{version: v1}.build(t, delegates[0], false)
	upId := upIdOf(proposal)
	registered, err := apply(
		t, NewState(testGenesisParams()), env, 1, delegates[0], v0, proposal,
	)
	require.NoError(t, err)

	var unknown UpId
	unknown[0] = 0x01
	_, err = apply(t, registered, env, 2, delegates[0], v0, nil,
		newVote(t, delegates[0], unknown, false))
	require.ErrorAs(t, err, &VoteProposalNotRegisteredError{})

	_, err = apply(t, registered, env, 2, delegates[0], v0, nil,
		newVote(t, newDelegate(t, 0x99), upId, false))
	require.ErrorAs(t, err, &VoteVoterNotDelegateError{})

	_, err = apply(t, registered, env, 2, delegates[0], v0, nil,
		newVote(t, delegates[1], upId, true))
	require.ErrorAs(t, err, &VoteInvalidSignatureError{})

	_, err = apply(t, registered, env, 2, delegates[0], v0, nil,
		newVote(t, delegates[1], upId, false),
		newVote(t, delegates[1], upId, false))
	require.ErrorAs(t, err, &VoteAlreadyCastError{})

	// floor(0.6 * 7) = 4 votes confirm; three do not.
	three, err := apply(t, registered, env, 2, delegates[0], v0, nil,
		newVote(t, delegates[0], upId, false),
		newVote(t, delegates[1], upId, false),
		newVote(t, delegates[2], upId, false))
	require.NoError(t, err)
	assert.NotContains(t, three.confirmed, upId)
	four, err := apply(t, three, env, 3, delegates[0], v0, nil,
		newVote(t, delegates[3], upId, false))
	require.NoError(t, err)
	assert.Equal(t, uint64(3), four.confirmed[upId])
	assert.NotContains(t, registered.votes, upId,
		"Apply must not mutate its receiver")
}

func TestSoftwareUpdateConfirmation(t *testing.T) {
	t.Parallel()
	delegates := newDelegates(t, 7)
	env := testEnv(delegates)
	proposal := proposalSpec{
		version: v0,
		appVer:  1,
	}.build(
		t,
		delegates[0],
		false,
	)
	state, err := apply(
		t, NewState(testGenesisParams()), env, 1, delegates[0], v0, proposal,
	)
	require.NoError(t, err)
	state = confirm(t, state, env, delegates, upIdOf(proposal), 2)
	assert.Equal(t, uint32(1), state.appVersions["csl-daedalus"].version)
	assert.Empty(t, state.softwareProposals)
}

func TestProposalExpiry(t *testing.T) {
	t.Parallel()
	delegates := newDelegates(t, 7)
	env := testEnv(delegates)
	proposal := proposalSpec{version: v1}.build(t, delegates[0], false)
	upId := upIdOf(proposal)
	state, err := apply(
		t, NewState(testGenesisParams()), env, 1, delegates[0], v0, proposal,
	)
	require.NoError(t, err)
	// The TTL is 50 slots: registered at 1, kept through 51.
	state, err = apply(t, state, env, 51, delegates[0], v0, nil)
	require.NoError(t, err)
	assert.Contains(t, state.protocolProposals, upId)
	state, err = apply(t, state, env, 52, delegates[0], v0, nil)
	require.NoError(t, err)
	assert.NotContains(t, state.protocolProposals, upId)
	assert.NotContains(t, state.registrationSlot, upId)
}

// newCandidateState registers, confirms and endorses a version-1.0.0
// proposal with a 5,000-byte transaction limit, returning the state and the
// slot at which it became a candidate.
func newCandidateState(
	t *testing.T,
	delegates []delegate,
	env Environment,
) (State, uint64) {
	t.Helper()
	proposal := proposalSpec{
		version: v2,
		maxTx:   big.NewInt(5_000),
	}.build(t, delegates[0], false)
	state, err := apply(
		t, NewState(testGenesisParams()), env, 0, delegates[0], v0, proposal,
	)
	require.NoError(t, err)
	state = confirm(t, state, env, delegates, upIdOf(proposal), 1)
	// Endorsements before the confirmation is 2k = 20 slots deep are
	// ignored.
	state, err = apply(t, state, env, 20, delegates[0], v2, nil)
	require.NoError(t, err)
	assert.Empty(t, state.endorsements)
	slot := uint64(21)
	for _, endorser := range delegates[:4] {
		state, err = apply(t, state, env, slot, endorser, v2, nil)
		require.NoError(t, err)
		if endorser.keyHash != delegates[3].keyHash {
			assert.Empty(t, state.candidates)
			slot++
		}
	}
	require.Len(t, state.candidates, 1)
	return state, slot
}

func TestEndorsementAndAdoption(t *testing.T) {
	t.Parallel()
	delegates := newDelegates(t, 7)
	env := testEnv(delegates)
	state, candidateSlot := newCandidateState(t, delegates, env)
	assert.Equal(t, candidateSlot, state.candidates[0].Slot)

	// The candidate must be 4k = 40 slots old by the first slot of the
	// epoch that adopts it. From slot 24 that is slot 64, in epoch 0, so
	// epoch 1 (slot 100) adopts it.
	adopted := state.Tick(testConfig, 100)
	assert.Equal(t, v2, adopted.AdoptedVersion())
	assert.Equal(t, 0, big.NewInt(5_000).Cmp(adopted.AdoptedParams().MaxTxSize))
	assert.Empty(t, adopted.candidates)
	assert.Empty(t, adopted.protocolProposals)
	assert.Equal(t, v0, state.AdoptedVersion(),
		"Tick must not mutate its receiver")

	// A candidate first seen at slot 90 is not stable by slot 100.
	late := state.clone()
	late.candidates[0].Slot = 90
	assert.Equal(t, v0, late.Tick(testConfig, 100).AdoptedVersion())
	assert.Equal(t, v2, late.Tick(testConfig, 200).AdoptedVersion())

	// A block inside the same epoch does not adopt.
	assert.Equal(t, v0, state.Tick(testConfig, 99).AdoptedVersion())
}

func TestShelleyTransitionEpoch(t *testing.T) {
	t.Parallel()
	delegates := newDelegates(t, 7)
	env := testEnv(delegates)
	state, candidateSlot := newCandidateState(t, delegates, env)
	const major = 1
	// Adopted in the epoch after (24 + 40) / 100 = 0.
	_, known := state.ShelleyTransitionEpoch(testConfig, major)
	assert.False(t, known, "a fresh candidate is not yet stable")

	// Stable by slot: the tip is 2k = 20 slots past the candidate.
	bySlot, err := apply(t, state, env, candidateSlot+20, delegates[0], v2, nil)
	require.NoError(t, err)
	epoch, known := bySlot.ShelleyTransitionEpoch(testConfig, major)
	require.True(t, known)
	assert.Equal(t, uint64(1), epoch)
	_, known = bySlot.ShelleyTransitionEpoch(testConfig, major+1)
	assert.False(t, known, "only the configured major version transitions")

	// Stable by depth: k = 10 blocks past the candidate's block, even
	// within 2k slots.
	byDepth := state.clone()
	byDepth.lastSlot = candidateSlot + 5
	byDepth.tipBlockNo = byDepth.candidateBlockNo[v2] + 10
	epoch, known = byDepth.ShelleyTransitionEpoch(testConfig, major)
	require.True(t, known)
	assert.Equal(t, uint64(1), epoch)
	byDepth.tipBlockNo--
	_, known = byDepth.ShelleyTransitionEpoch(testConfig, major)
	assert.False(t, known)
}

func TestComplete(t *testing.T) {
	t.Parallel()
	fromGenesis := NewState(testGenesisParams()).Advance(0, 0)
	assert.True(t, fromGenesis.Complete())
	assert.True(t, fromGenesis.Advance(5, 1).Complete())
	fromTrustedStart := NewState(
		testGenesisParams(),
	).Advance(4_492_799, 4_490_510)
	assert.False(t, fromTrustedStart.Complete())
}

func TestCheckTransition(t *testing.T) {
	t.Parallel()
	delegates := newDelegates(t, 7)
	env := testEnv(delegates)
	candidate, candidateSlot := newCandidateState(t, delegates, env)
	stable, err := apply(
		t,
		candidate,
		env,
		candidateSlot+20,
		delegates[0],
		v2,
		nil,
	)
	require.NoError(t, err)
	const major = 1

	// Leaving Byron in the adoption epoch is allowed; in any other epoch,
	// or with no stable candidate, it is not.
	require.NoError(t, stable.CheckTransition(testConfig, major, 1, true))
	require.ErrorAs(t, stable.CheckTransition(testConfig, major, 2, true),
		&TransitionNotAdoptedError{})
	require.ErrorAs(t, candidate.CheckTransition(testConfig, major, 1, true),
		&TransitionNotAdoptedError{})
	none := NewState(testGenesisParams()).Advance(0, 0)
	require.ErrorAs(t, none.CheckTransition(testConfig, major, 1, true),
		&TransitionNotAdoptedError{})

	// Staying in Byron is fine until the adoption epoch.
	require.NoError(t, stable.CheckTransition(testConfig, major, 0, false))
	require.ErrorAs(t, stable.CheckTransition(testConfig, major, 1, false),
		&TransitionMissedError{})
	require.NoError(t, candidate.CheckTransition(testConfig, major, 1, false),
		"an unstable candidate does not force the transition")
	require.NoError(t, none.CheckTransition(testConfig, major, 5, false))
}
