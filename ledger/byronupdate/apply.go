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
	"errors"
	"fmt"
	"math/big"

	"github.com/blinklabs-io/dingo/ledger/eras"
	byronconsensus "github.com/blinklabs-io/gouroboros/consensus/byron"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// applicationNameMaxLength bounds an application name
// (checkApplicationName).
const applicationNameMaxLength = 12

// stagingProtocolMagic and its two slots are the reference's
// nullUpdateExemptions: staging carries two historical proposals that change
// nothing.
const stagingProtocolMagic = 633343913

var stagingNullUpdateSlots = map[uint64]struct{}{
	969188:  {},
	1915231: {},
}

// Block is the update-relevant content of one Byron main block.
type Block struct {
	// Slot is the block's flat slot number, epoch * 10k + slot in epoch.
	Slot    uint64
	BlockNo uint64
	// Proposal is the block's update proposal, if any.
	Proposal *byron.ByronUpdateProposal
	Votes    []*byron.UpdateVote
	// Version is the protocol version the block header declares, which the
	// block issuer endorses.
	Version ProtocolVersion
	// IssuerKeyHash is the hash of the delegate key that signed the block.
	IssuerKeyHash KeyHash
}

// Environment is the per-block environment of the update rules.
type Environment struct {
	Config
	// DelegateToGenesis maps each active delegate key hash to the genesis
	// key hash it signs for (Delegation.Map, read right to left).
	DelegateToGenesis map[KeyHash]KeyHash
}

// Apply runs registerUpdate for a main block already ticked to its slot:
// the proposal, then the votes in order, then the endorsement. Any failure
// invalidates the block.
func (s State) Apply(env Environment, block Block) (State, error) {
	ret := s.clone()
	if block.Proposal != nil {
		if err := registerProposal(&ret, env, block.Slot, block.Proposal); err != nil {
			return State{}, err
		}
	}
	if err := registerVotes(&ret, env, block.Slot, block.Votes); err != nil {
		return State{}, err
	}
	if err := registerEndorsement(&ret, env, block.Slot, endorsement{
		version: block.Version,
		keyHash: block.IssuerKeyHash,
	}); err != nil {
		return State{}, err
	}
	recordTip(&ret, block.Slot, block.BlockNo)
	// Keep the block number at which each current candidate first appeared
	// and forget versions that are no longer candidates.
	current := make(map[ProtocolVersion]uint64, len(ret.candidates))
	for _, candidate := range ret.candidates {
		if blockNo, ok := ret.candidateBlockNo[candidate.Version]; ok {
			current[candidate.Version] = blockNo
		} else {
			current[candidate.Version] = block.BlockNo
		}
	}
	ret.candidateBlockNo = current
	return ret, nil
}

// registerProposal is UPIREG: the proposer must be an active delegate, the
// signature must verify, and the proposal must update the protocol, the
// software version, or both.
func registerProposal(
	s *State,
	env Environment,
	slot uint64,
	proposal *byron.ByronUpdateProposal,
) error {
	proposer, err := byronconsensus.PBFTVerificationKeyHash(proposal.From)
	if err != nil {
		return fmt.Errorf("update proposal issuer: %w", err)
	}
	if _, ok := env.DelegateToGenesis[proposer]; !ok {
		return ProposalInvalidProposerError{Proposer: proposer}
	}
	if err := proposal.Validate(env.ProtocolMagic); err != nil {
		if errors.Is(err, byron.ErrInvalidSignature) {
			return ProposalInvalidSignatureError{Err: err}
		}
		return fmt.Errorf("update proposal: %w", err)
	}
	upId := lcommon.Blake2b256Hash(proposal.Cbor())
	version := ProtocolVersion{
		Major: proposal.BlockVersion.Major,
		Minor: proposal.BlockVersion.Minor,
		Alt:   proposal.BlockVersion.Unknown,
	}
	newParams, err := s.adoptedParams.ApplyUpdate(proposal.BlockVersionMod)
	if err != nil {
		return fmt.Errorf("update proposal parameters: %w", err)
	}
	appName := proposal.SoftwareVersion.Name
	appVersion := proposal.SoftwareVersion.Version
	current, known := s.appVersions[appName]
	softwareChanged := !known || current.version != appVersion
	protocolChanged := version != s.adoptedVersion ||
		!newParams.Equal(s.adoptedParams)
	_, exempt := stagingNullUpdateSlots[slot]
	exempt = exempt && env.ProtocolMagic == stagingProtocolMagic
	if !protocolChanged && !softwareChanged && !exempt {
		return ProposalNullUpdateError{}
	}
	if protocolChanged {
		if err := registerProtocolUpdate(s,
			upId, version, newParams, uint64(len(proposal.Cbor())),
		); err != nil {
			return err
		}
	}
	if softwareChanged {
		if err := registerSoftwareUpdate(s,
			upId, appName, appVersion,
		); err != nil {
			return err
		}
	}
	s.registrationSlot[upId] = slot
	return nil
}

func registerProtocolUpdate(
	s *State,
	upId UpId,
	version ProtocolVersion,
	newParams *eras.ByronProtocolParameters,
	proposalSize uint64,
) error {
	for _, registered := range s.protocolProposals {
		if registered.version == version {
			return ProposalDuplicateProtocolVersionError{Version: version}
		}
	}
	if !version.canFollow(s.adoptedVersion) {
		return ProposalInvalidProtocolVersionError{
			Version: version,
			Adopted: s.adoptedVersion,
		}
	}
	adopted := s.adoptedParams
	size := new(big.Int).SetUint64(proposalSize)
	if adopted.MaxProposalSize != nil &&
		size.Cmp(adopted.MaxProposalSize) > 0 {
		return ProposalTooLargeError{Size: size, Max: adopted.MaxProposalSize}
	}
	if adopted.MaxBlockSize != nil && newParams.MaxBlockSize != nil {
		limit := new(big.Int).Lsh(adopted.MaxBlockSize, 1)
		if newParams.MaxBlockSize.Cmp(limit) > 0 {
			return ProposalMaxBlockSizeTooLargeError{
				Proposed: newParams.MaxBlockSize,
				Adopted:  adopted.MaxBlockSize,
			}
		}
	}
	if newParams.MaxTxSize != nil && newParams.MaxBlockSize != nil &&
		newParams.MaxTxSize.Cmp(newParams.MaxBlockSize) >= 0 {
		return ProposalMaxTxSizeTooLargeError{
			MaxTxSize:    newParams.MaxTxSize,
			MaxBlockSize: newParams.MaxBlockSize,
		}
	}
	// The difference is a Word16, so a lower script version wraps around
	// and fails the bound.
	if newParams.ScriptVersion-adopted.ScriptVersion > 1 {
		return ProposalInvalidScriptVersionError{
			Adopted:  adopted.ScriptVersion,
			Proposed: newParams.ScriptVersion,
		}
	}
	s.protocolProposals[upId] = protocolUpdateProposal{
		version: version,
		params:  newParams,
	}
	return nil
}

func registerSoftwareUpdate(
	s *State,
	upId UpId,
	appName string,
	appVersion uint32,
) error {
	for _, registered := range s.softwareProposals {
		if registered.appName == appName {
			return ProposalDuplicateSoftwareVersionError{
				AppName: appName,
				Version: appVersion,
			}
		}
	}
	if len([]rune(appName)) > applicationNameMaxLength {
		return ProposalInvalidApplicationNameError{AppName: appName}
	}
	for _, r := range appName {
		if r > 0x7f {
			return ProposalInvalidApplicationNameError{AppName: appName}
		}
	}
	current, known := s.appVersions[appName]
	if (!known && appVersion > 1) ||
		(known && appVersion != current.version+1) {
		return ProposalInvalidSoftwareVersionError{
			AppName: appName,
			Version: appVersion,
		}
	}
	s.softwareProposals[upId] = softwareUpdateProposal{
		appName: appName,
		version: appVersion,
	}
	return nil
}

// registerVotes is UPIVOTES: each vote in order, then every confirmed
// software update becomes the application's current version.
func registerVotes(
	s *State,
	env Environment,
	slot uint64,
	votes []*byron.UpdateVote,
) error {
	threshold := s.adoptedParams.UpdateAdoptionThreshold(env.NumGenesisKeys)
	for _, vote := range votes {
		if err := registerVote(s, env, slot, threshold, vote); err != nil {
			return err
		}
	}
	for upId, proposal := range s.softwareProposals {
		if _, ok := s.confirmed[upId]; !ok {
			continue
		}
		s.appVersions[proposal.appName] = applicationVersion{
			version: proposal.version,
			slot:    slot,
		}
		delete(s.softwareProposals, upId)
	}
	return nil
}

// registerVote is UPIVOTE: the proposal must be registered, the voter must be
// an active delegate whose genesis key has not already voted for it, and the
// signature must verify. A proposal is confirmed once the adoption threshold
// of genesis keys has voted for it.
func registerVote(
	s *State,
	env Environment,
	slot uint64,
	threshold int,
	vote *byron.UpdateVote,
) error {
	if vote == nil {
		return errors.New("update vote is nil")
	}
	var upId UpId
	if len(vote.ProposalId) != len(upId) {
		return fmt.Errorf(
			"update vote proposal id is %d bytes, expected %d",
			len(vote.ProposalId),
			len(upId),
		)
	}
	copy(upId[:], vote.ProposalId)
	if _, ok := s.registrationSlot[upId]; !ok {
		return VoteProposalNotRegisteredError{UpId: upId}
	}
	voter, err := byronconsensus.PBFTVerificationKeyHash(vote.VoterVK)
	if err != nil {
		return fmt.Errorf("update vote voter: %w", err)
	}
	delegator, ok := env.DelegateToGenesis[voter]
	if !ok {
		return VoteVoterNotDelegateError{Voter: voter}
	}
	if _, voted := s.votes[upId][delegator]; voted {
		return VoteAlreadyCastError{Voter: delegator}
	}
	if err := vote.Verify(env.ProtocolMagic); err != nil {
		return VoteInvalidSignatureError{Err: err}
	}
	if s.votes[upId] == nil {
		s.votes[upId] = make(map[KeyHash]struct{})
	}
	s.votes[upId][delegator] = struct{}{}
	if _, confirmed := s.confirmed[upId]; !confirmed &&
		len(s.votes[upId]) >= threshold {
		s.confirmed[upId] = slot
	}
	return nil
}

// registerEndorsement is UPIEND: the block issuer endorses its header's
// protocol version. A confirmed proposal for that version that has been
// stable for 2k slots becomes a candidate once enough genesis keys have
// endorsed it. Proposals whose time to live has passed without confirmation
// are then dropped with their votes and endorsements.
func registerEndorsement(
	s *State,
	env Environment,
	slot uint64,
	endorsed endorsement,
) error {
	var matchId UpId
	var match *protocolUpdateProposal
	for upId, proposal := range s.protocolProposals {
		if proposal.version != endorsed.version {
			continue
		}
		if match != nil {
			return EndorsementMultipleProposalsError{Version: endorsed.version}
		}
		matchId = upId
		match = &proposal
	}
	if match != nil {
		confirmedAt, confirmed := s.confirmed[matchId]
		// kSlotSecurityParam: 2k slots.
		if confirmed && confirmedAt+2*env.K <= slot {
			if genesis, ok := env.DelegateToGenesis[endorsed.keyHash]; ok {
				s.endorsements[endorsement{
					version: endorsed.version,
					keyHash: genesis,
				}] = struct{}{}
			}
			count := 0
			for registered := range s.endorsements {
				if registered.version == endorsed.version {
					count++
				}
			}
			threshold := s.adoptedParams.UpdateAdoptionThreshold(
				env.NumGenesisKeys,
			)
			if count >= threshold {
				// updateCandidateProtocolUpdates keeps the list newest
				// first and ignores a version no newer than its head.
				if len(s.candidates) == 0 ||
					s.candidates[0].Version.Less(endorsed.version) {
					s.candidates = append([]Candidate{{
						Slot:    slot,
						Version: endorsed.version,
						Params:  match.params,
					}}, s.candidates...)
				}
			}
		}
	}
	ttl := s.adoptedParams.UpdateProposalTTL
	keep := make(map[UpId]struct{}, len(s.registrationSlot))
	for upId, registeredAt := range s.registrationSlot {
		if slot <= registeredAt+ttl {
			keep[upId] = struct{}{}
		}
	}
	for upId := range s.confirmed {
		keep[upId] = struct{}{}
	}
	for upId := range s.protocolProposals {
		if _, ok := keep[upId]; !ok {
			delete(s.protocolProposals, upId)
		}
	}
	for upId := range s.softwareProposals {
		if _, ok := keep[upId]; !ok {
			delete(s.softwareProposals, upId)
		}
	}
	for upId := range s.votes {
		if _, ok := keep[upId]; !ok {
			delete(s.votes, upId)
		}
	}
	for upId := range s.registrationSlot {
		if _, ok := keep[upId]; !ok {
			delete(s.registrationSlot, upId)
		}
	}
	versions := make(map[ProtocolVersion]struct{}, len(s.protocolProposals))
	for _, proposal := range s.protocolProposals {
		versions[proposal.version] = struct{}{}
	}
	for registered := range s.endorsements {
		if _, ok := versions[registered.version]; !ok {
			delete(s.endorsements, registered)
		}
	}
	return nil
}
