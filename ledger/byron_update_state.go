// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ledger

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
	"unicode"

	"github.com/blinklabs-io/gouroboros/cbor"
	byronconsensus "github.com/blinklabs-io/gouroboros/consensus/byron"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/common"
)

type byronUpdateProposalID [common.Blake2b256Size]byte

type byronRegisteredUpdate struct {
	version      byron.ByronBlockVersion
	params       byron.ByronGenesisBlockVersionData
	hasProtocol  bool
	softwareName string
	softwareVer  uint32
	registeredAt uint64
}

type byronProtocolAdoption struct {
	slot    uint64
	version byron.ByronBlockVersion
	params  byron.ByronGenesisBlockVersionData
}

type byronUpdateState struct {
	protocolVersion  byron.ByronBlockVersion
	params           byron.ByronGenesisBlockVersionData
	numGenesisKeys   int
	lastEpoch        uint64
	epochInitialized bool
	proposals        map[byronUpdateProposalID]byronRegisteredUpdate
	confirmed        map[byronUpdateProposalID]uint64
	votes            map[byronUpdateProposalID]map[common.Blake2b224]struct{}
	endorsements     map[byron.ByronBlockVersion]map[common.Blake2b224]struct{}
	applications     map[string]uint32
	candidates       []byronProtocolAdoption
}

func (s byronUpdateState) clone() byronUpdateState {
	ret := s
	ret.proposals = make(map[byronUpdateProposalID]byronRegisteredUpdate, len(s.proposals))
	for id, proposal := range s.proposals {
		ret.proposals[id] = proposal
	}
	ret.confirmed = make(map[byronUpdateProposalID]uint64, len(s.confirmed))
	for id, slot := range s.confirmed {
		ret.confirmed[id] = slot
	}
	ret.votes = cloneByronUpdateKeySets(s.votes)
	ret.endorsements = cloneByronUpdateKeySets(s.endorsements)
	ret.applications = make(map[string]uint32, len(s.applications))
	for name, version := range s.applications {
		ret.applications[name] = version
	}
	ret.candidates = append([]byronProtocolAdoption(nil), s.candidates...)
	return ret
}

func cloneByronUpdateKeySets[K comparable](
	input map[K]map[common.Blake2b224]struct{},
) map[K]map[common.Blake2b224]struct{} {
	ret := make(map[K]map[common.Blake2b224]struct{}, len(input))
	for key, values := range input {
		ret[key] = make(map[common.Blake2b224]struct{}, len(values))
		for value := range values {
			ret[key][value] = struct{}{}
		}
	}
	return ret
}

func newByronUpdateState(
	params byron.ByronGenesisBlockVersionData,
	numGenesisKeys int,
) byronUpdateState {
	return byronUpdateState{
		params:         params,
		numGenesisKeys: numGenesisKeys,
		proposals:      make(map[byronUpdateProposalID]byronRegisteredUpdate),
		confirmed:      make(map[byronUpdateProposalID]uint64),
		votes:          make(map[byronUpdateProposalID]map[common.Blake2b224]struct{}),
		endorsements:   make(map[byron.ByronBlockVersion]map[common.Blake2b224]struct{}),
		applications:   make(map[string]uint32),
	}
}

func (s byronUpdateState) registerProposal(
	proposal byron.ByronUpdateProposal,
	delegations map[common.Blake2b224]common.Blake2b224,
	slot uint64,
) (byronUpdateState, error) {
	state := s.clone()
	issuerHash, err := byronconsensus.PBFTVerificationKeyHash(proposal.From)
	if err != nil {
		return s, fmt.Errorf("hash Byron update proposer key: %w", err)
	}
	if _, ok := genesisForByronDelegate(delegations, issuerHash); !ok {
		return s, fmt.Errorf("byron update proposer %x is not an active genesis delegate", issuerHash)
	}

	proposalID := byronUpdateProposalID(common.Blake2b256Hash(proposal.Cbor()))
	if _, exists := state.proposals[proposalID]; exists {
		return s, fmt.Errorf("byron update proposal %x is already registered", proposalID)
	}
	if state.params.MaxProposalSize <= 0 || len(proposal.Cbor()) > state.params.MaxProposalSize {
		return s, fmt.Errorf(
			"byron update proposal size %d exceeds maxProposalSize %d",
			len(proposal.Cbor()), state.params.MaxProposalSize,
		)
	}

	updatedParams, err := applyByronBlockVersionMod(state.params, proposal.BlockVersionMod)
	if err != nil {
		return s, fmt.Errorf("apply Byron update proposal parameters: %w", err)
	}
	versionChanged := proposal.BlockVersion != state.protocolVersion
	if versionChanged {
		if err := validateByronProtocolVersionSuccessor(
			state.protocolVersion, proposal.BlockVersion,
		); err != nil {
			return s, err
		}
		for _, existing := range state.proposals {
			if existing.hasProtocol && existing.version == proposal.BlockVersion {
				return s, fmt.Errorf(
					"byron protocol version %d.%d.%d is already proposed",
					proposal.BlockVersion.Major,
					proposal.BlockVersion.Minor,
					proposal.BlockVersion.Unknown,
				)
			}
		}
		if err := validateByronUpdateParameters(state.params, updatedParams); err != nil {
			return s, err
		}
	} else if updatedParams != state.params {
		return s, errors.New(
			"byron software update changes protocol parameters without a protocol-version bump",
		)
	}

	softwareVersionChanged, err := state.validateSoftwareVersion(proposal)
	if err != nil {
		return s, err
	}
	if !versionChanged && !softwareVersionChanged {
		return s, errors.New("byron update proposal changes neither protocol nor software version")
	}
	state.proposals[proposalID] = byronRegisteredUpdate{
		version:      proposal.BlockVersion,
		params:       updatedParams,
		hasProtocol:  versionChanged,
		softwareName: proposal.SoftwareVersion.Name,
		softwareVer:  proposal.SoftwareVersion.Version,
		registeredAt: slot,
	}
	return state, nil
}

func (s byronUpdateState) validateSoftwareVersion(
	proposal byron.ByronUpdateProposal,
) (bool, error) {
	name := proposal.SoftwareVersion.Name
	if len(name) > 12 || strings.IndexFunc(name, func(r rune) bool {
		return r > unicode.MaxASCII
	}) >= 0 {
		return false, errors.New("byron software application name is invalid")
	}
	currentVersion, exists := s.applications[name]
	if exists && proposal.SoftwareVersion.Version == currentVersion {
		return false, nil
	}
	if exists {
		if currentVersion == ^uint32(0) || proposal.SoftwareVersion.Version != currentVersion+1 {
			return false, fmt.Errorf("byron software version for %q must increment by one", name)
		}
	} else if proposal.SoftwareVersion.Version > 1 {
		return false, fmt.Errorf("initial byron software version for %q must be 0 or 1", name)
	}
	for _, registered := range s.proposals {
		if registered.softwareName == name {
			return false, fmt.Errorf("byron software update for %q is already proposed", name)
		}
	}
	return true, nil
}

func validateByronProtocolVersionSuccessor(
	current byron.ByronBlockVersion,
	next byron.ByronBlockVersion,
) error {
	majorDelta := int32(next.Major) - int32(current.Major)
	minorDelta := uint32(next.Minor) - uint32(current.Minor)
	if majorDelta < 0 || majorDelta > 1 ||
		(majorDelta == 0 && minorDelta != 1) ||
		(majorDelta == 1 && next.Minor != 0) ||
		(majorDelta == 0 && current.Unknown >= next.Unknown && current.Minor == next.Minor) {
		return fmt.Errorf(
			"byron protocol version %d.%d.%d cannot follow %d.%d.%d",
			next.Major, next.Minor, next.Unknown,
			current.Major, current.Minor, current.Unknown,
		)
	}
	return nil
}

func validateByronUpdateParameters(
	current byron.ByronGenesisBlockVersionData,
	updated byron.ByronGenesisBlockVersionData,
) error {
	if current.MaxBlockSize <= 0 || updated.MaxBlockSize < 0 ||
		(updated.MaxBlockSize > current.MaxBlockSize &&
			updated.MaxBlockSize-current.MaxBlockSize > current.MaxBlockSize) {
		return fmt.Errorf("byron update maxBlockSize %d exceeds twice the current maximum %d", updated.MaxBlockSize, current.MaxBlockSize)
	}
	if updated.MaxTxSize >= updated.MaxBlockSize {
		return fmt.Errorf("byron update maxTxSize %d must be less than maxBlockSize %d", updated.MaxTxSize, updated.MaxBlockSize)
	}
	if updated.ScriptVersion < current.ScriptVersion ||
		(updated.ScriptVersion > current.ScriptVersion &&
			updated.ScriptVersion-current.ScriptVersion > 1) {
		return fmt.Errorf("byron update scriptVersion %d must stay the same or increase by one from %d", updated.ScriptVersion, current.ScriptVersion)
	}
	return nil
}

func genesisForByronDelegate(
	delegations map[common.Blake2b224]common.Blake2b224,
	delegate common.Blake2b224,
) (common.Blake2b224, bool) {
	for genesis, active := range delegations {
		if active == delegate {
			return genesis, true
		}
	}
	return common.Blake2b224{}, false
}

func (s byronUpdateState) applyUpdatePayload(
	block *byron.ByronMainBlock,
	delegations map[common.Blake2b224]common.Blake2b224,
	protocolMagic uint32,
	slot uint64,
) (byronUpdateState, error) {
	state := s
	if len(block.Body.UpdPayload.Proposals) > 0 {
		proposal := block.Body.UpdPayload.Proposals[0]
		if err := proposal.Validate(protocolMagic); err != nil {
			return s, fmt.Errorf("validate Byron update proposal: %w", err)
		}
		var err error
		state, err = state.registerProposal(proposal, delegations, slot)
		if err != nil {
			return s, err
		}
	}
	voteEntries, err := byronUpdateVoteEntries(block)
	if err != nil {
		return s, err
	}
	for index, rawVote := range voteEntries {
		vote, err := byron.ParseUpdateVote(rawVote)
		if err != nil {
			return s, fmt.Errorf("parse Byron update vote %d: %w", index, err)
		}
		if err := vote.Verify(protocolMagic); err != nil {
			return s, fmt.Errorf("verify Byron update vote %d: %w", index, err)
		}
		state, err = state.registerVote(vote, delegations, slot)
		if err != nil {
			return s, fmt.Errorf("apply Byron update vote %d: %w", index, err)
		}
	}
	return state, nil
}

func byronUpdateVoteEntries(
	block *byron.ByronMainBlock,
) ([]cbor.RawMessage, error) {
	if len(block.Body.UpdPayload.Votes) == 0 {
		return nil, nil
	}
	var bodyFields []cbor.RawMessage
	if _, err := cbor.Decode(block.Body.Cbor(), &bodyFields); err != nil {
		return nil, fmt.Errorf("decode Byron main-block body for update votes: %w", err)
	}
	if len(bodyFields) != 4 {
		return nil, fmt.Errorf("byron main-block body has %d fields, expected 4", len(bodyFields))
	}
	var updateFields []cbor.RawMessage
	if _, err := cbor.Decode(bodyFields[3], &updateFields); err != nil {
		return nil, fmt.Errorf("decode Byron update payload for votes: %w", err)
	}
	if len(updateFields) != 2 {
		return nil, fmt.Errorf("byron update payload has %d fields, expected 2", len(updateFields))
	}
	var votes []cbor.RawMessage
	if _, err := cbor.Decode(updateFields[1], &votes); err != nil {
		return nil, fmt.Errorf("decode Byron update votes: %w", err)
	}
	return votes, nil
}

func (s byronUpdateState) registerVote(
	vote *byron.UpdateVote,
	delegations map[common.Blake2b224]common.Blake2b224,
	slot uint64,
) (byronUpdateState, error) {
	state := s.clone()
	proposalID := byronUpdateProposalID(vote.ProposalId)
	if _, exists := state.proposals[proposalID]; !exists {
		return s, fmt.Errorf("byron update vote references unregistered proposal %x", vote.ProposalId)
	}
	voterHash, err := byronconsensus.PBFTVerificationKeyHash(vote.VoterVK)
	if err != nil {
		return s, fmt.Errorf("hash Byron update voter key: %w", err)
	}
	genesis, ok := genesisForByronDelegate(delegations, voterHash)
	if !ok {
		return s, fmt.Errorf("byron update voter %x is not an active genesis delegate", voterHash)
	}
	voters := state.votes[proposalID]
	if voters == nil {
		voters = make(map[common.Blake2b224]struct{})
		state.votes[proposalID] = voters
	}
	if _, exists := voters[genesis]; exists {
		return s, fmt.Errorf("byron genesis delegate %x voted more than once on proposal %x", genesis, vote.ProposalId)
	}
	voters[genesis] = struct{}{}
	threshold := byronPortionThreshold(state.params.UpdateVoteThd, state.numGenesisKeys)
	if len(voters) >= threshold {
		if _, confirmed := state.confirmed[proposalID]; !confirmed {
			state.confirmed[proposalID] = slot
			proposal := state.proposals[proposalID]
			if proposal.softwareName != "" {
				state.applications[proposal.softwareName] = proposal.softwareVer
			}
		}
	}
	return state, nil
}

func byronPortionThreshold(portion int64, genesisKeys int) int {
	if portion <= 0 || genesisKeys <= 0 {
		return 0
	}
	value := new(big.Int).Mul(big.NewInt(portion), big.NewInt(int64(genesisKeys)))
	value.Quo(value, big.NewInt(1_000_000_000_000_000))
	if value.Sign() < 0 || value.Cmp(big.NewInt(int64(genesisKeys))) >= 0 {
		return genesisKeys
	}
	return int(value.Int64())
}

func (s byronUpdateState) registerEndorsement(
	version byron.ByronBlockVersion,
	delegateHash common.Blake2b224,
	delegations map[common.Blake2b224]common.Blake2b224,
	slot uint64,
	securityParam uint64,
) (byronUpdateState, error) {
	state := s.clone()
	var proposalID byronUpdateProposalID
	var proposal byronRegisteredUpdate
	found := false
	for id, candidate := range state.proposals {
		if candidate.hasProtocol && candidate.version == version {
			if found {
				return s, fmt.Errorf("multiple Byron update proposals target protocol version %d.%d.%d", version.Major, version.Minor, version.Unknown)
			}
			proposalID, proposal, found = id, candidate, true
		}
	}
	if !found {
		return state, nil
	}
	genesis, ok := genesisForByronDelegate(delegations, delegateHash)
	if !ok {
		return s, fmt.Errorf("byron update endorser %x is not an active genesis delegate", delegateHash)
	}
	endorsers := state.endorsements[version]
	if endorsers == nil {
		endorsers = make(map[common.Blake2b224]struct{})
		state.endorsements[version] = endorsers
	}
	endorsers[genesis] = struct{}{}
	threshold := byronPortionThreshold(state.params.UpdateProposalThd, state.numGenesisKeys)
	if len(endorsers) < threshold {
		return state, nil
	}
	confirmedAt, confirmed := state.confirmed[proposalID]
	if securityParam > ^uint64(0)/2 {
		return s, fmt.Errorf("byron update stability window overflows for security parameter %d", securityParam)
	}
	if !confirmed || confirmedAt > slot || slot-confirmedAt < securityParam*2 {
		return s, fmt.Errorf("byron update proposal %x is not confirmed and stable", proposalID)
	}
	candidate := byronProtocolAdoption{
		slot: slot, version: proposal.version, params: proposal.params,
	}
	if len(state.candidates) == 0 || byronProtocolVersionLess(state.candidates[0].version, version) {
		state.candidates = append([]byronProtocolAdoption{candidate}, state.candidates...)
	}
	return state, nil
}

func (s byronUpdateState) tickEpoch(
	epochFirstSlot uint64,
	securityParam uint64,
) byronUpdateState {
	state := s.clone()
	if securityParam > ^uint64(0)/4 {
		return state
	}
	stabilityDelay := securityParam * 4
	if epochFirstSlot < stabilityDelay {
		return state
	}
	cutoff := epochFirstSlot - stabilityDelay
	for _, candidate := range state.candidates {
		if candidate.slot > cutoff {
			continue
		}
		state.protocolVersion = candidate.version
		state.params = candidate.params
		state.candidates = nil
		state.proposals = make(map[byronUpdateProposalID]byronRegisteredUpdate)
		state.confirmed = make(map[byronUpdateProposalID]uint64)
		state.votes = make(map[byronUpdateProposalID]map[common.Blake2b224]struct{})
		state.endorsements = make(map[byron.ByronBlockVersion]map[common.Blake2b224]struct{})
		return state
	}
	return state
}

func (s byronUpdateState) advanceEpoch(
	epoch uint64,
	epochFirstSlot uint64,
	securityParam uint64,
) (byronUpdateState, error) {
	if s.epochInitialized && epoch < s.lastEpoch {
		return s, fmt.Errorf(
			"byron update epoch regressed from %d to %d",
			s.lastEpoch, epoch,
		)
	}
	if s.epochInitialized && epoch == s.lastEpoch {
		return s, nil
	}
	state := s.tickEpoch(epochFirstSlot, securityParam)
	state.lastEpoch = epoch
	state.epochInitialized = true
	return state, nil
}

func (s byronUpdateState) pruneExpired(slot uint64) byronUpdateState {
	state := s.clone()
	ttl := uint64(0)
	if state.params.UpdateImplicit > 0 {
		ttl = uint64(state.params.UpdateImplicit)
	}
	for id, proposal := range state.proposals {
		_, confirmed := state.confirmed[id]
		if confirmed || (proposal.registeredAt <= slot && slot-proposal.registeredAt <= ttl) {
			continue
		}
		delete(state.proposals, id)
		delete(state.votes, id)
	}
	for version := range state.endorsements {
		found := false
		for _, proposal := range state.proposals {
			if proposal.hasProtocol && proposal.version == version {
				found = true
				break
			}
		}
		if !found {
			delete(state.endorsements, version)
		}
	}
	return state
}

func byronProtocolVersionLess(
	left byron.ByronBlockVersion,
	right byron.ByronBlockVersion,
) bool {
	if left.Major != right.Major {
		return left.Major < right.Major
	}
	if left.Minor != right.Minor {
		return left.Minor < right.Minor
	}
	return left.Unknown < right.Unknown
}

var byronFeeNanoScale = big.NewInt(1_000_000_000)

func roundByronNanoToInteger(value *big.Int) *big.Int {
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(value, byronFeeNanoScale, remainder)
	twiceRemainder := new(big.Int).Lsh(remainder, 1)
	twiceRemainder.Abs(twiceRemainder)
	comparison := twiceRemainder.Cmp(byronFeeNanoScale)
	if comparison > 0 || (comparison == 0 && quotient.Bit(0) == 1) {
		if value.Sign() < 0 {
			quotient.Sub(quotient, big.NewInt(1))
		} else {
			quotient.Add(quotient, big.NewInt(1))
		}
	}
	return quotient
}

// applyByronBlockVersionMod returns the block-version parameters produced by
// applying a Byron protocol-parameter update. The input remains unchanged so
// the caller can retain the prior parameters until the update is enacted.
func applyByronBlockVersionMod(
	current byron.ByronGenesisBlockVersionData,
	mod byron.ByronUpdateProposalBlockVersionMod,
) (byron.ByronGenesisBlockVersionData, error) {
	updated := current
	maxInt := int64(int(^uint(0) >> 1))
	setInt := func(name string, values []*big.Int, target *int) error {
		if len(values) == 0 {
			return nil
		}
		value := values[0]
		if value == nil || !value.IsInt64() || value.Sign() < 0 || value.Int64() > maxInt {
			return fmt.Errorf("byron update %s does not fit in int", name)
		}
		*target = int(value.Int64())
		return nil
	}
	setInt64 := func(name string, values []byron.ByronLovelacePortion, target *int64) error {
		if len(values) == 0 {
			return nil
		}
		if uint64(values[0]) > uint64(^uint64(0)>>1) {
			return fmt.Errorf("byron update %s does not fit in int64", name)
		}
		*target = int64(values[0]) // #nosec G115 -- the preceding bound checks this conversion.
		return nil
	}
	setUint64 := func(values []uint64, target *uint64) {
		if len(values) != 0 {
			*target = values[0]
		}
	}

	if len(mod.ScriptVersion) != 0 {
		if uint64(mod.ScriptVersion[0]) > uint64(int(^uint(0)>>1)) {
			return current, errors.New("byron update scriptVersion does not fit in int")
		}
		updated.ScriptVersion = int(mod.ScriptVersion[0])
	}
	for _, field := range []struct {
		name   string
		values []*big.Int
		target *int
	}{
		{"slotDuration", mod.SlotDuration, &updated.SlotDuration},
		{"maxBlockSize", mod.MaxBlockSize, &updated.MaxBlockSize},
		{"maxHeaderSize", mod.MaxHeaderSize, &updated.MaxHeaderSize},
		{"maxTxSize", mod.MaxTxSize, &updated.MaxTxSize},
		{"maxProposalSize", mod.MaxProposalSize, &updated.MaxProposalSize},
	} {
		if err := setInt(field.name, field.values, field.target); err != nil {
			return current, err
		}
	}
	for _, field := range []struct {
		name   string
		values []byron.ByronLovelacePortion
		target *int64
	}{
		{"mpcThd", mod.MpcThd, &updated.MpcThd},
		{"heavyDelThd", mod.HeavyDelThd, &updated.HeavyDelThd},
		{"updateVoteThd", mod.UpdateVoteThd, &updated.UpdateVoteThd},
		{"updateProposalThd", mod.UpdateProposalThd, &updated.UpdateProposalThd},
	} {
		if err := setInt64(field.name, field.values, field.target); err != nil {
			return current, err
		}
	}
	if len(mod.UpdateImplicit) != 0 {
		if mod.UpdateImplicit[0] > uint64(maxInt) {
			return current, errors.New("byron update updateImplicit does not fit in int")
		}
		updated.UpdateImplicit = int(mod.UpdateImplicit[0]) // #nosec G115 -- the preceding bound checks this conversion.
	}
	if len(mod.SoftForkRule) != 0 {
		rule := mod.SoftForkRule[0]
		maxInt64 := uint64(^uint64(0) >> 1)
		if uint64(rule.InitThreshold) > maxInt64 || uint64(rule.MinThreshold) > maxInt64 ||
			uint64(rule.ThresholdDecrement) > maxInt64 {
			return current, errors.New("byron update softforkRule does not fit in int64")
		}
		updated.SoftforkRule = byron.ByronGenesisBlockVersionDataSoftforkRule{
			InitThd:      int64(rule.InitThreshold),      // #nosec G115 -- all thresholds were bounded above.
			MinThd:       int64(rule.MinThreshold),       // #nosec G115 -- all thresholds were bounded above.
			ThdDecrement: int64(rule.ThresholdDecrement), // #nosec G115 -- all thresholds were bounded above.
		}
	}
	if len(mod.TxFeePolicy) != 0 {
		policy := mod.TxFeePolicy[0]
		if policy.SummandNano == nil || policy.MultiplierNano == nil {
			return current, errors.New("byron update txFeePolicy is missing a coefficient")
		}
		if policy.SummandNano.Sign() < 0 || policy.MultiplierNano.Sign() < 0 {
			return current, errors.New("byron update txFeePolicy coefficients must be nonnegative")
		}
		summand := roundByronNanoToInteger(policy.SummandNano)
		multiplier := roundByronNanoToInteger(policy.MultiplierNano)
		if !summand.IsInt64() || !multiplier.IsInt64() {
			return current, errors.New("byron update txFeePolicy coefficient does not fit in int64")
		}
		updated.TxFeePolicy = byron.ByronGenesisBlockVersionDataTxFeePolicy{
			Summand:    summand.Int64(),
			Multiplier: multiplier.Int64(),
		}
	}
	setUint64(mod.UnlockStakeEpoch, &updated.UnlockStakeEpoch)
	return updated, nil
}
