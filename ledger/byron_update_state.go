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
	"maps"
	"math"
	"math/big"
	"unicode/utf8"

	"github.com/blinklabs-io/gouroboros/cbor"
	byronconsensus "github.com/blinklabs-io/gouroboros/consensus/byron"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/common"
)

type byronUpdateProposalID [common.Blake2b256Size]byte

// byronProtocolParameters holds Byron's adopted protocol parameters in the
// reference value ranges. Size limits are Natural, so an update can carry
// values beyond int64; the fee policy is whole Lovelace plus an exact
// multiplier in nano-Lovelace per byte.
type byronProtocolParameters struct {
	scriptVersion     uint16
	slotDuration      *big.Int
	maxBlockSize      *big.Int
	maxHeaderSize     *big.Int
	maxTxSize         *big.Int
	maxProposalSize   *big.Int
	mpcThd            uint64
	heavyDelThd       uint64
	updateVoteThd     uint64
	updateProposalThd uint64
	updateProposalTTL uint64
	softforkInitThd   uint64
	softforkMinThd    uint64
	softforkThdDec    uint64
	feeSummand        uint64
	feeMultiplierNano *big.Int
	unlockStakeEpoch  uint64
}

var (
	byronFeeNanoScale         = big.NewInt(1_000_000_000)
	byronLovelacePortionScale = big.NewInt(1_000_000_000_000_000)
)

func byronNonNegative(name string, value int64) (uint64, error) {
	if value < 0 {
		return 0, fmt.Errorf(
			"byron genesis %s must not be negative, got %d",
			name, value,
		)
	}
	return uint64(value), nil
}

// byronProtocolParametersFromGenesis loads genesis blockVersionData the way
// the reference FromJSON instances do: the fee summand is summand div 10^9
// and the multiplier is multiplier % 10^9.
func byronProtocolParametersFromGenesis(
	data byron.ByronGenesisBlockVersionData,
) (byronProtocolParameters, error) {
	if data.ScriptVersion < 0 || data.ScriptVersion > math.MaxUint16 {
		return byronProtocolParameters{}, fmt.Errorf(
			"byron genesis scriptVersion %d does not fit in Word16",
			data.ScriptVersion,
		)
	}
	params := byronProtocolParameters{
		scriptVersion:     uint16(data.ScriptVersion),
		unlockStakeEpoch:  data.UnlockStakeEpoch,
		feeMultiplierNano: big.NewInt(data.TxFeePolicy.Multiplier),
	}
	for _, field := range []struct {
		name   string
		value  int
		target **big.Int
	}{
		{"slotDuration", data.SlotDuration, &params.slotDuration},
		{"maxBlockSize", data.MaxBlockSize, &params.maxBlockSize},
		{"maxHeaderSize", data.MaxHeaderSize, &params.maxHeaderSize},
		{"maxTxSize", data.MaxTxSize, &params.maxTxSize},
		{"maxProposalSize", data.MaxProposalSize, &params.maxProposalSize},
	} {
		value, err := byronNonNegative(field.name, int64(field.value))
		if err != nil {
			return byronProtocolParameters{}, err
		}
		*field.target = new(big.Int).SetUint64(value)
	}
	for _, field := range []struct {
		name   string
		value  int64
		target *uint64
	}{
		{"mpcThd", data.MpcThd, &params.mpcThd},
		{"heavyDelThd", data.HeavyDelThd, &params.heavyDelThd},
		{"updateVoteThd", data.UpdateVoteThd, &params.updateVoteThd},
		{"updateProposalThd", data.UpdateProposalThd, &params.updateProposalThd},
		{"updateImplicit", int64(data.UpdateImplicit), &params.updateProposalTTL},
		{"softforkRule.initThd", data.SoftforkRule.InitThd, &params.softforkInitThd},
		{"softforkRule.minThd", data.SoftforkRule.MinThd, &params.softforkMinThd},
		{"softforkRule.thdDecrement", data.SoftforkRule.ThdDecrement, &params.softforkThdDec},
	} {
		value, err := byronNonNegative(field.name, field.value)
		if err != nil {
			return byronProtocolParameters{}, err
		}
		*field.target = value
	}
	summand, err := byronNonNegative(
		"txFeePolicy.summand",
		data.TxFeePolicy.Summand,
	)
	if err != nil {
		return byronProtocolParameters{}, err
	}
	params.feeSummand = summand / byronFeeNanoScale.Uint64()
	return params, nil
}

func (p byronProtocolParameters) equal(other byronProtocolParameters) bool {
	return p.scriptVersion == other.scriptVersion &&
		p.slotDuration.Cmp(other.slotDuration) == 0 &&
		p.maxBlockSize.Cmp(other.maxBlockSize) == 0 &&
		p.maxHeaderSize.Cmp(other.maxHeaderSize) == 0 &&
		p.maxTxSize.Cmp(other.maxTxSize) == 0 &&
		p.maxProposalSize.Cmp(other.maxProposalSize) == 0 &&
		p.mpcThd == other.mpcThd &&
		p.heavyDelThd == other.heavyDelThd &&
		p.updateVoteThd == other.updateVoteThd &&
		p.updateProposalThd == other.updateProposalThd &&
		p.updateProposalTTL == other.updateProposalTTL &&
		p.softforkInitThd == other.softforkInitThd &&
		p.softforkMinThd == other.softforkMinThd &&
		p.softforkThdDec == other.softforkThdDec &&
		p.feeSummand == other.feeSummand &&
		p.feeMultiplierNano.Cmp(other.feeMultiplierNano) == 0 &&
		p.unlockStakeEpoch == other.unlockStakeEpoch
}

// byronSizeLimit saturates a Natural size limit. Every encoded block, header
// and transaction is far below math.MaxUint64 bytes, so a comparison against
// the saturated value decides exactly as one against the Natural.
func byronSizeLimit(value *big.Int) uint64 {
	if !value.IsUint64() {
		return math.MaxUint64
	}
	return value.Uint64()
}

func (p byronProtocolParameters) blockLimits() byronBlockLimits {
	return byronBlockLimits{
		maxBlockSize:  byronSizeLimit(p.maxBlockSize),
		maxHeaderSize: byronSizeLimit(p.maxHeaderSize),
	}
}

func (p byronProtocolParameters) maxTxSizeLimit() uint64 {
	return byronSizeLimit(p.maxTxSize)
}

// feePolicyNano returns the fee policy with both coefficients scaled by
// 10^9, the representation eras.ByronFeePolicyProvider carries. A policy
// outside that representation is an error rather than a rounded value.
func (p byronProtocolParameters) feePolicyNano() (int64, int64, error) {
	summand := new(big.Int).Mul(
		new(big.Int).SetUint64(p.feeSummand),
		byronFeeNanoScale,
	)
	if !summand.IsInt64() || !p.feeMultiplierNano.IsInt64() {
		return 0, 0, fmt.Errorf(
			"byron fee policy summand %d multiplier %s nano-Lovelace exceeds the supported range",
			p.feeSummand,
			p.feeMultiplierNano,
		)
	}
	return summand.Int64(), p.feeMultiplierNano.Int64(), nil
}

// adoptionThreshold is upAdptThd: floor(minThd * numGenesisKeys), with minThd
// the adopted softfork rule's LovelacePortion over 10^15. The reference uses
// it both to confirm a proposal by votes and to adopt a version by
// endorsements; updateVoteThd and updateProposalThd take no part.
func (p byronProtocolParameters) adoptionThreshold(numGenesisKeys int) int {
	value := new(big.Int).Mul(
		new(big.Int).SetUint64(p.softforkMinThd),
		big.NewInt(int64(numGenesisKeys)),
	)
	value.Quo(value, byronLovelacePortionScale)
	if !value.IsInt64() || value.Int64() > math.MaxInt {
		return math.MaxInt
	}
	return int(value.Int64())
}

type byronProtocolProposal struct {
	version byron.ByronBlockVersion
	params  byronProtocolParameters
}

type byronSoftwareProposal struct {
	name    string
	version uint32
}

type byronEndorsement struct {
	version byron.ByronBlockVersion
	genesis common.Blake2b224
}

type byronProtocolAdoption struct {
	slot    uint64
	version byron.ByronBlockVersion
	params  byronProtocolParameters
}

// byronUpdateState is the Byron update-interface state. Proposals are keyed
// by UpId, the hash of the proposal's complete encoding. Slots are absolute
// slots under the network's configured epoch length.
type byronUpdateState struct {
	protocolVersion   byron.ByronBlockVersion
	params            byronProtocolParameters
	numGenesisKeys    int
	lastEpoch         uint64
	candidates        []byronProtocolAdoption
	applications      map[string]uint32
	protocolProposals map[byronUpdateProposalID]byronProtocolProposal
	softwareProposals map[byronUpdateProposalID]byronSoftwareProposal
	confirmed         map[byronUpdateProposalID]uint64
	votes             map[byronUpdateProposalID]map[common.Blake2b224]struct{}
	endorsements      map[byronEndorsement]struct{}
	registeredAt      map[byronUpdateProposalID]uint64
}

func newByronUpdateState(
	genesis byron.ByronGenesisBlockVersionData,
	numGenesisKeys int,
) (byronUpdateState, error) {
	params, err := byronProtocolParametersFromGenesis(genesis)
	if err != nil {
		return byronUpdateState{}, err
	}
	return byronUpdateState{
		params:            params,
		numGenesisKeys:    numGenesisKeys,
		applications:      make(map[string]uint32),
		protocolProposals: make(map[byronUpdateProposalID]byronProtocolProposal),
		softwareProposals: make(map[byronUpdateProposalID]byronSoftwareProposal),
		confirmed:         make(map[byronUpdateProposalID]uint64),
		votes:             make(map[byronUpdateProposalID]map[common.Blake2b224]struct{}),
		endorsements:      make(map[byronEndorsement]struct{}),
		registeredAt:      make(map[byronUpdateProposalID]uint64),
	}, nil
}

func (s *byronUpdateState) clone() byronUpdateState {
	ret := *s
	ret.candidates = append([]byronProtocolAdoption(nil), s.candidates...)
	ret.applications = maps.Clone(s.applications)
	ret.protocolProposals = maps.Clone(s.protocolProposals)
	ret.softwareProposals = maps.Clone(s.softwareProposals)
	ret.confirmed = maps.Clone(s.confirmed)
	ret.votes = make(
		map[byronUpdateProposalID]map[common.Blake2b224]struct{},
		len(s.votes),
	)
	for id, voters := range s.votes {
		ret.votes[id] = maps.Clone(voters)
	}
	ret.endorsements = maps.Clone(s.endorsements)
	ret.registeredAt = maps.Clone(s.registeredAt)
	return ret
}

// genesisForByronDelegate is Delegation.lookupR: the genesis key whose active
// delegate is delegate.
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

// advanceEpoch is the reference epochTransition for a main block in epoch.
// It must not run for EBBs: they leave cvsLastSlot, and so the epoch compared
// here, unchanged, and ticking at an EBB could adopt an older candidate than
// the first main block of a later epoch would.
func (s *byronUpdateState) advanceEpoch(
	epoch uint64,
	epochFirstSlot uint64,
	securityParam uint64,
) (byronUpdateState, error) {
	if epoch < s.lastEpoch {
		return *s, fmt.Errorf(
			"byron update epoch regressed from %d to %d",
			s.lastEpoch, epoch,
		)
	}
	if epoch == s.lastEpoch {
		return *s, nil
	}
	state := s.registerEpoch(epochFirstSlot, securityParam)
	state.lastEpoch = epoch
	return state, nil
}

// registerEpoch adopts the newest candidate that became a candidate at least
// 4k slots before the first slot of the new epoch (tryBumpVersion). Slot
// arithmetic wraps like the reference's Word64 SlotNumber.
func (s *byronUpdateState) registerEpoch(
	epochFirstSlot uint64,
	securityParam uint64,
) byronUpdateState {
	stability := 4 * securityParam
	for _, candidate := range s.candidates {
		if candidate.slot+stability > epochFirstSlot {
			continue
		}
		if candidate.version == s.protocolVersion {
			return s.clone()
		}
		state := s.clone()
		state.protocolVersion = candidate.version
		state.params = candidate.params
		state.candidates = nil
		state.protocolProposals = make(map[byronUpdateProposalID]byronProtocolProposal)
		state.softwareProposals = make(map[byronUpdateProposalID]byronSoftwareProposal)
		state.confirmed = make(map[byronUpdateProposalID]uint64)
		state.votes = make(map[byronUpdateProposalID]map[common.Blake2b224]struct{})
		state.endorsements = make(map[byronEndorsement]struct{})
		state.registeredAt = make(map[byronUpdateProposalID]uint64)
		return state
	}
	return s.clone()
}

// byronUpdateSignal is one main block's input to the update interface: its
// proposal and votes, and the endorsement its header implies.
type byronUpdateSignal struct {
	proposal     *byron.ByronUpdateProposal
	votes        []*byron.UpdateVote
	endorsement  byron.ByronBlockVersion
	endorserHash common.Blake2b224
}

// byronUpdateSignalFromBlock extracts the update signal of a main block whose
// update payload has passed ValidateUpdatePayload.
func byronUpdateSignalFromBlock(
	block *byron.ByronMainBlock,
	endorserHash common.Blake2b224,
) (byronUpdateSignal, error) {
	signal := byronUpdateSignal{
		endorsement:  block.BlockHeader.ExtraData.BlockVersion,
		endorserHash: endorserHash,
	}
	proposals := block.Body.UpdPayload.Proposals
	switch len(proposals) {
	case 0:
	case 1:
		proposal := proposals[0]
		signal.proposal = &proposal
	default:
		return byronUpdateSignal{}, fmt.Errorf(
			"byron update payload carries %d proposals, expected at most one",
			len(proposals),
		)
	}
	rawVotes, err := byronUpdateVoteEntries(block)
	if err != nil {
		return byronUpdateSignal{}, err
	}
	for index, rawVote := range rawVotes {
		vote, err := byron.ParseUpdateVote(rawVote)
		if err != nil {
			return byronUpdateSignal{}, fmt.Errorf(
				"parse Byron update vote %d: %w",
				index, err,
			)
		}
		signal.votes = append(signal.votes, vote)
	}
	return signal, nil
}

func byronUpdateVoteEntries(
	block *byron.ByronMainBlock,
) ([]cbor.RawMessage, error) {
	if len(block.Body.UpdPayload.Votes) == 0 {
		return nil, nil
	}
	var bodyFields []cbor.RawMessage
	if _, err := cbor.Decode(block.Body.Cbor(), &bodyFields); err != nil {
		return nil, fmt.Errorf(
			"decode Byron main-block body for update votes: %w",
			err,
		)
	}
	if len(bodyFields) != 4 {
		return nil, fmt.Errorf(
			"byron main-block body has %d fields, expected 4",
			len(bodyFields),
		)
	}
	var updateFields []cbor.RawMessage
	if _, err := cbor.Decode(bodyFields[3], &updateFields); err != nil {
		return nil, fmt.Errorf("decode Byron update payload for votes: %w", err)
	}
	if len(updateFields) != 2 {
		return nil, fmt.Errorf(
			"byron update payload has %d fields, expected 2",
			len(updateFields),
		)
	}
	var votes []cbor.RawMessage
	if _, err := cbor.Decode(updateFields[1], &votes); err != nil {
		return nil, fmt.Errorf("decode Byron update votes: %w", err)
	}
	return votes, nil
}

// registerUpdate applies registerProposal, registerVotes and
// registerEndorsement in that order. delegations is the delegation map as it
// stood after the previous main block: the reference registers a block's
// update payload against the delegation state its own certificates and tick
// have not yet changed.
func (s *byronUpdateState) registerUpdate(
	signal byronUpdateSignal,
	delegations map[common.Blake2b224]common.Blake2b224,
	protocolMagic uint32,
	slot uint64,
	securityParam uint64,
) (byronUpdateState, error) {
	state := s.clone()
	if signal.proposal != nil {
		if err := state.registerProposal(
			*signal.proposal, delegations, protocolMagic, slot,
		); err != nil {
			return *s, err
		}
	}
	if err := state.registerVotes(
		signal.votes, delegations, protocolMagic, slot,
	); err != nil {
		return *s, err
	}
	if err := state.registerEndorsement(
		signal.endorsement,
		signal.endorserHash,
		delegations,
		slot,
		securityParam,
	); err != nil {
		return *s, err
	}
	return state, nil
}

// byronNullUpdateExempt reproduces the reference exemption for the two null
// update proposals on the legacy staging network.
func byronNullUpdateExempt(protocolMagic uint32, slot uint64) bool {
	return protocolMagic == 633343913 && (slot == 969188 || slot == 1915231)
}

func (s *byronUpdateState) registerProposal(
	proposal byron.ByronUpdateProposal,
	delegations map[common.Blake2b224]common.Blake2b224,
	protocolMagic uint32,
	slot uint64,
) error {
	proposerHash, err := byronconsensus.PBFTVerificationKeyHash(proposal.From)
	if err != nil {
		return fmt.Errorf("hash Byron update proposer key: %w", err)
	}
	if _, ok := genesisForByronDelegate(delegations, proposerHash); !ok {
		return fmt.Errorf(
			"byron update proposer %x is not a genesis delegate",
			proposerHash,
		)
	}
	if err := proposal.Validate(protocolMagic); err != nil {
		return fmt.Errorf("validate Byron update proposal: %w", err)
	}
	newParams, err := applyByronBlockVersionMod(
		s.params,
		proposal.BlockVersionMod,
	)
	if err != nil {
		return fmt.Errorf("apply Byron update proposal parameters: %w", err)
	}
	name := proposal.SoftwareVersion.Name
	currentAppVersion, knownApp := s.applications[name]
	softwareChanged := !knownApp ||
		currentAppVersion != proposal.SoftwareVersion.Version
	protocolChanged := proposal.BlockVersion != s.protocolVersion ||
		!newParams.equal(s.params)
	if !protocolChanged && !softwareChanged &&
		!byronNullUpdateExempt(protocolMagic, slot) {
		return errors.New(
			"byron update proposal changes neither protocol nor software version",
		)
	}
	if protocolChanged {
		if err := s.validateProtocolUpdate(proposal, newParams); err != nil {
			return err
		}
	}
	if softwareChanged {
		if err := s.validateSoftwareUpdate(proposal); err != nil {
			return err
		}
	}
	proposalID := byronUpdateProposalID(common.Blake2b256Hash(proposal.Cbor()))
	if protocolChanged {
		s.protocolProposals[proposalID] = byronProtocolProposal{
			version: proposal.BlockVersion,
			params:  newParams,
		}
	}
	if softwareChanged {
		s.softwareProposals[proposalID] = byronSoftwareProposal{
			name:    name,
			version: proposal.SoftwareVersion.Version,
		}
	}
	s.registeredAt[proposalID] = slot
	return nil
}

// validateProtocolUpdate is registerProtocolUpdate with canUpdate. The
// proposal-size bound belongs to canUpdate, so a software-only proposal is
// not subject to it.
func (s *byronUpdateState) validateProtocolUpdate(
	proposal byron.ByronUpdateProposal,
	newParams byronProtocolParameters,
) error {
	version := proposal.BlockVersion
	for _, existing := range s.protocolProposals {
		if existing.version == version {
			return fmt.Errorf(
				"byron protocol version %d.%d.%d is already proposed",
				version.Major, version.Minor, version.Unknown,
			)
		}
	}
	if !byronProtocolVersionCanFollow(version, s.protocolVersion) {
		return fmt.Errorf(
			"byron protocol version %d.%d.%d cannot follow %d.%d.%d",
			version.Major, version.Minor, version.Unknown,
			s.protocolVersion.Major, s.protocolVersion.Minor,
			s.protocolVersion.Unknown,
		)
	}
	proposalSize := big.NewInt(int64(len(proposal.Cbor())))
	if proposalSize.Cmp(s.params.maxProposalSize) > 0 {
		return fmt.Errorf(
			"byron update proposal size %s exceeds maxProposalSize %s",
			proposalSize, s.params.maxProposalSize,
		)
	}
	doubled := new(big.Int).Lsh(s.params.maxBlockSize, 1)
	if newParams.maxBlockSize.Cmp(doubled) > 0 {
		return fmt.Errorf(
			"byron update maxBlockSize %s exceeds twice the adopted %s",
			newParams.maxBlockSize, s.params.maxBlockSize,
		)
	}
	if newParams.maxTxSize.Cmp(newParams.maxBlockSize) >= 0 {
		return fmt.Errorf(
			"byron update maxTxSize %s must be less than maxBlockSize %s",
			newParams.maxTxSize, newParams.maxBlockSize,
		)
	}
	// The reference subtracts Word16 values, so the difference wraps.
	if newParams.scriptVersion-s.params.scriptVersion > 1 {
		return fmt.Errorf(
			"byron update scriptVersion %d must equal or follow %d",
			newParams.scriptVersion, s.params.scriptVersion,
		)
	}
	return nil
}

// byronProtocolVersionCanFollow is pvCanFollow. Major and minor differences
// are Word16 arithmetic in the reference.
func byronProtocolVersionCanFollow(
	next byron.ByronBlockVersion,
	adopted byron.ByronBlockVersion,
) bool {
	if !byronProtocolVersionLess(adopted, next) {
		return false
	}
	switch next.Major - adopted.Major {
	case 0:
		return next.Minor == adopted.Minor+1
	case 1:
		return next.Minor == 0
	default:
		return false
	}
}

// validateSoftwareUpdate is registerSoftwareUpdate. The metadata system tags
// are checked by ByronUpdateProposal.Validate.
func (s *byronUpdateState) validateSoftwareUpdate(
	proposal byron.ByronUpdateProposal,
) error {
	name := proposal.SoftwareVersion.Name
	for _, existing := range s.softwareProposals {
		if existing.name == name {
			return fmt.Errorf(
				"byron software update for %q is already proposed",
				name,
			)
		}
	}
	if utf8.RuneCountInString(name) > 12 {
		return fmt.Errorf("byron application name %q is too long", name)
	}
	for _, r := range name {
		if r > 0x7f {
			return fmt.Errorf("byron application name %q is not ASCII", name)
		}
	}
	version := proposal.SoftwareVersion.Version
	current, known := s.applications[name]
	if !known {
		if version != 0 && version != 1 {
			return fmt.Errorf(
				"byron software version %d for new application %q must be 0 or 1",
				version, name,
			)
		}
		return nil
	}
	// Word32 arithmetic, as in svCanFollow.
	if version != current+1 {
		return fmt.Errorf(
			"byron software version %d for %q must follow %d",
			version, name, current,
		)
	}
	return nil
}

// registerVotes folds registerVoteWithConfirmation over the votes, then
// records the version of every confirmed software proposal and removes those
// proposals, so the application can be proposed again.
func (s *byronUpdateState) registerVotes(
	votes []*byron.UpdateVote,
	delegations map[common.Blake2b224]common.Blake2b224,
	protocolMagic uint32,
	slot uint64,
) error {
	threshold := s.params.adoptionThreshold(s.numGenesisKeys)
	for index, vote := range votes {
		if vote == nil || len(vote.ProposalId) != common.Blake2b256Size {
			return fmt.Errorf("byron update vote %d is malformed", index)
		}
		proposalID := byronUpdateProposalID(vote.ProposalId)
		if _, registered := s.registeredAt[proposalID]; !registered {
			return fmt.Errorf(
				"byron update vote %d references unregistered proposal %x",
				index, vote.ProposalId,
			)
		}
		voterHash, err := byronconsensus.PBFTVerificationKeyHash(vote.VoterVK)
		if err != nil {
			return fmt.Errorf(
				"hash Byron update vote %d voter key: %w",
				index, err,
			)
		}
		genesis, ok := genesisForByronDelegate(delegations, voterHash)
		if !ok {
			return fmt.Errorf(
				"byron update vote %d voter %x is not a genesis delegate",
				index, voterHash,
			)
		}
		voters := s.votes[proposalID]
		if _, voted := voters[genesis]; voted {
			return fmt.Errorf(
				"byron genesis key %x voted more than once on proposal %x",
				genesis, vote.ProposalId,
			)
		}
		if err := vote.Verify(protocolMagic); err != nil {
			return fmt.Errorf("verify Byron update vote %d: %w", index, err)
		}
		if voters == nil {
			voters = make(map[common.Blake2b224]struct{})
			s.votes[proposalID] = voters
		}
		voters[genesis] = struct{}{}
		if _, confirmed := s.confirmed[proposalID]; !confirmed &&
			len(voters) >= threshold {
			s.confirmed[proposalID] = slot
		}
	}
	for proposalID, proposal := range s.softwareProposals {
		if _, confirmed := s.confirmed[proposalID]; !confirmed {
			continue
		}
		s.applications[proposal.name] = proposal.version
		delete(s.softwareProposals, proposalID)
	}
	return nil
}

// registerEndorsement is Endorsement.register followed by the interface's
// removal of expired, unconfirmed proposals. An endorsement of an unproposed
// version, of a proposal not yet confirmed for 2k slots, or by a key that is
// not a genesis delegate is ignored rather than rejected.
func (s *byronUpdateState) registerEndorsement(
	version byron.ByronBlockVersion,
	endorserHash common.Blake2b224,
	delegations map[common.Blake2b224]common.Blake2b224,
	slot uint64,
	securityParam uint64,
) error {
	var (
		proposalID byronUpdateProposalID
		proposal   byronProtocolProposal
		matches    int
	)
	for id, candidate := range s.protocolProposals {
		if candidate.version == version {
			proposalID, proposal = id, candidate
			matches++
		}
	}
	if matches > 1 {
		return fmt.Errorf(
			"multiple Byron update proposals target protocol version %d.%d.%d",
			version.Major, version.Minor, version.Unknown,
		)
	}
	if matches == 1 {
		confirmedAt, confirmed := s.confirmed[proposalID]
		if confirmed && confirmedAt+2*securityParam <= slot {
			genesis, ok := genesisForByronDelegate(delegations, endorserHash)
			if ok {
				s.endorsements[byronEndorsement{
					version: version,
					genesis: genesis,
				}] = struct{}{}
			}
			endorsed := 0
			for endorsement := range s.endorsements {
				if endorsement.version == version {
					endorsed++
				}
			}
			if endorsed >= s.params.adoptionThreshold(s.numGenesisKeys) &&
				(len(s.candidates) == 0 ||
					byronProtocolVersionLess(s.candidates[0].version, version)) {
				s.candidates = append([]byronProtocolAdoption{{
					slot:    slot,
					version: version,
					params:  proposal.params,
				}}, s.candidates...)
			}
		}
	}
	s.pruneExpired(slot)
	return nil
}

// pruneExpired keeps proposals registered within ppUpdateProposalTTL slots,
// and every confirmed proposal. Endorsements survive only for versions a
// remaining protocol proposal targets.
func (s *byronUpdateState) pruneExpired(slot uint64) {
	ttl := s.params.updateProposalTTL
	for proposalID, registeredAt := range s.registeredAt {
		if _, confirmed := s.confirmed[proposalID]; confirmed {
			continue
		}
		// Word64 addition, which wraps as the reference's addSlotCount does.
		if slot <= registeredAt+ttl {
			continue
		}
		delete(s.registeredAt, proposalID)
		delete(s.protocolProposals, proposalID)
		delete(s.softwareProposals, proposalID)
		delete(s.votes, proposalID)
	}
	versions := make(
		map[byron.ByronBlockVersion]struct{},
		len(s.protocolProposals),
	)
	for _, proposal := range s.protocolProposals {
		versions[proposal.version] = struct{}{}
	}
	for endorsement := range s.endorsements {
		if _, ok := versions[endorsement.version]; !ok {
			delete(s.endorsements, endorsement)
		}
	}
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

// roundByronNanoToInteger rounds a Nano value to an integer, halves to even,
// as the reference's round does when TxSizeLinear decodes its summand.
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

// applyByronBlockVersionMod is ProtocolParametersUpdate.apply. The input is
// not modified, so the adopted parameters stay in force until an adoption.
func applyByronBlockVersionMod(
	current byronProtocolParameters,
	mod byron.ByronUpdateProposalBlockVersionMod,
) (byronProtocolParameters, error) {
	updated := current
	if len(mod.ScriptVersion) != 0 {
		updated.scriptVersion = mod.ScriptVersion[0]
	}
	for _, field := range []struct {
		name   string
		values []*big.Int
		target **big.Int
	}{
		{"slotDuration", mod.SlotDuration, &updated.slotDuration},
		{"maxBlockSize", mod.MaxBlockSize, &updated.maxBlockSize},
		{"maxHeaderSize", mod.MaxHeaderSize, &updated.maxHeaderSize},
		{"maxTxSize", mod.MaxTxSize, &updated.maxTxSize},
		{"maxProposalSize", mod.MaxProposalSize, &updated.maxProposalSize},
	} {
		if len(field.values) == 0 {
			continue
		}
		value := field.values[0]
		if value == nil || value.Sign() < 0 {
			return current, fmt.Errorf(
				"byron update %s is not a Natural",
				field.name,
			)
		}
		*field.target = new(big.Int).Set(value)
	}
	for _, field := range []struct {
		values []byron.ByronLovelacePortion
		target *uint64
	}{
		{mod.MpcThd, &updated.mpcThd},
		{mod.HeavyDelThd, &updated.heavyDelThd},
		{mod.UpdateVoteThd, &updated.updateVoteThd},
		{mod.UpdateProposalThd, &updated.updateProposalThd},
	} {
		if len(field.values) != 0 {
			*field.target = uint64(field.values[0])
		}
	}
	if len(mod.UpdateImplicit) != 0 {
		updated.updateProposalTTL = mod.UpdateImplicit[0]
	}
	if len(mod.SoftForkRule) != 0 {
		rule := mod.SoftForkRule[0]
		updated.softforkInitThd = uint64(rule.InitThreshold)
		updated.softforkMinThd = uint64(rule.MinThreshold)
		updated.softforkThdDec = uint64(rule.ThresholdDecrement)
	}
	if len(mod.TxFeePolicy) != 0 {
		policy := mod.TxFeePolicy[0]
		if policy.SummandNano == nil || policy.MultiplierNano == nil {
			return current, errors.New(
				"byron update txFeePolicy is missing a coefficient",
			)
		}
		// DecCBOR TxSizeLinear rounds only the summand to whole Lovelace
		// and keeps the multiplier as an exact Nano rational.
		summand := roundByronNanoToInteger(policy.SummandNano)
		if summand.Sign() < 0 || !summand.IsUint64() {
			return current, fmt.Errorf(
				"byron update txFeePolicy summand %s is not a Lovelace value",
				summand,
			)
		}
		updated.feeSummand = summand.Uint64()
		updated.feeMultiplierNano = new(big.Int).Set(policy.MultiplierNano)
	}
	if len(mod.UnlockStakeEpoch) != 0 {
		updated.unlockStakeEpoch = mod.UnlockStakeEpoch[0]
	}
	return updated, nil
}
