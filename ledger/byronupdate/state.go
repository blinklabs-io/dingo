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

// Package byronupdate implements the Byron update-system state transition:
// update proposal registration, votes, confirmation, protocol-version
// endorsement and adoption at an epoch boundary. It follows
// Cardano.Chain.Update.Validation.Interface and its Registration, Voting and
// Endorsement rules in cardano-ledger's Byron ledger, plus the candidate
// block-depth tracking ouroboros-consensus keeps to decide the Byron-to-Shelley
// transition.
//
// A State is a value: every transition returns a new State and leaves its
// receiver untouched, so a caller can keep an older State as a rollback
// snapshot.
package byronupdate

import (
	"maps"
	"slices"

	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// KeyHash is a Byron verification-key hash (hashKey).
type KeyHash = lcommon.Blake2b224

// UpId identifies an update proposal: the hash of its serialized form.
type UpId = lcommon.Blake2b256

// ProtocolVersion is a Byron protocol version. Versions are ordered
// lexicographically by major, minor and alt.
type ProtocolVersion struct {
	Major uint16
	Minor uint16
	Alt   uint8
}

// Less reports whether v precedes other.
func (v ProtocolVersion) Less(other ProtocolVersion) bool {
	if v.Major != other.Major {
		return v.Major < other.Major
	}
	if v.Minor != other.Minor {
		return v.Minor < other.Minor
	}
	return v.Alt < other.Alt
}

// canFollow is pvCanFollow: a proposed version must be greater than the
// adopted one and either the next minor version or the next major version
// with minor zero.
func (v ProtocolVersion) canFollow(adopted ProtocolVersion) bool {
	if !adopted.Less(v) {
		return false
	}
	switch int(v.Major) - int(adopted.Major) {
	case 0:
		return v.Minor == adopted.Minor+1
	case 1:
		return v.Minor == 0
	default:
		return false
	}
}

// Candidate is a protocol update endorsed by enough genesis keys to be
// adopted once it is stable (CandidateProtocolUpdate).
type Candidate struct {
	Slot    uint64
	Version ProtocolVersion
	Params  *eras.ByronProtocolParameters
}

type protocolUpdateProposal struct {
	version ProtocolVersion
	params  *eras.ByronProtocolParameters
}

type softwareUpdateProposal struct {
	appName string
	version uint32
}

type applicationVersion struct {
	version uint32
	slot    uint64
}

type endorsement struct {
	version ProtocolVersion
	keyHash KeyHash
}

// Config holds the chain constants the update rules read.
type Config struct {
	ProtocolMagic uint32
	// K is the Byron security parameter.
	K uint64
	// NumGenesisKeys is the number of genesis keys, the base of the
	// adoption threshold.
	NumGenesisKeys int
}

func (c Config) epochSlots() uint64 {
	return 10 * c.K
}

// State is the Byron update-interface state (UPI.State) together with the
// chain tip it describes.
type State struct {
	adoptedVersion ProtocolVersion
	adoptedParams  *eras.ByronProtocolParameters
	// candidates is ordered newest first.
	candidates        []Candidate
	appVersions       map[string]applicationVersion
	protocolProposals map[UpId]protocolUpdateProposal
	softwareProposals map[UpId]softwareUpdateProposal
	confirmed         map[UpId]uint64
	votes             map[UpId]map[KeyHash]struct{}
	endorsements      map[endorsement]struct{}
	registrationSlot  map[UpId]uint64
	// candidateBlockNo records the block number at which each candidate
	// version became a candidate (ouroboros-consensus ByronTransitionInfo).
	candidateBlockNo map[ProtocolVersion]uint64
	// lastSlot is cvsLastSlot, the slot of the last applied block.
	lastSlot    uint64
	tipBlockNo  uint64
	hasTip      bool
	initialized bool
	// complete records that the first block this state applied was the
	// chain's first block, so it holds the whole update history. A state
	// rebuilt from a trusted start after genesis misses every proposal
	// registered before that start.
	complete bool
}

// NewState returns the initial state: protocol version 0.0.0 with the
// genesis parameters.
func NewState(genesisParams *eras.ByronProtocolParameters) State {
	return State{
		adoptedParams: genesisParams.Clone(),
		initialized:   true,
	}
}

// Initialized reports whether s came from NewState.
func (s State) Initialized() bool {
	return s.initialized
}

// Complete reports whether s has applied the chain from its first block,
// block number 0 at slot 0, and so reflects every update registered on it.
func (s State) Complete() bool {
	return s.complete
}

// recordTip moves the tip to a newly applied block, noting on the first
// block whether it opens the chain.
func recordTip(s *State, slot, blockNo uint64) {
	if !s.hasTip {
		s.complete = slot == 0 && blockNo == 0
	}
	s.lastSlot = slot
	s.tipBlockNo = blockNo
	s.hasTip = true
}

// AdoptedVersion returns the adopted protocol version.
func (s State) AdoptedVersion() ProtocolVersion {
	return s.adoptedVersion
}

// AdoptedParams returns a copy of the adopted protocol parameters.
func (s State) AdoptedParams() *eras.ByronProtocolParameters {
	return s.adoptedParams.Clone()
}

// Candidates returns the candidate protocol updates, newest first.
func (s State) Candidates() []Candidate {
	ret := make([]Candidate, len(s.candidates))
	for i, candidate := range s.candidates {
		ret[i] = Candidate{
			Slot:    candidate.Slot,
			Version: candidate.Version,
			Params:  candidate.Params.Clone(),
		}
	}
	return ret
}

// clone returns a copy that shares nothing mutable with s. Parameters are
// never mutated in place, so they are shared.
func (s State) clone() State {
	ret := s
	ret.candidates = slices.Clone(s.candidates)
	ret.appVersions = maps.Clone(s.appVersions)
	ret.protocolProposals = maps.Clone(s.protocolProposals)
	ret.softwareProposals = maps.Clone(s.softwareProposals)
	ret.confirmed = maps.Clone(s.confirmed)
	ret.votes = make(map[UpId]map[KeyHash]struct{}, len(s.votes))
	for upId, voters := range s.votes {
		ret.votes[upId] = maps.Clone(voters)
	}
	ret.endorsements = maps.Clone(s.endorsements)
	ret.registrationSlot = maps.Clone(s.registrationSlot)
	ret.candidateBlockNo = maps.Clone(s.candidateBlockNo)
	if ret.appVersions == nil {
		ret.appVersions = make(map[string]applicationVersion)
	}
	if ret.protocolProposals == nil {
		ret.protocolProposals = make(map[UpId]protocolUpdateProposal)
	}
	if ret.softwareProposals == nil {
		ret.softwareProposals = make(map[UpId]softwareUpdateProposal)
	}
	if ret.confirmed == nil {
		ret.confirmed = make(map[UpId]uint64)
	}
	if ret.endorsements == nil {
		ret.endorsements = make(map[endorsement]struct{})
	}
	if ret.registrationSlot == nil {
		ret.registrationSlot = make(map[UpId]uint64)
	}
	if ret.candidateBlockNo == nil {
		ret.candidateBlockNo = make(map[ProtocolVersion]uint64)
	}
	return ret
}

// Tick applies the epoch transition for a block at slot (epochTransition, as
// applyChainTick runs it before every block, boundary blocks included). When
// slot opens a later epoch than the last applied block, the newest candidate
// that has been stable for 4k slots by the new epoch's first slot is adopted
// and every pending proposal, vote and endorsement is cleared.
func (s State) Tick(config Config, slot uint64) State {
	epochSlots := config.epochSlots()
	if epochSlots == 0 {
		return s
	}
	nextEpoch := slot / epochSlots
	if nextEpoch <= s.lastSlot/epochSlots {
		return s
	}
	firstSlot := nextEpoch * epochSlots
	for _, candidate := range s.candidates {
		// kUpdateStabilityParam: 4k slots.
		if candidate.Slot+4*config.K > firstSlot {
			continue
		}
		if candidate.Version == s.adoptedVersion {
			return s
		}
		ret := s.clone()
		ret.adoptedVersion = candidate.Version
		ret.adoptedParams = candidate.Params
		ret.candidates = nil
		ret.protocolProposals = make(map[UpId]protocolUpdateProposal)
		ret.softwareProposals = make(map[UpId]softwareUpdateProposal)
		ret.confirmed = make(map[UpId]uint64)
		ret.votes = make(map[UpId]map[KeyHash]struct{})
		ret.endorsements = make(map[endorsement]struct{})
		ret.registrationSlot = make(map[UpId]uint64)
		return ret
	}
	return s
}

// Advance records a block that registers nothing as the new tip: an epoch
// boundary block, which carries no update payload.
func (s State) Advance(slot, blockNo uint64) State {
	ret := s
	recordTip(&ret, slot, blockNo)
	return ret
}

// ShelleyTransitionEpoch reports the epoch at which the chain moves to the
// protocol major version shelleyMajor, if a candidate for it is known and
// stable (byronTransition in ouroboros-consensus). A candidate is stable when
// the tip is at least 2k slots past it, or at least k blocks past the block
// that made it a candidate. It takes effect in the epoch after the one
// containing its slot plus 4k.
func (s State) ShelleyTransitionEpoch(
	config Config,
	shelleyMajor uint16,
) (uint64, bool) {
	epochSlots := config.epochSlots()
	if epochSlots == 0 || !s.hasTip {
		return 0, false
	}
	for _, candidate := range s.candidates {
		if candidate.Version.Major != shelleyMajor {
			continue
		}
		adoptedIn := (candidate.Slot+4*config.K)/epochSlots + 1
		if s.lastSlot >= candidate.Slot &&
			s.lastSlot-candidate.Slot >= 2*config.K {
			return adoptedIn, true
		}
		blockNo, ok := s.candidateBlockNo[candidate.Version]
		if ok && s.tipBlockNo >= blockNo &&
			s.tipBlockNo-blockNo >= config.K {
			return adoptedIn, true
		}
	}
	return 0, false
}

// CheckTransition decides whether the block opening newEpoch may leave or
// must leave Byron under a version trigger for shelleyMajor. Byron may be
// left only in the epoch a stable candidate for that version is adopted in,
// and a Byron block is invalid from that epoch on.
func (s State) CheckTransition(
	config Config,
	shelleyMajor uint16,
	newEpoch uint64,
	leaving bool,
) error {
	transitionEpoch, known := s.ShelleyTransitionEpoch(config, shelleyMajor)
	switch {
	case leaving && (!known || transitionEpoch != newEpoch):
		return TransitionNotAdoptedError{Epoch: newEpoch, Major: shelleyMajor}
	case !leaving && known && transitionEpoch <= newEpoch:
		return TransitionMissedError{
			Epoch:           newEpoch,
			Major:           shelleyMajor,
			TransitionEpoch: transitionEpoch,
		}
	}
	return nil
}
