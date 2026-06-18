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

// Package praos contains pure Praos chain-selection primitives: the select
// view projected from a Shelley-family block header, VRF output extraction,
// and the equal-length tiebreaker comparison functions that mirror
// ouroboros-consensus' PraosTiebreakerView and preferCandidate logic.
//
// This package has no dependency on peer-selection lifecycle code; it is safe
// to import from ledger, chain-selection, or any other component that needs
// Praos ordering without pulling in the full ChainSelector.
package praos

import (
	"bytes"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

// VRFOutputSize is the expected size of a VRF output in bytes.
const VRFOutputSize = 64

const praosRestrictedTiebreakerMaxSlotDistance = 5

// PraosTiebreakerFlavor matches ouroboros-consensus' VRFTiebreakerFlavor.
type PraosTiebreakerFlavor uint8

const (
	PraosTiebreakerUnknown PraosTiebreakerFlavor = iota
	PraosTiebreakerUnrestricted
	PraosTiebreakerRestricted
)

// PraosTiebreakerConfig is the chain-order config projected from the current
// Shelley-family block config. The reference implementation uses unrestricted
// VRF before Conway and restricted VRF with max distance 5 from Conway onward.
type PraosTiebreakerConfig struct {
	Flavor          PraosTiebreakerFlavor
	MaxSlotDistance uint64
}

// PraosTiebreakerView mirrors ouroboros-consensus' PraosTiebreakerView for
// equal-length chain selection.
type PraosTiebreakerView struct {
	Slot              uint64
	Issuer            []byte
	IssueNo           uint64
	TieBreakVRF       []byte
	TiebreakerConfig  PraosTiebreakerConfig
	hasIssuerAndIssue bool
}

// PraosTiebreakerConfigBeforeConway returns the unrestricted VRF tiebreaker
// config used in eras before Conway.
func PraosTiebreakerConfigBeforeConway() PraosTiebreakerConfig {
	return PraosTiebreakerConfig{
		Flavor: PraosTiebreakerUnrestricted,
	}
}

// PraosTiebreakerConfigConway returns the restricted VRF tiebreaker config
// used from Conway onward (max slot distance 5).
func PraosTiebreakerConfigConway() PraosTiebreakerConfig {
	return PraosTiebreakerConfig{
		Flavor:          PraosTiebreakerRestricted,
		MaxSlotDistance: praosRestrictedTiebreakerMaxSlotDistance,
	}
}

// PraosTiebreakerConfigUnknown returns a zero-value config (flavor Unknown).
func PraosTiebreakerConfigUnknown() PraosTiebreakerConfig {
	return PraosTiebreakerConfig{}
}

func (c PraosTiebreakerConfig) known() bool {
	return c.Flavor == PraosTiebreakerUnrestricted ||
		c.Flavor == PraosTiebreakerRestricted
}

func (v PraosTiebreakerView) hasIssuerIssueNo() bool {
	return v.hasIssuerAndIssue && len(v.Issuer) > 0
}

// PraosTiebreakerViewFromTip builds a partial view when only a chainsync tip
// and VRF are available. The issuer/opcert rule cannot be applied to partial
// views.
func PraosTiebreakerViewFromTip(
	tip ochainsync.Tip,
	vrfOutput []byte,
	config PraosTiebreakerConfig,
) PraosTiebreakerView {
	return PraosTiebreakerView{
		Slot:             tip.Point.Slot,
		TieBreakVRF:      cloneBytes(vrfOutput),
		TiebreakerConfig: config,
	}
}

// NewPraosTiebreakerViewFull constructs a complete view with issuer and opcert
// issue number set. Use this when full header data is available (e.g. in
// tests, or when constructing views from decoded block headers manually).
func NewPraosTiebreakerViewFull(
	tip ochainsync.Tip,
	issuer []byte,
	issueNo uint64,
	vrfOutput []byte,
	config PraosTiebreakerConfig,
) PraosTiebreakerView {
	return PraosTiebreakerView{
		Slot:              tip.Point.Slot,
		Issuer:            cloneBytes(issuer),
		IssueNo:           issueNo,
		TieBreakVRF:       cloneBytes(vrfOutput),
		TiebreakerConfig:  config,
		hasIssuerAndIssue: true,
	}
}

// GetPraosTiebreakerView extracts the Praos select-view fields from a
// Shelley-family header.
func GetPraosTiebreakerView(
	header ledger.BlockHeader,
) (PraosTiebreakerView, bool) {
	if header == nil {
		return PraosTiebreakerView{}, false
	}
	issueNo, ok := headerIssueNo(header)
	if !ok {
		return PraosTiebreakerView{}, false
	}
	issuer := header.IssuerVkey()
	return PraosTiebreakerView{
		Slot:              header.SlotNumber(),
		Issuer:            cloneBytes(issuer[:]),
		IssueNo:           issueNo,
		TieBreakVRF:       cloneBytes(GetVRFOutput(header)),
		TiebreakerConfig:  praosTiebreakerConfigForHeader(header),
		hasIssuerAndIssue: true,
	}, true
}

func headerIssueNo(header ledger.BlockHeader) (uint64, bool) {
	switch h := header.(type) {
	case *shelley.ShelleyBlockHeader:
		return uint64(h.Body.OpCertSequenceNumber), true
	case *allegra.AllegraBlockHeader:
		return uint64(h.Body.OpCertSequenceNumber), true
	case *mary.MaryBlockHeader:
		return uint64(h.Body.OpCertSequenceNumber), true
	case *alonzo.AlonzoBlockHeader:
		return uint64(h.Body.OpCertSequenceNumber), true
	case *babbage.BabbageBlockHeader:
		return uint64(h.Body.OpCert.SequenceNumber), true
	case *conway.ConwayBlockHeader:
		return uint64(h.Body.OpCert.SequenceNumber), true
	default:
		return 0, false
	}
}

func praosTiebreakerConfigForHeader(
	header ledger.BlockHeader,
) PraosTiebreakerConfig {
	if header.Era().Id >= conway.EraIdConway {
		return PraosTiebreakerConfigConway()
	}
	return PraosTiebreakerConfigBeforeConway()
}

// GetVRFOutput extracts the VRF output from a block header.
// Returns nil if the header type is not supported or doesn't contain VRF data.
//
// The VRF output is used for tie-breaking in chain selection when two chains
// have equal block numbers and equal density. The chain with the LOWER VRF
// output (lexicographically) wins.
func GetVRFOutput(header ledger.BlockHeader) []byte {
	switch h := header.(type) {
	case *shelley.ShelleyBlockHeader:
		return h.Body.LeaderVrf.Output
	case *allegra.AllegraBlockHeader:
		return h.Body.LeaderVrf.Output
	case *mary.MaryBlockHeader:
		return h.Body.LeaderVrf.Output
	case *alonzo.AlonzoBlockHeader:
		return h.Body.LeaderVrf.Output
	case *babbage.BabbageBlockHeader:
		return h.Body.VrfResult.Output
	case *conway.ConwayBlockHeader:
		return h.Body.VrfResult.Output
	default:
		return nil
	}
}

// CompareVRFOutputs compares two VRF outputs for chain selection tie-breaking.
// Returns:
//   - ChainABetter (1) if vrfA is lower (A wins)
//   - ChainBBetter (-1) if vrfB is lower (B wins)
//   - ChainEqual (0) if equal, either is nil, or either has invalid length
//
// Per Ouroboros Praos: the chain with the LOWER VRF output wins the tie-break.
// Both outputs must be exactly VRFOutputSize (64) bytes; outputs of any other
// length are treated the same as nil to prevent truncated values from winning.
func CompareVRFOutputs(vrfA, vrfB []byte) ChainComparisonResult {
	if len(vrfA) != VRFOutputSize || len(vrfB) != VRFOutputSize {
		return ChainEqual
	}
	cmp := bytes.Compare(vrfA, vrfB)
	if cmp < 0 {
		return ChainABetter
	}
	if cmp > 0 {
		return ChainBBetter
	}
	return ChainEqual
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
