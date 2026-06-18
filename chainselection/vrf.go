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

package chainselection

// This file re-exports the pure Praos VRF and header-view primitives from
// consensus/praos so that chainselection internals and existing call sites
// continue to compile without change. New code should import
// consensus/praos directly.

import (
	"github.com/blinklabs-io/dingo/consensus/praos"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

// VRFOutputSize is re-exported from consensus/praos.
const VRFOutputSize = praos.VRFOutputSize

// PraosTiebreakerFlavor and its constants are type-aliased from
// consensus/praos so that chainselection.PraosTiebreakerFlavor and
// praos.PraosTiebreakerFlavor are the same type.
type PraosTiebreakerFlavor = praos.PraosTiebreakerFlavor

const (
	PraosTiebreakerUnknown      = praos.PraosTiebreakerUnknown
	PraosTiebreakerUnrestricted = praos.PraosTiebreakerUnrestricted
	PraosTiebreakerRestricted   = praos.PraosTiebreakerRestricted
)

// PraosTiebreakerConfig is aliased from consensus/praos.
type PraosTiebreakerConfig = praos.PraosTiebreakerConfig

// PraosTiebreakerView is aliased from consensus/praos.
type PraosTiebreakerView = praos.PraosTiebreakerView

// PraosTiebreakerConfigBeforeConway delegates to consensus/praos.
func PraosTiebreakerConfigBeforeConway() PraosTiebreakerConfig {
	return praos.PraosTiebreakerConfigBeforeConway()
}

// PraosTiebreakerConfigConway delegates to consensus/praos.
func PraosTiebreakerConfigConway() PraosTiebreakerConfig {
	return praos.PraosTiebreakerConfigConway()
}

// PraosTiebreakerConfigUnknown delegates to consensus/praos.
func PraosTiebreakerConfigUnknown() PraosTiebreakerConfig {
	return praos.PraosTiebreakerConfigUnknown()
}

// PraosTiebreakerViewFromTip delegates to consensus/praos.
func PraosTiebreakerViewFromTip(
	tip ochainsync.Tip,
	vrfOutput []byte,
	config PraosTiebreakerConfig,
) PraosTiebreakerView {
	return praos.PraosTiebreakerViewFromTip(tip, vrfOutput, config)
}

// NewPraosTiebreakerViewFull delegates to consensus/praos.
func NewPraosTiebreakerViewFull(
	tip ochainsync.Tip,
	issuer []byte,
	issueNo uint64,
	vrfOutput []byte,
	config PraosTiebreakerConfig,
) PraosTiebreakerView {
	return praos.NewPraosTiebreakerViewFull(tip, issuer, issueNo, vrfOutput, config)
}

// GetPraosTiebreakerView delegates to consensus/praos.
func GetPraosTiebreakerView(
	header gledger.BlockHeader,
) (PraosTiebreakerView, bool) {
	return praos.GetPraosTiebreakerView(header)
}

// GetVRFOutput delegates to consensus/praos.
func GetVRFOutput(header gledger.BlockHeader) []byte {
	return praos.GetVRFOutput(header)
}

// CompareVRFOutputs delegates to consensus/praos.
func CompareVRFOutputs(vrfA, vrfB []byte) ChainComparisonResult {
	return praos.CompareVRFOutputs(vrfA, vrfB)
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func clonePraosTiebreakerView(
	src PraosTiebreakerView,
) PraosTiebreakerView {
	src.Issuer = cloneBytes(src.Issuer)
	src.TieBreakVRF = cloneBytes(src.TieBreakVRF)
	return src
}
