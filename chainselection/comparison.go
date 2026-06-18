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

// This file re-exports pure Praos comparison primitives from consensus/praos.
// New code should import consensus/praos directly.

import (
	"github.com/blinklabs-io/dingo/consensus/praos"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

// ChainComparisonResult is aliased from consensus/praos.
type ChainComparisonResult = praos.ChainComparisonResult

const (
	ChainEqual             = praos.ChainEqual
	ChainABetter           = praos.ChainABetter
	ChainBBetter           = praos.ChainBBetter
	ChainComparisonUnknown = praos.ChainComparisonUnknown
)

// ComparePraosTips delegates to consensus/praos.
func ComparePraosTips(
	tipA, tipB ochainsync.Tip,
	viewA, viewB PraosTiebreakerView,
) ChainComparisonResult {
	return praos.ComparePraosTips(tipA, tipB, viewA, viewB)
}

// PreferPraosCandidate delegates to consensus/praos.
func PreferPraosCandidate(
	ours, cand PraosTiebreakerView,
) bool {
	return praos.PreferPraosCandidate(ours, cand)
}
