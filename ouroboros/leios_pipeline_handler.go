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

package ouroboros

import (
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// LeiosPipelineHandler is the Leios pipeline surface the protocol handlers
// notify when an endorser block is received and cached. It is implemented
// by the ledger/leios.PipelineManager and assigned post-construction from
// node wiring, like the other Ouroboros component dependencies. It is kept
// separate from LeiosVoteHandler so vote management and pipeline timing
// stay distinct concerns; handlers nil-check it so the protocols stay
// functional when pipeline management is not wired.
type LeiosPipelineHandler interface {
	// ObserveEndorserBlock registers an endorser block observed for a
	// slot into the pipeline (for stage/timing tracking and EB
	// equivocation detection).
	ObserveEndorserBlock(slot uint64, ebHash lcommon.Blake2b256)
}
