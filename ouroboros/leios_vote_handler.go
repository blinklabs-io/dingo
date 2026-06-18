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
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// LeiosVoteHandler is the vote collection/serving surface the Leios
// protocol handlers delegate to. It is implemented by the
// ledger/leios.VoteManager and assigned post-construction from node
// wiring, like the other Ouroboros component dependencies. Handlers
// nil-check it so the protocols stay functional (receive-and-log only)
// when vote management is not wired.
type LeiosVoteHandler interface {
	// HandleVote validates and stores a vote received from a peer.
	HandleVote(connKey string, vote lcommon.LeiosVote) error
	// NextVotes blocks until count votes not originating from connKey
	// are available and returns exactly count votes. done aborts the
	// wait (e.g. protocol shutdown).
	NextVotes(
		done <-chan struct{},
		connKey string,
		count uint64,
	) ([]lcommon.LeiosVote, error)
	// VotesByIds returns raw CBOR for the requested votes, omitting
	// unknown ids.
	VotesByIds(ids []lcommon.LeiosVoteId) []cbor.RawMessage
	// HandleEndorserBlock triggers local vote emission for a stored
	// endorser block.
	HandleEndorserBlock(slot uint64, ebHash lcommon.Blake2b256)
	// RemoveConnection drops per-connection vote serving state.
	RemoveConnection(connKey string)
}
