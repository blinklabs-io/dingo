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

package leios

import (
	"github.com/blinklabs-io/dingo/event"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// EbQuorumEventType is emitted once per endorser block when collected
// verified votes meet the stake quorum and a certificate has been built.
// This is the hook for future forge-loop integration that embeds certificates
// into Dijkstra ranking block bodies.
const EbQuorumEventType event.EventType = "leios.eb_quorum"

// EbQuorumEvent carries the certificate built when an endorser block
// reached stake quorum.
type EbQuorumEvent struct {
	SlotNo            uint64
	EndorserBlockHash lcommon.Blake2b256
	Epoch             uint64
	Certificate       *lcommon.LeiosEbCertificate
	VerifiedStake     uint64 // stake of signature-verified votes
	ObservedStake     uint64 // stake of all membership-valid votes
	TotalActiveStake  uint64 // quorum denominator
}
