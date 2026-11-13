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

package peergov

// Local close reasons (connmanager.ConnectionClosedEvent.LocalCloseReason)
// that demote a peer: the node closed its connection because the peer stopped
// helping Genesis chain selection. Peer governance does not redial a demoted
// peer, and withholds a connection it opens from chain selection, until the
// demotion ends.
const (
	// CloseReasonPatienceExhausted closes a peer that ran out of the Genesis
	// Limit on Patience.
	CloseReasonPatienceExhausted = "genesis_patience_exhausted"
	// CloseReasonEagernessStandoffLost closes a peer that lost a standoff at
	// the Limit on Eagerness.
	CloseReasonEagernessStandoffLost = "eagerness_standoff_lost"
)

// IsChainsyncDemotionReason reports whether reason is one of the local close
// reasons that demote a peer. The reason demotes the peer whatever the
// transport error on the same close.
func IsChainsyncDemotionReason(reason string) bool {
	switch reason {
	case CloseReasonPatienceExhausted, CloseReasonEagernessStandoffLost:
		return true
	default:
		return false
	}
}
