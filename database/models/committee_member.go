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

package models

import "github.com/blinklabs-io/dingo/database/types"

// CommitteeMember represents a Constitutional Committee member imported
// from a Mithril snapshot. This is separate from the certificate-based
// AuthCommitteeHot/ResignCommitteeCold tables, which track committee
// membership changes from on-chain certificates. This table captures
// the committee composition at the time of the snapshot.
type CommitteeMember struct {
	ID                uint
	ColdCredentialTag uint8
	ColdCredHash      []byte // 28-byte credential hash
	ExpiresEpoch      uint64
	TermStartSlot     uint64 // Slot from which credentials apply to this term
	// TermStartSlotSet distinguishes an explicit slot-zero term start from a
	// legacy caller that leaves TermStartSlot unset and expects AddedSlot.
	TermStartSlotSet bool
	AddedSlot        uint64  // Slot when imported/registered
	DeletedSlot      *uint64 // For rollback support
}

// CommitteeCredential preserves a committee credential's key/script tag with
// its hash across storage APIs.
type CommitteeCredential struct {
	CredentialTag uint8
	Credential    []byte
	TermStartSlot uint64
}

// Key returns a collision-free map key for the tagged credential.
func (c CommitteeCredential) Key() string {
	return string([]byte{c.CredentialTag}) + string(c.Credential)
}

// CommitteeQuorum records the quorum threshold enacted with a committee.
type CommitteeQuorum struct {
	Quorum    *types.Rat
	ID        uint
	AddedSlot uint64
}
