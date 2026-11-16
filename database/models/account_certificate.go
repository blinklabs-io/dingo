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

import (
	"errors"
	"fmt"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// AccountDelegation is the certificate-driven part of an account: whether it
// is registered and what it delegates to.
type AccountDelegation struct {
	Pool     []byte
	Drep     []byte
	DrepType uint64
	Active   bool
}

// AccountCertificateDrep returns the DRep credential and type a certificate
// names; the AlwaysAbstain and AlwaysNoConfidence types have no credential.
func AccountCertificateDrep(drep lcommon.Drep) ([]byte, uint64, error) {
	drepType, err := DrepTypeFromInt(drep.Type)
	if err != nil {
		return nil, 0, err
	}
	if drepType == DrepTypeAlwaysAbstain ||
		drepType == DrepTypeAlwaysNoConfidence {
		return nil, drepType, nil
	}
	return drep.Credential[:], drepType, nil
}

// AccountCertificateEffect returns the stake credential an account
// certificate is for and the delegation it states: Active false for a
// deregistration, and a Pool or DRep only when it names one. ok is false for
// a certificate that is not an account certificate.
func AccountCertificateEffect(
	certificate lcommon.Certificate,
) (cred lcommon.Credential, effect AccountDelegation, ok bool, err error) {
	effect.Active = true
	var drep *lcommon.Drep
	switch cert := certificate.(type) {
	case *lcommon.StakeRegistrationCertificate:
		cred = cert.StakeCredential
	case *lcommon.RegistrationCertificate:
		cred = cert.StakeCredential
	case *lcommon.StakeDeregistrationCertificate:
		cred = cert.StakeCredential
		effect.Active = false
	case *lcommon.DeregistrationCertificate:
		cred = cert.StakeCredential
		effect.Active = false
	case *lcommon.StakeDelegationCertificate:
		if cert.StakeCredential == nil {
			return cred, effect, true, errors.New(
				"stake delegation certificate has nil stake credential",
			)
		}
		cred = *cert.StakeCredential
		effect.Pool = cert.PoolKeyHash[:]
	case *lcommon.StakeRegistrationDelegationCertificate:
		cred = cert.StakeCredential
		effect.Pool = cert.PoolKeyHash[:]
	case *lcommon.StakeVoteDelegationCertificate:
		cred = cert.StakeCredential
		effect.Pool = cert.PoolKeyHash[:]
		drep = &cert.Drep
	case *lcommon.StakeVoteRegistrationDelegationCertificate:
		cred = cert.StakeCredential
		effect.Pool = cert.PoolKeyHash[:]
		drep = &cert.Drep
	case *lcommon.VoteRegistrationDelegationCertificate:
		cred = cert.StakeCredential
		drep = &cert.Drep
	case *lcommon.VoteDelegationCertificate:
		cred = cert.StakeCredential
		drep = &cert.Drep
	default:
		return cred, effect, false, nil
	}
	if drep != nil {
		if effect.Drep, effect.DrepType, err = AccountCertificateDrep(*drep); err != nil {
			return cred, effect, true, fmt.Errorf("account certificate DRep: %w", err)
		}
	}
	return cred, effect, true, nil
}

// ApplyAccountCertificate returns an account's delegation after a
// certificate whose effect is next, given its delegation before, prev, nil
// for an account that does not exist. Registering a new or inactive
// credential starts with no delegations, a deregistration clears them, and
// a pool or DRep the certificate names replaces the one held.
func ApplyAccountCertificate(
	prev *AccountDelegation,
	next AccountDelegation,
) AccountDelegation {
	var out AccountDelegation
	if prev != nil && prev.Active {
		out = AccountDelegation{
			Pool: prev.Pool, Drep: prev.Drep, DrepType: prev.DrepType,
		}
	}
	out.Active = next.Active
	if !next.Active {
		out.Pool, out.Drep, out.DrepType = nil, nil, 0
	}
	if next.Pool != nil {
		out.Pool = next.Pool
	}
	if next.Drep != nil || next.DrepType >= DrepTypeAlwaysAbstain {
		out.Drep, out.DrepType = next.Drep, next.DrepType
	}
	return out
}

// ShouldRemovePriorDrepDelegation reports whether a vote-delegation change
// closes the account's link in its prior DRep's delegator set. Only a link
// to a credential DRep exists to close. From protocol version 10 every such
// change closes it; before, only a change to a pseudo DRep or to none does,
// so re-delegating to another DRep leaves the old link open, as the
// reference ledger's version 9 reverse set does.
func ShouldRemovePriorDrepDelegation(
	protocolMajor uint64,
	oldDrepType uint64,
	hasOldCredential bool,
	newDrepType uint64,
	hasNewCredential bool,
) bool {
	if !hasOldCredential || oldDrepType > DrepTypeScriptHash {
		return false
	}
	if protocolMajor >= 10 {
		return true
	}
	return newDrepType > DrepTypeScriptHash || !hasNewCredential
}

// DrepCertificateKind is what a DRep certificate does.
type DrepCertificateKind uint8

const (
	DrepCertificateRegistration DrepCertificateKind = iota + 1
	DrepCertificateDeregistration
	DrepCertificateUpdate
)

// DrepCertificateEffect returns the DRep credential a DRep certificate is
// for, its kind and the anchor it states, empty for a deregistration. ok is
// false for a certificate that is not a DRep certificate.
func DrepCertificateEffect(certificate lcommon.Certificate) (
	cred lcommon.Credential,
	kind DrepCertificateKind,
	anchorURL string,
	anchorHash []byte,
	ok bool,
) {
	var anchor *lcommon.GovAnchor
	switch cert := certificate.(type) {
	case *lcommon.RegistrationDrepCertificate:
		cred, kind, anchor = cert.DrepCredential, DrepCertificateRegistration, cert.Anchor
	case *lcommon.DeregistrationDrepCertificate:
		cred, kind = cert.DrepCredential, DrepCertificateDeregistration
	case *lcommon.UpdateDrepCertificate:
		cred, kind, anchor = cert.DrepCredential, DrepCertificateUpdate, cert.Anchor
	default:
		return cred, 0, "", nil, false
	}
	if anchor != nil {
		anchorURL = anchor.Url
		anchorHash = anchor.DataHash[:]
	}
	return cred, kind, anchorURL, anchorHash, true
}

// ChangesDrepDelegation reports whether an account certificate rewrites the
// account's DRep delegation, so its prior DRep link may close.
func ChangesDrepDelegation(certificate lcommon.Certificate) bool {
	switch certificate.(type) {
	case *lcommon.StakeDeregistrationCertificate,
		*lcommon.DeregistrationCertificate,
		*lcommon.StakeVoteDelegationCertificate,
		*lcommon.StakeVoteRegistrationDelegationCertificate,
		*lcommon.VoteRegistrationDelegationCertificate,
		*lcommon.VoteDelegationCertificate:
		return true
	}
	return false
}
