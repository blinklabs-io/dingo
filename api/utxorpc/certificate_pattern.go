// Copyright 2025 Blink Labs Software
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

package utxorpc

import (
	"bytes"

	gledger "github.com/blinklabs-io/gouroboros/ledger"
	cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
	"google.golang.org/protobuf/proto"
)

// Certificate pattern matching notes (utxorpc Cardano TxPattern.has_certificate):
//   - Empty nested messages (no populated inner fields) mean “match this
//     certificate type only” (e.g. any stake delegation, any pool retirement).
//   - StakeDelegationPattern: optional stake_credential and pool_keyhash; when
//     both are absent, any StakeDelegationCert matches.
//   - PoolRegistrationPattern: optional operator and pool_keyhash (each compared
//     to the pool cold key on PoolRegistrationCert); when both absent, any
//     pool registration matches.
//   - PoolRetirementPattern: optional pool_keyhash. With no pool key, any
//     retirement matches unless epoch is non-zero (then epoch must match).
//     With a pool key set, epoch 0 means any epoch for that pool.
//   - AnyPoolKeyhash: enumerates operator/pool key hashes from delegation and
//     pool registration/retirement; VRF key hashes are not included.
//   - any_stake_credential / any_pool_keyhash / any_drep: empty []byte remains
//     unevaluable (not a nested message; no wildcard semantics).

// txPatternMatchHasCertificate reports whether any certificate on the
// transaction matches the given utxorpc CertificatePattern.
func (u *Utxorpc) txPatternMatchHasCertificate(
	tx gledger.Transaction,
	pat *cardano.CertificatePattern,
) predOutcome {
	if pat == nil || pat.GetCertificateType() == nil {
		return predUnevaluable
	}
	certs := tx.Certificates()
	if len(certs) == 0 {
		return predNoMatch
	}
	var sawUnevaluable bool
	for _, c := range certs {
		uc, err := c.Utxorpc()
		if err != nil {
			u.config.Logger.Error(
				"failed to convert certificate for tx predicate",
				"error", err,
			)
			sawUnevaluable = true
			continue
		}
		switch certificatePatternMatches(uc, pat) {
		case predMatch:
			return predMatch
		case predUnevaluable:
			sawUnevaluable = true
		case predNoMatch:
		}
	}
	if sawUnevaluable {
		return predUnevaluable
	}
	return predNoMatch
}

func certificatePatternMatches(
	uc *cardano.Certificate,
	pat *cardano.CertificatePattern,
) predOutcome {
	if uc == nil {
		return predNoMatch
	}
	switch pat.GetCertificateType().(type) {
	case *cardano.CertificatePattern_StakeRegistration:
		want := pat.GetStakeRegistration()
		if want == nil {
			return predUnevaluable
		}
		got := uc.GetStakeRegistration()
		if got == nil {
			return predNoMatch
		}
		if !stakeCredentialHasConstraint(want) {
			return predMatch
		}
		if proto.Equal(want, got) {
			return predMatch
		}
		return predNoMatch

	case *cardano.CertificatePattern_StakeDeregistration:
		want := pat.GetStakeDeregistration()
		if want == nil {
			return predUnevaluable
		}
		got := uc.GetStakeDeregistration()
		if got == nil {
			return predNoMatch
		}
		if !stakeCredentialHasConstraint(want) {
			return predMatch
		}
		if proto.Equal(want, got) {
			return predMatch
		}
		return predNoMatch

	case *cardano.CertificatePattern_StakeDelegation:
		sdp := pat.GetStakeDelegation()
		if sdp == nil {
			return predUnevaluable
		}
		got := uc.GetStakeDelegation()
		if got == nil {
			return predNoMatch
		}
		if sc := sdp.GetStakeCredential(); stakeCredentialHasConstraint(sc) {
			if !proto.Equal(sc, got.GetStakeCredential()) {
				return predNoMatch
			}
		}
		if pkh := sdp.GetPoolKeyhash(); len(pkh) > 0 {
			if !bytes.Equal(pkh, got.GetPoolKeyhash()) {
				return predNoMatch
			}
		}
		return predMatch

	case *cardano.CertificatePattern_PoolRegistration:
		prp := pat.GetPoolRegistration()
		if prp == nil {
			return predUnevaluable
		}
		got := uc.GetPoolRegistration()
		if got == nil {
			return predNoMatch
		}
		if op := prp.GetOperator(); len(op) > 0 {
			if !bytes.Equal(op, got.GetOperator()) {
				return predNoMatch
			}
		}
		if pkh := prp.GetPoolKeyhash(); len(pkh) > 0 {
			if !bytes.Equal(pkh, got.GetOperator()) {
				return predNoMatch
			}
		}
		return predMatch

	case *cardano.CertificatePattern_PoolRetirement:
		prt := pat.GetPoolRetirement()
		if prt == nil {
			return predUnevaluable
		}
		got := uc.GetPoolRetirement()
		if got == nil {
			return predNoMatch
		}
		if len(prt.GetPoolKeyhash()) == 0 {
			if wantEp := prt.GetEpoch(); wantEp != 0 && wantEp != got.GetEpoch() {
				return predNoMatch
			}
			return predMatch
		}
		if !bytes.Equal(prt.GetPoolKeyhash(), got.GetPoolKeyhash()) {
			return predNoMatch
		}
		if wantEp := prt.GetEpoch(); wantEp != 0 && wantEp != got.GetEpoch() {
			return predNoMatch
		}
		return predMatch

	case *cardano.CertificatePattern_AnyStakeCredential:
		h := pat.GetAnyStakeCredential()
		if len(h) == 0 {
			return predUnevaluable
		}
		if anyStakeCredentialMatches(h, uc) {
			return predMatch
		}
		return predNoMatch

	case *cardano.CertificatePattern_AnyPoolKeyhash:
		h := pat.GetAnyPoolKeyhash()
		if len(h) == 0 {
			return predUnevaluable
		}
		if anyPoolKeyhashMatches(h, uc) {
			return predMatch
		}
		return predNoMatch

	case *cardano.CertificatePattern_AnyDrep:
		h := pat.GetAnyDrep()
		if len(h) == 0 {
			return predUnevaluable
		}
		if anyDrepMatches(h, uc) {
			return predMatch
		}
		return predNoMatch

	default:
		return predUnevaluable
	}
}

// stakeCredentialHasConstraint reports whether the protobuf carries an actual
// addr key hash or script hash (a non-nil but empty StakeCredential is ignored).
func stakeCredentialHasConstraint(sc *cardano.StakeCredential) bool {
	if sc == nil {
		return false
	}
	return len(sc.GetAddrKeyHash()) > 0 || len(sc.GetScriptHash()) > 0
}

func anyStakeCredentialMatches(want []byte, uc *cardano.Certificate) bool {
	if uc == nil || len(want) == 0 {
		return false
	}
	var found bool
	forEachStakeHashInCert(uc, func(h []byte) {
		if found {
			return
		}
		if bytes.Equal(want, h) {
			found = true
		}
	})
	return found
}

func forEachStakeHashInCert(uc *cardano.Certificate, fn func([]byte)) {
	if uc == nil {
		return
	}
	addSC := func(sc *cardano.StakeCredential) {
		if sc == nil {
			return
		}
		if b := sc.GetAddrKeyHash(); len(b) > 0 {
			fn(b)
		}
		if b := sc.GetScriptHash(); len(b) > 0 {
			fn(b)
		}
	}
	switch {
	case uc.GetStakeRegistration() != nil:
		addSC(uc.GetStakeRegistration())
	case uc.GetStakeDeregistration() != nil:
		addSC(uc.GetStakeDeregistration())
	case uc.GetStakeDelegation() != nil:
		addSC(uc.GetStakeDelegation().GetStakeCredential())
	case uc.GetRegCert() != nil:
		addSC(uc.GetRegCert().GetStakeCredential())
	case uc.GetUnregCert() != nil:
		addSC(uc.GetUnregCert().GetStakeCredential())
	case uc.GetVoteDelegCert() != nil:
		addSC(uc.GetVoteDelegCert().GetStakeCredential())
	case uc.GetStakeVoteDelegCert() != nil:
		addSC(uc.GetStakeVoteDelegCert().GetStakeCredential())
	case uc.GetStakeRegDelegCert() != nil:
		addSC(uc.GetStakeRegDelegCert().GetStakeCredential())
	case uc.GetVoteRegDelegCert() != nil:
		addSC(uc.GetVoteRegDelegCert().GetStakeCredential())
	case uc.GetStakeVoteRegDelegCert() != nil:
		addSC(uc.GetStakeVoteRegDelegCert().GetStakeCredential())
	case uc.GetPoolRegistration() != nil:
		pr := uc.GetPoolRegistration()
		if ra := pr.GetRewardAccount(); len(ra) > 0 {
			fn(ra)
		}
		for _, o := range pr.GetPoolOwners() {
			if len(o) > 0 {
				fn(o)
			}
		}
	case uc.GetAuthCommitteeHotCert() != nil:
		h := uc.GetAuthCommitteeHotCert()
		addSC(h.GetCommitteeColdCredential())
		addSC(h.GetCommitteeHotCredential())
	case uc.GetResignCommitteeColdCert() != nil:
		addSC(uc.GetResignCommitteeColdCert().GetCommitteeColdCredential())
	case uc.GetRegDrepCert() != nil:
		addSC(uc.GetRegDrepCert().GetDrepCredential())
	case uc.GetUnregDrepCert() != nil:
		addSC(uc.GetUnregDrepCert().GetDrepCredential())
	case uc.GetUpdateDrepCert() != nil:
		addSC(uc.GetUpdateDrepCert().GetDrepCredential())
	case uc.GetMirCert() != nil:
		for _, mt := range uc.GetMirCert().GetTo() {
			addSC(mt.GetStakeCredential())
		}
	}
}

func anyPoolKeyhashMatches(want []byte, uc *cardano.Certificate) bool {
	if uc == nil || len(want) == 0 {
		return false
	}
	var found bool
	forEachPoolKeyhashInCert(uc, func(h []byte) {
		if found {
			return
		}
		if bytes.Equal(want, h) {
			found = true
		}
	})
	return found
}

func forEachPoolKeyhashInCert(uc *cardano.Certificate, fn func([]byte)) {
	if uc == nil {
		return
	}
	switch {
	case uc.GetStakeDelegation() != nil:
		if p := uc.GetStakeDelegation().GetPoolKeyhash(); len(p) > 0 {
			fn(p)
		}
	case uc.GetStakeVoteDelegCert() != nil:
		if p := uc.GetStakeVoteDelegCert().GetPoolKeyhash(); len(p) > 0 {
			fn(p)
		}
	case uc.GetStakeRegDelegCert() != nil:
		if p := uc.GetStakeRegDelegCert().GetPoolKeyhash(); len(p) > 0 {
			fn(p)
		}
	case uc.GetStakeVoteRegDelegCert() != nil:
		if p := uc.GetStakeVoteRegDelegCert().GetPoolKeyhash(); len(p) > 0 {
			fn(p)
		}
	case uc.GetPoolRegistration() != nil:
		if op := uc.GetPoolRegistration().GetOperator(); len(op) > 0 {
			fn(op)
		}
	case uc.GetPoolRetirement() != nil:
		if p := uc.GetPoolRetirement().GetPoolKeyhash(); len(p) > 0 {
			fn(p)
		}
	}
}

func drepBytesFromDRep(d *cardano.DRep) [][]byte {
	if d == nil {
		return nil
	}
	var out [][]byte
	if b := d.GetAddrKeyHash(); len(b) > 0 {
		out = append(out, b)
	}
	if b := d.GetScriptHash(); len(b) > 0 {
		out = append(out, b)
	}
	return out
}

func anyDrepMatches(want []byte, uc *cardano.Certificate) bool {
	if uc == nil || len(want) == 0 {
		return false
	}
	var found bool
	forEachDrepHashInCert(uc, func(h []byte) {
		if found {
			return
		}
		if bytes.Equal(want, h) {
			found = true
		}
	})
	return found
}

func forEachDrepHashInCert(uc *cardano.Certificate, fn func([]byte)) {
	if uc == nil {
		return
	}
	addSC := func(sc *cardano.StakeCredential) {
		if sc == nil {
			return
		}
		if b := sc.GetAddrKeyHash(); len(b) > 0 {
			fn(b)
		}
		if b := sc.GetScriptHash(); len(b) > 0 {
			fn(b)
		}
	}
	switch {
	case uc.GetVoteDelegCert() != nil:
		for _, b := range drepBytesFromDRep(uc.GetVoteDelegCert().GetDrep()) {
			fn(b)
		}
	case uc.GetStakeVoteDelegCert() != nil:
		for _, b := range drepBytesFromDRep(uc.GetStakeVoteDelegCert().GetDrep()) {
			fn(b)
		}
	case uc.GetVoteRegDelegCert() != nil:
		for _, b := range drepBytesFromDRep(uc.GetVoteRegDelegCert().GetDrep()) {
			fn(b)
		}
	case uc.GetStakeVoteRegDelegCert() != nil:
		for _, b := range drepBytesFromDRep(uc.GetStakeVoteRegDelegCert().GetDrep()) {
			fn(b)
		}
	case uc.GetRegDrepCert() != nil:
		addSC(uc.GetRegDrepCert().GetDrepCredential())
	case uc.GetUnregDrepCert() != nil:
		addSC(uc.GetUnregDrepCert().GetDrepCredential())
	case uc.GetUpdateDrepCert() != nil:
		addSC(uc.GetUpdateDrepCert().GetDrepCredential())
	}
}
