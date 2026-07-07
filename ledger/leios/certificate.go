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
	"errors"
	"fmt"
	"math/big"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
)

// ErrQuorumNotMet is returned when a certificate's signers do not
// represent enough stake to meet the quorum threshold.
var ErrQuorumNotMet = errors.New("stake quorum not met")

// VerifiedVote identifies a committee member whose vote signature has been
// cryptographically verified, for aggregation into a certificate.
type VerifiedVote struct {
	VoterId   uint64
	Signature []byte // 48-byte compressed G1 vote signature
}

// BuildEbCertificate aggregates verified votes for one endorser block into
// a CIP-0164 certificate: a signers bitfield over the committee plus one
// aggregated BLS signature. Only cryptographically verified votes may be
// aggregated -- including an unverified signature would silently produce a
// certificate that fails verification.
//
// NOTE: certificates built here are not yet embedded in forged blocks; this is
// the aggregation path consumed by future forge-loop certificate integration.
func BuildEbCertificate(
	slotNo uint64,
	ebHash lcommon.Blake2b256,
	committee *Committee,
	verifiedVotes []VerifiedVote,
) (*lcommon.LeiosEbCertificate, error) {
	if committee == nil {
		return nil, errors.New("nil committee")
	}
	if len(verifiedVotes) == 0 {
		return nil, errors.New("no verified votes to aggregate")
	}
	committeeSize := committee.Size()
	signers := make(
		[]byte,
		lcommon.LeiosSignerBitfieldSize(committeeSize),
	)
	sigs := make([][]byte, 0, len(verifiedVotes))
	for _, vote := range verifiedVotes {
		if vote.VoterId >= committeeSize {
			return nil, fmt.Errorf(
				"voter id %d out of range for committee size %d",
				vote.VoterId,
				committeeSize,
			)
		}
		// Bits are MSB-first, matching lcommon.LeiosSignerBit
		byteIdx := vote.VoterId / 8
		bitMask := byte(1) << (7 - vote.VoterId%8)
		if signers[byteIdx]&bitMask != 0 {
			return nil, fmt.Errorf(
				"duplicate vote for voter id %d",
				vote.VoterId,
			)
		}
		signers[byteIdx] |= bitMask
		sigs = append(sigs, vote.Signature)
	}
	aggSig, err := AggregateSignatures(sigs)
	if err != nil {
		return nil, fmt.Errorf("aggregate vote signatures: %w", err)
	}
	cert := &lcommon.LeiosEbCertificate{
		SlotNo:              slotNo,
		EndorserBlockHash:   ebHash,
		Signers:             signers,
		AggregatedSignature: aggSig,
	}
	if err := cert.Validate(committeeSize); err != nil {
		return nil, fmt.Errorf("built certificate is invalid: %w", err)
	}
	return cert, nil
}

// ValidateEbCertificate checks a certificate against the committee for its
// epoch: structural validity (bitfield size, unused bits, signature size),
// stake quorum (signer stake >= tau * total active stake), and -- when the
// registry knows every signer's public key -- the aggregated BLS
// signature. sigChecked reports whether the signature was verified; it is
// false when one or more signer public keys are unknown (lenient mode,
// pending CIP-0164 key registration).
func ValidateEbCertificate(
	cert *lcommon.LeiosEbCertificate,
	committee *Committee,
	quorumStakeThreshold *big.Rat,
	registry *VoterRegistry,
) (sigChecked bool, err error) {
	if cert == nil {
		return false, errors.New("nil certificate")
	}
	if committee == nil {
		return false, errors.New("nil committee")
	}
	if err := cert.Validate(committee.Size()); err != nil {
		return false, err
	}
	var signerStake uint64
	signerPubs := make([]*bls12381.G2Affine, 0, len(committee.Members))
	allKeysKnown := true
	for _, member := range committee.Members {
		if !cert.Signer(member.VoterId) {
			continue
		}
		signerStake += member.Stake
		if registry == nil {
			allKeysKnown = false
			continue
		}
		pub, ok := registry.PublicKeyFor(member.PoolKeyHash)
		if !ok {
			allKeysKnown = false
			continue
		}
		signerPubs = append(signerPubs, pub)
	}
	quorumMet, err := MeetsStakeQuorum(
		signerStake,
		committee.TotalActiveStake,
		quorumStakeThreshold,
	)
	if err != nil {
		return false, err
	}
	if !quorumMet {
		return false, fmt.Errorf(
			"%w: signer stake %d of total active stake %d below threshold %s",
			ErrQuorumNotMet,
			signerStake,
			committee.TotalActiveStake,
			quorumStakeThreshold.String(),
		)
	}
	if !allKeysKnown {
		// Lenient mode: quorum validated, but the aggregate signature
		// cannot be checked without every signer's public key.
		return false, nil
	}
	msg := VoteMessageBytes(cert.SlotNo, cert.EndorserBlockHash)
	if err := VerifyAggregateSignature(
		signerPubs,
		msg,
		cert.AggregatedSignature,
	); err != nil {
		return false, err
	}
	return true, nil
}
