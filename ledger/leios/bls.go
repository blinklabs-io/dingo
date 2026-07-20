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
	"encoding/binary"
	"errors"
	"fmt"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
)

// LeiosVoteDST is the proof-of-possession ciphersuite used by
// cardano-crypto-leios' minSigPoPDST.
const LeiosVoteDST = "BLS_SIG_BLS12381G1_XMD:SHA-256_SSWU_RO_POP_"

// Vote signatures follow the BLS MinSig variant on BLS12-381: signatures
// are compressed G1 points (48 bytes, matching gouroboros
// lcommon.LeiosBlsSignatureSize) and public keys are compressed G2 points
// (96 bytes).
const VotePublicKeySize = 96

// ErrInvalidSignature is returned when a BLS signature fails verification.
var ErrInvalidSignature = errors.New("invalid BLS signature")

// negG2Gen is the negated G2 generator, precomputed for pairing checks:
// e(sig, G2gen) == e(H(msg), pk) iff
// e(sig, -G2gen) * e(H(msg), pk) == 1.
var negG2Gen = func() bls12381.G2Affine {
	_, _, _, g2Gen := bls12381.Generators()
	var neg bls12381.G2Affine
	neg.Neg(&g2Gen)
	return neg
}()

// PrototypeVoteMessageBytes returns the current prototype's signed message:
// the hash of the ranking block that announced the endorser block.
func PrototypeVoteMessageBytes(announcingRbHash lcommon.Blake2b256) []byte {
	return announcingRbHash.Bytes()
}

// VoteMessageBytes retains the legacy standalone leios-votes message shape.
func VoteMessageBytes(slotNo uint64, ebHash lcommon.Blake2b256) []byte {
	msg := make([]byte, 8, 8+len(ebHash))
	binary.BigEndian.PutUint64(msg, slotNo)
	return append(msg, ebHash.Bytes()...)
}

// SignVote signs a vote message with the MinSig scheme and returns the
// 48-byte compressed G1 signature.
func SignVote(key *VoteSigningKey, msg []byte) ([]byte, error) {
	if key == nil {
		return nil, errors.New("nil vote signing key")
	}
	hashPoint, err := bls12381.HashToG1(msg, []byte(LeiosVoteDST))
	if err != nil {
		return nil, fmt.Errorf("hash vote message to G1: %w", err)
	}
	var sig bls12381.G1Affine
	sig.ScalarMultiplication(&hashPoint, key.sk)
	sigBytes := sig.Bytes()
	return sigBytes[:], nil
}

// decodeSignaturePoint decodes a 48-byte compressed G1 signature,
// rejecting points not in the subgroup and the point at infinity.
// SetBytes validates curve membership and subgroup order.
func decodeSignaturePoint(sig []byte) (*bls12381.G1Affine, error) {
	if len(sig) != lcommon.LeiosBlsSignatureSize {
		return nil, fmt.Errorf(
			"signature must be %d bytes, got %d",
			lcommon.LeiosBlsSignatureSize,
			len(sig),
		)
	}
	var point bls12381.G1Affine
	if _, err := point.SetBytes(sig); err != nil {
		return nil, fmt.Errorf("decoding signature point: %w", err)
	}
	if point.IsInfinity() {
		return nil, errors.New("signature point is infinity")
	}
	return &point, nil
}

// VerifyVoteSignature verifies a 48-byte compressed G1 signature over msg
// against a G2 public key.
func VerifyVoteSignature(
	pub *bls12381.G2Affine,
	msg []byte,
	sig []byte,
) error {
	if pub == nil || pub.IsInfinity() {
		return errors.New("invalid public key")
	}
	// Defense in depth for callers outside this package: keys parsed by
	// ParseVoterPublicKey are already subgroup-checked at decode time.
	if !pub.IsInSubGroup() {
		return errors.New("public key is not in the G2 subgroup")
	}
	sigPoint, err := decodeSignaturePoint(sig)
	if err != nil {
		return err
	}
	hashPoint, err := bls12381.HashToG1(msg, []byte(LeiosVoteDST))
	if err != nil {
		return fmt.Errorf("hash vote message to G1: %w", err)
	}
	ok, err := bls12381.PairingCheck(
		[]bls12381.G1Affine{*sigPoint, hashPoint},
		[]bls12381.G2Affine{negG2Gen, *pub},
	)
	if err != nil {
		return fmt.Errorf("pairing check: %w", err)
	}
	if !ok {
		return ErrInvalidSignature
	}
	return nil
}

// AggregateSignatures sums the given 48-byte compressed G1 signatures into
// one aggregate signature.
func AggregateSignatures(sigs [][]byte) ([]byte, error) {
	if len(sigs) == 0 {
		return nil, errors.New("no signatures to aggregate")
	}
	var agg bls12381.G1Affine
	for i, sig := range sigs {
		point, err := decodeSignaturePoint(sig)
		if err != nil {
			return nil, fmt.Errorf("signature %d: %w", i, err)
		}
		if i == 0 {
			agg = *point
			continue
		}
		agg.Add(&agg, point)
	}
	aggBytes := agg.Bytes()
	return aggBytes[:], nil
}

// VerifyAggregateSignature verifies an aggregate signature over a single
// shared message against the sum of the signers' public keys. All Leios
// votes for the same endorser block sign the same message, so aggregate
// verification reduces to one pairing check.
//
// SECURITY: summing public keys over a shared message is forgeable via
// rogue-key attacks (an attacker choosing pk_evil = g2^x - sum(pk_honest)
// can forge the aggregate alone) unless every public key carries a
// verified proof of possession. Callers must only pass keys whose
// possession is established; today that holds because keys come
// exclusively from the operator-configured VoterRegistry.
func VerifyAggregateSignature(
	pubs []*bls12381.G2Affine,
	msg []byte,
	aggSig []byte,
) error {
	if len(pubs) == 0 {
		return errors.New("no public keys to verify against")
	}
	var aggPub bls12381.G2Affine
	for i, pub := range pubs {
		if pub == nil || pub.IsInfinity() {
			return fmt.Errorf("invalid public key %d", i)
		}
		if i == 0 {
			aggPub = *pub
			continue
		}
		aggPub.Add(&aggPub, pub)
	}
	return VerifyVoteSignature(&aggPub, msg, aggSig)
}
