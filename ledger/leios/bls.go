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

// LeiosVoteDST is the hash_to_point ciphersuite used by CoreSign/CoreVerify
// for ordinary vote signatures, matching cardano-crypto-leios' minSigPoPDST.
// Its "_POP_" suffix names the MinSig ciphersuite *variant* (the one that
// supports a proof-of-possession scheme) -- it does not mean this DST is
// shared with PopProve/PopVerify. See leiosPopDST.
const LeiosVoteDST = "BLS_SIG_BLS12381G1_XMD:SHA-256_SSWU_RO_POP_"

// leiosPopDST is the hash_pubkey_to_point ciphersuite for PopProve/
// PopVerify. Per the IETF BLS-signature draft (section 4.1), this MUST
// differ from the CoreSign DST (LeiosVoteDST) used for ordinary signing --
// derived from the same ciphersuite ID with the "BLS_POP_" prefix in place
// of "BLS_SIG_". Reusing LeiosVoteDST here was an earlier bug in this file:
// it verified a possession proof under the signing DST, which a
// spec-correct proof from a real pool would fail, silently turning every
// correctly-registered pool into a keyless seat.
const leiosPopDST = "BLS_POP_BLS12381G1_XMD:SHA-256_SSWU_RO_POP_"

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

// prototypeRbHashCborHeader is the CBOR byte-string header for a 32-byte
// payload: major type 2 with a one-byte length (0x58) followed by the length
// itself (0x20). Equivalent to cbor.Encode of the hash, but the length is
// fixed here, so the header is a constant and the encoding cannot fail.
const prototypeRbHashCborHeader = "\x58\x20"

// PrototypeVoteMessageBytes returns the current prototype's signed message:
// the hash of the ranking block that announced the endorser block, encoded
// as a CBOR byte string.
//
// The reference signs the RbHash SignableRepresentation, which is
// toStrictByteString (encodeRbHash h) == CBOR.encodeBytes of the hash, so
// the signed preimage is the 34-byte CBOR encoding rather than the bare
// 32 hash bytes. Signing the bare hash hashes a different preimage to the
// curve and every pairing check fails, even with a correct key.
func PrototypeVoteMessageBytes(announcingRbHash lcommon.Blake2b256) []byte {
	msg := make(
		[]byte,
		0,
		len(prototypeRbHashCborHeader)+lcommon.Blake2b256Size,
	)
	msg = append(msg, prototypeRbHashCborHeader...)
	return append(msg, announcingRbHash.Bytes()...)
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
	return signWithDST(key, msg, LeiosVoteDST)
}

// signWithDST signs msg under the given domain-separation tag. Shared by
// SignVote (LeiosVoteDST) and, in tests, PoP construction (leiosPopDST) --
// production code never needs to construct a proof of possession, only
// verify one, since a pool's PoP is generated once by its own operator
// tooling, not by dingo.
func signWithDST(
	key *VoteSigningKey,
	msg []byte,
	dst string,
) ([]byte, error) {
	if key == nil {
		return nil, errors.New("nil vote signing key")
	}
	hashPoint, err := bls12381.HashToG1(msg, []byte(dst))
	if err != nil {
		return nil, fmt.Errorf("hash message to G1: %w", err)
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
// against a G2 public key, under LeiosVoteDST.
func VerifyVoteSignature(
	pub *bls12381.G2Affine,
	msg []byte,
	sig []byte,
) error {
	return verifyWithDST(pub, msg, sig, LeiosVoteDST)
}

// verifyWithDST verifies a 48-byte compressed G1 signature over msg against
// a G2 public key, under the given domain-separation tag. Shared by
// VerifyVoteSignature (LeiosVoteDST) and VerifyLeiosKeyProofOfPossession
// (leiosPopDST) -- the two must never share a call site with the same DST
// hardcoded, or a proof of possession and a vote signature become the same
// primitive over the same domain, which is exactly the bug this was split
// out to fix.
func verifyWithDST(
	pub *bls12381.G2Affine,
	msg []byte,
	sig []byte,
	dst string,
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
	hashPoint, err := bls12381.HashToG1(msg, []byte(dst))
	if err != nil {
		return fmt.Errorf("hash message to G1: %w", err)
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

// VerifyLeiosKeyProofOfPossession verifies that a registered Dijkstra pool
// Leios key's possession proof is a valid PopProve/PopVerify signature,
// under leiosPopDST (distinct from LeiosVoteDST -- see that constant),
// over the key's own serialized public key. gouroboros decodes LeiosKey and
// checks only field lengths (LeiosKey.validate), not the proof itself --
// callers must not treat an on-chain leios_key as usable until this passes.
//
// NOT YET CHECKED AGAINST A REAL INTEROP VECTOR: the DST direction (this
// function's core correctness property) is now right per the IETF draft,
// but no test here has verified a real key/proof pair produced by
// cardano-crypto-leios -- every test in this package both signs and
// verifies with the same code, so a shared, still-wrong assumption
// (message encoding, HashToG1 parameters) would pass here undetected. Get
// one real vector before relying on this against a live network.
func VerifyLeiosKeyProofOfPossession(key *lcommon.LeiosKey) error {
	if key == nil {
		return errors.New("nil leios key")
	}
	if len(key.PublicKey) != VotePublicKeySize {
		return fmt.Errorf(
			"leios key public key must be %d bytes, got %d",
			VotePublicKeySize,
			len(key.PublicKey),
		)
	}
	var pub bls12381.G2Affine
	// SetBytes validates curve membership and subgroup order, matching
	// ParseVoterPublicKey's checks.
	if _, err := pub.SetBytes(key.PublicKey); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidPublicKey, err)
	}
	if pub.IsInfinity() {
		return fmt.Errorf("%w: point is infinity", ErrInvalidPublicKey)
	}
	if err := verifyWithDST(
		&pub,
		key.PublicKey,
		key.PossessionProof,
		leiosPopDST,
	); err != nil {
		return fmt.Errorf("leios key proof of possession: %w", err)
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
