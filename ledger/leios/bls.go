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
	"runtime"
	"sync"
	"sync/atomic"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
)

const (
	LeiosVoteDST = "BLS_SIG_BLS12381G1_XMD:SHA-256_SSWU_RO_POP_"
	LeiosPoPDST  = "BLS_POP_BLS12381G1_XMD:SHA-256_SSWU_RO_POP_"
)

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

// SignLeiosKeyProofOfPossession signs a voting key's serialized public key
// with the Dijkstra Leios proof-of-possession domain.
func SignLeiosKeyProofOfPossession(key *VoteSigningKey) ([]byte, error) {
	if key == nil {
		return nil, errors.New("nil vote signing key")
	}
	return signWithDST(key, key.PublicKeyBytes(), LeiosPoPDST)
}

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
	return verifySignatureWithDST(pub, msg, sig, LeiosVoteDST)
}

func verifySignatureWithDST(
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

// VerifyLeiosKeyProofOfPossession verifies a registered Dijkstra pool's
// proof over its serialized public key with the dedicated BLS PoP domain.
// gouroboros decodes LeiosKey and
// checks only field lengths (LeiosKey.validate), not the proof itself --
// callers must not treat an on-chain leios_key as usable until this passes.
func VerifyLeiosKeyProofOfPossession(key *lcommon.LeiosKey) error {
	return VerifyLeiosKeyProofForDomain(key, lcommon.DijkstraBlsProofDomainPop)
}

// VerifyLeiosKeyProofForDomain verifies key's proof under exactly the given
// domain; the other domain is never tried.
func VerifyLeiosKeyProofForDomain(
	key *lcommon.LeiosKey,
	domain lcommon.DijkstraBlsProofDomain,
) error {
	_, err := verifiedLeiosKey(key, domain)
	return err
}

func keyProofDST(domain lcommon.DijkstraBlsProofDomain) (string, error) {
	switch domain {
	case lcommon.DijkstraBlsProofDomainPop:
		return LeiosPoPDST, nil
	case lcommon.DijkstraBlsProofDomainLegacySig:
		return LeiosVoteDST, nil
	default:
		return "", fmt.Errorf(
			"unsupported Dijkstra BLS proof domain %d",
			domain,
		)
	}
}

// verifiedLeiosKeyCacheMax bounds verifiedLeiosKeys. A committee holds 900
// seats and its keys change only with pool registrations, so the cache
// holds several committees' keys; filling it empties it.
const verifiedLeiosKeyCacheMax = 8192

// verifiedLeiosKeys maps a registered key's public key and proof of
// possession, as bytes, to the decoded public key, for keys whose proof has
// verified. Each check is a G2 decompression with a subgroup check, a hash
// to G1 and a pairing, about 10ms, and every epoch's committee resolves
// every key again. The outcome is a function of the bytes it is keyed on, so
// an entry is never stale. Failures are not cached.
var verifiedLeiosKeys = struct {
	sync.RWMutex
	keys map[verifiedLeiosKeyID]*bls12381.G2Affine
}{keys: make(map[verifiedLeiosKeyID]*bls12381.G2Affine)}

type verifiedLeiosKeyID struct {
	domain    lcommon.DijkstraBlsProofDomain
	publicKey [VotePublicKeySize]byte
	proof     [lcommon.LeiosBlsPossessionProofSize]byte
}

// verifiedLeiosKey returns key's decoded public key once its proof of
// possession verifies under domain. The returned point is shared and must not
// be modified. The cache is keyed by domain: a proof verified under one domain
// must not satisfy a lookup under the other.
func verifiedLeiosKey(
	key *lcommon.LeiosKey,
	domain lcommon.DijkstraBlsProofDomain,
) (*bls12381.G2Affine, error) {
	if key == nil {
		return nil, errors.New("nil leios key")
	}
	dst, err := keyProofDST(domain)
	if err != nil {
		return nil, err
	}
	if len(key.PublicKey) != VotePublicKeySize {
		return nil, fmt.Errorf(
			"leios key public key must be %d bytes, got %d",
			VotePublicKeySize,
			len(key.PublicKey),
		)
	}
	cacheable := len(key.PossessionProof) == lcommon.LeiosBlsPossessionProofSize
	id := verifiedLeiosKeyID{domain: domain}
	if cacheable {
		copy(id.publicKey[:], key.PublicKey)
		copy(id.proof[:], key.PossessionProof)
		verifiedLeiosKeys.RLock()
		pub, ok := verifiedLeiosKeys.keys[id]
		verifiedLeiosKeys.RUnlock()
		if ok {
			return pub, nil
		}
	}
	var pub bls12381.G2Affine
	// SetBytes validates curve membership and subgroup order, matching
	// ParseVoterPublicKey's checks.
	if _, err := pub.SetBytes(key.PublicKey); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidPublicKey, err)
	}
	if pub.IsInfinity() {
		return nil, fmt.Errorf("%w: point is infinity", ErrInvalidPublicKey)
	}
	if err := verifySignatureWithDST(
		&pub,
		key.PublicKey,
		key.PossessionProof,
		dst,
	); err != nil {
		return nil, fmt.Errorf("leios key proof of possession: %w", err)
	}
	if cacheable {
		verifiedLeiosKeys.Lock()
		if len(verifiedLeiosKeys.keys) >= verifiedLeiosKeyCacheMax {
			verifiedLeiosKeys.keys = make(
				map[verifiedLeiosKeyID]*bls12381.G2Affine,
			)
		}
		verifiedLeiosKeys.keys[id] = &pub
		verifiedLeiosKeys.Unlock()
	}
	return &pub, nil
}

// AggregateSignatures sums the given 48-byte compressed G1 signatures into
// one aggregate signature.
func AggregateSignatures(sigs [][]byte) ([]byte, error) {
	if len(sigs) == 0 {
		return nil, errors.New("no signatures to aggregate")
	}
	// Decoding a signature decompresses a G1 point and checks its subgroup,
	// which for a 900-seat committee dominates aggregation, so it runs in
	// parallel. The sum is taken in Jacobian coordinates: an affine addition
	// inverts a field element.
	points, err := decodeSignaturePoints(sigs)
	if err != nil {
		return nil, err
	}
	var agg bls12381.G1Jac
	agg.FromAffine(points[0])
	for _, point := range points[1:] {
		agg.AddMixed(point)
	}
	var aggAffine bls12381.G1Affine
	aggAffine.FromJacobian(&agg)
	aggBytes := aggAffine.Bytes()
	return aggBytes[:], nil
}

// decodeSignaturePoints decodes sigs on up to GOMAXPROCS goroutines and
// returns the first failure by index.
func decodeSignaturePoints(sigs [][]byte) ([]*bls12381.G1Affine, error) {
	points := make([]*bls12381.G1Affine, len(sigs))
	errs := make([]error, len(sigs))
	parallelFor(len(sigs), func(i int) {
		points[i], errs[i] = decodeSignaturePoint(sigs[i])
	})
	for i, err := range errs {
		if err != nil {
			return nil, fmt.Errorf("signature %d: %w", i, err)
		}
	}
	return points, nil
}

// parallelFor calls fn(i) for every i in [0, n) on up to GOMAXPROCS
// goroutines and returns when all calls have.
func parallelFor(n int, fn func(int)) {
	workers := min(runtime.GOMAXPROCS(0), n)
	if workers <= 1 {
		for i := range n {
			fn(i)
		}
		return
	}
	var next atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for {
				i := int(next.Add(1) - 1)
				if i >= n {
					return
				}
				fn(i)
			}
		})
	}
	wg.Wait()
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
	// Summed in Jacobian coordinates: an affine addition inverts a field
	// element, which for a large committee costs more than the pairing.
	var aggJac bls12381.G2Jac
	for i, pub := range pubs {
		if pub == nil || pub.IsInfinity() {
			return fmt.Errorf("invalid public key %d", i)
		}
		if i == 0 {
			aggJac.FromAffine(pub)
			continue
		}
		aggJac.AddMixed(pub)
	}
	var aggPub bls12381.G2Affine
	aggPub.FromJacobian(&aggJac)
	return VerifyVoteSignature(&aggPub, msg, aggSig)
}
