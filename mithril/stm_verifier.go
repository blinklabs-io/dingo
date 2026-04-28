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

package mithril

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/bits"

	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"golang.org/x/crypto/blake2b"
)

// stmBLSDomainSeparationTag is intentionally empty to match the Mithril
// reference implementation's BLS hash-to-curve domain separation tag.
var stmBLSDomainSeparationTag = []byte{}

type stmAggregateVerificationKey struct {
	MTCommitment stmMerkleTreeBatchCommitment `json:"mt_commitment"`
	TotalStake   uint64                       `json:"total_stake"`
}

type stmMerkleTreeBatchCommitment struct {
	Root     []byte `json:"root"`
	NrLeaves int    `json:"nr_leaves"`
}

type stmAggregateSignature struct {
	Signatures []stmSingleSignatureWithRegisteredParty `json:"signatures"`
	BatchProof stmMerkleBatchPath                      `json:"batch_proof"`
}

type stmSingleSignatureWithRegisteredParty struct {
	Sig      stmSingleSignature
	RegParty stmClosedRegistrationEntry
}

type stmSingleSignature struct {
	Sigma       []byte   `json:"sigma"`
	Indexes     []uint64 `json:"indexes"`
	SignerIndex uint64   `json:"signer_index"`
}

type stmClosedRegistrationEntry struct {
	VerificationKey []byte
	Stake           uint64
}

type stmMerkleBatchPath struct {
	Values  [][]byte `json:"values"`
	Indices []int    `json:"indices"`
}

type stmSignerVerificationKey struct {
	VK  []byte `json:"vk"`
	Pop []byte `json:"pop"`
}

func (e *stmClosedRegistrationEntry) UnmarshalJSON(data []byte) error {
	var tuple []json.RawMessage
	if err := json.Unmarshal(data, &tuple); err != nil {
		return fmt.Errorf("unmarshal tuple for closed registration entry: %w", err)
	}
	if len(tuple) != 2 {
		return fmt.Errorf("expected 2-tuple registration entry, got %d fields", len(tuple))
	}
	if err := json.Unmarshal(tuple[0], &e.VerificationKey); err != nil {
		return fmt.Errorf("decoding registration verification key: %w", err)
	}
	if err := json.Unmarshal(tuple[1], &e.Stake); err != nil {
		return fmt.Errorf("decoding registration stake: %w", err)
	}
	return nil
}

func (s *stmSingleSignatureWithRegisteredParty) UnmarshalJSON(data []byte) error {
	var tuple []json.RawMessage
	if err := json.Unmarshal(data, &tuple); err != nil {
		return fmt.Errorf("unmarshal tuple for signature with registered party: %w", err)
	}
	if len(tuple) != 2 {
		return fmt.Errorf("expected 2-tuple signature/register entry, got %d fields", len(tuple))
	}
	if err := json.Unmarshal(tuple[0], &s.Sig); err != nil {
		return fmt.Errorf("decoding single signature: %w", err)
	}
	if err := json.Unmarshal(tuple[1], &s.RegParty); err != nil {
		return fmt.Errorf("decoding registered party: %w", err)
	}
	return nil
}

func verifySTMCertificate(cert *Certificate) error {
	if cert == nil {
		return errors.New("certificate is nil")
	}
	if cert.IsGenesis() {
		return nil
	}
	return verifySTMSignature(
		[]byte(cert.SignedMessage),
		cert.AggregateVerificationKey,
		cert.MultiSignature,
		cert.Metadata.Parameters,
	)
}

func verifySTMSignature(
	msg []byte,
	encodedAVK string,
	encodedSig string,
	params ProtocolParameters,
) error {
	avk, err := parseSTMAggregateVerificationKey(encodedAVK)
	if err != nil {
		return fmt.Errorf("parsing aggregate verification key: %w", err)
	}
	aggrSig, err := parseSTMAggregateSignature(encodedSig)
	if err != nil {
		return fmt.Errorf("parsing multi-signature: %w", err)
	}
	return verifySTMConcatenationProof(msg, avk, aggrSig, params)
}

func parseSTMAggregateVerificationKey(
	encoded string,
) (*stmAggregateVerificationKey, error) {
	raw, ok := decodePrimaryEncodedBytes(encoded)
	if !ok {
		return nil, errors.New("could not decode aggregate verification key")
	}
	var ret stmAggregateVerificationKey
	if err := json.Unmarshal(raw, &ret); err == nil && len(ret.MTCommitment.Root) > 0 {
		if ret.MTCommitment.NrLeaves <= 0 {
			return nil, errors.New("nrLeaves must be positive")
		}
		return &ret, nil
	}
	if len(raw) < 48 {
		return nil, fmt.Errorf("invalid aggregate verification key payload length %d", len(raw))
	}
	nrLeaves, err := readUint64BE(raw[0:8])
	if err != nil {
		return nil, err
	}
	totalStake, err := readUint64BE(raw[len(raw)-8:])
	if err != nil {
		return nil, err
	}
	if nrLeaves == 0 {
		return nil, errors.New("nrLeaves must be positive")
	}
	if nrLeaves > uint64(math.MaxInt) {
		return nil, fmt.Errorf("nrLeaves %d exceeds maximum int value", nrLeaves)
	}
	return &stmAggregateVerificationKey{
		MTCommitment: stmMerkleTreeBatchCommitment{
			Root:     append([]byte(nil), raw[8:len(raw)-8]...),
			NrLeaves: int(nrLeaves), //nolint:gosec // validated against MaxInt above
		},
		TotalStake: totalStake,
	}, nil
}

func parseSTMAggregateSignature(encoded string) (*stmAggregateSignature, error) {
	raw, ok := decodePrimaryEncodedBytes(encoded)
	if !ok {
		return nil, errors.New("could not decode multi-signature")
	}
	var ret stmAggregateSignature
	if err := json.Unmarshal(raw, &ret); err == nil && len(ret.Signatures) > 0 {
		return &ret, nil
	}
	return parseSTMAggregateSignatureBytes(raw)
}

func parseSTMSignerVerificationKey(encoded string) ([]byte, error) {
	raw, ok := decodePrimaryEncodedBytes(encoded)
	if !ok {
		return nil, errors.New("could not decode signer verification key")
	}
	var ret stmSignerVerificationKey
	if err := json.Unmarshal(raw, &ret); err == nil && len(ret.VK) == 96 {
		return ret.VK, nil
	}
	if len(raw) == 96 {
		return raw, nil
	}
	return nil, fmt.Errorf("invalid signer verification key payload length %d", len(raw))
}

func parseSTMAggregateSignatureBytes(raw []byte) (*stmAggregateSignature, error) {
	if len(raw) < 9 {
		return nil, fmt.Errorf(
			"aggregate signature payload too short: need >= 9, got %d",
			len(raw),
		)
	}
	if raw[0] != 0 {
		return nil, fmt.Errorf("unsupported aggregate signature proof type prefix %d", raw[0])
	}
	offset := 1
	totalSigs, err := readUint64BE(raw[offset : offset+8])
	if err != nil {
		return nil, err
	}
	offset += 8
	// Each signature requires at least 8 bytes (size prefix), so cap the
	// pre-allocation against the remaining payload length to prevent OOM
	// from a malformed totalSigs value.
	remaining := len(raw) - offset
	const minSigSize = 8
	maxSigs := uint64(remaining / minSigSize) //nolint:gosec // remaining is non-negative (checked len >= 8 above)
	if totalSigs > maxSigs {
		return nil, fmt.Errorf(
			"totalSigs %d exceeds maximum possible given payload size (%d bytes remaining)",
			totalSigs, remaining,
		)
	}
	ret := &stmAggregateSignature{
		Signatures: make([]stmSingleSignatureWithRegisteredParty, 0, totalSigs),
	}
	for i := range totalSigs {
		if offset+8 > len(raw) {
			return nil, fmt.Errorf(
				"truncated aggregate signature at sig %d offset %d",
				i, offset,
			)
		}
		sizeSigReg, err := readUint64BE(raw[offset : offset+8])
		if err != nil {
			return nil, err
		}
		offset += 8
		if sizeSigReg > uint64(len(raw)-offset) { //nolint:gosec // offset <= len(raw) guaranteed by bounds check above
			return nil, errors.New("signature-with-party bytes overflow")
		}
		sigRegEnd := offset + int(sizeSigReg) //nolint:gosec // sizeSigReg <= len(raw)-offset, fits in int
		sigReg, err := parseSTMSignatureWithRegisteredPartyBytes(raw[offset:sigRegEnd])
		if err != nil {
			return nil, err
		}
		ret.Signatures = append(ret.Signatures, *sigReg)
		offset = sigRegEnd
	}
	batchProof, err := parseSTMMerkleBatchPathBytes(raw[offset:])
	if err != nil {
		return nil, err
	}
	ret.BatchProof = *batchProof
	return ret, nil
}

func parseSTMSignatureWithRegisteredPartyBytes(
	raw []byte,
) (*stmSingleSignatureWithRegisteredParty, error) {
	if len(raw) < 8 {
		return nil, fmt.Errorf(
			"signature-with-party payload too short: need >= 8, got %d",
			len(raw),
		)
	}
	regPartySize, err := readUint64BE(raw[0:8])
	if err != nil {
		return nil, err
	}
	regPartyStart := 8
	if regPartySize > uint64(len(raw)-regPartyStart) { //nolint:gosec // regPartyStart <= len(raw) guaranteed above
		return nil, errors.New("registered party bytes overflow")
	}
	regPartyEnd := regPartyStart + int(regPartySize) //nolint:gosec // regPartySize <= len(raw)-regPartyStart, fits in int
	regParty, err := parseSTMClosedRegistrationEntryBytes(raw[regPartyStart:regPartyEnd])
	if err != nil {
		return nil, err
	}
	sigLenOffset := regPartyEnd
	if sigLenOffset+8 > len(raw) {
		return nil, fmt.Errorf(
			"signature length prefix overflow at offset %d",
			sigLenOffset,
		)
	}
	sigSize, err := readUint64BE(raw[sigLenOffset : sigLenOffset+8])
	if err != nil {
		return nil, err
	}
	sigStart := sigLenOffset + 8
	if sigSize > uint64(len(raw)-sigStart) { //nolint:gosec // sigStart <= len(raw) guaranteed above
		return nil, errors.New("single signature bytes overflow")
	}
	sigEnd := sigStart + int(sigSize) //nolint:gosec // sigSize <= len(raw)-sigStart, fits in int
	sig, err := parseSTMSingleSignatureBytes(raw[sigStart:sigEnd])
	if err != nil {
		return nil, err
	}
	return &stmSingleSignatureWithRegisteredParty{
		Sig:      *sig,
		RegParty: *regParty,
	}, nil
}

func parseSTMClosedRegistrationEntryBytes(
	raw []byte,
) (*stmClosedRegistrationEntry, error) {
	if len(raw) < 104 {
		return nil, fmt.Errorf("invalid closed registration entry length %d", len(raw))
	}
	stake, err := readUint64BE(raw[96:104])
	if err != nil {
		return nil, err
	}
	return &stmClosedRegistrationEntry{
		VerificationKey: append([]byte(nil), raw[:96]...),
		Stake:           stake,
	}, nil
}

func parseSTMSingleSignatureBytes(raw []byte) (*stmSingleSignature, error) {
	if len(raw) < 8 {
		return nil, fmt.Errorf(
			"single signature payload too short: need >= 8, got %d",
			len(raw),
		)
	}
	nrIndexes, err := readUint64BE(raw[0:8])
	if err != nil {
		return nil, err
	}
	// Each index is 8 bytes; cap pre-allocation against remaining payload.
	remaining := uint64(len(raw) - 8) //nolint:gosec // non-negative: checked len >= 8 above
	if nrIndexes > remaining/8 {
		return nil, fmt.Errorf(
			"nrIndexes %d exceeds maximum possible given payload size (%d bytes remaining)",
			nrIndexes, remaining,
		)
	}
	offset := 8
	indexes := make([]uint64, 0, nrIndexes)
	for range nrIndexes {
		index, err := readUint64BE(raw[offset : offset+8])
		if err != nil {
			return nil, err
		}
		offset += 8
		indexes = append(indexes, index)
	}
	if offset+56 > len(raw) {
		return nil, errors.New("single signature payload too short")
	}
	sigma := append([]byte(nil), raw[offset:offset+48]...)
	offset += 48
	signerIndex, err := readUint64BE(raw[offset : offset+8])
	if err != nil {
		return nil, err
	}
	return &stmSingleSignature{
		Sigma:       sigma,
		Indexes:     indexes,
		SignerIndex: signerIndex,
	}, nil
}

func parseSTMMerkleBatchPathBytes(raw []byte) (*stmMerkleBatchPath, error) {
	if len(raw) < 16 {
		return nil, fmt.Errorf("invalid Merkle batch path length %d", len(raw))
	}
	lenValues, err := readUint64BE(raw[0:8])
	if err != nil {
		return nil, err
	}
	lenIndices, err := readUint64BE(raw[8:16])
	if err != nil {
		return nil, err
	}
	// Each value is 32 bytes and each index is 8 bytes; validate that the
	// claimed counts are consistent with the remaining payload length.
	remaining := uint64(len(raw) - 16) //nolint:gosec // non-negative: checked len >= 16 above
	if lenValues > remaining/32 {
		return nil, fmt.Errorf(
			"lenValues %d exceeds maximum possible given payload size (%d bytes remaining)",
			lenValues, remaining,
		)
	}
	afterValues := remaining - lenValues*32
	if lenIndices > afterValues/8 {
		return nil, fmt.Errorf(
			"lenIndices %d exceeds maximum possible given payload size (%d bytes remaining after values)",
			lenIndices, afterValues,
		)
	}
	offset := 16
	ret := &stmMerkleBatchPath{
		Values:  make([][]byte, 0, lenValues),
		Indices: make([]int, 0, lenIndices),
	}
	for range lenValues {
		if offset+32 > len(raw) {
			return nil, errors.New("merkle batch path values overflow")
		}
		ret.Values = append(ret.Values, append([]byte(nil), raw[offset:offset+32]...))
		offset += 32
	}
	for range lenIndices {
		if offset+8 > len(raw) {
			return nil, errors.New("merkle batch path indices overflow")
		}
		index, err := readUint64BE(raw[offset : offset+8])
		if err != nil {
			return nil, err
		}
		offset += 8
		ret.Indices = append(ret.Indices, int(index)) //nolint:gosec // index validated against payload bounds
	}
	return ret, nil
}

func verifySTMConcatenationProof(
	msg []byte,
	avk *stmAggregateVerificationKey,
	sig *stmAggregateSignature,
	params ProtocolParameters,
) error {
	if params.K == 0 {
		return errors.New("invalid protocol parameter K=0")
	}
	if params.M == 0 {
		return errors.New("invalid protocol parameter M=0")
	}
	if params.PhiF <= 0 || params.PhiF > 1.0 {
		return fmt.Errorf(
			"invalid protocol parameter phi_f=%f (must be in (0, 1])",
			params.PhiF,
		)
	}
	msgp := stmConcatenateWithMessage(avk, msg)
	sigs := make([]bls12381.G1Affine, 0, len(sig.Signatures))
	vks := make([]bls12381.G2Affine, 0, len(sig.Signatures))
	leaves := make([]stmClosedRegistrationEntry, 0, len(sig.Signatures))
	uniqueIndices := make(map[uint64]struct{})
	nrIndices := 0

	for _, sigReg := range sig.Signatures {
		vk, err := decodeSTMVerificationKey(sigReg.RegParty.VerificationKey)
		if err != nil {
			return fmt.Errorf("decoding signer verification key: %w", err)
		}
		sigma, err := decodeSTMSignature(sigReg.Sig.Sigma)
		if err != nil {
			return fmt.Errorf("decoding signer signature: %w", err)
		}
		for _, index := range sigReg.Sig.Indexes {
			if index >= params.M {
				return fmt.Errorf("lottery index %d exceeds parameter m=%d", index, params.M)
			}
			ev := stmDenseMapping(sigReg.Sig.Sigma, msgp, index)
			if !stmIsLotteryWon(params.PhiF, ev, sigReg.RegParty.Stake, avk.TotalStake) {
				return fmt.Errorf("lottery lost for signer index %d at lottery index %d", sigReg.Sig.SignerIndex, index)
			}
			uniqueIndices[index] = struct{}{}
			nrIndices++
		}
		sigs = append(sigs, sigma)
		vks = append(vks, vk)
		leaves = append(leaves, sigReg.RegParty)
	}
	if len(uniqueIndices) != nrIndices {
		return errors.New("aggregate signature contains duplicate lottery indices")
	}
	if uint64(nrIndices) < params.K {
		return fmt.Errorf("aggregate signature has %d lottery indices, requires at least %d", nrIndices, params.K)
	}
	if err := stmVerifyLeavesMembershipFromBatchPath(avk, leaves, &sig.BatchProof); err != nil {
		return fmt.Errorf("verifying Merkle batch proof: %w", err)
	}
	if err := stmVerifyBLSSignatureAggregate(msgp, vks, sigs); err != nil {
		return fmt.Errorf("aggregate signature verification failed: %w", err)
	}
	return nil
}

func stmVerifyBLSSignatureAggregate(
	msg []byte,
	vks []bls12381.G2Affine,
	sigs []bls12381.G1Affine,
) error {
	if len(vks) != len(sigs) || len(vks) == 0 {
		return fmt.Errorf("invalid aggregate inputs: %d keys, %d signatures", len(vks), len(sigs))
	}
	msgHash, err := bls12381.HashToG1(msg, stmBLSDomainSeparationTag)
	if err != nil {
		return fmt.Errorf("hash-to-g1: %w", err)
	}
	var negMsgHash bls12381.G1Affine
	negMsgHash.Neg(&msgHash)
	_, _, _, g2 := bls12381.Generators()
	g1Points := make([]bls12381.G1Affine, 0, 2*len(sigs))
	g2Points := make([]bls12381.G2Affine, 0, 2*len(sigs))
	for idx := range sigs {
		if sigs[idx].IsInfinity() {
			return fmt.Errorf("signature %d is infinity", idx)
		}
		if vks[idx].IsInfinity() {
			return fmt.Errorf("verification key %d is infinity", idx)
		}
		g1Points = append(g1Points, sigs[idx], negMsgHash)
		g2Points = append(g2Points, g2, vks[idx])
	}
	ok, err := bls12381.PairingCheck(g1Points, g2Points)
	if err != nil {
		return fmt.Errorf("batch pairing check: %w", err)
	}
	if !ok {
		return errors.New("aggregate BLS signature verification failed")
	}
	return nil
}

func decodeSTMSignature(raw []byte) (bls12381.G1Affine, error) {
	var ret bls12381.G1Affine
	if err := ret.Unmarshal(raw); err != nil {
		return bls12381.G1Affine{}, err
	}
	if ret.IsInfinity() {
		return bls12381.G1Affine{}, errors.New("signature is infinity")
	}
	return ret, nil
}

func decodeSTMVerificationKey(raw []byte) (bls12381.G2Affine, error) {
	var ret bls12381.G2Affine
	if err := ret.Unmarshal(raw); err != nil {
		return bls12381.G2Affine{}, err
	}
	if ret.IsInfinity() {
		return bls12381.G2Affine{}, errors.New("verification key is infinity")
	}
	return ret, nil
}

func stmConcatenateWithMessage(
	avk *stmAggregateVerificationKey,
	msg []byte,
) []byte {
	ret := make([]byte, 0, len(msg)+len(avk.MTCommitment.Root))
	ret = append(ret, msg...)
	ret = append(ret, avk.MTCommitment.Root...)
	return ret
}

func stmDenseMapping(
	sigma []byte,
	msg []byte,
	index uint64,
) [64]byte {
	h := blake2b.Sum512(
		bytes.Join(
			[][]byte{
				[]byte("map"),
				msg,
				uint64ToLittleEndianBytes(index),
				sigma,
			},
			nil,
		),
	)
	return h
}

func stmVerifyLeavesMembershipFromBatchPath(
	avk *stmAggregateVerificationKey,
	leaves []stmClosedRegistrationEntry,
	proof *stmMerkleBatchPath,
) error {
	if len(leaves) != len(proof.Indices) {
		return fmt.Errorf("leaf count %d does not match proof index count %d", len(leaves), len(proof.Indices))
	}
	orderedIndices := append([]int(nil), proof.Indices...)
	for idx, v := range orderedIndices {
		if v < 0 || v >= avk.MTCommitment.NrLeaves {
			return fmt.Errorf(
				"merkle batch proof index %d out of range [0, %d)",
				v, avk.MTCommitment.NrLeaves,
			)
		}
		if idx > 0 && orderedIndices[idx-1] >= v {
			return errors.New("merkle batch proof indices are not sorted")
		}
	}
	nextPow2 := stmNextPowerOfTwo(avk.MTCommitment.NrLeaves)
	nrNodes := avk.MTCommitment.NrLeaves + nextPow2 - 1
	for idx := range orderedIndices {
		orderedIndices[idx] += nextPow2 - 1
	}
	hashedLeaves := make([][]byte, 0, len(leaves))
	for _, leaf := range leaves {
		hashedLeaves = append(hashedLeaves, stmBlake2b256(stmMerkleLeafBytes(leaf)))
	}
	values := proof.Values
	valueOffset := 0
	idx := orderedIndices[0]
	for idx > 0 {
		newHashes := make([][]byte, 0, len(orderedIndices))
		newIndices := make([]int, 0, len(orderedIndices))
		idx = stmParent(idx)
		for i := 0; i < len(orderedIndices); i++ {
			newIndices = append(newIndices, stmParent(orderedIndices[i]))
			if orderedIndices[i]&1 == 0 {
				if valueOffset >= len(values) {
					return errors.New("merkle batch proof ran out of sibling values")
				}
				newHashes = append(
					newHashes,
					stmBlake2b256(bytes.Join([][]byte{values[valueOffset], hashedLeaves[i]}, nil)),
				)
				valueOffset++
				continue
			}
			sibling := stmSibling(orderedIndices[i])
			if i < len(orderedIndices)-1 && orderedIndices[i+1] == sibling {
				newHashes = append(
					newHashes,
					stmBlake2b256(bytes.Join([][]byte{hashedLeaves[i], hashedLeaves[i+1]}, nil)),
				)
				i++
				continue
			}
			if sibling < nrNodes {
				if valueOffset >= len(values) {
					return errors.New("merkle batch proof ran out of sibling values")
				}
				newHashes = append(
					newHashes,
					stmBlake2b256(bytes.Join([][]byte{hashedLeaves[i], values[valueOffset]}, nil)),
				)
				valueOffset++
				continue
			}
			newHashes = append(
				newHashes,
				stmBlake2b256(bytes.Join([][]byte{hashedLeaves[i], stmBlake2b256([]byte{0})}, nil)),
			)
		}
		hashedLeaves = newHashes
		orderedIndices = newIndices
	}
	if len(hashedLeaves) != 1 || !bytes.Equal(hashedLeaves[0], avk.MTCommitment.Root) {
		return errors.New("merkle batch proof root mismatch")
	}
	return nil
}

func stmMerkleLeafBytes(leaf stmClosedRegistrationEntry) []byte {
	ret := make([]byte, 0, 104)
	ret = append(ret, leaf.VerificationKey...)
	ret = append(ret, uint64ToBigEndianBytes(leaf.Stake)...)
	return ret
}

func stmBlake2b256(data []byte) []byte {
	sum := blake2b.Sum256(data)
	return sum[:]
}

func stmIsLotteryWon(
	phiF float64,
	ev [64]byte,
	stake uint64,
	totalStake uint64,
) bool {
	if math.Abs(phiF-1.0) < f64Epsilon {
		return true
	}
	if totalStake == 0 {
		return false
	}
	evMax := new(big.Int).Lsh(big.NewInt(1), 512)
	evInt := stmBigIntFromLittleEndian(ev[:])
	denominator := new(big.Int).Sub(new(big.Int).Set(evMax), evInt)
	if denominator.Sign() <= 0 {
		return false
	}
	q := new(big.Rat).SetFrac(new(big.Int).Set(evMax), denominator)
	c := new(big.Rat)
	c.SetFloat64(math.Log1p(-phiF))
	w := new(big.Rat).SetFrac(
		new(big.Int).SetUint64(stake),
		new(big.Int).SetUint64(totalStake),
	)
	x := new(big.Rat).Mul(w, c)
	x.Neg(x)
	return stmTaylorComparison(1000, q, x)
}

const f64Epsilon = 2.220446049250313e-16

func stmTaylorComparison(
	bound int,
	cmp *big.Rat,
	x *big.Rat,
) bool {
	newX := new(big.Rat).Set(x)
	phi := big.NewRat(1, 1)
	divisor := big.NewInt(1)
	for range bound {
		phi = new(big.Rat).Add(phi, newX)
		divisor = new(big.Int).Add(divisor, big.NewInt(1))
		newX = new(big.Rat).Quo(
			new(big.Rat).Mul(newX, x),
			new(big.Rat).SetInt(divisor),
		)
		errorTerm := stmAbsRat(newX)
		errorTerm.Mul(errorTerm, big.NewRat(3, 1))
		if cmp.Cmp(new(big.Rat).Add(phi, errorTerm)) > 0 {
			return false
		}
		if cmp.Cmp(new(big.Rat).Sub(phi, errorTerm)) < 0 {
			return true
		}
	}
	return false
}

func stmAbsRat(v *big.Rat) *big.Rat {
	if v.Sign() >= 0 {
		return new(big.Rat).Set(v)
	}
	return new(big.Rat).Neg(v)
}

func stmBigIntFromLittleEndian(raw []byte) *big.Int {
	reversed := make([]byte, len(raw))
	for idx := range raw {
		reversed[len(raw)-1-idx] = raw[idx]
	}
	return new(big.Int).SetBytes(reversed)
}

func stmNextPowerOfTwo(v int) int {
	if v <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(v-1))
}

func stmParent(idx int) int {
	return (idx - 1) / 2
}

func stmSibling(idx int) int {
	if idx&1 == 0 {
		return idx - 1
	}
	return idx + 1
}

func uint64ToLittleEndianBytes(v uint64) []byte {
	ret := make([]byte, 8)
	binary.LittleEndian.PutUint64(ret, v)
	return ret
}

func readUint64BE(raw []byte) (uint64, error) {
	if len(raw) < 8 {
		return 0, fmt.Errorf("need 8 bytes, got %d", len(raw))
	}
	return binary.BigEndian.Uint64(raw[:8]), nil
}
