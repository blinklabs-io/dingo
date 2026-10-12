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
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
)

// stmProofOfPossessionMessage is the fixed message a signer's proof of
// possession signs; it is the reference implementation's "PoP" constant.
var stmProofOfPossessionMessage = []byte("PoP")

// stmRegistration is one signer's registration for the epoch: the BLS
// verification key with its proof of possession, and its stake.
type stmRegistration struct {
	PartyID           string
	VerificationKey   []byte
	ProofOfPossession []byte
	Stake             uint64
}

// verifySTMProofOfPossession checks that the registrant holds the secret key
// behind vk: k1 must be a signature over the PoP message under vk, and k2 must
// be that same key's public key in G1.
func verifySTMProofOfPossession(vkBytes, pop []byte) error {
	if len(pop) != 96 {
		return fmt.Errorf(
			"proof of possession must be 96 bytes, got %d",
			len(pop),
		)
	}
	vk, err := decodeSTMVerificationKey(vkBytes)
	if err != nil {
		return fmt.Errorf("decoding verification key: %w", err)
	}
	var k1, k2 bls12381.G1Affine
	if err := k1.Unmarshal(pop[:48]); err != nil {
		return fmt.Errorf("decoding proof of possession signature: %w", err)
	}
	if err := k2.Unmarshal(pop[48:]); err != nil {
		return fmt.Errorf("decoding proof of possession key: %w", err)
	}
	if k1.IsInfinity() || k2.IsInfinity() {
		return errors.New("proof of possession is infinity")
	}
	if err := stmVerifyBLSSignatureAggregate(
		stmProofOfPossessionMessage,
		[]bls12381.G2Affine{vk}, []bls12381.G1Affine{k1},
	); err != nil {
		return fmt.Errorf("proof of possession signature: %w", err)
	}
	_, _, g1, g2 := bls12381.Generators()
	var negK2 bls12381.G1Affine
	negK2.Neg(&k2)
	ok, err := bls12381.PairingCheck(
		[]bls12381.G1Affine{g1, negK2},
		[]bls12381.G2Affine{vk, g2},
	)
	if err != nil {
		return fmt.Errorf("proof of possession pairing: %w", err)
	}
	if !ok {
		return errors.New(
			"proof of possession key does not match verification key",
		)
	}
	return nil
}

// closeSTMRegistration orders registrations the way the reference key
// registry does (stake, then verification key bytes) and returns the closed
// entries. A signer's position in the result is its signer index.
func closeSTMRegistration(
	regs []stmRegistration,
) ([]stmRegistration, []stmClosedRegistrationEntry, uint64) {
	ordered := slices.Clone(regs)
	slices.SortFunc(ordered, func(a, b stmRegistration) int {
		if a.Stake != b.Stake {
			if a.Stake < b.Stake {
				return -1
			}
			return 1
		}
		return bytes.Compare(a.VerificationKey, b.VerificationKey)
	})
	entries := make([]stmClosedRegistrationEntry, len(ordered))
	var total uint64
	for i, r := range ordered {
		entries[i] = stmClosedRegistrationEntry{
			VerificationKey: r.VerificationKey,
			Stake:           r.Stake,
		}
		total += r.Stake
	}
	return ordered, entries, total
}

// stmMerkleTree is the registration commitment tree: leaves are
// blake2b-256(vk || stake), laid out as a heap with missing children hashed as
// blake2b-256(0x00).
type stmMerkleTree struct {
	nodes      [][]byte
	leafOffset int
	leaves     int
}

func newSTMMerkleTree(entries []stmClosedRegistrationEntry) *stmMerkleTree {
	n := len(entries)
	// Leaves are padded to a power of two, as stmMerkleTreeDimensions lays
	// out the tree the verifier walks.
	nextPow2 := 1
	for nextPow2 < n {
		nextPow2 <<= 1
	}
	numNodes := n + nextPow2 - 1
	nodes := make([][]byte, numNodes)
	leafOffset := numNodes - n
	for i, e := range entries {
		nodes[leafOffset+i] = stmBlake2b256(stmMerkleLeafBytes(e))
	}
	zero := stmBlake2b256([]byte{0})
	child := func(idx int) []byte {
		if idx < numNodes {
			return nodes[idx]
		}
		return zero
	}
	for i := leafOffset - 1; i >= 0; i-- {
		nodes[i] = stmBlake2b256(
			bytes.Join([][]byte{child(2*i + 1), child(2*i + 2)}, nil),
		)
	}
	return &stmMerkleTree{nodes: nodes, leafOffset: leafOffset, leaves: n}
}

func (t *stmMerkleTree) root() []byte { return t.nodes[0] }

// batchPath returns the sibling hashes needed to verify the given leaves
// together. indices must be sorted ascending and unique.
func (t *stmMerkleTree) batchPath(indices []int) (stmMerkleBatchPath, error) {
	if len(indices) == 0 {
		return stmMerkleBatchPath{}, errors.New("no leaves to prove")
	}
	ordered := make([]int, len(indices))
	for i, idx := range indices {
		if idx < 0 || idx >= t.leaves {
			return stmMerkleBatchPath{}, fmt.Errorf(
				"leaf index %d out of range [0, %d)", idx, t.leaves,
			)
		}
		if i > 0 && indices[i-1] >= idx {
			return stmMerkleBatchPath{}, errors.New(
				"leaf indices must be sorted and unique",
			)
		}
		ordered[i] = idx + t.leafOffset
	}
	var values [][]byte
	for ordered[0] > 0 {
		next := make([]int, 0, len(ordered))
		for i := 0; i < len(ordered); i++ {
			next = append(next, stmParent(ordered[i]))
			sibling := stmSibling(ordered[i])
			if i < len(ordered)-1 && ordered[i+1] == sibling {
				i++
			} else if sibling < len(t.nodes) {
				values = append(values, t.nodes[sibling])
			}
		}
		ordered = next
	}
	return stmMerkleBatchPath{
		Values:  values,
		Indices: slices.Clone(indices),
	}, nil
}

// encodeSTMAggregateVerificationKey renders an aggregate verification key in
// the byte layout parseSTMAggregateVerificationKey reads: leaf count, merkle
// root, total stake.
func encodeSTMAggregateVerificationKey(
	root []byte,
	leaves int,
	totalStake uint64,
) string {
	count := uint64(leaves) //nolint:gosec // len is non-negative
	raw := binary.BigEndian.AppendUint64(nil, count)
	raw = append(raw, root...)
	raw = binary.BigEndian.AppendUint64(raw, totalStake)
	return hex.EncodeToString(raw)
}

func encodeSTMSingleSignature(sig stmSingleSignature) []byte {
	raw := binary.BigEndian.AppendUint64(nil, uint64(len(sig.Indexes)))
	for _, idx := range sig.Indexes {
		raw = binary.BigEndian.AppendUint64(raw, idx)
	}
	raw = append(raw, sig.Sigma...)
	return binary.BigEndian.AppendUint64(raw, sig.SignerIndex)
}

// encodeSTMAggregateSignature is the inverse of
// parseSTMAggregateSignatureBytes: the proof-type prefix, the length-prefixed
// signatures with their registered parties, then the merkle batch path.
func encodeSTMAggregateSignature(
	sigs []stmSingleSignatureWithRegisteredParty,
	proof stmMerkleBatchPath,
) []byte {
	raw := []byte{0}
	raw = binary.BigEndian.AppendUint64(raw, uint64(len(sigs)))
	for _, s := range sigs {
		party := append(
			bytes.Clone(s.RegParty.VerificationKey),
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
		)
		binary.BigEndian.PutUint64(party[96:], s.RegParty.Stake)
		single := encodeSTMSingleSignature(s.Sig)
		entry := binary.BigEndian.AppendUint64(nil, uint64(len(party)))
		entry = append(entry, party...)
		entry = binary.BigEndian.AppendUint64(entry, uint64(len(single)))
		entry = append(entry, single...)
		raw = binary.BigEndian.AppendUint64(raw, uint64(len(entry)))
		raw = append(raw, entry...)
	}
	raw = binary.BigEndian.AppendUint64(raw, uint64(len(proof.Values)))
	raw = binary.BigEndian.AppendUint64(raw, uint64(len(proof.Indices)))
	for _, v := range proof.Values {
		raw = append(raw, v...)
	}
	for _, idx := range proof.Indices {
		leaf := uint64(idx) //nolint:gosec // indices are validated
		raw = binary.BigEndian.AppendUint64(raw, leaf)
	}
	return raw
}

// verifySTMSingleSignature checks one signer's signature for msg: it must be
// a valid BLS signature under the party's key over msg || commitment root,
// and every claimed lottery index must be in range and won at the party's
// stake.
func verifySTMSingleSignature(
	msg []byte,
	avk *stmAggregateVerificationKey,
	params ProtocolParameters,
	party stmClosedRegistrationEntry,
	sig *stmSingleSignature,
) error {
	if len(sig.Indexes) == 0 {
		return errors.New("signature claims no lottery indices")
	}
	vk, err := decodeSTMVerificationKey(party.VerificationKey)
	if err != nil {
		return fmt.Errorf("decoding signer verification key: %w", err)
	}
	sigma, err := decodeSTMSignature(sig.Sigma)
	if err != nil {
		return fmt.Errorf("decoding signature: %w", err)
	}
	msgp := stmConcatenateWithMessage(avk, msg)
	seen := make(map[uint64]struct{}, len(sig.Indexes))
	for _, index := range sig.Indexes {
		if index >= params.M {
			return fmt.Errorf(
				"lottery index %d exceeds parameter m=%d",
				index,
				params.M,
			)
		}
		if _, dup := seen[index]; dup {
			return fmt.Errorf("lottery index %d claimed twice", index)
		}
		seen[index] = struct{}{}
		ev := stmDenseMapping(sig.Sigma, msgp, index)
		if !stmIsLotteryWon(params.PhiF, ev, party.Stake, avk.TotalStake) {
			return fmt.Errorf("lottery lost at index %d", index)
		}
	}
	return stmVerifyBLSSignatureAggregate(
		msgp, []bls12381.G2Affine{vk}, []bls12381.G1Affine{sigma},
	)
}

// selectSTMSignatures picks, for the lowest lottery indices first, the
// smallest valid signature that won each index, and returns the signatures
// trimmed to the indices they were chosen for, ordered by signer index. It
// fails when the signatures cover fewer than k distinct indices.
func selectSTMSignatures(
	sigs []stmSingleSignature,
	k uint64,
) ([]stmSingleSignature, error) {
	best := make(map[uint64]int)
	for i, s := range sigs {
		for _, index := range s.Indexes {
			if prev, ok := best[index]; !ok ||
				bytes.Compare(s.Sigma, sigs[prev].Sigma) < 0 {
				best[index] = i
			}
		}
	}
	indices := make([]uint64, 0, len(best))
	for index := range best {
		indices = append(indices, index)
	}
	slices.Sort(indices)
	if uint64(len(indices)) < k {
		return nil, fmt.Errorf(
			"signatures cover %d lottery indices, need %d", len(indices), k,
		)
	}
	chosen := make(map[int][]uint64)
	for _, index := range indices[:k] {
		chosen[best[index]] = append(chosen[best[index]], index)
	}
	out := make([]stmSingleSignature, 0, len(chosen))
	for i, idx := range chosen {
		trimmed := sigs[i]
		trimmed.Indexes = idx
		out = append(out, trimmed)
	}
	slices.SortFunc(out, func(a, b stmSingleSignature) int {
		switch {
		case a.SignerIndex < b.SignerIndex:
			return -1
		case a.SignerIndex > b.SignerIndex:
			return 1
		}
		return 0
	})
	return out, nil
}
