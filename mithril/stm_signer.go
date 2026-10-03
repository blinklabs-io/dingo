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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"

	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
)

// MaxSTMLotteryCount bounds signing and verification work for aggregator-
// supplied protocol parameters. It is above published network values while
// keeping malformed parameters from causing unbounded lottery iteration.
const MaxSTMLotteryCount uint64 = 1 << 16

// stmProofOfPossessionMessage is the message hashed for the first half of a
// verification key's proof of possession.
var stmProofOfPossessionMessage = []byte("PoP")

// STMSigningKey is a signer's BLS12-381 secret key for the Mithril STM
// multi-signature scheme.
type STMSigningKey struct {
	sk fr.Element
}

// NewSTMSigningKey generates a random signing key.
func NewSTMSigningKey() (*STMSigningKey, error) {
	var seed [64]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return nil, fmt.Errorf("reading randomness: %w", err)
	}
	key := &STMSigningKey{}
	key.sk.SetBigInt(new(big.Int).SetBytes(seed[:]))
	if key.sk.IsZero() {
		return nil, errors.New("generated zero signing key")
	}
	return key, nil
}

// STMSigningKeyFromBytes decodes the 32-byte big-endian form returned by
// Bytes.
func STMSigningKeyFromBytes(raw []byte) (*STMSigningKey, error) {
	key := &STMSigningKey{}
	if err := key.sk.SetBytesCanonical(raw); err != nil {
		return nil, fmt.Errorf("invalid STM signing key: %w", err)
	}
	if key.sk.IsZero() {
		return nil, errors.New("invalid STM signing key: zero")
	}
	return key, nil
}

// Bytes returns the 32-byte big-endian encoding of the secret key.
func (k *STMSigningKey) Bytes() []byte {
	b := k.sk.Bytes()
	return b[:]
}

// STMVerificationKey is a signer's verification key with its proof of
// possession.
type STMVerificationKey struct {
	// VK is the compressed G2 public key (96 bytes).
	VK []byte
	// PoP is the compressed G1 proof of possession (96 bytes).
	PoP []byte
}

// VerificationKey derives the verification key and its proof of possession.
func (k *STMSigningKey) VerificationKey() (*STMVerificationKey, error) {
	_, _, g1, g2 := bls12381.Generators()
	skInt := k.sk.BigInt(new(big.Int))
	var vk bls12381.G2Affine
	vk.ScalarMultiplication(&g2, skInt)
	k1, err := k.signG1(stmProofOfPossessionMessage)
	if err != nil {
		return nil, err
	}
	var k2 bls12381.G1Affine
	k2.ScalarMultiplication(&g1, skInt)
	vkBytes := vk.Bytes()
	k1Bytes := k1.Bytes()
	k2Bytes := k2.Bytes()
	return &STMVerificationKey{
		VK:  vkBytes[:],
		PoP: append(k1Bytes[:], k2Bytes[:]...),
	}, nil
}

// Bytes returns the verification key followed by its proof of possession,
// the byte string a signer's KES key signs to bind it to a pool.
func (v *STMVerificationKey) Bytes() []byte {
	return append(slices.Clone(v.VK), v.PoP...)
}

// Encode returns the aggregator wire form: hex of a JSON object holding the
// key and proof as byte arrays.
func (v *STMVerificationKey) Encode() (string, error) {
	return encodeSTMJSON(struct {
		VK  jsonByteArray `json:"vk"`
		PoP jsonByteArray `json:"pop"`
	}{VK: v.VK, PoP: v.PoP})
}

func (k *STMSigningKey) signG1(msg []byte) (bls12381.G1Affine, error) {
	h, err := bls12381.HashToG1(msg, stmBLSDomainSeparationTag)
	if err != nil {
		return bls12381.G1Affine{}, fmt.Errorf("hash-to-g1: %w", err)
	}
	var sig bls12381.G1Affine
	sig.ScalarMultiplication(&h, k.sk.BigInt(new(big.Int)))
	return sig, nil
}

// STMClosedRegistration is the set of registered signers and stakes for one
// epoch, ordered and committed to as the reference implementation does.
type STMClosedRegistration struct {
	entries    []stmClosedRegistrationEntry
	root       []byte
	totalStake uint64
}

// NewSTMClosedRegistration closes a registration over the given signers.
// Each party's verification key is the encoded key-with-proof the aggregator
// publishes.
func NewSTMClosedRegistration(
	parties []MithrilStakeDistributionParty,
) (*STMClosedRegistration, error) {
	if len(parties) == 0 {
		return nil, errors.New("no registered signers")
	}
	reg := &STMClosedRegistration{
		entries: make([]stmClosedRegistrationEntry, 0, len(parties)),
	}
	for _, party := range parties {
		vk, err := parseSTMSignerVerificationKey(party.VerificationKey)
		if err != nil {
			return nil, fmt.Errorf(
				"signer %q verification key: %w",
				party.PartyID,
				err,
			)
		}
		if reg.totalStake+party.Stake < reg.totalStake {
			return nil, errors.New("total stake overflows uint64")
		}
		reg.totalStake += party.Stake
		reg.entries = append(
			reg.entries,
			stmClosedRegistrationEntry{VerificationKey: vk, Stake: party.Stake},
		)
	}
	slices.SortStableFunc(
		reg.entries,
		func(a, b stmClosedRegistrationEntry) int {
			if a.Stake != b.Stake {
				if a.Stake < b.Stake {
					return -1
				}
				return 1
			}
			return bytes.Compare(a.VerificationKey, b.VerificationKey)
		},
	)
	reg.root = stmMerkleRoot(reg.entries)
	return reg, nil
}

// SignerIndex returns the position of the signer with the given
// verification key and its stake.
func (r *STMClosedRegistration) SignerIndex(
	vk []byte,
) (index uint64, stake uint64, ok bool) {
	for i, entry := range r.entries {
		if bytes.Equal(entry.VerificationKey, vk) {
			return uint64(i), entry.Stake, true
		}
	}
	return 0, 0, false
}

// AggregateVerificationKey returns the aggregator wire form of the
// registration's commitment.
func (r *STMClosedRegistration) AggregateVerificationKey() (string, error) {
	var avk struct {
		MTCommitment struct {
			Root     jsonByteArray `json:"root"`
			NrLeaves int           `json:"nr_leaves"`
			Hasher   *struct{}     `json:"hasher"`
		} `json:"mt_commitment"`
		TotalStake uint64 `json:"total_stake"`
	}
	avk.MTCommitment.Root = r.root
	avk.MTCommitment.NrLeaves = len(r.entries)
	avk.TotalStake = r.totalStake
	return encodeSTMJSON(avk)
}

// stmMerkleRoot builds the batch-compatible Merkle commitment the
// verifier's membership check reconstructs: leaves occupy the tail of the
// node array and any missing child is the hash of a single zero byte.
func stmMerkleRoot(entries []stmClosedRegistrationEntry) []byte {
	n := len(entries)
	numNodes := n + stmNextPowerOfTwo(n) - 1
	empty := stmBlake2b256([]byte{0})
	child := func(nodes [][]byte, idx int) []byte {
		if idx < numNodes {
			return nodes[idx]
		}
		return empty
	}
	nodes := make([][]byte, numNodes)
	for i := range nodes {
		nodes[i] = empty
	}
	for i, entry := range entries {
		nodes[numNodes-n+i] = stmBlake2b256(stmMerkleLeafBytes(entry))
	}
	for i := numNodes - n - 1; i >= 0; i-- {
		nodes[i] = stmBlake2b256(
			slices.Concat(child(nodes, 2*i+1), child(nodes, 2*i+2)),
		)
	}
	return nodes[0]
}

// Sign produces the signer's individual signature over msg: a BLS signature
// of msg bound to the registration's commitment, plus every lottery index
// below params.M that the signer's stake wins.
func (k *STMSigningKey) Sign(
	msg []byte,
	reg *STMClosedRegistration,
	params ProtocolParameters,
) (*STMSingleSignature, error) {
	if err := validateSTMParameters(params); err != nil {
		return nil, err
	}
	vk, err := k.VerificationKey()
	if err != nil {
		return nil, err
	}
	signerIndex, stake, ok := reg.SignerIndex(vk.VK)
	if !ok {
		return nil, errors.New("signer is not in the closed registration")
	}
	msgp := slices.Concat(msg, reg.root)
	sigma, err := k.signG1(msgp)
	if err != nil {
		return nil, err
	}
	sigmaBytes := sigma.Bytes()
	return &STMSingleSignature{
		Sigma: sigmaBytes[:],
		Indexes: stmWonIndexes(
			sigmaBytes[:],
			msgp,
			params,
			stake,
			reg.totalStake,
		),
		SignerIndex: signerIndex,
	}, nil
}

// stmWonIndexes lists the lottery indexes below params.M won by a signer with
// the given stake and signature over msgp.
func stmWonIndexes(
	sigma []byte,
	msgp []byte,
	params ProtocolParameters,
	stake uint64,
	totalStake uint64,
) []uint64 {
	var won []uint64
	for index := range params.M {
		ev := stmDenseMapping(sigma, msgp, index)
		if stmIsLotteryWon(params.PhiF, ev, stake, totalStake) {
			won = append(won, index)
		}
	}
	return won
}

// Encode returns the aggregator wire form of the signature.
func (s *STMSingleSignature) Encode() (string, error) {
	indexes := s.Indexes
	if indexes == nil {
		indexes = []uint64{}
	}
	return encodeSTMJSON(struct {
		Sigma       jsonByteArray `json:"sigma"`
		Indexes     []uint64      `json:"indexes"`
		SignerIndex uint64        `json:"signer_index"`
	}{Sigma: s.Sigma, Indexes: indexes, SignerIndex: s.SignerIndex})
}

// jsonByteArray marshals as an array of numbers, the form the reference
// implementation's serializer uses for byte strings.
type jsonByteArray []byte

func (b jsonByteArray) MarshalJSON() ([]byte, error) {
	values := make([]int, len(b))
	for i, v := range b {
		values[i] = int(v)
	}
	return json.Marshal(values)
}

func encodeSTMJSON(v any) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("encoding STM value: %w", err)
	}
	return hex.EncodeToString(raw), nil
}
