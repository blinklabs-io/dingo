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
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"strings"

	"github.com/blinklabs-io/dingo/keystore"
	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
)

// ErrInvalidSigningKey is returned when a vote signing key scalar is
// malformed, zero, or not below the BLS12-381 scalar field modulus.
var ErrInvalidSigningKey = errors.New("invalid vote signing key")

// ErrInvalidPublicKey is returned when a voter public key is malformed,
// not on the curve/subgroup, or the point at infinity.
var ErrInvalidPublicKey = errors.New("invalid voter public key")

const voteSigningKeySize = 32

// voteSigningKeyFileMaxSize bounds reads of the signing key file: a hex
// scalar plus surrounding whitespace fits comfortably in 1 KiB.
const voteSigningKeyFileMaxSize = 1024

// voterPoolKeyHashSize is the Blake2b-224 pool key hash length.
const voterPoolKeyHashSize = 28

// DerivePrototypeVoteSigningKey reproduces the current Leios prototype's
// temporary key derivation: the 28-byte pool cold-key hash is right-padded
// with four zero bytes and interpreted as a BLS12-381 secret scalar.
func DerivePrototypeVoteSigningKey(
	poolKeyHash []byte,
) (*VoteSigningKey, error) {
	if len(poolKeyHash) != voterPoolKeyHashSize {
		return nil, fmt.Errorf(
			"%w: pool key hash must be %d bytes, got %d",
			ErrInvalidSigningKey,
			voterPoolKeyHashSize,
			len(poolKeyHash),
		)
	}
	raw := make([]byte, voteSigningKeySize)
	copy(raw, poolKeyHash)
	return ParseVoteSigningKey(hex.EncodeToString(raw))
}

// VoteSigningKey is a BLS12-381 MinSig signing key for Leios votes: a
// scalar secret key with its G2 public key.
type VoteSigningKey struct {
	sk  *big.Int
	pub bls12381.G2Affine
}

// PublicKey returns the G2 public key for this signing key.
func (k *VoteSigningKey) PublicKey() *bls12381.G2Affine {
	return &k.pub
}

// PublicKeyBytes returns the 96-byte compressed G2 public key.
func (k *VoteSigningKey) PublicKeyBytes() []byte {
	pubBytes := k.pub.Bytes()
	return pubBytes[:]
}

// ParseVoteSigningKey parses a hex-encoded 32-byte big-endian scalar.
// Scalars of zero or at least the scalar field modulus are rejected (no
// silent reduction).
func ParseVoteSigningKey(hexStr string) (*VoteSigningKey, error) {
	raw, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidSigningKey, err)
	}
	if len(raw) != voteSigningKeySize {
		return nil, fmt.Errorf(
			"%w: must be %d bytes, got %d",
			ErrInvalidSigningKey,
			voteSigningKeySize,
			len(raw),
		)
	}
	sk := new(big.Int).SetBytes(raw)
	if sk.Sign() == 0 || sk.Cmp(fr.Modulus()) >= 0 {
		return nil, fmt.Errorf(
			"%w: scalar must be in [1, r)",
			ErrInvalidSigningKey,
		)
	}
	key := &VoteSigningKey{sk: sk}
	key.pub.ScalarMultiplicationBase(sk)
	return key, nil
}

// LoadVoteSigningKeyFile reads a hex-encoded vote signing key from a file.
// The file must not be accessible beyond its owner (checked on the open
// handle via keystore, which avoids TOCTOU races between check and read)
// and must not exceed voteSigningKeyFileMaxSize.
func LoadVoteSigningKeyFile(path string) (*VoteSigningKey, error) {
	f, err := os.Open(path) // #nosec G304 -- operator-configured key path
	if err != nil {
		return nil, fmt.Errorf("open vote signing key file: %w", err)
	}
	defer f.Close() //nolint:errcheck // read-only handle
	if err := keystore.CheckOpenFilePermissions(f); err != nil {
		return nil, err
	}
	// Read one byte past the limit so oversized files are rejected
	// rather than silently truncated.
	data, err := io.ReadAll(io.LimitReader(f, voteSigningKeyFileMaxSize+1))
	if err != nil {
		return nil, fmt.Errorf("read vote signing key file: %w", err)
	}
	if len(data) > voteSigningKeyFileMaxSize {
		return nil, fmt.Errorf(
			"vote signing key file %q exceeds %d bytes",
			path,
			voteSigningKeyFileMaxSize,
		)
	}
	key, err := ParseVoteSigningKey(strings.TrimSpace(string(data)))
	if err != nil {
		return nil, fmt.Errorf("vote signing key file %q: %w", path, err)
	}
	return key, nil
}

// ParseVoterPublicKey parses a hex-encoded 96-byte compressed G2 public
// key, rejecting points not in the subgroup and the point at infinity.
func ParseVoterPublicKey(hexStr string) (*bls12381.G2Affine, error) {
	raw, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidPublicKey, err)
	}
	if len(raw) != VotePublicKeySize {
		return nil, fmt.Errorf(
			"%w: must be %d bytes, got %d",
			ErrInvalidPublicKey,
			VotePublicKeySize,
			len(raw),
		)
	}
	var point bls12381.G2Affine
	// SetBytes validates curve membership and subgroup order.
	if _, err := point.SetBytes(raw); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidPublicKey, err)
	}
	if point.IsInfinity() {
		return nil, fmt.Errorf(
			"%w: point is infinity",
			ErrInvalidPublicKey,
		)
	}
	return &point, nil
}

// VoterRegistry maps pool key hashes to their registered BLS voting public
// keys. CIP-0164's key registration/rotation mechanism is not yet
// specified, so the registry is populated from static configuration
// (devnet-style). Voters absent from the registry can still have their
// committee membership checked, but not their signatures.
//
// SECURITY: the registry is the trust root that discharges the
// proof-of-possession requirement of BLS aggregate verification
// (VerifyAggregateSignature): operators vouch for the keys they
// configure. Any registration mechanism that replaces this static
// configuration must verify a proof of possession before a key enters
// the registry, or aggregate certificate verification becomes forgeable
// via rogue-key attacks.
type VoterRegistry struct {
	keys map[string]*bls12381.G2Affine // lowercase-hex pool key hash -> pubkey
}

// NewVoterRegistry builds a registry from hex pool key hash -> hex
// 96-byte compressed G2 public key. Every entry is validated at
// construction.
func NewVoterRegistry(entries map[string]string) (*VoterRegistry, error) {
	registry := &VoterRegistry{
		keys: make(map[string]*bls12381.G2Affine, len(entries)),
	}
	for poolHashHex, pubHex := range entries {
		poolHash, err := hex.DecodeString(poolHashHex)
		if err != nil {
			return nil, fmt.Errorf(
				"malformed voter pool key hash %q: %w",
				poolHashHex,
				err,
			)
		}
		if len(poolHash) != voterPoolKeyHashSize {
			return nil, fmt.Errorf(
				"malformed voter pool key hash %q: must be %d bytes, got %d",
				poolHashHex,
				voterPoolKeyHashSize,
				len(poolHash),
			)
		}
		pub, err := ParseVoterPublicKey(pubHex)
		if err != nil {
			return nil, fmt.Errorf(
				"voter public key for pool %q: %w",
				poolHashHex,
				err,
			)
		}
		canonical := hex.EncodeToString(poolHash)
		if _, exists := registry.keys[canonical]; exists {
			return nil, fmt.Errorf(
				"duplicate voter pool key hash %q after normalization",
				poolHashHex,
			)
		}
		registry.keys[canonical] = pub
	}
	return registry, nil
}

// PublicKeyFor returns the registered public key for a pool key hash.
func (r *VoterRegistry) PublicKeyFor(
	poolKeyHash []byte,
) (*bls12381.G2Affine, bool) {
	pub, ok := r.keys[hex.EncodeToString(poolKeyHash)]
	return pub, ok
}

// Size returns the number of registered voter public keys.
func (r *VoterRegistry) Size() int {
	return len(r.keys)
}
