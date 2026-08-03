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

// Package forging provides block production functionality for Cardano SPOs.
package forging

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/blinklabs-io/bursa"
	"github.com/blinklabs-io/gouroboros/kes"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/gouroboros/vrf"
)

var ErrVRFKeyHashMismatch = errors.New("VRF key hash mismatch")

// PoolCredentials holds the cryptographic keys required for block production.
// All keys are loaded using Bursa from standard cardano-cli format files.
// Fields are unexported to enforce thread-safe access via the mutex.
type PoolCredentials struct {
	// Pool identification
	poolID lcommon.PoolId // Blake2b-224 hash of cold verification key

	// VRF key pair for leader election
	vrfSKey []byte // 32-byte VRF secret key (seed)
	vrfVKey []byte // 32-byte VRF verification key

	// KES key pair for block signing
	kesSKey *kes.SecretKey // KES secret key (608 bytes for depth 6)
	kesVKey []byte         // 32-byte KES verification key

	// Operational certificate linking KES to pool cold key
	opCert *OpCert

	mu sync.RWMutex
}

// OpCert represents an operational certificate that binds a KES key to a pool.
type OpCert struct {
	KESVKey     []byte // KES verification key (32 bytes)
	IssueNumber uint64 // Certificate sequence number
	KESPeriod   uint64 // KES period when certificate was created
	Signature   []byte // Cold key signature (64 bytes)
	ColdVKey    []byte // Cold verification key (32 bytes)
}

// NewPoolCredentials creates an empty PoolCredentials instance.
func NewPoolCredentials() *PoolCredentials {
	return &PoolCredentials{}
}

// LoadFromFiles loads all pool credentials from the specified file paths.
// Uses Bursa to parse cardano-cli format key files.
func (pc *PoolCredentials) LoadFromFiles(
	vrfSKeyPath string,
	kesSKeyPath string,
	opCertPath string,
) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	// Load VRF signing key
	vrfKey, err := bursa.LoadKeyFromFile(vrfSKeyPath)
	if err != nil {
		return fmt.Errorf("failed to load VRF signing key: %w", err)
	}
	if len(vrfKey.SKey) != vrf.SeedSize {
		return fmt.Errorf(
			"invalid VRF key size: expected %d, got %d",
			vrf.SeedSize,
			len(vrfKey.SKey),
		)
	}
	pc.vrfSKey = vrfKey.SKey
	pc.vrfVKey = vrfKey.VKey

	// Load KES signing key
	kesKey, err := bursa.LoadKeyFromFile(kesSKeyPath)
	if err != nil {
		return fmt.Errorf("failed to load KES signing key: %w", err)
	}
	if len(kesKey.SKey) != kes.CardanoKesSecretKeySize {
		return fmt.Errorf(
			"invalid KES key size: expected %d, got %d",
			kes.CardanoKesSecretKeySize,
			len(kesKey.SKey),
		)
	}
	pc.kesSKey = &kes.SecretKey{
		Depth:  kes.CardanoKesDepth,
		Period: 0, // Will be updated during block production
		Data:   kesKey.SKey,
	}
	pc.kesVKey = kesKey.VKey

	// Load operational certificate
	opCertKey, err := bursa.LoadKeyFromFile(opCertPath)
	if err != nil {
		return fmt.Errorf("failed to load operational certificate: %w", err)
	}
	pc.opCert = &OpCert{
		KESVKey:     opCertKey.VKey,
		IssueNumber: opCertKey.OpCertIssueNumber,
		KESPeriod:   opCertKey.OpCertKesPeriod,
		Signature:   opCertKey.OpCertSignature,
		ColdVKey:    opCertKey.OpCertColdVKey,
	}

	// Derive pool ID from cold verification key (Blake2b-224 hash)
	pc.poolID = lcommon.PoolId(lcommon.Blake2b224Hash(pc.opCert.ColdVKey))

	// Validate that OpCert KES vkey matches the loaded KES key
	if !bytes.Equal(pc.kesVKey, pc.opCert.KESVKey) {
		return errors.New(
			"KES verification key mismatch: loaded key does not match OpCert KES vkey",
		)
	}

	return nil
}

func (pc *PoolCredentials) relativeKESPeriodUnsafe(
	absolutePeriod uint64,
) (uint64, error) {
	if pc.opCert == nil {
		return absolutePeriod, nil
	}
	if absolutePeriod < pc.opCert.KESPeriod {
		return 0, fmt.Errorf(
			"current KES period %d is before opcert start period %d",
			absolutePeriod,
			pc.opCert.KESPeriod,
		)
	}
	return absolutePeriod - pc.opCert.KESPeriod, nil
}

// UpdateKESPeriod evolves the KES key to the specified ABSOLUTE period.
// The secret key itself tracks the relative period within the opcert window,
// so we translate chain KES periods by subtracting the opcert start period
// when an opcert is loaded.
func (pc *PoolCredentials) UpdateKESPeriod(period uint64) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.kesSKey == nil {
		return errors.New("KES key not loaded")
	}

	targetPeriod, err := pc.relativeKESPeriodUnsafe(period)
	if err != nil {
		return fmt.Errorf(
			"failed to compute target KES period for absolute period %d: %w",
			period,
			err,
		)
	}

	if targetPeriod < pc.kesSKey.Period {
		return fmt.Errorf(
			"cannot evolve KES key backward: current period %d, requested %d (absolute %d)",
			pc.kesSKey.Period,
			targetPeriod,
			period,
		)
	}

	// Evolve KES key to the target period. kes.Update returns a new SecretKey
	// with a deep copy of the data, so pc.kesSKey is only replaced on success.
	evolvedKey := pc.kesSKey
	for evolvedKey.Period < targetPeriod {
		newKey, err := kes.Update(evolvedKey)
		if err != nil {
			return fmt.Errorf(
				"failed to update KES key to period %d (absolute %d): %w",
				targetPeriod,
				period,
				err,
			)
		}
		evolvedKey = newKey
	}
	pc.kesSKey = evolvedKey

	return nil
}

// VRFProve generates a VRF proof for leader election.
// alpha should be MkInputVrf(slot, epochNonce).
func (pc *PoolCredentials) VRFProve(alpha []byte) ([]byte, []byte, error) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	if pc.vrfSKey == nil {
		return nil, nil, errors.New("VRF key not loaded")
	}

	proof, output, err := vrf.Prove(pc.vrfSKey, alpha)
	if err != nil {
		return nil, nil, fmt.Errorf("VRFProve: %w", err)
	}
	return proof, output, nil
}

// KESSign signs a message with the KES key at the specified ABSOLUTE period.
//
// IMPORTANT: Callers must ensure UpdateKESPeriod(period) was called before KESSign
// to evolve the key to the correct period. The kes.Sign function expects the key
// to already be at the relative period within the opcert window when an opcert
// is loaded.
func (pc *PoolCredentials) KESSign(period uint64, message []byte) ([]byte, error) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	if pc.kesSKey == nil {
		return nil, errors.New("KES key not loaded")
	}

	relativePeriod, err := pc.relativeKESPeriodUnsafe(period)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to compute signing KES period for absolute period %d: %w",
			period,
			err,
		)
	}

	sig, err := kes.Sign(pc.kesSKey, relativePeriod, message)
	if err != nil {
		return nil, fmt.Errorf(
			"KESSign relative period %d (absolute %d): %w",
			relativePeriod,
			period,
			err,
		)
	}
	return sig, nil
}

// GetVRFSKey returns a copy of the VRF secret key (seed).
func (pc *PoolCredentials) GetVRFSKey() []byte {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	if pc.vrfSKey == nil {
		return nil
	}
	return append([]byte(nil), pc.vrfSKey...)
}

// GetVRFVKey returns a copy of the VRF verification key.
func (pc *PoolCredentials) GetVRFVKey() []byte {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	if pc.vrfVKey == nil {
		return nil
	}
	return append([]byte(nil), pc.vrfVKey...)
}

// GetKESVKey returns a copy of the KES verification key.
func (pc *PoolCredentials) GetKESVKey() []byte {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	if pc.kesVKey == nil {
		return nil
	}
	return append([]byte(nil), pc.kesVKey...)
}

// GetPoolID returns the pool ID (Blake2b-224 of cold vkey).
func (pc *PoolCredentials) GetPoolID() lcommon.PoolId {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.poolID
}

// GetOpCert returns a copy of the operational certificate.
// Returns nil if no certificate is loaded.
func (pc *PoolCredentials) GetOpCert() *OpCert {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	if pc.opCert == nil {
		return nil
	}

	// Return a defensive copy to prevent modification of internal state
	return &OpCert{
		KESVKey:     append([]byte(nil), pc.opCert.KESVKey...),
		IssueNumber: pc.opCert.IssueNumber,
		KESPeriod:   pc.opCert.KESPeriod,
		Signature:   append([]byte(nil), pc.opCert.Signature...),
		ColdVKey:    append([]byte(nil), pc.opCert.ColdVKey...),
	}
}

// GetKESPeriod returns the current KES period of the loaded key.
// Returns 0 if the KES key is not loaded.
func (pc *PoolCredentials) GetKESPeriod() uint64 {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	if pc.kesSKey == nil {
		return 0
	}
	return pc.kesSKey.Period
}

// IsLoaded returns true if all credentials have been loaded.
func (pc *PoolCredentials) IsLoaded() bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.vrfSKey != nil && pc.kesSKey != nil && pc.opCert != nil
}

// ValidateOpCert validates that the operational certificate matches the KES key
// and that the cold key signature over the certificate body is valid.
func (pc *PoolCredentials) ValidateOpCert() error {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	if pc.opCert == nil {
		return errors.New("operational certificate not loaded")
	}
	if pc.kesVKey == nil {
		return errors.New("KES verification key not loaded")
	}

	// Verify KES public key matches OpCert's KES vkey
	if !bytes.Equal(pc.kesVKey, pc.opCert.KESVKey) {
		return errors.New("KES verification key mismatch: loaded key does not match OpCert.KESVKey")
	}

	// Verify cold key signature over the raw signable representation:
	//   KES vkey (32 bytes) || issue number (8 bytes BE) || KES period (8 bytes BE)
	// See: cardano-ledger OCertSignable.getSignableRepresentation
	if len(pc.opCert.ColdVKey) != ed25519.PublicKeySize {
		return fmt.Errorf(
			"invalid cold verification key size: expected %d, got %d",
			ed25519.PublicKeySize,
			len(pc.opCert.ColdVKey),
		)
	}
	var certBody [48]byte
	copy(certBody[:32], pc.opCert.KESVKey)
	binary.BigEndian.PutUint64(certBody[32:40], pc.opCert.IssueNumber)
	binary.BigEndian.PutUint64(certBody[40:48], pc.opCert.KESPeriod)
	if !ed25519.Verify(pc.opCert.ColdVKey, certBody[:], pc.opCert.Signature) {
		return errors.New("OpCert signature verification failed: cold key signature is invalid")
	}

	return nil
}

// OpCertExpiryPeriod returns the KES period at which the OpCert expires.
// For depth 6, max periods = 2^6 = 64, so expiry = startPeriod + 64.
func (pc *PoolCredentials) OpCertExpiryPeriod() uint64 {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	if pc.opCert == nil {
		return 0
	}
	return pc.opCert.KESPeriod + kes.MaxPeriod(kes.CardanoKesDepth)
}

// PeriodsRemaining returns how many KES periods remain before expiry.
func (pc *PoolCredentials) PeriodsRemaining(currentPeriod uint64) uint64 {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	if pc.opCert == nil {
		return 0
	}
	expiryPeriod := pc.opCert.KESPeriod + kes.MaxPeriod(kes.CardanoKesDepth)
	if currentPeriod >= expiryPeriod {
		return 0
	}
	return expiryPeriod - currentPeriod
}

// ValidateKESPeriod checks that the loaded operational certificate's KES
// period is plausible at currentSlot, given the chain's Shelley genesis. A
// non-nil result means the node should refuse to start: either the opcert
// claims a period that hasn't started yet (rotated key staged too early, or
// wrong network) or the opcert has expired and needs to be rotated.
//
// The protocol-level expiry uses MaxKESEvolutions from genesis rather
// than the raw 2^depth ceiling, so this matches the chain's view of when
// an opcert stops being valid.
func (pc *PoolCredentials) ValidateKESPeriod(
	genesis *shelley.ShelleyGenesis,
	currentSlot uint64,
) error {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	if pc.opCert == nil {
		return errors.New("operational certificate not loaded")
	}
	if genesis == nil {
		return errors.New("shelley genesis is required")
	}
	current, err := CurrentKESPeriodFromGenesis(genesis, currentSlot)
	if err != nil {
		return err
	}
	if pc.opCert.KESPeriod > current {
		return fmt.Errorf(
			"opcert KES period %d is in the future (current %d)",
			pc.opCert.KESPeriod, current,
		)
	}
	maxEvolutions := uint64(genesis.MaxKESEvolutions) // #nosec G115
	if maxEvolutions > 0 &&
		current >= pc.opCert.KESPeriod+maxEvolutions {
		return fmt.Errorf(
			"opcert KES period %d has expired (current %d, max evolutions %d); rotate the operational certificate",
			pc.opCert.KESPeriod, current, maxEvolutions,
		)
	}
	return nil
}

// LedgerView is the subset of ledger state the post-startup credential
// cross-check needs. The forging package depends on it as a small
// interface so the package itself stays free of a ledger dependency, and
// tests can drive the logic with a fake.
type LedgerView interface {
	// PoolRegistrationVRFKeyHash returns the VRF key hash recorded on
	// the most recent active pool registration certificate for poolID.
	// found is false when the pool has no on-chain registration yet.
	PoolRegistrationVRFKeyHash(poolID [28]byte) (vrfKeyHash [32]byte, found bool, err error)
	// LatestOpCertSequence returns the highest opcert IssueNumber
	// observed on chain for poolID. found is false when on-chain
	// counter tracking is not implemented or this pool has never
	// minted a block.
	LatestOpCertSequence(poolID [28]byte) (sequence uint64, found bool, err error)
}

// ValidateAgainstLedger cross-checks the loaded credentials against
// ledger state once it is available. It is best-effort: a missing pool
// registration is not fatal because operators commonly stage their keys
// before submitting the registration certificate.
//
// Three return values describe the outcome:
//   - registered: true if the pool registration was found on chain.
//   - vrfMatched: true if registered AND the on-chain VRF key hash
//     matched our loaded VRF verification key. False otherwise (also
//     false when registered is false or the VRF verification key is
//     unavailable, e.g. for a seed-only VRF skey).
//   - err: a non-nil error means the ledger view disagrees with the
//     loaded credentials. Normal networks refuse startup for these;
//     devnet callers may choose to warn on ErrVRFKeyHashMismatch.
func (pc *PoolCredentials) ValidateAgainstLedger(
	view LedgerView,
) (registered, vrfMatched bool, err error) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	if view == nil {
		return false, false, errors.New("ledger view is nil")
	}
	if pc.opCert == nil {
		return false, false, errors.New("operational certificate not loaded")
	}
	var poolID [28]byte
	copy(poolID[:], pc.poolID[:])

	regVRF, found, err := view.PoolRegistrationVRFKeyHash(poolID)
	if err != nil {
		return false, false, fmt.Errorf("pool registration lookup: %w", err)
	}
	if !found {
		return false, false, nil
	}
	if regVRF == ([32]byte{}) {
		// Fresh devnets and partially bootstrapped ledgers can expose a
		// placeholder pool row before a trustworthy VRF hash is available.
		// Treat that like "not registered yet" so startup can continue.
		return false, false, nil
	}
	if pc.vrfVKey != nil {
		ourVRF := lcommon.Blake2b256Hash(pc.vrfVKey)
		if ourVRF != regVRF {
			return true, false, fmt.Errorf(
				"%w: pool registration has %x but loaded VRF key hashes to %x",
				ErrVRFKeyHashMismatch,
				regVRF, ourVRF,
			)
		}
		vrfMatched = true
	}
	latestSeq, seqFound, err := view.LatestOpCertSequence(poolID)
	if err != nil {
		return true, vrfMatched, fmt.Errorf("opcert sequence lookup: %w", err)
	}
	if seqFound && pc.opCert.IssueNumber < latestSeq {
		return true, vrfMatched, fmt.Errorf(
			"opcert sequence %d is stale: ledger has observed %d for this pool",
			pc.opCert.IssueNumber, latestSeq,
		)
	}
	return true, vrfMatched, nil
}
