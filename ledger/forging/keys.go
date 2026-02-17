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
	"github.com/blinklabs-io/gouroboros/vrf"
)

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

// UpdateKESPeriod updates the KES key to the specified period.
// This should be called when the KES period advances.
func (pc *PoolCredentials) UpdateKESPeriod(period uint64) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.kesSKey == nil {
		return errors.New("KES key not loaded")
	}

	if period < pc.kesSKey.Period {
		return fmt.Errorf(
			"cannot evolve KES key backward: current period %d, requested %d",
			pc.kesSKey.Period,
			period,
		)
	}

	// Evolve KES key to the target period. kes.Update returns a new SecretKey
	// with a deep copy of the data, so pc.kesSKey is only replaced on success.
	evolvedKey := pc.kesSKey
	for evolvedKey.Period < period {
		newKey, err := kes.Update(evolvedKey)
		if err != nil {
			return fmt.Errorf(
				"failed to update KES key to period %d: %w",
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

// KESSign signs a message with the KES key at the specified period.
//
// IMPORTANT: Callers must ensure UpdateKESPeriod(period) was called before KESSign
// to evolve the key to the correct period. The kes.Sign function expects the key
// to already be at the requested period.
func (pc *PoolCredentials) KESSign(period uint64, message []byte) ([]byte, error) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	if pc.kesSKey == nil {
		return nil, errors.New("KES key not loaded")
	}

	sig, err := kes.Sign(pc.kesSKey, period, message)
	if err != nil {
		return nil, fmt.Errorf("KESSign period %d: %w", period, err)
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
