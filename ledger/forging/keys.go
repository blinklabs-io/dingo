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
	"io"
	"math"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/blinklabs-io/bursa"
	"github.com/blinklabs-io/dingo/keystore"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/kes"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/gouroboros/vrf"
)

var (
	ErrVRFKeyHashMismatch          = errors.New("VRF key hash mismatch")
	errOpCertExpired               = errors.New("operational certificate expired")
	errCredentialGenerationChanged = errors.New(
		"credential generation changed during block production",
	)
)

const maxSecretKeyFileSize = 1 << 20

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
	// Protocol lifetime loaded from the Shelley genesis that validated the
	// operational certificate. This is distinct from the KES key's 2^depth
	// cryptographic capacity.
	maxKESEvolutions uint64
	opCertStartKES   uint64
	opCertExpiryKES  uint64
	opCertValidated  bool
	generation       uint64
	identitySet      bool
	identityPoolID   lcommon.PoolId
	identityVRFVKey  []byte

	mu    sync.RWMutex
	kesMu sync.RWMutex
}

// credentialGeneration is an independently owned snapshot of one complete
// credential and protocol-policy generation. It never aliases mutable secret
// material in PoolCredentials, so callbacks may reload or revalidate the owner
// without blocking an in-flight attempt or mixing key generations.
type credentialGeneration struct {
	owner            *PoolCredentials
	id               uint64
	loaded           bool
	vrfSKey          []byte
	vrfVerification  []byte
	kesSKey          *kes.SecretKey
	kesVerification  []byte
	operationalCert  *OpCert
	maxKESEvolutions uint64
	opCertStartKES   uint64
	opCertExpiryKES  uint64
	opCertValidated  bool
	releaseOnce      sync.Once
}

type loadedPoolCredentials struct {
	poolID  lcommon.PoolId
	vrfSKey []byte
	vrfVKey []byte
	kesSKey *kes.SecretKey
	kesVKey []byte
	opCert  *OpCert
}

// wipeCredentialBytes performs best-effort zeroization of independently owned
// VRF secret snapshots. runtime.KeepAlive prevents the compiler from proving
// the stores dead before the wipe completes.
//
//go:noinline
func wipeCredentialBytes(data []byte) {
	for i := range data {
		data[i] = 0
	}
	runtime.KeepAlive(data)
}

func cloneOpCert(opCert *OpCert) *OpCert {
	if opCert == nil {
		return nil
	}
	return &OpCert{
		KESVKey:     append([]byte(nil), opCert.KESVKey...),
		IssueNumber: opCert.IssueNumber,
		KESPeriod:   opCert.KESPeriod,
		Signature:   append([]byte(nil), opCert.Signature...),
		ColdVKey:    append([]byte(nil), opCert.ColdVKey...),
	}
}

func cloneKESSecretKey(key *kes.SecretKey) *kes.SecretKey {
	if key == nil {
		return nil
	}
	return &kes.SecretKey{
		Depth:  key.Depth,
		Period: key.Period,
		Data:   append([]byte(nil), key.Data...),
	}
}

func (loaded *loadedPoolCredentials) zeroize() {
	if loaded == nil {
		return
	}
	wipeCredentialBytes(loaded.vrfSKey)
	loaded.vrfSKey = nil
	if loaded.kesSKey != nil {
		loaded.kesSKey.Zeroize()
		loaded.kesSKey = nil
	}
	loaded.vrfVKey = nil
	loaded.kesVKey = nil
	loaded.opCert = nil
	loaded.poolID = lcommon.PoolId{}
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

// loadSecretKeyFromFile opens and checks a secret key before reading from the
// same handle, avoiding a TOCTOU race between the permission check and read.
func loadSecretKeyFromFile(path string) (*bursa.LoadedKey, error) {
	f, err := openSecretKeyFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open key file %q: %w", path, err)
	}
	defer f.Close() //nolint:errcheck // read-only handle

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat key file %q: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf(
			"key file %q is not a regular file (mode %s)",
			path, info.Mode(),
		)
	}
	if err := keystore.CheckOpenFilePermissions(f); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(f, maxSecretKeyFileSize+1))
	if err != nil {
		return nil, fmt.Errorf("failed to read key file %q: %w", path, err)
	}
	if len(data) > maxSecretKeyFileSize {
		return nil, fmt.Errorf(
			"key file %q exceeds maximum size of %d bytes",
			path, maxSecretKeyFileSize,
		)
	}
	key, err := bursa.LoadKeyFromBytes(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse key file %q: %w", path, err)
	}
	key.File = filepath.Base(path)
	return key, nil
}

func loadPoolCredentialsFromFiles(
	vrfSKeyPath string,
	kesSKeyPath string,
	opCertPath string,
) (_ *loadedPoolCredentials, retErr error) {
	loaded := &loadedPoolCredentials{}
	defer func() {
		if retErr != nil {
			loaded.zeroize()
		}
	}()

	// Load VRF signing key
	vrfKey, err := loadSecretKeyFromFile(vrfSKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load VRF signing key: %w", err)
	}
	loaded.vrfSKey = vrfKey.SKey
	if len(vrfKey.SKey) != vrf.SeedSize {
		return nil, fmt.Errorf(
			"invalid VRF key size: expected %d, got %d",
			vrf.SeedSize,
			len(vrfKey.SKey),
		)
	}
	derivedVRFVKey, derivedSeed, err := vrf.KeyGen(vrfKey.SKey)
	if len(derivedSeed) > 0 {
		wipeCredentialBytes(derivedSeed)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to derive VRF verification key: %w", err)
	}
	if len(vrfKey.VKey) > 0 &&
		(len(vrfKey.VKey) != vrf.PublicKeySize ||
			!bytes.Equal(vrfKey.VKey, derivedVRFVKey)) {
		return nil, errors.New(
			"VRF verification key mismatch: supplied key does not match signing seed",
		)
	}
	// Use only the identity derived from the signing seed. Bursa's parsed
	// verification key is an untrusted suffix in 64-byte cardano-cli files.
	loaded.vrfVKey = derivedVRFVKey

	// Load KES signing key
	kesKey, err := loadSecretKeyFromFile(kesSKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load KES signing key: %w", err)
	}
	loaded.kesSKey = &kes.SecretKey{
		Depth:  kes.CardanoKesDepth,
		Period: 0, // Will be updated during block production
		Data:   kesKey.SKey,
	}
	if len(kesKey.SKey) != kes.CardanoKesSecretKeySize {
		return nil, fmt.Errorf(
			"invalid KES key size: expected %d, got %d",
			kes.CardanoKesSecretKeySize,
			len(kesKey.SKey),
		)
	}
	loaded.kesVKey = kesKey.VKey

	// Load operational certificate
	opCertKey, err := bursa.LoadKeyFromFile(opCertPath)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to load operational certificate: %w",
			err,
		)
	}
	loaded.opCert = &OpCert{
		KESVKey:     opCertKey.VKey,
		IssueNumber: opCertKey.OpCertIssueNumber,
		KESPeriod:   opCertKey.OpCertKesPeriod,
		Signature:   opCertKey.OpCertSignature,
		ColdVKey:    opCertKey.OpCertColdVKey,
	}

	// Derive pool ID from cold verification key (Blake2b-224 hash)
	loaded.poolID = lcommon.PoolId(
		lcommon.Blake2b224Hash(loaded.opCert.ColdVKey),
	)

	// Validate that OpCert KES vkey matches the loaded KES key
	if !bytes.Equal(loaded.kesVKey, loaded.opCert.KESVKey) {
		return nil, errors.New(
			"KES verification key mismatch: loaded key does not match OpCert KES vkey",
		)
	}

	return loaded, nil
}

func (pc *PoolCredentials) clearUnsafe() {
	wipeCredentialBytes(pc.vrfSKey)
	if pc.kesSKey != nil {
		pc.kesSKey.Zeroize()
	}
	pc.poolID = lcommon.PoolId{}
	pc.vrfSKey = nil
	pc.vrfVKey = nil
	pc.kesSKey = nil
	pc.kesVKey = nil
	pc.opCert = nil
	pc.maxKESEvolutions = 0
	pc.opCertStartKES = 0
	pc.opCertExpiryKES = 0
	pc.opCertValidated = false
}

// LoadFromFiles loads all pool credentials from the specified file paths.
// Uses Bursa to parse cardano-cli format key files. The full loaded material
// replaces the prior generation atomically. A failed reload invalidates the
// prior generation so an active forger cannot continue with stale policy.
func (pc *PoolCredentials) LoadFromFiles(
	vrfSKeyPath string,
	kesSKeyPath string,
	opCertPath string,
) error {
	loaded, err := loadPoolCredentialsFromFiles(
		vrfSKeyPath,
		kesSKeyPath,
		opCertPath,
	)

	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.generation++
	if err != nil {
		pc.clearUnsafe()
		return err
	}
	if pc.identitySet &&
		(pc.identityPoolID != loaded.poolID ||
			!bytes.Equal(pc.identityVRFVKey, loaded.vrfVKey)) {
		pc.clearUnsafe()
		loaded.zeroize()
		return errors.New(
			"runtime credential reload cannot change pool or VRF identity",
		)
	}
	if !pc.identitySet {
		pc.identitySet = true
		pc.identityPoolID = loaded.poolID
		pc.identityVRFVKey = append([]byte(nil), loaded.vrfVKey...)
	}
	pc.clearUnsafe()
	pc.poolID = loaded.poolID
	pc.vrfSKey = loaded.vrfSKey
	pc.vrfVKey = loaded.vrfVKey
	pc.kesSKey = loaded.kesSKey
	pc.kesVKey = loaded.kesVKey
	pc.opCert = loaded.opCert
	pc.maxKESEvolutions = 0
	pc.opCertStartKES = loaded.opCert.KESPeriod
	pc.opCertExpiryKES = 0
	pc.opCertValidated = false
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
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.updateKESPeriodUnsafe(period)
}

func (pc *PoolCredentials) updateKESPeriodUnsafe(period uint64) error {
	pc.kesMu.Lock()
	defer pc.kesMu.Unlock()

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

	// kes.Update consumes its input key on success. Install each successor
	// before the next update so a later failure cannot leave pc.kesSKey
	// pointing at an erased predecessor.
	for pc.kesSKey.Period < targetPeriod {
		newKey, err := kes.Update(pc.kesSKey)
		if err != nil {
			return fmt.Errorf(
				"failed to update KES key to period %d (absolute %d): %w",
				targetPeriod,
				period,
				err,
			)
		}
		pc.kesSKey = newKey
	}

	return nil
}

// VRFProve generates a VRF proof for leader election.
// alpha should be MkInputVrf(slot, epochNonce).
func (pc *PoolCredentials) VRFProve(alpha []byte) ([]byte, []byte, error) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.vrfProveUnsafe(alpha)
}

func (pc *PoolCredentials) vrfProveUnsafe(
	alpha []byte,
) ([]byte, []byte, error) {
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
func (pc *PoolCredentials) KESSign(
	period uint64,
	message []byte,
) ([]byte, error) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.kesSignUnsafe(period, message)
}

func (pc *PoolCredentials) kesSignUnsafe(
	period uint64,
	message []byte,
) ([]byte, error) {
	pc.kesMu.RLock()
	defer pc.kesMu.RUnlock()

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
	return pc.getOpCertUnsafe()
}

func (pc *PoolCredentials) getOpCertUnsafe() *OpCert {
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
	pc.kesMu.RLock()
	defer pc.kesMu.RUnlock()
	if pc.kesSKey == nil {
		return 0
	}
	return pc.kesSKey.Period
}

// IsLoaded returns true if all credentials have been loaded.
func (pc *PoolCredentials) IsLoaded() bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	pc.kesMu.RLock()
	defer pc.kesMu.RUnlock()
	return pc.isLoadedUnsafe()
}

func (pc *PoolCredentials) isLoadedUnsafe() bool {
	return pc.vrfSKey != nil && pc.kesSKey != nil && pc.opCert != nil
}

func (pc *PoolCredentials) acquireCredentialGeneration() *credentialGeneration {
	pc.mu.RLock()
	pc.kesMu.RLock()
	generation := &credentialGeneration{
		owner:            pc,
		id:               pc.generation,
		loaded:           pc.isLoadedUnsafe(),
		vrfSKey:          append([]byte(nil), pc.vrfSKey...),
		vrfVerification:  append([]byte(nil), pc.vrfVKey...),
		kesSKey:          cloneKESSecretKey(pc.kesSKey),
		kesVerification:  append([]byte(nil), pc.kesVKey...),
		operationalCert:  cloneOpCert(pc.opCert),
		maxKESEvolutions: pc.maxKESEvolutions,
		opCertStartKES:   pc.opCertStartKES,
		opCertExpiryKES:  pc.opCertExpiryKES,
		opCertValidated:  pc.opCertValidated,
	}
	pc.kesMu.RUnlock()
	pc.mu.RUnlock()
	return generation
}

func (g *credentialGeneration) release() {
	if g == nil {
		return
	}
	g.releaseOnce.Do(func() {
		wipeCredentialBytes(g.vrfSKey)
		g.vrfSKey = nil
		if g.kesSKey != nil {
			g.kesSKey.Zeroize()
			g.kesSKey = nil
		}
	})
}

func (g *credentialGeneration) ensureCurrent() error {
	g.owner.mu.RLock()
	defer g.owner.mu.RUnlock()
	if g.owner.generation != g.id {
		return fmt.Errorf(
			"%w: selected %d, current %d",
			errCredentialGenerationChanged,
			g.id,
			g.owner.generation,
		)
	}
	return nil
}

func (g *credentialGeneration) validatedKESProtocolLifetime() (
	uint64,
	uint64,
	uint64,
	error,
) {
	if !g.loaded {
		return 0, 0, 0, errors.New("credentials not loaded")
	}
	if !g.opCertValidated {
		return 0, 0, 0, errors.New(
			"operational certificate is not validated",
		)
	}
	if g.maxKESEvolutions == 0 || g.opCertExpiryKES == 0 {
		return 0, 0, 0, errors.New("KES protocol lifetime is not validated")
	}
	return g.opCertStartKES,
		g.maxKESEvolutions,
		g.opCertExpiryKES,
		nil
}

func (g *credentialGeneration) validateKESPeriod(period uint64) error {
	start, maxEvolutions, expiry, err := g.validatedKESProtocolLifetime()
	if err != nil {
		return err
	}
	if period < start {
		return fmt.Errorf(
			"operational certificate is not valid before KES period %d (current %d)",
			start,
			period,
		)
	}
	if period >= expiry {
		return fmt.Errorf(
			"%w: operational certificate expired at KES period %d (current %d, max evolutions %d)",
			errOpCertExpired,
			expiry,
			period,
			maxEvolutions,
		)
	}
	return nil
}

func (g *credentialGeneration) periodsRemaining(currentPeriod uint64) uint64 {
	if currentPeriod < g.opCertStartKES ||
		currentPeriod >= g.opCertExpiryKES ||
		g.maxKESEvolutions == 0 {
		return 0
	}
	return g.opCertExpiryKES - currentPeriod
}

func (g *credentialGeneration) updateKESPeriod(period uint64) error {
	if err := g.owner.updateKESPeriodForGeneration(g.id, period); err != nil {
		return err
	}
	if g.kesSKey == nil {
		return errors.New("KES key not loaded")
	}
	if period < g.opCertStartKES {
		return fmt.Errorf(
			"current KES period %d is before opcert start period %d",
			period,
			g.opCertStartKES,
		)
	}
	targetPeriod := period - g.opCertStartKES
	if targetPeriod < g.kesSKey.Period {
		return fmt.Errorf(
			"cannot evolve KES snapshot backward: current period %d, requested %d (absolute %d)",
			g.kesSKey.Period,
			targetPeriod,
			period,
		)
	}
	for g.kesSKey.Period < targetPeriod {
		newKey, err := kes.Update(g.kesSKey)
		if err != nil {
			return fmt.Errorf(
				"failed to update KES snapshot to period %d (absolute %d): %w",
				targetPeriod,
				period,
				err,
			)
		}
		g.kesSKey = newKey
	}
	return nil
}

func (pc *PoolCredentials) updateKESPeriodForGeneration(
	generation uint64,
	period uint64,
) error {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	if pc.generation != generation {
		return fmt.Errorf(
			"%w: selected %d, current %d",
			errCredentialGenerationChanged,
			generation,
			pc.generation,
		)
	}
	return pc.updateKESPeriodUnsafe(period)
}

func (g *credentialGeneration) vrfVKey() []byte {
	if g.vrfVerification == nil {
		return nil
	}
	return append([]byte(nil), g.vrfVerification...)
}

func (g *credentialGeneration) vrfProve(
	alpha []byte,
) ([]byte, []byte, error) {
	if g.vrfSKey == nil {
		return nil, nil, errors.New("VRF key not loaded")
	}
	proof, output, err := vrf.Prove(g.vrfSKey, alpha)
	if err != nil {
		return nil, nil, fmt.Errorf("VRFProve: %w", err)
	}
	return proof, output, nil
}

func (g *credentialGeneration) opCert() *OpCert {
	return cloneOpCert(g.operationalCert)
}

func (g *credentialGeneration) kesSign(
	period uint64,
	message []byte,
) ([]byte, error) {
	if g.kesSKey == nil {
		return nil, errors.New("KES key not loaded")
	}
	if period < g.opCertStartKES {
		return nil, fmt.Errorf(
			"failed to compute signing KES period for absolute period %d: current KES period %d is before opcert start period %d",
			period,
			period,
			g.opCertStartKES,
		)
	}
	relativePeriod := period - g.opCertStartKES
	sig, err := kes.Sign(g.kesSKey, relativePeriod, message)
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

// ValidateOpCert validates that the operational certificate matches the KES key
// and that the cold key signature over the certificate body is valid.
func (pc *PoolCredentials) ValidateOpCert() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.generation++
	previousStart := pc.opCertStartKES
	previousMaxEvolutions := pc.maxKESEvolutions
	previousExpiry := pc.opCertExpiryKES
	hadValidatedLifetime := pc.opCertValidated &&
		previousMaxEvolutions > 0 && previousExpiry > previousStart
	pc.opCertValidated = false
	pc.maxKESEvolutions = 0
	pc.opCertExpiryKES = 0

	if err := pc.validateOpCertUnsafe(); err != nil {
		return err
	}
	pc.opCertValidated = true
	if hadValidatedLifetime && pc.opCert.KESPeriod == previousStart {
		pc.maxKESEvolutions = previousMaxEvolutions
		pc.opCertExpiryKES = previousExpiry
	}
	return nil
}

func (pc *PoolCredentials) validateOpCertUnsafe() error {
	if pc.opCert == nil {
		return errors.New("operational certificate not loaded")
	}
	if pc.kesVKey == nil {
		return errors.New("KES verification key not loaded")
	}

	// Verify KES public key matches OpCert's KES vkey
	if !bytes.Equal(pc.kesVKey, pc.opCert.KESVKey) {
		return errors.New(
			"KES verification key mismatch: loaded key does not match OpCert.KESVKey",
		)
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
		return errors.New(
			"OpCert signature verification failed: cold key signature is invalid",
		)
	}

	return nil
}

func validateMaxKESEvolutions(maxEvolutions uint64) error {
	if maxEvolutions == 0 {
		return errors.New("max KES evolutions must be positive")
	}
	capacity := kes.MaxPeriod(kes.CardanoKesDepth)
	if maxEvolutions > capacity {
		return fmt.Errorf(
			"max KES evolutions %d exceeds KES key capacity %d",
			maxEvolutions,
			capacity,
		)
	}
	return nil
}

func opCertExpiryPeriod(
	startPeriod uint64,
	maxEvolutions uint64,
) (uint64, error) {
	if err := validateMaxKESEvolutions(maxEvolutions); err != nil {
		return 0, err
	}
	if startPeriod > math.MaxUint64-maxEvolutions {
		return 0, fmt.Errorf(
			"opcert expiry overflows uint64: start period %d, max evolutions %d",
			startPeriod,
			maxEvolutions,
		)
	}
	return startPeriod + maxEvolutions, nil
}

func (pc *PoolCredentials) opCertExpiryPeriodUnsafe() (uint64, error) {
	if pc.opCert == nil {
		return 0, errors.New("operational certificate not loaded")
	}
	if !pc.opCertValidated ||
		pc.maxKESEvolutions == 0 ||
		pc.opCertExpiryKES == 0 {
		return 0, errors.New("KES protocol lifetime is not validated")
	}
	return pc.opCertExpiryKES, nil
}

// OpCertExpiryPeriod returns the protocol KES period at which the OpCert
// expires. ValidateKESPeriod must first load MaxKESEvolutions from Shelley
// genesis. It returns zero when no validated protocol lifetime is available.
func (pc *PoolCredentials) OpCertExpiryPeriod() uint64 {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	expiryPeriod, err := pc.opCertExpiryPeriodUnsafe()
	if err != nil {
		return 0
	}
	return expiryPeriod
}

// PeriodsRemaining returns how many protocol KES periods remain before
// expiry. It returns zero before the OpCert start, at or after expiry, or when
// no validated protocol lifetime is available.
func (pc *PoolCredentials) PeriodsRemaining(currentPeriod uint64) uint64 {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	expiryPeriod, err := pc.opCertExpiryPeriodUnsafe()
	if err != nil || currentPeriod < pc.opCertStartKES {
		return 0
	}
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
// an opcert stops being valid. A successful result retains that protocol
// lifetime for runtime forging checks and operational metrics.
func (pc *PoolCredentials) ValidateKESPeriod(
	genesis *shelley.ShelleyGenesis,
	currentSlot uint64,
) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.generation++
	previousOpCertValidation := pc.opCertValidated
	// Any failed validation leaves the credentials unusable for production
	// forging rather than retaining a policy from an earlier genesis.
	pc.maxKESEvolutions = 0
	pc.opCertExpiryKES = 0
	pc.opCertValidated = false

	if pc.opCert == nil {
		pc.opCertStartKES = 0
		return errors.New("operational certificate not loaded")
	}
	pc.opCertStartKES = pc.opCert.KESPeriod
	if pc.isLoadedUnsafe() {
		if err := pc.validateOpCertUnsafe(); err != nil {
			return fmt.Errorf("validate operational certificate: %w", err)
		}
		pc.opCertValidated = true
	} else {
		// Preserve an explicit ValidateOpCert result for focused callers that
		// validate certificate metadata without loading signing material.
		pc.opCertValidated = previousOpCertValidation
	}
	if genesis == nil {
		return errors.New("shelley genesis is required")
	}
	current, err := CurrentKESPeriodFromGenesis(genesis, currentSlot)
	if err != nil {
		return err
	}
	if genesis.MaxKESEvolutions <= 0 {
		return fmt.Errorf(
			"genesis maxKESEvolutions must be positive, got %d",
			genesis.MaxKESEvolutions,
		)
	}
	// #nosec G115 -- guarded positive above; int fits within uint64.
	maxEvolutions := uint64(genesis.MaxKESEvolutions)
	expiryPeriod, err := opCertExpiryPeriod(
		pc.opCert.KESPeriod,
		maxEvolutions,
	)
	if err != nil {
		return err
	}
	if pc.opCert.KESPeriod > current {
		return fmt.Errorf(
			"opcert KES period %d is in the future (current %d)",
			pc.opCert.KESPeriod, current,
		)
	}
	if current >= expiryPeriod {
		return fmt.Errorf(
			"%w: opcert KES period %d expired at period %d (current %d, max evolutions %d); rotate the operational certificate",
			errOpCertExpired,
			pc.opCert.KESPeriod,
			expiryPeriod,
			current,
			maxEvolutions,
		)
	}
	pc.maxKESEvolutions = maxEvolutions
	pc.opCertExpiryKES = expiryPeriod
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
	PoolRegistrationVRFKeyHash(
		poolID [28]byte,
	) (vrfKeyHash [32]byte, found bool, err error)
	// LatestOpCertSequence returns the highest opcert IssueNumber
	// observed on chain for poolID. found is false when on-chain
	// counter tracking is not implemented or this pool has never
	// minted a block.
	LatestOpCertSequence(
		poolID [28]byte,
	) (sequence uint64, found bool, err error)
}

// ValidateAgainstLedger cross-checks the loaded credentials against
// ledger state once it is available. It is best-effort: a missing pool
// registration is not fatal because operators commonly stage their keys
// before submitting the registration certificate.
//
// The opcert counter check here is staleness-only (candidate below the
// last observed value), not the full era-scoped no-gap rule the forge
// loop's checkOpCertSequence and block application enforce. Startup
// cannot apply that rule safely: the era for "now" would have to come
// from a wall-clock slot, while the observed baseline
// (LatestOpCertSequence) only reflects the applied chain, and those two
// can disagree on a node whose applied tip is behind wall-clock time (an
// interrupted sync, a resume after downtime, a restore to an older
// snapshot) -- a pool several rotations into its life would look gapped
// against a baseline that just hasn't caught up yet, and refusing
// startup for it would prevent the node from ever syncing to the point
// that makes the baseline correct. The forge loop's own gate does not
// have this problem, since it runs near the chain tip.
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
	// enforceNoGap is always false here; see the doc comment above for why
	// startup cannot safely apply the era-scoped no-gap half of this rule.
	// validateOpCertSequence rather than eras.ValidateOpCertCounter so this
	// check and the forge loop's share the persistable-counter bound as well
	// as the era rule.
	if seqErr := validateOpCertSequence(
		latestSeq,
		seqFound,
		pc.opCert.IssueNumber,
		false,
	); seqErr != nil {
		return true, vrfMatched, fmt.Errorf(
			"opcert sequence %d invalid: %w",
			pc.opCert.IssueNumber,
			seqErr,
		)
	}
	return true, vrfMatched, nil
}

// validateOpCertSequence enforces the same operational-certificate counter
// rule block application applies (ledger/verify_opcert.go
// validateOpCertCounter), so the forge loop can decline a leader slot for a
// key state ledgerProcessBlock would reject, before spending the slot on
// VRF/KES/Leios work or committing the duplicate-slot fence. The two must
// stay in agreement: a candidate this accepts and block application rejects
// wastes a leader slot; the reverse blocks a slot the chain would have
// adopted. The rule itself lives in ledger/eras.ValidateOpCertCounter, the
// single source both call sites share, so it cannot drift between them.
//
// The persistable-counter bound is applied alongside it. A candidate above it
// is a counter the chain accepts and this node cannot record, so declining the
// leader slot is the same disposition block application takes for an inbound
// block carrying one -- and it is taken before the slot is spent rather than
// after the block is built.
func validateOpCertSequence(
	stored uint64,
	found bool,
	candidate uint64,
	enforceNoGap bool,
) error {
	if err := eras.ValidateOpCertPersistableCounter(candidate); err != nil {
		return err
	}
	return eras.ValidateOpCertCounter(stored, found, candidate, enforceNoGap)
}
