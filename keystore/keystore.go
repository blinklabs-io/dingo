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

// Package keystore provides key management for Cardano stake pool operators.
// It wraps pool credentials (VRF, KES, OpCert) and handles KES key evolution
// as the blockchain progresses through KES periods.
package keystore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/blinklabs-io/gouroboros/kes"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/vrf"
)

// Common errors returned by KeyStore operations.
var (
	ErrKeysNotLoaded    = errors.New("keys not loaded")
	ErrVRFKeyNotLoaded  = errors.New("VRF key not loaded")
	ErrKESKeyNotLoaded  = errors.New("KES key not loaded")
	ErrOpCertNotLoaded  = errors.New("operational certificate not loaded")
	ErrAlreadyRunning   = errors.New("keystore already running")
	ErrNotRunning       = errors.New("keystore not running")
	ErrInsecureFileMode = errors.New("insecure file permissions")
)

// OpCert represents an operational certificate that binds a KES key to a pool.
type OpCert struct {
	KESVKey     []byte // KES verification key (32 bytes)
	IssueNumber uint64 // Certificate sequence number
	KESPeriod   uint64 // KES period when certificate was created
	Signature   []byte // Cold key signature (64 bytes)
	ColdVKey    []byte // Cold verification key (32 bytes)
}

// VRFSigner provides VRF signing capabilities for leader election.
type VRFSigner interface {
	// Prove generates a VRF proof for the given input.
	// Returns (proof, output, error).
	Prove(alpha []byte) ([]byte, []byte, error)
	// VKey returns the VRF verification key.
	VKey() []byte
}

// KESSigner provides KES signing capabilities for block production.
type KESSigner interface {
	// Sign signs a message at the specified KES period.
	Sign(period uint64, message []byte) ([]byte, error)
	// VKey returns the KES verification key.
	VKey() []byte
	// CurrentPeriod returns the current KES period the key is evolved to.
	CurrentPeriod() uint64
}

// KeyStoreConfig holds configuration for the KeyStore.
type KeyStoreConfig struct {
	// VRFSKeyPath is the path to the VRF signing key file.
	VRFSKeyPath string
	// KESSKeyPath is the path to the KES signing key file.
	KESSKeyPath string
	// OpCertPath is the path to the operational certificate file.
	OpCertPath string
	// SlotsPerKESPeriod is the number of slots in a KES period.
	// Default: 129600 (preview network value)
	SlotsPerKESPeriod uint64
	// MaxKESEvolutions is the maximum number of KES evolutions.
	// Default: 62 (for KES depth 6)
	MaxKESEvolutions uint64
	// Logger for keystore events.
	Logger *slog.Logger
}

// DefaultKeyStoreConfig returns the default configuration.
func DefaultKeyStoreConfig() KeyStoreConfig {
	return KeyStoreConfig{
		SlotsPerKESPeriod: DefaultSlotsPerKESPeriod,
		MaxKESEvolutions:  DefaultMaxKESEvolutions,
	}
}

// KeyStore manages pool credentials and KES key evolution.
type KeyStore struct {
	config KeyStoreConfig
	logger *slog.Logger

	// Credentials
	poolID  lcommon.PoolId // Blake2b-224 hash of cold verification key
	hasPool bool           // true if poolID has been set
	vrfSKey []byte         // VRF secret key (seed)
	vrfVKey []byte         // VRF verification key
	kesSKey *kes.SecretKey // KES secret key
	kesVKey []byte         // KES verification key
	opCert  *OpCert        // Operational certificate

	// State
	mu      sync.RWMutex
	running bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewKeyStore creates a new KeyStore with the given configuration.
func NewKeyStore(config KeyStoreConfig) *KeyStore {
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.SlotsPerKESPeriod == 0 {
		config.SlotsPerKESPeriod = DefaultSlotsPerKESPeriod
	}
	if config.MaxKESEvolutions == 0 {
		config.MaxKESEvolutions = DefaultMaxKESEvolutions
	}
	return &KeyStore{
		config: config,
		logger: config.Logger.With("component", "keystore"),
	}
}

// LoadFromFiles loads all pool credentials from the configured file paths.
// Parses cardano-cli format key files.
// Security: Verifies file permissions are not world-readable.
func (ks *KeyStore) LoadFromFiles() error {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	// Check file permissions for all key files
	paths := []string{
		ks.config.VRFSKeyPath,
		ks.config.KESSKeyPath,
		ks.config.OpCertPath,
	}
	for _, path := range paths {
		if err := checkKeyFilePermissions(path); err != nil {
			return fmt.Errorf("security check failed for %s: %w", path, err)
		}
	}

	// Load VRF signing key
	vrfKey, err := loadKeyFromFile(ks.config.VRFSKeyPath)
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
	ks.vrfSKey = vrfKey.SKey
	ks.vrfVKey = vrfKey.VKey

	// Load KES signing key
	kesKey, err := loadKeyFromFile(ks.config.KESSKeyPath)
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
	ks.kesSKey = &kes.SecretKey{
		Depth:  kes.CardanoKesDepth,
		Period: 0, // Will be evolved during Start()
		Data:   kesKey.SKey,
	}
	ks.kesVKey = kesKey.VKey

	// Load operational certificate
	opCertKey, err := loadKeyFromFile(ks.config.OpCertPath)
	if err != nil {
		return fmt.Errorf("failed to load operational certificate: %w", err)
	}
	ks.opCert = &OpCert{
		KESVKey:     opCertKey.VKey,
		IssueNumber: opCertKey.OpCertIssueNumber,
		KESPeriod:   opCertKey.OpCertKesPeriod,
		Signature:   opCertKey.OpCertSignature,
		ColdVKey:    opCertKey.OpCertColdVKey,
	}

	// Verify KES verification key in OpCert matches loaded KES key
	if !bytes.Equal(ks.kesVKey, ks.opCert.KESVKey) {
		return errors.New(
			"opCert KES vkey does not match loaded KES key: " +
				"ensure the operational certificate was generated for this KES key",
		)
	}

	// Derive pool ID from cold verification key (Blake2b-224 hash)
	ks.poolID = lcommon.PoolId(lcommon.Blake2b224Hash(ks.opCert.ColdVKey))
	ks.hasPool = true

	ks.logger.Info(
		"pool credentials loaded",
		"pool_id", ks.poolID.String(),
		"opcert_issue", ks.opCert.IssueNumber,
		"opcert_kes_period", ks.opCert.KESPeriod,
	)

	return nil
}

// SlotClock provides slot information for KES evolution monitoring.
type SlotClock interface {
	// CurrentSlot returns the current slot number.
	CurrentSlot() (uint64, error)
	// Subscribe returns a channel that receives slot tick notifications.
	Subscribe() <-chan SlotTick
	// Unsubscribe removes a subscriber channel.
	Unsubscribe(ch <-chan SlotTick)
}

// SlotTick represents a slot boundary notification (minimal interface).
type SlotTick struct {
	Slot uint64
}

// Start begins KES evolution monitoring using the provided slot clock.
// The keystore will automatically evolve the KES key when the period advances.
func (ks *KeyStore) Start(ctx context.Context, slotClock SlotClock) error {
	ks.mu.Lock()
	if ks.running {
		ks.mu.Unlock()
		return ErrAlreadyRunning
	}
	if !ks.isLoadedUnsafe() {
		ks.mu.Unlock()
		return ErrKeysNotLoaded
	}

	// Get current slot and evolve to current period
	currentSlot, err := slotClock.CurrentSlot()
	if err != nil {
		ks.mu.Unlock()
		return fmt.Errorf("failed to get current slot: %w", err)
	}
	currentPeriod := ks.slotToKESPeriod(currentSlot)

	// Evolve KES key to current period
	if err := ks.evolveKESToUnsafe(currentPeriod); err != nil {
		ks.mu.Unlock()
		return fmt.Errorf("failed to evolve KES to current period: %w", err)
	}

	// Check OpCert expiry
	ks.checkOpCertExpiryUnsafe(currentPeriod)

	ks.running = true
	ctx, ks.cancel = context.WithCancel(ctx)
	ks.mu.Unlock()

	// Start monitoring goroutine
	ks.wg.Add(1)
	go ks.monitorSlots(ctx, slotClock)

	ks.logger.Info(
		"keystore started",
		"current_slot", currentSlot,
		"current_kes_period", currentPeriod,
	)

	return nil
}

// Stop halts KES evolution monitoring and cleans up resources.
func (ks *KeyStore) Stop() error {
	ks.mu.Lock()
	if !ks.running {
		ks.mu.Unlock()
		return ErrNotRunning
	}
	ks.running = false
	if ks.cancel != nil {
		ks.cancel()
	}
	ks.mu.Unlock()

	// Wait for monitoring goroutine to exit
	ks.wg.Wait()

	ks.logger.Info("keystore stopped")
	return nil
}

// VRFSigner returns a VRF signer for leader election.
// Returns nil if VRF key is not loaded.
func (ks *KeyStore) VRFSigner() VRFSigner {
	ks.mu.RLock()
	defer ks.mu.RUnlock()

	if ks.vrfSKey == nil {
		return nil
	}
	// Use defensive copies to prevent modification of internal state
	return &vrfSignerImpl{
		skey: bytes.Clone(ks.vrfSKey),
		vkey: bytes.Clone(ks.vrfVKey),
	}
}

// KESSigner returns a KES signer for block signing.
// Returns nil if KES key is not loaded.
func (ks *KeyStore) KESSigner() KESSigner {
	ks.mu.RLock()
	defer ks.mu.RUnlock()

	if ks.kesSKey == nil {
		return nil
	}
	return &kesSignerImpl{
		ks: ks,
	}
}

// OpCert returns a copy of the operational certificate.
// Returns nil if not loaded.
func (ks *KeyStore) OpCert() *OpCert {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	if ks.opCert == nil {
		return nil
	}
	// Return a defensive copy to prevent callers from modifying internal state
	return &OpCert{
		KESVKey:     bytes.Clone(ks.opCert.KESVKey),
		IssueNumber: ks.opCert.IssueNumber,
		KESPeriod:   ks.opCert.KESPeriod,
		Signature:   bytes.Clone(ks.opCert.Signature),
		ColdVKey:    bytes.Clone(ks.opCert.ColdVKey),
	}
}

// PoolID returns the pool ID (Blake2b-224 of cold vkey).
// Returns nil if not loaded.
func (ks *KeyStore) PoolID() *lcommon.PoolId {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	if !ks.hasPool {
		return nil
	}
	result := ks.poolID // copy the array
	return &result
}

// IsLoaded returns true if all credentials have been loaded.
func (ks *KeyStore) IsLoaded() bool {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	return ks.isLoadedUnsafe()
}

func (ks *KeyStore) isLoadedUnsafe() bool {
	return ks.vrfSKey != nil && ks.kesSKey != nil && ks.opCert != nil
}

// ValidateOpCert validates that the operational certificate matches the KES key.
func (ks *KeyStore) ValidateOpCert() error {
	ks.mu.RLock()
	defer ks.mu.RUnlock()

	if ks.opCert == nil {
		return ErrOpCertNotLoaded
	}
	if ks.kesVKey == nil {
		return ErrKESKeyNotLoaded
	}

	// Verify KES public key matches OpCert's hot vkey
	if !bytes.Equal(ks.kesVKey, ks.opCert.KESVKey) {
		return errors.New(
			"KES verification key mismatch: loaded key does not match OpCert.KESVKey",
		)
	}

	return nil
}

// OpCertExpiryPeriod returns the KES period at which the OpCert expires.
func (ks *KeyStore) OpCertExpiryPeriod() uint64 {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	return ks.opCertExpiryPeriodUnsafe()
}

func (ks *KeyStore) opCertExpiryPeriodUnsafe() uint64 {
	if ks.opCert == nil {
		return 0
	}
	return ks.opCert.KESPeriod + ks.config.MaxKESEvolutions
}

// PeriodsRemaining returns how many KES periods remain before expiry.
func (ks *KeyStore) PeriodsRemaining(currentPeriod uint64) uint64 {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	return ks.periodsRemainingUnsafe(currentPeriod)
}

func (ks *KeyStore) periodsRemainingUnsafe(currentPeriod uint64) uint64 {
	expiryPeriod := ks.opCertExpiryPeriodUnsafe()
	if currentPeriod >= expiryPeriod {
		return 0
	}
	return expiryPeriod - currentPeriod
}

// CurrentKESPeriod returns the current KES period the key is evolved to.
func (ks *KeyStore) CurrentKESPeriod() uint64 {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	if ks.kesSKey == nil {
		return 0
	}
	return ks.kesSKey.Period
}

// EvolveKESTo evolves the KES key to the specified period.
func (ks *KeyStore) EvolveKESTo(targetPeriod uint64) error {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	return ks.evolveKESToUnsafe(targetPeriod)
}

func (ks *KeyStore) evolveKESToUnsafe(targetPeriod uint64) error {
	if ks.kesSKey == nil {
		return ErrKESKeyNotLoaded
	}

	for ks.kesSKey.Period < targetPeriod {
		newKey, err := kes.Update(ks.kesSKey)
		if err != nil {
			return fmt.Errorf(
				"failed to evolve KES key to period %d: %w",
				targetPeriod,
				err,
			)
		}
		ks.kesSKey = newKey
		ks.logger.Debug(
			"KES key evolved",
			"new_period", ks.kesSKey.Period,
		)
	}

	return nil
}

// slotToKESPeriod converts a slot number to a KES period.
// Returns 0 if SlotsPerKESPeriod is 0 to avoid division by zero.
func (ks *KeyStore) slotToKESPeriod(slot uint64) uint64 {
	if ks.config.SlotsPerKESPeriod == 0 {
		return 0
	}
	return slot / ks.config.SlotsPerKESPeriod
}

// checkOpCertExpiryUnsafe logs warnings if OpCert expiry is approaching.
// Caller must hold ks.mu (read or write lock).
func (ks *KeyStore) checkOpCertExpiryUnsafe(currentPeriod uint64) {
	if ks.opCert == nil {
		return
	}

	remaining := ks.periodsRemainingUnsafe(currentPeriod)

	if remaining == 0 {
		ks.logger.Error(
			"OpCert has EXPIRED! Cannot produce blocks",
			"opcert_kes_period", ks.opCert.KESPeriod,
			"current_period", currentPeriod,
		)
		return
	}

	// Warn at various thresholds
	switch {
	case remaining <= 3:
		ks.logger.Error(
			"OpCert expiry CRITICAL - renew immediately",
			"periods_remaining", remaining,
			"current_period", currentPeriod,
		)
	case remaining <= 7:
		ks.logger.Warn(
			"OpCert expiry WARNING - renew soon",
			"periods_remaining", remaining,
			"current_period", currentPeriod,
		)
	case remaining <= 14:
		ks.logger.Info(
			"OpCert expiry approaching",
			"periods_remaining", remaining,
			"current_period", currentPeriod,
		)
	}
}

// monitorSlots watches the slot clock and evolves KES key when period changes.
func (ks *KeyStore) monitorSlots(ctx context.Context, slotClock SlotClock) {
	defer ks.wg.Done()

	ch := slotClock.Subscribe()
	defer slotClock.Unsubscribe(ch)

	var lastPeriod uint64
	ks.mu.RLock()
	if ks.kesSKey != nil {
		lastPeriod = ks.kesSKey.Period
	}
	ks.mu.RUnlock()

	for {
		select {
		case <-ctx.Done():
			return
		case tick, ok := <-ch:
			if !ok {
				return
			}

			currentPeriod := ks.slotToKESPeriod(tick.Slot)
			if currentPeriod > lastPeriod {
				ks.logger.Info(
					"KES period changed, evolving key",
					"old_period", lastPeriod,
					"new_period", currentPeriod,
					"slot", tick.Slot,
				)

				if err := ks.EvolveKESTo(currentPeriod); err != nil {
					ks.logger.Error(
						"failed to evolve KES key",
						"error", err,
						"target_period", currentPeriod,
					)
					continue
				}

				// Check OpCert expiry after evolution
				ks.mu.RLock()
				ks.checkOpCertExpiryUnsafe(currentPeriod)
				ks.mu.RUnlock()

				lastPeriod = currentPeriod
			}
		}
	}
}

// vrfSignerImpl implements VRFSigner.
type vrfSignerImpl struct {
	skey []byte
	vkey []byte
}

func (v *vrfSignerImpl) Prove(alpha []byte) ([]byte, []byte, error) {
	return vrf.Prove(v.skey, alpha)
}

func (v *vrfSignerImpl) VKey() []byte {
	return bytes.Clone(v.vkey)
}

// kesSignerImpl implements KESSigner.
type kesSignerImpl struct {
	ks *KeyStore
}

func (k *kesSignerImpl) Sign(period uint64, message []byte) ([]byte, error) {
	k.ks.mu.RLock()
	defer k.ks.mu.RUnlock()

	if k.ks.kesSKey == nil {
		return nil, ErrKESKeyNotLoaded
	}

	return kes.Sign(k.ks.kesSKey, period, message)
}

func (k *kesSignerImpl) VKey() []byte {
	k.ks.mu.RLock()
	defer k.ks.mu.RUnlock()
	return bytes.Clone(k.ks.kesVKey)
}

func (k *kesSignerImpl) CurrentPeriod() uint64 {
	k.ks.mu.RLock()
	defer k.ks.mu.RUnlock()
	if k.ks.kesSKey == nil {
		return 0
	}
	return k.ks.kesSKey.Period
}
