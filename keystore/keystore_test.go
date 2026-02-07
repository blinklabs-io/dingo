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

package keystore

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/blinklabs-io/gouroboros/vrf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func isWindows() bool {
	return runtime.GOOS == "windows"
}

// Sample test keys from config/cardano/devnet/keys/
const (
	testVRFSKeyJSON = `{
    "type": "VrfSigningKey_PraosVRF",
    "description": "VRF Signing Key",
    "cborHex": "5840899795b70e9f34b737159fe21a6170568d6031e187f0cc84555c712b7c29b45cb882007593ef70f86e5c0948561a3b8e8851529a4f98975f2b24e768dda38ce2"
}`

	testKESSKeyJSON = `{
    "type": "KesSigningKey_ed25519_kes_2^6",
    "description": "KES Signing Key",
    "cborHex": "590260a199f16b11da6c7f5c1e0f1eb0b9bbe278d3d8f35bfd50d0951c2ff94d0344cd57df5f64c9bac1dd60b4482f9c636168f40737d526625a2ec82f22ec0c72de0013f86ef743a7bba0286db6ddf3d85bf8e49ddbf14d9d3b7ee22f4857c77b740948f84f2e72f6bcf91f405e34ea50a2c53fa4876b43cfce2bcfe87c06a903de8bb33d968ca7930b67d0c23f5cb2d74e422d773ba80e388de384691000d6ba8a9b4dc7d3187f76048fbef9a52b72d80d835bb76eced7c0e0cdc5b58869b73c095dffa01db4ff51765afcead565395a5ed1cf74e5f2134d61076fece21aacd080bbbfaab94125401d7bbc74eafc7e7e3a2235f59dc03d6e332e53d558493a1e22213b92c77b1328ff1b83855da704fc366bf4415490602481d1939136eeaf252c65184912a779d9d94a90e32b72c1877ef60b6d79e707ce5a762acb4bed46436efe4fe62aae50b39068cc508a09427c92791cbcbea44318529cc68d297ca24e1b73b2394c385ec63fcd85ed56eec3de48860a1ec950aad4f91cbf741dbd7bf1d3c278875bd20e31ff5372339f6aa5280ad9b8bf3514889ac44600fe57ca0b535d6dc6b0b981e079595aad186ee0be9b07e837391ab165e4ca406601c876a86e246a3f53311e21199cccc0b080f28d18f4dc6987731e10e4ade00df7c6921c5ef3022b6f49a29ba307a2c8f4bd2ba42fcfa0aad68a2f0ad31fff69a99d3471f9036d3f5817a3edfeff7fc3c14e1151d767aaa043481cfd1a6ee55e8e5d7853ecdaf9da2bb36c716beae8d706bc648a790d4697e1d044a11a49f305ab8bc64a094bd81bda7395fe6f77dd5557c39919dd9bb9cf22a87fe47408ae3ec2247007d015a5"
}`

	testOpCertJSON = `{
    "type": "NodeOperationalCertificate",
    "description": "",
    "cborHex": "828458204cd49bb05e9885142fe7af1481107995298771fd1a24e72b506a4d600ee2b3120000584089fc9e9f551b2ea873bf31643659d049152d5c8e8de86be4056370bccc5fa62dd12e3f152f1664e614763e46eaa7a17ed366b5cef19958773d1ab96941442e0b58205a3d778e76741a009e29d23093cfe046131808d34d7c864967b515e98dfc3583"
}`
)

func setupTestKeys(t *testing.T) (string, string, string) {
	t.Helper()

	tmpDir := t.TempDir()

	vrfPath := filepath.Join(tmpDir, "vrf.skey")
	kesPath := filepath.Join(tmpDir, "kes.skey")
	opCertPath := filepath.Join(tmpDir, "opcert.cert")

	require.NoError(t, os.WriteFile(vrfPath, []byte(testVRFSKeyJSON), 0o600))
	require.NoError(t, os.WriteFile(kesPath, []byte(testKESSKeyJSON), 0o600))
	require.NoError(t, os.WriteFile(opCertPath, []byte(testOpCertJSON), 0o600))

	return vrfPath, kesPath, opCertPath
}

func TestKeyStoreLoadFromFiles(t *testing.T) {
	vrfPath, kesPath, opCertPath := setupTestKeys(t)

	config := KeyStoreConfig{
		VRFSKeyPath: vrfPath,
		KESSKeyPath: kesPath,
		OpCertPath:  opCertPath,
	}

	ks := NewKeyStore(config)
	err := ks.LoadFromFiles()
	require.NoError(t, err)

	// Verify VRF keys
	assert.True(t, ks.IsLoaded())
	vrfSigner := ks.VRFSigner()
	require.NotNil(t, vrfSigner)
	assert.Equal(t, vrf.PublicKeySize, len(vrfSigner.VKey()))

	// Verify KES keys
	kesSigner := ks.KESSigner()
	require.NotNil(t, kesSigner)
	assert.Equal(t, 32, len(kesSigner.VKey()))
	assert.Equal(t, uint64(0), kesSigner.CurrentPeriod())

	// Verify OpCert
	opCert := ks.OpCert()
	require.NotNil(t, opCert)
	assert.Equal(t, uint64(0), opCert.IssueNumber)
	assert.Equal(t, uint64(0), opCert.KESPeriod)
	assert.Equal(t, 64, len(opCert.Signature))
	assert.Equal(t, 32, len(opCert.ColdVKey))

	// Verify pool ID is derived from cold key
	poolID := ks.PoolID()
	require.NotNil(t, poolID)
	assert.Equal(t, 28, len(*poolID))
}

func TestVRFSigner(t *testing.T) {
	vrfPath, kesPath, opCertPath := setupTestKeys(t)

	config := KeyStoreConfig{
		VRFSKeyPath: vrfPath,
		KESSKeyPath: kesPath,
		OpCertPath:  opCertPath,
	}

	ks := NewKeyStore(config)
	require.NoError(t, ks.LoadFromFiles())

	vrfSigner := ks.VRFSigner()
	require.NotNil(t, vrfSigner)

	// Generate VRF proof for a sample slot
	epochNonce := make([]byte, 32) // All zeros for test
	alpha := vrf.MkInputVrf(1000, epochNonce)

	proof, output, err := vrfSigner.Prove(alpha)
	require.NoError(t, err)

	assert.Equal(t, vrf.ProofSize, len(proof))
	assert.Equal(t, vrf.OutputSize, len(output))

	// Verify the proof
	ok, err := vrf.Verify(vrfSigner.VKey(), proof, output, alpha)
	require.NoError(t, err)
	assert.True(t, ok, "VRF proof verification failed")
}

func TestKESSigner(t *testing.T) {
	vrfPath, kesPath, opCertPath := setupTestKeys(t)

	config := KeyStoreConfig{
		VRFSKeyPath: vrfPath,
		KESSKeyPath: kesPath,
		OpCertPath:  opCertPath,
	}

	ks := NewKeyStore(config)
	require.NoError(t, ks.LoadFromFiles())

	kesSigner := ks.KESSigner()
	require.NotNil(t, kesSigner)

	// Sign a test message at period 0
	message := []byte("test block header data")
	signature, err := kesSigner.Sign(0, message)
	require.NoError(t, err)

	// KES signature for depth 6 is 448 bytes
	assert.Equal(t, kes.CardanoKesSignatureSize, len(signature))

	// Verify the signature
	ok := kes.VerifySignedKES(kesSigner.VKey(), 0, message, signature)
	assert.True(t, ok, "KES signature verification failed")
}

func TestKESEvolution(t *testing.T) {
	vrfPath, kesPath, opCertPath := setupTestKeys(t)

	config := KeyStoreConfig{
		VRFSKeyPath: vrfPath,
		KESSKeyPath: kesPath,
		OpCertPath:  opCertPath,
	}

	ks := NewKeyStore(config)
	require.NoError(t, ks.LoadFromFiles())

	// Initial period should be 0
	assert.Equal(t, uint64(0), ks.CurrentKESPeriod())

	// Evolve to period 1
	err := ks.EvolveKESTo(1)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), ks.CurrentKESPeriod())

	// Sign at period 1
	kesSigner := ks.KESSigner()
	require.NotNil(t, kesSigner)
	message := []byte("test message for period 1")
	signature, err := kesSigner.Sign(1, message)
	require.NoError(t, err)

	// Verify the signature at period 1
	ok := kes.VerifySignedKES(kesSigner.VKey(), 1, message, signature)
	assert.True(t, ok, "KES signature verification failed at period 1")

	// Evolve to period 3 (skipping 2)
	err = ks.EvolveKESTo(3)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), ks.CurrentKESPeriod())
}

func TestOpCertValidation(t *testing.T) {
	vrfPath, kesPath, opCertPath := setupTestKeys(t)

	config := KeyStoreConfig{
		VRFSKeyPath:      vrfPath,
		KESSKeyPath:      kesPath,
		OpCertPath:       opCertPath,
		MaxKESEvolutions: 64, // Depth 6
	}

	ks := NewKeyStore(config)
	require.NoError(t, ks.LoadFromFiles())

	// Validate OpCert - should pass since keys match
	err := ks.ValidateOpCert()
	require.NoError(t, err)

	// Check expiry period
	expiryPeriod := ks.OpCertExpiryPeriod()
	// For depth 6, max periods = 64, starting at period 0
	assert.Equal(t, uint64(64), expiryPeriod)

	// Check periods remaining
	remaining := ks.PeriodsRemaining(0)
	assert.Equal(t, uint64(64), remaining)

	remaining = ks.PeriodsRemaining(32)
	assert.Equal(t, uint64(32), remaining)

	remaining = ks.PeriodsRemaining(64)
	assert.Equal(t, uint64(0), remaining)

	remaining = ks.PeriodsRemaining(100)
	assert.Equal(t, uint64(0), remaining)
}

// mockSlotClock implements SlotClock for testing.
type mockSlotClock struct {
	mu          sync.RWMutex
	currentSlot uint64
	subscribers []chan SlotTick
}

func newMockSlotClock(initialSlot uint64) *mockSlotClock {
	return &mockSlotClock{
		currentSlot: initialSlot,
		subscribers: make([]chan SlotTick, 0),
	}
}

func (m *mockSlotClock) CurrentSlot() (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentSlot, nil
}

func (m *mockSlotClock) Subscribe() <-chan SlotTick {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := make(chan SlotTick, 1)
	m.subscribers = append(m.subscribers, ch)
	return ch
}

func (m *mockSlotClock) Unsubscribe(ch <-chan SlotTick) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, sub := range m.subscribers {
		if sub == ch {
			// Don't close the channel here - AdvanceSlot may be concurrently
			// sending to a copy of the subscribers slice after releasing the lock.
			// Just remove from slice; the channel will be garbage collected.
			m.subscribers = append(m.subscribers[:i], m.subscribers[i+1:]...)
			return
		}
	}
}

func (m *mockSlotClock) AdvanceSlot(slot uint64) {
	m.mu.Lock()
	m.currentSlot = slot
	subs := make([]chan SlotTick, len(m.subscribers))
	copy(subs, m.subscribers)
	m.mu.Unlock()

	tick := SlotTick{Slot: slot}
	for _, ch := range subs {
		select {
		case ch <- tick:
		default:
		}
	}
}

func (m *mockSlotClock) HasSubscribers() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.subscribers) > 0
}

func TestKeyStoreStartStop(t *testing.T) {
	vrfPath, kesPath, opCertPath := setupTestKeys(t)

	config := KeyStoreConfig{
		VRFSKeyPath:       vrfPath,
		KESSKeyPath:       kesPath,
		OpCertPath:        opCertPath,
		SlotsPerKESPeriod: 100, // Small value for testing
	}

	ks := NewKeyStore(config)
	require.NoError(t, ks.LoadFromFiles())

	// Create a mock slot clock starting at slot 50 (period 0)
	slotClock := newMockSlotClock(50)

	// Start the keystore
	ctx := context.Background()
	err := ks.Start(ctx, slotClock)
	require.NoError(t, err)

	// Starting again should fail
	err = ks.Start(ctx, slotClock)
	assert.ErrorIs(t, err, ErrAlreadyRunning)

	// Stop the keystore
	err = ks.Stop()
	require.NoError(t, err)

	// Stopping again should fail
	err = ks.Stop()
	assert.ErrorIs(t, err, ErrNotRunning)
}

func TestKeyStoreKESEvolutionOnPeriodChange(t *testing.T) {
	vrfPath, kesPath, opCertPath := setupTestKeys(t)

	config := KeyStoreConfig{
		VRFSKeyPath:       vrfPath,
		KESSKeyPath:       kesPath,
		OpCertPath:        opCertPath,
		SlotsPerKESPeriod: 100, // Small value for testing
	}

	ks := NewKeyStore(config)
	require.NoError(t, ks.LoadFromFiles())

	// Create a mock slot clock starting at slot 50 (period 0)
	slotClock := newMockSlotClock(50)

	// Start the keystore
	ctx := context.Background()
	err := ks.Start(ctx, slotClock)
	require.NoError(t, err)
	defer func() { _ = ks.Stop() }()

	// Wait for monitor goroutine to subscribe to slot clock
	require.Eventually(t, func() bool {
		return slotClock.HasSubscribers()
	}, time.Second, 10*time.Millisecond, "monitor should subscribe to slot clock")

	// Initial period should be 0
	assert.Equal(t, uint64(0), ks.CurrentKESPeriod())

	// Advance to slot 150 (period 1)
	slotClock.AdvanceSlot(150)

	// Wait for evolution to period 1
	require.Eventually(t, func() bool {
		return ks.CurrentKESPeriod() == 1
	}, time.Second, 10*time.Millisecond, "KES period should evolve to 1")

	// Advance to slot 350 (period 3)
	slotClock.AdvanceSlot(350)

	// Wait for evolution to period 3
	require.Eventually(t, func() bool {
		return ks.CurrentKESPeriod() == 3
	}, time.Second, 10*time.Millisecond, "KES period should evolve to 3")
}

func TestKeyStoreNotLoadedError(t *testing.T) {
	config := KeyStoreConfig{}
	ks := NewKeyStore(config)

	// Not loaded
	assert.False(t, ks.IsLoaded())
	assert.Nil(t, ks.VRFSigner())
	assert.Nil(t, ks.KESSigner())
	assert.Nil(t, ks.OpCert())
	assert.Nil(t, ks.PoolID())

	// Cannot start without loading
	slotClock := newMockSlotClock(0)
	err := ks.Start(context.Background(), slotClock)
	assert.ErrorIs(t, err, ErrKeysNotLoaded)
}

func TestEvolutionConstants(t *testing.T) {
	// Test slot to KES period conversion
	assert.Equal(t, uint64(0), SlotToKESPeriod(0, DefaultSlotsPerKESPeriod))
	assert.Equal(t, uint64(0), SlotToKESPeriod(129599, DefaultSlotsPerKESPeriod))
	assert.Equal(t, uint64(1), SlotToKESPeriod(129600, DefaultSlotsPerKESPeriod))
	assert.Equal(t, uint64(1), SlotToKESPeriod(259199, DefaultSlotsPerKESPeriod))
	assert.Equal(t, uint64(2), SlotToKESPeriod(259200, DefaultSlotsPerKESPeriod))

	// Test period start/end slots
	assert.Equal(t, uint64(0), KESPeriodStartSlot(0, DefaultSlotsPerKESPeriod))
	assert.Equal(t, uint64(129599), KESPeriodEndSlot(0, DefaultSlotsPerKESPeriod))
	assert.Equal(t, uint64(129600), KESPeriodStartSlot(1, DefaultSlotsPerKESPeriod))
	assert.Equal(t, uint64(259199), KESPeriodEndSlot(1, DefaultSlotsPerKESPeriod))

	// Test KES period duration
	duration := DefaultKESPeriodDuration()
	assert.Equal(t, time.Duration(DefaultSlotsPerKESPeriod)*time.Second, duration)

	// Test OpCert lifetime
	lifetimeSlots := DefaultOpCertLifetimeSlots()
	assert.Equal(t, uint64(DefaultSlotsPerKESPeriod*DefaultMaxKESEvolutions), lifetimeSlots)
}

func TestExpiryWarningThresholds(t *testing.T) {
	thresholds := DefaultExpiryWarningThresholds()
	assert.Equal(t, uint64(3), thresholds.Critical)
	assert.Equal(t, uint64(7), thresholds.Warning)
	assert.Equal(t, uint64(14), thresholds.Info)
}

func TestInsecureFileModeUnix(t *testing.T) {
	if isWindows() {
		t.Skip("Unix permission test; see TestInsecureFileModeWindows for Windows DACL test")
	}

	tmpDir := t.TempDir()

	vrfPath := filepath.Join(tmpDir, "vrf.skey")
	kesPath := filepath.Join(tmpDir, "kes.skey")
	opCertPath := filepath.Join(tmpDir, "opcert.cert")

	// Write files first, then explicitly set permissions with os.Chmod
	// to avoid umask interference with os.WriteFile permissions
	require.NoError(t, os.WriteFile(vrfPath, []byte(testVRFSKeyJSON), 0o600))
	require.NoError(t, os.WriteFile(kesPath, []byte(testKESSKeyJSON), 0o600))
	require.NoError(t, os.WriteFile(opCertPath, []byte(testOpCertJSON), 0o600))

	// Set VRF key to insecure permissions (0644) after creation
	require.NoError(t, os.Chmod(vrfPath, 0o644))
	require.NoError(t, os.Chmod(kesPath, 0o600))
	require.NoError(t, os.Chmod(opCertPath, 0o600))

	config := KeyStoreConfig{
		VRFSKeyPath: vrfPath,
		KESSKeyPath: kesPath,
		OpCertPath:  opCertPath,
	}

	ks := NewKeyStore(config)
	err := ks.LoadFromFiles()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInsecureFileMode)
}
