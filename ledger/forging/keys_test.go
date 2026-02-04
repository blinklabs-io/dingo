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

package forging

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/blinklabs-io/gouroboros/vrf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// createTestKeys creates temp key files for testing and returns their paths.
func createTestKeys(t *testing.T) (string, string, string) {
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

func TestPoolCredentialsLoadFromFiles(t *testing.T) {
	vrfPath, kesPath, opCertPath := createTestKeys(t)

	// Load credentials
	pc := NewPoolCredentials()
	err := pc.LoadFromFiles(vrfPath, kesPath, opCertPath)
	require.NoError(t, err)

	// Verify VRF keys
	assert.Equal(t, vrf.SeedSize, len(pc.vrfSKey))
	assert.Equal(t, vrf.PublicKeySize, len(pc.vrfVKey))
	// Note: Only log public keys, never secret keys
	t.Logf("VRF VKey: %s", hex.EncodeToString(pc.vrfVKey))

	// Verify KES keys
	assert.NotNil(t, pc.kesSKey)
	assert.Equal(t, kes.CardanoKesSecretKeySize, len(pc.kesSKey.Data))
	assert.Equal(t, kes.CardanoKesDepth, int(pc.kesSKey.Depth))
	assert.Equal(t, 32, len(pc.kesVKey))
	t.Logf("KES VKey: %s", hex.EncodeToString(pc.kesVKey))

	// Verify OpCert
	assert.NotNil(t, pc.opCert)
	assert.Equal(t, uint64(0), pc.opCert.IssueNumber)
	assert.Equal(t, uint64(0), pc.opCert.KESPeriod)
	assert.Equal(t, 64, len(pc.opCert.Signature))
	assert.Equal(t, 32, len(pc.opCert.ColdVKey))
	t.Logf("OpCert Cold VKey: %s", hex.EncodeToString(pc.opCert.ColdVKey))

	// Verify OpCert KES VKey matches loaded KES VKey
	assert.Equal(t, pc.kesVKey, pc.opCert.KESVKey)

	// Verify pool ID is derived from cold key
	assert.Equal(t, 28, len(pc.poolID))
	t.Logf("Pool ID: %s", pc.poolID.String())

	// Verify IsLoaded
	assert.True(t, pc.IsLoaded())
}

func TestVRFProve(t *testing.T) {
	vrfPath, kesPath, opCertPath := createTestKeys(t)

	// Load credentials
	pc := NewPoolCredentials()
	require.NoError(t, pc.LoadFromFiles(vrfPath, kesPath, opCertPath))

	// Generate VRF proof for a sample slot
	epochNonce := make([]byte, 32) // All zeros for test
	alpha := vrf.MkInputVrf(1000, epochNonce)

	proof, output, err := pc.VRFProve(alpha)
	require.NoError(t, err)

	assert.Equal(t, vrf.ProofSize, len(proof))
	assert.Equal(t, vrf.OutputSize, len(output))

	t.Logf("VRF Proof: %s...", hex.EncodeToString(proof[:16]))
	t.Logf("VRF Output: %s...", hex.EncodeToString(output[:16]))

	// Verify the proof
	ok, err := vrf.Verify(pc.vrfVKey, proof, output, alpha)
	require.NoError(t, err)
	assert.True(t, ok, "VRF proof verification failed")
}

func TestKESSign(t *testing.T) {
	vrfPath, kesPath, opCertPath := createTestKeys(t)

	// Load credentials
	pc := NewPoolCredentials()
	require.NoError(t, pc.LoadFromFiles(vrfPath, kesPath, opCertPath))

	// Sign a test message at period 0
	message := []byte("test block header data")
	signature, err := pc.KESSign(0, message)
	require.NoError(t, err)

	// KES signature for depth 6 is 448 bytes
	assert.Equal(t, kes.CardanoKesSignatureSize, len(signature))
	t.Logf("KES Signature: %s...", hex.EncodeToString(signature[:32]))

	// Verify the signature
	ok := kes.VerifySignedKES(pc.kesVKey, 0, message, signature)
	assert.True(t, ok, "KES signature verification failed")
}

func TestKESPeriodUpdate(t *testing.T) {
	vrfPath, kesPath, opCertPath := createTestKeys(t)

	// Load credentials
	pc := NewPoolCredentials()
	require.NoError(t, pc.LoadFromFiles(vrfPath, kesPath, opCertPath))

	// Update to period 1
	err := pc.UpdateKESPeriod(1)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), pc.kesSKey.Period)

	// Sign at period 1
	message := []byte("test message for period 1")
	signature, err := pc.KESSign(1, message)
	require.NoError(t, err)

	// Verify the signature at period 1
	ok := kes.VerifySignedKES(pc.kesVKey, 1, message, signature)
	assert.True(t, ok, "KES signature verification failed at period 1")
}

func TestKESPeriodUpdateExhaustedKey(t *testing.T) {
	vrfPath, kesPath, opCertPath := createTestKeys(t)

	pc := NewPoolCredentials()
	require.NoError(t, pc.LoadFromFiles(vrfPath, kesPath, opCertPath))

	// For depth 6, max periods = 2^6 = 64 (periods 0-63).
	// Evolve to the last valid period (63).
	err := pc.UpdateKESPeriod(63)
	require.NoError(t, err)
	assert.Equal(t, uint64(63), pc.kesSKey.Period)

	// Attempting to evolve to period 64 should fail (key exhausted)
	err = pc.UpdateKESPeriod(64)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to update KES key")

	// Verify the key is still at period 63 (original preserved on failure)
	assert.Equal(t, uint64(63), pc.kesSKey.Period)

	// Verify the key is still usable at period 63
	message := []byte("test message after failed evolution")
	signature, err := pc.KESSign(63, message)
	require.NoError(t, err)
	ok := kes.VerifySignedKES(pc.kesVKey, 63, message, signature)
	assert.True(t, ok, "key should still be usable after failed evolution")
}

func TestKESPeriodUpdateBackward(t *testing.T) {
	vrfPath, kesPath, opCertPath := createTestKeys(t)

	pc := NewPoolCredentials()
	require.NoError(t, pc.LoadFromFiles(vrfPath, kesPath, opCertPath))

	// Evolve to period 5
	require.NoError(t, pc.UpdateKESPeriod(5))

	// Attempting to evolve backward should fail
	err := pc.UpdateKESPeriod(3)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot evolve KES key backward")

	// Key should still be at period 5
	assert.Equal(t, uint64(5), pc.kesSKey.Period)
}

func TestOpCertValidation(t *testing.T) {
	// Create a properly-signed OpCert programmatically so we can verify
	// the full validation path including cold key signature verification
	pc := NewPoolCredentials()

	kesVKey := make([]byte, 32)
	for i := range kesVKey {
		kesVKey[i] = byte(i)
	}

	coldPubKey, coldPrivKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	issueNumber := uint64(0)
	kesPeriod := uint64(0)

	// Sign the cert body with the cold private key
	certBody := []any{kesVKey, issueNumber, kesPeriod}
	certCbor, err := cbor.Encode(certBody)
	require.NoError(t, err)
	signature := ed25519.Sign(coldPrivKey, certCbor)

	pc.kesVKey = kesVKey
	pc.opCert = &OpCert{
		KESVKey:     kesVKey,
		IssueNumber: issueNumber,
		KESPeriod:   kesPeriod,
		Signature:   signature,
		ColdVKey:    coldPubKey,
	}

	// Validate OpCert - should pass since keys match and signature is valid
	err = pc.ValidateOpCert()
	require.NoError(t, err)

	// Check expiry period
	expiryPeriod := pc.OpCertExpiryPeriod()
	// For depth 6, max periods = 64, starting at period 0
	assert.Equal(t, uint64(64), expiryPeriod)

	// Check periods remaining
	remaining := pc.PeriodsRemaining(0)
	assert.Equal(t, uint64(64), remaining)

	remaining = pc.PeriodsRemaining(32)
	assert.Equal(t, uint64(32), remaining)

	remaining = pc.PeriodsRemaining(64)
	assert.Equal(t, uint64(0), remaining)

	remaining = pc.PeriodsRemaining(100)
	assert.Equal(t, uint64(0), remaining)
}

func TestOpCertSignatureVerification(t *testing.T) {
	// Generate a fresh ed25519 keypair for cold key to test signature verification
	coldPubKey, coldPrivKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	// Create test KES vkey and OpCert data
	kesVKey := make([]byte, 32)
	for i := range kesVKey {
		kesVKey[i] = byte(i)
	}
	issueNumber := uint64(5)
	kesPeriod := uint64(10)

	// Create the cert data to sign: [KES vkey, issue number, KES period]
	certData := []any{kesVKey, issueNumber, kesPeriod}
	certCbor, err := cbor.Encode(certData)
	require.NoError(t, err)

	// Sign with cold private key
	signature := ed25519.Sign(coldPrivKey, certCbor)

	// Create OpCert with the signature
	opCert := &OpCert{
		KESVKey:     kesVKey,
		IssueNumber: issueNumber,
		KESPeriod:   kesPeriod,
		Signature:   signature,
		ColdVKey:    coldPubKey,
	}

	// Verify the signature using the verification logic
	verifyCertData := []any{opCert.KESVKey, opCert.IssueNumber, opCert.KESPeriod}
	verifyCertCbor, err := cbor.Encode(verifyCertData)
	require.NoError(t, err)

	pubKey := ed25519.PublicKey(opCert.ColdVKey)
	valid := ed25519.Verify(pubKey, verifyCertCbor, opCert.Signature)
	assert.True(t, valid, "OpCert signature verification should succeed")
}

func TestOpCertSignatureVerificationInvalidSignature(t *testing.T) {
	// Generate a fresh ed25519 keypair for cold key
	coldPubKey, coldPrivKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	// Create test KES vkey and OpCert data
	kesVKey := make([]byte, 32)
	issueNumber := uint64(5)
	kesPeriod := uint64(10)

	// Create the cert data to sign
	certData := []any{kesVKey, issueNumber, kesPeriod}
	certCbor, err := cbor.Encode(certData)
	require.NoError(t, err)

	// Sign with cold private key
	signature := ed25519.Sign(coldPrivKey, certCbor)

	// Create OpCert with the signature
	opCert := &OpCert{
		KESVKey:     kesVKey,
		IssueNumber: issueNumber,
		KESPeriod:   kesPeriod,
		Signature:   signature,
		ColdVKey:    coldPubKey,
	}

	// Verify with WRONG issue number - should fail
	wrongCertData := []any{opCert.KESVKey, opCert.IssueNumber + 1, opCert.KESPeriod}
	wrongCertCbor, err := cbor.Encode(wrongCertData)
	require.NoError(t, err)

	pubKey := ed25519.PublicKey(opCert.ColdVKey)
	valid := ed25519.Verify(pubKey, wrongCertCbor, opCert.Signature)
	assert.False(t, valid, "OpCert signature should fail with wrong issue number")

	// Verify with WRONG KES period - should fail
	wrongCertData2 := []any{opCert.KESVKey, opCert.IssueNumber, opCert.KESPeriod + 1}
	wrongCertCbor2, err := cbor.Encode(wrongCertData2)
	require.NoError(t, err)

	valid = ed25519.Verify(pubKey, wrongCertCbor2, opCert.Signature)
	assert.False(t, valid, "OpCert signature should fail with wrong KES period")

	// Verify with WRONG KES vkey - should fail
	wrongKesVKey := make([]byte, 32)
	wrongKesVKey[0] = 0xFF
	wrongCertData3 := []any{wrongKesVKey, opCert.IssueNumber, opCert.KESPeriod}
	wrongCertCbor3, err := cbor.Encode(wrongCertData3)
	require.NoError(t, err)

	valid = ed25519.Verify(pubKey, wrongCertCbor3, opCert.Signature)
	assert.False(t, valid, "OpCert signature should fail with wrong KES vkey")
}

func TestOpCertValidationInvalidSignature(t *testing.T) {
	// Create credentials where the OpCert signature doesn't match the cert body
	pc := NewPoolCredentials()

	kesVKey := make([]byte, 32)
	for i := range kesVKey {
		kesVKey[i] = byte(i)
	}

	// Generate a real cold key pair so the key is valid but the signature is wrong
	coldPubKey, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	pc.kesVKey = kesVKey
	pc.opCert = &OpCert{
		KESVKey:     kesVKey, // Matches KESVKey, so KES check passes
		IssueNumber: 0,
		KESPeriod:   0,
		Signature:   make([]byte, 64), // Invalid signature (all zeros)
		ColdVKey:    coldPubKey,
	}

	// Validation should fail because the signature is invalid
	err = pc.ValidateOpCert()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "OpCert signature verification failed")
}

func TestOpCertValidationMismatchedKESKey(t *testing.T) {
	// Create credentials where the KES vkey doesn't match the OpCert's KES vkey
	pc := NewPoolCredentials()

	// Set up a KES vkey that doesn't match the OpCert
	pc.kesVKey = make([]byte, 32)
	for i := range pc.kesVKey {
		pc.kesVKey[i] = 0xAA // Different from OpCert.KESVKey
	}

	// Set up an OpCert with a different KES vkey
	pc.opCert = &OpCert{
		KESVKey:     make([]byte, 32), // All zeros - different from KESVKey
		IssueNumber: 0,
		KESPeriod:   0,
		Signature:   make([]byte, 64),
		ColdVKey:    make([]byte, 32),
	}

	// Validation should fail because keys don't match
	err := pc.ValidateOpCert()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "KES verification key mismatch")
}
