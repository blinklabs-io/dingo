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
	"crypto/ed25519"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// loadedOpCertKeyStore returns a KeyStore holding the test fixture's
// credentials. The fixture's operational certificate carries a genuine cold
// signature over its own KES vkey, counter and period, so ValidateOpCert must
// accept it unmodified -- see TestOpCertValidation for that direction.
func loadedOpCertKeyStore(t *testing.T) *KeyStore {
	t.Helper()
	vrfPath, kesPath, opCertPath := setupTestKeys(t)
	ks := NewKeyStore(KeyStoreConfig{
		VRFSKeyPath:      vrfPath,
		KESSKeyPath:      kesPath,
		OpCertPath:       opCertPath,
		MaxKESEvolutions: 62,
	})
	require.NoError(t, ks.LoadFromFiles())
	require.NoError(t, ks.ValidateOpCert())
	return ks
}

// TestValidateOpCertRejectsTamperedColdSignature pins the check that makes
// ValidateOpCert more than a restatement of what LoadFromFiles already did.
// LoadFromFiles compares the loaded KES vkey against OpCert.KESVKey, so a
// certificate whose cold signature is corrupt -- truncated file, wrong
// certificate copied into place, tampered bytes -- still loads and still
// matches the KES key. Only the cold signature shows that the pool's cold key
// ever authorized this hot key.
func TestValidateOpCertRejectsTamperedColdSignature(t *testing.T) {
	t.Parallel()

	ks := loadedOpCertKeyStore(t)
	ks.opCert.Signature[0] ^= 0xFF

	require.Error(
		t,
		ks.ValidateOpCert(),
		"ValidateOpCert accepted an operational certificate whose cold signature does not verify",
	)
}

// TestValidateOpCertRejectsUnrelatedColdVKey covers the field that decides the
// node's identity. KeyStore derives its pool id from OpCert.ColdVKey
// (Blake2b-224), so a certificate carrying someone else's cold vkey makes the
// keystore report a pool id whose cold key never signed this hot key. The
// forged pool id is asserted here so the test fails for that reason rather
// than for an incidental one.
func TestValidateOpCertRejectsUnrelatedColdVKey(t *testing.T) {
	t.Parallel()

	ks := loadedOpCertKeyStore(t)
	originalPoolID := ks.PoolID()
	require.NotNil(t, originalPoolID)

	otherVKey, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	ks.opCert.ColdVKey = otherVKey
	ks.poolID = lcommon.PoolId(lcommon.Blake2b224Hash(otherVKey))

	require.NotEqual(t, originalPoolID.String(), ks.PoolID().String())
	require.Error(
		t,
		ks.ValidateOpCert(),
		"ValidateOpCert accepted an operational certificate whose cold vkey did not sign it",
	)
}

// TestValidateOpCertRejectsMalformedColdVKey covers the size guard: an
// ed25519 verification key of the wrong length cannot be checked at all, and
// must be refused rather than silently skipped.
func TestValidateOpCertRejectsMalformedColdVKey(t *testing.T) {
	t.Parallel()

	ks := loadedOpCertKeyStore(t)
	ks.opCert.ColdVKey = ks.opCert.ColdVKey[:16]

	require.Error(
		t,
		ks.ValidateOpCert(),
		"ValidateOpCert accepted an operational certificate with a malformed cold vkey",
	)
}
