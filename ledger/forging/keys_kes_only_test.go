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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/bursa"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPoolCredentialsLoadKESFromFiles(t *testing.T) {
	t.Parallel()
	_, kesPath, opCertPath := createTestKeys(t)
	genesis := synthGenesis(
		129600, 62, time.Second,
		time.Date(2017, 9, 23, 21, 44, 51, 0, time.UTC),
	)

	pc := NewPoolCredentials()
	require.NoError(t, pc.LoadKESFromFiles(kesPath, opCertPath))

	// The KES material is installed and checkable, and there is no VRF key.
	assert.False(t, pc.IsLoaded())
	assert.Empty(t, pc.GetVRFVKey())
	assert.Len(t, pc.GetKESVKey(), 32)
	assert.Equal(t, pc.GetKESVKey(), pc.GetOpCert().KESVKey)
	assert.NotEqual(t, [28]byte{}, [28]byte(pc.GetPoolID()))
	require.NoError(t, pc.ValidateOpCert())
	require.NoError(t, pc.ValidateKESPeriod(genesis, 3*129600))

	// Signing at an absolute period evolves the key relative to the opcert.
	const period = 3
	require.NoError(t, pc.UpdateKESPeriod(period))
	msg := []byte("message")
	sig, err := pc.KESSign(period, msg)
	require.NoError(t, err)
	assert.True(t, kes.VerifySignedKES(pc.GetKESVKey(), period, msg, sig))
}

func TestPoolCredentialsLoadKESFromFilesRejectsBadMaterial(t *testing.T) {
	t.Parallel()
	_, kesPath, opCertPath := createTestKeys(t)
	otherKESPath := createAlternateTestKESKey(t)

	pc := NewPoolCredentials()
	err := pc.LoadKESFromFiles(otherKESPath, opCertPath)
	require.ErrorContains(t, err, "KES verification key mismatch")
	assert.Empty(t, pc.GetKESVKey())

	err = pc.LoadKESFromFiles(
		filepath.Join(t.TempDir(), "missing.skey"),
		opCertPath,
	)
	require.ErrorContains(t, err, "failed to load KES signing key")

	err = pc.LoadKESFromFiles(kesPath, filepath.Join(t.TempDir(), "missing"))
	require.ErrorContains(t, err, "failed to load operational certificate")
}

// createAlternateTestKESKey writes a valid KES signing key whose public key
// differs from the one the test operational certificate names.
func createAlternateTestKESKey(t *testing.T) string {
	t.Helper()
	seed := make([]byte, 32)
	for i := range seed {
		seed[i] = byte(i + 1)
	}
	secretKey, _, err := bursa.GetKESKeyPair(seed)
	require.NoError(t, err)
	keyFile, err := bursa.GetKESSKey(secretKey)
	require.NoError(t, err)
	data, err := json.Marshal(keyFile)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "alternate-kes.skey")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	testutil.RestrictFileToCurrentUser(t, path)
	return path
}
