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
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

// credentialsRotationFixture loads the devnet VRF and KES keys under opcerts
// signed by one cold key, so each certificate belongs to the same pool and
// only its counter and start period differ.
type credentialsRotationFixture struct {
	vrfPath string
	kesPath string
	kesVKey []byte
	coldSK  ed25519.PrivateKey
	coldVK  ed25519.PublicKey
}

func newCredentialsRotationFixture(t *testing.T) *credentialsRotationFixture {
	t.Helper()
	vrfPath, kesPath, devnetOpCert := createTestKeys(t)
	devnet := NewPoolCredentials()
	require.NoError(t, devnet.LoadFromFiles(vrfPath, kesPath, devnetOpCert))
	coldVK, coldSK, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return &credentialsRotationFixture{
		vrfPath: vrfPath,
		kesPath: kesPath,
		kesVKey: devnet.GetKESVKey(),
		coldSK:  coldSK,
		coldVK:  coldVK,
	}
}

// opCert writes a certificate over the fixture's KES key with the given
// counter and start period, signed by the fixture's cold key.
func (f *credentialsRotationFixture) opCert(
	t *testing.T,
	counter uint64,
	kesPeriod uint64,
) string {
	t.Helper()
	var body [48]byte
	copy(body[:32], f.kesVKey)
	binary.BigEndian.PutUint64(body[32:40], counter)
	binary.BigEndian.PutUint64(body[40:48], kesPeriod)
	signature := ed25519.Sign(f.coldSK, body[:])
	certCbor, err := cbor.Encode([]any{
		[]any{f.kesVKey, counter, kesPeriod, signature},
		[]byte(f.coldVK),
	})
	require.NoError(t, err)
	envelope, err := json.Marshal(map[string]string{
		"type":    "NodeOperationalCertificate",
		"cborHex": hex.EncodeToString(certCbor),
	})
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "opcert.cert")
	require.NoError(t, os.WriteFile(path, envelope, 0o644))
	return path
}

// validated loads and validates credentials the way block producer startup
// does, at a slot inside the certificate's window.
func (f *credentialsRotationFixture) validated(
	t *testing.T,
	counter uint64,
) *PoolCredentials {
	t.Helper()
	pc := NewPoolCredentials()
	t.Cleanup(pc.Close)
	require.NoError(t, pc.LoadFromFiles(
		f.vrfPath, f.kesPath, f.opCert(t, counter, 0),
	))
	require.NoError(t, pc.ValidateOpCert())
	require.NoError(t, pc.ValidateKESPeriod(
		synthGenesis(100, 62, time.Second, time.Unix(0, 0)),
		0,
	))
	return pc
}

func requireZeroed(t *testing.T, data []byte, what string) {
	t.Helper()
	require.NotEmpty(t, data, "%s: precondition, the secret must exist", what)
	for _, b := range data {
		if b != 0 {
			require.Failf(
				t,
				"secret not zeroized",
				"%s still holds key bytes",
				what,
			)
		}
	}
}

func TestPoolCredentialsCloseZeroizesAndRefusesUse(t *testing.T) {
	t.Parallel()
	fixture := newCredentialsRotationFixture(t)
	pc := fixture.validated(t, 1)

	// Aliases onto the live secrets: what a heap scan would find.
	vrfSeed := pc.vrfSKey
	kesData := pc.kesSKey.Data
	inFlight := pc.acquireCredentialGeneration()
	defer inFlight.release()

	pc.Close()

	requireZeroed(t, vrfSeed, "VRF seed")
	requireZeroed(t, kesData, "KES secret key")
	require.False(t, pc.IsLoaded())
	require.Nil(t, pc.GetVRFSKey())
	_, _, err := pc.VRFProve([]byte("alpha"))
	require.Error(t, err, "a closed credential must not prove")
	_, err = pc.KESSign(0, []byte("body"))
	require.Error(t, err, "a closed credential must not sign")
	require.ErrorIs(
		t,
		inFlight.ensureCurrent(),
		errCredentialGenerationChanged,
		"an attempt holding a snapshot must be abandoned by Close",
	)

	// Whatever still holds a reference (a KES agent loop, a reload trigger)
	// must not be able to load key material back in.
	require.ErrorIs(t, pc.LoadFromFiles(
		fixture.vrfPath, fixture.kesPath, fixture.opCert(t, 2, 0),
	), errCredentialsClosed)
	require.ErrorIs(
		t,
		pc.ReplaceWith(fixture.validated(t, 2)),
		errCredentialsClosed,
	)
	require.False(t, pc.IsLoaded())

	pc.Close() // idempotent
}

func TestPoolCredentialsReplaceWithRotatesAtomically(t *testing.T) {
	t.Parallel()
	fixture := newCredentialsRotationFixture(t)
	live := fixture.validated(t, 1)
	next := NewPoolCredentials()
	t.Cleanup(next.Close)
	require.NoError(
		t,
		next.LoadFromFiles(
			fixture.vrfPath,
			fixture.kesPath,
			fixture.opCert(t, 2, 1),
		),
	)
	require.NoError(t, next.ValidateOpCert())
	require.NoError(
		t,
		next.ValidateKESPeriod(
			synthGenesis(100, 62, time.Second, time.Unix(0, 0)),
			100,
		),
	)
	oldKES := live.kesSKey.Data
	// An attempt that selected the outgoing material before the swap.
	inFlight := live.acquireCredentialGeneration()
	defer inFlight.release()

	require.NoError(t, live.ReplaceWith(next))

	require.Equal(t, uint64(2), live.GetOpCert().IssueNumber)
	require.True(t, live.IsLoaded())
	require.NotZero(
		t,
		live.OpCertExpiryPeriod(),
		"the validated KES lifetime must travel with the material",
	)
	requireZeroed(t, oldKES, "outgoing KES secret key")
	require.False(
		t,
		next.IsLoaded(),
		"the replacement's material is owned by the live credentials now",
	)
	// The in-flight attempt owns a private snapshot of the outgoing
	// material, which is still validly signed; the swap must not make it
	// forfeit its leader slot.
	require.NoError(t, inFlight.ensureCurrent())
	require.NoError(
		t,
		inFlight.updateKESPeriod(0),
		"outgoing snapshots must not evolve the replacement certificate",
	)
	_, err := inFlight.kesSign(0, []byte("body"))
	require.NoError(t, err)
	// New attempts see the new certificate.
	newGeneration := live.acquireCredentialGeneration()
	defer newGeneration.release()
	require.Equal(t, uint64(2), newGeneration.operationalCert.IssueNumber)
}

func TestPoolCredentialsReplaceWithRejectsAndKeepsLive(t *testing.T) {
	t.Parallel()
	fixture := newCredentialsRotationFixture(t)

	otherCold := newCredentialsRotationFixture(t)
	alternateVRF := NewPoolCredentials()
	t.Cleanup(alternateVRF.Close)
	require.NoError(
		t,
		alternateVRF.LoadFromFiles(
			createAlternateTestVRFKey(t),
			fixture.kesPath,
			fixture.opCert(t, 9, 0),
		),
	)
	require.NoError(t, alternateVRF.ValidateOpCert())
	require.NoError(
		t,
		alternateVRF.ValidateKESPeriod(
			synthGenesis(100, 62, time.Second, time.Unix(0, 0)),
			0,
		),
	)
	unvalidated := NewPoolCredentials()
	require.NoError(t, unvalidated.LoadFromFiles(
		fixture.vrfPath, fixture.kesPath, fixture.opCert(t, 5, 0),
	))
	for _, tc := range []struct {
		name    string
		next    *PoolCredentials
		wantErr string
	}{
		{"lower counter", fixture.validated(t, 2), "below the loaded counter"},
		{"other pool", otherCold.validated(t, 9), "pool or VRF identity"},
		{"other VRF", alternateVRF, "pool or VRF identity"},
		{"not validated", unvalidated, "not validated"},
		{"nil", nil, "replacement"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			live := fixture.validated(t, 3)
			before := live.acquireCredentialGeneration()
			defer before.release()
			vrfSeed := live.vrfSKey

			require.ErrorContains(t, live.ReplaceWith(tc.next), tc.wantErr)

			require.True(t, live.IsLoaded())
			require.Equal(t, uint64(3), live.GetOpCert().IssueNumber)
			require.NotZero(t, live.OpCertExpiryPeriod())
			require.NotEmpty(t, vrfSeed)
			require.NotEqual(t, make([]byte, len(vrfSeed)), vrfSeed,
				"a rejected replacement must not wipe the live secrets")
			require.NoError(t, before.ensureCurrent())
		})
	}
}

func TestPoolCredentialsReciprocalReplacementDoesNotDeadlock(t *testing.T) {
	t.Parallel()
	fixture := newCredentialsRotationFixture(t)
	first := fixture.validated(t, 1)
	second := fixture.validated(t, 1)
	t.Cleanup(first.Close)
	t.Cleanup(second.Close)
	start := make(chan struct{})
	done := make(chan struct{}, 2)
	go func() { <-start; _ = first.ReplaceWith(second); done <- struct{}{} }()
	go func() { <-start; _ = second.ReplaceWith(first); done <- struct{}{} }()
	close(start)
	for range 2 {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("reciprocal replacements deadlocked")
		}
	}
}
