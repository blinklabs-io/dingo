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
	"time"

	"github.com/blinklabs-io/bursa"
	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/stretchr/testify/require"

	"testing"
)

// fakeRemoteKESSigner is a minimal RemoteKESSigner for exercising the
// agent-backed kesSign/updateKESPeriod paths without a real kesagent.Client.
type fakeRemoteKESSigner struct {
	signFunc func(period uint64, message []byte) ([]byte, error)
	calls    []uint64
}

func (f *fakeRemoteKESSigner) Sign(
	period uint64,
	message []byte,
) ([]byte, error) {
	f.calls = append(f.calls, period)
	if f.signFunc != nil {
		return f.signFunc(period, message)
	}
	return append([]byte(nil), message...), nil
}

// TestCredentialGenerationKesSignRejectsExpiredPeriod proves the
// opcert-lifetime gate applies inside kesSign itself, not only at its callers
// (BlockForger.SignBlockHeader, DefaultBlockBuilder.buildBlock). dingo#3115's
// KES agent client bypassed exactly this: it signed through a direct call to
// the agent instead of through this method, so the opcert-lifetime check both
// of those callers otherwise rely on never ran for the agent path.
//
// Evolving to period 5 (via updateKESPeriod, which does not itself check
// expiry -- only kesSign does) keeps the KES key within its own 2^6
// cryptographic capacity, so kes.Sign's unrelated "key is at this period"
// check cannot be what rejects the sign below: only the opcert-lifetime
// policy gate can be.
func TestCredentialGenerationKesSignRejectsExpiredPeriod(t *testing.T) {
	t.Parallel()

	vrfPath, kesPath, opCertPath := createTestKeys(t)
	pc := NewPoolCredentials()
	require.NoError(t, pc.LoadFromFiles(vrfPath, kesPath, opCertPath))
	// maxKESEvolutions=3 -> validated lifetime is periods [0, 3).
	require.NoError(t, pc.ValidateKESPeriod(
		synthGenesis(1, 3, time.Second, time.Unix(0, 0)),
		0,
	))

	generation := pc.acquireCredentialGeneration()
	defer generation.release()
	require.Equal(t, uint64(0), generation.opCertStartKES)
	require.Equal(t, uint64(3), generation.opCertExpiryKES)

	const expiredPeriod = 5
	require.NoError(t, generation.updateKESPeriod(expiredPeriod))

	_, err := generation.kesSign(expiredPeriod, []byte("header"))
	require.ErrorIs(t, err, errOpCertExpired)
}

// TestCredentialGenerationKesSignAgentPathRejectsExpiredPeriod is the same
// proof for the agent-backed ("sign" mode) signing path: the gate must apply
// identically whether or not a remote signer is installed, and the agent must
// never even be asked to sign a period the opcert has not authorized.
func TestCredentialGenerationKesSignAgentPathRejectsExpiredPeriod(
	t *testing.T,
) {
	t.Parallel()

	vrfPath, _, opCertPath := createTestKeys(t)
	signer := &fakeRemoteKESSigner{}
	pc := NewPoolCredentials()
	require.NoError(t, pc.LoadFromAgentSign(vrfPath, opCertPath, signer))
	require.NoError(t, pc.ValidateKESPeriod(
		synthGenesis(1, 3, time.Second, time.Unix(0, 0)),
		0,
	))

	generation := pc.acquireCredentialGeneration()
	defer generation.release()

	const expiredPeriod = 5
	require.NoError(t, generation.updateKESPeriod(expiredPeriod))

	_, err := generation.kesSign(expiredPeriod, []byte("header"))
	require.ErrorIs(t, err, errOpCertExpired)
	require.Empty(
		t,
		signer.calls,
		"agent must not be asked to sign a period outside the validated opcert lifetime",
	)
}

// TestCredentialGenerationKesSignAgentPathDelegatesWithinLifetime proves the
// positive case alongside the negative one above: a period the opcert does
// authorize reaches the remote signer, carrying the caller's ABSOLUTE period
// unchanged (matching the bursa KES agent sign-mode wire protocol, which also
// takes an absolute period and translates internally).
func TestCredentialGenerationKesSignAgentPathDelegatesWithinLifetime(
	t *testing.T,
) {
	t.Parallel()

	vrfPath, _, opCertPath := createTestKeys(t)
	wantSig := []byte("agent-signature")
	signer := &fakeRemoteKESSigner{
		signFunc: func(uint64, []byte) ([]byte, error) {
			return wantSig, nil
		},
	}
	pc := NewPoolCredentials()
	require.NoError(t, pc.LoadFromAgentSign(vrfPath, opCertPath, signer))
	require.NoError(t, pc.ValidateKESPeriod(
		synthGenesis(1, 3, time.Second, time.Unix(0, 0)),
		0,
	))

	generation := pc.acquireCredentialGeneration()
	defer generation.release()

	require.NoError(t, generation.updateKESPeriod(1))
	sig, err := generation.kesSign(1, []byte("header"))
	require.NoError(t, err)
	require.Equal(t, wantSig, sig)
	require.Equal(t, []uint64{1}, signer.calls)
}

// TestCredentialGenerationUpdateKESPeriodAgentPathRejectsBackward proves the
// agent-backed path enforces the same never-evolve-backward invariant the
// local-key path enforces in updateKESPeriodUnsafe/credentialGeneration.
func TestCredentialGenerationUpdateKESPeriodAgentPathRejectsBackward(
	t *testing.T,
) {
	t.Parallel()

	vrfPath, _, opCertPath := createTestKeys(t)
	pc := NewPoolCredentials()
	require.NoError(
		t,
		pc.LoadFromAgentSign(vrfPath, opCertPath, &fakeRemoteKESSigner{}),
	)
	require.NoError(t, pc.ValidateKESPeriod(
		synthGenesis(1, 3, time.Second, time.Unix(0, 0)),
		0,
	))

	generation := pc.acquireCredentialGeneration()
	defer generation.release()

	require.NoError(t, generation.updateKESPeriod(2))
	require.ErrorContains(
		t,
		generation.updateKESPeriod(1),
		"cannot evolve KES period backward",
	)
}

// TestPoolCredentialsLoadFromAgentServeKeyMatchesLocalPath proves serve-key
// material installs through the same identity/generation path LoadFromFiles
// uses, so it is signable, opcert-validatable, and produces a signature that
// verifies against the pushed KES verification key -- indistinguishable from
// a local key file once installed.
func TestPoolCredentialsLoadFromAgentServeKeyMatchesLocalPath(t *testing.T) {
	t.Parallel()

	vrfPath, kesPath, opCertPath := createTestKeys(t)
	kesKey, err := loadSecretKeyFromFile(kesPath)
	require.NoError(t, err)
	opCertKey, err := bursa.LoadKeyFromFile(opCertPath)
	require.NoError(t, err)

	material := AgentKESMaterial{
		AbsolutePeriod: opCertKey.OpCertKesPeriod,
		KESSKeyData:    kesKey.SKey,
		KESVKey:        opCertKey.VKey,
		OpCert: OpCert{
			KESVKey:     opCertKey.VKey,
			IssueNumber: opCertKey.OpCertIssueNumber,
			KESPeriod:   opCertKey.OpCertKesPeriod,
			Signature:   opCertKey.OpCertSignature,
			ColdVKey:    opCertKey.OpCertColdVKey,
		},
	}

	pc := NewPoolCredentials()
	require.NoError(t, pc.LoadFromAgentServeKey(vrfPath, material))
	require.True(t, pc.IsLoaded())
	require.NoError(t, pc.ValidateOpCert())
	require.NoError(t, pc.ValidateKESPeriod(
		synthGenesis(1, 3, time.Second, time.Unix(0, 0)),
		0,
	))

	generation := pc.acquireCredentialGeneration()
	defer generation.release()
	require.NoError(t, generation.updateKESPeriod(0))
	sig, err := generation.kesSign(0, []byte("header"))
	require.NoError(t, err)
	require.True(
		t,
		kes.VerifySignedKES(opCertKey.VKey, 0, []byte("header"), sig),
	)
}

// TestPoolCredentialsLoadFromAgentServeKeyRejectsVKeyMismatch proves a pushed
// key whose verification key does not match its own operational certificate
// is refused rather than installed (P1: "pushed key material accepted
// without validation").
func TestPoolCredentialsLoadFromAgentServeKeyRejectsVKeyMismatch(t *testing.T) {
	t.Parallel()

	vrfPath, kesPath, opCertPath := createTestKeys(t)
	kesKey, err := loadSecretKeyFromFile(kesPath)
	require.NoError(t, err)
	opCertKey, err := bursa.LoadKeyFromFile(opCertPath)
	require.NoError(t, err)

	wrongVKey := append([]byte(nil), opCertKey.VKey...)
	wrongVKey[0] ^= 0xFF

	material := AgentKESMaterial{
		AbsolutePeriod: opCertKey.OpCertKesPeriod,
		KESSKeyData:    kesKey.SKey,
		KESVKey:        wrongVKey,
		OpCert: OpCert{
			KESVKey:     opCertKey.VKey,
			IssueNumber: opCertKey.OpCertIssueNumber,
			KESPeriod:   opCertKey.OpCertKesPeriod,
			Signature:   opCertKey.OpCertSignature,
			ColdVKey:    opCertKey.OpCertColdVKey,
		},
	}

	pc := NewPoolCredentials()
	err = pc.LoadFromAgentServeKey(vrfPath, material)
	require.ErrorContains(t, err, "does not match OpCert KES vkey")
	require.False(t, pc.IsLoaded())
}

// TestPoolCredentialsLoadFromAgentServeKeyRejectsWrongKeySize proves an
// undersized/oversized pushed secret key is refused before installation.
func TestPoolCredentialsLoadFromAgentServeKeyRejectsWrongKeySize(t *testing.T) {
	t.Parallel()

	vrfPath, kesPath, opCertPath := createTestKeys(t)
	kesKey, err := loadSecretKeyFromFile(kesPath)
	require.NoError(t, err)
	opCertKey, err := bursa.LoadKeyFromFile(opCertPath)
	require.NoError(t, err)

	material := AgentKESMaterial{
		AbsolutePeriod: opCertKey.OpCertKesPeriod,
		KESSKeyData:    kesKey.SKey[:len(kesKey.SKey)-1],
		KESVKey:        opCertKey.VKey,
		OpCert: OpCert{
			KESVKey:     opCertKey.VKey,
			IssueNumber: opCertKey.OpCertIssueNumber,
			KESPeriod:   opCertKey.OpCertKesPeriod,
			Signature:   opCertKey.OpCertSignature,
			ColdVKey:    opCertKey.OpCertColdVKey,
		},
	}

	pc := NewPoolCredentials()
	err = pc.LoadFromAgentServeKey(vrfPath, material)
	require.ErrorContains(t, err, "invalid agent KES key size")
	require.False(t, pc.IsLoaded())
}

// TestPoolCredentialsLoadFromAgentSignRequiresSigner proves a nil signer is
// refused rather than silently leaving PoolCredentials in an unsignable state.
func TestPoolCredentialsLoadFromAgentSignRequiresSigner(t *testing.T) {
	t.Parallel()

	vrfPath, _, opCertPath := createTestKeys(t)
	pc := NewPoolCredentials()
	err := pc.LoadFromAgentSign(vrfPath, opCertPath, nil)
	require.ErrorContains(t, err, "requires a non-nil signer")
}
