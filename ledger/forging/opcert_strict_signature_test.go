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
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// identityColdVKey and identitySignature are the edwards25519 identity point
// and the signature (R = identity, S = 0) that crypto/ed25519.Verify accepts
// against it for any message: the equation [S]B == R + [h]A reduces to
// identity == identity. libsodium's crypto_sign_ed25519_verify_detached, which
// cardano-node reaches through Ed25519DSIGN, rejects both points as
// small-order.
var (
	identityColdVKey  = append([]byte{0x01}, make([]byte, 31)...)
	identitySignature = append(
		append([]byte{0x01}, make([]byte, 31)...),
		make([]byte, 32)...,
	)
)

func strictTestKESVKey() []byte {
	kesVKey := make([]byte, 32)
	for i := range kesVKey {
		kesVKey[i] = byte(i)
	}
	return kesVKey
}

// TestValidateOpCertAcceptsGenuineColdSignature keeps the rejection below
// conditional: a certificate whose cold key really did sign its own hot vkey,
// counter and period still validates.
func TestValidateOpCertAcceptsGenuineColdSignature(t *testing.T) {
	t.Parallel()

	kesVKey := strictTestKESVKey()
	coldVKey, coldSKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	pc := NewPoolCredentials()
	pc.kesVKey = kesVKey
	pc.opCert = &OpCert{
		KESVKey:     kesVKey,
		IssueNumber: 7,
		KESPeriod:   3,
		ColdVKey:    coldVKey,
		Signature: ed25519.Sign(
			coldSKey,
			lcommon.OpCertSignableBytes(kesVKey, 7, 3),
		),
	}

	require.NoError(t, pc.ValidateOpCert())
}

// TestValidateOpCertRejectsSmallOrderColdKey pins the criteria, not merely the
// presence, of the cold-signature check. The identity cold vkey with an
// all-zero S is accepted by crypto/ed25519 for any hot vkey, counter and
// period, so a node validating with it passes its own startup check and then
// forges blocks whose opcert every conformant peer rejects.
func TestValidateOpCertRejectsSmallOrderColdKey(t *testing.T) {
	t.Parallel()

	kesVKey := strictTestKESVKey()
	signable := lcommon.OpCertSignableBytes(kesVKey, 7, 3)
	require.True(
		t,
		ed25519.Verify(identityColdVKey, signable, identitySignature),
		"crypto/ed25519 no longer accepts the identity pair; this test no longer discriminates",
	)

	pc := NewPoolCredentials()
	pc.kesVKey = kesVKey
	pc.opCert = &OpCert{
		KESVKey:     kesVKey,
		IssueNumber: 7,
		KESPeriod:   3,
		ColdVKey:    identityColdVKey,
		Signature:   identitySignature,
	}

	require.ErrorContains(
		t,
		pc.ValidateOpCert(),
		"signature verification failed",
	)
}
