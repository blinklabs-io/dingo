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

package leios

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Golden vector generated with the reference tooling:
//
//	cardano-cli dijkstra node key-gen-BLS \
//	  --verification-key-file bls.vkey --signing-key-file bls.skey
//
// (cardano-node prototype-2026w31 / cardano-cli 11.1.0.0). It pins the
// text-envelope signing-key format Dingo must accept and asserts that the G2
// public key gnark-crypto derives from the loaded scalar is byte-identical to
// the verification key cardano-cli (blst) produced for the same key pair. A
// regression here would mean Dingo-signed Leios votes verify against a
// different public key than the network holds.
const (
	goldenBLSSigningKeyEnvelope = `{
    "type": "BlsSigningKey_bls12-381-BLS-Signature-Mininimal-Signature-Size",
    "description": "BLS12-381 signing key",
    "cborHex": "58205f5cb4fc11f53a49c8b687a27ed99c80b0635876d1231fb6f60fe7da5cd13634"
}`
	// Verification-key envelope cborHex: 0x5860 (CBOR byte string, len 96)
	// followed by the 96-byte compressed G2 public key.
	goldenBLSVerificationKeyCborHex = "5860" +
		"aa75d7e0762ecac2576fe14666c382bde4e5bcd68edafb615e75e03e8d087b36" +
		"cb5be8bfa605a3dc983bc170cf9465250cd234533c1eba39db036aadd701f60c" +
		"2458e1a2f08c2cc3d71b7d60c0aa844c2a3a9bc2b6c56e6936322e3959ec4ca8"
)

func TestLoadVoteSigningKeyFileTextEnvelopeMatchesReferenceVKey(t *testing.T) {
	vkeyEnvelope, err := hex.DecodeString(goldenBLSVerificationKeyCborHex)
	require.NoError(t, err)
	require.Equal(t, byte(0x58), vkeyEnvelope[0])
	require.Equal(t, byte(0x60), vkeyEnvelope[1]) // 96-byte byte string
	wantPub := vkeyEnvelope[2:]
	require.Len(t, wantPub, 96)

	// exercise the full public loader (incl. envelope detection) via a file
	dir := t.TempDir()
	path := filepath.Join(dir, "bls.skey")
	require.NoError(t, os.WriteFile(path, []byte(goldenBLSSigningKeyEnvelope), 0o600))

	key, err := LoadVoteSigningKeyFile(path)
	require.NoError(t, err, "text-envelope key-gen-BLS file must load")

	require.Equal(t,
		hex.EncodeToString(wantPub),
		hex.EncodeToString(key.PublicKeyBytes()),
		"gnark-derived G2 public key must equal the cardano-cli/blst verification key",
	)
}
