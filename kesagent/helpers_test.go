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

package kesagent

import (
	"encoding/hex"
	"net"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/bursa"
	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/stretchr/testify/require"
)

// testKESSKeyJSON/testOpCertJSON are the same devnet fixture pair
// ledger/forging/keys_test.go uses (config/cardano/devnet/keys/{kes,opcert}),
// so a real, already-validated KES key and operational certificate back
// these tests rather than synthetic bytes.
const (
	testKESSKeyJSON = `{
    "type": "KesSigningKey_ed25519_kes_2^6",
    "description": "KES Signing Key",
    "cborHex": "590260a199f16b11da6c7f5c1e0f1eb0b9bbe278d3d8f35bfd50d0951c2ff94d0344cd57df5f64c9bac1dd60b4482f9c636168f40737d526625a2ec82f22ec0c72de0013f86ef743a7bba0286db6ddf3d85bf8e49ddbf14d9d3b7ee22f4857c77b740948f84f2e72f6bcf91f405e34ea50a2c53fa4876b43cfce2bcfe87c06a903de8bb33d968ca7930b67d0c23f5cb2d74e422d773ba80e388de384691000d6ba8a9b4dc7d3187f76048fbef9a52b72d80d835bb76eced7c0e0cdc5b58869b73c095dffa01db4ff51765afcead565395a5ed1cf74e5f2134d61076fece21aacd080bbbfaab94125401d7bbc74eafc7e7e3a2235f59dc03d6e332e53d558493a1e22213b92c77b1328ff1b83855da704fc366bf4415490602481d1939136eeaf252c65184912a779d9d94a90e32b72c1877ef60b6d79e707ce5a762acb4bed46436efe4fe62aae50b39068cc508a09427c92791cbcbea44318529cc68d297ca24e1b73b2394c385ec63fcd85ed56eec3de48860a1ec950aad4f91cbf741dbd7bf1d3c278875bd20e31ff5372339f6aa5280ad9b8bf3514889ac44600fe57ca0b535d6dc6b0b981e079595aad186ee0be9b07e837391ab165e4ca406601c876a86e246a3f53311e21199cccc0b080f28d18f4dc6987731e10e4ade00df7c6921c5ef3022b6f49a29ba307a2c8f4bd2ba42fcfa0aad68a2f0ad31fff69a99d3471f9036d3f5817a3edfeff7fc3c14e1151d767aaa043481cfd1a6ee55e8e5d7853ecdaf9da2bb36c716beae8d706bc648a790d4697e1d044a11a49f305ab8bc64a094bd81bda7395fe6f77dd5557c39919dd9bb9cf22a87fe47408ae3ec2247007d015a5"
}`

	// The raw CBOR bytes of the opcert (KESVKey=4cd49bb0..., IssueNumber=0,
	// KESPeriod=0), matching testOpCertJSON's cborHex with its cardano-cli
	// text-envelope wrapper stripped. This is what the bursa wire protocol's
	// KeyPush.OpCert field carries directly.
	testOpCertCBORHex = "828458204cd49bb05e9885142fe7af1481107995298771fd1a24e72b506a4d600ee2b3120000584089fc9e9f551b2ea873bf31643659d049152d5c8e8de86be4056370bccc5fa62dd12e3f152f1664e614763e46eaa7a17ed366b5cef19958773d1ab96941442e0b58205a3d778e76741a009e29d23093cfe046131808d34d7c864967b515e98dfc3583"
)

// testKESMaterial loads the fixture KES secret key and decodes the fixture
// opcert, returning everything a fake agent server needs to build real
// KeyPush/SignResponse frames.
func testKESMaterial(t testing.TB) (skeyData, vkey []byte, opCertCBOR []byte) {
	t.Helper()
	key, err := bursa.LoadKeyFromBytes([]byte(testKESSKeyJSON))
	require.NoError(t, err)
	require.Len(t, key.SKey, kes.CardanoKesSecretKeySize)

	opCertCBOR, err = hex.DecodeString(testOpCertCBORHex)
	require.NoError(t, err)
	decoded, err := bursa.DecodeOpCert(opCertCBOR)
	require.NoError(t, err)

	return key.SKey, decoded.KESVKey, opCertCBOR
}

// listenUnix opens a Unix-domain listener at a fresh path under t.TempDir().
func listenUnix(t testing.TB) (net.Listener, string) {
	t.Helper()
	sockPath := filepath.Join(t.TempDir(), "kes-agent.sock")
	ln, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	return ln, sockPath
}

// sendHello writes a real Hello frame to conn.
func sendHello(t testing.TB, conn net.Conn, mode string) {
	t.Helper()
	require.NoError(
		t,
		writeFrame(conn, MaxHelloFrameLen, Hello{Protocol: ProtocolID, Mode: mode}),
	)
}
