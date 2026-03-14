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

package mithril

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"testing"

	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
	"github.com/stretchr/testify/require"
)

const stmGoldenAggregateVerificationKeyJSON = `{
  "mt_commitment": {
    "root": [56,37,95,107,157,98,252,194,190,204,170,26,224,10,212,7,214,89,116,196,217,122,111,56,113,253,96,45,170,121,235,159],
    "nr_leaves": 2,
    "hasher": null
  },
  "total_stake": 2
}`

const stmGoldenAggregateSignatureJSON = `{
  "signatures": [
    [
      {
        "sigma": [149,157,201,187,140,54,0,128,209,88,16,203,61,78,77,98,161,133,58,152,29,74,217,113,64,100,10,161,186,167,133,114,211,153,218,56,223,84,105,242,41,54,224,170,208,185,126,83],
        "indexes": [1,4,5,8],
        "signer_index": 0
      },
      [
        [143,161,255,48,78,57,204,220,25,221,164,252,248,14,56,126,186,135,228,188,145,181,52,200,97,99,213,46,0,199,193,89,187,88,29,135,173,244,86,36,83,54,67,164,6,137,94,72,6,105,128,128,93,48,176,11,4,246,138,48,180,133,90,142,192,24,193,111,142,31,76,111,110,234,153,90,208,192,31,124,95,102,49,158,99,52,220,165,94,251,68,69,121,16,224,194],
        1
      ]
    ],
    [
      {
        "sigma": [149,169,22,201,216,97,163,188,115,210,217,236,233,161,201,13,42,132,12,63,5,31,120,22,78,177,125,134,208,205,73,58,247,141,59,62,187,81,213,30,153,218,41,42,110,156,161,205],
        "indexes": [0,3,6],
        "signer_index": 1
      },
      [
        [145,56,175,32,122,187,214,226,251,148,88,9,1,103,159,146,80,166,107,243,251,236,41,28,111,128,207,164,132,147,228,83,246,228,170,68,89,78,60,28,123,130,88,234,38,97,42,65,1,100,53,18,78,131,8,61,122,131,238,84,233,223,154,118,118,73,28,27,101,78,80,233,123,206,220,174,134,205,71,110,112,180,97,98,0,113,69,145,231,168,43,173,172,56,104,208],
        1
      ]
    ]
  ],
  "batch_proof": {
    "values": [],
    "indices": [0,1],
    "hasher": null
  }
}`

const stmGoldenSignerVerificationKeyJSON = `{
  "vk": [143,161,255,48,78,57,204,220,25,221,164,252,248,14,56,126,186,135,228,188,145,181,52,200,97,99,213,46,0,199,193,89,187,88,29,135,173,244,86,36,83,54,67,164,6,137,94,72,6,105,128,128,93,48,176,11,4,246,138,48,180,133,90,142,192,24,193,111,142,31,76,111,110,234,153,90,208,192,31,124,95,102,49,158,99,52,220,165,94,251,68,69,121,16,224,194],
  "pop": [168,50,233,193,15,136,65,72,123,148,129,176,38,198,209,47,28,204,176,144,57,251,42,28,66,76,89,97,158,63,54,198,194,176,135,221,14,185,197,225,202,98,243,74,233,225,143,151,147,177,170,117,66,165,66,62,33,216,232,75,68,114,195,22,100,65,44,198,4,166,102,233,253,240,59,175,60,117,142,114,140,122,17,87,110,187,1,17,10,195,154,13,249,86,54,226]
}`

func testCreateEncodedSTMProof(
	t *testing.T,
	msg []byte,
) (string, string) {
	t.Helper()

	sk, err := rand.Int(rand.Reader, fr.Modulus())
	require.NoError(t, err)
	if sk.Sign() == 0 {
		sk = big.NewInt(1)
	}
	_, _, _, g2 := bls12381.Generators()

	var vkJac bls12381.G2Jac
	vkJac.FromAffine(&g2).ScalarMultiplication(&vkJac, sk)
	var vkAff bls12381.G2Affine
	vkAff.FromJacobian(&vkJac)
	vkBytes := vkAff.Bytes()

	leafBytes := append(append([]byte(nil), vkBytes[:]...), uint64ToBigEndianBytes(1)...)
	root := stmBlake2b256(leafBytes)
	msgp := append(append([]byte(nil), msg...), root...)
	h, err := bls12381.HashToG1(msgp, stmBLSDomainSeparationTag)
	require.NoError(t, err)
	var sigJac bls12381.G1Jac
	sigJac.FromAffine(&h).ScalarMultiplication(&sigJac, sk)
	var sigAff bls12381.G1Affine
	sigAff.FromJacobian(&sigJac)
	sigBytes := sigAff.Bytes()

	avkJSON, err := json.Marshal(
		map[string]any{
			"mt_commitment": map[string]any{
				"root":      root,
				"nr_leaves": 1,
				"hasher":    nil,
			},
			"total_stake": 1,
		},
	)
	require.NoError(t, err)
	sigJSON, err := json.Marshal(
		map[string]any{
			"signatures": []any{
				[]any{
					map[string]any{
						"sigma":        sigBytes[:],
						"indexes":      []uint64{0},
						"signer_index": 0,
					},
					[]any{
						vkBytes[:],
						1,
					},
				},
			},
			"batch_proof": map[string]any{
				"values":  []any{},
				"indices": []int{0},
				"hasher":  nil,
			},
		},
	)
	require.NoError(t, err)
	return hex.EncodeToString(avkJSON), hex.EncodeToString(sigJSON)
}

func TestParseSTMSignerVerificationKeyGolden(t *testing.T) {
	encoded := hex.EncodeToString([]byte(stmGoldenSignerVerificationKeyJSON))
	vkBytes, err := parseSTMSignerVerificationKey(encoded)
	require.NoError(t, err)
	require.Len(t, vkBytes, 96)
}

func TestVerifySTMSignatureGolden(t *testing.T) {
	msg := make([]byte, 16)
	encodedAVK := hex.EncodeToString([]byte(stmGoldenAggregateVerificationKeyJSON))
	encodedSig := hex.EncodeToString([]byte(stmGoldenAggregateSignatureJSON))
	err := verifySTMSignature(
		msg,
		encodedAVK,
		encodedSig,
		ProtocolParameters{K: 5, M: 10, PhiF: 0.8},
	)
	require.NoError(t, err)
}

func TestVerifySTMSignatureRejectsWrongMessage(t *testing.T) {
	msg := make([]byte, 16)
	msg[0] = 1
	encodedAVK := hex.EncodeToString([]byte(stmGoldenAggregateVerificationKeyJSON))
	encodedSig := hex.EncodeToString([]byte(stmGoldenAggregateSignatureJSON))
	err := verifySTMSignature(
		msg,
		encodedAVK,
		encodedSig,
		ProtocolParameters{K: 5, M: 10, PhiF: 0.8},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "lottery lost")
}

func TestVerifySTMSignatureRejectsTamperedBatchProof(t *testing.T) {
	var sigObj map[string]any
	require.NoError(t, json.Unmarshal([]byte(stmGoldenAggregateSignatureJSON), &sigObj))
	batchProof := sigObj["batch_proof"].(map[string]any)
	batchProof["indices"] = []any{1.0, 0.0}
	raw, err := json.Marshal(sigObj)
	require.NoError(t, err)

	msg := make([]byte, 16)
	encodedAVK := hex.EncodeToString([]byte(stmGoldenAggregateVerificationKeyJSON))
	encodedSig := hex.EncodeToString(raw)
	err = verifySTMSignature(
		msg,
		encodedAVK,
		encodedSig,
		ProtocolParameters{K: 5, M: 10, PhiF: 0.8},
	)
	require.Error(t, err)
	require.Contains(
		t,
		err.Error(),
		"merkle batch proof indices are not sorted",
	)
}

func TestVerifySTMSignatureRejectsZeroK(t *testing.T) {
	msg := make([]byte, 16)
	encodedAVK := hex.EncodeToString(
		[]byte(stmGoldenAggregateVerificationKeyJSON),
	)
	encodedSig := hex.EncodeToString(
		[]byte(stmGoldenAggregateSignatureJSON),
	)
	err := verifySTMSignature(
		msg,
		encodedAVK,
		encodedSig,
		ProtocolParameters{K: 0, M: 10, PhiF: 0.8},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "K=0")
}

func TestParseSTMAggregateVerificationKeyRejectsZeroLeaves(t *testing.T) {
	// Build a binary-format AVK: 8 bytes nrLeaves + 32 bytes root + 8 bytes totalStake.
	var buf [48]byte
	// nrLeaves = 0 (first 8 bytes already zero).
	// root = arbitrary 32 bytes (bytes 8..39).
	buf[8] = 0xAB
	// totalStake = 1 (last 8 bytes).
	buf[47] = 1
	encoded := hex.EncodeToString(buf[:])
	_, err := parseSTMAggregateVerificationKey(encoded)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nrLeaves must be positive")
}
