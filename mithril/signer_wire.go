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
	"fmt"

	"github.com/blinklabs-io/gouroboros/kes"
)

// kesSignatureDepth is the depth of the Sum KES scheme Cardano pool keys use.
const kesSignatureDepth = kes.CardanoKesDepth

// EncodeKESSignature returns the aggregator wire form of a Cardano Sum6 KES
// signature (as produced by kes.Sign): hex of nested JSON objects, one per
// tree level, each holding the inner signature and the two public keys that
// join its subtrees.
func EncodeKESSignature(raw []byte) (string, error) {
	if len(raw) != kes.SignatureSize(kesSignatureDepth) {
		return "", fmt.Errorf(
			"invalid KES signature length %d, want %d",
			len(raw),
			kes.SignatureSize(kesSignatureDepth),
		)
	}
	const keySize = 32
	const leafSize = 64
	type level struct {
		Sigma any           `json:"sigma"`
		LhsPK jsonByteArray `json:"lhs_pk"`
		RhsPK jsonByteArray `json:"rhs_pk"`
	}
	var sigma any = jsonByteArray(raw[:leafSize])
	offset := leafSize
	for range kesSignatureDepth {
		sigma = level{
			Sigma: sigma,
			LhsPK: raw[offset : offset+keySize],
			RhsPK: raw[offset+keySize : offset+2*keySize],
		}
		offset += 2 * keySize
	}
	return encodeSTMJSON(sigma)
}

// EncodeOperationalCertificate returns the aggregator wire form of an
// operational certificate: hex of the JSON tuple the reference signer
// serializes, ((KES key, issue number, KES start period, cold signature),
// cold key).
func EncodeOperationalCertificate(
	kesVKey []byte,
	issueNumber uint64,
	kesStartPeriod uint64,
	coldSignature []byte,
	coldVKey []byte,
) (string, error) {
	return encodeSTMJSON([]any{
		[]any{
			jsonByteArray(kesVKey),
			issueNumber,
			kesStartPeriod,
			jsonByteArray(coldSignature),
		},
		jsonByteArray(coldVKey),
	})
}
