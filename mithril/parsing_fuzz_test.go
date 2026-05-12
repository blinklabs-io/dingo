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
	"encoding/hex"
	"testing"
)

func FuzzParseVerificationKey(f *testing.F) {
	f.Add("")
	f.Add("0x00")
	f.Add("ce13cd433cdcb3dfb00c04e216956aeb622dcd7f282b03304d9fc9de804723b2")
	f.Add(`{
  "type": "PaymentVerificationKeyShelley_ed25519",
  "description": "Payment Verification Key",
  "cborHex": "5820ce13cd433cdcb3dfb00c04e216956aeb622dcd7f282b03304d9fc9de804723b2"
}`)

	f.Fuzz(func(t *testing.T, data string) {
		if len(data) > 64*1024 {
			t.Skip("verification key input is too large for fast fuzzing")
		}

		key, err := ParseVerificationKey(data)
		if err != nil {
			return
		}
		if key == nil {
			t.Fatalf("ParseVerificationKey returned nil without an error")
		}
		if len(key.RawKeyBytes) == 0 {
			t.Fatalf("ParseVerificationKey returned an empty raw key")
		}
		if _, err := hex.DecodeString(key.RawKeyBytesHex()); err != nil {
			t.Fatalf("RawKeyBytesHex is not valid hex: %v", err)
		}
	})
}

func FuzzDecodePrimaryEncodedBytes(f *testing.F) {
	f.Add("")
	f.Add("00ff")
	f.Add("AP8=")
	f.Add("AP8")

	f.Fuzz(func(t *testing.T, data string) {
		if len(data) > 64*1024 {
			t.Skip("encoded input is too large for fast fuzzing")
		}

		decoded, ok := decodePrimaryEncodedBytes(data)
		if !ok {
			return
		}
		if decoded == nil {
			t.Fatalf("decodePrimaryEncodedBytes returned nil with ok=true")
		}
	})
}

func FuzzParseSTMSignerVerificationKey(f *testing.F) {
	f.Add("")
	f.Add(hex.EncodeToString(make([]byte, 96)))
	f.Add(hex.EncodeToString([]byte(`{"vk":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","pop":""}`)))

	f.Fuzz(func(t *testing.T, data string) {
		if len(data) > 64*1024 {
			t.Skip("signer key input is too large for fast fuzzing")
		}

		_, err := parseSTMSignerVerificationKey(data)
		if err != nil {
			return
		}
	})
}

func FuzzParseSTMAggregateSignatureBytes(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte{0})
	f.Add(append([]byte{0}, make([]byte, 8)...))

	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 64*1024 {
			t.Skip("aggregate signature input is too large for fast fuzzing")
		}

		sig, err := parseSTMAggregateSignatureBytes(raw)
		if err != nil {
			return
		}
		if sig == nil {
			t.Fatalf("parseSTMAggregateSignatureBytes returned nil without an error")
		}
	})
}
