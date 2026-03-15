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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseVerificationKeyTextEnvelope(t *testing.T) {
	key, err := ParseVerificationKey(`{
  "type": "PaymentVerificationKeyShelley_ed25519",
  "description": "Payment Verification Key",
  "cborHex": "5820ce13cd433cdcb3dfb00c04e216956aeb622dcd7f282b03304d9fc9de804723b2"
}`)
	require.NoError(t, err)
	require.Equal(t, "PaymentVerificationKeyShelley_ed25519", key.Type)
	require.Equal(t, "Payment Verification Key", key.Description)
	require.Equal(
		t,
		"ce13cd433cdcb3dfb00c04e216956aeb622dcd7f282b03304d9fc9de804723b2",
		key.RawKeyBytesHex(),
	)
}

func TestParseVerificationKeyRawHex(t *testing.T) {
	key, err := ParseVerificationKey(
		"ce13cd433cdcb3dfb00c04e216956aeb622dcd7f282b03304d9fc9de804723b2",
	)
	require.NoError(t, err)
	require.Equal(
		t,
		"ce13cd433cdcb3dfb00c04e216956aeb622dcd7f282b03304d9fc9de804723b2",
		key.RawKeyBytesHex(),
	)
}

func TestParseVerificationKeyMithrilJSONHex(t *testing.T) {
	// Hex-encoded JSON byte array that decodes to the same
	// ed25519 key used in the other test cases.
	const mithrilHexJSON = "5b3230362c31392c323035" +
		"2c36372c36302c3232302c3137392c323233" +
		"2c3137362c31322c342c3232362c32322c31" +
		"34392c3130362c3233352c39382c34352c32" +
		"30352c3132372c34302c34332c332c34382c" +
		"37372c3135392c3230312c3232322c313238" +
		"2c37312c33352c3137385d"
	key, err := ParseVerificationKey(mithrilHexJSON)
	require.NoError(t, err)
	require.Equal(
		t,
		"ce13cd433cdcb3dfb00c04e216956aeb"+
			"622dcd7f282b03304d9fc9de804723b2",
		key.RawKeyBytesHex(),
	)
}

func TestParseVerificationKeyInvalid(t *testing.T) {
	_, err := ParseVerificationKey("not-hex")
	require.Error(t, err)
}
