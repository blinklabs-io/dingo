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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package blockfrost

import (
	"encoding/hex"
	"strings"
	"testing"

	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/btcsuite/btcd/btcutil/bech32"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// vrfKey and hotVkey are arbitrary 32-byte values used to exercise the header
// field extraction. The exact bytes don't matter, only that they round-trip.
var (
	testVRFKey  = mustHex("00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff")
	testHotVkey = mustHex("ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100")
)

func mustHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

// expectedVRFBech32 encodes the test VRF key exactly as praosHeaderFields
// should, giving the tests a reference value.
func expectedVRFBech32(t *testing.T) string {
	t.Helper()
	conv, err := bech32.ConvertBits(testVRFKey, 8, 5, true)
	require.NoError(t, err)
	encoded, err := bech32.Encode("vrf_vk", conv)
	require.NoError(t, err)
	return encoded
}

func TestPraosHeaderFieldsShelley(t *testing.T) {
	var h shelley.ShelleyBlockHeader
	h.Body.VrfKey = testVRFKey
	h.Body.OpCertHotVkey = testHotVkey
	h.Body.OpCertSequenceNumber = 7

	vrf, opCert, counter := praosHeaderFields(&h)

	require.NotNil(t, vrf)
	assert.Equal(t, expectedVRFBech32(t), *vrf)
	assert.True(t, strings.HasPrefix(*vrf, "vrf_vk1"))
	require.NotNil(t, opCert)
	assert.Equal(t, hex.EncodeToString(testHotVkey), *opCert)
	require.NotNil(t, counter)
	assert.Equal(t, "7", *counter)
}

func TestPraosHeaderFieldsAllegraTPraos(t *testing.T) {
	// Allegra embeds the Shelley header body (TPraos era).
	var h allegra.AllegraBlockHeader
	h.Body.VrfKey = testVRFKey
	h.Body.OpCertHotVkey = testHotVkey
	h.Body.OpCertSequenceNumber = 0

	vrf, opCert, counter := praosHeaderFields(&h)

	require.NotNil(t, vrf)
	assert.Equal(t, expectedVRFBech32(t), *vrf)
	require.NotNil(t, opCert)
	assert.Equal(t, hex.EncodeToString(testHotVkey), *opCert)
	require.NotNil(t, counter)
	assert.Equal(t, "0", *counter)
}

func TestPraosHeaderFieldsBabbage(t *testing.T) {
	var h babbage.BabbageBlockHeader
	h.Body.VrfKey = testVRFKey
	h.Body.OpCert.HotVkey = testHotVkey
	h.Body.OpCert.SequenceNumber = 42

	vrf, opCert, counter := praosHeaderFields(&h)

	require.NotNil(t, vrf)
	assert.Equal(t, expectedVRFBech32(t), *vrf)
	require.NotNil(t, opCert)
	assert.Equal(t, hex.EncodeToString(testHotVkey), *opCert)
	require.NotNil(t, counter)
	assert.Equal(t, "42", *counter)
}

func TestPraosHeaderFieldsConway(t *testing.T) {
	var h conway.ConwayBlockHeader
	h.Body.VrfKey = testVRFKey
	h.Body.OpCert.HotVkey = testHotVkey
	h.Body.OpCert.SequenceNumber = 123

	vrf, opCert, counter := praosHeaderFields(&h)

	require.NotNil(t, vrf)
	assert.Equal(t, expectedVRFBech32(t), *vrf)
	require.NotNil(t, opCert)
	assert.Equal(t, hex.EncodeToString(testHotVkey), *opCert)
	require.NotNil(t, counter)
	assert.Equal(t, "123", *counter)
}

// TestPraosHeaderFieldsByron covers the genesis/pre-Shelley edge case: Byron
// headers carry no VRF or operational certificate, so all three fields are nil.
func TestPraosHeaderFieldsByron(t *testing.T) {
	var h byron.ByronMainBlockHeader
	// Sanity check that the Byron header satisfies the header interface used
	// by praosHeaderFields.
	var _ gledger.BlockHeader = &h

	vrf, opCert, counter := praosHeaderFields(&h)

	assert.Nil(t, vrf)
	assert.Nil(t, opCert)
	assert.Nil(t, counter)
}

// TestPraosHeaderFieldsEmptyVRFAndOpCert verifies that empty header byte slices
// (which can occur for malformed or partially populated headers) do not produce
// empty-string values; block_vrf/op_cert stay nil while the counter is always
// reported.
func TestPraosHeaderFieldsEmptyVRFAndOpCert(t *testing.T) {
	var h conway.ConwayBlockHeader
	h.Body.OpCert.SequenceNumber = 5

	vrf, opCert, counter := praosHeaderFields(&h)

	assert.Nil(t, vrf)
	assert.Nil(t, opCert)
	require.NotNil(t, counter)
	assert.Equal(t, "5", *counter)
}

func TestBech32EncodeDataVRF(t *testing.T) {
	encoded, err := bech32EncodeData("vrf_vk", testVRFKey)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(encoded, "vrf_vk1"))

	// Round-trip: decoding must recover the original bytes.
	hrp, data, err := bech32.Decode(encoded)
	require.NoError(t, err)
	assert.Equal(t, "vrf_vk", hrp)
	decoded, err := bech32.ConvertBits(data, 5, 8, false)
	require.NoError(t, err)
	assert.Equal(t, testVRFKey, decoded)
}
