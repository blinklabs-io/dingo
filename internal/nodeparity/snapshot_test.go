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

package nodeparity

import (
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/require"
)

// datumOptionOutput builds a real BabbageTransactionOutput carrying the
// given datum option CBOR (decoded via the real gouroboros type, not a
// hand-rolled fake), so DatumHash()/Datum() behave exactly as they would
// for a genuine decoded LocalStateQuery reply.
func datumOptionOutput(
	t *testing.T, addr lcommon.Address, optionCbor []byte,
) *babbage.BabbageTransactionOutput {
	t.Helper()
	var opt babbage.BabbageTransactionOutputDatumOption
	require.NoError(t, opt.UnmarshalCBOR(optionCbor))
	return &babbage.BabbageTransactionOutput{
		OutputAddress: addr,
		OutputAmount:  mary.MaryTransactionOutputValue{Amount: 1_000_000},
		DatumOption:   &opt,
	}
}

// TestCanonicalUTxOEntry_DistinguishesInlineDatumFromHashOnlyReference
// covers a real node-parity scenario: two nodes agree on a UTxO's datum
// *content* (same hash) but report it via different wire forms -- one
// carries the datum inline, the other only its hash. This is a genuine
// indexing/decoding divergence between the two implementations, which is
// exactly what this tool exists to catch, and must not be masked just
// because DatumHash() alone matches for both.
func TestCanonicalUTxOEntry_DistinguishesInlineDatumFromHashOnlyReference(
	t *testing.T,
) {
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		make([]byte, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)

	datumBytes, err := data.Encode(data.NewInteger(big.NewInt(42)))
	require.NoError(t, err)
	inlineOptionCbor, err := cbor.Encode(
		[]any{babbage.DatumOptionTypeData, cbor.WrappedCbor(datumBytes)},
	)
	require.NoError(t, err)
	inline := datumOptionOutput(t, addr, inlineOptionCbor)

	hash := inline.DatumHash()
	require.NotNil(t, hash, "inline datum must still report a DatumHash")

	hashOnlyOptionCbor, err := cbor.Encode(
		[]any{babbage.DatumOptionTypeHash, hash},
	)
	require.NoError(t, err)
	hashOnly := datumOptionOutput(t, addr, hashOnlyOptionCbor)

	require.Equal(
		t, hash, hashOnly.DatumHash(),
		"the fixture must carry the identical DatumHash to isolate the form-only difference",
	)
	require.NotNil(t, inline.Datum(), "inline form must have a non-nil Datum()")
	require.Nil(t, hashOnly.Datum(), "hash-only form must have a nil Datum()")

	assert := require.New(t)
	inlineEntry := canonicalUTxOEntry(inline)
	hashOnlyEntry := canonicalUTxOEntry(hashOnly)
	assert.NotEqual(
		inlineEntry, hashOnlyEntry,
		"identical datum content presented as different wire forms must "+
			"encode differently, not be masked by a matching DatumHash",
	)
}
