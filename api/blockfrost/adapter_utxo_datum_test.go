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
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testDatumAddr = "addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd"

// decodeBabbageOutput builds a Babbage (map-form) transaction output CBOR with
// the optional datum option and reference script, then decodes it through the
// same path the adapter uses (NewTransactionOutputFromCbor). This mirrors both
// live sync and API-mode historical backfill, where inline datum and reference
// script are recovered from output CBOR rather than persisted metadata columns.
func decodeBabbageOutput(
	t *testing.T,
	datumOption any,
	scriptRef *lcommon.ScriptRef,
) lcommon.TransactionOutput {
	t.Helper()
	addr, err := lcommon.NewAddress(testDatumAddr)
	require.NoError(t, err)
	addrBytes, err := addr.Bytes()
	require.NoError(t, err)

	outputMap := map[int]any{
		0: addrBytes,
		1: uint64(1_000_000),
	}
	if datumOption != nil {
		outputMap[2] = datumOption
	}
	if scriptRef != nil {
		outputMap[3] = scriptRef
	}
	cborBytes, err := cbor.Encode(outputMap)
	require.NoError(t, err)
	decoded, err := gledger.NewTransactionOutputFromCbor(cborBytes)
	require.NoError(t, err)
	return decoded
}

func TestUtxoDatumAndScriptRefInlineAndScript(t *testing.T) {
	// Inline datum option is [1, 24(<datum cbor>)] per CIP-0032.
	datum := lcommon.Datum{Data: data.NewInteger(big.NewInt(42))}
	datumCbor, err := cbor.Encode(&datum)
	require.NoError(t, err)
	datumOption := []any{
		1,
		cbor.Tag{Number: 24, Content: datumCbor},
	}

	script := lcommon.PlutusV2Script([]byte{0x01, 0x02, 0x03, 0x04})
	scriptRef := &lcommon.ScriptRef{
		Type:   lcommon.ScriptRefTypePlutusV2,
		Script: script,
	}

	output := decodeBabbageOutput(t, datumOption, scriptRef)

	inlineDatum, referenceScriptHash := utxoDatumAndScriptRef(output)
	require.NotNil(t, inlineDatum)
	assert.Equal(t, hex.EncodeToString(datumCbor), *inlineDatum)
	require.NotNil(t, referenceScriptHash)
	assert.Equal(
		t,
		hex.EncodeToString(script.Hash().Bytes()),
		*referenceScriptHash,
	)
}

func TestUtxoDatumAndScriptRefDatumHashOnly(t *testing.T) {
	// A datum-hash-only output (option [0, <hash>]) carries no inline datum and
	// no reference script, so both Blockfrost fields must be nil (data_hash is
	// populated elsewhere from the persisted column).
	datumHash := lcommon.NewBlake2b256(make([]byte, 32))
	datumOption := []any{0, datumHash}

	output := decodeBabbageOutput(t, datumOption, nil)

	inlineDatum, referenceScriptHash := utxoDatumAndScriptRef(output)
	assert.Nil(t, inlineDatum)
	assert.Nil(t, referenceScriptHash)
}

func TestUtxoDatumAndScriptRefNone(t *testing.T) {
	output := decodeBabbageOutput(t, nil, nil)
	inlineDatum, referenceScriptHash := utxoDatumAndScriptRef(output)
	assert.Nil(t, inlineDatum)
	assert.Nil(t, referenceScriptHash)
}
