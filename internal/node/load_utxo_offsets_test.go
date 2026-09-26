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

package node

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

func TestTxBodyMapValueRangeFindsCollateralReturn(t *testing.T) {
	t.Parallel()

	// Preview invalid Conway tx with one regular output and one
	// collateral return. The live collateral-return UTxO index is 1.
	const txHex = "84a700818258200c07395aed88bdddc6de0518d1462dd0ec7e52e1e3a53599f7cdb24dc80237f8010181a20058390073a817bb425cbe179af824529d96ceb93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebeef022245944e3914f5bcca3a793011a02dc6c00021a001e84800b5820192d0c0c2c2320e843e080b5f91a9ca35155bc50f3ef3bfdbc72c1711b86367e0d818258203af629a5cd75f76d0cc21172e1193b85f199ca78e837c3965d77d7d6bc90206b0010a20058390073a817bb425cbe179af824529d96ceb93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebeef022245944e3914f5bcca3a793011a006acfc0111a002dc6c0a4008182582025fcacade3fffc096b53bdaf4c7d012bded303c9edbee686d24b372dae60aa1b58409da928a064ff9f795110bdcb8ab05d2a7a023dd15ebc42044f102ce366c0c9077024c7951c2d63584b7d2eea7bf1da4a7453bde4c99dd083889c1e2e2e3db804048119077a0581840000187b820a0a06814746010000222601f4f6"

	txBytes, err := hex.DecodeString(txHex)
	require.NoError(t, err)

	var txParts []cbor.RawMessage
	_, err = cbor.Decode(txBytes, &txParts)
	require.NoError(t, err)
	require.NotEmpty(t, txParts)
	bodyCbor := []byte(txParts[0])

	tx, err := gledger.NewTransactionFromCbor(
		uint(conway.EraIdConway),
		txBytes,
	)
	require.NoError(t, err)
	require.Len(t, tx.Outputs(), 1)
	require.Len(t, tx.Produced(), 1)
	require.Equal(t, uint32(1), tx.Produced()[0].Id.Index())
	collateralReturn := tx.CollateralReturn()
	if collateralReturn == nil {
		t.Fatal("expected collateral return")
	}

	offset, length, found, err := txBodyMapValueRange(
		bodyCbor,
		0,
		database.TxBodyKeyCollateralReturn,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(
		t,
		collateralReturn.Cbor(),
		bodyCbor[offset:offset+length],
	)
}

func TestTxBodyMapValueRangeHonorsBaseOffset(t *testing.T) {
	t.Parallel()

	// Same fixture as above, but called with a non-zero bodyOffset.
	// Guards against absolute-offset regressions (e.g. if the returned
	// offset were interpreted relative to the body slice rather than
	// the outer buffer).
	const txHex = "84a700818258200c07395aed88bdddc6de0518d1462dd0ec7e52e1e3a53599f7cdb24dc80237f8010181a20058390073a817bb425cbe179af824529d96ceb93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebeef022245944e3914f5bcca3a793011a02dc6c00021a001e84800b5820192d0c0c2c2320e843e080b5f91a9ca35155bc50f3ef3bfdbc72c1711b86367e0d818258203af629a5cd75f76d0cc21172e1193b85f199ca78e837c3965d77d7d6bc90206b0010a20058390073a817bb425cbe179af824529d96ceb93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebeef022245944e3914f5bcca3a793011a006acfc0111a002dc6c0a4008182582025fcacade3fffc096b53bdaf4c7d012bded303c9edbee686d24b372dae60aa1b58409da928a064ff9f795110bdcb8ab05d2a7a023dd15ebc42044f102ce366c0c9077024c7951c2d63584b7d2eea7bf1da4a7453bde4c99dd083889c1e2e2e3db804048119077a0581840000187b820a0a06814746010000222601f4f6"

	txBytes, err := hex.DecodeString(txHex)
	require.NoError(t, err)

	var txParts []cbor.RawMessage
	_, err = cbor.Decode(txBytes, &txParts)
	require.NoError(t, err)
	require.NotEmpty(t, txParts)
	bodyCbor := []byte(txParts[0])

	tx, err := gledger.NewTransactionFromCbor(
		uint(conway.EraIdConway),
		txBytes,
	)
	require.NoError(t, err)
	collateralReturn := tx.CollateralReturn()
	require.NotNil(t, collateralReturn)

	const bodyOffset uint32 = 42
	prefix := bytes.Repeat([]byte{0xaa}, int(bodyOffset))
	prefixed := append(prefix, bodyCbor...)

	offset, length, found, err := txBodyMapValueRange(
		bodyCbor,
		bodyOffset,
		database.TxBodyKeyCollateralReturn,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.GreaterOrEqual(t, offset, bodyOffset)
	require.Equal(
		t,
		collateralReturn.Cbor(),
		prefixed[offset:offset+length],
	)
}

func TestTxBodyMapValueRangeMissingKey(t *testing.T) {
	t.Parallel()

	bodyCbor := []byte{
		0xa2,
		0x00, 0x80,
		0x01, 0x80,
	}

	_, _, found, err := txBodyMapValueRange(
		bodyCbor,
		0,
		database.TxBodyKeyCollateralReturn,
	)
	require.NoError(t, err)
	require.False(t, found)
}

func TestRawBlockTransactionValidity_PreAlonzo(t *testing.T) {
	t.Parallel()

	// Shelley through Mary blocks carry no invalid_transactions field.
	block, err := cbor.Encode([]any{
		uint64(0),
		[]cbor.RawMessage{
			cbor.RawMessage(rawInvalidTxTestBody(t, 1)),
			cbor.RawMessage(rawInvalidTxTestBody(t, 2)),
		},
		[]any{map[uint64]any{}, map[uint64]any{}},
		map[uint64]any{},
	})
	require.NoError(t, err)
	offsets, err := lcommon.ExtractTransactionOffsets(block)
	require.NoError(t, err)

	validity, err := rawBlockTransactionValidity(offsets)
	require.NoError(t, err)
	require.Equal(t, []bool{true, true}, validity)
}

func TestRawBlockTransactionValidity_AlonzoEmpty(t *testing.T) {
	t.Parallel()

	offsets, err := lcommon.ExtractTransactionOffsets(rawInvalidTxTestBlock(
		t,
		[][]byte{rawInvalidTxTestBody(t, 1), rawInvalidTxTestBody(t, 2)},
		nil,
	))
	require.NoError(t, err)

	validity, err := rawBlockTransactionValidity(offsets)
	require.NoError(t, err)
	require.Equal(t, []bool{true, true}, validity)
}

func TestRawBlockTransactionValidity_AlonzoMultiple(t *testing.T) {
	t.Parallel()

	bodies := make([][]byte, 8)
	for i := range bodies {
		bodies[i] = rawInvalidTxTestBody(t, uint64(i)+1)
	}
	offsets, err := lcommon.ExtractTransactionOffsets(
		rawInvalidTxTestBlock(t, bodies, []uint{1, 3, 7}),
	)
	require.NoError(t, err)

	validity, err := rawBlockTransactionValidity(offsets)
	require.NoError(t, err)
	require.Equal(
		t,
		[]bool{true, false, true, false, true, true, true, false},
		validity,
	)
}
