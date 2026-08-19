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

package forging

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// TestBuildLeiosEBBodiesAlignWithRefs verifies that buildLeiosEB returns the
// transaction bodies in the same order as the manifest references, and that a
// transaction dropped from the manifest (invalid hash or size) is dropped from
// the bodies too, so body i stays aligned with reference i.
func TestBuildLeiosEBBodiesAlignWithRefs(t *testing.T) {
	txs := []MempoolTransaction{
		{Hash: strings.Repeat("11", 32), Cbor: []byte{0x01}},
		{Hash: "not-hex", Cbor: []byte{0x02}},            // dropped: bad hash
		{Hash: strings.Repeat("22", 32), Cbor: []byte{}}, // dropped: zero size
		{Hash: strings.Repeat("33", 32), Cbor: []byte{0x03, 0x04}},
	}

	ebCbor, ebHash, bodies, hashes, err := buildLeiosEB(txs)
	require.NoError(t, err)
	require.NotEmpty(t, ebHash)

	// Only the two valid transactions survive, in input order.
	require.Len(t, bodies, 2)
	require.Equal(t, []string{txs[0].Hash, txs[3].Hash}, hashes)
	require.Equal(t, []byte{0x01}, bodies[0])
	require.Equal(t, []byte{0x03, 0x04}, bodies[1])

	// The manifest references match the surviving bodies in order and size.
	eb, err := lcommon.NewLeiosEndorserBlockFromCbor(ebCbor)
	require.NoError(t, err)
	require.Len(t, eb.TransactionReferences, len(bodies))
	for i, ref := range eb.TransactionReferences {
		require.Equalf(
			t,
			len(bodies[i]),
			int(ref.TransactionSize),
			"reference %d size matches body length",
			i,
		)
	}
}

// TestBuildLeiosEBNoValidRefs verifies buildLeiosEB returns errNoValidTxRefs
// when no transaction yields a valid reference.
func TestBuildLeiosEBNoValidRefs(t *testing.T) {
	_, _, bodies, hashes, err := buildLeiosEB([]MempoolTransaction{
		{Hash: "not-hex", Cbor: []byte{0x01}},
	})
	require.ErrorIs(t, err, errNoValidTxRefs)
	require.Nil(t, bodies)
	require.Nil(t, hashes)
}

type leiosOverlayValidator struct {
	base   map[string]struct{}
	reject map[string]struct{}
}

func (v *leiosOverlayValidator) ValidateTx(tx ledger.Transaction) error {
	return v.ValidateTxWithOverlay(tx, nil, nil)
}

func (v *leiosOverlayValidator) ValidateTxWithOverlay(
	tx ledger.Transaction,
	consumed map[string]struct{},
	created map[string]lcommon.Utxo,
) error {
	if _, reject := v.reject[tx.Hash().String()]; reject {
		return errors.New("rejected parent")
	}
	for _, input := range tx.Inputs() {
		key := fmt.Sprintf("%s:%d", input.Id().String(), input.Index())
		if _, spent := consumed[key]; spent {
			return errors.New("already consumed")
		}
		if _, ok := created[key]; ok {
			continue
		}
		if _, ok := v.base[key]; !ok {
			return errors.New("missing input")
		}
	}
	return nil
}

func TestSelectValidLeiosTransactionsPreservesDependentChain(t *testing.T) {
	parentCbor := makeMinimalTxCbor(t, 0x41, 29)
	parent, err := conway.NewConwayTransactionFromCbor(parentCbor)
	require.NoError(t, err)
	childCbor := makeMinimalTxCborWithInput(t, parent.Hash().Bytes(), 0)
	child, err := conway.NewConwayTransactionFromCbor(childCbor)
	require.NoError(t, err)
	baseInput := parent.Inputs()[0]
	baseKey := fmt.Sprintf("%s:%d", baseInput.Id().String(), baseInput.Index())
	txs := []MempoolTransaction{
		{Hash: parent.Hash().String(), Cbor: parentCbor, Type: conway.TxTypeConway},
		{Hash: child.Hash().String(), Cbor: childCbor, Type: conway.TxTypeConway},
	}

	selected, err := selectValidLeiosTransactions(txs, &leiosOverlayValidator{
		base:   map[string]struct{}{baseKey: {}},
		reject: map[string]struct{}{},
	})
	require.NoError(t, err)
	require.Equal(t, txs, selected)
}

func TestSelectValidLeiosTransactionsRejectsInvalidChain(t *testing.T) {
	parentCbor := makeMinimalTxCbor(t, 0x42, 29)
	parent, err := conway.NewConwayTransactionFromCbor(parentCbor)
	require.NoError(t, err)
	childCbor := makeMinimalTxCborWithInput(t, parent.Hash().Bytes(), 0)
	child, err := conway.NewConwayTransactionFromCbor(childCbor)
	require.NoError(t, err)
	baseInput := parent.Inputs()[0]
	baseKey := fmt.Sprintf("%s:%d", baseInput.Id().String(), baseInput.Index())
	txs := []MempoolTransaction{
		{Hash: parent.Hash().String(), Cbor: parentCbor, Type: conway.TxTypeConway},
		{Hash: child.Hash().String(), Cbor: childCbor, Type: conway.TxTypeConway},
	}

	selected, err := selectValidLeiosTransactions(txs, &leiosOverlayValidator{
		base: map[string]struct{}{baseKey: {}},
		reject: map[string]struct{}{
			parent.Hash().String(): {},
		},
	})
	require.NoError(t, err)
	require.Empty(t, selected, "a rejected parent must not expose its output")
}
