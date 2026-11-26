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

package dblifecycle

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func stakedOutput(t *testing.T, txByte byte, amount uint64) badgerutxo.Output {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey, lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x37}, lcommon.AddressHashSize),
		bytes.Repeat([]byte{0x41}, lcommon.AddressHashSize),
	)
	require.NoError(t, err)
	encoded, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
		OutputAddress: addr, OutputAmount: amount,
	})
	require.NoError(t, err)
	return badgerutxo.Output{
		Ref:  badgerutxo.NewRef(bytes.Repeat([]byte{txByte}, 32), 0),
		Cbor: encoded,
	}
}

// A rollback by the offline lifecycle restores outputs, so it must keep the
// output index and the running sums the node reads, or a node reopening the
// set finds the index marked complete with rows missing.
func TestOfflineRollbackKeepsTheOutputIndexAndSums(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	dir := badgerutxo.DirFor(dataDir)
	nodeOpts := []badgerutxo.Option{
		badgerutxo.WithAggregator(ledger.UtxoStakeOf),
		badgerutxo.WithIndexer(ledger.UtxoIndexBodies),
	}

	spent := stakedOutput(t, 0x01, 2_000_000)
	created := stakedOutput(t, 0x02, 1_000_000)
	p1 := ocommon.Point{Slot: 1, Hash: bytes.Repeat([]byte{0x11}, 32)}
	p2 := ocommon.Point{Slot: 2, Hash: bytes.Repeat([]byte{0x22}, 32)}

	node, err := badgerutxo.Open(dir, nodeOpts...)
	require.NoError(t, err)
	require.NoError(t, node.Bootstrap(
		p1, func(put func(badgerutxo.Output) error) error {
			return put(spent)
		},
	))
	_, err = node.Apply(
		p2, []badgerutxo.Output{created}, []badgerutxo.Ref{spent.Ref},
	)
	require.NoError(t, err)
	require.NoError(t, node.Close())

	offline, err := OpenUtxoSet(dataDir)
	require.NoError(t, err)
	require.NotNil(t, offline)
	_, err = offline.Rollback(p1)
	require.NoError(t, err)
	require.NoError(t, offline.Close())

	node, err = badgerutxo.Open(dir, nodeOpts...)
	require.NoError(t, err)
	defer node.Close()
	_, live, err := node.Get(spent.Ref)
	require.NoError(t, err)
	require.True(t, live)

	indexed, err := node.AddressIndexed()
	require.NoError(t, err)
	require.True(t, indexed)
	bodies := ledger.UtxoIndexBodies(spent.Cbor)
	require.NotEmpty(t, bodies)
	var found []badgerutxo.Ref
	require.NoError(t, node.EachIndexed(
		bodies, nil, func(o badgerutxo.Output) (bool, error) {
			found = append(found, o.Ref)
			return false, nil
		},
	))
	require.Equal(t, []badgerutxo.Ref{spent.Ref}, found)

	key, amount, ok := ledger.UtxoStakeOf(spent.Cbor)
	require.True(t, ok)
	sum, err := node.Sum(key)
	require.NoError(t, err)
	require.Equal(t, amount, sum)
}
