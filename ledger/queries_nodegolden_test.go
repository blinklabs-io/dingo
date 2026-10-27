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

package ledger

import (
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// poolDistrNodeReply is the GetPoolDistr reply cardano-node 11.1.3 sent over
// NtC v19 for a two-pool devnet, as the result element of MsgResult: the era
// wrapper holding a map from pool to [stake fraction, VRF key hash]. The
// fractions are each pool's stake over the total, here
// 125006519458/250115698965 and 125109179507/250115698965.
const poolDistrNodeReply = "81" +
	"a2" +
	"581c8362063f951441ae0e7850053efad297e83db2625386e6c485273a60" +
	"82" + "d81e82" + "1b0000001d1af81ca2" + "1b0000003a3c0eb115" +
	"5820" + "64220713c0d74b1f520bbbcc4632194196c9db540c0d14ce8f2b12b0e28ff197" +
	"581cc355a49e1c9af15286ff0e7a72ffc5fa2c652978e2af306f6f376abf" +
	"82" + "d81e82" + "1b0000001d21169473" + "1b0000003a3c0eb115" +
	"5820" + "07fe86794f2d2f9117f5e15a449b00e54257d384e1a265e3dae49619b80ec541"

// TestQueryShelleyPoolDistr_NodeReply answers GetPoolDistr with the bytes
// cardano-node sends for the same stake: the pools' mark-snapshot stakes
// become the numerators of fractions over their sum.
func TestQueryShelleyPoolDistr_NodeReply(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	seedPoolDistr2Fixture(
		t,
		db,
		mustDecodeHex(t, "8362063f951441ae0e7850053efad297e83db2625386e6c485273a60"),
		mustDecodeHex(t, "64220713c0d74b1f520bbbcc4632194196c9db540c0d14ce8f2b12b0e28ff197"),
		125_006_519_458,
		0,
	)
	seedPoolDistr2Fixture(
		t,
		db,
		mustDecodeHex(t, "c355a49e1c9af15286ff0e7a72ffc5fa2c652978e2af306f6f376abf"),
		mustDecodeHex(t, "07fe86794f2d2f9117f5e15a449b00e54257d384e1a265e3dae49619b80ec541"),
		125_109_179_507,
		0,
	)
	ls := newPoolDistr2Ledger(t, db)

	result, err := ls.Query(t.Context(), poolDistrQuery(), QueryPoint{})
	require.NoError(t, err)
	wire, err := cbor.Encode(result)
	require.NoError(t, err)
	require.Equal(t, poolDistrNodeReply, hex.EncodeToString(wire))
}

// TestQueryLedgerTip_NodeReply answers GetLedgerTip with the layout
// cardano-node sends: the era wrapper holding [slot, hash]. The slot and hash
// are those of a block of a devnet the node answered for.
func TestQueryLedgerTip_NodeReply(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	hash := mustDecodeHex(t, "3ca62353f21410174b7293f83522ae6d9b0570a973d71686f3dee05d9f4a1a97")
	seedBlockAtSlot(t, ls, 335, hash)
	tip := ochainsync.Tip{Point: ocommon.NewPoint(400, repeatedBytes(32, 0xCD))}
	require.NoError(t, db.SetTip(tip, nil))
	ls.currentTip = tip
	ls.publishSnapshotsLocked()

	got, err := ls.Query(
		t.Context(),
		shelleyBlockQuery(&olocalstatequery.ShelleyLedgerTipQuery{}),
		QueryPoint{Slot: 335, Hash: hash},
	)
	require.NoError(t, err)
	wire, err := cbor.Encode(got)
	require.NoError(t, err)
	require.Equal(
		t,
		"818219014f58203ca62353f21410174b7293f83522ae6d9b0570a973d71686f3dee05d9f4a1a97",
		hex.EncodeToString(wire),
	)
}
