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
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// utxoByTxInAsOf calls queryShelleyUtxoByTxIn directly for a single ref
// pinned at atSlot -- bypassing Query's verifyPointOnChain, the same way
// queries_asofslot_test.go's PoolStakeDistribution AsOf tests call
// PoolStakeDistribution directly -- and decodes the reply into a
// UtxoId->TransactionOutput map for assertions.
func utxoByTxInAsOf(
	t *testing.T,
	ls *LedgerState,
	txId []byte,
	outputIdx uint32,
	atSlot uint64,
) map[olocalstatequery.UtxoId]ledger.TransactionOutput {
	t.Helper()
	txIn := ledger.NewShelleyTransactionInput(
		hex.EncodeToString(txId),
		int(outputIdx),
	)
	result, err := ls.queryShelleyUtxoByTxIn(
		[]ledger.ShelleyTransactionInput{txIn},
		QueryPoint{Slot: atSlot},
		nil,
	)
	require.NoError(t, err)
	arr, ok := result.([]any)
	require.True(t, ok, "expected []any result")
	require.Len(t, arr, 1)
	m, ok := arr[0].(map[olocalstatequery.UtxoId]ledger.TransactionOutput)
	require.True(t, ok, "expected UtxoId map")
	return m
}

// TestQueryShelleyUtxoByTxIn_AsOfSlot_LiveBetweenCreationAndSpend covers the
// core #1900 UTxO-query fix: a pinned point between a UTxO's creation
// (slot 100, via seedBabbageUtxo) and its later spend (marked deleted at
// slot 500) must report it live. Before this fix, this handler ignored the
// pinned point entirely and always answered from live state -- exactly the
// false-"missing"/false-"present" divergence node-parity's incremental
// mode proved live: acquiring an older point and querying a ref that a
// later block touched returned that later state through the same
// still-acquired session.
func TestQueryShelleyUtxoByTxIn_AsOfSlot_LiveBetweenCreationAndSpend(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1_000, repeatedBytes(32, 0x0B)),
	}, nil))

	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAA}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	// seedBabbageUtxo always creates its row at AddedSlot 100.
	txId := seedBabbageUtxo(t, db, 0xC1, 0, addr, 5_000_000)
	require.NoError(t, db.MarkUtxosDeletedAtSlot(
		nil,
		[]dbtypes.UtxoKey{{TxId: txId, OutputIdx: 0}},
		500,
	))

	m := utxoByTxInAsOf(t, ls, txId, 0, 300)
	require.Len(
		t, m, 1,
		"utxo must be reported live between its creation (100) and spend (500)",
	)
	out := m[olocalstatequery.UtxoId{Hash: ledger.NewBlake2b256(txId), Idx: 0}]
	require.NotNil(t, out)
	require.Equal(t, uint64(5_000_000), out.Amount().Uint64())
}

// TestQueryShelleyUtxoByTxIn_AsOfSlot_BeforeCreation_Absent covers a pinned
// point earlier than the UTxO's own creation: it must not exist yet.
func TestQueryShelleyUtxoByTxIn_AsOfSlot_BeforeCreation_Absent(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1_000, repeatedBytes(32, 0x0B)),
	}, nil))

	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAA}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	txId := seedBabbageUtxo(t, db, 0xC2, 0, addr, 1_000_000)
	require.NoError(t, db.MarkUtxosDeletedAtSlot(
		nil,
		[]dbtypes.UtxoKey{{TxId: txId, OutputIdx: 0}},
		500,
	))

	m := utxoByTxInAsOf(t, ls, txId, 0, 50)
	require.Empty(t, m, "utxo must not exist before its own creation slot")
}

// TestQueryShelleyUtxoByTxIn_AsOfSlot_SpentAtExactSlot_Absent covers the
// spend-side boundary: a pin naming exactly the slot a UTxO was spent at
// must report it absent (spent "at" a slot, not "strictly after" it, is
// already gone as of that slot) -- the mirror of AddedSlot's own
// at-or-before inclusion on the creation side.
func TestQueryShelleyUtxoByTxIn_AsOfSlot_SpentAtExactSlot_Absent(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1_000, repeatedBytes(32, 0x0B)),
	}, nil))

	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAA}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	txId := seedBabbageUtxo(t, db, 0xC3, 0, addr, 1_000_000)
	require.NoError(t, db.MarkUtxosDeletedAtSlot(
		nil,
		[]dbtypes.UtxoKey{{TxId: txId, OutputIdx: 0}},
		500,
	))

	m := utxoByTxInAsOf(t, ls, txId, 0, 500)
	require.Empty(t, m, "utxo spent at slot 500 must be absent as of slot 500")
}

// TestQueryShelleyUtxoByTxIn_AsOfSlot_AfterSpend_Absent covers a pinned
// point after the UTxO was spent: it must be reported absent, not the
// live-state answer this handler gave before the #1900 fix.
func TestQueryShelleyUtxoByTxIn_AsOfSlot_AfterSpend_Absent(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1_000, repeatedBytes(32, 0x0B)),
	}, nil))

	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAA}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	txId := seedBabbageUtxo(t, db, 0xC4, 0, addr, 1_000_000)
	require.NoError(t, db.MarkUtxosDeletedAtSlot(
		nil,
		[]dbtypes.UtxoKey{{TxId: txId, OutputIdx: 0}},
		500,
	))

	m := utxoByTxInAsOf(t, ls, txId, 0, 700)
	require.Empty(
		t,
		m,
		"utxo must be reported absent once spent, even live-state-wise",
	)
}

// TestQueryShelleyUtxoByTxIn_AsOfSlot_NeverSpent_StillLive covers the
// simple never-spent case at an arbitrary later pinned point: a UTxO that
// has never been marked deleted must remain live at any slot at or after
// its creation, regardless of how far the live tip has since advanced.
func TestQueryShelleyUtxoByTxIn_AsOfSlot_NeverSpent_StillLive(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(100_000, repeatedBytes(32, 0x0B)),
	}, nil))

	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAA}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	txId := seedBabbageUtxo(t, db, 0xC5, 0, addr, 1_000_000)

	m := utxoByTxInAsOf(t, ls, txId, 0, 99_000)
	require.Len(
		t,
		m,
		1,
		"a never-spent utxo must remain live at any later pinned point",
	)
}

// TestQueryShelleyUtxoByTxIn_RetentionWindow_TooOldRejected covers the
// retention-window boundary: a pinned point older than this node's
// spent-UTxO retention floor (tip - stability window, the same threshold
// UtxosDeleteConsumed prunes by) must reject cleanly with
// ErrHistoricalStateUnavailable rather than risk answering "absent" for a
// ref that may have been live at that point but whose spend record has
// since been hard-deleted.
func TestQueryShelleyUtxoByTxIn_RetentionWindow_TooOldRejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	// newPoolDistr2Ledger leaves CardanoNodeConfig nil, so
	// calculateStabilityWindow returns the default (50_000) regardless of
	// era -- see TestCleanupConsumedUtxos_CoreModePrunes's identical setup.
	const tipSlot = 200_000
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(tipSlot, repeatedBytes(32, 0x0B)),
	}, nil))

	// floor = 200_000 - 50_000 = 150_000; one slot behind it must reject.
	txIn := ledger.NewShelleyTransactionInput(
		hex.EncodeToString(repeatedBytes(32, 0xEE)),
		0,
	)
	_, err := ls.queryShelleyUtxoByTxIn(
		[]ledger.ShelleyTransactionInput{txIn},
		QueryPoint{Slot: 149_999},
		nil,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// TestQueryShelleyUtxoByTxIn_RetentionWindow_AtFloor_Succeeds covers the
// exact floor slot itself: a ref spent at any slot strictly after the
// floor is guaranteed to have survived the periodic cleanup sweep (see
// checkUtxoRetentionWindow's doc comment for why), so a pin naming the
// floor slot exactly must be accepted, not rejected.
func TestQueryShelleyUtxoByTxIn_RetentionWindow_AtFloor_Succeeds(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	const tipSlot = 200_000
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(tipSlot, repeatedBytes(32, 0x0B)),
	}, nil))

	txIn := ledger.NewShelleyTransactionInput(
		hex.EncodeToString(repeatedBytes(32, 0xEE)),
		0,
	)
	_, err := ls.queryShelleyUtxoByTxIn(
		[]ledger.ShelleyTransactionInput{txIn},
		QueryPoint{Slot: 150_000},
		nil,
	)
	require.NoError(t, err)
}

// TestQueryShelleyUtxoByTxIn_RetentionWindow_APIModeNeverRejects covers
// API storage mode, which never hard-deletes spent UTxO rows (see
// cleanupConsumedUtxos' identical StorageModeAPI check) -- so no
// retention-window rejection applies there, even for a pin that would be
// rejected in core mode.
func TestQueryShelleyUtxoByTxIn_RetentionWindow_APIModeNeverRejects(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDBForCleanup(t, dbtypes.StorageModeAPI)
	ls := newPoolDistr2Ledger(t, db)
	const tipSlot = 200_000
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(tipSlot, repeatedBytes(32, 0x0B)),
	}, nil))

	txIn := ledger.NewShelleyTransactionInput(
		hex.EncodeToString(repeatedBytes(32, 0xEE)),
		0,
	)
	_, err := ls.queryShelleyUtxoByTxIn(
		[]ledger.ShelleyTransactionInput{txIn},
		// Far below what would be the core-mode floor (150_000).
		QueryPoint{Slot: 1},
		nil,
	)
	require.NoError(t, err)
}

// TestQueryShelleyUtxoByTxIn_RetentionWindow_SurvivesEraWindowWidening covers
// the gap a retention floor derived from only the CURRENT era's stability
// window leaves open right after an era transition that widens it (Byron's
// small 2k vs every Shelley+ era's much larger 3k/f): periodic cleanup
// recomputes its own floor from whatever era was live each time it ran, so a
// row pruned while still in Byron can already be gone even though the
// current (Shelley+) era's own window alone would compute a floor far
// enough back in time to call this pin "safe." checkUtxoRetentionWindow
// must reject using the smaller of the two windows (minEverStabilityWindow),
// not just the current era's.
func TestQueryShelleyUtxoByTxIn_RetentionWindow_SurvivesEraWindowWidening(
	t *testing.T,
) {
	t.Parallel()

	byronGenesisJSON := `{
		"protocolConsts": {"k": 10, "protocolMagic": 2}
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 10,
		"systemStart": "2022-10-25T00:00:00Z"
	}`
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)),
	)
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.config.CardanoNodeConfig = cfg
	// Byron window = 2*10 = 20; Shelley window = 3*10/0.05 = 600. Node is
	// now past Byron, in a Shelley+ era.
	ls.currentEra = eras.ShelleyEraDesc

	const tipSlot = 1_000
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(tipSlot, repeatedBytes(32, 0x0B)),
	}, nil))

	// The current era's own window alone would compute floor = 1000 - 600 =
	// 400, incorrectly accepting slot 500 as "safely retained." The correct,
	// Byron-aware floor is 1000 - 20 = 980, which must reject it.
	txIn := ledger.NewShelleyTransactionInput(
		hex.EncodeToString(repeatedBytes(32, 0xEE)),
		0,
	)
	_, err := ls.queryShelleyUtxoByTxIn(
		[]ledger.ShelleyTransactionInput{txIn},
		QueryPoint{Slot: 500},
		nil,
	)
	require.Error(
		t,
		err,
		"a pin the current era's window alone would wrongly call safe "+
			"must still be rejected using the smaller Byron-era window",
	)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// TestQuery_UtxoByTxIn_WiredThroughDispatch is an end-to-end check that
// Query's dispatch switch (ledger/queries.go) actually threads at and txn
// into queryShelleyUtxoByTxIn -- not just that the handler works when
// called directly, which every other test in this file exercises.
func TestQuery_UtxoByTxIn_WiredThroughDispatch(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAA}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	txId := seedBabbageUtxo(t, db, 0xC6, 0, addr, 1_000_000)
	require.NoError(t, db.MarkUtxosDeletedAtSlot(
		nil,
		[]dbtypes.UtxoKey{{TxId: txId, OutputIdx: 0}},
		500,
	))

	pointHash := bytes.Repeat([]byte{0xAB}, 32)
	seedBlockAtSlot(t, ls, 300, pointHash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(300, pointHash),
	}, nil))

	txIn := ledger.NewShelleyTransactionInput(hex.EncodeToString(txId), 0)
	query := &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyUtxoByTxinQuery{
				TxIns: []ledger.ShelleyTransactionInput{txIn},
			},
		},
	}

	result, err := ls.Query(query, QueryPoint{Slot: 300, Hash: pointHash})
	require.NoError(t, err)
	arr, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)
	m, ok := arr[0].(map[olocalstatequery.UtxoId]ledger.TransactionOutput)
	require.True(t, ok)
	require.Len(
		t, m, 1,
		"utxo created at slot 100 and spent at slot 500 must be live as "+
			"of the pinned point (slot 300), proving the pin -- not live "+
			"state -- drove this answer",
	)
}
