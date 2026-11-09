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
	"context"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// storeByronPubkeyOutput gives an existing UTxO row the output body the
// HARDFORK rule's scan of the live set reads.
func storeByronPubkeyOutput(
	t *testing.T,
	db *database.Database,
	txID []byte,
	amount uint64,
) {
	t.Helper()
	addr, err := lcommon.NewByronAddressFromParts(
		lcommon.ByronAddressTypePubkey,
		bytes.Repeat([]byte{0x55}, lcommon.AddressHashSize),
		lcommon.ByronAddressAttributes{},
	)
	require.NoError(t, err)
	encoded, err := cbor.Encode(byron.ByronTransactionOutput{
		OutputAddress: addr,
		OutputAmount:  amount,
	})
	require.NoError(t, err)
	txn := db.Transaction(context.Background(), true)
	require.NoError(t, db.Blob().SetUtxo(txn.Blob(), txID, 0, encoded))
	require.NoError(t, txn.Commit())
}

// seedByronAvvmUtxoAt stores a live Byron redeem-address output added at slot.
func seedByronAvvmUtxoAt(
	t *testing.T,
	db *database.Database,
	slot uint64,
	amount uint64,
) {
	t.Helper()
	addr, err := lcommon.NewByronAddressRedeem(
		bytes.Repeat([]byte{0x42}, 32),
		lcommon.ByronAddressAttributes{},
	)
	require.NoError(t, err)
	encoded, err := cbor.Encode(byron.ByronTransactionOutput{
		OutputAddress: addr,
		OutputAmount:  amount,
	})
	require.NoError(t, err)
	txID := bytes.Repeat([]byte{0xaa}, 32)
	txn := db.Transaction(context.Background(), true)
	require.NoError(t, db.CreateUtxo(context.Background(), txn, &models.Utxo{
		TxId:      txID,
		OutputIdx: 0,
		AddedSlot: slot,
	}))
	require.NoError(t, db.Blob().SetUtxo(txn.Blob(), txID, 0, encoded))
	require.NoError(t, txn.Commit())
}

// A Shelley boundary that enacts a classic parameter update bumping the
// protocol major version to 3 also runs the Allegra HARDFORK rule that returns
// Byron redeem outputs to the reserves, and credits the epoch's MIR
// certificate. The update proposal and the MIR certificate are in blocks above
// the rollback target. Rolling the boundary back must leave every table as it
// was at the target, and replaying it must reproduce the first application.
func TestRollbackAcrossClassicUpdateMIRAndAvvmRestoresEveryTable(
	t *testing.T,
) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	h := newBoundaryReplayHarnessWithDB(t, db)
	h.ls.config.CardanoNodeConfig.ShelleyGenesis().UpdateQuorum = 1
	// The guard on a voted parameter set refuses block sizes of zero. A
	// rollback reloads the parameters from the genesis defaults, so the set is
	// installed again before each application.
	installPParams := func() {
		pp := *h.ls.currentPParams.(*shelley.ShelleyProtocolParameters)
		pp.ProtocolMajor = shelley.MinProtocolVersionShelley
		pp.MaxBlockBodySize = 65_536
		pp.MaxTxSize = 16_384
		pp.MaxBlockHeaderSize = 1_100
		h.ls.currentPParams = &pp
	}
	raw, err := dbtest.RawSQLiteMetadata(t, h.ls.db)
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	delegatorKey := bytes.Repeat([]byte{0x72}, 28)
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 8_000, 0, nil))
	seedByronAvvmUtxoAt(t, db, 5, 1_000)
	for _, id := range []byte{0x10, 0x11} {
		storeByronPubkeyOutput(t, db, bytes.Repeat([]byte{id}, 32), 20_000_000)
	}
	for _, key := range [][]byte{
		bytes.Repeat([]byte{0x71}, 28), delegatorKey,
	} {
		_, err := raw.Exec(`
INSERT INTO stake_registration (staking_key, credential_tag, added_slot, deposit_amount)
VALUES (?, 0, 0, '0')`, key)
		require.NoError(t, err)
		_, err = raw.Exec(`
INSERT INTO stake_delegation (staking_key, credential_tag, pool_key_hash, added_slot)
VALUES (?, 0, ?, 0)`, key, h.poolHash)
		require.NoError(t, err)
	}

	// The proposal and the MIR certificate sit in blocks between the rollback
	// target and the boundary.
	aboveTarget := h.next.StartSlot - 1
	require.Greater(t, aboveTarget, h.fixture.ancestorTip.Point.Slot)
	update, err := cbor.Encode(map[uint64]any{
		14: lcommon.ProtocolParametersProtocolVersion{Major: 3, Minor: 0},
	})
	require.NoError(t, err)
	writeAboveTarget := func() {
		require.NoError(t, db.SetPParamUpdate(
			[]byte{0xaa}, update, aboveTarget, replayBoundaryEndedEra, nil,
		))
		seedMIRDistribution(t, raw, mirPotReserves, aboveTarget,
			[]models.MoveInstantaneousRewardsReward{{
				Credential: delegatorKey,
				Amount:     new(big.Int).SetUint64(600),
			}})
	}

	writeAboveTarget()
	h.rollbackBeforeBoundary()
	before := dumpComparableTables(t, raw)
	for _, table := range []string{
		"pparam_update", "move_instantaneous_rewards",
		"move_instantaneous_rewards_reward",
	} {
		require.Empty(t, before[table],
			"the rollback must remove the %s rows above its target", table)
	}

	writeAboveTarget()
	installPParams()
	first := h.applyBoundary()
	firstTables := dumpComparableTables(t, raw)
	changed := changedTables(before, firstTables)
	preMajor, err := GetProtocolVersion(first.pparams)
	require.NoError(t, err)
	require.Equal(t, uint(3), preMajor.Major,
		"the update must enact the protocol version bump")
	require.Equal(t, uint64(8_400), uint64(first.network.Reserves),
		"reserves: 8000 less the 600 MIR credit plus the 1000 AVVM output")
	for _, table := range []string{
		"pparams", "network_state", "utxo", "account", "epoch",
	} {
		require.True(t, changed[table], "the boundary did not change %s", table)
	}

	h.rollbackBeforeBoundaryRepairing(true)
	after := dumpComparableTables(t, raw)
	diffs := diffMetadataTables(before, after)
	for _, d := range diffs {
		t.Log(d)
	}
	require.Empty(t, diffs)

	writeAboveTarget()
	installPParams()
	second := h.applyBoundary()
	require.Equal(t, first.network, second.network)
	require.Equal(t, first.epoch, second.epoch)
	require.Empty(t, diffMetadataTables(
		withoutRowIDs(firstTables),
		withoutRowIDs(dumpComparableTables(t, raw)),
	))
}
