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

package mcp

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/livesettest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

// coreLedger is a ledger over a ledger's database, which opens no metadata
// store, with its LedgerDB set.
type coreLedger struct {
	db  *database.Database
	set *utxoset.Set
	ls  *ledger.LedgerState
}

func newCoreLedger(t *testing.T) coreLedger {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	require.Nil(t, db.Metadata(), "a ledger's database opens no metadata store")
	set := livesettest.OpenLedgerDB(t, db)
	cm, err := chain.NewManager(t.Context(), db, nil)
	require.NoError(t, err)
	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:     db,
		UtxoSet:      set,
		ChainManager: cm,
	})
	require.NoError(t, err)
	return coreLedger{db: db, set: set, ls: ls}
}

// session serves the MCP tools as a node does: from the ledger, over the
// database the node gives its APIs, with indexer the IndexerDB's SQLite
// connection or nil for a node without one.
func (c coreLedger) session(t *testing.T, indexer *sql.DB) *mcp.ClientSession {
	t.Helper()
	server, _, err := NewMCPServer(
		t.Context(),
		DefaultProviderConfig(),
		ProviderDependencies{
			Database:    c.db,
			SQLDB:       indexer,
			LedgerState: c.ls,
			Network:     "preview",
		},
	)
	require.NoError(t, err)
	ct, st := mcp.NewInMemoryTransports()
	ss, err := server.Connect(t.Context(), st, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := mcp.NewClient(
		&mcp.Implementation{Name: "test", Version: "1"}, nil,
	).Connect(t.Context(), ct, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

// get_account answers from the ledger's accounts on a node without an
// IndexerDB, naming the IndexerDB row references it cannot fill, and takes
// those from the IndexerDB on a node with one.
func TestAccountToolReadsTheLedgerState(t *testing.T) {
	t.Parallel()
	c := newCoreLedger(t)
	key := bytes.Repeat([]byte{0xab}, 28)
	pool := bytes.Repeat([]byte{0xcd}, 28)
	require.NoError(t, c.set.ApplyLedgerChanges(nil, ledger.LedgerChanges{
		Accounts: []ledger.AccountChange{{
			Slot: 40,
			Account: models.Account{
				StakingKey: key, Pool: pool, Reward: 1234567,
				AddedSlot: 40, CreatedSlot: 30, Active: true,
			},
		}},
	}))
	args := map[string]any{
		"stake_address_or_credential": hex.EncodeToString(key),
		"credential_type":             "key",
	}

	text := callTool(t, c.session(t, nil), "get_account", args, false)
	require.Contains(t, text, hex.EncodeToString(pool))
	require.Contains(t, text, "1234567")
	require.Contains(t, text, needsIndexerDB)

	indexer := newSchemaDB(t)
	_, err := indexer.Exec(
		`INSERT INTO account (id, staking_key, credential_tag, certificate_id, added_slot, created_slot)
 VALUES (77, ?, 0, 88, 40, 30)`,
		key,
	)
	require.NoError(t, err)
	text = callTool(t, c.session(t, indexer), "get_account", args, false)
	require.Contains(t, text, hex.EncodeToString(pool))
	require.Contains(t, text, "| 77 | 88 |")

	missing := map[string]any{
		"stake_address_or_credential": hex.EncodeToString(
			bytes.Repeat([]byte{0x01}, 28),
		),
		"credential_type": "key",
	}
	text = callTool(t, c.session(t, nil), "get_account", missing, true)
	require.Contains(t, text, "not found")
}

// get_pool_performance reads the epochs the ledger keeps on a node without an
// IndexerDB, saying that older ones need one, and the IndexerDB's history on
// a node with one.
func TestPoolPerformanceReadsTheLedgerStateOrTheIndexerDB(t *testing.T) {
	t.Parallel()
	c := newCoreLedger(t)
	pool := bytes.Repeat([]byte{0x61}, 28)
	other := bytes.Repeat([]byte{0x62}, 28)
	blocks := uint64(7)
	require.NoError(t, c.set.ApplyLedgerChanges(nil, ledger.LedgerChanges{
		Epochs: []models.Epoch{
			{EpochId: 4, StartSlot: 400, LengthInSlots: 100, SlotLength: 1000, EraId: 6},
			{EpochId: 5, StartSlot: 500, LengthInSlots: 100, SlotLength: 1000, EraId: 6},
		},
		RewardInputs: []ledger.RewardInputChange{{
			Kind:  ledger.RewardInputPools,
			Epoch: 5,
			Pools: []*models.RewardPoolInput{{
				Epoch: 5, PoolKeyHash: pool, Pledge: 300, Cost: 340,
				DelegatedStake: 9000, BlocksProduced: &blocks,
				Margin:        &types.Rat{Rat: big.NewRat(1, 20)},
				RewardAccount: bytes.Repeat([]byte{1}, 29),
				CapturedSlot:  510, BoundarySlot: 500,
			}},
		}},
		RewardDerived: []ledger.RewardDerivedChange{{
			Kind:  ledger.RewardDerivedPools,
			Epoch: 5,
			Pools: []*models.RewardPoolOutput{
				{
					Epoch: 5, PoolKeyHash: pool, TotalReward: 5555,
					LeaderReward: 555, MemberRewardTotal: 5000,
					ApparentPerformance: &types.Rat{Rat: big.NewRat(9, 10)},
					CapturedSlot:        520, BoundarySlot: 500,
				},
				{
					Epoch: 5, PoolKeyHash: other, TotalReward: 1,
					CapturedSlot: 520, BoundarySlot: 500,
				},
			},
		}},
	}))
	args := map[string]any{"pool_id": hex.EncodeToString(pool)}

	text := callTool(t, c.session(t, nil), "get_pool_performance", args, false)
	require.Contains(
		t, text,
		"| 5 | 7 | 9000 | 300 | 340 | 1/20 | 9/10 | 5555 | 555 | 5000 |",
	)
	require.Contains(t, text, "Showing 1 epoch snapshots")
	require.Contains(t, text, "older epochs "+needsIndexerDB)

	indexer := newSchemaDB(t)
	_, err := indexer.Exec(
		`INSERT INTO reward_pool_output (epoch, pool_key_hash, apparent_performance, optimal_reward, total_reward, leader_reward, member_reward_total, owner_stake, undistributed, unspendable, captured_slot, boundary_slot)
 VALUES (2, ?, '1', '0', '4444', '444', '4000', '0', '0', '0', 220, 200)`,
		pool,
	)
	require.NoError(t, err)
	text = callTool(
		t, c.session(t, indexer), "get_pool_performance", args, false,
	)
	require.Contains(t, text, "| 2 |")
	require.Contains(t, text, "4444")
	require.Contains(t, text, "Showing 1 epoch snapshots")
}

// On a node without an IndexerDB, get_utxos reads the live outputs from the
// ledger's set, by full address and by credential; the tools that read only
// API indexes say they need an IndexerDB.
func TestUtxosReadTheLiveSetWithoutAnIndexerDB(t *testing.T) {
	t.Parallel()
	c := newCoreLedger(t)
	payment := bytes.Repeat([]byte{0x11}, 28)
	stake := bytes.Repeat([]byte{0x22}, 28)
	base, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey, 0, payment, stake,
	)
	require.NoError(t, err)
	enterprise, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone, 0, payment, nil,
	)
	require.NoError(t, err)
	stranger, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone, 0, bytes.Repeat([]byte{0x33}, 28), nil,
	)
	require.NoError(t, err)
	var added []ledger.UtxoOutput
	for i, out := range []struct {
		addr   lcommon.Address
		amount uint64
	}{{base, 2_000_000}, {enterprise, 3_000_000}, {stranger, 4_000_000}} {
		raw, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
			OutputAddress: out.addr,
			OutputAmount:  out.amount,
		})
		require.NoError(t, err)
		added = append(added, ledger.UtxoOutput{
			Ref: models.UtxoId{
				Hash: bytes.Repeat([]byte{byte(0xa0 + i)}, 32),
				Idx:  uint32(i),
			},
			Cbor: raw,
		})
	}
	require.NoError(t, c.set.ApplyUtxoChanges(nil, []ledger.UtxoEvent{
		{Added: added},
	}))
	cs := c.session(t, nil)

	text := callTool(t, cs, "get_utxos", map[string]any{
		"address_or_credential": base.String(),
	}, false)
	require.Contains(t, text, "Showing 1 results")
	require.Contains(t, text, "2000000")

	text = callTool(t, cs, "get_utxos", map[string]any{
		"address_or_credential": hex.EncodeToString(payment),
	}, false)
	require.Contains(t, text, "Showing 2 results")
	require.Contains(t, text, "2000000")
	require.Contains(t, text, "3000000")

	text = callTool(t, cs, "get_utxos", map[string]any{
		"address_or_credential": hex.EncodeToString(payment),
		"offset":                1,
	}, false)
	require.Contains(t, text, "Showing 1 results")

	text = callTool(t, cs, "get_utxos", map[string]any{
		"address_or_credential": base.String(),
		"cursor":                "abc",
	}, true)
	require.Contains(t, text, needsIndexerDB)

	text = callTool(t, cs, "get_utxos_by_asset", map[string]any{
		"policy_id": hex.EncodeToString(bytes.Repeat([]byte{0x44}, 28)),
	}, true)
	require.Contains(t, text, needsIndexerDB)
}

// On a node with an IndexerDB the SQL tools query it, its account and epoch
// tables included; on a node without one they say an IndexerDB is needed.
func TestSQLiteToolsQueryTheIndexerDB(t *testing.T) {
	t.Parallel()
	c := newCoreLedger(t)
	for _, tool := range []string{"sqlite_query", "sqlite_explain"} {
		for _, query := range []string{
			"SELECT count(*) FROM account",
			"SELECT epoch_id FROM epoch",
			"SELECT count(*) FROM block_nonce",
		} {
			args := map[string]any{"query": query}
			text := callTool(t, c.session(t, newSchemaDB(t)), tool, args, false)
			require.NotContains(t, text, "refused", tool+": "+query)

			text = callTool(t, c.session(t, nil), tool, args, true)
			require.Contains(t, text, needsIndexerDB, tool+": "+query)
		}
	}
}

// get_epoch_summary on a node built as NewMCPServer builds it counts the
// IndexerDB's blocks, with no retained-window caveat, beside a ledger that
// holds the epochs.
func TestEpochSummaryServerCountsIndexerBlocks(t *testing.T) {
	t.Parallel()
	c := newCoreLedger(t)
	require.NoError(t, c.set.ApplyLedgerChanges(nil, ledger.LedgerChanges{
		Epochs: []models.Epoch{
			{
				EpochId: 5, StartSlot: 1000, LengthInSlots: 100,
				SlotLength: 1000, EraId: 6, Nonce: bytes.Repeat([]byte{0xa5}, 32),
			},
			{
				EpochId: 6, StartSlot: 1100, LengthInSlots: 100,
				SlotLength: 1000, EraId: 6, Nonce: bytes.Repeat([]byte{0xa6}, 32),
			},
		},
	}))
	idx, err := dbtest.NewIndexerDatabase(t, c.db, dbtest.IndexerOptions{})
	require.NoError(t, err)
	// The IndexerDB writer derives its own epoch rows, so an API node's
	// IndexerDB holds the epochs the ledger does.
	for _, e := range []uint64{5, 6} {
		nonce := bytes.Repeat([]byte{byte(0xa0 + e)}, 32)
		require.NoError(t, idx.SetEpoch(
			1000+100*(e-5), e, nonce, nil, nil, nil, 6, 1000, 100, nil,
		))
	}
	for i, slot := range []uint64{1000, 1010, 1050, 1099, 1100} {
		h := bytes.Repeat([]byte{byte(0x70 + i)}, 32)
		require.NoError(t, idx.SetBlockNonce(h, slot, h, false, nil))
	}
	server, _, err := NewMCPServer(
		t.Context(),
		DefaultProviderConfig(),
		ProviderDependencies{
			Database:    idx,
			LedgerState: c.ls,
			Network:     "preview",
		},
	)
	require.NoError(t, err)
	ct, st := mcp.NewInMemoryTransports()
	ss, err := server.Connect(t.Context(), st, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := mcp.NewClient(
		&mcp.Implementation{Name: "test", Version: "1"}, nil,
	).Connect(t.Context(), ct, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })

	got := callTool(t, cs, "get_epoch_summary", map[string]any{"epoch": 5}, false)
	require.Contains(t, got, "- **Recorded Blocks**: 4\n")
	require.NotContains(t, got, retainedWindowNote)
}
