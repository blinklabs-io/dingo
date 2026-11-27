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
	"context"
	"database/sql"
	"encoding/hex"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

// recordingLedgerDB is the LedgerDB over a database, noting the name of each
// ledger state read the tools make through it.
type recordingLedgerDB struct {
	ledgerdb.LedgerDB
	mu    sync.Mutex
	calls []string
}

func (r *recordingLedgerDB) note(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, name)
}

func (r *recordingLedgerDB) taken() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := slices.Clone(r.calls)
	r.calls = nil
	return out
}

func (r *recordingLedgerDB) GetEpochs(
	txn ledgerdb.Txn,
) ([]models.Epoch, error) {
	r.note("GetEpochs")
	return r.LedgerDB.GetEpochs(txn)
}

func (r *recordingLedgerDB) GetEpoch(
	epoch uint64,
	txn ledgerdb.Txn,
) (*models.Epoch, error) {
	r.note("GetEpoch")
	return r.LedgerDB.GetEpoch(epoch, txn)
}

func (r *recordingLedgerDB) GetEpochSummary(
	epoch uint64,
	txn ledgerdb.Txn,
) (*models.EpochSummary, error) {
	r.note("GetEpochSummary")
	return r.LedgerDB.GetEpochSummary(epoch, txn)
}

func (r *recordingLedgerDB) GetBlockNoncesInSlotRange(
	start, end uint64,
	txn ledgerdb.Txn,
) ([]models.BlockNonce, error) {
	r.note("GetBlockNoncesInSlotRange")
	return r.LedgerDB.GetBlockNoncesInSlotRange(start, end, txn)
}

func (r *recordingLedgerDB) GetConstitution(
	txn ledgerdb.Txn,
) (*models.Constitution, error) {
	r.note("GetConstitution")
	return r.LedgerDB.GetConstitution(txn)
}

func (r *recordingLedgerDB) LatestCommitteeQuorum(
	ctx context.Context,
	txn ledgerdb.Txn,
) (*models.CommitteeQuorum, error) {
	r.note("LatestCommitteeQuorum")
	return r.LedgerDB.LatestCommitteeQuorum(ctx, txn)
}

func (r *recordingLedgerDB) GetCommitteeMembers(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.CommitteeMember, error) {
	r.note("GetCommitteeMembers")
	return r.LedgerDB.GetCommitteeMembers(ctx, txn)
}

func (r *recordingLedgerDB) GetActiveDreps(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.Drep, error) {
	r.note("GetActiveDreps")
	return r.LedgerDB.GetActiveDreps(ctx, txn)
}

func (r *recordingLedgerDB) GetDrepByCredential(
	ctx context.Context,
	tag uint8,
	cred []byte,
	includeInactive bool,
	txn ledgerdb.Txn,
) (*models.Drep, error) {
	r.note("GetDrepByCredential")
	return r.LedgerDB.GetDrepByCredential(ctx, tag, cred, includeInactive, txn)
}

func (r *recordingLedgerDB) GetGovernanceProposal(
	ctx context.Context,
	txHash []byte,
	actionIndex uint32,
	txn ledgerdb.Txn,
) (*models.GovernanceProposal, error) {
	r.note("GetGovernanceProposal")
	return r.LedgerDB.GetGovernanceProposal(ctx, txHash, actionIndex, txn)
}

func (r *recordingLedgerDB) GetAllGovernanceProposals(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	r.note("GetAllGovernanceProposals")
	return r.LedgerDB.GetAllGovernanceProposals(ctx, txn)
}

func (r *recordingLedgerDB) GetGovernanceVotes(
	ctx context.Context,
	proposalID uint,
	txn ledgerdb.Txn,
) ([]*models.GovernanceVote, error) {
	r.note("GetGovernanceVotes")
	return r.LedgerDB.GetGovernanceVotes(ctx, proposalID, txn)
}

// recordedLedgerState is a ledger state whose LedgerDB is a recording one.
// The tools under test read nothing else of it.
type recordedLedgerState struct {
	*ledger.LedgerState
	ldb ledgerdb.LedgerDB
}

func (s recordedLedgerState) LedgerDB() ledgerdb.LedgerDB { return s.ldb }

// ledgerToolSession serves the Cardano tools from ldb alone: no SQLite
// connection is given, so a tool that still queries SQL fails.
func ledgerToolSession(
	t *testing.T,
	ldb ledgerdb.LedgerDB,
) *mcp.ClientSession {
	t.Helper()
	return ledgerToolSessionWithIndexer(t, ldb, indexerReads{})
}

// ledgerToolSessionWithIndexer is ledgerToolSession on a node whose IndexerDB
// answers reads.
func ledgerToolSessionWithIndexer(
	t *testing.T,
	ldb ledgerdb.LedgerDB,
	reads indexerReads,
) *mcp.ClientSession {
	t.Helper()
	server := mcp.NewServer(
		&mcp.Implementation{Name: "test", Version: "1"},
		nil,
	)
	registerCardanoTools(
		server, nil, recordedLedgerState{ldb: ldb}, nil, "preview",
		5*time.Second, newReadLimits(), reads,
	)
	ct, st := mcp.NewInMemoryTransports()
	ss, err := server.Connect(t.Context(), st, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).
		Connect(t.Context(), ct, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

func seedLedgerStateRows(
	t *testing.T,
	db *sql.DB,
) (proposal string, drep string) {
	t.Helper()
	exec := func(q string, args ...any) {
		_, err := db.Exec(q, args...)
		require.NoError(t, err, q)
	}
	hash := func(b byte, n int) []byte { return bytes.Repeat([]byte{b}, n) }
	for _, e := range []struct {
		id, start, length int
		nonce             byte
	}{{5, 1000, 100, 0xa5}, {6, 1100, 100, 0xa6}} {
		exec(
			`INSERT INTO epoch (epoch_id, nonce, start_slot, era_id, slot_length, length_in_slots)
 VALUES (?,?,?,6,1000,?)`,
			e.id,
			hash(e.nonce, 32),
			e.start,
			e.length,
		)
	}
	exec(
		`INSERT INTO epoch_summary (epoch,total_active_stake,total_pool_count,total_delegators,epoch_nonce,boundary_slot,snapshot_ready)
 VALUES (6,'1',1,1,?,1100,1)`,
		hash(0xb6, 32),
	)
	for i, slot := range []int{1100, 1150, 1199, 1200, 1099} {
		exec(
			`INSERT INTO block_nonce (hash,nonce,slot,is_checkpoint) VALUES (?,?,?,0)`,
			hash(byte(i+1), 32),
			hash(byte(i+1), 32),
			slot,
		)
	}
	exec(
		`INSERT INTO constitution (anchor_url,anchor_hash,policy_hash,added_slot) VALUES ('https://example.com/constitution',?,?,5000)`,
		hash(0x44, 32),
		hash(0x45, 28),
	)
	exec(`INSERT INTO committee_quorum (quorum,added_slot) VALUES ('2/3',5000)`)
	exec(
		`INSERT INTO committee_member (cold_cred_hash,expires_epoch,term_start_slot,added_slot) VALUES (?,700,5000,5000)`,
		hash(0x55, 28),
	)
	exec(
		`INSERT INTO committee_member (cold_cred_hash,expires_epoch,term_start_slot,added_slot) VALUES (?,600,4000,4000)`,
		hash(0x56, 28),
	)
	exec(
		`INSERT INTO committee_member (cold_cred_hash,expires_epoch,term_start_slot,added_slot,deleted_slot) VALUES (?,650,4000,4000,4500)`,
		hash(0x57, 28),
	)
	exec(
		`INSERT INTO drep (credential_tag,credential,anchor_url,anchor_hash,added_slot,last_activity_epoch,expiry_epoch,active)
 VALUES (0,?,'https://drep.example',?,5100,480,550,1)`,
		hash(0x66, 28),
		hash(0x44, 32),
	)
	exec(
		`INSERT INTO drep (credential_tag,credential,anchor_url,anchor_hash,added_slot,last_activity_epoch,expiry_epoch,active)
 VALUES (0,?,'',NULL,5200,0,0,1)`,
		hash(0x67, 28),
	)
	exec(
		`INSERT INTO drep (credential_tag,credential,anchor_url,anchor_hash,added_slot,last_activity_epoch,expiry_epoch,active)
 VALUES (0,?,'https://gone.example',?,5300,1,2,0)`,
		hash(0x68, 28),
		hash(0x44, 32),
	)
	for i, p := range []struct {
		hash                       byte
		enacted, ratified, expired any
		deleted                    any
	}{
		{0xca, nil, nil, nil, nil},
		{0xcb, 510, nil, nil, nil},
		{0xcc, nil, 509, nil, nil},
		{0xcd, nil, nil, 508, nil},
		{0xce, nil, nil, nil, 130},
	} {
		exec(`INSERT INTO governance_proposal
 (id,tx_hash,action_index,action_type,proposed_epoch,expires_epoch,enacted_epoch,ratified_epoch,expired_epoch,anchor_url,anchor_hash,deposit,return_address,added_slot,deleted_slot)
 VALUES (?,?,0,0,500,505,?,?,?,'https://example.com/proposal',?,100000000000,?,123,?)`,
			i+1, hash(p.hash, 32), p.enacted, p.ratified, p.expired,
			hash(0xda, 32), hash(1, 29), p.deleted)
	}
	for i, vote := range [][2]int{{0, 1}, {0, 1}, {1, 1}, {1, 0}, {2, 2}, {1, 2}} {
		slot := 123
		var deleted any
		if i == 5 {
			deleted = 124
		}
		exec(
			`INSERT INTO governance_vote (proposal_id,voter_type,voter_credential,vote,anchor_url,added_slot,deleted_slot) VALUES (1,?,?,?,'',?,?)`,
			vote[0],
			hash(byte(i+1), 28),
			vote[1],
			slot,
			deleted,
		)
	}
	return hex.EncodeToString(
			hash(0xca, 32),
		), hex.EncodeToString(
			hash(0x66, 28),
		)
}

// TestLedgerStateToolsReadThroughLedgerDB runs the tools that read ledger
// state with a recording LedgerDB and no SQLite connection, and requires each
// to answer, from the LedgerDB reads that name its data, with the text the
// SQLite path prints for the same database.
func TestLedgerStateToolsReadThroughLedgerDB(t *testing.T) {
	t.Parallel()
	nodeDB, err := dbtest.NewReleasedDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, nodeDB)
	require.NoError(t, err)
	proposal, drep := seedLedgerStateRows(t, raw)
	rec := &recordingLedgerDB{LedgerDB: dbadapter.New(nodeDB)}
	viaLedger := ledgerToolSession(t, rec)
	viaSQL := newToolSession(t, raw, 100)

	cases := []struct {
		name string
		tool string
		args map[string]any
		want []string
	}{
		{
			"latest epoch", "get_epoch_summary", map[string]any{},
			[]string{
				"GetEpochs",
				"GetEpoch",
				"GetEpochSummary",
				"GetBlockNoncesInSlotRange",
			},
		},
		{
			"named epoch", "get_epoch_summary", map[string]any{"epoch": 5},
			[]string{
				"GetEpoch",
				"GetEpochSummary",
				"GetBlockNoncesInSlotRange",
			},
		},
		{
			"governance state", "get_governance_state", map[string]any{},
			[]string{
				"GetConstitution", "LatestCommitteeQuorum",
				"GetCommitteeMembers", "GetActiveDreps",
			},
		},
		{
			"one DRep", "get_governance_state",
			map[string]any{"drep_credential": drep},
			[]string{
				"GetConstitution", "LatestCommitteeQuorum",
				"GetCommitteeMembers", "GetActiveDreps", "GetDrepByCredential",
			},
		},
		{
			"missing DRep", "get_governance_state",
			map[string]any{
				"drep_credential": hex.EncodeToString(
					bytes.Repeat([]byte{0x99}, 28),
				),
			},
			[]string{
				"GetConstitution", "LatestCommitteeQuorum",
				"GetCommitteeMembers", "GetActiveDreps", "GetDrepByCredential",
			},
		},
		{
			"proposal detail", "get_governance_proposal",
			map[string]any{"tx_hash": proposal},
			[]string{"GetGovernanceProposal", "GetGovernanceVotes"},
		},
		{
			"proposal of another status", "get_governance_proposal",
			map[string]any{"tx_hash": proposal, "status": "enacted"},
			[]string{"GetGovernanceProposal"},
		},
	}
	for _, status := range []string{"", "active", "ratified", "enacted", "expired", "all"} {
		cases = append(cases, struct {
			name string
			tool string
			args map[string]any
			want []string
		}{
			"proposals " + status, "get_governance_proposal",
			map[string]any{"status": status, "limit": 3},
			[]string{"GetAllGovernanceProposals"},
		})
	}
	for _, tc := range cases {
		rec.taken()
		res, err := viaLedger.CallTool(
			t.Context(), &mcp.CallToolParams{Name: tc.tool, Arguments: tc.args},
		)
		require.NoError(t, err, tc.name)
		require.False(
			t,
			res.IsError,
			"%s: %v",
			tc.name,
			res.Content[0].(*mcp.TextContent).Text,
		)
		got := res.Content[0].(*mcp.TextContent).Text
		require.Equal(t, tc.want, rec.taken(), tc.name)
		// Only the ledger's count is limited to its retained window, so only
		// it carries the caveat; the counts themselves must agree.
		got = strings.Replace(got, retainedWindowNote, "", 1)
		require.Equal(
			t, callTool(t, viaSQL, tc.tool, tc.args, false), got, tc.name,
		)
	}
}

// With the epoch and proposal rows absent the tools report what the SQLite
// path does, through the LedgerDB.
func TestLedgerStateToolsReportAbsentRows(t *testing.T) {
	t.Parallel()
	nodeDB, err := dbtest.NewReleasedDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, nodeDB)
	require.NoError(t, err)
	viaLedger := ledgerToolSession(t, dbadapter.New(nodeDB))
	viaSQL := newToolSession(t, raw, 100)
	hash := hex.EncodeToString(bytes.Repeat([]byte{0xca}, 32))
	for _, tc := range []struct {
		tool string
		args map[string]any
	}{
		{"get_governance_state", map[string]any{}},
		{"get_governance_proposal", map[string]any{}},
		{"get_governance_proposal", map[string]any{"tx_hash": hash}},
	} {
		require.Equal(
			t,
			callTool(t, viaSQL, tc.tool, tc.args, false),
			callTool(t, viaLedger, tc.tool, tc.args, false),
			tc.tool,
		)
	}
	for _, args := range []map[string]any{{}, {"epoch": 9}} {
		sqlText := callTool(t, viaSQL, "get_epoch_summary", args, true)
		require.Equal(
			t, sqlText,
			callTool(t, viaLedger, "get_epoch_summary", args, true),
		)
	}
}

// TestEpochSummaryCountsIndexerBlocksOnAnAPINode gives the ledger one retained
// block nonce of epoch 5, as pruning leaves an older epoch, and the IndexerDB
// all four of its blocks. An API node reports the IndexerDB's count; a core
// node reports the ledger's and says it covers the retained window only.
func TestEpochSummaryCountsIndexerBlocksOnAnAPINode(t *testing.T) {
	t.Parallel()
	nodeDB, err := dbtest.NewReleasedDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, nodeDB)
	require.NoError(t, err)
	seedLedgerStateRows(t, raw)
	idx, err := dbtest.NewIndexerDatabase(t, nodeDB, dbtest.IndexerOptions{})
	require.NoError(t, err)
	// The ledger's epoch 6 runs from slot 1100, so a block at 1100 is not
	// epoch 5's.
	for i, slot := range []uint64{1000, 1010, 1050, 1099, 1100} {
		h := bytes.Repeat([]byte{byte(0x70 + i)}, 32)
		require.NoError(t, idx.SetBlockNonce(h, slot, h, false, nil))
	}
	ldb := dbadapter.New(nodeDB)
	args := map[string]any{"epoch": 5}

	api := callTool(t, ledgerToolSessionWithIndexer(t, ldb, indexerReads{
		epochBlocks: indexerEpochBlocks(idx),
	}), "get_epoch_summary", args, false)
	require.Contains(t, api, "- **Recorded Blocks**: 4\n")

	core := callTool(
		t, ledgerToolSession(t, ldb), "get_epoch_summary", args, false,
	)
	require.Contains(t, core, "- **Recorded Blocks**: 1"+retainedWindowNote+"\n")
}

// A ledger-state import seeds an enacted root at added_slot 0 whose
// enacted_epoch is the snapshot anchor. Both read paths must list it as
// enacted, match it with the enacted filter, and leave out the anchor epoch.
func TestSeededRootProposalReadsEnactedWithoutEpoch(t *testing.T) {
	t.Parallel()
	nodeDB, err := dbtest.NewReleasedDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, nodeDB)
	require.NoError(t, err)
	txHash := bytes.Repeat([]byte{0x06}, 32)
	_, err = raw.Exec(`INSERT INTO governance_proposal
 (id,tx_hash,action_index,action_type,proposed_epoch,expires_epoch,enacted_epoch,anchor_url,anchor_hash,deposit,return_address,added_slot)
 VALUES (1,?,0,2,0,0,1458,'',?,0,?,0)`,
		txHash, make([]byte, 32), make([]byte, 29))
	require.NoError(t, err)
	viaLedger := ledgerToolSession(t, dbadapter.New(nodeDB))
	viaSQL := newToolSession(t, raw, 100)
	hash := hex.EncodeToString(txHash)
	for _, args := range []map[string]any{
		{"tx_hash": hash, "status": "enacted"},
		{"status": "enacted"},
		{"status": "all"},
	} {
		got := callTool(t, viaLedger, "get_governance_proposal", args, false)
		require.Equal(
			t, callTool(t, viaSQL, "get_governance_proposal", args, false),
			got, "%v", args,
		)
		require.Contains(t, got, "Enacted", "%v", args)
		require.NotContains(t, got, "1458", "%v", args)
		require.NotContains(t, got, "Active", "%v", args)
	}
}
