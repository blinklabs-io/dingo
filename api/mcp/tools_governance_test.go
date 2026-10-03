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

package mcp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetGovernanceState(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE constitution (
			anchor_url TEXT,
			anchor_hash BLOB,
			policy_hash BLOB,
			added_slot INTEGER,
			deleted_slot INTEGER
		);
		CREATE TABLE committee_quorum (
			quorum TEXT,
			added_slot INTEGER
		);
		CREATE TABLE committee_member (
			cold_cred_hash BLOB,
			expires_epoch INTEGER,
			term_start_slot INTEGER,
			added_slot INTEGER,
			deleted_slot INTEGER
		);
		CREATE TABLE drep (
		credential_tag INTEGER NOT NULL DEFAULT 0,
			credential BLOB,
			anchor_url TEXT,
			anchor_hash BLOB,
			added_slot INTEGER,
			last_activity_epoch INTEGER,
			expiry_epoch INTEGER,
			active BOOLEAN
		);
	`)
	require.NoError(t, err)

	constHash, _ := hex.DecodeString(
		"4444444444444444444444444444444444444444444444444444444444444444",
	)
	commColdHash, _ := hex.DecodeString(
		"55555555555555555555555555555555555555555555555555555555",
	)
	drepCred, _ := hex.DecodeString(
		"66666666666666666666666666666666666666666666666666666666",
	)

	// 1. Insert governance state
	_, err = db.Exec(`
		INSERT INTO constitution (anchor_url, anchor_hash, added_slot)
		VALUES ('https://cardanofoundation.org/constitution.pdf', ?, 5000)
	`, constHash)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO committee_quorum (quorum, added_slot) VALUES ('2/3', 5000)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(`
		INSERT INTO committee_member (cold_cred_hash, expires_epoch, term_start_slot)
		VALUES (?, 600, 5000)
	`, commColdHash)
	require.NoError(t, err)

	_, err = db.Exec(`
		INSERT INTO drep (credential, anchor_url, anchor_hash, added_slot, last_activity_epoch, expiry_epoch, active)
		VALUES (?, 'https://drep.me', ?, 5100, 480, 550, 1)
	`, drepCred, constHash)
	require.NoError(t, err)

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test-server", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(server, db, nil, nil, "preview", 5*time.Second)

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)

	client := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"},
		nil,
	)
	cs, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer cs.Close()

	// 1. Success query overall governance state
	resGov, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_governance_state",
		Arguments: map[string]any{},
	})
	require.NoError(t, err)
	assert.False(t, resGov.IsError)
	govText := resGov.Content[0].(*mcp.TextContent).Text
	assert.Contains(
		t,
		govText,
		"https://cardanofoundation.org/constitution.pdf",
	)
	assert.Contains(t, govText, "2/3")
	assert.Contains(t, govText, "**Active Registered DReps**: 1")

	// 2. Query specific DRep by hex
	resDrep, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name: "get_governance_state",
		Arguments: map[string]any{
			"drep_credential": "66666666666666666666666666666666666666666666666666666666",
		},
	})
	require.NoError(t, err)
	assert.False(t, resDrep.IsError)
	assert.Contains(
		t,
		resDrep.Content[0].(*mcp.TextContent).Text,
		"https://drep.me",
	)

	// 3. Query specific DRep with invalid format
	resBadDrep, err := cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_governance_state",
		Arguments: map[string]any{"drep_credential": "invalid-drep-id!"},
	})
	require.NoError(t, err)
	assert.False(t, resBadDrep.IsError)
	assert.Contains(
		t,
		resBadDrep.Content[0].(*mcp.TextContent).Text,
		"Could not parse DRep identifier",
	)

	// 4. Nil DB
	serverNil := mcp.NewServer(
		&mcp.Implementation{Name: "nil-server", Version: "1.0"},
		nil,
	)
	RegisterCardanoTools(serverNil, nil, nil, nil, "preview", 5*time.Second)
	ctNil, stNil := mcp.NewInMemoryTransports()
	_, _ = serverNil.Connect(ctx, stNil, nil)
	csNil, _ := client.Connect(ctx, ctNil, nil)
	defer csNil.Close()

	resNilDB, err := csNil.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_governance_state",
		Arguments: map[string]any{},
	})
	require.NoError(t, err)
	assert.True(t, resNilDB.IsError)
}

func newGovernanceProposalDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	nodeDB, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	db, err := dbtest.RawSQLiteMetadata(t, nodeDB)
	require.NoError(t, err)
	hash := bytes.Repeat([]byte{0xca}, 32)
	_, err = db.Exec(`INSERT INTO governance_proposal
 (id,tx_hash,action_index,action_type,proposed_epoch,expires_epoch,anchor_url,anchor_hash,deposit,return_address,added_slot)
 VALUES (1,?,0,0,500,505,'https://example.com/proposal',?,100000000000,?,123)`, hash, bytes.Repeat([]byte{0xda}, 32), bytes.Repeat([]byte{1}, 29))
	require.NoError(t, err)
	for i, vote := range [][2]int{{0, 1}, {0, 1}, {0, 1}, {1, 1}, {1, 1}, {1, 0}, {2, 2}} {
		_, err = db.Exec(
			`INSERT INTO governance_vote (proposal_id,voter_type,voter_credential,vote,added_slot) VALUES (1,?,?,?,123)`,
			vote[0],
			bytes.Repeat([]byte{byte(i + 1)}, 28),
			vote[1],
		)
		require.NoError(t, err)
	}
	return db, hex.EncodeToString(hash)
}

func TestGetGovernanceProposal(t *testing.T) {
	t.Parallel()
	db, hash := newGovernanceProposalDB(t)
	cs := newToolSession(t, db, 100)
	list := callTool(
		t,
		cs,
		"get_governance_proposal",
		map[string]any{"status": "active"},
		false,
	)
	require.Contains(
		t,
		list,
		"| `"+hash+"#0` | ParameterChange | Active | Epoch 500 | Epoch 505 | 100000.00 |",
	)
	detail := callTool(
		t,
		cs,
		"get_governance_proposal",
		map[string]any{"tx_hash": hash},
		false,
	)
	for _, row := range []string{
		"| **Proposal Deposit** | 100000000000 Lovelace (100000.000000 ADA) |",
		"| **Constitutional Committee (CC)** | 3 | 0 | 0 |",
		"| **Delegated Representatives (DReps)** | 2 | 1 | 0 |",
		"| **Stake Pool Operators (SPOs)** | 0 | 0 | 1 |",
	} {
		require.Contains(t, detail, row)
	}
}

func TestGovernanceProposalExcludesDeletedRecords(t *testing.T) {
	t.Parallel()
	t.Run("votes", func(t *testing.T) {
		t.Parallel()
		db, hash := newGovernanceProposalDB(t)
		_, err := db.Exec(
			"UPDATE governance_vote SET deleted_slot=124 WHERE voter_type=0",
		)
		require.NoError(t, err)
		text := callTool(
			t,
			newToolSession(t, db, 100),
			"get_governance_proposal",
			map[string]any{"tx_hash": hash},
			false,
		)
		require.Contains(
			t,
			text,
			"| **Constitutional Committee (CC)** | 0 | 0 | 0 |",
		)
		require.Contains(
			t,
			text,
			"| **Delegated Representatives (DReps)** | 2 | 1 | 0 |",
		)
	})
	t.Run("proposal", func(t *testing.T) {
		t.Parallel()
		db, hash := newGovernanceProposalDB(t)
		_, err := db.Exec("UPDATE governance_proposal SET deleted_slot=124")
		require.NoError(t, err)
		cs := newToolSession(t, db, 100)
		for _, status := range []string{"active", "all", "ratified", "enacted", "expired"} {
			text := callTool(
				t,
				cs,
				"get_governance_proposal",
				map[string]any{"status": status},
				false,
			)
			require.NotContains(t, text, hash)
		}
		text := callTool(
			t,
			cs,
			"get_governance_proposal",
			map[string]any{"tx_hash": hash},
			false,
		)
		require.Contains(t, text, "was not found")
	})
}

func TestGovernanceProposalQueryErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, sql, expected string
		detail              bool
	}{
		{"missing vote table", "DROP TABLE governance_vote", "query vote tallies", true},
		{"invalid vote type", "UPDATE governance_vote SET voter_type='bad'", "scan vote tally", true},
		{"unknown voter role", "UPDATE governance_vote SET voter_type=99", "invalid voter type", true},
		{"unknown vote", "UPDATE governance_vote SET vote=99", "invalid vote", true},
		{"invalid detail amount", "UPDATE governance_proposal SET deposit='bad'", "query governance proposal", true},
		{"invalid list amount", "UPDATE governance_proposal SET deposit='bad'", "scan governance proposal", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, hash := newGovernanceProposalDB(t)
			_, err := db.Exec(tc.sql)
			require.NoError(t, err)
			args := map[string]any{}
			if tc.detail {
				args["tx_hash"] = hash
			}
			text := callTool(
				t,
				newToolSession(t, db, 100),
				"get_governance_proposal",
				args,
				true,
			)
			require.Contains(t, text, tc.expected)
		})
	}
}

func TestGovernanceProposalStatusFilters(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		update   string
		included map[string]bool
	}{
		{name: "active", included: map[string]bool{"": true, "active": true, "all": true}},
		{name: "ratified", update: "ratified_epoch=503", included: map[string]bool{"ratified": true, "all": true}},
		{
			name:   "enacted",
			update: "ratified_epoch=503, enacted_epoch=504",
			included: map[string]bool{
				"ratified": true,
				"enacted":  true,
				"all":      true,
			},
		},
		{name: "expired", update: "expired_epoch=506", included: map[string]bool{"expired": true, "all": true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, hash := newGovernanceProposalDB(t)
			if tc.update != "" {
				_, err := db.Exec("UPDATE governance_proposal SET " + tc.update)
				require.NoError(t, err)
			}
			cs := newToolSession(t, db, 100)
			for _, status := range []string{"", "active", "ratified", "enacted", "expired", "all"} {
				text := callTool(
					t,
					cs,
					"get_governance_proposal",
					map[string]any{"status": status},
					false,
				)
				require.Equal(
					t,
					tc.included[status],
					strings.Contains(text, hash),
					"status %q: %s",
					status,
					text,
				)
			}
			text := callTool(
				t,
				cs,
				"get_governance_proposal",
				map[string]any{"status": " " + strings.ToUpper(tc.name) + " "},
				false,
			)
			require.Contains(t, text, hash)
			_, err := db.Exec("UPDATE governance_proposal SET deleted_slot=124")
			require.NoError(t, err)
			for status := range tc.included {
				text := callTool(
					t,
					cs,
					"get_governance_proposal",
					map[string]any{"status": status},
					false,
				)
				require.NotContains(
					t,
					text,
					hash,
					"deleted proposal in status %q",
					status,
				)
			}
		})
	}
}
