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
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	_ "modernc.org/sqlite"
)

// sqliteFileURI formats a filesystem path into a valid file: URI for SQLite DSN.
func sqliteFileURI(databasePath string) string {
	if !filepath.IsAbs(databasePath) {
		if absolutePath, err := filepath.Abs(databasePath); err == nil {
			databasePath = absolutePath
		}
	}
	path := filepath.ToSlash(databasePath)
	if filepath.VolumeName(databasePath) != "" &&
		!strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return (&url.URL{Scheme: "file", Path: path}).String()
}

// OpenReadOnlySQLite opens a read-only SQLite connection pool with busy timeout and query_only pragma.
func OpenReadOnlySQLite(databasePath string) (*sql.DB, error) {
	if _, err := os.Stat(databasePath); err != nil {
		return nil, fmt.Errorf(
			"database file does not exist at '%s': %w",
			databasePath,
			err,
		)
	}

	uri := sqliteFileURI(databasePath)
	dsn := uri + "?mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open read-only SQLite: %w", err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	return db, nil
}

// NewMCPServer constructs and initializes an MCP Server with all Cardano & SQLite tools and resources.
func NewMCPServer(
	cfg ProviderConfig,
	deps ProviderDependencies,
) (*mcp.Server, *sql.DB, error) {
	var db *sql.DB
	var openedDB *sql.DB

	if deps.SQLDB != nil {
		db = deps.SQLDB
	} else if deps.DataDir != "" {
		dbFile := filepath.Join(deps.DataDir, "metadata.sqlite")
		if _, err := os.Stat(dbFile); err == nil {
			readDB, err := OpenReadOnlySQLite(dbFile)
			if err != nil {
				if deps.Logger != nil {
					deps.Logger.Warn("could not open read-only SQLite metadata store", "path", dbFile, "error", err)
				}
			} else {
				db = readDB
				openedDB = readDB
			}
		}
	}

	instructions := "Dingo Cardano MCP Server.\n" +
		"Dingo is a pure Go, modular Cardano node implementation developed by Blink Labs.\n" +
		"Key concepts & guidance for agents:\n" +
		"- Node & Block Identification: Dingo identifies blocks it forges on-chain by setting Block Header Protocol Minor Version to 69 (BlockHeaderProtocolMinor = 69). This fingerprints Dingo-forged blocks without altering the ledger protocol major version. In network handshakes, it identifies as Dingo with its version string.\n" +
		"- Block Production: When running with --block-producer, Dingo maintains a durable forge fence in SQLite ('forge_fence:<pool_id>' in table sync_state) to prevent double-signing, and tracks minting in 'pool_opcert_sequence'.\n" +
		"- Storage Modes: 'core' (Badger block blobs + SQLite metadata) and 'api' (PostgreSQL / indexed store).\n" +
		"- Documentation Resources: Inspect 'dingo://docs/identity' for details on node and block identification, 'dingo://docs/faq' for frequently asked questions, 'dingo://docs/architecture' for node internals, and 'dingo://dbsync/cheatsheet' for schema mappings.\n" +
		"- Active Tools (21 total): Use 'get_node_info' for architecture/FAQ/identity queries, 'get_cardano_tip' for sync status, 'get_protocol_parameters' for fee/ExUnit pricing, 'get_mempool_info' for in-flight pending txs, 'decode_address' for address decomposition, 'get_governance_proposal' for CIP-1694 vote tallies, 'calculate_min_utxo' for CIP-55 deposit sizing, 'get_pool_performance' for SPO metrics, 'get_governance_state' for Voltaire governance, and 'sqlite_query' for arbitrary read-only queries.\n" +
		"- Official Agent Prompts: Pre-engineered workflows available via prompts/list and prompts/get: 'diagnose_node_health' (node sync and health), 'simulate_and_diagnose_tx' (Plutus transaction dry-run & redeemer ExUnits), 'audit_pool_rewards' (SPO performance audit), 'track_asset_portfolio' (Kupo-style asset intelligence), 'conway_governance_brief' (CIP-1694 governance brief), and 'investigate_address' (EUTxO state analysis)."

	opts := &mcp.ServerOptions{
		Instructions: instructions,
		Logger:       deps.Logger,
		Capabilities: &mcp.ServerCapabilities{
			Prompts: &mcp.PromptCapabilities{
				ListChanged: true,
			},
			Resources: &mcp.ResourceCapabilities{
				ListChanged: true,
			},
			Tools: &mcp.ToolCapabilities{
				ListChanged: true,
			},
		},
	}

	server := mcp.NewServer(&mcp.Implementation{
		Name:    "dingo-mcp",
		Version: "0.1.0",
	}, opts)

	// Register tools, resources & prompts
	RegisterSQLiteTools(server, db, cfg.QueryTimeout, cfg.MaxRows)
	RegisterCardanoTools(
		server,
		db,
		deps.LedgerState,
		deps.Mempool,
		deps.Network,
		cfg.QueryTimeout,
	)
	RegisterResources(server, db, deps.LedgerState, deps.Network)
	RegisterPrompts(server, db, deps.LedgerState, deps.Network)

	return server, openedDB, nil
}
