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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
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

// OpenReadOnlySQLite opens a read-only SQLite connection pool with busy timeout
// and query_only pragma, using ctx for the initial connection.
func OpenReadOnlySQLite(
	ctx context.Context,
	databasePath string,
) (*sql.DB, error) {
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

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open read-only SQLite: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	return db, nil
}

// NewMCPServer constructs and initializes an MCP Server with all Cardano and
// SQLite tools and resources, using ctx for startup work.
func NewMCPServer(
	ctx context.Context,
	cfg ProviderConfig,
	deps ProviderDependencies,
) (*mcp.Server, *sql.DB, error) {
	if cfg.RateLimit < 0 || math.IsNaN(cfg.RateLimit) ||
		math.IsInf(cfg.RateLimit, 0) {
		return nil, nil, errors.New(
			"MCP rateLimit must be finite and nonnegative",
		)
	}
	var db *sql.DB
	var openedDB *sql.DB
	var sqlitePath string
	if deps.Database != nil {
		if provider, ok := deps.Database.Metadata().(interface{ SQLitePath() string }); ok {
			sqlitePath = provider.SQLitePath()
		}
	}

	if deps.SQLDB != nil {
		db = deps.SQLDB
	} else if deps.Database != nil {
		if sqlitePath != "" {
			readDB, err := OpenReadOnlySQLite(ctx, sqlitePath)
			if err != nil {
				return nil, nil, err
			}
			db, openedDB = readDB, readDB
		}
	}

	instructions := "Dingo Cardano MCP Server.\n" +
		"Dingo is a pure Go, modular Cardano node implementation developed by Blink Labs.\n" +
		"Key concepts & guidance for agents:\n" +
		"- Node & Block Identification: Dingo identifies blocks it forges on-chain using identifier 69 (BlockHeaderProtocolMinor = 69 by default), optionally with a CIP-0203 release-version payload. This fingerprints Dingo-forged blocks without altering the ledger protocol major version. In network handshakes, it identifies as Dingo with its version string.\n" +
		"- Block Production: When running with --block-producer, Dingo keeps a durable forge fence as block-store chain state ('forge_fence:<pool_id>'), which no SQL table holds, to prevent double-signing, and keeps operational certificate counters in the LedgerDB.\n" +
		"- Storage Modes: 'core' (block store, ImmutableDB and LedgerDB; no SQL store) and 'api' (core plus an IndexerDB on SQLite, PostgreSQL or MySQL, which the SQL tools query).\n" +
		"- Documentation Resources: Inspect 'dingo://docs/identity' for details on node and block identification, 'dingo://docs/faq' for frequently asked questions, 'dingo://docs/architecture' for node internals, and 'dingo://dbsync/cheatsheet' for schema mappings.\n" +
		"- Active Tools (21 total): Use 'get_node_info' for architecture/FAQ/identity queries, 'get_cardano_tip' for sync status, 'get_protocol_parameters' for fee/ExUnit pricing, 'get_mempool_info' for in-flight pending txs, 'decode_address' for address decomposition, 'get_governance_proposal' for CIP-1694 vote tallies, 'calculate_min_utxo' for CIP-55 deposit sizing, 'get_pool_performance' for SPO metrics, 'get_governance_state' for Voltaire governance, and 'sqlite_query' for arbitrary read-only queries of the IndexerDB.\n" +
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

	// The address index is the IndexerDB's: a ledger's database has no
	// metadata store to page from, and get_utxos then reads the live UTxO set.
	var reads indexerReads
	if deps.Database != nil && deps.Database.Metadata() != nil {
		reads.addressLookup = deps.Database.UtxosByAddressPage
		reads.epochBlocks = indexerEpochBlocks(deps.Database)
	}

	// Register tools, resources & prompts
	limits := newReadLimits()
	if deps.Database != nil {
		limits.payloads = deps.Database
	}
	registerSQLiteTools(server, db, cfg.QueryTimeout, cfg.MaxRows, limits)
	registerCardanoTools(
		server,
		db,
		ledgerStateOf(deps.LedgerState),
		deps.Mempool,
		deps.Network,
		cfg.QueryTimeout,
		limits,
		reads,
	)
	resourceLogger := deps.Logger
	if resourceLogger == nil {
		resourceLogger = slog.Default()
	}
	registerResources(
		ctx,
		server,
		db,
		deps.LedgerState,
		deps.Network,
		cfg.QueryTimeout,
		tableEnumerationTimeout,
		resourceLogger,
	)
	RegisterPrompts(server, db, deps.LedgerState, deps.Network)

	return server, openedDB, nil
}

// indexerEpochBlocks counts blocks from the IndexerDB's block nonces, which it
// keeps for every block it indexes, or is nil when its store has no slot-range
// statistics.
func indexerEpochBlocks(db *database.Database) epochBlockCounter {
	slotRanges, ok := db.Metadata().(metadata.SlotRangeStore)
	if !ok {
		return nil
	}
	return func(ctx context.Context, start, end uint64) (int, error) {
		txn := db.Transaction(ctx, false)
		defer txn.Release()
		stats, err := slotRanges.GetBlockSlotRangeStats(start, end, txn.Metadata())
		if err != nil {
			return 0, err
		}
		return stats.Count, nil
	}
}
