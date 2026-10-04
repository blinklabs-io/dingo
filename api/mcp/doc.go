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

// Package mcp implements a Model Context Protocol (MCP) server for Dingo.
//
// It enables Large Language Models (LLMs) and AI agents (e.g. Claude Desktop,
// Cursor, Cline) to query Dingo's SQLite metadata store and inspect live Cardano
// blockchain state directly.
//
// The server exposes:
//   - Read-only SQLite query tools (sqlite_query, sqlite_table_schema, sqlite_explain)
//   - Granular Cardano domain tools (get_cardano_tip, get_block, get_transaction,
//     get_utxos, get_account, get_epoch_summary, get_pool_performance, get_asset_info,
//     get_utxos_by_asset, get_governance_state, resolve_datum_or_script, evaluate_tx,
//     get_node_info, get_protocol_parameters, get_mempool_info, decode_address,
//     get_governance_proposal, calculate_min_utxo)
//   - Passive context resources (dingo://node/status, dingo://schema/tables,
//     dingo://schema/table/<name>, dingo://dbsync/cheatsheet, dingo://docs/...)
//   - Official agent workflows via MCP prompts (diagnose_node_health, simulate_and_diagnose_tx,
//     audit_pool_rewards, track_asset_portfolio, conway_governance_brief, investigate_address)
//   - Transport support for Streamable HTTP (/mcp) and Server-Sent Events (/sse)
//   - Security middleware with token-bucket rate limiting, optional Bearer auth,
//     and input/output sanitization against prompt injection attacks.
package mcp
