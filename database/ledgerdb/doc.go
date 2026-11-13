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

// Package ledgerdb defines the LedgerDB: the ledger state consensus reads and
// writes, independent of how it is stored.
//
// The interfaces are grouped by the kind of state they hold (UTxO, accounts,
// rewards, pools, stake snapshots, governance, epochs, chain state), so each
// group can move to its own storage without touching the others. LedgerDB
// composes them with transactions.
//
// Package dbadapter implements LedgerDB over the metadata and blob stores of
// *database.Database. Queries that serve the APIs rather than validation are
// not part of the LedgerDB; see package indexerdb.
package ledgerdb
