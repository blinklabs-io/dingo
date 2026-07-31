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

// Package recovery provides crash recovery and state consistency for the
// node's two-store database.
//
// # What this is not
//
// This is not a redo log for block or ledger data. Both underlying stores are
// already crash safe on their own: the blob store (Badger) has a value log and
// the metadata store (SQLite/MySQL/Postgres) has its own journal. Duplicating
// their durability here would be pure overhead.
//
// # What this is
//
// A logical commit in dingo spans two independent stores, and no two-phase
// commit exists between them. Txn.Commit deliberately commits blob first, syncs
// it, then commits metadata, so the blob store can only ever be ahead of the
// metadata tip. A crash inside that window leaves the two stores at different
// commit timestamps, and the store contents alone cannot say which write was in
// flight or what it was trying to do.
//
// This package journals that missing information:
//
//   - WAL is an append-only intent journal. Before a combined commit touches
//     either store it records what the commit intends to do (a block append at
//     a point, a rollback to a point) and the commit timestamp fencing it, then
//     records a commit marker once both stores are through. A begin record with
//     no commit marker is exactly the in-flight window a crash can interrupt.
//
//   - CheckpointStore records periodic, merkle-rooted summaries of agreed store
//     state. A checkpoint bounds how far back recovery ever has to look and
//     gives recovery a verified anchor when the journal itself is unreadable.
//     WAL segments fully covered by a durable checkpoint are removed.
//
//   - Checker runs the startup consistency checks: tip agreement across the
//     stores, block continuity beneath the tip, UTxO integrity, and orphaned
//     data beyond the tip.
//
//   - Manager ties those together at startup: load the newest valid checkpoint,
//     replay the journal above it, compare the intent record against what the
//     stores actually hold, and drive repair through the Repairer the node
//     supplies.
//
// # Import direction
//
// This package sits below the database package that wires it in, so it never
// imports database, ledger, chain, or node code. Everything it needs from those
// layers arrives through the StateSource and Repairer interfaces declared here.
package recovery
