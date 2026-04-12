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

// Package database is Dingo's storage abstraction. It provides a single
// Database type backed by two pluggable layers:
//
//   - a Blob store for full block and transaction CBOR — plugins in
//     database/plugin/blob/ (badger [default], gcs, s3)
//   - a Metadata store for indexed queries over UTxOs, certs, pools,
//     stake snapshots, and governance state — plugins in
//     database/plugin/metadata/ (sqlite [default], postgres, mysql)
//
// Plugins register themselves with database/plugin on import. The
// concrete plugin used at runtime is selected by configuration
// (DINGO_DATABASE_BLOB_PLUGIN, DINGO_DATABASE_METADATA_PLUGIN).
//
// # CBOR extraction
//
// UTxOs and transactions are not stored as full CBOR in the metadata
// layer. Instead, each reference is a fixed 52-byte CborOffset (magic
// "DOFF" + slot + hash + offset + length) that points into the block
// stored in the blob layer. The TieredCborCache resolves CBOR on
// demand through three tiers: hot entry cache → block LRU → cold
// extraction from the blob store.
//
// See database/cbor_offset.go for the offset encoding,
// database/cbor_cache.go for the cache, and database/block_indexer.go
// for how per-block offset tables are built.
//
// # Writing queries
//
// Avoid N+1 query patterns. When loading state for many entities at
// once, prefer a single `WHERE id IN ?` batch query over a loop of
// per-entity reads. When certificates can share a slot, always order
// by (added_slot, cert_index) from the unified `certs` table as a
// tie-breaker — cert_index alone is meaningless across slots.
package database
