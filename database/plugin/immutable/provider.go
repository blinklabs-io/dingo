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

// Package immutable defines the storage.immutable plugin contract: the
// ImmutableDB service that holds promoted block bodies in cardano-node
// compatible chunk, primary and secondary files. Providers live in
// subpackages; the built-in one is database/plugin/immutable/chunkfile.
package immutable

import (
	"log/slog"

	"github.com/blinklabs-io/dingo/database/immutable"
)

// DirName is the directory under the data directory that holds the chunk
// files of the built-in provider.
const DirName = "immutable"

// ProviderDependencies are shared application settings injected into any
// storage.immutable provider.
type ProviderDependencies struct {
	DataDir string
	Logger  *slog.Logger
}

// Store is the single writer of one ImmutableDB. It appends blocks, reads
// them back and rewinds the tip.
type Store interface {
	// Append writes b after the current tip and returns where its wrapped
	// bytes landed.
	Append(b immutable.WriteBlock) (immutable.Location, error)
	// Sync makes every appended byte durable.
	Sync() error
	// Tip returns the last block written, and false for an empty store.
	Tip() (immutable.StoreTip, bool)
	// ReadLocation reads the block at loc and returns its era type and inner
	// CBOR.
	ReadLocation(loc immutable.Location) (uint, []byte, error)
	// GetBlock returns the block at the point, or nil, nil when absent.
	GetBlock(slot uint64, hash []byte) (*immutable.Block, *immutable.Location, error)
	// TruncateAfter removes every block after the one at the point.
	TruncateAfter(slot uint64, hash []byte) error
	// TruncateAll removes every block.
	TruncateAll() error
	// Close releases the store's files. It is safe to call more than once.
	Close() error
}

// ImmutableDB is the service a storage.immutable provider returns.
type ImmutableDB interface {
	// Dir is the directory holding the chunk files. Chunk adoption, snapshot
	// capture and restore place and read files there, next to the adoption
	// marker.
	Dir() string
	// Open opens the writer under geom, recovering a torn tail left by an
	// interrupted append. At most one Store may be open at a time.
	Open(geom immutable.Geometry) (Store, error)
	// ReadBlockAt reads the block at loc without opening the writer, which is
	// all a reader holding a marker needs.
	ReadBlockAt(loc immutable.Location) (uint, []byte, error)
	// ListFiles returns the chunk, primary and secondary files in Dir, in
	// chunk order.
	ListFiles() ([]immutable.File, error)
}
