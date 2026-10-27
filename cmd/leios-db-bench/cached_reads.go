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

package main

import (
	"slices"
	"sync"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/txresponse"
	"github.com/blinklabs-io/gouroboros/cbor"
)

type ebReadCache struct {
	manifest func() ([]byte, error)
	txs      func() ([]cbor.RawMessage, error)
}

type ebReader interface {
	GetLeiosEBManifest([]byte, uint64) ([]byte, error)
	GetLeiosEBTxs([]byte, uint64) ([]cbor.RawMessage, error)
}

func newEBReadCache(db ebReader, hash []byte, slot uint64) *ebReadCache {
	// Each cache belongs to one committed occurrence, not just its hash.
	hash = slices.Clone(hash)
	return &ebReadCache{
		manifest: sync.OnceValues(func() ([]byte, error) {
			return db.GetLeiosEBManifest(hash, slot)
		}),
		txs: sync.OnceValues(func() ([]cbor.RawMessage, error) {
			return db.GetLeiosEBTxs(hash, slot)
		}),
	}
}

func readEBTxs(
	db *database.Database,
	eb *benchEb,
	slot uint64,
	rng txRange,
) ([]cbor.RawMessage, error) {
	var txs []cbor.RawMessage
	var err error
	if eb.readCache == nil {
		txs, err = db.GetLeiosEBTxs(eb.hash, slot)
	} else {
		txs, err = eb.readCache.txs()
	}
	if rng.end == -1 {
		rng.end = len(txs)
	}
	if err != nil || rng.start < 0 || rng.end < rng.start ||
		len(txs) < rng.end {
		return nil, err
	}
	txs = txs[rng.start:rng.end]
	if eb.readCache != nil {
		// Serving must not hand out mutable aliases into the shared closure.
		return txresponse.Clone(txs), nil
	}
	return txs, nil
}
