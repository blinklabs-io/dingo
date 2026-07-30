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

package sqlite

import (
	"context"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/offchain"
	"github.com/blinklabs-io/dingo/database/types"
)

func (d *MetadataStoreSqlite) EnsureOffchainMetadataPointers(
	ctx context.Context,
	now time.Time,
	txn types.Txn,
) (int, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return 0, err
	}
	return offchain.EnsurePointers(ctx, db, now)
}

func (d *MetadataStoreSqlite) GetOffchainMetadataFetchBatch(
	ctx context.Context,
	limit int,
	now time.Time,
	txn types.Txn,
) ([]models.OffchainMetadata, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	return offchain.FetchBatch(ctx, db, limit, now)
}

func (d *MetadataStoreSqlite) SetOffchainMetadataFetchResult(
	ctx context.Context,
	doc *models.OffchainMetadata,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return offchain.SetFetchResult(ctx, db, doc)
}

func (d *MetadataStoreSqlite) GetOffchainMetadata(
	sourceType string,
	url string,
	hash []byte,
	txn types.Txn,
) (*models.OffchainMetadata, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	return offchain.Get(db, sourceType, url, hash)
}

// GetOffchainMetadataBatch retrieves cached off-chain documents for many
// URLs in one or more queries, chunked at sqliteBindVarLimit to stay under
// sqlite's bound-parameter limit for the url IN (...) clause.
func (d *MetadataStoreSqlite) GetOffchainMetadataBatch(
	sourceType string,
	urls []string,
	txn types.Txn,
) ([]models.OffchainMetadata, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	// Deduplicate across the whole list before chunking, not per chunk:
	// GetBatch removes duplicates within whatever slice it is handed, so a
	// URL appearing in two different chunks would otherwise be queried twice
	// and its row appended twice, breaking the batch's one-row-per-match
	// contract. Two pools can share a metadata anchor, so repeats are
	// expected input here rather than pathological.
	uniq := offchain.DedupeURLs(urls)
	docs := make([]models.OffchainMetadata, 0, len(uniq))
	for start := 0; start < len(uniq); start += sqliteBindVarLimit {
		end := min(start+sqliteBindVarLimit, len(uniq))
		chunk, err := offchain.GetBatch(db, sourceType, uniq[start:end])
		if err != nil {
			return nil, err
		}
		docs = append(docs, chunk...)
	}
	return docs, nil
}
