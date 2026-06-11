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

package mysql

import (
	"context"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/offchain"
	"github.com/blinklabs-io/dingo/database/types"
)

func (d *MetadataStoreMysql) EnsureOffchainMetadataPointers(
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

func (d *MetadataStoreMysql) GetOffchainMetadataFetchBatch(
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

func (d *MetadataStoreMysql) SetOffchainMetadataFetchResult(
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

func (d *MetadataStoreMysql) GetOffchainMetadata(
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
