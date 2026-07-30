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

//nolint:gosec // SQL INTEGER mappings preserve the existing unsigned domain API.
package sqlstore

import (
	"context"
	"database/sql"
	"errors"

	"github.com/blinklabs-io/dingo/database/models"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

func (s *Store) GetAssetByPolicyAndName(
	policyID lcommon.Blake2b224,
	assetName []byte,
	txn types.Txn,
) (models.Asset, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return models.Asset{}, err
	}
	q, err := s.sqliteQueries(db)
	if err != nil {
		return models.Asset{}, err
	}
	row, err := q.GetAssetByPolicyAndName(
		context.Background(),
		sqlitequery.GetAssetByPolicyAndNameParams{
			PolicyID: policyID[:],
			Name:     assetName,
		},
	)
	if errors.Is(err, sql.ErrNoRows) {
		return models.Asset{}, nil
	}
	if err != nil {
		return models.Asset{}, err
	}
	amount := uint64(0)
	if row.Amount.Valid {
		amount, err = parseUint64("asset amount", row.Amount.String)
		if err != nil {
			return models.Asset{}, err
		}
	}
	return models.Asset{
		Name:        row.Name,
		NameHex:     row.NameHex,
		PolicyId:    row.PolicyID,
		Fingerprint: row.Fingerprint,
		ID:          uint(row.ID),
		UtxoID:      uint(row.UtxoID.Int64),
		Amount:      types.Uint64(amount),
	}, nil
}

func (s *Store) GetAssetQuantityByPolicyAndName(
	policyID lcommon.Blake2b224,
	assetName []byte,
	txn types.Txn,
) (uint64, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	q, err := s.sqliteQueries(db)
	if err != nil {
		return 0, err
	}
	value, err := q.GetAssetQuantityByPolicyAndName(
		context.Background(),
		sqlitequery.GetAssetQuantityByPolicyAndNameParams{
			PolicyID: policyID[:],
			Name:     assetName,
		},
	)
	return uint64(value), err
}

func (s *Store) GetAssetMintBurnInfo(
	policyID lcommon.Blake2b224,
	assetName []byte,
	txn types.Txn,
) ([]byte, int, error) {
	db, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, 0, err
	}
	q, err := s.sqliteQueries(db)
	if err != nil {
		return nil, 0, err
	}
	row, err := q.GetAssetMintBurnInfo(
		context.Background(),
		sqlitequery.GetAssetMintBurnInfoParams{
			PolicyID:   policyID[:],
			Name:       assetName,
			PolicyID_2: policyID[:],
			Name_2:     assetName,
		},
	)
	if err != nil {
		return nil, 0, err
	}
	return row.InitialTxHash, int(row.EventCount), nil
}
