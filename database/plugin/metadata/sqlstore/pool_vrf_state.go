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

//nolint:gosec,rowserrcheck,sqlclosecheck // SQL INTEGER mappings preserve the unsigned domain API; cursors are explicitly closed before dependent queries.
package sqlstore

import (
	"database/sql"
	"errors"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// GetPoolVrfState projects only PoolKeyHash, VrfKeyHash and zero or one latest
// Registration.VrfKeyHash. It includes inactive pools and preserves the exact
// registration order used by GetPool, without loading owners or relays.
func (s *Store) GetPoolVrfState(
	pkh lcommon.PoolKeyHash,
	txn types.Txn,
) (*models.Pool, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	query := `-- name: GetPoolVrfState :one
SELECT p.pool_key_hash, p.vrf_key_hash, registration.id,
       registration.vrf_key_hash
FROM pool p
LEFT JOIN pool_registration registration ON registration.id = (
    SELECT r.id
    FROM pool_registration r
    LEFT JOIN certs c ON c.id = r.certificate_id
    LEFT JOIN ` + s.dialect.QuoteIdentifier("transaction") + ` tx ON tx.id = c.transaction_id
    WHERE r.pool_id = p.id
    ORDER BY r.added_slot DESC, COALESCE(tx.block_index, 0) DESC,
             COALESCE(c.cert_index, 0) DESC, r.registration_index DESC,
             r.id DESC
    LIMIT 1
)
WHERE p.pool_key_hash = ?`
	var pool models.Pool
	var registrationID sql.NullInt64
	var registrationHash []byte
	err = db.QueryRowContext(ctx, s.dialect.Rebind(query), pkh.Bytes()).Scan(
		&pool.PoolKeyHash, &pool.VrfKeyHash, &registrationID, &registrationHash,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if registrationID.Valid {
		pool.Registration = []models.PoolRegistration{
			{VrfKeyHash: registrationHash},
		}
	}
	return &pool, nil
}
