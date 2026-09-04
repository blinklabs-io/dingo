// Copyright 2025 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	ledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// resolvePointerStakeCredential fills in a UTxO's stake credential when its
// address is a pointer address (types 4 and 5).
//
// A pointer address carries no credential of its own -- it carries the position
// of the stake registration certificate that registered one, as
// (slot, tx index within the block, cert index within the transaction).
// gouroboros therefore reports an empty StakeKeyHash for it, correctly, since
// resolving the pointer is the ledger's job and not the address's.
//
// Dingo was not doing that job, so the output was stored with a NULL
// staking_key and its value never reached the stake distribution. That
// understates the delegated stake of any account holding funds at a pointer
// address, which tightens that pool's Praos leader threshold and makes the node
// reject blocks the network accepted (issues #3854 and #3811).
//
// The lookup is safe to do here: a pointer can only name a certificate that is
// already on the chain, so the registration is committed before any output
// referencing it is applied.
//
// A pointer that resolves to nothing is left unattributed rather than being
// treated as an error. The ledger has the same outcome -- stake that points at
// no registration is simply not counted -- and a dangling pointer must not stop
// a block from being applied.
func (s *Store) resolvePointerStakeCredential(
	ctx context.Context,
	db queryer,
	addr ledger.Address,
	model *models.Utxo,
) error {
	if model == nil || len(model.StakingKey) > 0 {
		return nil
	}
	// Enterprise addresses and Byron addresses also carry no staking payload
	// and are far more common, so the type assertion runs before any query.
	ptr, ok := addr.StakingPayload().(lcommon.AddressPayloadPointer)
	if !ok {
		return nil
	}
	slot, err := checkedInt64(ptr.Slot)
	if err != nil {
		return nil //nolint:nilerr // an unrepresentable pointer names no cert
	}
	txIndex, err := checkedInt64(ptr.TxIndex)
	if err != nil {
		return nil //nolint:nilerr // as above
	}
	certIndex, err := checkedInt64(ptr.CertIndex)
	if err != nil {
		return nil //nolint:nilerr // as above
	}
	var (
		stakingKey    []byte
		credentialTag uint8
	)
	err = db.QueryRowContext(ctx, `
SELECT sr.staking_key, sr.credential_tag
FROM certs c
JOIN "transaction" t ON t.id = c.transaction_id
JOIN stake_registration sr ON sr.certificate_id = c.id
WHERE c.slot = ? AND t.block_index = ? AND c.cert_index = ?
LIMIT 1`,
		slot, txIndex, certIndex,
	).Scan(&stakingKey, &credentialTag)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf(
			"resolve pointer (%d,%d,%d): %w",
			ptr.Slot, ptr.TxIndex, ptr.CertIndex, err,
		)
	}
	if len(stakingKey) == 0 {
		return nil
	}
	model.StakingKey = stakingKey
	model.CredentialTag = credentialTag
	return nil
}
