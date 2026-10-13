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

package ledger

import (
	"context"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

type dijkstraDirectDepositSource interface {
	DijkstraDirectDeposits() dijkstra.DijkstraDirectDeposits
}

// DijkstraDirectDepositEffect identifies one reward-account credit declared
// by a Dijkstra transaction body.
type DijkstraDirectDepositEffect struct {
	Credential common.Credential
	Amount     uint64
}

// DijkstraDirectDepositEffects decodes the reward-account keys for one body.
func DijkstraDirectDepositEffects(
	tx common.Transaction,
) ([]DijkstraDirectDepositEffect, error) {
	if tx == nil || !tx.IsValid() {
		return nil, nil
	}
	var deposits dijkstra.DijkstraDirectDeposits
	switch source := tx.(type) {
	case dijkstraDirectDepositSource:
		deposits = source.DijkstraDirectDeposits()
	case *dijkstra.DijkstraTransaction:
		if source != nil {
			deposits = source.Body.TxDirectDeposits
		}
	default:
		return nil, nil
	}
	effects := make([]DijkstraDirectDepositEffect, 0, len(deposits))
	for rewardAddress, amount := range deposits {
		address, err := common.NewAddressFromBytes(rewardAddress.Bytes())
		if err != nil {
			return nil, fmt.Errorf("decode direct-deposit reward account: %w", err)
		}
		credential, err := (&address).RewardAccountCredential()
		if err != nil {
			return nil, fmt.Errorf("decode direct-deposit credential: %w", err)
		}
		effects = append(effects, DijkstraDirectDepositEffect{
			Credential: credential,
			Amount:     amount,
		})
	}
	return effects, nil
}

// ApplyDijkstraDirectDeposits credits the registered reward accounts declared
// by one valid Dijkstra transaction body. The transaction body's hash is the
// journal source so rollback removes the credit with that body.
func ApplyDijkstraDirectDeposits(
	ctx context.Context,
	db *database.Database,
	tx common.Transaction,
	slot uint64,
	txn *database.Txn,
) error {
	effects, err := DijkstraDirectDepositEffects(tx)
	if err != nil {
		return err
	}
	for _, effect := range effects {
		credentialTag, err := models.CredentialTagFromUint(effect.Credential.CredType)
		if err != nil {
			return err
		}
		if err := db.AddAccountRewardByCredential(
			ctx,
			credentialTag,
			effect.Credential.Credential[:],
			effect.Amount,
			slot,
			tx.Hash().Bytes(),
			txn,
		); err != nil {
			return fmt.Errorf("credit direct-deposit account: %w", err)
		}
	}
	return nil
}
