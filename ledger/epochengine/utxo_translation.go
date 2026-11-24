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

package epochengine

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// LiveUtxoLovelace is the lovelace in every live output the host's UTxO set
// holds as txn sees it.
func (e *Engine) LiveUtxoLovelace(txn ledgerdb.Txn) (uint64, error) {
	var total uint64
	err := e.host.IterateLiveUtxos(txn, func(u *models.Utxo) error {
		out, err := u.Decode()
		if err != nil {
			return fmt.Errorf("decode utxo %x#%d: %w", u.TxId, u.OutputIdx, err)
		}
		amount, err := models.CheckedUint64FromBigInt(out.Amount())
		if err != nil {
			return err
		}
		if total > math.MaxUint64-amount {
			return errors.New("live UTxO lovelace overflows uint64")
		}
		total += amount
		return nil
	})
	return total, err
}

// ApplyByronTranslationReserves resets the reserves at the Byron-to-Shelley
// boundary to the maximum supply less the live UTxO balance, as the hard-fork
// combinator's translation of the Byron ledger does before the first Shelley
// tick. The treasury is kept.
func (e *Engine) ApplyByronTranslationReserves(
	txn ledgerdb.Txn,
	newEpoch uint64,
	boundarySlot uint64,
) error {
	if e.config.CardanoNodeConfig == nil {
		return errors.New("the Byron translation needs the node config")
	}
	shelleyGenesis := e.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil {
		return errors.New("missing Shelley genesis")
	}
	live, err := e.LiveUtxoLovelace(txn)
	if err != nil {
		return fmt.Errorf("sum live UTxO balance: %w", err)
	}
	if live == 0 {
		return fmt.Errorf(
			"no live UTxO at the Byron-to-Shelley boundary "+
				"(epoch %d, slot %d): UTxO set is not populated",
			newEpoch,
			boundarySlot,
		)
	}
	if live > shelleyGenesis.MaxLovelaceSupply {
		return fmt.Errorf(
			"live UTxO balance %d exceeds max lovelace supply %d",
			live,
			shelleyGenesis.MaxLovelaceSupply,
		)
	}
	reserves := shelleyGenesis.MaxLovelaceSupply - live
	ldb := e.ledgerDB()
	state, err := ldb.GetNetworkState(txn)
	if err != nil {
		return fmt.Errorf("get network state: %w", err)
	}
	var treasury uint64
	if state != nil {
		treasury = uint64(state.Treasury)
	}
	if err := ldb.SetNetworkState(
		treasury, reserves, boundarySlot, txn,
	); err != nil {
		return fmt.Errorf("set translated reserves: %w", err)
	}
	e.config.Logger.Info(
		"set Shelley reserves from Byron UTxO balance",
		"reserves", reserves,
		"live_utxo_lovelace", live,
		"epoch", newEpoch,
		"boundary_slot", boundarySlot,
		"component", "ledger",
	)
	return nil
}

// ApplyAllegraAvvmReturn is the Allegra HARDFORK rule (protocol major 3): every
// live output at a Byron redeem (AVVM) address leaves the UTxO set and its
// lovelace returns to the reserves, as cardano-ledger's
// Allegra/Translation.hs returnRedeemAddrsToReserves does. The set is scanned
// rather than a stash read, which gives the same answer because a spent
// output is absent from both.
func (e *Engine) ApplyAllegraAvvmReturn(
	ctx context.Context,
	txn ledgerdb.Txn,
	newEpoch uint64,
	boundarySlot uint64,
) error {
	ldb := e.ledgerDB()
	state, err := ldb.GetNetworkState(txn)
	if err != nil {
		return fmt.Errorf("get network state before AVVM return: %w", err)
	}
	var treasury, reserves uint64
	if state != nil {
		treasury = uint64(state.Treasury)
		reserves = uint64(state.Reserves)
	}
	removed, total, err := e.avvmUtxos(txn)
	if err != nil {
		return fmt.Errorf(
			"pv3 remove AVVM UTxOs at slot %d: %w", boundarySlot, err,
		)
	}
	if len(removed) > 0 {
		e.host.PublishAvvmRemoval(ctx, txn, boundarySlot, removed)
		if total > math.MaxUint64-reserves {
			return fmt.Errorf(
				"AVVM reserve credit overflows uint64: reserves %d, credit %d",
				reserves, total,
			)
		}
		if err := ldb.SetNetworkState(
			treasury, reserves+total, boundarySlot, txn,
		); err != nil {
			return fmt.Errorf("credit AVVM value to reserves: %w", err)
		}
	}
	e.config.Logger.Info(
		"applied Allegra HARDFORK rule (pv3 AVVM return)",
		"removed_avvm_utxos", len(removed),
		"reclaimed_lovelace", total,
		"epoch", newEpoch,
		"boundary_slot", boundarySlot,
		"component", "ledger",
	)
	return nil
}

func (e *Engine) avvmUtxos(
	txn ledgerdb.Txn,
) ([]models.UtxoId, uint64, error) {
	var (
		removed []models.UtxoId
		total   uint64
	)
	err := e.host.IterateLiveUtxos(txn, func(u *models.Utxo) error {
		out, err := u.Decode()
		if err != nil {
			return fmt.Errorf("decode utxo %x#%d: %w", u.TxId[:8], u.OutputIdx, err)
		}
		addr := out.Address()
		if addr.Type() != lcommon.AddressTypeByron ||
			addr.ByronType() != lcommon.ByronAddressTypeRedeem {
			return nil
		}
		amount := out.Amount()
		if amount == nil || amount.Sign() < 0 || !amount.IsUint64() {
			return fmt.Errorf(
				"AVVM UTxO %x#%d has invalid amount %v",
				u.TxId[:8], u.OutputIdx, amount,
			)
		}
		value := amount.Uint64()
		if total > math.MaxUint64-value {
			return fmt.Errorf(
				"AVVM lovelace total overflows uint64 at tx %x#%d",
				u.TxId[:8], u.OutputIdx,
			)
		}
		total += value
		// The row's buffers are reused between calls.
		removed = append(removed, models.UtxoId{
			Hash: append([]byte(nil), u.TxId...), Idx: u.OutputIdx,
		})
		return nil
	})
	if err != nil {
		return nil, 0, fmt.Errorf("scan live utxos: %w", err)
	}
	return removed, total, nil
}
