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
	"encoding/hex"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/governance"
)

const (
	// mirPotReserves is the on-chain CBOR encoding of the reserves pot (0).
	mirPotReserves = uint(0)
	// mirPotTreasury is the on-chain CBOR encoding of the treasury pot (1).
	mirPotTreasury = uint(1)
)

// applyMIRCerts applies all MIR (Move Instantaneous Rewards) certificate
// effects accumulated during the ended epoch at the given boundary slot. This
// implements the Shelley-era INSTANT rule, which runs at each epoch boundary
// for Shelley through Babbage. In Conway and later, MIR certificates are not
// valid, so no records exist in the DB and this function returns immediately.
//
// epochStartSlot is the first slot of the ended epoch (inclusive lower bound);
// boundarySlot is the first slot of the new epoch (exclusive upper bound).
// MIR certs with added_slot in [epochStartSlot, boundarySlot) are applied.
//
// Distribution MIR (credential→amount map):
//   - Registered, active reward accounts are credited from the source pot.
//   - Credentials without a registered account are silently skipped — unlike
//     POOLREAP, there is no fallback routing to the treasury.
//   - The source pot is debited only for amounts actually credited.
//
// Pot-to-pot transfer MIR (OtherPot > 0):
//   - Source=0 (Reserves) moves OtherPot lovelace from reserves to treasury.
//   - Source=1 (Treasury) moves OtherPot lovelace from treasury to reserves.
func (ls *LedgerState) applyMIRCerts(
	txn *database.Txn,
	epochStartSlot uint64,
	boundarySlot uint64,
) error {
	effects, err := ls.db.GetMIRCertsInSlotRange(
		epochStartSlot, boundarySlot, txn,
	)
	if err != nil {
		return fmt.Errorf("get MIR certs: %w", err)
	}
	if len(effects) == 0 {
		return nil
	}
	for _, effect := range effects {
		if effect.OtherPot > 0 {
			if err := ls.applyMIRPotTransfer(
				txn, effect.Pot, effect.OtherPot, boundarySlot,
			); err != nil {
				return err
			}
			continue
		}
		var totalDebited uint64
		for _, reward := range effect.Rewards {
			credited, err := governance.CreditRegisteredRewardAccount(
				ls.db, txn, reward.Credential, reward.Amount, boundarySlot,
			)
			if err != nil {
				return fmt.Errorf(
					"apply MIR reward to %x: %w",
					reward.Credential, err,
				)
			}
			if credited {
				totalDebited += reward.Amount
				ls.config.Logger.Debug(
					"applied MIR reward",
					"credential", hex.EncodeToString(reward.Credential),
					"amount", reward.Amount,
					"pot", effect.Pot,
					"component", "ledger",
				)
			}
		}
		if totalDebited > 0 {
			if err := ls.debitMIRPot(
				txn, effect.Pot, totalDebited, boundarySlot,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

// debitMIRPot decrements the given Ada pot by amount, writing an updated
// NetworkState at boundarySlot. Shared by distribution MIR processing; the
// pot-to-pot path uses applyMIRPotTransfer which does a combined read-modify.
func (ls *LedgerState) debitMIRPot(
	txn *database.Txn,
	pot uint,
	amount uint64,
	slot uint64,
) error {
	treasury, reserves, err := ls.readNetworkState(txn)
	if err != nil {
		return fmt.Errorf("debit MIR pot: %w", err)
	}
	switch pot {
	case mirPotReserves:
		if amount > reserves {
			return fmt.Errorf(
				"MIR would underflow reserves: pot has %d, distributing %d",
				reserves, amount,
			)
		}
		reserves -= amount
	case mirPotTreasury:
		if amount > treasury {
			return fmt.Errorf(
				"MIR would underflow treasury: pot has %d, distributing %d",
				treasury, amount,
			)
		}
		treasury -= amount
	default:
		return fmt.Errorf("unknown MIR pot %d", pot)
	}
	return ls.db.Metadata().SetNetworkState(treasury, reserves, slot, txn.Metadata())
}

// applyMIRPotTransfer moves amount lovelace between the two Ada pots at slot.
// Source=0 (Reserves) transfers from reserves to treasury.
// Source=1 (Treasury) transfers from treasury to reserves.
func (ls *LedgerState) applyMIRPotTransfer(
	txn *database.Txn,
	sourcePot uint,
	amount uint64,
	slot uint64,
) error {
	treasury, reserves, err := ls.readNetworkState(txn)
	if err != nil {
		return fmt.Errorf("apply MIR pot transfer: %w", err)
	}
	switch sourcePot {
	case mirPotReserves:
		if amount > reserves {
			return fmt.Errorf(
				"MIR pot transfer would underflow reserves: pot has %d, moving %d",
				reserves, amount,
			)
		}
		reserves -= amount
		treasury += amount
	case mirPotTreasury:
		if amount > treasury {
			return fmt.Errorf(
				"MIR pot transfer would underflow treasury: pot has %d, moving %d",
				treasury, amount,
			)
		}
		treasury -= amount
		reserves += amount
	default:
		return fmt.Errorf("unknown MIR source pot %d", sourcePot)
	}
	ls.config.Logger.Debug(
		"applied MIR pot-to-pot transfer",
		"source_pot", sourcePot,
		"amount", amount,
		"component", "ledger",
	)
	return ls.db.Metadata().SetNetworkState(treasury, reserves, slot, txn.Metadata())
}

// readNetworkState returns the current treasury and reserves from the most
// recent NetworkState row, returning (0, 0) if none exists yet.
func (ls *LedgerState) readNetworkState(txn *database.Txn) (treasury, reserves uint64, err error) {
	state, err := ls.db.Metadata().GetNetworkState(txn.Metadata())
	if err != nil {
		return 0, 0, fmt.Errorf("get network state: %w", err)
	}
	if state != nil {
		treasury = uint64(state.Treasury)
		reserves = uint64(state.Reserves)
	}
	return treasury, reserves, nil
}
