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
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/governance"
)

const (
	// mirPotReserves is the on-chain CBOR encoding of the reserves pot (0).
	mirPotReserves = uint(0)
	// mirPotTreasury is the on-chain CBOR encoding of the treasury pot (1).
	mirPotTreasury = uint(1)

	mirRewardSourcePrefix = "dingo:mir:"
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
// The boundary is evaluated as a whole before any of it is applied, mirroring
// cardano-ledger's MIR rule (`Cardano.Ledger.Shelley.Rules.Mir.mirTransition`),
// which folds every pending certificate into one `InstantaneousRewards` value
// and applies it only when both pots can cover their totals:
//
//	availableReserves = reserves + deltaReserves
//	availableTreasury = treasury + deltaTreasury
//	if totR <= availableReserves && totT <= availableTreasury then ... else ...
//
// Distribution MIR (credential→amount map):
//   - Registered, active reward accounts are credited from the source pot.
//   - Credentials without a registered account are silently skipped — unlike
//     POOLREAP, there is no fallback routing to the treasury. They are also
//     excluded from the pot totals, matching the `Map.intersection accountsMap`
//     restriction cardano-ledger applies before folding.
//   - The source pot is debited only for amounts actually credited.
//
// Pot-to-pot transfer MIR (OtherPot > 0):
//   - Source=0 (Reserves) moves OtherPot lovelace from reserves to treasury.
//   - Source=1 (Treasury) moves OtherPot lovelace from treasury to reserves.
//   - Transfers are folded into the available pot balances before the capacity
//     check, so they fund distributions made at the same boundary.
//
// When either pot cannot cover its total the whole boundary is a no-op: no
// credit, no debit and no transfer is written, and the epoch rollover still
// succeeds. cardano-ledger's else branch returns the original
// `ChainAccountState` and clears the pending rewards, so an over-budget
// certificate is discarded rather than retried. Failing the boundary instead
// would wedge the node, since the stored certificates are re-read and re-fail
// on every deterministic retry.
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
	boundary, err := ls.collectMIRBoundary(txn, effects)
	if err != nil {
		return err
	}
	if boundary.isEmpty() {
		return nil
	}
	treasury, reserves, err := ls.readNetworkState(txn)
	if err != nil {
		return fmt.Errorf("apply MIR certs: %w", err)
	}
	availableReserves, reservesOk, err := mirAvailablePot(
		"reserves", reserves, boundary.reservesIn, boundary.reservesOut,
	)
	if err != nil {
		return err
	}
	availableTreasury, treasuryOk, err := mirAvailablePot(
		"treasury", treasury, boundary.treasuryIn, boundary.treasuryOut,
	)
	if err != nil {
		return err
	}
	if !reservesOk || !treasuryOk ||
		boundary.totalReserves > availableReserves ||
		boundary.totalTreasury > availableTreasury {
		ls.config.Logger.Warn(
			"skipping over-budget MIR at epoch boundary",
			"slot", boundarySlot,
			"reserves", reserves,
			"reserves_distributed", boundary.totalReserves,
			"treasury", treasury,
			"treasury_distributed", boundary.totalTreasury,
			"component", "ledger",
		)
		return nil
	}
	appliedReserves, appliedTreasury, err := ls.applyMIRCredits(
		txn, boundary.credits, boundarySlot,
	)
	if err != nil {
		return err
	}
	newReserves := availableReserves - appliedReserves
	newTreasury := availableTreasury - appliedTreasury
	if newReserves == reserves && newTreasury == treasury {
		return nil
	}
	return ls.db.Metadata().
		SetNetworkState(newTreasury, newReserves, boundarySlot, txn.Metadata())
}

// mirCredit is one registered-account credit selected for application at an
// epoch boundary.
type mirCredit struct {
	mirID         uint
	pot           uint
	credentialTag uint8
	credential    []byte
	amount        uint64
}

// mirBoundary is the aggregate of every MIR certificate in one ended epoch,
// collected before any of it is applied. It is the Dingo equivalent of
// cardano-ledger's accumulated `InstantaneousRewards`: totalReserves/
// totalTreasury correspond to `totR`/`totT` over the registered-account
// restriction, and the in/out sums to `deltaReserves`/`deltaTreasury`.
type mirBoundary struct {
	credits       []mirCredit
	totalReserves uint64
	totalTreasury uint64
	reservesIn    uint64
	reservesOut   uint64
	treasuryIn    uint64
	treasuryOut   uint64
}

// isEmpty reports whether the boundary carries no pot movement at all, in
// which case no NetworkState row is written for it.
func (b *mirBoundary) isEmpty() bool {
	return len(b.credits) == 0 &&
		b.reservesIn == 0 && b.reservesOut == 0 &&
		b.treasuryIn == 0 && b.treasuryOut == 0
}

// addCredit records a credit against its source pot, keeping the per-pot total
// that the capacity check compares against the pot.
func (b *mirBoundary) addCredit(credit mirCredit) error {
	total := &b.totalReserves
	if credit.pot == mirPotTreasury {
		total = &b.totalTreasury
	}
	if credit.amount > ^uint64(0)-*total {
		return fmt.Errorf(
			"MIR distribution total overflow: current=%d adding=%d",
			*total,
			credit.amount,
		)
	}
	*total += credit.amount
	b.credits = append(b.credits, credit)
	return nil
}

// addTransfer records a pot-to-pot transfer as an outflow from its source pot
// and an inflow to the other one.
func (b *mirBoundary) addTransfer(sourcePot uint, amount uint64) error {
	var out, in *uint64
	switch sourcePot {
	case mirPotReserves:
		out, in = &b.reservesOut, &b.treasuryIn
	case mirPotTreasury:
		out, in = &b.treasuryOut, &b.reservesIn
	default:
		return fmt.Errorf("unknown MIR source pot %d", sourcePot)
	}
	if amount > ^uint64(0)-*out || amount > ^uint64(0)-*in {
		return fmt.Errorf(
			"MIR pot transfer total overflow: moving %d",
			amount,
		)
	}
	*out += amount
	*in += amount
	return nil
}

// collectMIRBoundary folds every effect for the ended epoch into a single
// mirBoundary without mutating any state. Distribution credits are restricted
// to registered, active reward accounts, resolved in one batched lookup.
func (ls *LedgerState) collectMIRBoundary(
	txn *database.Txn,
	effects []models.MIREffect,
) (*mirBoundary, error) {
	registered, err := ls.registeredMIRAccounts(txn, effects)
	if err != nil {
		return nil, err
	}
	boundary := &mirBoundary{}
	for _, effect := range effects {
		if effect.OtherPot > 0 {
			if err := boundary.addTransfer(
				effect.Pot, effect.OtherPot,
			); err != nil {
				return nil, err
			}
			continue
		}
		if effect.Pot != mirPotReserves && effect.Pot != mirPotTreasury {
			return nil, fmt.Errorf("unknown MIR pot %d", effect.Pot)
		}
		for _, reward := range effect.Rewards {
			ref := models.NewStakeCredentialRef(
				reward.CredentialTag, reward.Credential,
			)
			if !registered[ref.MapKey()] {
				continue
			}
			if err := boundary.addCredit(mirCredit{
				mirID:         effect.ID,
				pot:           effect.Pot,
				credentialTag: reward.CredentialTag,
				credential:    reward.Credential,
				amount:        reward.Amount,
			}); err != nil {
				return nil, err
			}
		}
	}
	return boundary, nil
}

// registeredMIRAccounts returns the set of distribution credentials that have a
// registered, active reward account, keyed by StakeCredentialRef.MapKey(). This
// is the read-only equivalent of the account lookup the credit path performs,
// and it establishes the same registered-vs-skipped split before any pot is
// debited.
func (ls *LedgerState) registeredMIRAccounts(
	txn *database.Txn,
	effects []models.MIREffect,
) (map[string]bool, error) {
	refs := make([]models.StakeCredentialRef, 0, len(effects))
	seen := make(map[string]struct{})
	for _, effect := range effects {
		if effect.OtherPot > 0 {
			continue
		}
		for _, reward := range effect.Rewards {
			ref := models.NewStakeCredentialRef(
				reward.CredentialTag, reward.Credential,
			)
			if _, ok := seen[ref.MapKey()]; ok {
				continue
			}
			seen[ref.MapKey()] = struct{}{}
			refs = append(refs, ref)
		}
	}
	if len(refs) == 0 {
		return nil, nil
	}
	accounts, err := ls.db.GetAccountsByCredential(refs, false, txn)
	if err != nil {
		return nil, fmt.Errorf("get MIR reward accounts: %w", err)
	}
	registered := make(map[string]bool, len(accounts))
	for key, account := range accounts {
		if account != nil {
			registered[key] = true
		}
	}
	return registered, nil
}

// applyMIRCredits credits every selected reward account and returns the amount
// actually applied per source pot. The totals are re-derived here rather than
// reused from the capacity check so the pot debit can never exceed what was
// credited.
func (ls *LedgerState) applyMIRCredits(
	txn *database.Txn,
	credits []mirCredit,
	boundarySlot uint64,
) (appliedReserves, appliedTreasury uint64, err error) {
	for _, credit := range credits {
		credited, err := governance.CreditRegisteredRewardAccountBeforeSnapshot(
			ls.db,
			txn,
			credit.credentialTag,
			credit.credential,
			credit.amount,
			boundarySlot,
			// MIR has no transaction hash in this processed effect, so
			// the MIR row ID is encoded as a synthetic discriminator.
			// That keeps two MIR certs crediting the same account in
			// one epoch as distinct journal rows while still mapping a
			// crash-replayed boundary to the same row.
			mirRewardSourceHash(credit.mirID),
		)
		if err != nil {
			return 0, 0, fmt.Errorf(
				"apply MIR reward to %x: %w",
				credit.credential, err,
			)
		}
		if !credited {
			continue
		}
		if credit.pot == mirPotTreasury {
			appliedTreasury += credit.amount
		} else {
			appliedReserves += credit.amount
		}
		ls.config.Logger.Debug(
			"applied MIR reward",
			"credential", hex.EncodeToString(credit.credential),
			"amount", credit.amount,
			"pot", credit.pot,
			"component", "ledger",
		)
	}
	return appliedReserves, appliedTreasury, nil
}

// mirAvailablePot folds the epoch's pot-to-pot transfers into a pot balance.
// cardano-ledger computes `reserves + deltaReserves` over unbounded Coin, so a
// net outflow larger than the pot yields a negative available balance and the
// rule takes its no-op branch; ok=false reports that case. A uint64 overflow on
// the inbound side has no cardano-ledger analogue and is reported as an error
// against corrupt stored state.
func mirAvailablePot(
	name string,
	balance, in, out uint64,
) (available uint64, ok bool, err error) {
	if balance > ^uint64(0)-in {
		return 0, false, fmt.Errorf(
			"MIR pot transfer would overflow %s: pot has %d, moving %d",
			name,
			balance,
			in,
		)
	}
	available = balance + in
	if out > available {
		return 0, false, nil
	}
	return available - out, true, nil
}

func mirRewardSourceHash(mirID uint) []byte {
	out := make([]byte, len(mirRewardSourcePrefix)+8)
	copy(out, mirRewardSourcePrefix)
	binary.BigEndian.PutUint64(
		out[len(mirRewardSourcePrefix):],
		uint64(mirID),
	)
	return out
}

// readNetworkState returns the current treasury and reserves from the most
// recent NetworkState row, returning (0, 0) if none exists yet.
func (ls *LedgerState) readNetworkState(
	txn *database.Txn,
) (treasury, reserves uint64, err error) {
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
