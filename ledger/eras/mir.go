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

package eras

import (
	"fmt"
	"math/big"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// MIRCredentialKey identifies one stake credential within one source pot, for
// the purpose of folding move instantaneous rewards deltas accumulated within
// an epoch. All three fields are required: a key hash and a script hash may
// coincidentally share 28 bytes (hence Tag), and the reference keeps reserves
// and treasury deltas in two entirely separate maps -- iRReserves and
// iRTreasury, each keyed by Credential -- so a surplus pending in one pot must
// never offset a deficit in the other (hence Pot). Pot uses the same 0
// (reserves) / 1 (treasury) convention as cert.Reward.Source and
// models.MIREffect.Pot.
type MIRCredentialKey struct {
	Tag        uint8
	Credential lcommon.Blake2b224
	Pot        uint
}

// MIRPendingRewardsProvider is satisfied by the dingo ledger state to expose
// each credential's already-committed InstantaneousRewards delta accumulated
// so far in the current epoch, as of and including uptoSlot. Exported so
// implementers (e.g. *ledger.LedgerView) can assert conformance at compile
// time; a signature drift here would otherwise silently disable the
// MIRProducesNegativeUpdate check at runtime.
//
// gouroboros's own shelley.validateMirDeltaSigns documents that this
// aggregate check "needs the pending InstantaneousRewards accumulated by
// earlier certificates in the epoch and so is not expressible against the
// LedgerState interface" -- this interface is dingo's answer to that gap.
type MIRPendingRewardsProvider interface {
	PendingMIRRewardDeltas(
		uptoSlot uint64,
	) (map[MIRCredentialKey]*big.Int, error)
}

// MIRProducesNegativeUpdateError indicates a move instantaneous rewards
// certificate whose delta, folded into the InstantaneousRewards accumulated
// for its credential so far this epoch, would drive that credential's pending
// reward below zero.
//
// Reference: MIRProducesNegativeUpdate in delegTransition,
// eras/shelley/impl/src/Cardano/Ledger/Shelley/Rules/Deleg.hs.
type MIRProducesNegativeUpdateError struct {
	CredentialTag uint8
	Credential    lcommon.Blake2b224
	Pot           uint
	Existing      *big.Int
	Delta         *big.Int
}

func (e MIRProducesNegativeUpdateError) Error() string {
	existing := "<nil>"
	if e.Existing != nil {
		existing = e.Existing.String()
	}
	delta := "<nil>"
	if e.Delta != nil {
		delta = e.Delta.String()
	}
	return fmt.Sprintf(
		"MIR produces negative update: credential %x pot %d existing %s delta %s",
		e.Credential[:],
		e.Pot,
		existing,
		delta,
	)
}

// validateMIRAccumulatedRewards enforces the reference MIRProducesNegativeUpdate
// rule. From protocol version 5 onward (lcommon.MirTransferAllowed), a move
// instantaneous rewards certificate may carry a negative reward delta only if
// folding it into that credential's InstantaneousRewards accumulated so far
// this epoch -- from certificates already committed earlier in the epoch, and
// from earlier certificates within this same transaction, applied in order --
// does not drive the result negative.
//
// Below protocol version 5, gouroboros's own shelley.validateMirDeltaSigns
// already rejects any negative delta outright via
// MIRNegativesNotCurrentlyAllowedError, so this function is a no-op there:
// the reference never reaches the accumulation check before Alonzo.
func validateMIRAccumulatedRewards(
	tx lcommon.Transaction,
	slot uint64,
	ls lcommon.LedgerState,
	major uint,
) error {
	if !lcommon.MirTransferAllowed(major) {
		return nil
	}
	var mirCerts []*lcommon.MoveInstantaneousRewardsCertificate
	for _, cert := range tx.Certificates() {
		c, ok := cert.(*lcommon.MoveInstantaneousRewardsCertificate)
		if ok && c != nil && len(c.Reward.Rewards) > 0 {
			mirCerts = append(mirCerts, c)
		}
	}
	if len(mirCerts) == 0 {
		return nil
	}
	provider, ok := ls.(MIRPendingRewardsProvider)
	if !ok {
		return nil
	}
	running, err := provider.PendingMIRRewardDeltas(slot)
	if err != nil {
		return fmt.Errorf("pending MIR reward deltas: %w", err)
	}
	if running == nil {
		running = make(map[MIRCredentialKey]*big.Int)
	}
	for _, cert := range mirCerts {
		for cred, delta := range cert.Reward.RewardsAmount() {
			if cred == nil || delta == nil {
				continue
			}
			key := MIRCredentialKey{
				//nolint:gosec // CredType is decode-validated to 0 or 1 by gouroboros
				Tag:        uint8(cred.CredType),
				Credential: cred.Credential,
				Pot:        cert.Reward.Source,
			}
			existing := running[key]
			if existing == nil {
				existing = new(big.Int)
			}
			result := new(big.Int).Add(existing, delta)
			if result.Sign() < 0 {
				return MIRProducesNegativeUpdateError{
					CredentialTag: key.Tag,
					Credential:    key.Credential,
					Pot:           key.Pot,
					Existing:      existing,
					Delta:         delta,
				}
			}
			running[key] = result
		}
	}
	return nil
}
