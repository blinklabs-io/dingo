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
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// witnessedRewardCredentials returns the reward-account stake credentials that
// tx witnesses under CIP-0163: the stake credential of every reward-withdrawal
// entry and of every stake registration/deregistration, stake delegation, and
// vote/DRep delegation certificate (including the combined
// registration+delegation certificates). Duplicates are collapsed.
//
// The tag/hash extraction mirrors exactly how
// database/plugin/metadata/sqlite applies withdrawals
// (applyTransactionRewardWithdrawals: StakeKeyHash +
// StakeCredentialTagFromAddress) and certificates (StakeCredential.Credential /
// CredentialTagFromUint(StakeCredential.CredType)), so the witnessed set matches
// the credentials the ledger itself writes for the same transaction. Callers
// are responsible for skipping phase-2-invalid transactions (see
// renewWitnessedAccountExpirations); this helper reports what a transaction
// would witness regardless of validity.
func witnessedRewardCredentials(
	tx lcommon.Transaction,
) []models.StakeCredentialRef {
	seen := make(map[string]struct{})
	var refs []models.StakeCredentialRef
	add := func(tag uint8, key []byte) {
		ref := models.NewStakeCredentialRef(tag, key)
		k := ref.MapKey()
		if _, ok := seen[k]; ok {
			return
		}
		seen[k] = struct{}{}
		refs = append(refs, ref)
	}
	// addFromCredential derives the credential tag from a stake Credential's
	// CredType the same way SetTransaction does. A CredType the ledger would
	// reject never reaches this hook (SetTransaction runs first and would have
	// failed the block), so an unmappable tag is skipped rather than surfaced as
	// an error from a hook that must not introduce new rejection paths.
	addFromCredential := func(cred *lcommon.Credential) {
		if cred == nil {
			return
		}
		tag, err := models.CredentialTagFromUint(cred.CredType)
		if err != nil {
			return
		}
		add(tag, cred.Credential[:])
	}
	// Reward withdrawals: each map key is a reward account whose stake
	// credential the transaction must witness. Amount is irrelevant to
	// witnessing (the withdrawal still requires the account's signature), so
	// unlike reward-balance application zero-amount entries are not skipped.
	for addr := range tx.Withdrawals() {
		if addr == nil {
			continue
		}
		tag, ok := models.StakeCredentialTagFromAddress(*addr)
		if !ok {
			continue
		}
		add(tag, addr.StakeKeyHash().Bytes())
	}
	for _, cert := range tx.Certificates() {
		switch c := cert.(type) {
		case *lcommon.StakeRegistrationCertificate:
			addFromCredential(&c.StakeCredential)
		case *lcommon.StakeDeregistrationCertificate:
			addFromCredential(&c.StakeCredential)
		case *lcommon.StakeDelegationCertificate:
			addFromCredential(c.StakeCredential)
		case *lcommon.VoteDelegationCertificate:
			addFromCredential(&c.StakeCredential)
		case *lcommon.StakeVoteDelegationCertificate:
			addFromCredential(&c.StakeCredential)
		case *lcommon.RegistrationCertificate:
			addFromCredential(&c.StakeCredential)
		case *lcommon.DeregistrationCertificate:
			addFromCredential(&c.StakeCredential)
		case *lcommon.StakeRegistrationDelegationCertificate:
			addFromCredential(&c.StakeCredential)
		case *lcommon.StakeVoteRegistrationDelegationCertificate:
			addFromCredential(&c.StakeCredential)
		case *lcommon.VoteRegistrationDelegationCertificate:
			addFromCredential(&c.StakeCredential)
		}
	}
	return refs
}

// renewWitnessedAccountExpirations bumps the CIP-0163 expiration_epoch of every
// reward account witnessed by the given block's transactions to
// currentEpoch + DelegatorInactivity. It is a no-op when the
// delegator-inactivity gate is disabled (compute nothing, call nothing).
//
// Phase-2-invalid transactions apply no certificates or reward withdrawals
// (database/plugin/metadata/sqlite SetTransaction gates both on tx.IsValid()),
// so they witness nothing under CIP-0163 and are skipped here to keep the
// witnessed set identical to the credentials the ledger actually wrote.
//
// RenewAccountExpirations only updates existing account rows, so this must run
// inside the same database transaction as the block's account writes and after
// them: a stake-key registration in this block creates the row that a later
// (or same) witnessing event in the same block then renews. Passing
// currentEpoch (the epoch of the block being applied) makes the write
// monotonic — a witness in the current epoch always yields an expiration >= any
// earlier renewal, and re-witnessing within an epoch is idempotent.
func (ls *LedgerState) renewWitnessedAccountExpirations(
	txn *database.Txn,
	currentEpoch uint64,
	txs []lcommon.Transaction,
) error {
	if !ls.config.DelegatorInactivityEnabled {
		return nil
	}
	expiration := currentEpoch + ls.config.DelegatorInactivity
	seen := make(map[string]struct{})
	var refs []models.StakeCredentialRef
	for _, tx := range txs {
		if tx == nil || !tx.IsValid() {
			continue
		}
		for _, ref := range witnessedRewardCredentials(tx) {
			k := ref.MapKey()
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			refs = append(refs, ref)
		}
	}
	if len(refs) == 0 {
		return nil
	}
	return ls.db.RenewAccountExpirations(refs, expiration, txn)
}
