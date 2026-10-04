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

package utxoref

import (
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// AccountEffectKind identifies one reward-account balance or registration
// change made by a transaction.
type AccountEffectKind uint8

const (
	// AccountWithdrawal drains Amount lovelace from the account.
	AccountWithdrawal AccountEffectKind = iota
	// AccountRegistration registers the stake credential with an empty
	// reward balance.
	AccountRegistration
	// AccountDeregistration unregisters the stake credential and discards
	// its reward account.
	AccountDeregistration
	// AccountDirectDeposit credits Amount lovelace to the account's
	// reward balance.
	AccountDirectDeposit
)

// AccountEffect is one ordered reward-account change.
type AccountEffect struct {
	Credential lcommon.Credential
	Kind       AccountEffectKind
	Amount     uint64
}

// AccountEffects returns the reward-account changes a valid transaction makes,
// in ledger order: each Dijkstra sub-transaction in encoded order, then the
// enclosing body, and within a body its withdrawals, then its certificates,
// then its direct deposits.
// A phase-2-invalid transaction changes no account.
func AccountEffects(tx lcommon.Transaction) []AccountEffect {
	if tx == nil || !tx.IsValid() {
		return nil
	}
	var effects []AccountEffect
	for _, body := range lcommon.SubTransactionBodiesFromTransaction(tx) {
		effects = appendBodyAccountEffects(effects, body)
	}
	return appendBodyAccountEffects(effects, tx)
}

func appendBodyAccountEffects(
	effects []AccountEffect,
	body lcommon.TransactionBody,
) []AccountEffect {
	for address, amount := range body.Withdrawals() {
		credential, ok := address.StakeCredential()
		if !ok || amount == nil || !amount.IsUint64() {
			continue
		}
		effects = append(effects, AccountEffect{
			Credential: credential,
			Kind:       AccountWithdrawal,
			Amount:     amount.Uint64(),
		})
	}
	for _, cert := range body.Certificates() {
		switch c := cert.(type) {
		case *lcommon.StakeRegistrationCertificate:
			effects = appendAccountRegistration(effects, c.StakeCredential)
		case *lcommon.RegistrationCertificate:
			effects = appendAccountRegistration(effects, c.StakeCredential)
		case *lcommon.StakeRegistrationDelegationCertificate:
			effects = appendAccountRegistration(effects, c.StakeCredential)
		case *lcommon.VoteRegistrationDelegationCertificate:
			effects = appendAccountRegistration(effects, c.StakeCredential)
		case *lcommon.StakeVoteRegistrationDelegationCertificate:
			effects = appendAccountRegistration(effects, c.StakeCredential)
		case *lcommon.StakeDeregistrationCertificate:
			effects = append(effects, AccountEffect{
				Credential: c.StakeCredential,
				Kind:       AccountDeregistration,
			})
		case *lcommon.DeregistrationCertificate:
			effects = append(effects, AccountEffect{
				Credential: c.StakeCredential,
				Kind:       AccountDeregistration,
			})
		}
	}
	return appendDirectDeposits(effects, body)
}

// appendDirectDeposits adds a Dijkstra body's reward-account credits. A key
// that is not a reward account is skipped: validation rejects it, so it can
// never take effect.
func appendDirectDeposits(
	effects []AccountEffect,
	body lcommon.TransactionBody,
) []AccountEffect {
	var deposits dijkstra.DijkstraDirectDeposits
	switch b := body.(type) {
	case *dijkstra.DijkstraTransaction:
		deposits = b.Body.TxDirectDeposits
	case *dijkstra.DijkstraTransactionBody:
		deposits = b.TxDirectDeposits
	case *dijkstra.DijkstraSubTransactionBody:
		deposits = b.TxDirectDeposits
	}
	for rewardAddress, amount := range deposits {
		address, err := lcommon.NewAddressFromBytes(rewardAddress.Bytes())
		if err != nil {
			continue
		}
		credential, err := address.RewardAccountCredential()
		if err != nil {
			continue
		}
		effects = append(effects, AccountEffect{
			Credential: credential,
			Kind:       AccountDirectDeposit,
			Amount:     amount,
		})
	}
	return effects
}

func appendAccountRegistration(
	effects []AccountEffect,
	credential lcommon.Credential,
) []AccountEffect {
	return append(effects, AccountEffect{
		Credential: credential,
		Kind:       AccountRegistration,
	})
}

// AccountOverlay accumulates the reward-account effects of transactions that
// are pending or already selected for a block but not yet applied to the
// ledger, so each later transaction is validated against the account state the
// earlier ones leave behind. A nil overlay holds no effects.
type AccountOverlay struct {
	accounts map[accountKey]*overlayAccount
}

// accountKey is the comparable form of a stake credential; Credential itself
// embeds CBOR bookkeeping that cannot key a map.
type accountKey struct {
	credType uint
	hash     lcommon.CredentialHash
}

func keyForCredential(credential lcommon.Credential) accountKey {
	return accountKey{credType: credential.CredType, hash: credential.Credential}
}

type overlayAccount struct {
	// changed is set once a pending registration or deregistration decides
	// the account's registration; registered then replaces the stored value.
	changed    bool
	registered bool
	withdrawn  uint64
	credited   uint64
}

// NewAccountOverlay returns an empty overlay.
func NewAccountOverlay() *AccountOverlay {
	return &AccountOverlay{
		accounts: make(map[accountKey]*overlayAccount),
	}
}

// Apply records effects after the transaction that made them is accepted.
func (o *AccountOverlay) Apply(effects []AccountEffect) {
	for _, effect := range effects {
		account := o.accounts[keyForCredential(effect.Credential)]
		if account == nil {
			account = &overlayAccount{}
			o.accounts[keyForCredential(effect.Credential)] = account
		}
		switch effect.Kind {
		case AccountWithdrawal:
			account.withdrawn = saturatingAdd(account.withdrawn, effect.Amount)
		case AccountDirectDeposit:
			account.credited = saturatingAdd(account.credited, effect.Amount)
		case AccountRegistration:
			account.changed = true
			account.registered = true
			account.withdrawn = 0
			account.credited = 0
		case AccountDeregistration:
			account.changed = true
			account.registered = false
			account.withdrawn = 0
			account.credited = 0
		}
	}
}

// Registration reports whether pending effects decide the credential's
// registration, and if so whether it ends up registered. A pending
// registration or deregistration supersedes the stored account entirely.
func (o *AccountOverlay) Registration(
	credential lcommon.Credential,
) (registered bool, decided bool) {
	if o == nil {
		return false, false
	}
	account := o.accounts[keyForCredential(credential)]
	if account == nil || !account.changed {
		return false, false
	}
	return account.registered, true
}

// Balance applies pending credits and withdrawals to the stored reward
// balance. It saturates at zero and at the uint64 maximum: a withdrawal
// already confirmed in the stored balance must not wrap the difference.
func (o *AccountOverlay) Balance(
	credential lcommon.Credential,
	stored uint64,
) uint64 {
	if o == nil {
		return stored
	}
	account := o.accounts[keyForCredential(credential)]
	if account == nil {
		return stored
	}
	return saturatingSub(saturatingAdd(stored, account.credited), account.withdrawn)
}

func saturatingSub(a, b uint64) uint64 {
	return a - min(a, b)
}

func saturatingAdd(a, b uint64) uint64 {
	if sum := a + b; sum >= a {
		return sum
	}
	return ^uint64(0)
}
