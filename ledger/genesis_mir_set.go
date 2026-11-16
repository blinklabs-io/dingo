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
	"bytes"
	"cmp"
	"context"
	"fmt"
	"math/big"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// MIRCert is a MIR certificate, as the metadata store's
// move_instantaneous_rewards rows keep it: a distribution of signed deltas
// to stake credentials from Pot, or, when OtherPot is not zero, a transfer
// of that many lovelace from Pot to the other pot. Rewards are ordered by
// credential tag and credential.
type MIRCert struct {
	Slot     uint64
	Position uint64
	Pot      uint
	OtherPot uint64
	Rewards  []models.MIRReward
}

// CertHistoryReader serves the genesis key delegations and MIR certificates
// from a live UTxO set. Both are kept whole: header verification reads a
// delegation as of any slot, and a certificate is one small row.
type CertHistoryReader interface {
	// GenesisMIRLoaded reports whether the reader holds every genesis key
	// delegation and MIR certificate up to its tip; until it does, reads
	// stay on the metadata store.
	GenesisMIRLoaded() (bool, error)
	// GenesisDelegations returns every delegation, oldest first.
	GenesisDelegations() ([]models.GenesisDelegation, error)
	// MIRCerts returns every MIR certificate, oldest first.
	MIRCerts() ([]MIRCert, error)
}

func genesisDelegationPosition(d *models.GenesisDelegation) uint64 {
	return PoolEventPosition(d.BlockIndex, uint32(d.CertIndex)) //nolint:gosec // a transaction holds far fewer than 2^32 certificates
}

// certHistoryReader returns the set's genesis delegation and MIR reader and
// the read mode, or UtxoSetReadsOff when these reads stay on the metadata
// store. Certificates are staged with the account certificates, so the set
// must follow accounts too.
func (ls *LedgerState) certHistoryReader(
	txn ledgerdb.Txn,
) (CertHistoryReader, string) {
	if ls.accountReader(txn) == nil || ls.utxoSetFailed.Load() {
		return nil, UtxoSetReadsOff
	}
	_, mode := ls.utxoSetReader()
	if mode == UtxoSetReadsOff {
		return nil, UtxoSetReadsOff
	}
	r, ok := ls.readSet(txn).(CertHistoryReader)
	if !ok {
		return nil, UtxoSetReadsOff
	}
	if !ls.genesisMIRLoaded.Load() {
		loaded, err := r.GenesisMIRLoaded()
		if err != nil || !loaded {
			return nil, UtxoSetReadsOff
		}
		ls.genesisMIRLoaded.Store(true)
	}
	return r, mode
}

// genesisDelegationView returns every genesis key delegation as txn sees
// the set, oldest first: the committed rows, cut back by each staged
// rollback, with the staged batches' delegations laid over them.
func (ls *LedgerState) genesisDelegationView(
	r CertHistoryReader,
	txn ledgerdb.Txn,
) ([]models.GenesisDelegation, error) {
	rows, err := r.GenesisDelegations()
	if err != nil {
		return nil, err
	}
	for _, o := range ls.stagedOps(txn) {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			rows = slices.DeleteFunc(rows, func(d models.GenesisDelegation) bool {
				return d.AddedSlot > to
			})
		}
		for i := range o.genesisDelegs {
			c := o.genesisDelegs[i]
			rows = slices.DeleteFunc(rows, func(d models.GenesisDelegation) bool {
				return d.AddedSlot == c.AddedSlot &&
					genesisDelegationPosition(&d) == genesisDelegationPosition(&c)
			})
			rows = append(rows, c)
		}
	}
	slices.SortFunc(rows, func(a, b models.GenesisDelegation) int {
		if c := cmp.Compare(a.AddedSlot, b.AddedSlot); c != 0 {
			return c
		}
		return cmp.Compare(genesisDelegationPosition(&a), genesisDelegationPosition(&b))
	})
	return rows, nil
}

// mirCertView is genesisDelegationView for MIR certificates.
func (ls *LedgerState) mirCertView(
	r CertHistoryReader,
	txn ledgerdb.Txn,
) ([]MIRCert, error) {
	rows, err := r.MIRCerts()
	if err != nil {
		return nil, err
	}
	for _, o := range ls.stagedOps(txn) {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			rows = slices.DeleteFunc(rows, func(c MIRCert) bool {
				return c.Slot > to
			})
		}
		for _, c := range o.mirCerts {
			rows = slices.DeleteFunc(rows, func(e MIRCert) bool {
				return e.Slot == c.Slot && e.Position == c.Position
			})
			rows = append(rows, c)
		}
	}
	slices.SortFunc(rows, func(a, b MIRCert) int {
		if c := cmp.Compare(a.Slot, b.Slot); c != 0 {
			return c
		}
		return cmp.Compare(a.Position, b.Position)
	})
	return rows, nil
}

// stageGenesisMIRCertificate stages a genesis key delegation or MIR
// certificate as the metadata store writes it, and reports whether cert was
// one.
func (ls *LedgerState) stageGenesisMIRCertificate(
	txn ledgerdb.Txn,
	cert lcommon.Certificate,
	slot uint64,
	txIndex uint32,
	certIndex int,
) (bool, error) {
	position := PoolEventPosition(txIndex, uint32(certIndex)) //nolint:gosec // a transaction holds far fewer than 2^32 certificates
	switch c := cert.(type) {
	case *lcommon.GenesisKeyDelegationCertificate:
		ls.stageLedgerChanges(txn, LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
			GenesisDelegations: []models.GenesisDelegation{{
				GenesisHash:         bytes.Clone(c.GenesisHash),
				GenesisDelegateHash: bytes.Clone(c.GenesisDelegateHash),
				VrfKeyHash:          bytes.Clone(c.VrfKeyHash[:]),
				AddedSlot:           slot,
				BlockIndex:          txIndex,
				CertIndex:           uint(certIndex), //nolint:gosec // an index is not negative
			}},
		})
		return true, nil
	case *lcommon.MoveInstantaneousRewardsCertificate:
		mir := MIRCert{
			Slot:     slot,
			Position: position,
			Pot:      c.Reward.Source,
			OtherPot: c.Reward.OtherPot,
			Rewards:  []models.MIRReward{},
		}
		for credential, amount := range c.Reward.RewardsAmount() {
			tag, err := models.CredentialTagFromUint(credential.CredType)
			if err != nil {
				return true, err
			}
			if amount == nil {
				return true, fmt.Errorf(
					"MIR reward for credential %x has no delta",
					credential.Credential[:],
				)
			}
			mir.Rewards = append(mir.Rewards, models.MIRReward{
				Credential:    bytes.Clone(credential.Credential[:]),
				CredentialTag: tag,
				Amount:        new(big.Int).Set(amount),
			})
		}
		sortMIRRewards(mir.Rewards)
		ls.stageLedgerChanges(txn, LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
			MIRCerts: []MIRCert{mir},
		})
		return true, nil
	}
	return false, nil
}

func sortMIRRewards(rewards []models.MIRReward) {
	slices.SortFunc(rewards, func(a, b models.MIRReward) int {
		if c := cmp.Compare(a.CredentialTag, b.CredentialTag); c != 0 {
			return c
		}
		return bytes.Compare(a.Credential, b.Credential)
	})
}

func sameGenesisDelegation(a, b *models.GenesisDelegation) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return bytes.Equal(a.GenesisHash, b.GenesisHash) &&
		bytes.Equal(a.GenesisDelegateHash, b.GenesisDelegateHash) &&
		bytes.Equal(a.VrfKeyHash, b.VrfKeyHash) &&
		a.AddedSlot == b.AddedSlot &&
		a.BlockIndex == b.BlockIndex && a.CertIndex == b.CertIndex
}

func sameGenesisDelegations(a, b []models.GenesisDelegation) bool {
	return slices.EqualFunc(a, b, func(x, y models.GenesisDelegation) bool {
		return sameGenesisDelegation(&x, &y)
	})
}

// sameMIREffects compares MIR effects as the consumers read them: in order,
// with each distribution's rewards as a set, since the metadata store keeps
// a certificate's reward map in the order Go iterated it.
func sameMIREffects(a, b []models.MIREffect) bool {
	return slices.EqualFunc(a, b, func(x, y models.MIREffect) bool {
		if x.AddedSlot != y.AddedSlot || x.Pot != y.Pot ||
			x.OtherPot != y.OtherPot || len(x.Rewards) != len(y.Rewards) {
			return false
		}
		xr, yr := slices.Clone(x.Rewards), slices.Clone(y.Rewards)
		sortMIRRewards(xr)
		sortMIRRewards(yr)
		return slices.EqualFunc(xr, yr, func(p, q models.MIRReward) bool {
			if p.CredentialTag != q.CredentialTag ||
				!bytes.Equal(p.Credential, q.Credential) {
				return false
			}
			if p.Amount == nil || q.Amount == nil {
				return p.Amount == nil && q.Amount == nil
			}
			return p.Amount.Cmp(q.Amount) == 0
		})
	})
}

func mirEffectsOf(certs []MIRCert, start, end uint64) []models.MIREffect {
	out := []models.MIREffect{}
	for _, c := range certs {
		if c.Slot < start || c.Slot >= end {
			continue
		}
		out = append(out, models.MIREffect{
			AddedSlot: c.Slot,
			Pot:       c.Pot,
			OtherPot:  c.OtherPot,
			Rewards:   slices.Clone(c.Rewards),
		})
	}
	return out
}

func (ls *LedgerState) compareCertHistoryRead(
	g verifyReadGuard,
	what string,
	same bool,
	setErr error,
) {
	if !ls.verifying() {
		return
	}
	if setErr == nil && !same && ls.verifyReadRaced(g) {
		return
	}
	ls.metrics.certHistoryReadsVerified.Inc()
	if same && setErr == nil {
		return
	}
	n := ls.certHistoryReadMismatches.Add(1)
	ls.metrics.certHistoryReadMismatches.Inc()
	if n > 20 && n%1000 != 0 {
		return
	}
	ls.config.Logger.Warn(
		"live UTxO set genesis delegation or MIR read differs from the metadata store",
		"component", "ledger",
		"read", what,
		"set_error", setErr,
		"mismatches", n,
	)
}

// GetGenesisDelegationForSlot answers from the set's delegations when it
// holds them, or compares them with the metadata store's answer in verify
// mode: the latest delegation of the genesis key certified at least the
// stability window before blockSlot.
func (s stagingLedgerDB) GetGenesisDelegationForSlot(
	genesisHash []byte,
	blockSlot uint64,
	stabilityWindow uint64,
	txn ledgerdb.Txn,
) (*models.GenesisDelegation, error) {
	r, mode := s.ls.certHistoryReader(txn)
	if r == nil {
		return s.LedgerDB.GetGenesisDelegationForSlot(
			genesisHash, blockSlot, stabilityWindow, txn,
		)
	}
	g := s.ls.verifyGuard(txn)
	var got *models.GenesisDelegation
	rows, setErr := s.ls.genesisDelegationView(r, txn)
	if setErr == nil && blockSlot >= stabilityWindow {
		for _, d := range slices.Backward(rows) {
			if bytes.Equal(d.GenesisHash, genesisHash) &&
				d.AddedSlot <= blockSlot-stabilityWindow {
				got = &d
				break
			}
		}
	}
	if mode == UtxoSetReadsSet {
		return got, setErr
	}
	want, err := s.LedgerDB.GetGenesisDelegationForSlot(
		genesisHash, blockSlot, stabilityWindow, txn,
	)
	if err != nil {
		return want, err
	}
	s.ls.compareCertHistoryRead(
		g,
		fmt.Sprintf("genesis delegation of %x at slot %d", genesisHash, blockSlot),
		sameGenesisDelegation(want, got), setErr,
	)
	return want, nil
}

// GetGenesisDelegationsInSlotRange answers from the set's delegations when
// it holds them, or compares them with the metadata store's answer in verify
// mode: every delegation certified in [fromSlot, uptoSlot].
func (s stagingLedgerDB) GetGenesisDelegationsInSlotRange(
	fromSlot uint64,
	uptoSlot uint64,
	txn ledgerdb.Txn,
) ([]models.GenesisDelegation, error) {
	r, mode := s.ls.certHistoryReader(txn)
	if r == nil {
		return s.LedgerDB.GetGenesisDelegationsInSlotRange(fromSlot, uptoSlot, txn)
	}
	g := s.ls.verifyGuard(txn)
	var got []models.GenesisDelegation
	rows, setErr := s.ls.genesisDelegationView(r, txn)
	if setErr == nil && uptoSlot >= fromSlot {
		for _, d := range rows {
			if d.AddedSlot >= fromSlot && d.AddedSlot <= uptoSlot {
				got = append(got, d)
			}
		}
	}
	if mode == UtxoSetReadsSet {
		return got, setErr
	}
	want, err := s.LedgerDB.GetGenesisDelegationsInSlotRange(fromSlot, uptoSlot, txn)
	if err != nil {
		return want, err
	}
	s.ls.compareCertHistoryRead(
		g,
		fmt.Sprintf("genesis delegations in slots [%d, %d]", fromSlot, uptoSlot),
		sameGenesisDelegations(want, got), setErr,
	)
	return want, nil
}

// GetMIRCertsInSlotRange answers from the set's MIR certificates when it
// holds them, or compares them with the metadata store's answer in verify
// mode: every certificate in [startSlot, endSlot), oldest first.
func (s stagingLedgerDB) GetMIRCertsInSlotRange(
	ctx context.Context,
	startSlot, endSlot uint64,
	txn ledgerdb.Txn,
) ([]models.MIREffect, error) {
	r, mode := s.ls.certHistoryReader(txn)
	if r == nil {
		return s.LedgerDB.GetMIRCertsInSlotRange(ctx, startSlot, endSlot, txn)
	}
	g := s.ls.verifyGuard(txn)
	certs, setErr := s.ls.mirCertView(r, txn)
	got := mirEffectsOf(certs, startSlot, endSlot)
	if mode == UtxoSetReadsSet {
		if setErr != nil {
			return nil, setErr
		}
		return got, nil
	}
	want, err := s.LedgerDB.GetMIRCertsInSlotRange(ctx, startSlot, endSlot, txn)
	if err != nil {
		return want, err
	}
	s.ls.compareCertHistoryRead(
		g,
		fmt.Sprintf("MIR certificates in slots [%d, %d)", startSlot, endSlot),
		sameMIREffects(want, got), setErr,
	)
	return want, nil
}
