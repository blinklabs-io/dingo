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

package utxoset

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
)

type stakeEventEnc struct {
	cbor.StructAsArray
	Registered bool
	Deposit    []uint64
	TxHash     []byte
	BlockHash  []byte
	TxSlot     uint64
	Class      uint8
	Effective  uint64
}

type drepDepositEnc struct {
	cbor.StructAsArray
	Slot    uint64
	Deposit []uint64
}

func optionalDeposit(d *uint64) []uint64 {
	if d == nil {
		return nil
	}
	return []uint64{*d}
}

func depositOf(e []uint64) *uint64 {
	if len(e) == 0 {
		return nil
	}
	d := e[0]
	return &d
}

func stakeHistoryPrefix(ref models.StakeCredentialRef) []byte {
	k := make([]byte, 0, 3+len(ref.Key))
	k = append(k, badgerutxo.StakeHistoryTag, ref.Tag, byte(len(ref.Key))) //nolint:gosec // credentialKey bounds a credential to a byte
	return append(k, ref.Key...)
}

func stakeEventKey(ref models.StakeCredentialRef, slot, position uint64) []byte {
	k := stakeHistoryPrefix(ref)
	k = binary.BigEndian.AppendUint64(k, slot)
	return binary.BigEndian.AppendUint64(k, position)
}

func drepDepositKey(tag uint8, cred []byte) []byte {
	k := make([]byte, 0, 3+len(cred))
	k = append(k, badgerutxo.DRepDepositTag, tag, byte(len(cred))) //nolint:gosec // credentialKey bounds a credential to a byte
	return append(k, cred...)
}

func credentialKey(key []byte) error {
	if len(key) == 0 || len(key) > math.MaxUint8 {
		return fmt.Errorf("utxoset: credential of %d bytes", len(key))
	}
	return nil
}

func encodeStakeEvent(ev ledger.StakeEvent) ([]byte, error) {
	return cbor.Encode(&stakeEventEnc{
		Registered: ev.Registered, Deposit: optionalDeposit(ev.Deposit),
		TxHash: ev.TxHash, BlockHash: ev.BlockHash, TxSlot: ev.TxSlot,
		Class: uint8(ev.Class), Effective: ev.EffectiveSlot,
	})
}

func decodeStakeEvent(k, v []byte) (ledger.StakeEvent, error) {
	var e stakeEventEnc
	if _, err := cbor.Decode(v, &e); err != nil {
		return ledger.StakeEvent{}, fmt.Errorf("utxoset: decode stake event: %w", err)
	}
	if len(k) < 16 {
		return ledger.StakeEvent{}, fmt.Errorf("utxoset: malformed stake event key %x", k)
	}
	return ledger.StakeEvent{
		Slot:       binary.BigEndian.Uint64(k[len(k)-16:]),
		Position:   binary.BigEndian.Uint64(k[len(k)-8:]),
		Registered: e.Registered,
		Deposit:    depositOf(e.Deposit),
		TxHash:     e.TxHash,
		BlockHash:  e.BlockHash,
		TxSlot:     e.TxSlot,
		Class:      ledger.StakeRegistrationClass(e.Class),

		EffectiveSlot: e.Effective,
	}, nil
}

func encodeDRepDeposit(slot uint64, deposit *uint64) ([]byte, error) {
	return cbor.Encode(&drepDepositEnc{Slot: slot, Deposit: optionalDeposit(deposit)})
}

// registrationHistoryRows converts a commit's stake events and DRep
// registration deposits to journaled rows.
func registrationHistoryRows(lc ledger.LedgerChanges) ([]badgerutxo.RowPut, error) {
	out := make([]badgerutxo.RowPut, 0, len(lc.StakeEvents)+len(lc.DRepDeposits))
	for _, c := range lc.StakeEvents {
		if err := credentialKey(c.Ref.Key); err != nil {
			return nil, err
		}
		v, err := encodeStakeEvent(c.Event)
		if err != nil {
			return nil, err
		}
		out = append(out, badgerutxo.RowPut{
			Slot:  c.Event.Slot,
			Key:   stakeEventKey(c.Ref, c.Event.Slot, c.Event.Position),
			Value: v,
		})
	}
	for _, c := range lc.DRepDeposits {
		if err := credentialKey(c.Credential); err != nil {
			return nil, err
		}
		v, err := encodeDRepDeposit(c.Slot, c.Deposit)
		if err != nil {
			return nil, err
		}
		out = append(out, badgerutxo.RowPut{
			Slot:  c.Slot,
			Key:   drepDepositKey(c.Tag, c.Credential),
			Value: v,
		})
	}
	return out, nil
}

// ensureRegistrationHistory loads every stake registration and deregistration
// and each DRep's latest registration deposit from the metadata store into a
// set from before it kept them.
func (s *Set) ensureRegistrationHistory(ctx context.Context, ldb ledgerdb.LedgerDB) error {
	loaded, err := s.store.RegistrationHistoryLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the registration history marker: %w", err)
	}
	if loaded {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	refs, err := ldb.GetStakeCredentialsWithRegistrationEvents(0, math.MaxInt64, txn)
	if err != nil {
		return fmt.Errorf("utxoset: list registered credentials: %w", err)
	}
	deposits, err := ldb.GetDrepRegistrationDepositsAtSlot(ctx, nil, math.MaxInt64, txn)
	if err != nil {
		return fmt.Errorf("utxoset: read DRep deposits: %w", err)
	}
	events := 0
	err = s.store.LoadRegistrationHistory(func(put func(k, v []byte) error) error {
		for _, ref := range refs {
			if err := credentialKey(ref.Key); err != nil {
				return err
			}
			rows, err := ldb.GetAccountRegistrationHistoryByCredential(
				ctx, ref.Tag, ref.Key, 0, 0, "asc", txn,
			)
			if err != nil {
				return err
			}
			legacy, err := ldb.GetStakeRegistrationsByCredential(ref.Tag, ref.Key, txn)
			if err != nil {
				return err
			}
			class := loadedStakeClass(rows, len(legacy))
			for _, r := range rows {
				ev := ledger.StakeEvent{
					Slot:       r.AddedSlot,
					Position:   ledger.PoolEventPosition(r.BlockIndex, r.CertIndex),
					Registered: r.Action == "registered",
					Deposit:    r.Deposit,
					TxHash:     r.TxHash,
					BlockHash:  r.BlockHash,
					TxSlot:     r.TxSlot,
					Class:      ledger.StakeClassOther,

					EffectiveSlot: r.EffectiveSlot,
				}
				if ev.Registered {
					ev.Class = class
				}
				v, err := encodeStakeEvent(ev)
				if err != nil {
					return err
				}
				if err := put(stakeEventKey(ref, ev.Slot, ev.Position), v); err != nil {
					return err
				}
				events++
			}
		}
		for key, d := range deposits {
			if len(key) < 2 {
				return fmt.Errorf("utxoset: malformed DRep deposit key %x", key)
			}
			if err := credentialKey([]byte(key[1:])); err != nil {
				return err
			}
			v, err := encodeDRepDeposit(0, &d)
			if err != nil {
				return err
			}
			if err := put(drepDepositKey(key[0], []byte(key[1:])), v); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("utxoset: load registration history: %w", err)
	}
	s.logger.Info("loaded registration history into the live UTxO set",
		"credentials", len(refs), "events", events, "drep_deposits", len(deposits))
	return nil
}

// loadedStakeClass classifies a credential's registered history rows, which
// do not say which table each came from, by the number of stake_registration
// rows the credential has: none means every one is another type, as many as
// registered rows means every one is a stake_registration, and anything else
// leaves them unknown.
func loadedStakeClass(
	rows []models.AccountRegistrationHistoryRow,
	legacy int,
) ledger.StakeRegistrationClass {
	registered := 0
	for _, r := range rows {
		if r.Action == "registered" {
			registered++
		}
	}
	switch legacy {
	case 0:
		return ledger.StakeClassOther
	case registered:
		return ledger.StakeClassLegacy
	}
	return ledger.StakeClassUnknown
}

// RegistrationHistoryLoaded reports whether the set holds every stake
// registration event and DRep registration deposit.
func (s *Set) RegistrationHistoryLoaded() (bool, error) {
	return s.store.RegistrationHistoryLoaded()
}

// StakeHistory returns every registration and deregistration of ref, oldest
// first.
func (s *Set) StakeHistory(ref models.StakeCredentialRef) ([]ledger.StakeEvent, error) {
	var out []ledger.StakeEvent
	err := s.store.Rows(stakeHistoryPrefix(ref), func(k, v []byte) error {
		ev, err := decodeStakeEvent(k, v)
		if err != nil {
			return err
		}
		out = append(out, ev)
		return nil
	})
	return out, err
}

func (s *Set) drepDepositFrom(v []byte, ok bool, err error) (*uint64, error) {
	if err != nil || !ok {
		return nil, err
	}
	var e drepDepositEnc
	if _, err := cbor.Decode(v, &e); err != nil {
		return nil, fmt.Errorf("utxoset: decode DRep deposit: %w", err)
	}
	return depositOf(e.Deposit), nil
}

// DRepDeposit returns the deposit of the DRep's latest registration, or nil
// when it has none or the deposit is unknown.
func (s *Set) DRepDeposit(tag uint8, cred []byte) (*uint64, error) {
	v, ok, err := s.store.Row(drepDepositKey(tag, cred))
	return s.drepDepositFrom(v, ok, err)
}

// DRepDepositAt returns DRepDeposit as a rollback to slot would leave it.
func (s *Set) DRepDepositAt(tag uint8, cred []byte, slot uint64) (*uint64, error) {
	v, ok, err := s.store.RowAt(drepDepositKey(tag, cred), slot)
	return s.drepDepositFrom(v, ok, err)
}

// StakeCredentialsInRange returns each credential with a stake registration
// or deregistration whose applied slot is in [from, to]. The events are keyed
// by credential first, so the scan reads every stake event key, without the
// values; the one caller runs once per reward round.
func (s *Set) StakeCredentialsInRange(
	from, to uint64,
) ([]models.StakeCredentialRef, error) {
	var out []models.StakeCredentialRef
	if from > to {
		return out, nil
	}
	seen := make(map[string]struct{})
	err := s.store.Keys(
		[]byte{badgerutxo.StakeHistoryTag},
		func(k []byte) error {
			if len(k) < 3+16 || len(k) != 3+int(k[2])+16 {
				return fmt.Errorf("utxoset: malformed stake event key %x", k)
			}
			slot := binary.BigEndian.Uint64(k[len(k)-16:])
			if slot < from || slot > to {
				return nil
			}
			ref := models.NewStakeCredentialRef(
				k[1], append([]byte(nil), k[3:3+int(k[2])]...),
			)
			if _, ok := seen[ref.MapKey()]; ok {
				return nil
			}
			seen[ref.MapKey()] = struct{}{}
			out = append(out, ref)
			return nil
		},
	)
	return out, err
}

// StakeRegistrationsAt implements ledger.PointerRegistrationReader: it returns
// the credentials with a stake registration at each wanted certificate
// position. The scan reads every stake event key, so a caller asks for all of
// its positions at once.
func (s *Set) StakeRegistrationsAt(
	positions map[ledger.PointerPosition]struct{},
) (map[ledger.PointerPosition][]models.StakeCredentialRef, error) {
	out := make(map[ledger.PointerPosition][]models.StakeCredentialRef)
	if len(positions) == 0 {
		return out, nil
	}
	err := s.store.Rows(
		[]byte{badgerutxo.StakeHistoryTag},
		func(k, v []byte) error {
			if len(k) < 3+16 || len(k) != 3+int(k[2])+16 {
				return fmt.Errorf("utxoset: malformed stake event key %x", k)
			}
			pos := binary.BigEndian.Uint64(k[len(k)-8:])
			at := ledger.PointerPosition{
				Slot:      binary.BigEndian.Uint64(k[len(k)-16:]),
				TxIndex:   pos >> 32,
				CertIndex: pos & math.MaxUint32,
			}
			if _, ok := positions[at]; !ok {
				return nil
			}
			ev, err := decodeStakeEvent(k, v)
			if err != nil {
				return err
			}
			if !ev.Registered || ev.Class == ledger.StakeClassOther {
				return nil
			}
			key := append([]byte(nil), k[3:3+int(k[2])]...)
			out[at] = append(out[at], models.NewStakeCredentialRef(k[1], key))
			return nil
		},
	)
	return out, err
}
