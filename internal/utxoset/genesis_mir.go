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
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
)

type genesisDelegationEnc struct {
	cbor.StructAsArray
	GenesisHash []byte
	Delegate    []byte
	Vrf         []byte
}

type mirRewardEnc struct {
	cbor.StructAsArray
	Tag        uint8
	Credential []byte
	Amount     string
}

type mirCertEnc struct {
	cbor.StructAsArray
	Pot      uint
	OtherPot uint64
	Rewards  []mirRewardEnc
}

func certHistoryKey(tag byte, slot, position uint64) []byte {
	k := make([]byte, 0, 17)
	k = append(k, tag)
	k = binary.BigEndian.AppendUint64(k, slot)
	return binary.BigEndian.AppendUint64(k, position)
}

func genesisDelegationRowKey(d *models.GenesisDelegation) []byte {
	return certHistoryKey(
		badgerutxo.GenesisDelegationTag, d.AddedSlot,
		ledger.PoolEventPosition(d.BlockIndex, uint32(d.CertIndex)), //nolint:gosec // a transaction holds far fewer than 2^32 certificates
	)
}

func encodeGenesisDelegation(d *models.GenesisDelegation) ([]byte, error) {
	return cbor.Encode(&genesisDelegationEnc{
		GenesisHash: d.GenesisHash,
		Delegate:    d.GenesisDelegateHash,
		Vrf:         d.VrfKeyHash,
	})
}

func decodeGenesisDelegation(k, v []byte) (models.GenesisDelegation, error) {
	var e genesisDelegationEnc
	if _, err := cbor.Decode(v, &e); err != nil {
		return models.GenesisDelegation{}, fmt.Errorf("utxoset: decode genesis delegation: %w", err)
	}
	if len(k) != 17 {
		return models.GenesisDelegation{}, fmt.Errorf("utxoset: malformed genesis delegation key %x", k)
	}
	position := binary.BigEndian.Uint64(k[9:])
	return models.GenesisDelegation{
		GenesisHash:         e.GenesisHash,
		GenesisDelegateHash: e.Delegate,
		VrfKeyHash:          e.Vrf,
		AddedSlot:           binary.BigEndian.Uint64(k[1:]),
		BlockIndex:          uint32(position >> 32),
		CertIndex:           uint(position & math.MaxUint32),
	}, nil
}

func encodeMIRCert(c ledger.MIRCert) ([]byte, error) {
	e := mirCertEnc{Pot: c.Pot, OtherPot: c.OtherPot}
	for _, r := range c.Rewards {
		if r.Amount == nil {
			return nil, fmt.Errorf("utxoset: MIR reward for %x has no delta", r.Credential)
		}
		e.Rewards = append(e.Rewards, mirRewardEnc{
			Tag: r.CredentialTag, Credential: r.Credential, Amount: r.Amount.String(),
		})
	}
	return cbor.Encode(&e)
}

func decodeMIRCert(k, v []byte) (ledger.MIRCert, error) {
	var e mirCertEnc
	if _, err := cbor.Decode(v, &e); err != nil {
		return ledger.MIRCert{}, fmt.Errorf("utxoset: decode MIR certificate: %w", err)
	}
	if len(k) != 17 {
		return ledger.MIRCert{}, fmt.Errorf("utxoset: malformed MIR certificate key %x", k)
	}
	c := ledger.MIRCert{
		Slot:     binary.BigEndian.Uint64(k[1:]),
		Position: binary.BigEndian.Uint64(k[9:]),
		Pot:      e.Pot,
		OtherPot: e.OtherPot,
		Rewards:  []models.MIRReward{},
	}
	for _, r := range e.Rewards {
		amount, ok := new(big.Int).SetString(r.Amount, 10)
		if !ok {
			return ledger.MIRCert{}, fmt.Errorf("utxoset: MIR delta %q is not a number", r.Amount)
		}
		c.Rewards = append(c.Rewards, models.MIRReward{
			Credential: r.Credential, CredentialTag: r.Tag, Amount: amount,
		})
	}
	return c, nil
}

// genesisMIRRows converts a commit's genesis delegations and MIR
// certificates to journaled rows.
func genesisMIRRows(lc ledger.LedgerChanges) ([]badgerutxo.RowPut, error) {
	out := make([]badgerutxo.RowPut, 0, len(lc.GenesisDelegations)+len(lc.MIRCerts))
	for i := range lc.GenesisDelegations {
		d := &lc.GenesisDelegations[i]
		v, err := encodeGenesisDelegation(d)
		if err != nil {
			return nil, err
		}
		out = append(out, badgerutxo.RowPut{
			Slot: d.AddedSlot, Key: genesisDelegationRowKey(d), Value: v,
		})
	}
	for _, c := range lc.MIRCerts {
		v, err := encodeMIRCert(c)
		if err != nil {
			return nil, err
		}
		out = append(out, badgerutxo.RowPut{
			Slot: c.Slot, Key: certHistoryKey(badgerutxo.MIRCertTag, c.Slot, c.Position), Value: v,
		})
	}
	return out, nil
}

// ensureGenesisMIR loads every genesis key delegation and MIR certificate
// from the metadata store into a set from before it kept them. A loaded MIR
// certificate's position is its rank in its slot, the order the store
// returns them in.
func (s *Set) ensureGenesisMIR(ctx context.Context, ldb ledgerdb.LedgerDB) error {
	loaded, err := s.store.GenesisMIRLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the genesis delegation marker: %w", err)
	}
	if loaded {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	delegs, err := ldb.GetGenesisDelegationsInSlotRange(0, math.MaxInt64, txn)
	if err != nil {
		return fmt.Errorf("utxoset: read genesis delegations: %w", err)
	}
	mirs, err := ldb.GetMIRCertsInSlotRange(ctx, 0, math.MaxInt64, txn)
	if err != nil {
		return fmt.Errorf("utxoset: read MIR certificates: %w", err)
	}
	err = s.store.LoadGenesisMIR(func(put func(k, v []byte) error) error {
		for i := range delegs {
			v, err := encodeGenesisDelegation(&delegs[i])
			if err != nil {
				return err
			}
			if err := put(genesisDelegationRowKey(&delegs[i]), v); err != nil {
				return err
			}
		}
		rank, prev := uint64(0), uint64(0)
		for i, m := range mirs {
			if i > 0 && m.AddedSlot == prev {
				rank++
			} else {
				rank = 0
			}
			prev = m.AddedSlot
			cert := ledger.MIRCert{
				Slot: m.AddedSlot, Position: rank, Pot: m.Pot, OtherPot: m.OtherPot,
				Rewards: make([]models.MIRReward, len(m.Rewards)),
			}
			copy(cert.Rewards, m.Rewards)
			// The store keeps a certificate's reward map in the order Go
			// iterated it when it wrote it.
			slices.SortFunc(cert.Rewards, func(a, b models.MIRReward) int {
				if c := cmp.Compare(a.CredentialTag, b.CredentialTag); c != 0 {
					return c
				}
				return bytes.Compare(a.Credential, b.Credential)
			})
			v, err := encodeMIRCert(cert)
			if err != nil {
				return err
			}
			if err := put(certHistoryKey(badgerutxo.MIRCertTag, cert.Slot, cert.Position), v); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("utxoset: load genesis delegations and MIR certificates: %w", err)
	}
	s.logger.Info("loaded genesis delegations and MIR certificates into the live UTxO set",
		"delegations", len(delegs), "mir_certificates", len(mirs))
	return nil
}

// GenesisMIRLoaded reports whether the set holds every genesis key
// delegation and MIR certificate.
func (s *Set) GenesisMIRLoaded() (bool, error) {
	return s.store.GenesisMIRLoaded()
}

// GenesisDelegations returns every genesis key delegation, oldest first.
func (s *Set) GenesisDelegations() ([]models.GenesisDelegation, error) {
	var out []models.GenesisDelegation
	err := s.store.Rows([]byte{badgerutxo.GenesisDelegationTag}, func(k, v []byte) error {
		d, err := decodeGenesisDelegation(k, v)
		if err != nil {
			return err
		}
		out = append(out, d)
		return nil
	})
	return out, err
}

// MIRCerts returns every MIR certificate, oldest first.
func (s *Set) MIRCerts() ([]ledger.MIRCert, error) {
	var out []ledger.MIRCert
	err := s.store.Rows([]byte{badgerutxo.MIRCertTag}, func(k, v []byte) error {
		c, err := decodeMIRCert(k, v)
		if err != nil {
			return err
		}
		out = append(out, c)
		return nil
	})
	return out, err
}
