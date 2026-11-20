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
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
)

// The DRep state's keys are declared in badgerutxo/keys.go.
func drepKey(tag uint8, cred []byte) []byte {
	return append([]byte{badgerutxo.DRepTag, tag}, cred...)
}

func drepLinkPrefix(drep models.StakeCredentialRef) []byte {
	k := make([]byte, 0, 3+len(drep.Key))
	k = append(k, badgerutxo.DRepLinkTag, drep.Tag, byte(len(drep.Key))) //nolint:gosec // a credential
	return append(k, drep.Key...)
}

func drepLinkKey(drep, stake models.StakeCredentialRef) []byte {
	k := drepLinkPrefix(drep)
	k = append(k, stake.Tag)
	return append(k, stake.Key...)
}

func decodeDrepLinkKey(k []byte) (drep, stake models.StakeCredentialRef, err error) {
	if len(k) < 4 || len(k) < 3+int(k[2])+1 {
		return drep, stake, errors.New("utxoset: malformed DRep link key")
	}
	n := int(k[2])
	drep = models.NewStakeCredentialRef(k[1], bytes.Clone(k[3:3+n]))
	rest := k[3+n:]
	stake = models.NewStakeCredentialRef(rest[0], bytes.Clone(rest[1:]))
	return drep, stake, nil
}

func appendLen(b, v []byte) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(v))) //nolint:gosec // an anchor
	return append(b, v...)
}

func encodeDrep(d models.Drep) []byte {
	b := []byte{0}
	if d.Active {
		b[0] = 1
	}
	b = binary.BigEndian.AppendUint64(b, d.AddedSlot)
	b = binary.BigEndian.AppendUint64(b, d.LastActivityEpoch)
	b = binary.BigEndian.AppendUint64(b, d.ExpiryEpoch)
	b = appendLen(b, []byte(d.AnchorURL))
	return appendLen(b, d.AnchorHash)
}

var errMalformedDrep = errors.New("utxoset: malformed DRep entry")

func takeLen(v []byte) ([]byte, []byte, error) {
	if len(v) < 4 {
		return nil, nil, errMalformedDrep
	}
	n := int(binary.BigEndian.Uint32(v))
	if len(v) < 4+n {
		return nil, nil, errMalformedDrep
	}
	if n == 0 {
		return nil, v[4:], nil
	}
	return bytes.Clone(v[4 : 4+n]), v[4+n:], nil
}

func decodeDrep(k, v []byte) (*models.Drep, error) {
	if len(k) < 2 || len(v) < 25 {
		return nil, errMalformedDrep
	}
	d := &models.Drep{
		CredentialTag:     k[1],
		Credential:        bytes.Clone(k[2:]),
		Active:            v[0] == 1,
		AddedSlot:         binary.BigEndian.Uint64(v[1:]),
		LastActivityEpoch: binary.BigEndian.Uint64(v[9:]),
		ExpiryEpoch:       binary.BigEndian.Uint64(v[17:]),
	}
	url, rest, err := takeLen(v[25:])
	if err != nil {
		return nil, err
	}
	d.AnchorURL = string(url)
	if d.AnchorHash, rest, err = takeLen(rest); err != nil {
		return nil, err
	}
	if len(rest) != 0 {
		return nil, errMalformedDrep
	}
	return d, nil
}

// drepRows converts a commit's DRep, link and dormancy changes to journaled
// rows.
func drepRows(lc ledger.LedgerChanges) []badgerutxo.RowPut {
	out := make([]badgerutxo.RowPut, 0,
		len(lc.DReps)+len(lc.DRepLinks)+len(lc.Dormancy)+len(lc.DRepHistory))
	for _, c := range lc.DReps {
		out = append(out, badgerutxo.RowPut{
			Slot:   c.Slot,
			Key:    drepKey(c.Drep.CredentialTag, c.Drep.Credential),
			Value:  encodeDrep(c.Drep),
			Delete: c.Delete,
		})
	}
	for _, c := range lc.DRepLinks {
		out = append(out, badgerutxo.RowPut{
			Slot:   c.Slot,
			Key:    drepLinkKey(c.Drep, c.Stake),
			Delete: !c.Live,
		})
	}
	for _, c := range lc.Dormancy {
		out = append(out, badgerutxo.RowPut{
			Slot:  c.Slot,
			Key:   badgerutxo.DormancyKey,
			Value: binary.BigEndian.AppendUint64(nil, c.Epochs),
		})
	}
	for _, c := range lc.DRepHistory {
		out = append(out, drepHistoryRow(c))
	}
	return out
}

func drepHistoryRow(c ledger.DRepHistoryChange) badgerutxo.RowPut {
	row := badgerutxo.RowPut{Slot: c.Slot}
	switch c.Kind {
	case ledger.DRepHistoryExpiry:
		row.Key = badgerutxo.DRepExpiryHistoryKey(c.Slot, c.Tag, c.Key)
		row.Value = badgerutxo.EncodeDRepExpiryHistoryValue(c.Activity, c.Expiry)
	case ledger.DRepHistoryDormancy:
		row.Key = badgerutxo.DRepDormancyHistoryKey(c.Slot, c.Previous)
	case ledger.DRepHistoryEvent:
		row.Key = badgerutxo.DRepExpiryEventKey(c.Slot)
	case ledger.DRepHistoryClear:
		row.Key = badgerutxo.DRepClearKey(c.Slot, c.Tag, c.Key)
	}
	return row
}

// ensureDrepState loads the metadata store's DReps, live delegator links
// and dormancy counter into a set from before it kept them.
func (s *Set) ensureDrepState(ctx context.Context, ldb ledgerdb.LedgerDB) error {
	loaded, err := s.store.DrepStateLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the DRep state marker: %w", err)
	}
	if loaded {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	dormant, err := ldb.GetDormantDRepEpochs(ctx, txn)
	if err != nil {
		return fmt.Errorf("utxoset: read DRep dormancy: %w", err)
	}
	dreps, links := 0, 0
	err = s.store.LoadDrepState(func(put func(k, v []byte) error) error {
		if err := ldb.IterateDreps(txn, func(d *models.Drep) error {
			dreps++
			k := drepKey(d.CredentialTag, d.Credential)
			s.merge.noteDrep(k)
			if s.merge != nil && s.mergeOverDrained.Load() {
				// The metadata store lost the rows a drain moved out; an
				// import of a DRep the ledger already holds keeps the slot
				// it was first written at.
				held, err := s.Drep(d.CredentialTag, d.Credential)
				if err != nil {
					return err
				}
				if held != nil {
					d.AddedSlot = held.AddedSlot
				}
			}
			return put(k, encodeDrep(*d))
		}); err != nil {
			return err
		}
		if err := ldb.IterateLiveDrepDelegators(txn,
			func(drep, stake models.StakeCredentialRef) error {
				links++
				return put(drepLinkKey(drep, stake), nil)
			}); err != nil {
			return err
		}
		return put(badgerutxo.DormancyKey, binary.BigEndian.AppendUint64(nil, dormant))
	})
	if err != nil {
		return fmt.Errorf("utxoset: load DRep state: %w", err)
	}
	s.logger.Info("loaded DRep state into the live UTxO set",
		"dreps", dreps, "delegator_links", links, "dormant_epochs", dormant)
	return nil
}

// DrepStateLoaded reports whether the set holds every DRep, live delegator
// link and the dormancy counter.
func (s *Set) DrepStateLoaded() (bool, error) {
	return s.store.DrepStateLoaded()
}

func (s *Set) drepFrom(k, v []byte, ok bool, err error) (*models.Drep, error) {
	if err != nil || !ok {
		return nil, err
	}
	return decodeDrep(k, v)
}

// Drep returns the DRep of (tag, cred), active or not, or nil.
func (s *Set) Drep(tag uint8, cred []byte) (*models.Drep, error) {
	k := drepKey(tag, cred)
	v, ok, err := s.store.Row(k)
	return s.drepFrom(k, v, ok, err)
}

// DrepAt returns the DRep of (tag, cred) as a rollback to slot would leave
// it, or nil.
func (s *Set) DrepAt(tag uint8, cred []byte, slot uint64) (*models.Drep, error) {
	k := drepKey(tag, cred)
	v, ok, err := s.store.RowAt(k, slot)
	return s.drepFrom(k, v, ok, err)
}

// Dreps calls fn with every DRep the set holds.
// DrepsAfter calls fn with each DRep whose key is strictly after the DRep key
// after (every DRep when nil), in key order, with that key, until fn returns
// ledger.ErrStopRows.
func (s *Set) DrepsAfter(
	after []byte,
	fn func(d *models.Drep, key []byte) error,
) error {
	return s.store.EachRowAfter(
		[]byte{badgerutxo.DRepTag}, after, nil,
		func(k, v []byte) error {
			d, err := decodeDrep(k, v)
			if err != nil {
				return err
			}
			return stopRows(fn(d, bytes.Clone(k)))
		},
	)
}

// DrepsChangedIn is AccountsChangedIn for DReps.
func (s *Set) DrepsChangedIn(
	from, through uint64,
	after []byte,
	maxVisit int,
	fn func(d *models.Drep, slot uint64, pos []byte) error,
) (next []byte, covered, done bool, err error) {
	return s.store.EachRowChangedIn(badgerutxo.DRepTag, from, through, after,
		maxVisit, func(slot uint64, key, value, pos []byte) error {
			d, err := decodeDrep(key, value)
			if err != nil {
				return err
			}
			return stopRows(fn(d, slot, bytes.Clone(pos)))
		})
}

func (s *Set) Dreps(fn func(*models.Drep) error) error {
	return s.store.Rows([]byte{badgerutxo.DRepTag}, func(k, v []byte) error {
		d, err := decodeDrep(k, v)
		if err != nil {
			return err
		}
		return fn(d)
	})
}

// DRepLinkLive reports whether stake's delegator link to drep is live.
func (s *Set) DRepLinkLive(drep, stake models.StakeCredentialRef) (bool, error) {
	_, ok, err := s.store.Row(drepLinkKey(drep, stake))
	return ok, err
}

// DRepLinkLiveAt reports whether the link is live as a rollback to slot
// would leave it.
func (s *Set) DRepLinkLiveAt(
	drep, stake models.StakeCredentialRef,
	slot uint64,
) (bool, error) {
	_, ok, err := s.store.RowAt(drepLinkKey(drep, stake), slot)
	return ok, err
}

// DRepLinks calls fn with every live delegator link, of drep alone when it
// is not nil.
func (s *Set) DRepLinks(
	drep *models.StakeCredentialRef,
	fn func(drep, stake models.StakeCredentialRef) error,
) error {
	prefix := []byte{badgerutxo.DRepLinkTag}
	if drep != nil {
		prefix = drepLinkPrefix(*drep)
	}
	return s.store.Rows(prefix, func(k, _ []byte) error {
		d, st, err := decodeDrepLinkKey(k)
		if err != nil {
			return err
		}
		return fn(d, st)
	})
}

// DormantEpochs returns the DRep dormancy counter.
func (s *Set) DormantEpochs() (uint64, error) {
	v, ok, err := s.store.Row(badgerutxo.DormancyKey)
	if err != nil || !ok {
		return 0, err
	}
	if len(v) != 8 {
		return 0, errors.New("utxoset: malformed dormancy counter")
	}
	return binary.BigEndian.Uint64(v), nil
}

// DormantEpochsAt returns the counter as a rollback to slot would leave it.
func (s *Set) DormantEpochsAt(slot uint64) (uint64, error) {
	v, ok, err := s.store.RowAt(badgerutxo.DormancyKey, slot)
	if err != nil || !ok {
		return 0, err
	}
	if len(v) != 8 {
		return 0, errors.New("utxoset: malformed dormancy counter")
	}
	return binary.BigEndian.Uint64(v), nil
}
