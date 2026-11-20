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
	"math/big"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
)

var errMalformedCommittee = errors.New("utxoset: malformed committee entry")

func memberKey(m models.CommitteeMember) []byte {
	k := binary.BigEndian.AppendUint64([]byte{badgerutxo.CommitteeMemberTag}, m.AddedSlot)
	k = append(k, m.ColdCredentialTag)
	return append(k, m.ColdCredHash...)
}

const (
	memberTermSet  = 1
	memberDeleted  = 2
	constDeletedFl = 1
)

func encodeMember(m models.CommitteeMember) []byte {
	v := binary.BigEndian.AppendUint64(nil, m.ExpiresEpoch)
	v = binary.BigEndian.AppendUint64(v, m.TermStartSlot)
	var flags byte
	if m.TermStartSlotSet {
		flags |= memberTermSet
	}
	var deleted uint64
	if m.DeletedSlot != nil {
		flags |= memberDeleted
		deleted = *m.DeletedSlot
	}
	v = append(v, flags)
	return binary.BigEndian.AppendUint64(v, deleted)
}

func decodeMember(k, v []byte) (models.CommitteeMember, error) {
	if len(k) < 10 || len(v) != 25 {
		return models.CommitteeMember{}, errMalformedCommittee
	}
	m := models.CommitteeMember{
		AddedSlot:         binary.BigEndian.Uint64(k[1:]),
		ColdCredentialTag: k[9],
		ColdCredHash:      bytes.Clone(k[10:]),
		ExpiresEpoch:      binary.BigEndian.Uint64(v),
		TermStartSlot:     binary.BigEndian.Uint64(v[8:]),
		TermStartSlotSet:  v[16]&memberTermSet != 0,
	}
	if v[16]&memberDeleted != 0 {
		d := binary.BigEndian.Uint64(v[17:])
		m.DeletedSlot = &d
	}
	return m, nil
}

func slotRowKey(tag byte, slot uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{tag}, slot)
}

func encodeQuorum(q *big.Rat) []byte {
	if q == nil {
		return []byte{0}
	}
	return append([]byte{1}, q.String()...)
}

func decodeQuorum(k, v []byte) (ledger.CommitteeQuorumRow, error) {
	if len(k) != 9 || len(v) < 1 {
		return ledger.CommitteeQuorumRow{}, errMalformedCommittee
	}
	row := ledger.CommitteeQuorumRow{Slot: binary.BigEndian.Uint64(k[1:])}
	if v[0] == 1 {
		q, ok := new(big.Rat).SetString(string(v[1:]))
		if !ok {
			return row, errMalformedCommittee
		}
		row.Quorum = q
	}
	return row, nil
}

func encodeConstitution(c models.Constitution) []byte {
	var flags byte
	var deleted uint64
	if c.DeletedSlot != nil {
		flags = constDeletedFl
		deleted = *c.DeletedSlot
	}
	v := binary.BigEndian.AppendUint64([]byte{flags}, deleted)
	v = appendLen(v, []byte(c.AnchorURL))
	v = appendLen(v, c.AnchorHash)
	return appendLen(v, c.PolicyHash)
}

func decodeConstitution(k, v []byte) (models.Constitution, error) {
	if len(k) != 9 || len(v) < 9 {
		return models.Constitution{}, errMalformedCommittee
	}
	c := models.Constitution{AddedSlot: binary.BigEndian.Uint64(k[1:])}
	if v[0]&constDeletedFl != 0 {
		d := binary.BigEndian.Uint64(v[1:])
		c.DeletedSlot = &d
	}
	url, rest, err := takeLen(v[9:])
	if err != nil {
		return c, err
	}
	c.AnchorURL = string(url)
	if c.AnchorHash, rest, err = takeLen(rest); err != nil {
		return c, err
	}
	if c.PolicyHash, rest, err = takeLen(rest); err != nil {
		return c, err
	}
	if len(rest) != 0 {
		return c, errMalformedCommittee
	}
	return c, nil
}

func coldRowKey(tag byte, cold models.AuthCommitteeHot) []byte {
	return append([]byte{tag, cold.ColdCredentialTag}, cold.ColdCredential...)
}

func encodeAuth(a models.AuthCommitteeHot) []byte {
	v := binary.BigEndian.AppendUint64([]byte{a.HotCredentialTag}, a.AddedSlot)
	return append(v, a.HotCredential...)
}

func decodeAuth(k, v []byte) (models.AuthCommitteeHot, error) {
	if len(k) < 2 || len(v) < 9 {
		return models.AuthCommitteeHot{}, errMalformedCommittee
	}
	return models.AuthCommitteeHot{
		ColdCredentialTag: k[1],
		ColdCredential:    bytes.Clone(k[2:]),
		HotCredentialTag:  v[0],
		HotCredential:     bytes.Clone(v[9:]),
		AddedSlot:         binary.BigEndian.Uint64(v[1:]),
	}, nil
}

func resignRowKey(r models.ResignCommitteeCold) []byte {
	return append([]byte{badgerutxo.ResignTag, r.ColdCredentialTag}, r.ColdCredential...)
}

func decodeResign(k, v []byte) (models.ResignCommitteeCold, error) {
	if len(k) < 2 || len(v) != 8 {
		return models.ResignCommitteeCold{}, errMalformedCommittee
	}
	return models.ResignCommitteeCold{
		ColdCredentialTag: k[1],
		ColdCredential:    bytes.Clone(k[2:]),
		AddedSlot:         binary.BigEndian.Uint64(v),
	}, nil
}

// committeeRows converts a commit's committee and constitution puts to
// journaled rows.
func committeeRows(lc ledger.LedgerChanges) []badgerutxo.RowPut {
	out := make([]badgerutxo.RowPut, 0, len(lc.Committee))
	for _, c := range lc.Committee {
		put := badgerutxo.RowPut{Slot: c.Slot}
		switch {
		case c.Member != nil:
			put.Key, put.Value = memberKey(*c.Member), encodeMember(*c.Member)
		case c.Quorum != nil:
			put.Key = slotRowKey(badgerutxo.CommitteeQuorumTag, c.Quorum.Slot)
			put.Value = encodeQuorum(c.Quorum.Quorum)
		case c.Constitution != nil:
			put.Key = slotRowKey(badgerutxo.ConstitutionTag, c.Constitution.AddedSlot)
			put.Value = encodeConstitution(*c.Constitution)
		case c.Auth != nil:
			put.Key = coldRowKey(badgerutxo.HotAuthTag, *c.Auth)
			put.Value = encodeAuth(*c.Auth)
		case c.Resign != nil:
			put.Key = resignRowKey(*c.Resign)
			put.Value = binary.BigEndian.AppendUint64(nil, c.Resign.AddedSlot)
		default:
			continue
		}
		out = append(out, put)
	}
	return out
}

// ensureCommittee loads the committee and constitution rows from the
// metadata store into a set from before it kept them.
func (s *Set) ensureCommittee(ctx context.Context, ldb ledgerdb.LedgerDB) error {
	loaded, err := s.store.CommitteeLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the committee marker: %w", err)
	}
	if loaded {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	members, err := ldb.GetCommitteeMembersIncludeDeleted(ctx, txn)
	if err != nil {
		return fmt.Errorf("utxoset: read committee members: %w", err)
	}
	quorum, err := ldb.LatestCommitteeQuorum(ctx, txn)
	if err != nil {
		return fmt.Errorf("utxoset: read committee quorum: %w", err)
	}
	constitution, err := ldb.GetConstitution(txn)
	if err != nil {
		return fmt.Errorf("utxoset: read constitution: %w", err)
	}
	auths, err := ldb.GetCommitteeHotAuthorizationsSince(ctx, 0, txn)
	if err != nil {
		return fmt.Errorf("utxoset: read hot authorizations: %w", err)
	}
	resigns, err := ldb.LatestCommitteeResignations(ctx, txn)
	if err != nil {
		return fmt.Errorf("utxoset: read resignations: %w", err)
	}
	err = s.store.LoadCommittee(func(put func(k, v []byte) error) error {
		for _, m := range members {
			if err := put(memberKey(*m), encodeMember(*m)); err != nil {
				return err
			}
		}
		if quorum != nil {
			var q *big.Rat
			if quorum.Quorum != nil {
				q = quorum.Quorum.Rat
			}
			err := put(slotRowKey(badgerutxo.CommitteeQuorumTag, quorum.AddedSlot), encodeQuorum(q))
			if err != nil {
				return err
			}
		}
		if constitution != nil {
			err := put(
				slotRowKey(badgerutxo.ConstitutionTag, constitution.AddedSlot),
				encodeConstitution(*constitution),
			)
			if err != nil {
				return err
			}
		}
		for _, a := range auths {
			if err := put(coldRowKey(badgerutxo.HotAuthTag, *a), encodeAuth(*a)); err != nil {
				return err
			}
		}
		for _, r := range resigns {
			err := put(resignRowKey(*r), binary.BigEndian.AppendUint64(nil, r.AddedSlot))
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("utxoset: load committee rows: %w", err)
	}
	s.logger.Info("loaded committee rows into the live UTxO set", "members", len(members))
	return nil
}

// CommitteeLoaded reports whether the set holds the committee and
// constitution rows.
func (s *Set) CommitteeLoaded() (bool, error) {
	return s.store.CommitteeLoaded()
}

// CommitteeStateAt returns the committee and constitution rows as a
// rollback to *at would leave them, or as they are now when at is nil. The
// set keeps history only from its load and within its journal.
func (s *Set) CommitteeStateAt(at *uint64) (*ledger.CommitteeState, error) {
	st := &ledger.CommitteeState{}
	each := func(tag byte, fn func(k, v []byte) error) error {
		if at == nil {
			return s.store.Rows([]byte{tag}, fn)
		}
		return s.store.RowsAt([]byte{tag}, *at, fn)
	}
	err := each(badgerutxo.CommitteeMemberTag, func(k, v []byte) error {
		m, err := decodeMember(k, v)
		st.Members = append(st.Members, m)
		return err
	})
	if err == nil {
		err = each(badgerutxo.CommitteeQuorumTag, func(k, v []byte) error {
			q, err := decodeQuorum(k, v)
			st.Quorums = append(st.Quorums, q)
			return err
		})
	}
	if err == nil {
		err = each(badgerutxo.ConstitutionTag, func(k, v []byte) error {
			c, err := decodeConstitution(k, v)
			st.Constitutions = append(st.Constitutions, c)
			return err
		})
	}
	if err == nil {
		err = each(badgerutxo.HotAuthTag, func(k, v []byte) error {
			a, err := decodeAuth(k, v)
			st.Auths = append(st.Auths, a)
			return err
		})
	}
	if err == nil {
		err = each(badgerutxo.ResignTag, func(k, v []byte) error {
			r, err := decodeResign(k, v)
			st.Resigns = append(st.Resigns, r)
			return err
		})
	}
	if err != nil {
		return nil, err
	}
	return st, nil
}
