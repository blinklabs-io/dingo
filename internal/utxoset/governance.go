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

var errMalformedGovernance = errors.New("utxoset: malformed governance entry")

func proposalPrefix(txHash []byte, actionIndex uint32) []byte {
	k := append([]byte{badgerutxo.ProposalTag}, txHash...)
	return binary.BigEndian.AppendUint32(k, actionIndex)
}

func votePrefix(txHash []byte, actionIndex uint32) []byte {
	k := append([]byte{badgerutxo.VoteTag}, txHash...)
	return binary.BigEndian.AppendUint32(k, actionIndex)
}

func voteKey(txHash []byte, actionIndex uint32, v models.GovernanceVote) []byte {
	k := append(votePrefix(txHash, actionIndex), v.VoterType, v.VoterCredentialTag)
	return append(k, v.VoterCredential...)
}

func appendOpt64(b []byte, p *uint64) []byte {
	if p == nil {
		return append(b, 0)
	}
	return binary.BigEndian.AppendUint64(append(b, 1), *p)
}

func appendOpt32(b []byte, p *uint32) []byte {
	if p == nil {
		return append(b, 0)
	}
	return binary.BigEndian.AppendUint32(append(b, 1), *p)
}

// govCursor reads the fields appendOpt64, appendOpt32 and appendLen wrote;
// the first malformed field leaves every later read zero.
type govCursor struct {
	b   []byte
	err error
}

func (c *govCursor) take(n int) []byte {
	if c.err != nil || len(c.b) < n {
		c.err = errMalformedGovernance
		return make([]byte, n)
	}
	out := c.b[:n]
	c.b = c.b[n:]
	return out
}

func (c *govCursor) u8() uint8   { return c.take(1)[0] }
func (c *govCursor) u64() uint64 { return binary.BigEndian.Uint64(c.take(8)) }
func (c *govCursor) u32() uint32 { return binary.BigEndian.Uint32(c.take(4)) }

func (c *govCursor) opt64() *uint64 {
	if c.u8() == 0 {
		return nil
	}
	v := c.u64()
	return &v
}

func (c *govCursor) opt32() *uint32 {
	if c.u8() == 0 {
		return nil
	}
	v := c.u32()
	return &v
}

// bytes returns nil for an empty field. The encoding does not tell an absent
// field from an empty one, and readers test the absent ones against nil: a
// proposal with no parent must read back with a nil ParentTxHash, as it was
// staged, or ratification takes it for a child of a missing root.
func (c *govCursor) bytes() []byte {
	n := int(c.u32())
	if n == 0 {
		return nil
	}
	return bytes.Clone(c.take(n))
}

// encodeProposal writes the proposal's metadata store row ID first, so a read
// answers with the ID the store would.
func encodeProposal(p models.GovernanceProposal) []byte {
	b := binary.BigEndian.AppendUint64(nil, uint64(p.ID))
	b = append(b, p.ActionType)
	b = binary.BigEndian.AppendUint64(b, p.ProposedEpoch)
	b = binary.BigEndian.AppendUint64(b, p.ExpiresEpoch)
	b = binary.BigEndian.AppendUint64(b, p.Deposit)
	b = binary.BigEndian.AppendUint64(b, p.AddedSlot)
	b = appendOpt32(b, p.ParentActionIdx)
	for _, o := range []*uint64{
		p.EnactedEpoch, p.EnactedSlot, p.RatifiedEpoch, p.RatifiedSlot,
		p.ExpiredEpoch, p.ExpiredSlot, p.DroppedEpoch, p.DroppedSlot,
		p.DeletedSlot,
	} {
		b = appendOpt64(b, o)
	}
	b = appendOpt32(b, p.TxIndex)
	b = appendLen(b, p.ParentTxHash)
	b = appendLen(b, p.PolicyHash)
	b = appendLen(b, []byte(p.AnchorURL))
	b = appendLen(b, p.AnchorHash)
	b = appendLen(b, p.ReturnAddress)
	return appendLen(b, p.GovActionCbor)
}

func decodeProposal(k, v []byte) (models.GovernanceProposal, error) {
	if len(k) < 5 {
		return models.GovernanceProposal{}, errMalformedGovernance
	}
	p := models.GovernanceProposal{
		TxHash:      bytes.Clone(k[1 : len(k)-4]),
		ActionIndex: binary.BigEndian.Uint32(k[len(k)-4:]),
	}
	c := govCursor{b: v}
	p.ID = uint(c.u64())
	p.ActionType = c.u8()
	p.ProposedEpoch = c.u64()
	p.ExpiresEpoch = c.u64()
	p.Deposit = c.u64()
	p.AddedSlot = c.u64()
	p.ParentActionIdx = c.opt32()
	p.EnactedEpoch, p.EnactedSlot = c.opt64(), c.opt64()
	p.RatifiedEpoch, p.RatifiedSlot = c.opt64(), c.opt64()
	p.ExpiredEpoch, p.ExpiredSlot = c.opt64(), c.opt64()
	p.DroppedEpoch, p.DroppedSlot = c.opt64(), c.opt64()
	p.DeletedSlot = c.opt64()
	p.TxIndex = c.opt32()
	p.ParentTxHash = c.bytes()
	p.PolicyHash = c.bytes()
	p.AnchorURL = string(c.bytes())
	p.AnchorHash = c.bytes()
	p.ReturnAddress = c.bytes()
	p.GovActionCbor = c.bytes()
	if c.err == nil && len(c.b) != 0 {
		c.err = errMalformedGovernance
	}
	return p, c.err
}

// encodeVote writes the vote's and its proposal's metadata store row IDs
// first.
func encodeVote(v models.GovernanceVote) []byte {
	b := binary.BigEndian.AppendUint64(nil, uint64(v.ID))
	b = binary.BigEndian.AppendUint64(b, uint64(v.ProposalID))
	b = append(b, v.Vote)
	b = binary.BigEndian.AppendUint64(b, v.AddedSlot)
	b = appendOpt64(b, v.VoteUpdatedSlot)
	b = appendOpt64(b, v.DeletedSlot)
	b = appendLen(b, []byte(v.AnchorURL))
	return appendLen(b, v.AnchorHash)
}

func decodeVote(k, v []byte) (models.GovernanceVote, error) {
	const prefix = 1 + 32 + 4
	if len(k) < prefix+2 {
		return models.GovernanceVote{}, errMalformedGovernance
	}
	out := models.GovernanceVote{
		VoterType:          k[prefix],
		VoterCredentialTag: k[prefix+1],
		VoterCredential:    bytes.Clone(k[prefix+2:]),
	}
	c := govCursor{b: v}
	out.ID = uint(c.u64())
	out.ProposalID = uint(c.u64())
	out.Vote = c.u8()
	out.AddedSlot = c.u64()
	out.VoteUpdatedSlot = c.opt64()
	out.DeletedSlot = c.opt64()
	out.AnchorURL = string(c.bytes())
	out.AnchorHash = c.bytes()
	if c.err == nil && len(c.b) != 0 {
		c.err = errMalformedGovernance
	}
	return out, c.err
}

// governanceRows converts a commit's proposal and vote puts to journaled
// rows.
func governanceRows(lc ledger.LedgerChanges) []badgerutxo.RowPut {
	out := make([]badgerutxo.RowPut, 0, len(lc.Proposals)+len(lc.Votes))
	for _, c := range lc.Proposals {
		out = append(out, badgerutxo.RowPut{
			Slot:  c.Slot,
			Key:   proposalPrefix(c.Proposal.TxHash, c.Proposal.ActionIndex),
			Value: encodeProposal(c.Proposal),
		})
	}
	for _, c := range lc.Votes {
		out = append(out, badgerutxo.RowPut{
			Slot:  c.Slot,
			Key:   voteKey(c.TxHash, c.ActionIndex, c.Vote),
			Value: encodeVote(c.Vote),
		})
	}
	return out
}

// governanceIDs returns the highest proposal and vote row IDs a commit puts,
// or nil when it puts none.
func governanceIDs(lc ledger.LedgerChanges) *badgerutxo.GovernanceIDs {
	if len(lc.Proposals) == 0 && len(lc.Votes) == 0 {
		return nil
	}
	var ids badgerutxo.GovernanceIDs
	for _, c := range lc.Proposals {
		ids.Proposal = max(ids.Proposal, uint64(c.Proposal.ID))
	}
	for _, c := range lc.Votes {
		ids.Vote = max(ids.Vote, uint64(c.Vote.ID))
	}
	return &ids
}

// NextGovernanceIDs returns the next proposal and vote row IDs, which exceed
// every ID the set has held a row under. A rollback leaves them as they are,
// so an ID is never handed out twice.
func (s *Set) NextGovernanceIDs() (proposal, vote uint64, err error) {
	ids, err := s.store.GovernanceNextIDs()
	return ids.Proposal, ids.Vote, err
}

// ensureGovernance loads every proposal and vote row from the metadata store
// into a set from before it kept them.
func (s *Set) ensureGovernance(ctx context.Context, ldb ledgerdb.LedgerDB) error {
	loaded, err := s.store.GovernanceLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the governance marker: %w", err)
	}
	if loaded {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	proposals, err := ldb.GetAllGovernanceProposals(ctx, txn)
	if err != nil {
		return fmt.Errorf("utxoset: read proposals: %w", err)
	}
	votes, err := ldb.GetAllGovernanceVotes(ctx, txn)
	if err != nil {
		return fmt.Errorf("utxoset: read votes: %w", err)
	}
	err = s.store.LoadGovernance(func(put func(k, v []byte) error) error {
		for _, p := range proposals {
			key := proposalPrefix(p.TxHash, p.ActionIndex)
			if err := put(key, encodeProposal(*p)); err != nil {
				return err
			}
		}
		for _, r := range votes {
			err := put(voteKey(r.TxHash, r.ActionIndex, r.Vote), encodeVote(r.Vote))
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("utxoset: load governance rows: %w", err)
	}
	s.logger.Info(
		"loaded governance proposals and votes into the live UTxO set",
		"proposals", len(proposals), "votes", len(votes),
	)
	return nil
}

// GovernanceLoaded reports whether the set holds every proposal and vote.
func (s *Set) GovernanceLoaded() (bool, error) {
	return s.store.GovernanceLoaded()
}

// ProposalAt returns the proposal row, soft-deleted or not, as a rollback to
// *at would leave it, or as it is now when at is nil; nil when there is none.
func (s *Set) ProposalAt(
	txHash []byte,
	actionIndex uint32,
	at *uint64,
) (*models.GovernanceProposal, error) {
	key := proposalPrefix(txHash, actionIndex)
	var v []byte
	var ok bool
	var err error
	if at == nil {
		v, ok, err = s.store.Row(key)
	} else {
		v, ok, err = s.store.RowAt(key, *at)
	}
	if err != nil || !ok {
		return nil, err
	}
	p, err := decodeProposal(key, v)
	return &p, err
}

// ProposalsAt returns every proposal row as a rollback to *at would leave
// them, or as they are now when at is nil.
func (s *Set) ProposalsAt(at *uint64) ([]models.GovernanceProposal, error) {
	var out []models.GovernanceProposal
	each := func(k, v []byte) error {
		p, err := decodeProposal(k, v)
		out = append(out, p)
		return err
	}
	var err error
	if at == nil {
		err = s.store.Rows([]byte{badgerutxo.ProposalTag}, each)
	} else {
		err = s.store.RowsAt([]byte{badgerutxo.ProposalTag}, *at, each)
	}
	return out, err
}

// VotesAt returns the vote rows on a proposal as a rollback to *at would
// leave them, or as they are now when at is nil.
func (s *Set) VotesAt(
	txHash []byte,
	actionIndex uint32,
	at *uint64,
) ([]models.GovernanceVote, error) {
	var out []models.GovernanceVote
	each := func(k, v []byte) error {
		vote, err := decodeVote(k, v)
		out = append(out, vote)
		return err
	}
	prefix := votePrefix(txHash, actionIndex)
	var err error
	if at == nil {
		err = s.store.Rows(prefix, each)
	} else {
		err = s.store.RowsAt(prefix, *at, each)
	}
	return out, err
}
