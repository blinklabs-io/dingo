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
	"math/big"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// CommitteeQuorumRow is a committee quorum as the metadata store records it
// at Slot: nil Quorum is the marker that no quorum is in effect.
type CommitteeQuorumRow struct {
	Slot   uint64
	Quorum *big.Rat
}

// CommitteeState is the constitutional committee and constitution rows the
// metadata store keeps, without their row IDs:
//
//   - Members are every committee member row, soft-deleted ones included,
//     by (added slot, tag, hash).
//   - Quorums and Constitutions are every row, by slot.
//   - Auths holds each cold credential's latest hot-key authorization, and
//     Resigns its latest resignation: the only rows any read selects.
type CommitteeState struct {
	Members       []models.CommitteeMember
	Quorums       []CommitteeQuorumRow
	Constitutions []models.Constitution
	Auths         []models.AuthCommitteeHot
	Resigns       []models.ResignCommitteeCold
}

// CommitteeChange is one row put: exactly one of its rows is set. Slot is
// the slot a rollback to before it undoes the put.
type CommitteeChange struct {
	Slot         uint64
	Member       *models.CommitteeMember
	Quorum       *CommitteeQuorumRow
	Constitution *models.Constitution
	Auth         *models.AuthCommitteeHot
	Resign       *models.ResignCommitteeCold
}

// CommitteeReader serves the committee and constitution rows from a live
// UTxO set that computes them.
type CommitteeReader interface {
	CommitteeLoaded() (bool, error)
	// CommitteeStateAt returns the rows as a rollback to *at would leave
	// them, or as they are now when at is nil.
	CommitteeStateAt(at *uint64) (*CommitteeState, error)
}

func (ls *LedgerState) committeeReader(txn ledgerdb.Txn) CommitteeReader {
	if ls.accountReader(txn) == nil || ls.utxoSetFailed.Load() {
		return nil
	}
	r, ok := ls.readSet(txn).(CommitteeReader)
	if !ok {
		return nil
	}
	return r
}

// Apply puts the row c names.
func (st *CommitteeState) Apply(c CommitteeChange) {
	switch {
	case c.Member != nil:
		m := *c.Member
		i := slices.IndexFunc(st.Members, func(o models.CommitteeMember) bool {
			return o.ColdCredentialTag == m.ColdCredentialTag &&
				bytes.Equal(o.ColdCredHash, m.ColdCredHash) &&
				o.AddedSlot == m.AddedSlot
		})
		if i >= 0 {
			st.Members[i] = m
			return
		}
		st.Members = append(st.Members, m)
		slices.SortFunc(st.Members, compareMembers)
	case c.Quorum != nil:
		q := *c.Quorum
		i := slices.IndexFunc(st.Quorums, func(o CommitteeQuorumRow) bool {
			return o.Slot == q.Slot
		})
		if i >= 0 {
			st.Quorums[i] = q
			return
		}
		st.Quorums = append(st.Quorums, q)
		slices.SortFunc(st.Quorums, func(a, b CommitteeQuorumRow) int {
			return cmp.Compare(a.Slot, b.Slot)
		})
	case c.Constitution != nil:
		k := *c.Constitution
		i := slices.IndexFunc(st.Constitutions, func(o models.Constitution) bool {
			return o.AddedSlot == k.AddedSlot
		})
		if i >= 0 {
			st.Constitutions[i] = k
			return
		}
		st.Constitutions = append(st.Constitutions, k)
		slices.SortFunc(st.Constitutions, func(a, b models.Constitution) int {
			return cmp.Compare(a.AddedSlot, b.AddedSlot)
		})
	case c.Auth != nil:
		a := *c.Auth
		i := slices.IndexFunc(st.Auths, func(o models.AuthCommitteeHot) bool {
			return o.ColdCredentialTag == a.ColdCredentialTag &&
				bytes.Equal(o.ColdCredential, a.ColdCredential)
		})
		if i >= 0 {
			st.Auths[i] = a
			return
		}
		st.Auths = append(st.Auths, a)
	case c.Resign != nil:
		r := *c.Resign
		i := slices.IndexFunc(st.Resigns, func(o models.ResignCommitteeCold) bool {
			return o.ColdCredentialTag == r.ColdCredentialTag &&
				bytes.Equal(o.ColdCredential, r.ColdCredential)
		})
		if i >= 0 {
			st.Resigns[i] = r
			return
		}
		st.Resigns = append(st.Resigns, r)
	}
}

func compareMembers(a, b models.CommitteeMember) int {
	return cmp.Or(
		cmp.Compare(a.AddedSlot, b.AddedSlot),
		cmp.Compare(a.ColdCredentialTag, b.ColdCredentialTag),
		bytes.Compare(a.ColdCredHash, b.ColdCredHash),
	)
}

func (st *CommitteeState) latestAuth(tag uint8, cold []byte) *models.AuthCommitteeHot {
	for i := range st.Auths {
		if st.Auths[i].ColdCredentialTag == tag &&
			bytes.Equal(st.Auths[i].ColdCredential, cold) {
			return &st.Auths[i]
		}
	}
	return nil
}

func (st *CommitteeState) latestResign(tag uint8, cold []byte) *models.ResignCommitteeCold {
	for i := range st.Resigns {
		if st.Resigns[i].ColdCredentialTag == tag &&
			bytes.Equal(st.Resigns[i].ColdCredential, cold) {
			return &st.Resigns[i]
		}
	}
	return nil
}

// ResignedSince reports a resignation of the cold credential at or after
// termStart.
func (st *CommitteeState) ResignedSince(tag uint8, cold []byte, termStart uint64) bool {
	r := st.latestResign(tag, cold)
	return r != nil && r.AddedSlot >= termStart
}

// AuthSince returns the cold credential's latest hot-key authorization when
// it was recorded at or after termStart.
func (st *CommitteeState) AuthSince(tag uint8, cold []byte, termStart uint64) *models.AuthCommitteeHot {
	a := st.latestAuth(tag, cold)
	if a == nil || a.AddedSlot < termStart {
		return nil
	}
	return a
}

// AuthsSince returns every cold credential's latest hot-key authorization
// recorded at or after minSlot.
func (st *CommitteeState) AuthsSince(minSlot uint64) []models.AuthCommitteeHot {
	out := make([]models.AuthCommitteeHot, 0, len(st.Auths))
	for _, a := range st.Auths {
		if a.AddedSlot >= minSlot {
			out = append(out, a)
		}
	}
	return out
}

// ActiveMembers returns the latest authorization of each seated committee
// member that was made in its term and has not resigned in it.
func (st *CommitteeState) ActiveMembers() []models.AuthCommitteeHot {
	out := make([]models.AuthCommitteeHot, 0)
	for _, m := range st.Members {
		if m.DeletedSlot != nil {
			continue
		}
		a := st.AuthSince(m.ColdCredentialTag, m.ColdCredHash, m.TermStartSlot)
		if a != nil && !st.ResignedSince(m.ColdCredentialTag, m.ColdCredHash, m.TermStartSlot) {
			out = append(out, *a)
		}
	}
	return out
}

// SeatedMembers returns the members that are not soft-deleted.
func (st *CommitteeState) SeatedMembers() []models.CommitteeMember {
	out := make([]models.CommitteeMember, 0, len(st.Members))
	for _, m := range st.Members {
		if m.DeletedSlot == nil {
			out = append(out, m)
		}
	}
	return out
}

// LatestQuorum returns the quorum of the highest-slot row, nil when that row
// clears it or there is none.
func (st *CommitteeState) LatestQuorum() *big.Rat {
	if len(st.Quorums) == 0 {
		return nil
	}
	return st.Quorums[len(st.Quorums)-1].Quorum
}

// CurrentConstitution returns the highest-slot constitution that is not
// deleted.
func (st *CommitteeState) CurrentConstitution() *models.Constitution {
	for _, c := range slices.Backward(st.Constitutions) {
		if c.DeletedSlot == nil {
			return &c
		}
	}
	return nil
}

// committeeView returns the committee rows as txn sees the set: the set's
// rows under the rollbacks and puts of the batches txn sees, in order. A
// staged rollback drops the staged puts after it and reads the set as the
// rollback will leave it.
func (ls *LedgerState) committeeView(
	r CommitteeReader,
	txn ledgerdb.Txn,
) (*CommitteeState, error) {
	var ceiling *uint64
	var changes []CommitteeChange
	for _, o := range ls.stagedOps(txn) {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			if ceiling == nil || to < *ceiling {
				ceiling = &to
			}
			changes = slices.DeleteFunc(changes, func(c CommitteeChange) bool {
				return c.Slot > to
			})
		}
		changes = append(changes, o.committee...)
	}
	st, err := r.CommitteeStateAt(ceiling)
	if err != nil {
		return nil, err
	}
	for _, c := range changes {
		st.Apply(c)
	}
	return st, nil
}

// committeeServed returns the committee rows as txn sees the set, or an
// error when the set cannot serve them. Row IDs are left zero: no committee read consumes one
// except as a tie-break between rows of one credential that the set never
// holds at once, and the set orders member rows by (added slot, tag, hash)
// where the store orders them by insertion, which no consumer depends on.
func (ls *LedgerState) committeeServed(
	txn ledgerdb.Txn,
) (*CommitteeState, error) {
	r := ls.committeeReader(txn)
	if r == nil || !ls.setServes(&ls.committeeSetLoaded, r.CommitteeLoaded) {
		return &CommitteeState{}, drainedUnserved(
			nodesettings.FamilyCommitteeConstitution,
		)
	}
	return ls.committeeView(r, txn)
}

func pointersTo[T any](rows []T) []*T {
	out := make([]*T, 0, len(rows))
	for i := range rows {
		out = append(out, &rows[i])
	}
	return out
}

// stageCommittee stages the puts build makes against the rows as txn sees
// them, each put seen by the ones after it.
func (ls *LedgerState) stageCommittee(
	txn ledgerdb.Txn,
	build func(st *CommitteeState, put func(CommitteeChange)),
) {
	r := ls.committeeReader(txn)
	if r == nil {
		// The set is the only copy, so a change it cannot stage would be
		// lost.
		ls.failAccountStaging(txn,
			drainedUnserved(nodesettings.FamilyCommitteeConstitution),
		)
		return
	}
	st, err := ls.committeeView(r, txn)
	if err != nil {
		ls.failAccountStaging(txn, err)
		return
	}
	var out []CommitteeChange
	build(st, func(c CommitteeChange) {
		st.Apply(c)
		out = append(out, c)
	})
	if len(out) > 0 {
		ls.stageLedgerChanges(txn, LedgerChanges{Committee: out}) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
	}
}

func cleanMember(m models.CommitteeMember) models.CommitteeMember {
	m.ID = 0
	m.ColdCredHash = bytes.Clone(m.ColdCredHash)
	if m.DeletedSlot != nil {
		d := *m.DeletedSlot
		m.DeletedSlot = &d
	}
	return m
}

// softDeleteMembers soft-deletes at slot every member row that matches and
// is not deleted, as the metadata store does.
func softDeleteMembers(
	st *CommitteeState,
	put func(CommitteeChange),
	match func(models.CommitteeMember) bool,
	slot uint64,
) {
	for _, m := range slices.Clone(st.Members) {
		if m.DeletedSlot != nil || !match(m) {
			continue
		}
		m = cleanMember(m)
		m.DeletedSlot = &slot
		put(CommitteeChange{Slot: slot, Member: &m})
	}
}

func (s stagingLedgerDB) SetCommitteeMembers(
	ctx context.Context,
	members []*models.CommitteeMember,
	txn ledgerdb.Txn,
) error {
	s.ls.stageCommittee(txn, func(st *CommitteeState, put func(CommitteeChange)) { //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		for _, m := range members {
			if m == nil {
				continue
			}
			softDeleteMembers(st, put, func(o models.CommitteeMember) bool {
				return o.ColdCredentialTag == m.ColdCredentialTag &&
					bytes.Equal(o.ColdCredHash, m.ColdCredHash)
			}, m.AddedSlot)
			row := cleanMember(*m)
			if !row.TermStartSlotSet {
				row.TermStartSlot = row.AddedSlot
				row.TermStartSlotSet = true
			}
			put(CommitteeChange{Slot: row.AddedSlot, Member: &row})
		}
	})
	return nil
}

func (s stagingLedgerDB) SoftDeleteCommitteeMembers(
	ctx context.Context,
	coldCredentials []models.CommitteeCredential,
	slot uint64,
	txn ledgerdb.Txn,
) error {
	s.ls.stageCommittee(txn, func(st *CommitteeState, put func(CommitteeChange)) { //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		for _, c := range coldCredentials {
			softDeleteMembers(st, put, func(o models.CommitteeMember) bool {
				return o.ColdCredentialTag == c.CredentialTag &&
					bytes.Equal(o.ColdCredHash, c.Credential)
			}, slot)
		}
	})
	return nil
}

func (s stagingLedgerDB) SoftDeleteAllCommitteeMembers(
	ctx context.Context,
	slot uint64,
	txn ledgerdb.Txn,
) error {
	s.ls.stageCommittee(txn, func(st *CommitteeState, put func(CommitteeChange)) { //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		softDeleteMembers(st, put, func(models.CommitteeMember) bool { return true }, slot)
	})
	return nil
}

func (s stagingLedgerDB) SetCommitteeQuorum(
	ctx context.Context,
	quorum *big.Rat,
	slot uint64,
	txn ledgerdb.Txn,
) error {
	s.ls.stageCommittee(txn, func(_ *CommitteeState, put func(CommitteeChange)) { //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		put(CommitteeChange{Slot: slot, Quorum: &CommitteeQuorumRow{
			Slot: slot, Quorum: new(big.Rat).Set(quorum),
		}})
	})
	return nil
}

func (s stagingLedgerDB) ClearCommitteeQuorum(
	ctx context.Context,
	slot uint64,
	txn ledgerdb.Txn,
) error {
	s.ls.stageCommittee(txn, func(_ *CommitteeState, put func(CommitteeChange)) { //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		put(CommitteeChange{Slot: slot, Quorum: &CommitteeQuorumRow{Slot: slot}})
	})
	return nil
}

func (s stagingLedgerDB) SetConstitution(
	constitution *models.Constitution,
	txn ledgerdb.Txn,
) error {
	s.ls.stageCommittee(txn, func(_ *CommitteeState, put func(CommitteeChange)) { //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		c := *constitution
		c.ID = 0
		c.AnchorHash = bytes.Clone(c.AnchorHash)
		c.PolicyHash = bytes.Clone(c.PolicyHash)
		if c.DeletedSlot != nil {
			d := *c.DeletedSlot
			c.DeletedSlot = &d
		}
		put(CommitteeChange{Slot: c.AddedSlot, Constitution: &c})
	})
	return nil
}

// stageCommitteeCertificate stages a committee hot-key authorization or
// resignation as the metadata store records it: a credential's latest row
// is the one with the highest slot, so a certificate at an earlier slot than
// the latest changes nothing. isCommittee is false for any other
// certificate.
func (ls *LedgerState) stageCommitteeCertificate(
	txn ledgerdb.Txn,
	cert lcommon.Certificate,
	slot uint64,
) (isCommittee bool, err error) {
	var stage func(*CommitteeState, func(CommitteeChange))
	switch c := cert.(type) {
	case *lcommon.AuthCommitteeHotCertificate:
		coldTag, err := models.CredentialTagFromUint(c.ColdCredential.CredType)
		if err != nil {
			return true, err
		}
		hotTag, err := models.CredentialTagFromUint(c.HotCredential.CredType)
		if err != nil {
			return true, err
		}
		stage = func(st *CommitteeState, put func(CommitteeChange)) {
			cold := c.ColdCredential.Credential[:]
			if prev := st.latestAuth(coldTag, cold); prev != nil && slot < prev.AddedSlot {
				return
			}
			put(CommitteeChange{Slot: slot, Auth: &models.AuthCommitteeHot{
				ColdCredentialTag: coldTag,
				ColdCredential:    bytes.Clone(cold),
				HotCredentialTag:  hotTag,
				HotCredential:     bytes.Clone(c.HotCredential.Credential[:]),
				AddedSlot:         slot,
			}})
		}
	case *lcommon.ResignCommitteeColdCertificate:
		coldTag, err := models.CredentialTagFromUint(c.ColdCredential.CredType)
		if err != nil {
			return true, err
		}
		stage = func(st *CommitteeState, put func(CommitteeChange)) {
			cold := c.ColdCredential.Credential[:]
			if prev := st.latestResign(coldTag, cold); prev != nil && slot < prev.AddedSlot {
				return
			}
			put(CommitteeChange{Slot: slot, Resign: &models.ResignCommitteeCold{
				ColdCredentialTag: coldTag,
				ColdCredential:    bytes.Clone(cold),
				AddedSlot:         slot,
			}})
		}
	default:
		return false, nil
	}
	ls.stageCommittee(txn, stage)
	return true, nil
}

func sameRat(a, b *big.Rat) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Cmp(b) == 0
}

func samePtr(a, b *uint64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func sameConstitution(a, b *models.Constitution) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.AnchorURL == b.AnchorURL &&
		bytes.Equal(a.AnchorHash, b.AnchorHash) &&
		bytes.Equal(a.PolicyHash, b.PolicyHash) &&
		a.AddedSlot == b.AddedSlot &&
		samePtr(a.DeletedSlot, b.DeletedSlot)
}

// sameMembers compares member rows without their IDs. The metadata store
// orders them by insertion; the set by (added slot, tag, hash), so both are
// ordered as the set records them.
func sameMembers(want []*models.CommitteeMember, got []models.CommitteeMember) bool {
	rows := make([]models.CommitteeMember, 0, len(want))
	for _, m := range want {
		rows = append(rows, cleanMember(*m))
	}
	slices.SortFunc(rows, compareMembers)
	return slices.EqualFunc(rows, got, func(a, b models.CommitteeMember) bool {
		return a.ColdCredentialTag == b.ColdCredentialTag &&
			bytes.Equal(a.ColdCredHash, b.ColdCredHash) &&
			a.ExpiresEpoch == b.ExpiresEpoch &&
			a.TermStartSlot == b.TermStartSlot &&
			a.TermStartSlotSet == b.TermStartSlotSet &&
			a.AddedSlot == b.AddedSlot &&
			samePtr(a.DeletedSlot, b.DeletedSlot)
	})
}

func sameAuth(a, b *models.AuthCommitteeHot) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.ColdCredentialTag == b.ColdCredentialTag &&
		bytes.Equal(a.ColdCredential, b.ColdCredential) &&
		a.HotCredentialTag == b.HotCredentialTag &&
		bytes.Equal(a.HotCredential, b.HotCredential) &&
		a.AddedSlot == b.AddedSlot
}

// sameAuths compares authorizations without their IDs, by cold credential.
func sameAuths(want []*models.AuthCommitteeHot, got []models.AuthCommitteeHot) bool {
	byCold := func(a, b *models.AuthCommitteeHot) int {
		return cmp.Or(
			cmp.Compare(a.ColdCredentialTag, b.ColdCredentialTag),
			bytes.Compare(a.ColdCredential, b.ColdCredential),
		)
	}
	w := slices.Clone(want)
	slices.SortFunc(w, byCold)
	g := make([]*models.AuthCommitteeHot, 0, len(got))
	for i := range got {
		g = append(g, &got[i])
	}
	slices.SortFunc(g, byCold)
	return slices.EqualFunc(w, g, sameAuth)
}

func (s stagingLedgerDB) GetConstitution(
	txn ledgerdb.Txn,
) (*models.Constitution, error) {
	st, err := s.ls.committeeServed(txn)
	if err != nil {
		return nil, err
	}
	return st.CurrentConstitution(), nil
}

func (s stagingLedgerDB) GetCommitteeQuorum(
	ctx context.Context,
	txn ledgerdb.Txn,
) (*big.Rat, error) {
	st, err := s.ls.committeeServed(txn)
	if err != nil {
		return nil, err
	}
	return st.LatestQuorum(), nil
}

// latestQuorumRow is the row LatestCommitteeQuorum reports for the set's
// highest-slot quorum row: a clear marker has a nil Quorum.
func (st *CommitteeState) latestQuorumRow() *models.CommitteeQuorum {
	if len(st.Quorums) == 0 {
		return nil
	}
	last := st.Quorums[len(st.Quorums)-1]
	row := &models.CommitteeQuorum{AddedSlot: last.Slot}
	if last.Quorum != nil {
		row.Quorum = &types.Rat{Rat: new(big.Rat).Set(last.Quorum)}
	}
	return row
}

// LatestCommitteeQuorum returns the highest-slot quorum row, including a
// cleared quorum.
func (s stagingLedgerDB) LatestCommitteeQuorum(
	ctx context.Context,
	txn ledgerdb.Txn,
) (*models.CommitteeQuorum, error) {
	st, err := s.ls.committeeServed(txn)
	if err != nil {
		return nil, err
	}
	return st.latestQuorumRow(), nil
}

// LatestCommitteeResignations returns each cold credential's latest
// resignation slot.
func (s stagingLedgerDB) LatestCommitteeResignations(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.ResignCommitteeCold, error) {
	st, err := s.ls.committeeServed(txn)
	if err != nil {
		return nil, err
	}
	return pointersTo(slices.Clone(st.Resigns)), nil
}

func (s stagingLedgerDB) GetCommitteeMembers(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.CommitteeMember, error) {
	st, err := s.ls.committeeServed(txn)
	if err != nil {
		return nil, err
	}
	seated := st.SeatedMembers()
	return pointersTo(seated), nil
}

func (s stagingLedgerDB) GetCommitteeMembersIncludeDeleted(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.CommitteeMember, error) {
	st, err := s.ls.committeeServed(txn)
	if err != nil {
		return nil, err
	}
	return pointersTo(st.Members), nil
}

func (s stagingLedgerDB) GetCommitteeMember(
	ctx context.Context,
	coldCredentialTag uint8,
	coldKey []byte,
	termStartSlot uint64,
	txn ledgerdb.Txn,
) (*models.AuthCommitteeHot, error) {
	st, err := s.ls.committeeServed(txn)
	if err != nil {
		return nil, err
	}
	a := st.AuthSince(coldCredentialTag, coldKey, termStartSlot)
	if a == nil {
		return nil, models.ErrCommitteeMemberNotFound
	}
	return a, nil
}

func (s stagingLedgerDB) IsCommitteeMemberResigned(
	ctx context.Context,
	coldCredentialTag uint8,
	coldKey []byte,
	termStartSlot uint64,
	txn ledgerdb.Txn,
) (bool, error) {
	st, err := s.ls.committeeServed(txn)
	if err != nil {
		return false, err
	}
	return st.ResignedSince(coldCredentialTag, coldKey, termStartSlot), nil
}

func (s stagingLedgerDB) GetResignedCommitteeMembers(
	ctx context.Context,
	coldCredentials []models.CommitteeCredential,
	txn ledgerdb.Txn,
) (map[string]bool, error) {
	st, err := s.ls.committeeServed(txn)
	if err != nil {
		return nil, err
	}
	got := make(map[string]bool)
	for _, c := range coldCredentials {
		if st.ResignedSince(c.CredentialTag, c.Credential, c.TermStartSlot) {
			got[c.Key()] = true
		}
	}
	return got, nil
}

func (s stagingLedgerDB) GetActiveCommitteeMembers(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.AuthCommitteeHot, error) {
	st, err := s.ls.committeeServed(txn)
	if err != nil {
		return nil, err
	}
	active := st.ActiveMembers()
	return pointersTo(active), nil
}

func (s stagingLedgerDB) GetCommitteeAuthorizedCount(
	ctx context.Context,
	txn ledgerdb.Txn,
) (int, error) {
	st, err := s.ls.committeeServed(txn)
	if err != nil {
		return 0, err
	}
	return len(st.ActiveMembers()), nil
}

func (s stagingLedgerDB) GetCommitteeHotAuthorizationsSince(
	ctx context.Context,
	minSlot uint64,
	txn ledgerdb.Txn,
) ([]*models.AuthCommitteeHot, error) {
	st, err := s.ls.committeeServed(txn)
	if err != nil {
		return nil, err
	}
	auths := st.AuthsSince(minSlot)
	return pointersTo(auths), nil
}
