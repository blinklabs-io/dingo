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
	"context"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// memDrepGovSet is a live set that answers DRep and governance reads from
// memory.
type memDrepGovSet struct {
	memAccountSet
	dreps []models.Drep
	votes []models.GovernanceVote
}

func (*memDrepGovSet) DrepStateLoaded() (bool, error) { return true, nil }

func (s *memDrepGovSet) Drep(tag uint8, cred []byte) (*models.Drep, error) {
	for _, d := range s.dreps {
		if d.CredentialTag == tag && bytes.Equal(d.Credential, cred) {
			return &d, nil
		}
	}
	return nil, nil
}

func (s *memDrepGovSet) DrepAt(
	tag uint8,
	cred []byte,
	_ uint64,
) (*models.Drep, error) {
	return s.Drep(tag, cred)
}

func (s *memDrepGovSet) Dreps(fn func(*models.Drep) error) error {
	for _, d := range s.dreps {
		if err := fn(&d); err != nil {
			return err
		}
	}
	return nil
}

func (*memDrepGovSet) DRepLinkLive(
	_, _ models.StakeCredentialRef,
) (bool, error) {
	return false, nil
}

func (*memDrepGovSet) DRepLinkLiveAt(
	_, _ models.StakeCredentialRef,
	_ uint64,
) (bool, error) {
	return false, nil
}

func (*memDrepGovSet) DRepLinks(
	*models.StakeCredentialRef,
	func(drep, stake models.StakeCredentialRef) error,
) error {
	return nil
}

func (*memDrepGovSet) DormantEpochs() (uint64, error) { return 0, nil }

func (*memDrepGovSet) DormantEpochsAt(uint64) (uint64, error) { return 0, nil }

func (*memDrepGovSet) GovernanceLoaded() (bool, error) { return true, nil }

func (*memDrepGovSet) NextGovernanceIDs() (uint64, uint64, error) {
	return 1, 1, nil
}

func (*memDrepGovSet) ProposalAt(
	[]byte, uint32, *uint64,
) (*models.GovernanceProposal, error) {
	return nil, nil
}

func (*memDrepGovSet) ProposalsAt(
	*uint64,
) ([]models.GovernanceProposal, error) {
	return nil, nil
}

func (s *memDrepGovSet) VotesAt(
	[]byte,
	uint32,
	*uint64,
) ([]models.GovernanceVote, error) {
	return s.votes, nil
}

// storeStub is a LedgerDB whose DRep and governance reads return fixed rows.
type storeStub struct {
	ledgerdb.LedgerDB
	dreps     []*models.Drep
	votes     []*models.GovernanceVote
	proposals []*models.GovernanceProposal
}

func (s storeStub) GetActiveDreps(
	context.Context,
	ledgerdb.Txn,
) ([]*models.Drep, error) {
	return s.dreps, nil
}

func (s storeStub) GetGovernanceProposalSet(
	context.Context, ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	return s.proposals, nil
}

func (s storeStub) GetGovernanceVotes(
	context.Context, uint, ledgerdb.Txn,
) ([]*models.GovernanceVote, error) {
	return s.votes, nil
}

func newVerifyDiffLedger(
	t *testing.T,
	set *memDrepGovSet,
) (*LedgerState, *logRecords) {
	t.Helper()
	ls, _ := newDiskTestLedger(t)
	ls.metrics.init(prometheus.NewRegistry())
	logs := &logRecords{}
	ls.config.Logger = slog.New(logs)
	set.memNonceSet = memNonceSet{nonces: map[string]models.BlockNonce{}}
	ls.config.UtxoSet = set
	ls.config.UtxoSetReads = UtxoSetReadsVerify
	return ls, logs
}

// A mismatching active DRep read names the DReps only one side holds and the
// fields of those both hold that differ.
func TestActiveDrepMismatchReportNamesTheDifferingDreps(t *testing.T) {
	t.Parallel()
	same := models.Drep{
		CredentialTag: 0, Credential: bytes.Repeat([]byte{1}, 28),
		AddedSlot: 5, Active: true, LastActivityEpoch: 9, ExpiryEpoch: 29,
	}
	storeDiffers := models.Drep{
		CredentialTag: 0, Credential: bytes.Repeat([]byte{2}, 28),
		AddedSlot: 6, Active: true, LastActivityEpoch: 10, ExpiryEpoch: 30,
	}
	setDiffers := storeDiffers
	setDiffers.ExpiryEpoch = 31
	onlyStore := models.Drep{
		CredentialTag: 1, Credential: bytes.Repeat([]byte{3}, 28),
		AddedSlot: 7, Active: true, ExpiryEpoch: 40,
	}
	onlySet := models.Drep{
		CredentialTag: 0, Credential: bytes.Repeat([]byte{4}, 28),
		AddedSlot: 8, Active: true, ExpiryEpoch: 41,
	}
	set := &memDrepGovSet{dreps: []models.Drep{same, setDiffers, onlySet}}
	ls, logs := newVerifyDiffLedger(t, set)
	store := stagingLedgerDB{
		LedgerDB: storeStub{
			dreps: []*models.Drep{&same, &storeDiffers, &onlyStore},
		},
		ls: ls,
	}
	_, err := store.GetActiveDreps(t.Context(), nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, ls.drepReadMismatches.Load())
	attrs := logs.attrs(
		"live UTxO set DRep read differs from the metadata store",
	)
	require.NotNil(t, attrs, "the mismatch is logged")
	require.Equal(t, "3", attrs["store_count"])
	require.Equal(t, "3", attrs["set_count"])
	require.Equal(t,
		"0/02020202020202020202020202020202020202020202020202020202: "+
			"expiry_epoch store=30 set=31"+
			" | only in set 0/04040404040404040404040404040404040404040404040404040404"+
			" (added_slot=8 last_activity_epoch=0 expiry_epoch=41)"+
			" | only in store 1/03030303030303030303030303030303030303030303030303030303"+
			" (added_slot=7 last_activity_epoch=0 expiry_epoch=40)",
		attrs["diff"])
	require.Equal(t, "batches=0 drep_changes=0 rollbacks=0", attrs["staged"])
}

func TestActiveDrepsAgreeLogsNothing(t *testing.T) {
	t.Parallel()
	d := models.Drep{
		Credential: bytes.Repeat([]byte{1}, 28), Active: true, ExpiryEpoch: 9,
	}
	set := &memDrepGovSet{dreps: []models.Drep{d}}
	ls, logs := newVerifyDiffLedger(t, set)
	store := stagingLedgerDB{
		LedgerDB: storeStub{dreps: []*models.Drep{&d}},
		ls:       ls,
	}
	_, err := store.GetActiveDreps(t.Context(), nil)
	require.NoError(t, err)
	require.Zero(t, ls.drepReadMismatches.Load())
	require.Nil(
		t,
		logs.attrs("live UTxO set DRep read differs from the metadata store"),
	)
}

func TestDrepDiffNamesFieldsAndAbsence(t *testing.T) {
	t.Parallel()
	a := &models.Drep{Credential: []byte{1}, Active: true, ExpiryEpoch: 5}
	b := *a
	b.Active = false
	b.LastActivityEpoch = 3
	require.Empty(t, drepDiff(nil, nil))
	require.Empty(t, drepDiff(a, a))
	require.Equal(t, "store=absent set=present", drepDiff(nil, a))
	require.Equal(t, "store=present set=absent", drepDiff(a, nil))
	require.Equal(t,
		"active store=true set=false; last_activity_epoch store=0 set=3",
		drepDiff(a, &b))
}

// A mismatching vote list read names the votes only one side holds and the
// fields of those both hold that differ, store against set.
func TestVoteMismatchReportNamesTheDifferingVotes(t *testing.T) {
	t.Parallel()
	vote := func(b byte, v uint8, slot uint64) models.GovernanceVote {
		return models.GovernanceVote{
			VoterType: models.VoterTypeDRep, VoterCredential: bytes.Repeat([]byte{b}, 28),
			Vote: v, AddedSlot: slot,
		}
	}
	same := vote(1, 1, 5)
	storeDiffers := vote(2, 1, 6)
	setDiffers := vote(2, 0, 6)
	onlyStore := vote(3, 2, 7)
	onlySet := vote(4, 1, 8)
	updated := uint64(99)
	setDiffers.VoteUpdatedSlot = &updated
	hash := bytes.Repeat([]byte{0xa4}, 32)
	set := &memDrepGovSet{
		votes: []models.GovernanceVote{same, setDiffers, onlySet},
	}
	ls, logs := newVerifyDiffLedger(t, set)
	store := stagingLedgerDB{
		LedgerDB: storeStub{
			votes: []*models.GovernanceVote{
				&same,
				&storeDiffers,
				&onlyStore,
			},
			proposals: []*models.GovernanceProposal{{ID: 7, TxHash: hash}},
		},
		ls: ls,
	}
	_, err := store.GetGovernanceVotes(t.Context(), 7, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, ls.governanceReadMismatches.Load())
	attrs := logs.attrs(
		"live UTxO set governance read differs from the metadata store",
	)
	require.NotNil(t, attrs, "the mismatch is logged")
	require.Equal(t, "3", attrs["store_count"])
	require.Equal(t, "3", attrs["set_count"])
	require.Equal(
		t,
		"only in set voter 1/0/04040404040404040404040404040404040404040404040404040404"+
			" (vote=1 added_slot=8 vote_updated_slot=nil)"+
			" | only in store voter 1/0/03030303030303030303030303030303030303030303030303030303"+
			" (vote=2 added_slot=7 vote_updated_slot=nil)"+
			" | voter 1/0/02020202020202020202020202020202020202020202020202020202:"+
			" vote store=1 set=0, vote_updated_slot store=nil set=99",
		attrs["diff"],
	)
}
