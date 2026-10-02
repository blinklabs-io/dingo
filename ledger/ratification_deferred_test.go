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
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// holdRatificationApply stops the scenario's ratification jobs between
// deciding and writing until the returned release runs.
func holdRatificationApply(
	t *testing.T,
	s *govDiffScenario,
) (held <-chan uint64, release func()) {
	t.Helper()
	ch := make(chan uint64, 8)
	gate := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	s.ls.ratificationApplyHook = func(epoch uint64) {
		ch <- epoch
		<-gate
	}
	return ch, release
}

func requireHeld(t *testing.T, held <-chan uint64, epoch uint64) {
	t.Helper()
	select {
	case got := <-held:
		require.Equal(t, epoch, got)
	case <-time.After(10 * time.Second):
		t.Fatalf("ratification job for epoch %d never decided", epoch)
	}
}

func (s *govDiffScenario) proposalByMarker(
	t *testing.T,
	marker byte,
) *models.GovernanceProposal {
	t.Helper()
	p, err := s.db.GetGovernanceProposal(
		context.Background(),
		repeatByte(32, marker),
		0,
		nil,
	)
	require.NoError(t, err)
	return p
}

// A job that has decided but not written by the next boundary must not lose
// the decision: that boundary writes it before ENACT reads it, and the job's
// own write then finds nothing to do.
func TestNextBoundaryWritesUndeliveredRatification(t *testing.T) {
	t.Parallel()

	s := newGovDiffScenario(t)
	held, release := holdRatificationApply(t, s)
	s.run(t, 1, func(*LedgerState) {})
	requireHeld(t, held, 742)
	require.Nil(t, s.proposalByMarker(t, 0x71).RatifiedEpoch)

	s.run(t, 1, func(*LedgerState) {})
	update := s.proposalByMarker(t, 0x71)
	require.NotNil(t, update.EnactedEpoch,
		"the boundary after an undelivered decision did not enact it")
	require.Equal(t, uint64(743), *update.EnactedEpoch)
	require.Equal(t, uint64(74_200), *update.RatifiedSlot)

	release()
	require.NoError(t, s.ls.WaitEpochBoundaryJob(t.Context()))
	rec, err := loadPendingRatification(s.db, nil)
	require.NoError(t, err)
	require.Nil(t, rec)
	require.Equal(t, uint64(743), *s.proposalByMarker(t, 0x71).EnactedEpoch)
}

// Readers of the boundary's marks wait for them to be durable.
func TestWaitEpochBoundaryJobBlocksUntilDurable(t *testing.T) {
	t.Parallel()

	s := newGovDiffScenario(t)
	held, release := holdRatificationApply(t, s)
	s.run(t, 1, func(*LedgerState) {})
	requireHeld(t, held, 742)

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(
		t, s.ls.WaitEpochBoundaryJob(cancelled), context.Canceled,
		"a reader went ahead before the decision was written",
	)

	release()
	require.NoError(t, s.ls.WaitEpochBoundaryJob(t.Context()))
	require.NotNil(t, s.proposalByMarker(t, 0x71).RatifiedEpoch)
}

func queryProposals(
	t *testing.T,
	ls *LedgerState,
) olocalstatequery.ProposalsResult {
	t.Helper()
	result, err := ls.queryShelleyGetProposals(nil)
	require.NoError(t, err)
	wrapped, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, wrapped, 1)
	proposals, ok := wrapped[0].(olocalstatequery.ProposalsResult)
	require.True(t, ok)
	return proposals
}

// LSQ GetProposals returns the Conway proposals set: an action RATIFY just
// classified expired, and its child, stay in it until the boundary that
// drops them.
func TestGetProposalsReturnsTheProposalsSet(t *testing.T) {
	t.Parallel()

	s := newGovDiffScenario(t)
	s.run(t, 1, func(ls *LedgerState) {
		require.NoError(t, ls.WaitEpochBoundaryJob(t.Context()))
	})
	require.NotNil(t, s.proposalByMarker(t, 0x77).ExpiredEpoch)

	proposals := queryProposals(t, s.ls)
	require.Len(t, proposals, 5, "expired members missing from GetProposals")

	s.run(t, 1, func(ls *LedgerState) {
		require.NoError(t, ls.WaitEpochBoundaryJob(t.Context()))
	})
	proposals = queryProposals(t, s.ls)
	// The committee update was enacted; the lapsed action and its child
	// were dropped.
	require.Len(t, proposals, 2)
}

// A rollback below the pending boundary discards its decision; one that keeps
// the boundary keeps it.
func TestRollbackDiscardsRatificationOfRemovedBoundary(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		rollbackTo uint64
		keeps      bool
	}{
		{"below the boundary", 74_199, false},
		{"within the epoch", 74_200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newGovDiffScenario(t)
			held, release := holdRatificationApply(t, s)
			s.run(t, 1, func(*LedgerState) {})
			requireHeld(t, held, 742)

			txn := s.db.Transaction(context.Background(), true)
			require.NoError(t, txn.Do(func(txn *database.Txn) error {
				return s.ls.discardPendingRatificationAfterSlot(
					txn, tc.rollbackTo,
				)
			}))
			release()
			if tc.keeps {
				require.NoError(
					t, s.ls.WaitEpochBoundaryJob(t.Context()),
				)
				require.NotNil(t, s.proposalByMarker(t, 0x71).RatifiedEpoch)
				return
			}
			require.NoError(t, s.ls.WaitEpochBoundaryJob(t.Context()))
			// The job's own write runs after release; let it finish.
			s.ls.ratificationWG.Wait()
			rec, err := loadPendingRatification(s.db, nil)
			require.NoError(t, err)
			require.Nil(t, rec)
			require.Nil(t, s.proposalByMarker(t, 0x71).RatifiedEpoch,
				"a rolled-back boundary's decision was written")
		})
	}
}

func seedResumeBlocks(
	t *testing.T,
	db *database.Database,
	firstSlot uint64,
	count int,
) {
	t.Helper()
	txn := db.BlobTxn(true)
	var prev []byte
	for i := range count {
		hash := make([]byte, 32)
		binary.BigEndian.PutUint64(hash, uint64(i)+1)
		require.NoError(t, db.BlockCreate(models.Block{
			Slot:     firstSlot + uint64(i),
			Number:   uint64(i) + 1,
			Hash:     hash,
			PrevHash: prev,
			Cbor:     []byte{0x80},
		}, txn))
		prev = hash
	}
	require.NoError(t, txn.Commit())
}

// A restart with an undecided boundary records a rewind below it; one too far
// back for the rollback intent fails start-up instead.
func TestResumePendingRatificationRewindsOrFails(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		blocks int
		fails  bool
	}{
		{"within the intent limit", 4, false},
		{"beyond the intent limit", maxRollbackIntentBlocks + 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			const boundary = 1_000
			seedResumeBlocks(t, db, boundary-1, tc.blocks)
			raw, err := json.Marshal(pendingRatificationRecord{
				Epoch: 10, BoundarySlot: boundary, ID: 1,
			})
			require.NoError(t, err)
			require.NoError(t, db.SetSyncState(
				pendingRatificationSyncKey, string(raw), nil,
			))
			ls := &LedgerState{db: db, config: LedgerStateConfig{
				Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			}}
			err = ls.resumePendingRatificationIntent(context.Background())
			if tc.fails {
				require.ErrorIs(t, err, errRollbackIntentTooLarge)
				require.ErrorContains(t, err, "resync required")
				return
			}
			require.NoError(t, err)
			point, blocks, pending, err := loadRollbackIntent(db)
			require.NoError(t, err)
			require.True(t, pending)
			require.Equal(t, uint64(boundary-1), point.Slot)
			require.Len(t, blocks, tc.blocks-1)
		})
	}
}

// The ledger rollback transaction discards a pending ratification whose
// boundary it removes, and keeps one it does not.
func TestLedgerRollbackDiscardsPendingRatification(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		above uint64
		keeps bool
	}{
		{"boundary above the rollback point", 1, false},
		{"boundary at the rollback point", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			ls := fixture.ls
			rec := pendingRatificationRecord{
				Epoch:        1,
				BoundarySlot: fixture.ancestorTip.Point.Slot + tc.above,
				ID:           7,
			}
			raw, err := json.Marshal(rec)
			require.NoError(t, err)
			require.NoError(t, ls.db.SetSyncState(
				pendingRatificationSyncKey, string(raw), nil,
			))
			job := &ratificationJob{
				record:  rec,
				decided: make(chan struct{}),
				settled: make(chan struct{}),
			}
			ls.ratificationMu.Lock()
			ls.ratificationJob = job
			ls.ratificationMu.Unlock()

			require.NoError(
				t,
				ls.rollback(context.Background(), fixture.ancestorTip.Point),
			)

			stored, err := loadPendingRatification(ls.db, nil)
			require.NoError(t, err)
			if tc.keeps {
				require.Equal(t, &rec, stored)
				return
			}
			require.Nil(t, stored, "rollback kept a removed boundary's record")
			select {
			case <-job.settled:
			default:
				t.Fatal("rollback left readers waiting on a discarded decision")
			}
		})
	}
}

// Ledger-state queries wait for the boundary job, so none answers from a
// state missing the boundary's mark snapshot or RATIFY marks.
func TestLedgerStateQueryWaitsForBoundaryJob(t *testing.T) {
	t.Parallel()

	s := newGovDiffScenario(t)
	wireDeferredBoundarySnapshot(s.ls, s.snapshotMgr)
	held, release := holdRatificationApply(t, s)
	s.run(t, 1, func(*LedgerState) {})
	requireHeld(t, held, 742)

	answered := make(chan error, 1)
	go func() {
		_, err := s.ls.Query(&olocalstatequery.BlockQuery{}, QueryPoint{})
		answered <- err
	}()
	require.Never(t, func() bool { return len(answered) > 0 },
		300*time.Millisecond, 10*time.Millisecond,
		"a ledger-state query answered before the boundary job wrote")
	rows, err := s.db.GetPoolStakeSnapshotsByEpoch(742, "mark", nil)
	require.NoError(t, err)
	require.Empty(t, rows, "the boundary wrote the mark snapshot itself")

	release()
	select {
	case <-answered:
	case <-time.After(10 * time.Second):
		t.Fatal("query still waiting after the boundary job wrote")
	}
	rows, err = s.db.GetPoolStakeSnapshotsByEpoch(742, "mark", nil)
	require.NoError(t, err)
	require.NotEmpty(t, rows)
}

// This file uses only APIs that predate deferred ratification, so the same
// scenario can run on an older tree and its dumps be compared.

type govDiffProposalDump struct {
	ID           string  `json:"id"`
	RatifiedAt   *uint64 `json:"ratified_slot"`
	RatifiedIn   *uint64 `json:"ratified_epoch"`
	EnactedAt    *uint64 `json:"enacted_slot"`
	EnactedIn    *uint64 `json:"enacted_epoch"`
	ExpiredAt    *uint64 `json:"expired_slot"`
	ExpiredIn    *uint64 `json:"expired_epoch"`
	DroppedAt    *uint64 `json:"dropped_slot"`
	DroppedIn    *uint64 `json:"dropped_epoch"`
	DeletedAfter *uint64 `json:"deleted_slot"`
}

type govDiffEpochDump struct {
	Epoch        uint64                `json:"epoch"`
	Treasury     uint64                `json:"treasury"`
	Reserves     uint64                `json:"reserves"`
	Rewards      map[string]uint64     `json:"rewards"`
	Proposals    []govDiffProposalDump `json:"proposals"`
	Committee    map[string]uint64     `json:"committee"`
	PParams      string                `json:"pparams"`
	Mark         map[string]uint64     `json:"mark"`
	MarkRows     []string              `json:"mark_rows"`
	Summary      string                `json:"summary"`
	StakeInputs  []string              `json:"stake_inputs"`
	DRepPower    uint64                `json:"drep_power"`
	DepositPower map[string]uint64     `json:"deposit_power"`
}

type govDiffScenario struct {
	ls          *LedgerState
	db          *database.Database
	epoch       models.Epoch
	pparams     lcommon.ProtocolParameters
	proposals   []*models.GovernanceProposal
	credentials [][]byte
	drep        []byte
	snapshotMgr *snapshot.Manager
}

const govDiffEpochLength = 100

// newGovDiffScenario seeds a Conway ledger with a committee update, a
// parameter change and a treasury withdrawal that all pass their votes, plus
// an action about to expire with a child. The committee update is a delaying
// action, so the three ratify and enact across different boundaries.
func newGovDiffScenario(t *testing.T) *govDiffScenario {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	const startEpoch = 741
	epoch := newTestEpoch(
		startEpoch, startEpoch*govDiffEpochLength, govDiffEpochLength,
		eras.ConwayEraDesc.Id,
	)
	require.NoError(t, db.SetEpoch(
		epoch.StartSlot, epoch.EpochId, epoch.Nonce, epoch.EvolvingNonce,
		epoch.CandidateNonce, epoch.LastEpochBlockNonce, epoch.EraId,
		epoch.SlotLength, epoch.LengthInSlots, nil,
	))
	require.NoError(t, db.Metadata().SetNetworkState(
		1_000_000, 10_000_000, epoch.StartSlot, nil,
	))
	pparams := donationTestConwayPParams(10)
	pparams.MinCommitteeSize = 1
	pparams.CommitteeTermLimit = 100
	pparams.NOpt = 500
	pparams.A0 = &cbor.Rat{Rat: big.NewRat(3, 10)}
	pparams.Rho = &cbor.Rat{Rat: big.NewRat(3, 1000)}
	pparams.Tau = &cbor.Rat{Rat: big.NewRat(2, 10)}

	seedLiveDelegatedStake(t, db, hfrLiveYesPool, 6_283)
	seedLiveDelegatedStake(t, db, hfrLiveSilentPool, 3_717)

	s := &govDiffScenario{db: db, epoch: epoch, pparams: pparams}
	s.seedRetiringPool(t, startEpoch+1)
	require.NoError(t, db.Metadata().AddNetworkDonation(
		epoch.StartSlot+5, startEpoch, 5_000, nil,
	))
	account := func(
		marker byte,
		drep []byte,
		pool string,
		utxo uint64,
	) []byte {
		cred := repeatByte(28, marker)
		var poolKey []byte
		if pool != "" {
			poolKey = []byte(pool)
		}
		require.NoError(
			t,
			db.CreateAccount(context.Background(), nil, &models.Account{
				StakingKey: cred,
				Drep:       drep,
				Pool:       poolKey,
				AddedSlot:  epoch.StartSlot,
				Active:     true,
				Reward:     types.Uint64(0),
			}),
		)
		if utxo > 0 {
			require.NoError(
				t,
				db.CreateUtxo(context.Background(), nil, &models.Utxo{
					TxId:       repeatByte(32, marker),
					OutputIdx:  0,
					StakingKey: cred,
					Amount:     types.Uint64(utxo),
					AddedSlot:  epoch.StartSlot,
				}),
			)
		}
		s.credentials = append(s.credentials, cred)
		return cred
	}
	rewardAddress := func(cred []byte) (lcommon.Address, []byte) {
		addr, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeNoneKey, lcommon.AddressNetworkTestnet,
			nil, cred,
		)
		require.NoError(t, err)
		raw, err := addr.Bytes()
		require.NoError(t, err)
		return addr, raw
	}

	s.drep = repeatByte(28, 0xe1)
	require.NoError(t, db.CreateDrep(context.Background(), nil, &models.Drep{
		Credential:  s.drep,
		AddedSlot:   epoch.StartSlot,
		ExpiryEpoch: startEpoch + 100,
		Active:      true,
	}))
	// Every post-SNAP credit below lands on an account delegated to a pool,
	// so a mark snapshot read after the boundary without excluding them
	// differs from the SNAP point.
	account(0xe2, s.drep, "", 50_000)
	returnCred := account(0xe3, s.drep, hfrLiveYesPool, 0)
	_, returnAddr := rewardAddress(returnCred)
	payee := account(0xe4, nil, hfrLiveSilentPool, 0)
	payeeAddr, _ := rewardAddress(payee)

	coldCredential := repeatByte(28, 0xd1)
	hotCredential := repeatByte(28, 0xd2)
	require.NoError(
		t,
		db.SetCommitteeMembers(context.Background(), []*models.CommitteeMember{{
			ColdCredHash: coldCredential,
			ExpiresEpoch: startEpoch + 100,
			AddedSlot:    1,
		}}, nil),
	)
	require.NoError(
		t,
		db.SetCommitteeQuorum(context.Background(), big.NewRat(1, 1), 1, nil),
	)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO auth_committee_hot (
    cold_credential, host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?)`, coldCredential, hotCredential, 1, 1)
	require.NoError(t, err)

	newMember := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(repeatByte(28, 0xd3)),
	}
	committeeUpdate, err := lcommon.NewUpdateCommitteeGovAction(
		nil, nil,
		map[*lcommon.Credential]uint64{&newMember: startEpoch + 50},
		cbor.Rat{Rat: big.NewRat(1, 1)},
	)
	require.NoError(t, err)
	minFeeA := uint(77)
	parameterChange := &conway.ConwayParameterChangeGovAction{
		Type: uint(lcommon.GovActionTypeParameterChange),
		ParamUpdate: conway.ConwayProtocolParameterUpdate{
			MinFeeA: &minFeeA,
		},
	}
	withdrawal := &lcommon.TreasuryWithdrawalGovAction{
		Type:        uint(lcommon.GovActionTypeTreasuryWithdrawal),
		Withdrawals: map[*lcommon.Address]uint64{&payeeAddr: 1_000},
	}
	expiringFee := uint(55)
	expiring := &conway.ConwayParameterChangeGovAction{
		Type: uint(lcommon.GovActionTypeParameterChange),
		ParamUpdate: conway.ConwayProtocolParameterUpdate{
			MinFeeA: &expiringFee,
		},
	}
	addProposal := func(
		marker byte,
		actionType lcommon.GovActionType,
		action any,
		expires uint64,
		parent *models.GovernanceProposal,
	) *models.GovernanceProposal {
		encoded, err := cbor.Encode(action)
		require.NoError(t, err)
		proposal := &models.GovernanceProposal{
			TxHash:        repeatByte(32, marker),
			ActionType:    uint8(actionType),
			ProposedEpoch: startEpoch - 1,
			ExpiresEpoch:  expires,
			AnchorURL:     "https://example.invalid/diff",
			AnchorHash:    repeatByte(32, marker+1),
			Deposit:       100,
			ReturnAddress: returnAddr,
			GovActionCbor: encoded,
			AddedSlot:     epoch.StartSlot - 10 + uint64(len(s.proposals)),
		}
		if parent != nil {
			idx := parent.ActionIndex
			proposal.ParentTxHash = parent.TxHash
			proposal.ParentActionIdx = &idx
		}
		require.NoError(
			t,
			db.SetGovernanceProposal(context.Background(), proposal, nil),
		)
		loaded, err := db.GetGovernanceProposal(
			context.Background(),
			proposal.TxHash,
			0,
			nil,
		)
		require.NoError(t, err)
		s.proposals = append(s.proposals, loaded)
		return loaded
	}
	vote := func(
		p *models.GovernanceProposal,
		voterType uint8,
		credential []byte,
	) {
		require.NoError(
			t,
			db.SetGovernanceVote(context.Background(), &models.GovernanceVote{
				ProposalID:      p.ID,
				VoterType:       voterType,
				VoterCredential: credential,
				Vote:            models.VoteYes,
				AddedSlot:       epoch.StartSlot - 5,
			}, nil),
		)
	}
	update := addProposal(0x71, lcommon.GovActionTypeUpdateCommittee,
		committeeUpdate, startEpoch+10, nil)
	vote(update, models.VoterTypeDRep, s.drep)
	vote(update, models.VoterTypeSPO, []byte(hfrLiveYesPool))
	change := addProposal(0x73, lcommon.GovActionTypeParameterChange,
		parameterChange, startEpoch+10, nil)
	vote(change, models.VoterTypeCC, hotCredential)
	vote(change, models.VoterTypeDRep, s.drep)
	vote(change, models.VoterTypeSPO, []byte(hfrLiveYesPool))
	payout := addProposal(0x75, lcommon.GovActionTypeTreasuryWithdrawal,
		withdrawal, startEpoch+10, nil)
	vote(payout, models.VoterTypeCC, hotCredential)
	vote(payout, models.VoterTypeDRep, s.drep)
	lapsing := addProposal(0x77, lcommon.GovActionTypeParameterChange,
		expiring, startEpoch, nil)
	addProposal(0x79, lcommon.GovActionTypeParameterChange,
		expiring, startEpoch+10, lapsing)

	cfg := epochBoundaryBenchNodeConfig(t)
	s.ls = &LedgerState{
		db:             db,
		currentEra:     eras.ConwayEraDesc,
		currentEpoch:   epoch,
		currentPParams: pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	snapshotMgr := snapshot.NewManager(db, event.NewEventBus(nil, nil), nil)
	s.snapshotMgr = snapshotMgr
	// Production reads the SNAP-point stake inside the boundary.
	s.ls.SetEpochBoundarySnapshotStakeHook(
		func(txn *database.Txn, evt event.EpochTransitionEvent) error {
			return snapshotMgr.ComputeEpochBoundarySnapshot(
				context.Background(), txn, evt,
			)
		},
	)
	s.ls.SetEpochBoundarySnapshotHook(
		func(txn *database.Txn, evt event.EpochTransitionEvent) error {
			return snapshotMgr.CaptureEpochBoundarySnapshot(
				context.Background(), txn, evt,
			)
		},
	)
	s.ls.SetCurrentBoundarySPOStakeHook(
		func(
			txn *database.Txn,
			evt event.EpochTransitionEvent,
		) ([]*models.PoolStakeSnapshot, error) {
			return snapshotMgr.CurrentBoundarySPOStakeRows(
				context.Background(), txn, evt,
			)
		},
	)
	return s
}

// run rolls the scenario over boundaries boundaries, calling settle after
// each commit, and dumps the ledger state after each.
func (s *govDiffScenario) run(
	t *testing.T,
	boundaries int,
	settle func(*LedgerState),
) []govDiffEpochDump {
	t.Helper()
	dumps := make([]govDiffEpochDump, 0, boundaries)
	epoch, pparams := s.epoch, s.pparams
	for range boundaries {
		// The reward application reads the performance epoch's parameters
		// from the table a real node writes every boundary.
		stored, err := s.db.GetPParams(
			epoch.EpochId, eras.ConwayEraDesc.Id,
			eras.ConwayEraDesc.DecodePParamsFunc, nil,
		)
		require.NoError(t, err)
		if stored == nil {
			encoded, err := cbor.Encode(&pparams)
			require.NoError(t, err)
			require.NoError(t, s.db.SetPParams(
				encoded, epoch.StartSlot, epoch.EpochId,
				eras.ConwayEraDesc.Id, nil,
			))
		}
		var result *EpochRolloverResult
		txn := s.db.Transaction(context.Background(), true)
		require.NoError(t, txn.Do(func(txn *database.Txn) error {
			var err error
			result, err = s.ls.processEpochRollover(
				txn, epoch, eras.ConwayEraDesc, pparams, false,
			)
			return err
		}))
		require.NotNil(t, result)
		settle(s.ls)
		epoch, pparams = result.NewCurrentEpoch, result.NewCurrentPParams
		s.ls.currentEpoch = epoch
		s.ls.currentPParams = pparams
		dumps = append(dumps, s.dump(t, epoch.EpochId, pparams))
	}
	s.epoch, s.pparams = epoch, pparams
	return dumps
}

func (s *govDiffScenario) dump(
	t *testing.T,
	epoch uint64,
	pparams lcommon.ProtocolParameters,
) govDiffEpochDump {
	t.Helper()
	treasury, reserves, _ := networkState(t, s.db)
	d := govDiffEpochDump{
		Epoch:     epoch,
		Treasury:  treasury,
		Reserves:  reserves,
		Rewards:   map[string]uint64{},
		Committee: map[string]uint64{},
		Mark:      map[string]uint64{},
	}
	for _, cred := range s.credentials {
		account, err := s.db.GetAccountByCredential(
			context.Background(),
			0,
			cred,
			true,
			nil,
		)
		require.NoError(t, err)
		require.NotNil(t, account)
		d.Rewards[hex.EncodeToString(cred[:4])] = uint64(account.Reward)
	}
	for _, p := range s.proposals {
		loaded, err := s.db.GetGovernanceProposal(
			context.Background(),
			p.TxHash,
			p.ActionIndex,
			nil,
		)
		require.NoError(t, err)
		d.Proposals = append(d.Proposals, govDiffProposalDump{
			ID:           hex.EncodeToString(p.TxHash[:2]),
			RatifiedAt:   loaded.RatifiedSlot,
			RatifiedIn:   loaded.RatifiedEpoch,
			EnactedAt:    loaded.EnactedSlot,
			EnactedIn:    loaded.EnactedEpoch,
			ExpiredAt:    loaded.ExpiredSlot,
			ExpiredIn:    loaded.ExpiredEpoch,
			DroppedAt:    loaded.DroppedSlot,
			DroppedIn:    loaded.DroppedEpoch,
			DeletedAfter: loaded.DeletedSlot,
		})
	}
	members, err := s.db.GetCommitteeMembers(context.Background(), nil)
	require.NoError(t, err)
	for _, m := range members {
		d.Committee[hex.EncodeToString(m.ColdCredHash[:4])] = m.ExpiresEpoch
	}
	encoded, err := cbor.Encode(pparams)
	require.NoError(t, err)
	d.PParams = hex.EncodeToString(encoded)
	rows, err := s.db.GetPoolStakeSnapshotsByEpoch(epoch, "mark", nil)
	require.NoError(t, err)
	for _, row := range rows {
		d.Mark[hex.EncodeToString(row.PoolKeyHash)] = uint64(row.TotalStake)
	}
	for _, row := range rows {
		d.MarkRows = append(d.MarkRows, fmt.Sprintf(
			"%x stake=%d delegators=%d slot=%d autovote=%d/%t version=%d",
			row.PoolKeyHash, row.TotalStake, row.DelegatorCount,
			row.CapturedSlot, row.RewardAccountAutoVote,
			row.RewardAccountAutoVoteResolved, row.CalculationVersion,
		))
	}
	sort.Strings(d.MarkRows)
	summary, err := s.db.Metadata().GetEpochSummary(epoch, nil)
	require.NoError(t, err)
	if summary != nil {
		d.Summary = fmt.Sprintf(
			"active=%d pools=%d delegators=%d boundary=%d nonce=%x ready=%t",
			summary.TotalActiveStake, summary.TotalPoolCount,
			summary.TotalDelegators, summary.BoundarySlot, summary.EpochNonce,
			summary.SnapshotReady,
		)
	}
	d.StakeInputs = s.stakeInputRows(t, epoch)
	d.DRepPower, err = s.db.GetDRepVotingPower(
		context.Background(),
		0,
		s.drep,
		0,
		nil,
	)
	require.NoError(t, err)
	deposits, _, err := governance.ActiveProposalDepositDRepPower(
		context.Background(),
		s.db,
		nil,
		epoch,
		0,
	)
	require.NoError(t, err)
	d.DepositPower = deposits
	return d
}

// seedRetiringPool registers a pool whose retirement takes effect at
// retireEpoch, with one delegator and a deposit refundable to a registered
// reward account.
func (s *govDiffScenario) seedRetiringPool(t *testing.T, retireEpoch uint64) {
	t.Helper()
	pool := repeatByte(28, 0xb1)
	owner := repeatByte(28, 0xb2)
	delegator := repeatByte(28, 0xb3)
	require.NoError(t, s.db.ImportPool(context.Background(), nil, &models.Pool{
		PoolKeyHash:   pool,
		VrfKeyHash:    make([]byte, 32),
		Pledge:        1_000_000,
		Cost:          340_000_000,
		Margin:        &types.Rat{Rat: big.NewRat(1, 100)},
		RewardAccount: owner,
	}, &models.PoolRegistration{
		PoolKeyHash:   pool,
		AddedSlot:     s.epoch.StartSlot,
		Pledge:        1_000_000,
		Cost:          340_000_000,
		Margin:        &types.Rat{Rat: big.NewRat(1, 100)},
		VrfKeyHash:    make([]byte, 32),
		RewardAccount: owner,
		DepositAmount: types.Uint64(500),
	}))
	for _, account := range []*models.Account{
		{StakingKey: owner, Pool: []byte(hfrLiveYesPool)},
		{StakingKey: delegator, Pool: pool},
	} {
		account.AddedSlot = s.epoch.StartSlot
		account.Active = true
		require.NoError(
			t,
			s.db.CreateAccount(context.Background(), nil, account),
		)
		s.credentials = append(s.credentials, account.StakingKey)
	}
	require.NoError(t, s.db.CreateUtxo(context.Background(), nil, &models.Utxo{
		TxId:       repeatByte(32, 0xb4),
		OutputIdx:  0,
		StakingKey: delegator,
		Amount:     types.Uint64(2_222),
		AddedSlot:  s.epoch.StartSlot,
	}))
	raw, err := dbtest.RawSQLiteMetadata(t, s.db)
	require.NoError(t, err)
	var poolID int64
	require.NoError(t, raw.QueryRow(
		`SELECT id FROM pool WHERE pool_key_hash = ?`, pool,
	).Scan(&poolID))
	res, err := raw.Exec(
		`INSERT INTO "transaction" (hash, slot, block_index) VALUES (?, ?, ?)`,
		repeatByte(32, 0xb5), s.epoch.StartSlot+1, 0,
	)
	require.NoError(t, err)
	txID, err := res.LastInsertId()
	require.NoError(t, err)
	res, err = raw.Exec(
		`INSERT INTO certs (transaction_id, slot, cert_index) VALUES (?, ?, ?)`,
		txID, s.epoch.StartSlot+1, 0,
	)
	require.NoError(t, err)
	certID, err := res.LastInsertId()
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO pool_retirement (
    pool_id, pool_key_hash, certificate_id, epoch, added_slot
) VALUES (?, ?, ?, ?, ?)`,
		poolID, pool, certID, retireEpoch, s.epoch.StartSlot+1,
	)
	require.NoError(t, err)
}

func (s *govDiffScenario) stakeInputRows(
	t *testing.T,
	epoch uint64,
) []string {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, s.db)
	require.NoError(t, err)
	rows, err := raw.Query(`
SELECT hex(pool_key_hash), credential_tag, hex(staking_key), stake, owner,
    registered, captured_slot, boundary_slot
FROM reward_stake_input WHERE epoch = ?
ORDER BY pool_key_hash, credential_tag, staking_key`, epoch)
	require.NoError(t, err)
	defer rows.Close()
	var ret []string
	for rows.Next() {
		var pool, key, stake string
		var tag, captured, boundary int64
		var owner, registered bool
		require.NoError(t, rows.Scan(
			&pool, &tag, &key, &stake, &owner, &registered, &captured,
			&boundary,
		))
		ret = append(ret, fmt.Sprintf(
			"%s/%d/%s=%s owner=%t registered=%t captured=%d boundary=%d",
			pool, tag, key, stake, owner, registered, captured, boundary,
		))
	}
	require.NoError(t, rows.Err())
	return ret
}

// Deferring RATIFY past the boundary commit must not change any governance
// outcome: the ratified, enacted, expired and dropped marks, the treasury,
// reserves and refunded deposits, the committee, the protocol parameters,
// the mark snapshot and DRep power all match RATIFY run inside the boundary
// transaction, boundary for boundary. DINGO_GOV_DIFF_OUT writes the deferred
// run's dumps for comparison against another tree.
func TestDeferredRatificationMatchesBoundaryRatification(t *testing.T) {
	t.Parallel()

	const boundaries = 4
	atBoundary := newGovDiffScenario(t)
	atBoundary.ls.ratifyAtBoundary = true
	want := atBoundary.run(t, boundaries, func(*LedgerState) {})

	deferred := newGovDiffScenario(t)
	wireDeferredBoundarySnapshot(deferred.ls, deferred.snapshotMgr)
	got := deferred.run(t, boundaries, func(ls *LedgerState) {
		require.NoError(t, ls.WaitEpochBoundaryJob(t.Context()))
	})
	require.Equal(t, want, got)
	if out := os.Getenv("DINGO_GOV_DIFF_OUT"); out != "" {
		raw, err := json.MarshalIndent(got, "", " ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(out, raw, 0o600))
	}

	// The scenario must reach every outcome it claims to compare.
	last := got[len(got)-1]
	enacted := 0
	for _, p := range last.Proposals {
		if p.EnactedIn != nil {
			enacted++
		}
	}
	require.Equal(t, 3, enacted, "committee, parameter and treasury actions")
	require.Len(t, last.Committee, 2)
	require.NotEqual(t, got[0].PParams, last.PParams)
	require.NotEqual(t, got[0].Treasury, last.Treasury)
	require.NotZero(t, last.DRepPower)
	require.NotEmpty(t, last.Mark)

}
