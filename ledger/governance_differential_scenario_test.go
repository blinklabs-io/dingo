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
	"encoding/hex"
	"io"
	"log/slog"
	"math/big"
	"testing"

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
	"github.com/stretchr/testify/require"
)

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
	account := func(marker byte, drep []byte, utxo uint64) []byte {
		cred := repeatByte(28, marker)
		require.NoError(t, db.CreateAccount(nil, &models.Account{
			StakingKey: cred,
			Drep:       drep,
			AddedSlot:  epoch.StartSlot,
			Active:     true,
			Reward:     types.Uint64(0),
		}))
		if utxo > 0 {
			require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
				TxId:       repeatByte(32, marker),
				OutputIdx:  0,
				StakingKey: cred,
				Amount:     types.Uint64(utxo),
				AddedSlot:  epoch.StartSlot,
			}))
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
	require.NoError(t, db.CreateDrep(nil, &models.Drep{
		Credential:  s.drep,
		AddedSlot:   epoch.StartSlot,
		ExpiryEpoch: startEpoch + 100,
		Active:      true,
	}))
	account(0xe2, s.drep, 50_000)
	returnCred := account(0xe3, s.drep, 0)
	_, returnAddr := rewardAddress(returnCred)
	payee := account(0xe4, nil, 0)
	payeeAddr, _ := rewardAddress(payee)

	coldCredential := repeatByte(28, 0xd1)
	hotCredential := repeatByte(28, 0xd2)
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{{
		ColdCredHash: coldCredential,
		ExpiresEpoch: startEpoch + 100,
		AddedSlot:    1,
	}}, nil))
	require.NoError(t, db.SetCommitteeQuorum(big.NewRat(1, 1), 1, nil))
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
		require.NoError(t, db.SetGovernanceProposal(proposal, nil))
		loaded, err := db.GetGovernanceProposal(proposal.TxHash, 0, nil)
		require.NoError(t, err)
		s.proposals = append(s.proposals, loaded)
		return loaded
	}
	vote := func(
		p *models.GovernanceProposal,
		voterType uint8,
		credential []byte,
	) {
		require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
			ProposalID:      p.ID,
			VoterType:       voterType,
			VoterCredential: credential,
			Vote:            models.VoteYes,
			AddedSlot:       epoch.StartSlot - 5,
		}, nil))
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
		txn := s.db.Transaction(true)
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
		account, err := s.db.GetAccountByCredential(0, cred, true, nil)
		require.NoError(t, err)
		require.NotNil(t, account)
		d.Rewards[hex.EncodeToString(cred[:4])] = uint64(account.Reward)
	}
	for _, p := range s.proposals {
		loaded, err := s.db.GetGovernanceProposal(p.TxHash, p.ActionIndex, nil)
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
	members, err := s.db.GetCommitteeMembers(nil)
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
	d.DRepPower, err = s.db.GetDRepVotingPower(0, s.drep, 0, nil)
	require.NoError(t, err)
	deposits, _, err := governance.ActiveProposalDepositDRepPower(
		s.db, nil, epoch, 0,
	)
	require.NoError(t, err)
	d.DepositPower = deposits
	return d
}
