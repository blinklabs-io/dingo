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
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/big"
	"slices"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/indexer"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/epochengine"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/require"
)

const governanceAda = uint64(1_000_000)

// governanceChain is the Conway chain both pipelines replay: a treasury with a
// donation in its first epoch, a motion of no confidence that ratifies at the
// first boundary and is enacted at the second, a proposal that expires, a DRep
// that lapses, and DReps with delegators.
type governanceChain struct {
	epoch  models.Epoch
	major  uint
	dreps  []*models.Drep
	accts  []*models.Account
	motion *models.GovernanceProposal
	info   *models.GovernanceProposal
	// hardFork is a proposal for protocol version 10 ratified in the first
	// epoch, which the first boundary enacts.
	hardFork *models.GovernanceProposal
	// hardForkMajor is the protocol version the hard-fork proposal initiates.
	hardForkMajor uint
	// retirements are pools retiring at the first boundary, one refunding a
	// registered reward account and one an unregistered one.
	retirements bool
	// mir moves lovelace between the pots and distributes from the reserves
	// to a registered account within the first epoch.
	mir bool
}

func newGovernanceChain(epoch models.Epoch, major uint) *governanceChain {
	c := &governanceChain{epoch: epoch, major: major}
	for i, expiry := range []uint64{epoch.EpochId + 20, epoch.EpochId + 1} {
		credential := repeatByte(28, byte(0xd1+i))
		c.dreps = append(c.dreps, &models.Drep{
			Credential: credential, AddedSlot: 1, Active: true,
			LastActivityEpoch: epoch.EpochId, ExpiryEpoch: expiry,
		})
		c.accts = append(c.accts, &models.Account{
			StakingKey: repeatByte(28, byte(0xe1+i)), Drep: credential,
			AddedSlot: 1, Active: true,
			Reward: types.Uint64((30 - 10*uint64(i)) * governanceAda),
		})
	}
	action := func(a any) []byte {
		raw, err := cbor.Encode(a)
		if err != nil {
			panic(err)
		}
		return raw
	}
	returnAddr := append([]byte{0xe0}, repeatByte(28, 0x91)...)
	c.motion = &models.GovernanceProposal{
		TxHash: repeatByte(32, 0x92), ActionType: uint8(lcommon.GovActionTypeNoConfidence),
		ProposedEpoch: epoch.EpochId, ExpiresEpoch: epoch.EpochId + 20,
		AnchorURL: "https://example.invalid/no-confidence", AnchorHash: repeatByte(32, 0x93),
		Deposit: 1 * governanceAda, ReturnAddress: returnAddr,
		GovActionCbor: action(&lcommon.NoConfidenceGovAction{
			Type: uint(lcommon.GovActionTypeNoConfidence),
		}),
		AddedSlot: epoch.StartSlot + 20,
	}
	c.info = &models.GovernanceProposal{
		TxHash: repeatByte(32, 0x94), ActionType: uint8(lcommon.GovActionTypeInfo),
		ProposedEpoch: epoch.EpochId, ExpiresEpoch: epoch.EpochId,
		AnchorURL: "https://example.invalid/info", AnchorHash: repeatByte(32, 0x95),
		Deposit: 1 * governanceAda, ReturnAddress: returnAddr,
		GovActionCbor: action(&lcommon.InfoGovAction{
			Type: uint(lcommon.GovActionTypeInfo),
		}),
		AddedSlot: epoch.StartSlot + 21,
	}
	return c
}

// withHardFork adds the ratified proposal for protocol version 10 and an
// account delegating to a DRep that is not registered, which the Plomin rule
// clears when the version is enacted.
func (c *governanceChain) withHardFork() *governanceChain {
	return c.withHardForkTo(10)
}

// withHardForkTo is withHardFork for the protocol version major.
func (c *governanceChain) withHardForkTo(major uint) *governanceChain {
	c.hardForkMajor = major
	raw, err := cbor.Encode(&lcommon.HardForkInitiationGovAction{
		Type: uint(lcommon.GovActionTypeHardForkInitiation),
		ProtocolVersion: struct {
			cbor.StructAsArray
			Major uint
			Minor uint
		}{Major: major},
	})
	if err != nil {
		panic(err)
	}
	ratifiedEpoch, ratifiedSlot := c.epoch.EpochId, c.epoch.StartSlot+50
	c.hardFork = &models.GovernanceProposal{
		TxHash: repeatByte(32, 0x96), ActionType: uint8(lcommon.GovActionTypeHardForkInitiation),
		ProposedEpoch: c.epoch.EpochId, ExpiresEpoch: c.epoch.EpochId + 20,
		AnchorURL: "https://example.invalid/hard-fork", AnchorHash: repeatByte(32, 0x97),
		Deposit: 1 * governanceAda, ReturnAddress: c.motion.ReturnAddress,
		GovActionCbor: raw, AddedSlot: c.epoch.StartSlot + 22,
		RatifiedEpoch: &ratifiedEpoch, RatifiedSlot: &ratifiedSlot,
	}
	c.accts = append(c.accts, &models.Account{
		StakingKey: repeatByte(28, 0xe3), Drep: repeatByte(28, 0xd9),
		AddedSlot: 1, Active: true, Reward: types.Uint64(5 * governanceAda),
	})
	return c
}

// pparams are Conway parameters whose motion-of-no-confidence thresholds the
// DReps' votes meet, so the motion ratifies at the first boundary.
func (c *governanceChain) pparams() *conway.ConwayProtocolParameters {
	p := donationTestConwayPParams(c.major)
	p.MinCommitteeSize = 1
	zero := cbor.Rat{Rat: new(big.Rat)}
	p.DRepVotingThresholds.MotionNoConfidence = zero
	p.PoolVotingThresholds.MotionNoConfidence = zero
	return p
}

// seedState writes the chain's facts the boundaries read through ldb.
func (c *governanceChain) seedState(t *testing.T, ldb ledgerdb.LedgerDB) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, ldb.AddNetworkDonation(
		c.epoch.StartSlot+10, c.epoch.EpochId, 50*governanceAda, nil,
	))
	proposals := []*models.GovernanceProposal{c.motion, c.info}
	if c.hardFork != nil {
		proposals = append(proposals, c.hardFork)
	}
	for _, p := range proposals {
		require.NoError(t, ldb.SetGovernanceProposal(ctx, p, nil))
	}
	for _, d := range c.dreps {
		require.NoError(t, ldb.SetGovernanceVote(ctx, &models.GovernanceVote{
			ProposalID: c.motion.ID, VoterType: models.VoterTypeDRep,
			VoterCredential: d.Credential, Vote: models.VoteYes,
			AddedSlot: c.epoch.StartSlot + 30,
		}, nil))
	}
}

// seedBasis is what a boundary reads of the epochs before it, as the ledger
// tests seed it for a rollover.
func (c *governanceChain) seedBasis(
	t *testing.T,
	ldb ledgerdb.LedgerDB,
	ended models.Epoch,
	pparams lcommon.ProtocolParameters,
) {
	t.Helper()
	epochs, ok := epochengine.StakeRewardEpochsForApplication(ended.EpochId + 1)
	require.True(t, ok)
	for _, id := range []uint64{epochs.Snapshot, epochs.Performance, epochs.Pots} {
		if e, err := ldb.GetEpoch(id, nil); err != nil || e != nil {
			require.NoError(t, err)
			continue
		}
		delta := ended.EpochId - id
		require.NoError(t, ldb.SetEpoch(
			ended.StartSlot-delta*uint64(ended.LengthInSlots), id,
			nil, nil, nil, nil, ended.EraId, ended.SlotLength,
			ended.LengthInSlots, nil,
		))
	}
	raw, err := cbor.Encode(pparams)
	require.NoError(t, err)
	perf, err := ldb.GetEpoch(epochs.Performance, nil)
	require.NoError(t, err)
	held, err := ldb.GetPParamsRows(epochs.Performance, ended.EraId, nil)
	require.NoError(t, err)
	if len(held) == 0 {
		require.NoError(t, ldb.SetPParams(
			raw, perf.StartSlot, epochs.Performance, ended.EraId, nil,
		))
	}
	state, err := ldb.GetNetworkState(nil)
	require.NoError(t, err)
	var treasury, reserves types.Uint64
	if state != nil {
		treasury, reserves = state.Treasury, state.Reserves
	}
	require.NoError(t, ldb.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch: epochs.Pots, Treasury: treasury, Reserves: reserves,
		CapturedSlot: ended.StartSlot,
	}, nil))
	require.NoError(t, ldb.DeleteRewardInputsForEpoch(epochs.Snapshot, nil))
	require.NoError(t, ldb.DeleteRewardOutputsForEpoch(epochs.Snapshot, nil))
	require.NoError(t, ldb.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch: epochs.Snapshot, SnapshotType: "mark",
		CapturedSlot: ended.StartSlot, BoundarySlot: ended.StartSlot,
	}, nil))
}

// governanceHistoryTables are the tables only the IndexerDB holds: the
// LedgerDB keeps no DRep history.
var governanceHistoryTables = []string{
	"drep_expiry_history", "drep_dormancy_history", "drep_expiry_epoch_event",
	"account_drep_clear",
}

func optEpoch(v *uint64) string {
	if v == nil {
		return "nil"
	}
	return fmt.Sprint(*v)
}

// ledgerStateFacts renders, through the LedgerDB interface, the state both
// pipelines hold after crossing the boundaries: epochs, treasury, donations,
// protocol parameters, proposals and the drops among them, DReps and accounts.
func ledgerStateFacts(
	t *testing.T,
	ldb ledgerdb.LedgerDB,
	boundary uint64,
	dreps func(func(*models.Drep) error) error,
	accounts func(func(*models.Account) error) error,
) map[string][]string {
	t.Helper()
	ctx := t.Context()
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	facts := map[string][]string{}
	epochs, err := ldb.GetEpochs(txn)
	require.NoError(t, err)
	for _, e := range epochs {
		facts["epoch"] = append(facts["epoch"], fmt.Sprintf(
			"epoch_id=%d era_id=%d length_in_slots=%d slot_length=%d start_slot=%d",
			e.EpochId, e.EraId, e.LengthInSlots, e.SlotLength, e.StartSlot,
		))
		rows, err := ldb.GetPParamsRows(e.EpochId, e.EraId, txn)
		require.NoError(t, err)
		for _, r := range rows {
			if r.AddedSlot >= boundary {
				facts["pparams"] = append(facts["pparams"], fmt.Sprintf(
					"epoch=%d era=%d cbor=%x", r.Epoch, r.EraId, r.Cbor,
				))
			}
		}
	}
	state, err := ldb.GetNetworkState(txn)
	require.NoError(t, err)
	if state != nil {
		facts["network_state"] = append(facts["network_state"], fmt.Sprintf(
			"treasury=%d reserves=%d", state.Treasury, state.Reserves,
		))
	}
	donations, err := ldb.ListNetworkDonations(txn)
	require.NoError(t, err)
	for _, d := range donations {
		facts["network_donation"] = append(facts["network_donation"], fmt.Sprintf(
			"slot=%d epoch=%d amount=%d", d.Slot, d.Epoch, d.Amount,
		))
	}
	proposals, err := ldb.GetAllGovernanceProposals(ctx, txn)
	require.NoError(t, err)
	for _, p := range proposals {
		facts["governance_proposal"] = append(facts["governance_proposal"], fmt.Sprintf(
			"tx_hash=%x action_index=%d ratified_epoch=%s ratified_slot=%s "+
				"enacted_epoch=%s enacted_slot=%s expired_epoch=%s expired_slot=%s",
			p.TxHash, p.ActionIndex, optEpoch(p.RatifiedEpoch), optEpoch(p.RatifiedSlot),
			optEpoch(p.EnactedEpoch), optEpoch(p.EnactedSlot),
			optEpoch(p.ExpiredEpoch), optEpoch(p.ExpiredSlot),
		))
		if p.DroppedEpoch != nil {
			facts["governance_proposal_drop"] = append(
				facts["governance_proposal_drop"],
				fmt.Sprintf(
					"tx_hash=%x action_index=%d dropped_epoch=%s dropped_slot=%s",
					p.TxHash, p.ActionIndex, optEpoch(p.DroppedEpoch), optEpoch(p.DroppedSlot),
				),
			)
		}
	}
	require.NoError(t, dreps(func(d *models.Drep) error {
		facts["drep"] = append(facts["drep"], fmt.Sprintf(
			"credential=%x credential_tag=%d active=%t added_slot=%d",
			d.Credential, d.CredentialTag, d.Active, d.AddedSlot,
		))
		return nil
	}))
	require.NoError(t, accounts(func(a *models.Account) error {
		facts["account"] = append(facts["account"], fmt.Sprintf(
			"staking_key=%x credential_tag=%d pool=%x drep=%x drep_type=%d "+
				"active=%t expiration_epoch=%d added_slot=%d reward=%d",
			a.StakingKey, a.CredentialTag, a.Pool, a.Drep, a.DrepType,
			a.Active, a.ExpirationEpoch, a.AddedSlot, a.Reward,
		))
		return nil
	}))
	for _, rows := range facts {
		slices.Sort(rows)
	}
	return facts
}

func renderRows(rows metadata.FactRows, keep []string) []string {
	cols := keep
	if cols == nil {
		cols = slices.DeleteFunc(
			slices.Clone(rows.Columns),
			func(c string) bool { return c == "id" || c == "folded" },
		)
	}
	cols = slices.Clone(cols)
	slices.Sort(cols)
	var out []string
	for _, row := range rows.Rows {
		parts := make([]string, 0, len(cols))
		for _, c := range cols {
			i := slices.Index(rows.Columns, c)
			if i < 0 {
				continue
			}
			switch x := row[i].(type) {
			case []byte:
				parts = append(parts, fmt.Sprintf("%s=%x", c, x))
			case bool:
				parts = append(parts, fmt.Sprintf("%s=%t", c, x))
			default:
				parts = append(parts, fmt.Sprintf("%s=%v", c, x))
			}
		}
		out = append(out, strings.Join(parts, " "))
	}
	slices.Sort(out)
	return out
}

func readFactTable(
	t *testing.T,
	read func(metadata.FactTable, int64) (metadata.FactRows, int64, error),
	name string,
) []string {
	t.Helper()
	var table metadata.FactTable
	for _, ft := range metadata.FactTables {
		if ft.Name == name {
			table = ft
		}
	}
	require.Equal(t, name, table.Name)
	var all metadata.FactRows
	var cursor int64
	for {
		rows, next, err := read(table, cursor)
		require.NoError(t, err, name)
		all.Columns = rows.Columns
		all.Rows = append(all.Rows, rows.Rows...)
		if next == 0 {
			break
		}
		cursor = next
	}
	return renderRows(all, nil)
}

// The state the IndexerDB derives across two Conway boundaries equals the
// ledger's for the same chain, and the DRep history it alone holds is as the
// scenario implies: the motion's ratification and
// enactment, the proposal that expires and is dropped, the DRep that lapses
// and the dormancy and expiry history around it, the donation, the treasury,
// and the parameters.
func requireGovernanceEquality(
	t *testing.T,
	major uint,
	configure func(*governanceChain),
) map[string][]string {
	t.Helper()
	f := newTreasuryRolloverFixture(t, 100*governanceAda)
	chain := newGovernanceChain(f.currentEpoch, major)
	if configure != nil {
		configure(chain)
	}
	f.currentPParams = chain.pparams()
	f.ls.currentPParams = f.currentPParams
	f.seedGovernance(t, chain)

	first := f.rollover(t, f.currentEpoch, f.currentPParams)
	second := f.rollover(t, first.NewCurrentEpoch, first.NewCurrentPParams)

	// The IndexerDB holds the same state, and the deriver crosses the same
	// two boundaries from a block of each new epoch.
	idx := newIndexerDBForDerive(t)
	chain.seedIndexer(t, idx, f)
	d, err := indexer.NewDeriver(idx, indexer.DeriveConfig{
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		NodeConfig: f.ls.config.CardanoNodeConfig,
	})
	require.NoError(t, err)
	blocks, err := fixtures.GenerateConwayChain(
		1, lcommon.Blake2b256{}, first.NewCurrentEpoch.StartSlot, second.NewCurrentEpoch.StartSlot-first.NewCurrentEpoch.StartSlot, 2,
	)
	require.NoError(t, err)
	ldb := dbadapter.New(idx)
	chain.seedBasis(t, ldb, f.currentEpoch, f.currentPParams)
	crossBlocks(t, d, idx, modelBlock(blocks[0]))
	chain.seedBasis(t, ldb, first.NewCurrentEpoch, first.NewCurrentPParams)
	crossBlocks(t, d, idx, modelBlock(blocks[1]))

	store, ok := idx.Metadata().(metadata.FactRowStore)
	require.True(t, ok)
	// The fixture rewrites the parameter rows it seeds: the parameters the
	// boundaries derive are what is compared.
	boundary := f.currentEpoch.StartSlot + uint64(f.currentEpoch.LengthInSlots)
	set, ok := f.ls.config.UtxoSet.(interface {
		drepLister
		accountLister
	})
	require.True(t, ok)
	want := ledgerStateFacts(t, f.ls.LedgerDB(), boundary, set.Dreps, set.Accounts)
	idxTxn := ldb.Begin(t.Context(), false)
	defer idxTxn.Release()
	got := ledgerStateFacts(t, ldb, boundary,
		func(fn func(*models.Drep) error) error { return ldb.IterateDreps(idxTxn, fn) },
		func(fn func(*models.Account) error) error { return ldb.IterateAccounts(idxTxn, fn) },
	)
	for name := range want {
		require.Equal(t, want[name], got[name], "state %s", name)
	}
	require.Equal(t, len(want), len(got))
	facts := want
	// The DRep history is the IndexerDB's alone.
	for _, name := range governanceHistoryTables {
		facts[name] = readFactTable(t, func(
			table metadata.FactTable, cursor int64,
		) (metadata.FactRows, int64, error) {
			txn := idx.MetadataTxn(t.Context(), false)
			defer txn.Release()
			return store.ReadFactRows(t.Context(), table, boundary, math.MaxInt64, cursor, 100, txn.Metadata())
		}, name)
	}
	return facts
}

func TestDerivedGovernanceEqualsTheLedgers(t *testing.T) {
	t.Parallel()
	facts := requireGovernanceEquality(t, 10, nil)
	// The scenario reaches each outcome it compares, so equality is not
	// two empty tables.
	require.Len(t, facts["governance_proposal"], 2)
	require.Contains(t, facts["governance_proposal"][0]+facts["governance_proposal"][1], "ratified_epoch=6")
	require.Contains(t, facts["governance_proposal"][0]+facts["governance_proposal"][1], "enacted_epoch=7")
	require.Contains(t, facts["governance_proposal"][0]+facts["governance_proposal"][1], "expired_epoch=6")
	require.Len(t, facts["governance_proposal_drop"], 1)
	require.Len(t, facts["network_donation"], 1)
	require.Len(t, facts["drep_expiry_history"], 2)
	require.NotEmpty(t, facts["drep_dormancy_history"])
}

// Enacting protocol version 10 clears the delegations to DReps that are not
// registered, at the same boundary and with the same history rows, as the
// ledger does; the enacted parameters carry the new version.
func TestDerivedHardForkRuleEqualsTheLedgers(t *testing.T) {
	t.Parallel()
	facts := requireGovernanceEquality(t, 9, func(c *governanceChain) { c.withHardFork() })
	require.Len(t, facts["account_drep_clear"], 1)
	require.Contains(t, strings.Join(facts["governance_proposal"], " "), "enacted_epoch=6")
}

// seedGovernance writes the chain's state to the ledger fixture's set.
func (f *treasuryRolloverFixture) seedGovernance(
	t *testing.T,
	c *governanceChain,
) {
	t.Helper()
	c.seedState(t, seedDB(t, f.db))
	for _, d := range c.dreps {
		require.NoError(t, seedCreateDrep(t, f.db, context.Background(), nil, d))
	}
	for _, a := range c.accts {
		require.NoError(t, seedCreateAccount(t, f.db, context.Background(), nil, a))
	}
	if c.retirements {
		for _, r := range c.retiringPools() {
			fxRetiringPool(
				t, f.db, r.keyHash, r.rewardAccount, r.deposit, 1,
				c.epoch.EpochId+1, c.epoch.StartSlot+5,
			)
		}
	}
	if c.mir {
		seedMIRPotTransfer(t, f.db, epochengine.MIRPotTreasury, mirTransfer, c.mirSlot())
		seedMIRDistribution(
			t, f.db, epochengine.MIRPotReserves, c.mirSlot(),
			c.mirRewards(),
		)
	}
}

const (
	mirTransfer     = 5 * governanceAda
	mirDistribution = 600
)

type retiringPool struct {
	keyHash, rewardAccount []byte
	deposit                uint64
}

// retiringPools are the pools that retire at the first boundary: the deposit
// of the first is refunded to the registered account of the first DRep
// delegator and the second's goes to the treasury, its account being
// unregistered.
func (c *governanceChain) retiringPools() []retiringPool {
	return []retiringPool{
		{repeatByte(28, 0xa1), c.accts[0].StakingKey, 7 * governanceAda},
		{repeatByte(28, 0xa2), repeatByte(28, 0xa3), 11 * governanceAda},
	}
}

func (c *governanceChain) mirSlot() uint64 { return c.epoch.StartSlot + 6 }

func (c *governanceChain) mirRewards() []models.MoveInstantaneousRewardsReward {
	return []models.MoveInstantaneousRewardsReward{{
		Credential: c.accts[1].StakingKey, Amount: big.NewInt(mirDistribution),
	}}
}

// seedIndexerExtras writes the pool and MIR rows the IndexerDB's block indexer
// would have written.
func (c *governanceChain) seedIndexerExtras(t *testing.T, idx *database.Database) {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, idx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	exec := func(query string, args ...any) int64 {
		res, err := raw.Exec(query, args...)
		require.NoError(t, err)
		id, err := res.LastInsertId()
		require.NoError(t, err)
		return id
	}
	if c.retirements {
		for _, r := range c.retiringPools() {
			poolID := exec(`INSERT INTO pool (pool_key_hash) VALUES (?)`, r.keyHash)
			exec(`INSERT INTO pool_registration (pool_id, pool_key_hash, `+
				`added_slot, reward_account, reward_account_credential_tag, `+
				`deposit_amount, deposit_held) VALUES (?, ?, 1, ?, 0, ?, ?)`,
				poolID, r.keyHash, r.rewardAccount,
				fmt.Sprint(r.deposit), fmt.Sprint(r.deposit))
			exec(`INSERT INTO pool_retirement (pool_id, pool_key_hash, `+
				`certificate_id, epoch, added_slot) VALUES (?, ?, 0, ?, ?)`,
				poolID, r.keyHash, c.epoch.EpochId+1, c.epoch.StartSlot+5)
		}
	}
	if c.mir {
		exec(`INSERT INTO move_instantaneous_rewards (pot, certificate_id, `+
			`added_slot, other_pot) VALUES (?, 0, ?, ?)`,
			epochengine.MIRPotTreasury, c.mirSlot(), fmt.Sprint(mirTransfer))
		mirID := exec(`INSERT INTO move_instantaneous_rewards (pot, `+
			`certificate_id, added_slot) VALUES (?, 0, ?)`,
			epochengine.MIRPotReserves, c.mirSlot())
		for _, r := range c.mirRewards() {
			exec(`INSERT INTO move_instantaneous_rewards_reward (credential, `+
				`credential_tag, amount, mir_id) VALUES (?, 0, ?, ?)`,
				r.Credential, r.Amount.String(), mirID)
		}
	}
}

// seedIndexer writes to the IndexerDB the state the ledger fixture starts from
// and the chain's own.
func (c *governanceChain) seedIndexer(
	t *testing.T,
	idx *database.Database,
	f *treasuryRolloverFixture,
) {
	t.Helper()
	ctx := context.Background()
	ldb := dbadapter.New(idx)
	e := c.epoch
	require.NoError(t, ldb.SetEpoch(
		e.StartSlot, e.EpochId, e.Nonce, e.EvolvingNonce, e.CandidateNonce,
		e.LastEpochBlockNonce, e.EraId, e.SlotLength, e.LengthInSlots, nil,
	))
	require.NoError(t, ldb.SetNetworkState(100*governanceAda, 1_000, 499, nil))
	require.NoError(t, ldb.SetCommitteeMembers(ctx, []*models.CommitteeMember{{
		ColdCredHash: repeatByte(28, 0xc1), ExpiresEpoch: e.EpochId + 20, AddedSlot: 1,
	}}, nil))
	require.NoError(t, ldb.SetCommitteeQuorum(ctx, big.NewRat(1, 1), 1, nil))
	raw, err := cbor.Encode(f.currentPParams)
	require.NoError(t, err)
	require.NoError(t, ldb.SetPParams(raw, e.StartSlot, e.EpochId, e.EraId, nil))
	c.seedState(t, ldb)
	for _, d := range c.dreps {
		require.NoError(t, idx.CreateDrep(ctx, nil, d))
	}
	for _, a := range c.accts {
		require.NoError(t, idx.CreateAccount(ctx, nil, a))
	}
	c.seedIndexerExtras(t, idx)
}

// A pool retiring at the boundary refunds its deposit to a registered reward
// account or, when the account is unregistered, to the treasury, in the
// IndexerDB as in the ledger.
func TestDerivedPoolRetirementRefundsEqualTheLedgers(t *testing.T) {
	t.Parallel()
	facts := requireGovernanceEquality(t, 10, func(c *governanceChain) {
		c.retirements = true
	})
	require.Contains(t, strings.Join(facts["account"], "\n"),
		fmt.Sprintf("reward=%d", (30+7)*governanceAda))
}

// Instantaneous rewards in the ended epoch move lovelace between the pots and
// credit a registered account at the boundary, in the IndexerDB as in the
// ledger.
func TestDerivedMIREqualsTheLedgers(t *testing.T) {
	t.Parallel()
	facts := requireGovernanceEquality(t, 10, func(c *governanceChain) {
		c.mir = true
	})
	require.Contains(t, strings.Join(facts["account"], "\n"),
		fmt.Sprintf("reward=%d", 20*governanceAda+mirDistribution))
}

// Enacting protocol version 12 makes the same boundary ratify after the
// enactment, so the motion of no confidence ratifies there as it does in the
// ledger.
func TestDerivedPostEnactmentRatificationEqualsTheLedgers(t *testing.T) {
	t.Parallel()
	facts := requireGovernanceEquality(t, 11, func(c *governanceChain) {
		c.withHardForkTo(12)
	})
	require.Contains(t, strings.Join(facts["governance_proposal"], "\n"),
		"ratified_epoch=6")
}
