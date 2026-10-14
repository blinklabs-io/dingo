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

package koiosparity

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"math/big"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

const previewGovActionDeposit = uint64(100_000_000_000)

func refundTestPParams() *conway.ConwayProtocolParameters {
	rat := func(n, d int64) cbor.Rat { return cbor.Rat{Rat: big.NewRat(n, d)} }
	p := &conway.ConwayProtocolParameters{}
	p.ProtocolVersion.Major = 10
	p.MinCommitteeSize = 3
	p.DRepVotingThresholds = conway.DRepVotingThresholds{
		MotionNoConfidence:    rat(67, 100),
		CommitteeNormal:       rat(67, 100),
		CommitteeNoConfidence: rat(60, 100),
		UpdateToConstitution:  rat(75, 100),
		HardForkInitiation:    rat(60, 100),
		PpNetworkGroup:        rat(67, 100),
		PpEconomicGroup:       rat(67, 100),
		PpTechnicalGroup:      rat(67, 100),
		PpGovGroup:            rat(75, 100),
		TreasuryWithdrawal:    rat(67, 100),
	}
	p.PoolVotingThresholds = conway.PoolVotingThresholds{
		MotionNoConfidence:    rat(51, 100),
		CommitteeNormal:       rat(51, 100),
		CommitteeNoConfidence: rat(51, 100),
		HardForkInitiation:    rat(51, 100),
		PpSecurityGroup:       rat(51, 100),
	}
	return p
}

func refundTestRewardAddress(t *testing.T, stakeKey []byte) []byte {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeKey,
	)
	require.NoError(t, err)
	out, err := addr.Bytes()
	require.NoError(t, err)
	return out
}

func runRefundBoundary(
	t *testing.T,
	db *database.Database,
	newEpoch, boundarySlot uint64,
) {
	t.Helper()
	txn := db.MetadataTxn(context.Background(), true)
	defer txn.Release()
	_, err := governance.ProcessEpoch(context.Background(), &governance.EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    newEpoch - 1,
		NewEpoch:     newEpoch,
		BoundarySlot: boundarySlot,
		PParams:      refundTestPParams(),
		UpdateFn: func(
			pparams lcommon.ProtocolParameters,
			_ any,
		) (lcommon.ProtocolParameters, error) {
			return pparams, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, txn.Commit())
}

// seedEpoch677Refunds reproduces the Preview shape: info actions proposed far
// earlier expire at the 676 to 677 boundary and are dropped, with their
// deposit refunded, at the 677 to 678 boundary. It returns the stake keys of
// the registered return accounts and of an unregistered one.
func seedEpoch677Refunds(
	t *testing.T,
	db *database.Database,
) (registered [][]byte, unregistered []byte) {
	t.Helper()
	registered = [][]byte{testPoolKeyHash(t, 0xa1), testPoolKeyHash(t, 0xa2)}
	unregistered = testPoolKeyHash(t, 0xa3)
	returns := append(append([][]byte{}, registered...), unregistered)
	for i, key := range returns {
		if i < len(registered) {
			require.NoError(t, db.Metadata().CreateAccount(nil, &models.Account{
				StakingKey: key,
				Active:     true,
			}))
		}
		require.NoError(t, db.SetGovernanceProposal(
			context.Background(),
			&models.GovernanceProposal{
				TxHash:        bytes.Repeat([]byte{byte(0x10 + i)}, 32),
				ActionType:    uint8(lcommon.GovActionTypeInfo),
				ProposedEpoch: 646,
				ExpiresEpoch:  676,
				AnchorURL:     "https://example.invalid/info",
				AnchorHash:    bytes.Repeat([]byte{0x01}, 32),
				Deposit:       previewGovActionDeposit,
				ReturnAddress: refundTestRewardAddress(t, key),
				AddedSlot:     100,
			},
			nil,
		))
	}
	runRefundBoundary(t, db, 677, 677_000)
	runRefundBoundary(t, db, 678, 678_000)
	return registered, unregistered
}

func requireOnlyRefunds(
	t *testing.T,
	got []governance.ProposalDepositRefund,
	keys [][]byte,
) {
	t.Helper()
	require.Len(t, got, len(keys))
	seen := map[string]uint64{}
	for _, r := range got {
		require.Equal(t, uint8(0), r.CredentialTag)
		seen[string(r.StakingKey)] += r.Amount
	}
	for _, key := range keys {
		require.Equal(t, previewGovActionDeposit, seen[string(key)])
	}
}

func TestDatabaseSourceGetProposalDepositRefunds(t *testing.T) {
	t.Parallel()

	db := newTestDatabaseSourceDB(t)
	registered, _ := seedEpoch677Refunds(t, db)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	got, err := source.GetProposalDepositRefunds(context.Background(), 678)
	require.NoError(t, err)
	// The unregistered return account's deposit went to the treasury, which
	// Koios does not list as a proposal_refund either.
	requireOnlyRefunds(t, got, registered)

	for _, epoch := range []uint64{677, 679} {
		got, err = source.GetProposalDepositRefunds(
			context.Background(), epoch,
		)
		require.NoError(t, err)
		require.Empty(t, got, "epoch %d", epoch)
	}
}

// DingoDB reads the same tables through SQL; the two sources must agree on
// the same committed state, so it runs over the same real schema.
func TestDingoDBGetProposalDepositRefundsMatchesDatabaseSource(t *testing.T) {
	t.Parallel()

	db := newTestDatabaseSourceDB(t)
	registered, _ := seedEpoch677Refunds(t, db)
	var raw *sql.DB = sourceSQLDB(t, db).db
	dingo := &DingoDB{db: raw, dialect: "sqlite"}

	got, err := dingo.GetProposalDepositRefunds(context.Background(), 678)
	require.NoError(t, err)
	requireOnlyRefunds(t, got, registered)

	for _, epoch := range []uint64{677, 679} {
		got, err = dingo.GetProposalDepositRefunds(
			context.Background(), epoch,
		)
		require.NoError(t, err)
		require.Empty(t, got, "epoch %d", epoch)
	}
}

// compareRefundEpoch runs the per-account comparison for Koios reporting
// epoch 677 against reference rows, using the real committed state.
func compareRefundEpoch(
	t *testing.T,
	source RewardParitySource,
	koios []KoiosAccountRewards,
) []CheckMismatch {
	t.Helper()
	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })
	require.NoError(t, cache.CommitAccountRewardsForEpoch(
		"preview", 677, koios, 1, true, time.Now(),
	))
	return compareEpochAccounts(
		context.Background(),
		cache,
		source,
		"preview",
		677,
		676,
		time.Now(),
		0,
		time.Time{},
		false,
		slog.New(slog.DiscardHandler),
	)
}

func koiosRefundRows(
	t *testing.T,
	keys [][]byte,
	amount string,
) []KoiosAccountRewards {
	t.Helper()
	var rows []KoiosAccountRewards
	for _, key := range keys {
		addr, err := StakeAddressFromCredential(key, 0)
		require.NoError(t, err)
		rows = append(rows, KoiosAccountRewards{
			StakeAddress: addr,
			RewardType:   "proposal_refund",
			Earned:       amount,
		})
	}
	return rows
}

// TestCompareEpochAccountsProposalRefundParity is the epoch 677 scenario end
// to end: the proposals are expired and dropped by the real boundary
// processing, the credit is read back through the parity source, and Koios
// reports one proposal_refund row for each return account.
func TestCompareEpochAccountsProposalRefundParity(t *testing.T) {
	t.Parallel()

	db := newTestDatabaseSourceDB(t)
	registered, _ := seedEpoch677Refunds(t, db)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	t.Run("matching refunds pass", func(t *testing.T) {
		t.Parallel()
		ms := compareRefundEpoch(
			t, source, koiosRefundRows(t, registered, "100000000000"),
		)
		require.Empty(t, ms)
		require.Equal(t, StatusPass, DetermineStatus(ms))
	})

	t.Run("refund Koios lacks fails", func(t *testing.T) {
		t.Parallel()
		ms := compareRefundEpoch(
			t, source, koiosRefundRows(t, registered[:1], "100000000000"),
		)
		require.Len(t, ms, 1)
		require.Equal(t, CategoryAcctOnlyDingo, ms[0].Category)
		require.Equal(t, StatusFail, DetermineStatus(ms))
	})

	t.Run("refund Dingo lacks fails", func(t *testing.T) {
		t.Parallel()
		extra := append(
			koiosRefundRows(t, registered, "100000000000"),
			koiosRefundRows(t, [][]byte{testPoolKeyHash(t, 0xa9)}, "100000000000")...,
		)
		ms := compareRefundEpoch(t, source, extra)
		require.Len(t, ms, 1)
		require.Equal(t, CategoryAcctOnlyKoios, ms[0].Category)
		require.Equal(t, StatusFail, DetermineStatus(ms))
	})

	t.Run("wrong amount fails", func(t *testing.T) {
		t.Parallel()
		ms := compareRefundEpoch(
			t, source, koiosRefundRows(t, registered, "500000000000"),
		)
		require.Len(t, ms, 2)
		for _, m := range ms {
			require.Equal(t, CategoryValueMismatch, m.Category)
			require.Equal(t, "100000000000", m.DingoValue)
			require.Equal(t, "500000000000", m.KoiosValue)
		}
	})
}

type failingRefundSource struct {
	RewardParitySource
}

func (failingRefundSource) GetProposalDepositRefunds(
	context.Context,
	uint64,
) ([]governance.ProposalDepositRefund, error) {
	return nil, errors.New("refund read failed")
}

// A failed Dingo read is the one case dingo_db_error describes, and it must
// not let the epoch read as a pass on the rows that were readable.
func TestCompareEpochAccountsRefundReadFailureIsDatabaseError(t *testing.T) {
	t.Parallel()

	db := newTestDatabaseSourceDB(t)
	inner, err := NewDatabaseSource(db)
	require.NoError(t, err)

	ms := compareRefundEpoch(t, failingRefundSource{inner}, nil)
	require.Len(t, ms, 1)
	require.Equal(t, "proposal_deposit_refund", ms[0].Field)
	require.Equal(t, CategoryDBError, ms[0].Category)
	require.Equal(t, StatusError, DetermineStatus(ms))
}

// A rolled-back proposal is soft-deleted and its refund went with it; neither
// source may report it.
func TestProposalDepositRefundsExcludeSoftDeletedProposals(t *testing.T) {
	t.Parallel()

	db := newTestDatabaseSourceDB(t)
	registered, _ := seedEpoch677Refunds(t, db)
	raw := sourceSQLDB(t, db).db
	_, err := raw.Exec(
		`UPDATE governance_proposal SET deleted_slot = 679000
		 WHERE tx_hash = ?`,
		bytes.Repeat([]byte{0x10}, 32),
	)
	require.NoError(t, err)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)
	dingo := &DingoDB{db: raw, dialect: "sqlite"}

	for name, src := range map[string]RewardParitySource{
		"database source": source,
		"dingo db":        dingo,
	} {
		got, err := src.GetProposalDepositRefunds(context.Background(), 678)
		require.NoError(t, err, name)
		requireOnlyRefunds(t, got, registered[1:])
	}
}
