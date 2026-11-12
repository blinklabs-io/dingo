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
	"crypto/ed25519"
	"errors"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// The gouroboros DELEG rule discovers each capability with a runtime type
// assertion; the assertions in the production code keep a signature drift a
// build error, and these tests pin the values.

func mirReaderView(
	ls *LedgerState,
	txn *database.Txn,
	epochStartSlot uint64,
	protocolMajor uint,
) *LedgerView {
	return &LedgerView{
		ls:             ls,
		txn:            txn,
		epochStartSlot: epochStartSlot,
		committeePParams: &shelley.ShelleyProtocolParameters{
			ProtocolMajor: protocolMajor,
		},
	}
}

func TestLedgerViewInstantaneousRewardsTotals(t *testing.T) {
	t.Parallel()
	ls, db, gdb := newMIRTestLedger(t)
	// A distribution from before the epoch is not pending.
	seedMIRDistribution(t, gdb, mirPotReserves, 50,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: mirCred28(1), Amount: big.NewInt(1_000)},
		})
	seedMIRPotTransfer(t, gdb, mirPotReserves, 7, 50)
	seedMIRDistribution(t, gdb, mirPotReserves, 150,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: mirCred28(1), Amount: big.NewInt(10)},
			{Credential: mirCred28(2), Amount: big.NewInt(20)},
		})
	seedMIRDistribution(t, gdb, mirPotReserves, 160,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: mirCred28(1), Amount: big.NewInt(5)},
		})
	seedMIRDistribution(t, gdb, mirPotTreasury, 170,
		[]models.MoveInstantaneousRewardsReward{
			{Credential: mirCred28(1), Amount: big.NewInt(100)},
		})
	seedMIRPotTransfer(t, gdb, mirPotReserves, 300, 180)
	seedMIRPotTransfer(t, gdb, mirPotTreasury, 40, 190)

	read := func(
		t *testing.T,
		epochStartSlot uint64,
		protocolMajor uint,
	) (reserves, treasury, reservesMoved, treasuryMoved, credit *big.Int) {
		t.Helper()
		txn := db.Transaction(context.Background(), false)
		require.NoError(t, txn.Do(func(txn *database.Txn) error {
			lv := mirReaderView(ls, txn, epochStartSlot, protocolMajor)
			var err error
			reserves, err = lv.InstantaneousRewardsTotal(mirPotReserves)
			require.NoError(t, err)
			treasury, err = lv.InstantaneousRewardsTotal(mirPotTreasury)
			require.NoError(t, err)
			reservesMoved, err = lv.InstantaneousPotTransfer(mirPotReserves)
			require.NoError(t, err)
			treasuryMoved, err = lv.InstantaneousPotTransfer(mirPotTreasury)
			require.NoError(t, err)
			credit, err = lv.PendingInstantaneousRewards(
				mirPotReserves, mirTestCredential(0, 1),
			)
			require.NoError(t, err)
			return nil
		}))
		return
	}

	t.Run("alonzo adds repeated distributions", func(t *testing.T) {
		t.Parallel()
		reserves, treasury, reservesMoved, treasuryMoved, credit := read(
			t,
			100,
			5,
		)
		require.Equal(
			t,
			"35",
			reserves.String(),
			"10+5 for credential 1, 20 for 2",
		)
		require.Equal(t, "100", treasury.String())
		require.Equal(t, "-260", reservesMoved.String(), "-300 out, +40 in")
		require.Equal(t, "260", treasuryMoved.String())
		require.Equal(t, "15", credit.String())
	})
	t.Run("before alonzo a later distribution replaces", func(t *testing.T) {
		t.Parallel()
		reserves, _, _, _, credit := read(t, 100, 4)
		require.Equal(
			t,
			"25",
			reserves.String(),
			"5 replaces 10 for credential 1, 20 for 2",
		)
		require.Equal(t, "5", credit.String())
	})
	t.Run("a new epoch starts empty", func(t *testing.T) {
		t.Parallel()
		reserves, treasury, reservesMoved, treasuryMoved, credit := read(
			t,
			200,
			5,
		)
		require.Zero(t, reserves.Sign())
		require.Zero(t, treasury.Sign())
		require.Zero(t, reservesMoved.Sign())
		require.Zero(t, treasuryMoved.Sign())
		require.Nil(t, credit)
	})
}

func TestLedgerViewInstantaneousRewardsTotalsRejectUnknownPot(t *testing.T) {
	t.Parallel()
	ls, db, _ := newMIRTestLedger(t)
	txn := db.Transaction(context.Background(), false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		lv := mirReaderView(ls, txn, 100, 5)
		_, err := lv.InstantaneousRewardsTotal(2)
		require.Error(t, err)
		_, err = lv.InstantaneousPotTransfer(2)
		require.Error(t, err)
		return nil
	}))
}

func TestLedgerViewStabilityWindowState(t *testing.T) {
	t.Parallel()
	ls, _, _ := newMIRTestLedger(t)
	byron := models.Epoch{
		EpochId: 0, StartSlot: 0, LengthInSlots: 21_600, EraId: 0,
	}
	shelleyEpoch := models.Epoch{
		EpochId: 1, StartSlot: 21_600, LengthInSlots: 432_000, EraId: 1,
	}
	withMIRCutoffEpoch(t, ls, shelleyEpoch)
	ls.Lock()
	ls.epochCache = []models.Epoch{byron, shelleyEpoch}
	ls.publishSnapshotsLocked()
	ls.Unlock()
	lv := &LedgerView{ls: ls}

	window, err := lv.StabilityWindow()
	require.NoError(t, err)
	require.Equal(t, uint64(129_600), window, "ceil(3k/f) for k=2160, f=1/20")

	for slot, want := range map[uint64]uint64{
		0:       21_600,
		21_599:  21_600,
		21_600:  453_600,
		453_599: 453_600,
	} {
		got, err := lv.NextEpochFirstSlot(slot)
		require.NoError(t, err, "slot %d", slot)
		require.Equal(t, want, got, "slot %d", slot)
	}
	_, err = lv.NextEpochFirstSlot(453_600)
	require.Error(t, err, "a slot past the known epochs is not guessed")

	// The MIR timing check rejects a slot at or after this cutoff.
	next, err := lv.NextEpochFirstSlot(100_000)
	require.NoError(t, err)
	require.Equal(t, uint64(453_600-129_600), next-window)
}

func TestLedgerViewGenesisDelegationRegistry(t *testing.T) {
	t.Parallel()
	var keys [4]ed25519.PrivateKey
	for i := range keys {
		seed := make([]byte, ed25519.SeedSize)
		seed[0] = byte(0xc0 + i)
		keys[i] = ed25519.NewKeyFromSeed(seed)
	}
	pub := func(i int) ed25519.PublicKey {
		return keys[i].Public().(ed25519.PublicKey)
	}
	lv := mirQuorumTestView(t, [3]ed25519.PublicKey{pub(0), pub(1), pub(2)})
	genesisKey := func(seed byte) lcommon.Blake2b224 {
		return lcommon.NewBlake2b224(bytes.Repeat([]byte{seed}, 28))
	}
	// The stability window is 3*432/0.05 = 25920 slots.
	const window = 25_920
	nextCertID := uint(1)
	seed := func(g byte, delegate byte, vrf byte, slot uint64, certIndex uint) {
		seedGenesisDelegation(t, lv.ls.db, models.GenesisDelegation{
			GenesisHash:         genesisKey(g).Bytes(),
			GenesisDelegateHash: bytes.Repeat([]byte{delegate}, 28),
			VrfKeyHash:          bytes.Repeat([]byte{vrf}, 32),
			AddedSlot:           slot,
			CertIndex:           certIndex,
			CertificateID:       nextCertID,
		})
		nextCertID++
	}
	// Genesis key 0x11 certifies twice in slot 100; the later certificate
	// replaces the earlier one. Key 0x22 certifies at slots 100 and 200.
	seed(0x11, 0xa1, 0xb1, 100, 0)
	seed(0x11, 0xa2, 0xb2, 100, 1)
	seed(0x22, 0xa3, 0xb3, 100, 0)
	seed(0x22, 0xa4, 0xb4, 200, 0)

	current, err := lv.CurrentGenesisDelegations(300)
	require.NoError(t, err)
	require.Len(t, current, 3)
	for i, want := range []lcommon.Blake2b224{
		genesisKey(0x11), genesisKey(0x22), genesisKey(0x33),
	} {
		require.Equal(t, want, current[i].GenesisKeyHash)
	}
	require.Equal(
		t,
		lcommon.Blake2b224Hash(pub(0)),
		current[0].DelegateKeyHash,
		"no delegation is in force yet",
	)

	future, err := lv.FutureGenesisDelegations(300)
	require.NoError(t, err)
	byGenesis := map[lcommon.Blake2b224][]lcommon.GenesisDelegation{}
	for _, d := range future {
		byGenesis[d.GenesisKeyHash] = append(byGenesis[d.GenesisKeyHash], d)
	}
	require.Len(t, future, 3)
	require.Len(t, byGenesis[genesisKey(0x11)], 1, "same slot replaces")
	require.Equal(
		t,
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xa2}, 28)),
		byGenesis[genesisKey(0x11)][0].DelegateKeyHash,
	)
	require.Len(t, byGenesis[genesisKey(0x22)], 2, "different slots both pend")

	// A delegation moves from future to current when its slot arrives.
	future, err = lv.FutureGenesisDelegations(100 + window - 1)
	require.NoError(t, err)
	require.Len(t, future, 3, "slot 100 delegations pend until 100+window")
	future, err = lv.FutureGenesisDelegations(100 + window)
	require.NoError(t, err)
	require.Len(t, future, 1, "only the slot 200 delegation still pends")
	require.Equal(t, genesisKey(0x22), future[0].GenesisKeyHash)
	current, err = lv.CurrentGenesisDelegations(100 + window)
	require.NoError(t, err)
	require.Equal(
		t,
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xa2}, 28)),
		current[0].DelegateKeyHash,
	)
	require.Equal(
		t,
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xa3}, 28)),
		current[1].DelegateKeyHash,
	)
	require.Equal(
		t,
		lcommon.NewBlake2b256(bytes.Repeat([]byte{0xb2}, 32)),
		current[0].VrfKeyHash,
	)
}

// The DELEG rule bounds a MIR certificate by the pots, which it reads through
// RewardState.GetAdaPots, an interface that cannot report an error.
func TestLedgerViewAdaPotsReportReservesAndTreasury(t *testing.T) {
	t.Parallel()
	ls, db, _ := newMIRTestLedger(t)
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 100, nil))
	txn := db.Transaction(context.Background(), false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		lv := mirReaderView(ls, txn, 100, 5)
		pots, err := lv.GetAdaPotsWithError()
		require.NoError(t, err)
		require.Equal(t, uint64(5_000), pots.Reserves)
		require.Equal(t, uint64(1_000), pots.Treasury)
		require.Equal(t, pots, lv.GetAdaPots())
		return nil
	}))
}

// With 60 pending for a credential in a pot of 100, a further distribution of
// 90 to it replaces the pending amount before Alonzo (Map.union) and fits the
// pot, and adds to it from Alonzo (Map.unionWith (<>)) and overdraws it. The
// view is built by NewView, as production validation builds it, so the era
// comes from the protocol parameters it pins. The fold of the stored
// distributions themselves is pinned by TestLedgerViewInstantaneousRewardsTotals:
// the verdict is the same under either fold because the rule cancels the
// credential's pending amount against the total.
func TestMIRRepeatedDistributionFoldsByEraThroughValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		major        uint
		wantOverdraw bool
	}{
		{"Mary replaces", lcommon.ProtocolVersionMary, false},
		{"Alonzo adds", lcommon.ProtocolVersionAlonzo, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ls, db, gdb := newMIRTestLedger(t)
			withMIRCutoffEpoch(t, ls, models.Epoch{
				EpochId: 1, StartSlot: 100, LengthInSlots: 432_000,
			})
			pp := &shelley.ShelleyProtocolParameters{ProtocolMajor: tc.major}
			ls.Lock()
			ls.currentPParams = pp
			ls.publishSnapshotsLocked()
			ls.Unlock()
			require.NoError(t, db.Metadata().SetNetworkState(0, 100, 100, nil))
			// 60 is pending for the credential from an earlier transaction.
			seedMIRDistribution(t, gdb, mirPotReserves, 150,
				[]models.MoveInstantaneousRewardsReward{
					{Credential: mirCred28(0x31), Amount: big.NewInt(60)},
				})
			cred := mirTestCredential(0, 0x31)
			cert := &lcommon.MoveInstantaneousRewardsCertificate{
				Reward: lcommon.MoveInstantaneousRewardsCertificateReward{
					Source: mirPotReserves,
					Rewards: map[*lcommon.Credential]*big.Int{
						&cred: big.NewInt(90),
					},
				},
			}
			tx := &shelley.ShelleyTransaction{
				Body: shelley.ShelleyTransactionBody{
					TxCertificates: []lcommon.CertificateWrapper{{
						Type: uint(
							lcommon.CertificateTypeMoveInstantaneousRewards,
						),
						Certificate: cert,
					}},
				},
			}

			var err error
			txn := db.Transaction(context.Background(), false)
			require.NoError(t, txn.Do(func(txn *database.Txn) error {
				err = eras.ValidateTxShelley(tx, 200, ls.NewView(txn), pp)
				return nil
			}))
			var overdraw shelley.InsufficientForInstantaneousRewardsError
			if tc.wantOverdraw {
				require.ErrorAs(t, err, &overdraw)
				require.Equal(t, "150", overdraw.Required.String())
				return
			}
			require.False(
				t,
				errors.As(err, &overdraw),
				"replacement fits the pot: %v",
				err,
			)
		})
	}
}

// A view with no pinned protocol parameters and no epoch era cannot tell which
// way to fold, so it refuses to answer.
func TestLedgerViewMIRFoldFailsClosedWithoutEra(t *testing.T) {
	t.Parallel()
	ls, db, _ := newMIRTestLedger(t)
	txn := db.Transaction(context.Background(), false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		lv := &LedgerView{ls: ls, txn: txn, epochStartSlot: 100}
		_, err := lv.InstantaneousRewardsTotal(mirPotReserves)
		require.ErrorContains(t, err, "MIR rewards fold is unknown")
		_, err = lv.PendingInstantaneousRewards(
			mirPotReserves, mirTestCredential(0, 1),
		)
		require.ErrorContains(t, err, "MIR rewards fold is unknown")
		return nil
	}))
}

// A view pinned with a typed-nil parameter pointer has no parameters to read
// the protocol version from, so it falls back like an unpinned view instead of
// dereferencing the nil receiver.
func TestLedgerViewMIRFoldTypedNilPParamsFailsClosed(t *testing.T) {
	t.Parallel()
	ls, db, _ := newMIRTestLedger(t)
	txn := db.Transaction(context.Background(), false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		lv := (&LedgerView{ls: ls, txn: txn, epochStartSlot: 100}).
			pinCommitteeState(0, (*shelley.ShelleyProtocolParameters)(nil))
		_, err := lv.InstantaneousRewardsTotal(mirPotReserves)
		require.ErrorContains(t, err, "MIR rewards fold is unknown")
		return nil
	}))
}
