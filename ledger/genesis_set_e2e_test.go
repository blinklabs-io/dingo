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

package ledger_test

import (
	"bytes"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// genesisReadBackNames are the metadata store reads the genesis writes used
// to stage the set's genesis state from: every account, DRep, DRep delegator
// link and pool the store held, and each DRep's deposit.
var genesisReadBackNames = []string{
	"IterateAccounts", "IterateDreps", "IterateLiveDrepDelegators",
	"ListPoolKeyHashes", "GetPools", "GetPoolDepositsHeld",
	"GetDrepLastRegistrationDeposit",
}

func genesisCredential(typ uint, seed byte) *lcommon.Credential {
	return &lcommon.Credential{
		CredType:   typ,
		Credential: lcommon.CredentialHash(bytes.Repeat([]byte{seed}, 28)),
	}
}

// genesisGovernance is a Conway genesis with two initial DReps, one with an
// anchor and one without, and one delegation of each type: to a pool and a
// credential DRep, to a script DRep, to a predefined DRep, and to a pool
// alone, and one more that re-delegates the Shelley genesis staker.
func genesisGovernance(
	staker []byte,
) (conway.ConwayGenesisInitialDReps, conway.ConwayGenesisDelegs) {
	pool := lcommon.PoolId(bytes.Repeat([]byte{0xb1}, 28))
	otherPool := lcommon.PoolId(bytes.Repeat([]byte{0xb2}, 28))
	existing := &lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.CredentialHash(staker),
	}
	dreps := conway.ConwayGenesisInitialDReps{
		genesisCredential(lcommon.CredentialTypeAddrKeyHash, 0xd1): {
			Expiry: 7, Deposit: 500_000_000,
			Anchor: &lcommon.GovAnchor{
				Url: "https://example.invalid/drep", DataHash: [32]byte{0xa1},
			},
		},
		genesisCredential(lcommon.CredentialTypeScriptHash, 0xd2): {
			Expiry: 9, Deposit: 100,
		},
	}
	delegs := conway.ConwayGenesisDelegs{
		genesisCredential(lcommon.CredentialTypeAddrKeyHash, 0xc1): {
			Type:   conway.ConwayGenesisDelegateeTypeStakeVote,
			PoolId: pool,
			DRep: lcommon.Drep{
				Type: lcommon.DrepTypeAddrKeyHash, Credential: bytes.Repeat([]byte{0xd1}, 28),
			},
		},
		genesisCredential(lcommon.CredentialTypeScriptHash, 0xc2): {
			Type: conway.ConwayGenesisDelegateeTypeVote,
			DRep: lcommon.Drep{
				Type: lcommon.DrepTypeScriptHash, Credential: bytes.Repeat([]byte{0xd2}, 28),
			},
		},
		genesisCredential(lcommon.CredentialTypeAddrKeyHash, 0xc3): {
			Type: conway.ConwayGenesisDelegateeTypeVote,
			DRep: lcommon.Drep{Type: lcommon.DrepTypeAbstain},
		},
		genesisCredential(lcommon.CredentialTypeAddrKeyHash, 0xc4): {
			Type: conway.ConwayGenesisDelegateeTypeStake, PoolId: pool,
		},
		existing: {
			Type: conway.ConwayGenesisDelegateeTypeStake, PoolId: otherPool,
		},
	}
	return dreps, delegs
}

// requireGenesisPoolsMatch requires the set's pools to be the metadata
// store's registered pools, with their deposits held, and returns how many
// there are.
func requireGenesisPoolsMatch(
	t *testing.T,
	set *utxoset.Set,
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
) int {
	t.Helper()
	keys, err := ldb.ListPoolKeyHashes(txn)
	require.NoError(t, err)
	pkhs := make([]lcommon.PoolKeyHash, 0, len(keys))
	for _, k := range keys {
		pkhs = append(pkhs, lcommon.PoolKeyHash(k))
	}
	pools, err := ldb.GetPools(t.Context(), pkhs, txn)
	require.NoError(t, err)
	deposits, err := ldb.GetPoolDepositsHeld(pkhs, txn)
	require.NoError(t, err)
	text := func(pool []byte, r *models.PoolRegistration, d *uint64) string {
		if r == nil {
			return fmt.Sprintf("%x none", pool)
		}
		var owners []string
		for _, o := range r.Owners {
			owners = append(owners, fmt.Sprintf("%x", o.KeyHash))
		}
		slices.Sort(owners)
		deposit := "-"
		if d != nil {
			deposit = fmt.Sprint(*d)
		}
		return fmt.Sprintf(
			"%x slot=%d vrf=%x pledge=%d cost=%d margin=%v reward=%d/%x meta=%s/%x owners=%v relays=%d deposit=%s",
			pool,
			r.AddedSlot,
			r.VrfKeyHash,
			r.Pledge,
			r.Cost,
			r.Margin,
			r.RewardAccountCredentialTag,
			r.RewardAccount,
			r.MetadataUrl,
			r.MetadataHash,
			owners,
			len(r.Relays),
			deposit,
		)
	}
	var want, got []string
	for i := range pools {
		latest, _ := ledger.PoolLatestEvents(&pools[i])
		var d *uint64
		if v, ok := deposits[string(pools[i].PoolKeyHash)]; ok {
			d = &v
		}
		want = append(
			want,
			text(pools[i].PoolKeyHash, ledger.CleanPoolRegistration(latest), d),
		)
	}
	require.NoError(t, set.PoolStates(func(st *ledger.PoolState) error {
		require.Nil(t, st.Future)
		require.Nil(t, st.RetiringEpoch)
		got = append(got, text(st.Pool, st.Params, st.DepositHeld))
		return nil
	}))
	require.ElementsMatch(t, want, got, "pools")
	return len(want)
}

// requireGenesisReadsMatch requires the ledger's reads of the import
// baseline and witness slot of every account to be the metadata store's, and
// returns how many accounts hold a baseline and how many a witness.
func requireGenesisReadsMatch(
	t *testing.T,
	scenario ledger.GenesisScenario,
	store ledgerdb.LedgerDB,
) (baselines, witnesses int) {
	t.Helper()
	var refs []models.StakeCredentialRef
	require.NoError(
		t,
		store.IterateAccounts(nil, func(a *models.Account) error {
			refs = append(
				refs,
				models.NewStakeCredentialRef(a.CredentialTag, a.StakingKey),
			)
			return nil
		}),
	)
	ldb := scenario.LedgerDB()
	for _, ref := range refs {
		want, err := store.GetAccountImportRegistrationByCredential(
			t.Context(), ref.Tag, ref.Key, nil,
		)
		require.NoError(t, err)
		got, err := ldb.GetAccountImportRegistrationByCredential(
			t.Context(), ref.Tag, ref.Key, nil,
		)
		require.NoError(t, err, "baseline of %x", ref.Key)
		require.Equal(t, want, got, "baseline of %x", ref.Key)
		if want != nil {
			baselines++
		}
	}
	want, err := store.AccountLastWitnessSlots(
		t.Context(),
		refs,
		math.MaxInt64,
		nil,
	)
	require.NoError(t, err)
	got, err := ldb.AccountLastWitnessSlots(
		t.Context(),
		refs,
		math.MaxInt64,
		nil,
	)
	require.NoError(t, err, "witness slots")
	require.Equal(t, want, got, "witness slots")
	return baselines, len(want)
}

// TestGenesisStateIsComputedInTheSet requires the genesis staking and
// governance writes to give the set the accounts, DReps, DRep deposits,
// delegator links and pools the metadata store holds, and the ledger's reads
// of the import baselines and witness slots they leave to agree with it,
// without reading any of it back from the metadata store.
func TestGenesisStateIsComputedInTheSet(t *testing.T) {
	t.Parallel()
	for _, reads := range []string{ledger.UtxoSetReadsVerify, ledger.UtxoSetReadsSet} {
		t.Run(reads, func(t *testing.T) {
			t.Parallel()
			set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
			require.NoError(t, err)
			t.Cleanup(func() { _ = set.Close() })
			scenario := ledger.NewGenesisScenarioWithReadsForTest(t, set, reads)
			store := dbadapter.New(scenario.DB)
			require.NoError(t, set.LoadLedgerState(t.Context(), store))
			hidden := slices.Clone(genesisReadBackNames)
			if reads == ledger.UtxoSetReadsSet {
				hidden = append(hidden,
					"GetAccountImportRegistrationByCredential",
					"AccountLastWitnessSlots")
			}
			reached := scenario.HideStoreReads(hidden...)
			scenario.CreateGenesis()
			var staker []byte
			require.NoError(
				t,
				store.IterateAccounts(nil, func(a *models.Account) error {
					staker = a.StakingKey
					return nil
				}),
			)
			require.NotNil(t, staker, "genesis staking made no account")
			dreps, delegs := genesisGovernance(staker)
			scenario.SetGenesisGovernance(dreps, delegs)
			require.Zero(t, reached(),
				"the genesis writes read the metadata store back")

			txn := store.Begin(t.Context(), false)
			defer txn.Release()
			accounts := requireAccountsMatch(t, set, store, txn)
			require.Equal(t, len(delegs), accounts)
			nd, nl := requireDrepStateMatches(t, set, store, txn)
			require.Equal(t, len(dreps), nd)
			require.Equal(t, 2, nl, "the two delegations to a credential DRep")
			require.Positive(t, requireGenesisPoolsMatch(t, set, store, txn))
			for cred := range dreps {
				want, err := store.GetDrepLastRegistrationDeposit(
					t.Context(), uint8(cred.CredType), cred.Credential[:], txn,
				)
				require.NoError(t, err)
				require.NotNil(t, want)
				got, err := set.DRepDeposit(
					uint8(cred.CredType),
					cred.Credential[:],
				)
				require.NoError(t, err)
				require.Equal(
					t,
					want,
					got,
					"deposit of DRep %x",
					cred.Credential[:],
				)
			}

			baselines, witnesses := requireGenesisReadsMatch(t, scenario, store)
			require.Equal(t, 1, baselines,
				"only the Shelley genesis staker has an import baseline")
			require.Equal(t, len(delegs), witnesses,
				"each Conway genesis delegation is a witness at slot 0")
			if reads == ledger.UtxoSetReadsVerify {
				verified, mismatches := scenario.RegistrationReads()
				require.Positive(t, verified, "no baseline read was compared")
				require.Zero(t, mismatches)
				verified, mismatches = scenario.AccountReads()
				require.Positive(t, verified, "no witness read was compared")
				require.Zero(t, mismatches)
			}
		})
	}
}
