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
	"errors"
	"fmt"
	"maps"
	"math"
	"math/big"
	"net"
	"slices"
	"strconv"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/ledger/epochengine"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// networkStateLister lists every network state row a set holds.
type networkStateLister interface {
	NetworkStates() ([]models.NetworkState, error)
}

// FamilyLoaded reports whether set holds every marker the drain of family
// reads.
func FamilyLoaded(
	f nodesettings.Family,
	set UtxoSetWriter,
) (bool, error) {
	//nolint:exhaustive // only graduated families have a drain
	switch f {
	case nodesettings.FamilyNonces:
		r, ok := set.(BlockNonceReader)
		if !ok {
			return false, nil
		}
		return r.NoncesLoaded()
	case nodesettings.FamilyNetworkDonations:
		r, ok := set.(NetworkStateReader)
		if !ok {
			return false, nil
		}
		return r.NetworkStateLoaded()
	case nodesettings.FamilyEpochs, nodesettings.FamilyEraSwitch:
		r, ok := set.(EpochStateReader)
		if !ok {
			return false, nil
		}
		return r.EpochStateLoaded()
	case nodesettings.FamilySummaries:
		r, ok := set.(EpochSummaryReader)
		if !ok {
			return false, nil
		}
		return r.EpochSummariesLoaded()
	case nodesettings.FamilyLiveStake:
		return liveStakeLoaded(set)
	case nodesettings.FamilyCommitteeConstitution:
		r, ok := set.(CommitteeReader)
		if !ok {
			return false, nil
		}
		return r.CommitteeLoaded()
	case nodesettings.FamilyOpCerts:
		r, ok := set.(OpCertReader)
		if !ok {
			return false, nil
		}
		return r.OpCertsLoaded()
	case nodesettings.FamilyGovernance:
		r, ok := set.(GovernanceReader)
		m, ok2 := set.(StateMarkReader)
		if !ok || !ok2 {
			return false, nil
		}
		loaded, err := r.GovernanceLoaded()
		if err != nil || !loaded {
			return false, err
		}
		return m.RatifyPendingLoaded()
	case nodesettings.FamilyPParamsUpdates:
		pr, ok := set.(PParamsReader)
		ur, ok2 := set.(PParamUpdateReader)
		mr, ok3 := set.(SyncMarkerReader)
		if !ok || !ok2 || !ok3 {
			return false, nil
		}
		for _, loaded := range []func() (bool, error){
			pr.PParamsLoaded, ur.PParamUpdatesLoaded, mr.SyncMarkersLoaded,
		} {
			ok, err := loaded()
			if err != nil || !ok {
				return false, err
			}
		}
		return true, nil
	case nodesettings.FamilyMIRGenesis:
		r, ok := set.(CertHistoryReader)
		if !ok {
			return false, nil
		}
		return r.GenesisMIRLoaded()
	case nodesettings.FamilyDReps:
		r, ok := set.(DRepReader)
		if !ok {
			return false, nil
		}
		return r.DrepStateLoaded()
	case nodesettings.FamilyAccounts:
		return accountsLoaded(set)
	case nodesettings.FamilyPoolsHistory:
		state, ok := set.(PoolReader)
		hist, ok2 := set.(PoolHistoryReader)
		if !ok || !ok2 {
			return false, nil
		}
		if _, ok := set.(PoolHistoryLister); !ok {
			return false, nil
		}
		stateLoaded, err := state.PoolsLoaded()
		if err != nil || !stateLoaded {
			return false, err
		}
		return hist.PoolHistoryLoaded()
	case nodesettings.FamilyRewardInputs:
		r, ok := set.(RewardInputsReader)
		if !ok {
			return false, nil
		}
		return r.RewardInputsLoaded()
	case nodesettings.FamilyRewardBlocksFees:
		r, ok := set.(RewardBlocksReader)
		if !ok {
			return false, nil
		}
		loaded, err := r.RewardBlocksLoaded()
		if err != nil || !loaded {
			return false, err
		}
		imported, ok := set.(ImportedBlocksReader)
		if !ok {
			return false, nil
		}
		return imported.ImportedBlocksLoaded()
	case nodesettings.FamilyRewardOutputs:
		derived, ok := set.(RewardDerivedReader)
		if !ok {
			return false, nil
		}
		loaded, err := derived.RewardDerivedLoaded()
		if err != nil || !loaded {
			return false, err
		}
		pending, ok := set.(RewardOutputReader)
		if !ok {
			return false, nil
		}
		return pending.RewardOutputsLoaded()
	default:
		return false, fmt.Errorf("no drain markers for family %s", f)
	}
}

// VerifyFamily compares every row of family in set with ldb's SQL rows and
// returns an error on the first difference.
func VerifyFamily(
	ctx context.Context,
	f nodesettings.Family,
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
) error {
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	//nolint:exhaustive // only graduated families have a drain
	switch f {
	case nodesettings.FamilyNonces:
		return verifyNonces(ldb, set, txn)
	case nodesettings.FamilyNetworkDonations:
		return verifyNetworkDonations(ldb, set, txn)
	case nodesettings.FamilyEpochs:
		return verifyEpochs(ldb, set, txn)
	case nodesettings.FamilySummaries:
		return verifySummaries(ldb, set, txn)
	case nodesettings.FamilyEraSwitch:
		return verifyEraSwitch(ldb, set, txn)
	case nodesettings.FamilyLiveStake:
		return verifyLiveStakeRows(ldb, set, txn)
	case nodesettings.FamilyCommitteeConstitution:
		return verifyCommitteeFamily(ctx, ldb, set, txn)
	case nodesettings.FamilyOpCerts:
		return verifyOpCertFamily(ctx, ldb, set, txn)
	case nodesettings.FamilyGovernance:
		if err := verifyGovernance(ctx, ldb, set, txn); err != nil {
			return err
		}
		return verifyRatifyPending(ldb, set, txn)
	case nodesettings.FamilyPParamsUpdates:
		return verifyPParams(ldb, set, txn)
	case nodesettings.FamilyMIRGenesis:
		return verifyGenesisDelegations(ldb, set, txn)
	case nodesettings.FamilyDReps:
		return verifyDReps(ctx, ldb, set, txn)
	case nodesettings.FamilyAccounts:
		return verifyAccounts(ctx, ldb, set, txn)
	case nodesettings.FamilyPoolsHistory:
		return verifyPools(ctx, ldb, set, txn)
	case nodesettings.FamilyRewardBlocksFees:
		return verifyRewardBlocksFees(ctx, ldb, set)
	case nodesettings.FamilyRewardInputs:
		return verifyRewardInputs(ctx, ldb, set)
	case nodesettings.FamilyRewardOutputs:
		return verifyRewardOutputRows(ctx, ldb, set)
	default:
		return fmt.Errorf("no verification for family %s", f)
	}
}

func verifyNonces(
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
	txn ledgerdb.Txn,
) error {
	r, ok := set.(BlockNonceReader)
	if !ok {
		return errors.New("the set does not serve block nonces")
	}
	stored, err := ldb.GetBlockNoncesInSlotRange(0, math.MaxInt64, txn)
	if err != nil {
		return err
	}
	fromSet, err := r.BlockNoncesInSlotRange(0, math.MaxInt64)
	if err != nil {
		return err
	}
	view := nonceView{}
	for _, n := range stored {
		view[nonceKeyOf(n.Slot, n.Hash)] = n
	}
	want := view.sorted()
	if !sameNonces(want, fromSet) {
		return fmt.Errorf(
			"block nonces differ between the metadata store and the set: %s",
			nonceDiff(want, fromSet),
		)
	}
	return nil
}

func verifyNetworkDonations(
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
	txn ledgerdb.Txn,
) error {
	lister, ok := set.(networkStateLister)
	r, ok2 := set.(NetworkStateReader)
	if !ok || !ok2 {
		return errors.New("the set does not serve network state")
	}
	var stored []models.NetworkState
	for slot := uint64(math.MaxInt64); ; {
		row, err := ldb.GetNetworkStateAsOfSlot(slot, txn)
		if err != nil {
			return err
		}
		if row == nil {
			break
		}
		stored = append(stored, *row)
		if row.Slot == 0 {
			break
		}
		slot = row.Slot - 1
	}
	slices.Reverse(stored)
	fromSet, err := lister.NetworkStates()
	if err != nil {
		return err
	}
	if !slices.EqualFunc(stored, fromSet, func(a, b models.NetworkState) bool {
		return sameNetworkState(&a, &b)
	}) {
		return fmt.Errorf(
			"network state differs between the metadata store and the set: "+
				"%d rows against %d", len(stored), len(fromSet),
		)
	}
	donations, err := ldb.ListNetworkDonations(txn)
	if err != nil {
		return err
	}
	setDonations, err := r.NetworkDonations()
	if err != nil {
		return err
	}
	if !slices.Equal(donations, setDonations) {
		return fmt.Errorf(
			"network donations differ between the metadata store and the set: "+
				"%d rows against %d", len(donations), len(setDonations),
		)
	}
	return nil
}

func verifyEpochs(
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
	txn ledgerdb.Txn,
) error {
	r, ok := set.(EpochStateReader)
	if !ok {
		return errors.New("the set does not serve epochs")
	}
	stored, err := ldb.GetEpochs(txn)
	if err != nil {
		return err
	}
	fromSet, err := r.Epochs()
	if err != nil {
		return err
	}
	if !sameEpochs(stored, fromSet) {
		return fmt.Errorf(
			"epochs differ between the metadata store and the set: "+
				"%d rows against %d", len(stored), len(fromSet),
		)
	}
	return nil
}

// verifySummaries compares the summary of every epoch up to three past the
// newest epoch row, the bound the set loads them by, and the highest epoch
// the metadata store holds a summary for, which catches a row beyond it.
func verifySummaries(
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
	txn ledgerdb.Txn,
) error {
	lister, ok := set.(EpochSummaryLister)
	if !ok {
		return errors.New("the set does not serve epoch summaries")
	}
	epochs, err := ldb.GetEpochs(txn)
	if err != nil {
		return err
	}
	var hi uint64
	for _, e := range epochs {
		hi = max(hi, e.EpochId)
	}
	var stored []models.EpochSummary
	for epoch := uint64(0); epoch <= hi+3; epoch++ {
		sm, err := ldb.GetEpochSummary(epoch, txn)
		if err != nil {
			return err
		}
		if sm != nil {
			stored = append(stored, *sm)
		}
	}
	fromSet, err := lister.EpochSummaries()
	if err != nil {
		return err
	}
	if !slices.EqualFunc(stored, fromSet, func(a, b models.EpochSummary) bool {
		return sameSummaryPtr(&a, &b)
	}) {
		return fmt.Errorf(
			"epoch summaries differ between the metadata store and the set: "+
				"%d rows against %d", len(stored), len(fromSet),
		)
	}
	latest, err := ldb.GetLatestEpochSummary(txn)
	if err != nil {
		return err
	}
	if latest != nil && (len(fromSet) == 0 ||
		fromSet[len(fromSet)-1].Epoch != latest.Epoch) {
		return fmt.Errorf(
			"epoch summaries differ between the metadata store and the set: "+
				"store holds one for epoch %d", latest.Epoch,
		)
	}
	return nil
}

func verifyEraSwitch(
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
	txn ledgerdb.Txn,
) error {
	r, ok := set.(EpochStateReader)
	if !ok {
		return errors.New("the set does not serve the era switch")
	}
	value, err := ldb.GetSyncState(database.MidEpochEraSwitchSyncKey, txn)
	if err != nil {
		return err
	}
	var stored *database.MidEpochEraSwitch
	if value != "" {
		sw, err := database.ParseMidEpochEraSwitch(value)
		if err != nil {
			return err
		}
		stored = &sw
	}
	fromSet, err := r.EraSwitch()
	if err != nil {
		return err
	}
	if (stored == nil) != (fromSet == nil) ||
		stored != nil && *stored != *fromSet {
		return fmt.Errorf(
			"era switch differs between the metadata store and the set: "+
				"%v against %v", stored, fromSet,
		)
	}
	return nil
}

// verifyCommitteeFamily compares every committee, quorum, constitution,
// authorization and resignation row any read selects: each member row, the
// latest quorum row, the current constitution, each cold credential's latest
// authorization and its latest resignation.
func verifyCommitteeFamily(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
	txn ledgerdb.Txn,
) error {
	r, ok := set.(CommitteeReader)
	if !ok {
		return errors.New("the set does not serve the committee")
	}
	st, err := r.CommitteeStateAt(nil)
	if err != nil {
		return err
	}
	differ := func(what string) error {
		return fmt.Errorf(
			"%s differ between the metadata store and the set", what,
		)
	}
	members, err := ldb.GetCommitteeMembersIncludeDeleted(ctx, txn)
	if err != nil {
		return err
	}
	if !sameMembers(members, st.Members) {
		return differ("committee members")
	}
	quorum, err := ldb.LatestCommitteeQuorum(ctx, txn)
	if err != nil {
		return err
	}
	var wantQuorum *big.Rat
	if quorum != nil && quorum.Quorum != nil {
		wantQuorum = quorum.Quorum.Rat
	}
	if !sameRat(wantQuorum, st.LatestQuorum()) {
		return differ("committee quorum")
	}
	constitution, err := ldb.GetConstitution(txn)
	if err != nil {
		return err
	}
	if !sameConstitution(constitution, st.CurrentConstitution()) {
		return differ("constitution")
	}
	auths, err := ldb.GetCommitteeHotAuthorizationsSince(ctx, 0, txn)
	if err != nil {
		return err
	}
	if !sameAuths(auths, st.AuthsSince(0)) {
		return differ("committee hot-key authorizations")
	}
	resigns, err := ldb.LatestCommitteeResignations(ctx, txn)
	if err != nil {
		return err
	}
	want := make(map[string]uint64, len(resigns))
	for _, rs := range resigns {
		want[models.CommitteeCredential{
			CredentialTag: rs.ColdCredentialTag,
			Credential:    rs.ColdCredential,
		}.Key()] = rs.AddedSlot
	}
	got := make(map[string]uint64, len(st.Resigns))
	for _, rs := range st.Resigns {
		got[models.CommitteeCredential{
			CredentialTag: rs.ColdCredentialTag,
			Credential:    rs.ColdCredential,
		}.Key()] = rs.AddedSlot
	}
	if !maps.Equal(want, got) {
		return differ("committee resignations")
	}
	return nil
}

// verifyOpCertFamily compares each pool's highest sequence and latest
// observation slot.
func verifyOpCertFamily(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
	txn ledgerdb.Txn,
) error {
	r, ok := set.(OpCertReader)
	if !ok {
		return errors.New("the set does not serve op-cert counters")
	}
	counters, err := ldb.LatestPoolOpCertCounters(ctx, txn)
	if err != nil {
		return err
	}
	want := make(map[string]OpCertState, len(counters))
	for _, c := range counters {
		want[string(c.PoolKeyHash)] = OpCertState{
			Pool: c.PoolKeyHash, Sequence: c.Sequence, Slot: c.Slot,
			LastSequence: c.LastSequence,
		}
	}
	got := make(map[string]OpCertState, len(want))
	if err := r.OpCertStates(func(st *OpCertState) error {
		got[string(st.Pool)] = *st
		return nil
	}); err != nil {
		return err
	}
	if len(want) != len(got) {
		return fmt.Errorf(
			"op-cert counters differ between the metadata store and the "+
				"set: %d pools against %d", len(want), len(got),
		)
	}
	for pool, w := range want {
		g, ok := got[pool]
		if !ok || g.Sequence != w.Sequence || g.Slot != w.Slot ||
			g.LastSequence != w.LastSequence {
			return fmt.Errorf(
				"op-cert counters differ between the metadata store and the "+
					"set: pool %x", pool,
			)
		}
	}
	return nil
}

// verifyPParamsEraBound is one past the highest era id verifyPParams reads:
// it reaches past the known eras, so a row of an era the set never loaded
// refuses the drain.
const verifyPParamsEraBound = 32

// verifyPParams compares the protocol parameter rows of every era, the
// update proposals and the sync markers.
func verifyPParams(
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
	txn ledgerdb.Txn,
) error {
	pr, ok := set.(PParamsReader)
	ur, ok2 := set.(PParamUpdateReader)
	mr, ok3 := set.(SyncMarkerReader)
	if !ok || !ok2 || !ok3 {
		return errors.New(
			"the set does not serve protocol parameters, proposals and markers",
		)
	}
	for era := range uint(verifyPParamsEraBound) {
		stored, err := ldb.ListPParamsForEra(era, txn)
		if err != nil {
			return err
		}
		fromSet, err := pr.PParamsForEra(era)
		if err != nil {
			return err
		}
		if !samePParamsRows(stored, fromSet) {
			return fmt.Errorf(
				"protocol parameters of era %d differ between the metadata "+
					"store and the set: %d rows against %d",
				era, len(stored), len(fromSet),
			)
		}
	}
	stored, err := ldb.ListPParamUpdates(txn)
	if err != nil {
		return err
	}
	var fromSet []models.PParamUpdate
	err = ur.PParamUpdates(func(u models.PParamUpdate) error {
		u.GenesisHash = slices.Clone(u.GenesisHash)
		u.Cbor = slices.Clone(u.Cbor)
		fromSet = append(fromSet, u)
		return nil
	})
	if err != nil {
		return err
	}
	slices.Reverse(stored)
	if len(stored) != len(fromSet) || !samePParamUpdates(stored, fromSet) {
		return fmt.Errorf(
			"protocol parameter updates differ between the metadata store "+
				"and the set: %d rows against %d", len(stored), len(fromSet),
		)
	}
	for _, name := range []string{
		database.SyntheticV2CostModelSyncKey,
		database.SyntheticV2CostModelClearedEpochSyncKey,
	} {
		want, err := ldb.GetSyncState(name, txn)
		if err != nil {
			return err
		}
		got, _, err := mr.SyncMarker(name)
		if err != nil {
			return err
		}
		if want != got {
			return fmt.Errorf(
				"sync marker %s differs between the metadata store and the "+
					"set: %q against %q", name, want, got,
			)
		}
	}
	return nil
}

// verifyGenesisDelegations compares every genesis key delegation. The MIR
// certificate rows are not drained, so they are not compared.
func verifyGenesisDelegations(
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
	txn ledgerdb.Txn,
) error {
	r, ok := set.(CertHistoryReader)
	if !ok {
		return errors.New("the set does not serve genesis delegations")
	}
	stored, err := ldb.GetGenesisDelegationsInSlotRange(0, math.MaxInt64, txn)
	if err != nil {
		return err
	}
	fromSet, err := r.GenesisDelegations()
	if err != nil {
		return err
	}
	if !sameGenesisDelegations(stored, fromSet) {
		return fmt.Errorf(
			"genesis delegations differ between the metadata store and the "+
				"set: %d rows against %d", len(stored), len(fromSet),
		)
	}
	return nil
}

type drepLister interface {
	DRepReader
}

func missingReader(what string) error {
	return fmt.Errorf("the set does not serve %s", what)
}

func accountKey(tag uint8, key []byte) string {
	return string(rune(tag)) + string(key)
}

// verifyDReps compares every DRep, every live delegator link and the
// dormancy counter. The registration, update and retirement certificate
// tables are not drained, so they are not compared.
func verifyDReps(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
	txn ledgerdb.Txn,
) error {
	r, ok := set.(drepLister)
	if !ok {
		return missingReader("DReps")
	}
	stored := map[string]models.Drep{}
	err := ldb.IterateDreps(txn, func(d *models.Drep) error {
		stored[accountKey(d.CredentialTag, d.Credential)] = *d
		return nil
	})
	if err != nil {
		return err
	}
	fromSet := 0
	err = r.Dreps(func(d *models.Drep) error {
		fromSet++
		want, ok := stored[accountKey(d.CredentialTag, d.Credential)]
		if !ok || !sameDrep(want, *d) {
			return fmt.Errorf(
				"DReps differ between the metadata store and the set at %x",
				d.Credential,
			)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if fromSet != len(stored) {
		return fmt.Errorf(
			"DReps differ between the metadata store and the set: "+
				"%d rows against %d", len(stored), fromSet,
		)
	}
	links := map[string]struct{}{}
	err = ldb.IterateLiveDrepDelegators(
		txn,
		func(drep, stake models.StakeCredentialRef) error {
			links[drep.MapKey()+"|"+stake.MapKey()] = struct{}{}
			return nil
		},
	)
	if err != nil {
		return err
	}
	setLinks := 0
	err = r.DRepLinks(nil, func(drep, stake models.StakeCredentialRef) error {
		setLinks++
		if _, ok := links[drep.MapKey()+"|"+stake.MapKey()]; !ok {
			return fmt.Errorf(
				"DRep delegator links differ between the metadata store "+
					"and the set at %x", stake.Key,
			)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if setLinks != len(links) {
		return fmt.Errorf(
			"DRep delegator links differ between the metadata store and "+
				"the set: %d rows against %d", len(links), setLinks,
		)
	}
	dormant, err := ldb.GetDormantDRepEpochs(ctx, txn)
	if err != nil {
		return err
	}
	setDormant, err := r.DormantEpochs()
	if err != nil {
		return err
	}
	if dormant != setDormant {
		return fmt.Errorf(
			"the DRep dormancy counter differs between the metadata store "+
				"and the set: %d against %d", dormant, setDormant,
		)
	}
	return nil
}

// verifyAccounts compares every account, import baseline and witness slot,
// and the registration history of every credential with a registration
// event. The reward delta, delegation and DRep certificate tables are not
// drained, so they are not compared.
func verifyAccounts(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
	txn ledgerdb.Txn,
) error {
	lister, ok := set.(accountLister)
	if !ok {
		return missingReader("accounts")
	}
	stored := map[string]models.Account{}
	err := ldb.IterateAccounts(txn, func(a *models.Account) error {
		stored[accountKey(a.CredentialTag, a.StakingKey)] = *a
		return nil
	})
	if err != nil {
		return err
	}
	fromSet := 0
	err = lister.Accounts(func(a *models.Account) error {
		fromSet++
		want, ok := stored[accountKey(a.CredentialTag, a.StakingKey)]
		if !ok || !sameAccount(want, *a) {
			return fmt.Errorf(
				"accounts differ between the metadata store and the set at %x",
				a.StakingKey,
			)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if fromSet != len(stored) {
		return fmt.Errorf(
			"accounts differ between the metadata store and the set: "+
				"%d rows against %d", len(stored), fromSet,
		)
	}
	if err := verifyImportBaselines(ldb, set, txn); err != nil {
		return err
	}
	if err := verifyWitnessSlots(ldb, set, txn); err != nil {
		return err
	}
	if err := verifyInactivity(ctx, ldb, set, txn, stored); err != nil {
		return err
	}
	return verifyRegistrationHistory(ctx, ldb, set, txn)
}

// verifyInactivity compares the epoch the set's delegator inactivity
// activation ran at with the metadata store's, and requires every credential
// the store records as stamped to be marked on its account in the set. The
// set may mark more: its load also marks an account its journal shows active
// just before the activation, which a store that never recorded the stamp
// does not list. The store answers membership for named credentials only, so
// it is asked about every account it holds, as the load of the set asks it.
func verifyInactivity(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
	txn ledgerdb.Txn,
	accounts map[string]models.Account,
) error {
	r, ok := set.(StateMarkReader)
	lister, ok2 := set.(accountLister)
	if !ok || !ok2 {
		return missingReader("the delegator inactivity activation")
	}
	want, err := ldb.GetSyncState(
		database.DelegatorInactivityActivatedSyncKey, txn,
	)
	if err != nil {
		return err
	}
	epoch, ran, err := r.InactivityActivation(nil)
	if err != nil {
		return err
	}
	var got string
	if ran {
		got = strconv.FormatUint(epoch, 10)
	}
	if want != got {
		return fmt.Errorf(
			"the delegator inactivity activation differs between the "+
				"metadata store and the set: %q against %q", want, got,
		)
	}
	stamped := map[string]struct{}{}
	if want != "" {
		refs := make([]models.StakeCredentialRef, 0, len(accounts))
		for _, a := range accounts {
			refs = append(
				refs,
				models.NewStakeCredentialRef(a.CredentialTag, a.StakingKey),
			)
		}
		stamped, err = ldb.AccountInactivityActivationMembership(
			ctx, refs, txn,
		)
		if err != nil {
			return err
		}
	}
	marked := 0
	err = lister.Accounts(func(a *models.Account) error {
		ref := models.NewStakeCredentialRef(a.CredentialTag, a.StakingKey)
		if _, ok := stamped[ref.MapKey()]; ok && a.InactivityActivation {
			marked++
		}
		return nil
	})
	if err != nil {
		return err
	}
	if marked != len(stamped) {
		return fmt.Errorf(
			"the credentials the delegator inactivity activation stamped "+
				"differ between the metadata store and the set: the set "+
				"marks %d of the store's %d", marked, len(stamped),
		)
	}
	return nil
}

// verifyRatifyPending compares the set's pending ratification record with
// the metadata store's, byte for byte.
func verifyRatifyPending(
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
	txn ledgerdb.Txn,
) error {
	r, ok := set.(StateMarkReader)
	if !ok {
		return missingReader("the pending ratification record")
	}
	want, err := ldb.GetSyncState(pendingRatificationSyncKey, txn)
	if err != nil {
		return err
	}
	tip, err := ldb.GetTip(txn)
	if err != nil {
		return err
	}
	// The record of a boundary above the store's tip is not loaded: the
	// ledger applies that boundary again.
	if ratifyPendingSlot([]byte(want)) > tip.Point.Slot {
		want = ""
	}
	got, _, err := r.RatifyPending(nil)
	if err != nil {
		return err
	}
	if want != string(got) {
		return fmt.Errorf(
			"the pending ratification record differs between the metadata "+
				"store and the set: %q against %q", want, got,
		)
	}
	return nil
}

func verifyImportBaselines(
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
	txn ledgerdb.Txn,
) error {
	r, ok := set.(ImportBaselineReader)
	if !ok {
		return missingReader("import baselines")
	}
	return ldb.IterateAccountImportBaselines(
		txn,
		func(row models.AccountImportBaselineRow) error {
			b, err := r.ImportBaseline(
				models.NewStakeCredentialRef(row.CredentialTag, row.StakingKey),
			)
			if err != nil {
				return err
			}
			if b == nil || b.Active != row.Active ||
				b.AddedSlot != row.AddedSlot ||
				!sameDepositPtr(b.Deposit, row.Deposit) {
				return fmt.Errorf(
					"import baselines differ between the metadata store and "+
						"the set at %x", row.StakingKey,
				)
			}
			return nil
		},
	)
}

func verifyWitnessSlots(
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
	txn ledgerdb.Txn,
) error {
	r, ok := set.(WitnessReader)
	if !ok {
		return missingReader("witness slots")
	}
	greatest := map[string]uint64{}
	refs := map[string]models.StakeCredentialRef{}
	err := ldb.IterateAccountWitnessSlots(
		txn,
		func(ref models.StakeCredentialRef, slot uint64) error {
			greatest[ref.MapKey()] = max(greatest[ref.MapKey()], slot)
			refs[ref.MapKey()] = ref
			return nil
		},
	)
	if err != nil {
		return err
	}
	for k, ref := range refs {
		slot, found, err := r.LastWitnessSlot(ref, math.MaxUint64)
		if err != nil {
			return err
		}
		if !found || slot != greatest[k] {
			return fmt.Errorf(
				"witness slots differ between the metadata store and the "+
					"set at %x", ref.Key,
			)
		}
	}
	er, ok := set.(WitnessEffectiveReader)
	if !ok {
		return missingReader("witness effective slots")
	}
	effective := map[string]uint64{}
	err = ldb.IterateAccountWitnessEffectiveSlots(
		txn,
		func(ref models.StakeCredentialRef, _, eff uint64) error {
			effective[ref.MapKey()] = max(effective[ref.MapKey()], eff)
			return nil
		},
	)
	if err != nil {
		return err
	}
	for k, ref := range refs {
		slot, found, err := er.LastWitnessEffectiveSlot(ref, math.MaxUint64)
		if err != nil {
			return err
		}
		if !found || slot != effective[k] {
			return fmt.Errorf(
				"witness effective slots differ between the metadata store "+
					"and the set at %x", ref.Key,
			)
		}
	}
	return nil
}

func verifyRegistrationHistory(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
	txn ledgerdb.Txn,
) error {
	r, ok := set.(RegistrationHistoryReader)
	if !ok {
		return missingReader("registration history")
	}
	refs, err := ldb.GetStakeCredentialsWithRegistrationEvents(
		0, math.MaxInt64, txn,
	)
	if err != nil {
		return err
	}
	for _, ref := range refs {
		rows, err := ldb.GetAccountRegistrationHistoryByCredential(
			ctx, ref.Tag, ref.Key, 0, 0, "asc", txn,
		)
		if err != nil {
			return err
		}
		events, err := r.StakeHistory(ref)
		if err != nil {
			return err
		}
		same := len(rows) == len(events)
		for i := 0; same && i < len(rows); i++ {
			same = sameHistoryRow(rows[i], events[i].historyRow())
		}
		if !same {
			return fmt.Errorf(
				"registration history differs between the metadata store "+
					"and the set at %x", ref.Key,
			)
		}
	}
	return nil
}

// accountsLoaded reports whether the set holds every marker the accounts
// drain reads: the accounts, the import baselines, the witness slots and the
// registration history.
func accountsLoaded(set UtxoSetWriter) (bool, error) {
	a, ok := set.(AccountReader)
	if !ok {
		return false, nil
	}
	b, ok := set.(ImportBaselineReader)
	if !ok {
		return false, nil
	}
	w, ok := set.(WitnessReader)
	if !ok {
		return false, nil
	}
	h, ok := set.(RegistrationHistoryReader)
	if !ok {
		return false, nil
	}
	m, ok := set.(StateMarkReader)
	if !ok {
		return false, nil
	}
	for _, loaded := range []func() (bool, error){
		a.AccountsLoaded, b.ImportBaselinesLoaded, w.WitnessSlotsLoaded,
		h.RegistrationHistoryLoaded, m.InactivityLoaded,
	} {
		ok, err := loaded()
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

// verifyPools compares every pool of the metadata store with the set: the
// pools named, each one's registrations and retirements in order, and the
// state the set keeps for each registered pool.
func verifyPools(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
	txn ledgerdb.Txn,
) error {
	state, ok := set.(PoolReader)
	hist, ok2 := set.(PoolHistoryReader)
	lister, ok3 := set.(PoolHistoryLister)
	if !ok || !ok2 || !ok3 {
		return errors.New("the set does not serve pools")
	}
	names, err := ldb.ListPoolKeyHashes(txn)
	if err != nil {
		return err
	}
	slices.SortFunc(names, bytes.Compare)
	named, err := lister.PoolHistoryPools()
	if err != nil {
		return err
	}
	slices.SortFunc(named, bytes.Compare)
	if !slices.EqualFunc(names, named, bytes.Equal) {
		return fmt.Errorf(
			"pools differ between the metadata store and the set: "+
				"%d rows against %d", len(names), len(named),
		)
	}
	pkhs := make([]lcommon.PoolKeyHash, 0, len(names))
	for _, n := range names {
		pkhs = append(pkhs, lcommon.PoolKeyHash(n))
	}
	pools, err := ldb.GetPools(ctx, pkhs, txn)
	if err != nil {
		return err
	}
	if len(pools) != len(names) {
		return fmt.Errorf(
			"the metadata store lists %d pools and returns %d",
			len(names), len(pools),
		)
	}
	for i := range pools {
		events, err := hist.PoolHistory(pools[i].PoolKeyHash)
		if err != nil {
			return err
		}
		if err := samePoolHistory(&pools[i], events); err != nil {
			return fmt.Errorf(
				"pool %x differs between the metadata store and the set: %w",
				pools[i].PoolKeyHash, err,
			)
		}
	}
	return verifyPoolStates(ldb, state, txn, pkhs, pools)
}

// samePoolHistory compares a pool's rows, which are newest first, with its
// events, which are oldest first.
func samePoolHistory(pool *models.Pool, events []PoolEvent) error {
	var regs, rets []PoolEvent
	for _, e := range slices.Backward(events) {
		if e.Registration != nil {
			regs = append(regs, e)
		} else {
			rets = append(rets, e)
		}
	}
	if len(regs) != len(pool.Registration) {
		return fmt.Errorf(
			"%d registrations against %d", len(pool.Registration), len(regs),
		)
	}
	for i := range regs {
		if pool.Registration[i].EffectiveSlot != regs[i].EffectiveSlot ||
			!sameRegistrationFull(&pool.Registration[i], regs[i].Registration) {
			return fmt.Errorf("registration %d at slot %d", i, regs[i].Slot)
		}
	}
	if len(rets) != len(pool.Retirement) {
		return fmt.Errorf(
			"%d retirements against %d", len(pool.Retirement), len(rets),
		)
	}
	for i := range rets {
		if pool.Retirement[i].Epoch != rets[i].RetiringEpoch ||
			pool.Retirement[i].AddedSlot != rets[i].Slot ||
			pool.Retirement[i].EffectiveSlot != rets[i].EffectiveSlot {
			return fmt.Errorf("retirement %d at slot %d", i, rets[i].Slot)
		}
	}
	return nil
}

// sameRegistrationFull is samePoolRegistration with the relays and the Leios
// key, which the set keeps with a registration.
func sameRegistrationFull(a, b *models.PoolRegistration) bool {
	if !samePoolRegistration(a, b) ||
		!bytes.Equal(a.LeiosKeyPublic, b.LeiosKeyPublic) ||
		!bytes.Equal(a.LeiosKeyPossessionProof, b.LeiosKeyPossessionProof) ||
		len(a.Relays) != len(b.Relays) {
		return false
	}
	for i := range a.Relays {
		x, y := a.Relays[i], b.Relays[i]
		if x.Hostname != y.Hostname || x.Port != y.Port ||
			!sameIP(x.Ipv4, y.Ipv4) || !sameIP(x.Ipv6, y.Ipv6) ||
			(x.Type == nil) != (y.Type == nil) ||
			(x.Type != nil && *x.Type != *y.Type) {
			return false
		}
	}
	return true
}

func sameIP(a, b *net.IP) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

// verifyPoolStates compares the registered pools the set keeps with the ones
// the metadata store derives for the newest epoch: the registration in
// effect, a later one waiting, a pending retirement and the deposit held.
func verifyPoolStates(
	ldb ledgerdb.LedgerDB,
	state PoolReader,
	txn ledgerdb.Txn,
	pkhs []lcommon.PoolKeyHash,
	pools []models.Pool,
) error {
	epochs, err := ldb.GetEpochs(txn)
	if err != nil {
		return err
	}
	var cur models.Epoch
	for _, e := range epochs {
		if e.EpochId >= cur.EpochId {
			cur = e
		}
	}
	effective, err := ldb.GetPoolRegistrationsEffectiveForEpoch(
		pkhs, cur.StartSlot, cur.EpochId, math.MaxInt64, txn,
	)
	if err != nil {
		return err
	}
	deposits, err := ldb.GetPoolDepositsHeld(pkhs, txn)
	if err != nil {
		return err
	}
	byPool := make(map[string]*models.Pool, len(pools))
	for i := range pools {
		byPool[string(pools[i].PoolKeyHash)] = &pools[i]
	}
	want := make(map[string]PoolState, len(effective))
	for i := range effective {
		r := effective[i]
		st := PoolState{Pool: r.PoolKeyHash, Params: CleanPoolRegistration(&r)}
		if d, ok := deposits[string(r.PoolKeyHash)]; ok {
			st.DepositHeld = &d
		}
		if p, ok := byPool[string(r.PoolKeyHash)]; ok {
			latest, retiring := PoolLatestEvents(p)
			if PoolRegistrationSupersedes(latest, &r) {
				st.Future = CleanPoolRegistration(latest)
			}
			st.RetiringEpoch = retiring
		}
		want[string(r.PoolKeyHash)] = st
	}
	got := 0
	err = state.PoolStates(func(st *PoolState) error {
		got++
		w, ok := want[string(st.Pool)]
		if !ok {
			return fmt.Errorf(
				"pool %x is registered in the set only", st.Pool,
			)
		}
		if !samePoolState(&w, st) {
			return fmt.Errorf(
				"state of pool %x differs between the metadata store and the set",
				st.Pool,
			)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if got != len(want) {
		return fmt.Errorf(
			"registered pools differ between the metadata store and the set: "+
				"%d against %d", len(want), got,
		)
	}
	return nil
}

// verifyRewardBlocksFees compares every epoch of the set's block counts and
// fees with the metadata store's answer for the same epochs.
func verifyRewardBlocksFees(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
) error {
	r, ok := set.(RewardBlocksReader)
	if !ok {
		return errors.New("the set does not serve reward block counts")
	}
	want, err := RewardBlocksFromStore(ctx, ldb)
	if err != nil {
		return fmt.Errorf("read reward block counts from the store: %w", err)
	}
	epochs, err := r.RewardBlocksEpochs()
	if err != nil {
		return err
	}
	seen := make(map[uint64]struct{}, len(want))
	for _, epoch := range epochs {
		seen[epoch] = struct{}{}
		w, ok := want[epoch]
		if !ok {
			return fmt.Errorf(
				"the set holds reward block rows for epoch %d, "+
					"which the metadata store does not",
				epoch,
			)
		}
		got, err := r.RewardBlocksEpoch(epoch, nil)
		if err != nil {
			return err
		}
		if err := sameRewardEpochBlocks(epoch, w, got); err != nil {
			return err
		}
	}
	for epoch, w := range want {
		if _, ok := seen[epoch]; !ok && (w.CountsKnown || w.FeesKnown) {
			return fmt.Errorf(
				"the set holds no reward block rows for epoch %d", epoch,
			)
		}
	}
	return verifyImportedBlocks(ctx, ldb, set)
}

// verifyImportedBlocks compares the imported block counts the set retains
// with the metadata store's.
func verifyImportedBlocks(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
) error {
	r, ok := set.(ImportedBlocksReader)
	if !ok {
		return errors.New("the set does not serve imported block counts")
	}
	src, ok := ldb.(ledgerdb.ImportedBlockRowsReader)
	if !ok {
		return errors.New("the store cannot list imported block counts")
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	want, err := src.ImportedBlockRows(txn)
	if err != nil {
		return fmt.Errorf("read imported block counts from the store: %w", err)
	}
	got, err := r.ImportedBlocks()
	if err != nil {
		return err
	}
	if len(want) != len(got) {
		return fmt.Errorf(
			"imported block counts cover %d epochs in the store, %d in the set",
			len(want), len(got),
		)
	}
	for epoch, w := range want {
		g, ok := got[epoch]
		if !ok || g.Total != w.Total || g.CapturedSlot != w.CapturedSlot ||
			!maps.Equal(g.Pools, w.Pools) {
			return fmt.Errorf(
				"imported block counts of epoch %d differ between the store and the set",
				epoch,
			)
		}
	}
	return nil
}

func samePoolState(a, b *PoolState) bool {
	if (a.Params == nil) != (b.Params == nil) ||
		(a.Params != nil && !sameRegistrationFull(a.Params, b.Params)) ||
		(a.Future == nil) != (b.Future == nil) ||
		(a.Future != nil && !sameRegistrationFull(a.Future, b.Future)) ||
		(a.RetiringEpoch == nil) != (b.RetiringEpoch == nil) ||
		(a.RetiringEpoch != nil && *a.RetiringEpoch != *b.RetiringEpoch) ||
		(a.DepositHeld == nil) != (b.DepositHeld == nil) ||
		(a.DepositHeld != nil && *a.DepositHeld != *b.DepositHeld) {
		return false
	}
	return true
}

func sameRewardEpochBlocks(epoch uint64, want, got RewardEpochBlocks) error {
	switch {
	case want.CountsKnown != got.CountsKnown:
		return fmt.Errorf(
			"epoch %d block counts known: store %t, set %t",
			epoch, want.CountsKnown, got.CountsKnown,
		)
	case want.CountsKnown && want.Total != got.Total:
		return fmt.Errorf(
			"epoch %d block total: store %d, set %d",
			epoch, want.Total, got.Total,
		)
	case want.CountsKnown && want.Next != got.Next:
		return fmt.Errorf(
			"epoch %d first countable slot: store %d, set %d",
			epoch, want.Next, got.Next,
		)
	case want.CountsKnown && !sameCounts(want.Pools, got.Pools):
		return fmt.Errorf(
			"epoch %d per-pool block counts differ: store %v, set %v",
			epoch, want.Pools, got.Pools,
		)
	case want.AllKnown != got.AllKnown:
		return fmt.Errorf(
			"epoch %d overlay-inclusive block counts known: store %t, set %t",
			epoch, want.AllKnown, got.AllKnown,
		)
	case want.AllKnown && (want.AllTotal != got.AllTotal ||
		want.AllNext != got.AllNext || !sameCounts(want.AllPools, got.AllPools)):
		return fmt.Errorf(
			"epoch %d overlay-inclusive block counts differ: store %d %v, set %d %v",
			epoch,
			want.AllTotal,
			want.AllPools,
			got.AllTotal,
			got.AllPools,
		)
	case want.FeesKnown != got.FeesKnown:
		return fmt.Errorf(
			"epoch %d fees known: store %t, set %t",
			epoch, want.FeesKnown, got.FeesKnown,
		)
	case want.FeesKnown && want.Fees != got.Fees:
		return fmt.Errorf(
			"epoch %d fees: store %d, set %d", epoch, want.Fees, got.Fees,
		)
	}
	return nil
}

// verifyRewardInputs compares the set's ADA pots, snapshot headers, pool
// inputs and imported basis failures of every epoch, and the stake inputs of
// the epochs the set keeps them for, with the metadata store's.
func verifyRewardInputs(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
) error {
	r, ok := set.(RewardInputsReader)
	if !ok {
		return errors.New("the set does not serve reward inputs")
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	epochs, err := ldb.GetEpochs(txn)
	if err != nil {
		return fmt.Errorf("read epochs: %w", err)
	}
	var newest uint64
	for _, e := range epochs {
		newest = max(newest, e.EpochId)
	}
	for _, e := range epochs {
		if err := verifyRewardInputEpoch(
			ldb, txn, r, e.EpochId,
			e.EpochId+RewardInputEpochWindow >= newest,
		); err != nil {
			return fmt.Errorf("reward inputs of epoch %d: %w", e.EpochId, err)
		}
	}
	return verifyPendingStakeInputs(ldb, txn, r)
}

// verifyPendingStakeInputs compares the pending stake-input record, which a
// drain of the family deletes from the metadata store.
func verifyPendingStakeInputs(
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
	r RewardInputsReader,
) error {
	raw, err := ldb.GetSyncState(epochengine.RewardStakeInputsPendingKey, txn)
	if err != nil {
		return err
	}
	want, err := ParseRewardStakeInputsPending(raw)
	if err != nil {
		return err
	}
	got, err := r.RewardStakePending(nil)
	if err != nil {
		return err
	}
	byEpoch := func(a, b RewardStakeInputsPendingEntry) int {
		return cmp.Compare(a.Epoch, b.Epoch)
	}
	slices.SortFunc(want, byEpoch)
	slices.SortFunc(got, byEpoch)
	if !slices.Equal(want, got) {
		return fmt.Errorf(
			"pending stake inputs differ: store %+v, set %+v", want, got,
		)
	}
	return nil
}

func verifyRewardInputEpoch(
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
	r RewardInputsReader,
	epoch uint64,
	withStakes bool,
) error {
	pots, err := ldb.GetRewardAdaPots(epoch, txn)
	if err != nil {
		return err
	}
	gotPots, err := r.RewardPots(epoch, nil)
	if err != nil {
		return err
	}
	if !samePots(pots, gotPots) {
		return fmt.Errorf("ADA pots differ: store %+v, set %+v", pots, gotPots)
	}
	snap, err := ldb.GetRewardSnapshot(epoch, rewardSnapshotMark, txn)
	if err != nil {
		return err
	}
	gotSnap, err := r.RewardSnapshotRow(epoch, rewardSnapshotMark, nil)
	if err != nil {
		return err
	}
	if !sameSnapshot(snap, gotSnap) {
		return fmt.Errorf(
			"snapshot differs: store %+v, set %+v", snap, gotSnap,
		)
	}
	reason, err := ldb.GetRewardSeedFailure(epoch, rewardSnapshotMark, txn)
	if err != nil {
		return err
	}
	gotReason, _, err := r.RewardFailureRow(epoch, rewardSnapshotMark, nil)
	if err != nil {
		return err
	}
	if reason != gotReason {
		return fmt.Errorf(
			"seed failure differs: store %q, set %q", reason, gotReason,
		)
	}
	pools, err := ldb.GetRewardPoolInputs(epoch, txn)
	if err != nil {
		return err
	}
	gotPools, err := r.RewardPoolRows(epoch, nil)
	if err != nil {
		return err
	}
	if !samePoolInputs(pools, gotPools) {
		return fmt.Errorf(
			"pool inputs differ: store %d rows, set %d rows",
			len(pools), len(gotPools),
		)
	}
	if !withStakes {
		return nil
	}
	stakes, err := ldb.GetRewardStakeInputs(epoch, txn)
	if err != nil {
		return err
	}
	gotStakes, err := r.RewardStakeRows(epoch, nil, nil, nil)
	if err != nil {
		return err
	}
	if !sameStakeInputs(stakes, gotStakes) {
		return fmt.Errorf(
			"stake inputs differ: store %d rows, set %d rows",
			len(stakes), len(gotStakes),
		)
	}
	return nil
}

// verifyRewardOutputRows compares the set's pool outputs, account outputs and
// cursors of the epochs it keeps them for, its credited rounds and its
// unfolded pending credits with the metadata store's.
func verifyRewardOutputRows(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
) error {
	derived, ok := set.(RewardDerivedReader)
	if !ok {
		return errors.New("the set does not serve derived reward rows")
	}
	pending, ok := set.(RewardOutputReader)
	if !ok {
		return errors.New("the set does not serve reward outputs")
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	epochs, err := ldb.GetEpochs(txn)
	if err != nil {
		return fmt.Errorf("read epochs: %w", err)
	}
	var newest uint64
	for _, e := range epochs {
		newest = max(newest, e.EpochId)
	}
	for _, e := range epochs {
		if e.EpochId+RewardInputEpochWindow < newest {
			continue
		}
		if err := verifyDerivedEpoch(ldb, txn, derived, e.EpochId); err != nil {
			return fmt.Errorf("reward outputs of epoch %d: %w", e.EpochId, err)
		}
	}
	rounds, err := ldb.GetPendingRewardCreditRounds(txn)
	if err != nil {
		return err
	}
	gotRounds, err := pending.RewardRounds(nil)
	if err != nil {
		return err
	}
	if !slices.EqualFunc(rounds, gotRounds, sameRewardRound) {
		return fmt.Errorf(
			"credited rounds differ: store %v, set %v", rounds, gotRounds,
		)
	}
	var storeRows int
	var iterErr error
	err = ldb.IterateUnfoldedRewardAccountOutputs(
		txn,
		func(o *models.RewardAccountOutput) error {
			storeRows++
			rows, err := pending.CredentialRewardOutputs(
				o.CredentialTag,
				o.StakingKey,
				nil,
			)
			if err != nil {
				return err
			}
			if !slices.ContainsFunc(
				rows,
				func(r models.RewardAccountOutput) bool {
					return sameRewardOutput(&r, o)
				},
			) {
				iterErr = fmt.Errorf(
					"unfolded output %d/%x of epoch %d is missing or differs in the set",
					o.CredentialTag,
					o.StakingKey,
					o.Epoch,
				)
				return iterErr
			}
			return nil
		},
	)
	if err != nil {
		return err
	}
	setEpochs, err := pending.RewardOutputEpochs(nil)
	if err != nil {
		return err
	}
	var setRows int
	for _, e := range setEpochs {
		refs, err := pending.EpochRewardOutputs(e, math.MaxInt32, nil)
		if err != nil {
			return err
		}
		setRows += len(refs)
	}
	if setRows != storeRows {
		return fmt.Errorf(
			"unfolded outputs differ: store %d rows, set %d rows",
			storeRows, setRows,
		)
	}
	return nil
}

func verifyDerivedEpoch(
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
	r RewardDerivedReader,
	epoch uint64,
) error {
	pools, err := ldb.GetRewardPoolOutputs(epoch, txn)
	if err != nil {
		return err
	}
	gotPools, err := r.RewardPoolOutputRows(epoch, nil)
	if err != nil {
		return err
	}
	if !slices.EqualFunc(pools, gotPools, samePoolOutput) {
		return fmt.Errorf(
			"pool outputs differ: store %d rows, set %d rows",
			len(pools), len(gotPools),
		)
	}
	accounts, err := ldb.GetRewardAccountOutputs(epoch, txn)
	if err != nil {
		return err
	}
	gotAccounts, err := r.RewardAccountOutputRows(epoch, nil)
	if err != nil {
		return err
	}
	if !slices.EqualFunc(accounts, gotAccounts, sameRewardOutput) {
		return fmt.Errorf(
			"account outputs differ: store %d rows, set %d rows",
			len(accounts), len(gotAccounts),
		)
	}
	cursor, err := ldb.GetSyncState(epochengine.RewardPrecomputeCursorKey(epoch), txn)
	if err != nil {
		return err
	}
	raw, _, err := r.RewardCursorRow(epoch, nil)
	if err != nil {
		return err
	}
	if cursor != string(raw) {
		return fmt.Errorf(
			"precompute cursor differs: store %q, set %q",
			cursor,
			raw,
		)
	}
	return nil
}
