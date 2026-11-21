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

package utxoset_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// DelegationHistoryRefs lists each credential with a delegation once, whatever
// the number of its delegations, and keeps a script credential apart from a
// key credential of the same hash.
func TestDelegationHistoryRefsListsEachCredentialOnce(t *testing.T) {
	t.Parallel()
	set := openAccountSet(t)
	a := models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x21}, 28))
	b := models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x22}, 28))
	script := models.NewStakeCredentialRef(1, a.Key)
	pool := bytes.Repeat([]byte{0x31}, 28)
	for i, ref := range []models.StakeCredentialRef{a, a, b, script, a} {
		slot := uint64(10 + i) //nolint:gosec // a test index
		applyAt(t, set, slot, ledger.LedgerChanges{
			DelegationEvents: []ledger.DelegationEventChange{{
				Ref:   ref,
				Event: ledger.DelegationEvent{Slot: slot, Pool: pool},
			}},
		})
	}
	refs, err := set.DelegationHistoryRefs()
	require.NoError(t, err)
	require.ElementsMatch(t, []models.StakeCredentialRef{a, b, script}, refs)
}

// EachWitness reports the applied and the effective slot of every witness of
// one credential and no other's.
func TestEachWitnessReportsBothSlots(t *testing.T) {
	t.Parallel()
	set := openAccountSet(t)
	a := models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x41}, 28))
	b := models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x42}, 28))
	applyAt(t, set, 30, ledger.LedgerChanges{Witnesses: []ledger.WitnessChange{
		{Slot: 30, Ref: a, EffectiveSlot: 12},
		{Slot: 30, Ref: b},
	}})
	applyAt(t, set, 31, ledger.LedgerChanges{Witnesses: []ledger.WitnessChange{
		{Slot: 31, Ref: a},
	}})
	var got [][2]uint64
	require.NoError(t, set.EachWitness(a, func(slot, effective uint64) error {
		got = append(got, [2]uint64{slot, effective})
		return nil
	}))
	require.Equal(t, [][2]uint64{{30, 12}, {31, 31}}, got)
}

// StakeCredentialsInRange lists each credential with an event whose applied
// slot is in the inclusive range once, keeps a script credential apart from a
// key credential of the same hash, and lists registrations and
// deregistrations alike.
func TestStakeCredentialsInRangeListsEachCredentialOnceInclusive(t *testing.T) {
	t.Parallel()
	set := openAccountSet(t)
	a := models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x51}, 28))
	b := models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x52}, 28))
	script := models.NewStakeCredentialRef(1, a.Key)
	events := []struct {
		ref        models.StakeCredentialRef
		slot       uint64
		registered bool
	}{
		{a, 10, true}, {b, 11, true}, {a, 12, false}, {script, 13, true},
		{a, 14, true},
	}
	for _, e := range events {
		applyAt(t, set, e.slot, ledger.LedgerChanges{
			StakeEvents: []ledger.StakeEventChange{{
				Ref: e.ref,
				Event: ledger.StakeEvent{
					Slot: e.slot, Registered: e.registered,
					Class: ledger.StakeClassOther,
				},
			}},
		})
	}
	for _, c := range []struct {
		name     string
		from, to uint64
		want     []models.StakeCredentialRef
	}{
		{"all", 0, 100, []models.StakeCredentialRef{a, b, script}},
		{"first slot only", 10, 10, []models.StakeCredentialRef{a}},
		{"both bounds are inclusive", 11, 13, []models.StakeCredentialRef{a, b, script}},
		{"a deregistration counts", 12, 12, []models.StakeCredentialRef{a}},
		{"script apart from key", 13, 13, []models.StakeCredentialRef{script}},
		{"above the last", 15, 100, nil},
		{"below the first", 0, 9, nil},
		{"empty range", 14, 10, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := set.StakeCredentialsInRange(c.from, c.to)
			require.NoError(t, err)
			require.ElementsMatch(t, c.want, got)
		})
	}
	_, err := set.Rollback(ocommon.NewPoint(11, bytes.Repeat([]byte{11}, 32)))
	require.NoError(t, err)
	rolled, err := set.StakeCredentialsInRange(0, 100)
	require.NoError(t, err)
	require.ElementsMatch(t, []models.StakeCredentialRef{a, b}, rolled)
}

// The pools and links a change after a slot removed are listed as changed
// after it, which a staged rollback to that slot restores.
func TestChangedAfterListsRemovedPoolsAndClosedLinks(t *testing.T) {
	t.Parallel()
	set := openAccountSet(t)
	pool := bytes.Repeat([]byte{0x61}, 28)
	kept := bytes.Repeat([]byte{0x62}, 28)
	drep := models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x63}, 28))
	stake := models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x64}, 28))
	applyAt(t, set, 10, ledger.LedgerChanges{
		Pools: []ledger.PoolChange{
			{Slot: 10, State: ledger.PoolState{Pool: pool}},
			{Slot: 10, State: ledger.PoolState{Pool: kept}},
		},
		DRepLinks: []ledger.DRepLinkChange{{
			Slot: 10, Drep: drep, Stake: stake, Live: true,
		}},
	})
	applyAt(t, set, 12, ledger.LedgerChanges{
		Pools: []ledger.PoolChange{
			{Slot: 12, State: ledger.PoolState{Pool: pool}, Delete: true},
		},
		DRepLinks: []ledger.DRepLinkChange{{
			Slot: 12, Drep: drep, Stake: stake, Live: false,
		}},
	})
	pools, err := set.PoolKeysChangedAfter(11)
	require.NoError(t, err)
	require.Equal(t, [][]byte{pool}, pools, "the removed pool, not the kept one")
	links, err := set.DRepLinksChangedAfter(11)
	require.NoError(t, err)
	require.Equal(t, [][2]models.StakeCredentialRef{{drep, stake}}, links)
	pools, err = set.PoolKeysChangedAfter(12)
	require.NoError(t, err)
	require.Empty(t, pools, "nothing changed after the last slot")
	links, err = set.DRepLinksChangedAfter(12)
	require.NoError(t, err)
	require.Empty(t, links)
}
