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
	"encoding/hex"
	"math/big"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// stakePoolStateQuery builds GetStakePoolState. A nil pools slice is the
// unfiltered form; a non-nil one (even empty) is an explicit pool set.
func stakePoolStateQuery(
	includeDelegators bool,
	pools [][]byte,
) *olocalstatequery.BlockQuery {
	q := &olocalstatequery.ShelleyStakePoolStateQuery{
		Type:              olocalstatequery.QueryTypeShelleyStakePoolState,
		IncludeDelegators: includeDelegators,
	}
	if pools != nil {
		ids := make([]gledger.PoolId, 0, len(pools))
		for _, pool := range pools {
			ids = append(ids, gledger.PoolId(pool))
		}
		q.Pools = []cbor.SetType[gledger.PoolId]{cbor.NewSetType(ids, true)}
	}
	return shelleyBlockQuery(q)
}

// runStakePoolState runs the query through ls.Query and decodes the reply the
// way the client does, so the wire form is part of what is checked.
func runStakePoolState(
	t *testing.T,
	ls *LedgerState,
	q *olocalstatequery.BlockQuery,
) map[gledger.PoolId]olocalstatequery.StakePoolStateEntry {
	t.Helper()
	got, err := ls.Query(t.Context(), q, QueryPoint{})
	require.NoError(t, err)
	wire, err := cbor.Encode(got)
	require.NoError(t, err)
	// The reply is the era wrapper, a one-element array holding the map,
	// which is the shape of StakePoolStateResult itself.
	var result olocalstatequery.StakePoolStateResult
	_, err = cbor.Decode(wire, &result)
	require.NoError(t, err)
	return result.Results
}

func seedActiveDelegator(
	t *testing.T,
	ls *LedgerState,
	key []byte,
	tag uint8,
	pool []byte,
	active bool,
) {
	t.Helper()
	require.NoError(t, ls.db.Metadata().ImportAccount(&models.Account{
		StakingKey:    key,
		CredentialTag: tag,
		Pool:          pool,
		Active:        active,
		AddedSlot:     2,
	}, nil))
}

// newStakePoolStateLedger builds a ledger whose live and stored tip is at
// slot 10 of epoch 0. Pool activity is read at the stored tip, which needs
// the epoch rows the real chain has.
func newStakePoolStateLedger(t *testing.T) *LedgerState {
	t.Helper()
	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.config.CardanoNodeConfig = newTestEraHistoryCfg(t)
	ls.config.StakePoolStateQuery = true
	placeStakePoolStateTip(t, ls, 10)
	return ls
}

// placeStakePoolStateTip puts the live and the stored tip at tipSlot, with an
// epoch row of 100 slots for every epoch up to the tip's.
func placeStakePoolStateTip(t *testing.T, ls *LedgerState, tipSlot uint64) {
	t.Helper()
	epochs := map[uint64]uint64{}
	for epoch := uint64(0); epoch <= tipSlot/100; epoch++ {
		epochs[epoch*100] = epoch
	}
	seedEpochs(t, ls, epochs)
	require.NoError(t, ls.db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(tipSlot, repeatedBytes(32, 0x0B)),
	}, nil))
	setStakePoolParamsLiveState(ls, tipSlot/100, (tipSlot/100)*100, tipSlot)
}

// TestQueryStakePoolState_PoolWithoutDelegators answers one registered pool
// with every field from its registration and no delegators.
func TestQueryStakePoolState_PoolWithoutDelegators(t *testing.T) {
	t.Parallel()
	ls := newStakePoolStateLedger(t)
	pool := repeatedBytes(28, 0x11)
	seedStakePoolParamsPool(t, ls, pool, 0)

	got := runStakePoolState(t, ls, stakePoolStateQuery(false, [][]byte{pool}))
	require.Len(t, got, 1)
	entry, ok := got[gledger.PoolId(pool)]
	require.True(t, ok)
	require.Equal(t, repeatedBytes(32, 0xAA), entry.Vrf[:])
	require.Nil(t, entry.BlsKey)
	require.Equal(t, uint64(1_000_000), entry.Pledge)
	require.Equal(t, uint64(340_000_000), entry.Cost)
	require.Equal(t, "3/10", entry.Margin.String())
	require.Equal(t, uint(0), entry.AccountId.CredType)
	require.Equal(t, repeatedBytes(28, 0x22), entry.AccountId.Credential[:])
	owners := entry.Owners.Items()
	require.Len(t, owners, 2)
	require.Equal(t, repeatedBytes(28, 0x33), owners[0][:])
	require.Equal(t, repeatedBytes(28, 0x44), owners[1][:])
	require.Len(t, entry.Relays, 2)
	require.NotNil(t, entry.Metadata)
	require.Equal(t, "https://a.io/p", entry.Metadata.Url)
	require.Equal(t, 0, entry.NumDelegators)
	require.Nil(t, entry.Delegators)
	require.Nil(t, entry.Retiring)
	require.Nil(t, entry.FutureStakePoolParams)
}

// TestQueryStakePoolState_Delegators counts the active delegators of the pool
// either way and lists them only when the query asks.
func TestQueryStakePoolState_Delegators(t *testing.T) {
	t.Parallel()
	ls := newStakePoolStateLedger(t)
	pool := repeatedBytes(28, 0x11)
	other := repeatedBytes(28, 0x12)
	seedStakePoolParamsPool(t, ls, pool, 0)
	seedStakePoolParamsPool(t, ls, other, 0)
	seedActiveDelegator(t, ls, repeatedBytes(28, 0x62), 0, pool, true)
	seedActiveDelegator(t, ls, repeatedBytes(28, 0x61), 1, pool, true)
	seedActiveDelegator(t, ls, repeatedBytes(28, 0x60), 0, pool, true)
	seedActiveDelegator(t, ls, repeatedBytes(28, 0x63), 0, pool, false)
	seedActiveDelegator(t, ls, repeatedBytes(28, 0x64), 0, other, true)

	with := runStakePoolState(t, ls, stakePoolStateQuery(true, [][]byte{pool}))
	entry := with[gledger.PoolId(pool)]
	require.Equal(t, 3, entry.NumDelegators)
	require.NotNil(t, entry.Delegators)
	got := entry.Delegators.Items()
	require.Len(t, got, 3)
	// The ledger's order: script credentials before key credentials (the
	// derived Ord of Credential puts ScriptHashObj first), then by hash.
	require.Equal(t, uint(1), got[0].CredType)
	require.Equal(t, repeatedBytes(28, 0x61), got[0].Credential[:])
	require.Equal(t, uint(0), got[1].CredType)
	require.Equal(t, repeatedBytes(28, 0x60), got[1].Credential[:])
	require.Equal(t, uint(0), got[2].CredType)
	require.Equal(t, repeatedBytes(28, 0x62), got[2].Credential[:])

	without := runStakePoolState(
		t, ls, stakePoolStateQuery(false, [][]byte{pool}),
	)
	entry = without[gledger.PoolId(pool)]
	require.Equal(t, 3, entry.NumDelegators)
	require.Nil(t, entry.Delegators)
}

// TestQueryStakePoolState_DelegatorSetLengthEncoding checks the wire form of
// the delegator set at the length where it changes. cardano-ledger encodes a
// Set from encoding version 9 as tag 258 followed by variableListLenEncoding
// (cardano-ledger-binary Encoding/Encoder.hs, encodeSet and
// variableListLenEncoding at 9df9f98c): a definite-length array for at most
// lengthThreshold = 23 elements, and an indefinite-length array closed by a
// break for more. The expected bytes are laid out by hand, one credential
// being the two-element array of its tag and 28-byte hash, in ascending hash
// order.
func TestQueryStakePoolState_DelegatorSetLengthEncoding(t *testing.T) {
	t.Parallel()
	ls := newStakePoolStateLedger(t)
	definite := repeatedBytes(28, 0x11)
	indefinite := repeatedBytes(28, 0x12)
	seedStakePoolParamsPool(t, ls, definite, 0)
	seedStakePoolParamsPool(t, ls, indefinite, 0)

	// Each pool takes its own run of credentials, since an account
	// delegates to one pool.
	setItems := func(header byte, base byte, count int, pool []byte) []byte {
		expected := []byte{0xd9, 0x01, 0x02, header}
		for i := range count {
			seed := base + byte(i)
			seedActiveDelegator(t, ls, repeatedBytes(28, seed), 0, pool, true)
			expected = append(expected, 0x82, 0x00, 0x58, 0x1c)
			expected = append(expected, repeatedBytes(28, seed)...)
		}
		return expected
	}
	// 23 elements: array(23) is 0x80+23 = 0x97.
	wantDefinite := setItems(0x97, 0x40, 23, definite)
	// 24 elements: indefinite array 0x9f, closed by break 0xff.
	wantIndefinite := append(setItems(0x9f, 0x80, 24, indefinite), 0xff)

	got, err := ls.Query(
		t.Context(),
		stakePoolStateQuery(true, [][]byte{definite, indefinite}),
		QueryPoint{},
	)
	require.NoError(t, err)
	wire, err := cbor.Encode(got)
	require.NoError(t, err)
	require.True(
		t,
		bytes.Contains(wire, wantDefinite),
		"23 delegators are a definite-length set",
	)
	require.True(
		t,
		bytes.Contains(wire, wantIndefinite),
		"24 delegators are an indefinite-length set",
	)
}

// TestQueryStakePoolState_NilFilterAnswersEveryPool covers the unfiltered
// query, and an explicit empty pool set, which selects nothing.
func TestQueryStakePoolState_NilFilterAnswersEveryPool(t *testing.T) {
	t.Parallel()
	ls := newStakePoolStateLedger(t)
	a := repeatedBytes(28, 0x11)
	b := repeatedBytes(28, 0x88)
	seedStakePoolParamsPool(t, ls, a, 0)
	seedStakePoolParamsPool(t, ls, b, 1)

	all := runStakePoolState(t, ls, stakePoolStateQuery(false, nil))
	require.Len(t, all, 2)
	require.Contains(t, all, gledger.PoolId(a))
	require.Contains(t, all, gledger.PoolId(b))
	require.Equal(t, uint(1), all[gledger.PoolId(b)].AccountId.CredType)

	none := runStakePoolState(t, ls, stakePoolStateQuery(false, [][]byte{}))
	require.Empty(t, none)
}

// TestQueryStakePoolState_UnknownPoolIsAbsent leaves an unregistered pool out
// of the map instead of failing the query.
func TestQueryStakePoolState_UnknownPoolIsAbsent(t *testing.T) {
	t.Parallel()
	ls := newStakePoolStateLedger(t)
	known := repeatedBytes(28, 0x11)
	unknown := repeatedBytes(28, 0x77)
	seedStakePoolParamsPool(t, ls, known, 0)

	got := runStakePoolState(
		t, ls, stakePoolStateQuery(false, [][]byte{known, unknown}),
	)
	require.Len(t, got, 1)
	require.Contains(t, got, gledger.PoolId(known))
	require.NotContains(t, got, gledger.PoolId(unknown))
}

// TestQueryStakePoolState_FutureParamsAndBlsKey reports a re-registration of
// this epoch as future parameters beside the parameters in effect, and the
// registered BLS key with its epoch.
func TestQueryStakePoolState_FutureParamsAndBlsKey(t *testing.T) {
	t.Parallel()
	ls := newStakePoolStateLedger(t)
	db := ls.db
	placeStakePoolStateTip(t, ls, 160)
	pp := &conway.ConwayProtocolParameters{}
	pp.ProtocolVersion.Major = 12
	ls.currentPParams = pp
	ls.publishSnapshotsLocked()
	pool := repeatedBytes(28, 0x11)
	epoch := uint64(0)
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash:   pool,
			VrfKeyHash:    repeatedBytes(32, 0xAA),
			RewardAccount: repeatedBytes(28, 0x22),
		},
		&models.PoolRegistration{
			PoolKeyHash:               pool,
			VrfKeyHash:                repeatedBytes(32, 0xAA),
			RewardAccount:             repeatedBytes(28, 0x22),
			Pledge:                    dbtypes.Uint64(1_000_000),
			Cost:                      dbtypes.Uint64(5),
			AddedSlot:                 5,
			LeiosKeyPublic:            repeatedBytes(96, 0x66),
			LeiosKeyPossessionProof:   repeatedBytes(48, 0x77),
			LeiosKeyRegistrationEpoch: &epoch,
		},
		nil,
	))
	seedStakePoolParamsRegistration(t, ls, pool, 0, 150, 2_000_000)

	got := runStakePoolState(t, ls, stakePoolStateQuery(false, [][]byte{pool}))
	entry := got[gledger.PoolId(pool)]
	require.Equal(t, uint64(1_000_000), entry.Pledge, "in effect")
	require.NotNil(t, entry.BlsKey)
	require.Equal(t, repeatedBytes(96, 0x66), entry.BlsKey.Key.PublicKey)
	require.Equal(t, uint64(0), entry.BlsKey.RegisteredIn)
	require.NotNil(t, entry.FutureStakePoolParams)
	require.Equal(t, uint64(2_000_000), entry.FutureStakePoolParams.Pledge)
	require.Equal(t, pool, entry.FutureStakePoolParams.Id[:])
}

// The ledger's BlsKeyState always carries the epoch the key was registered in,
// so a key whose epoch the database cannot establish has no faithful encoding.
// The reply omits the key (SNothing) rather than report an invented epoch,
// and still answers the pool's parameters and the rest of the entry.
func TestQueryStakePoolState_BlsKeyWithUnknownEpochIsOmitted(t *testing.T) {
	t.Parallel()
	ls := newStakePoolStateLedger(t)
	placeStakePoolStateTip(t, ls, 160)
	pp := &conway.ConwayProtocolParameters{}
	pp.ProtocolVersion.Major = 12
	ls.currentPParams = pp
	ls.publishSnapshotsLocked()
	cases := []struct {
		name       string
		pool       []byte
		ageUnknown bool
	}{
		{"imported without an epoch", repeatedBytes(28, 0x31), true},
		{"known age with no stored epoch", repeatedBytes(28, 0x32), false},
	}
	for _, c := range cases {
		require.NoError(t, ls.db.Metadata().ImportPool(
			&models.Pool{
				PoolKeyHash:   c.pool,
				VrfKeyHash:    repeatedBytes(32, 0xAA),
				RewardAccount: repeatedBytes(28, 0x22),
			},
			&models.PoolRegistration{
				PoolKeyHash:                    c.pool,
				VrfKeyHash:                     repeatedBytes(32, 0xAA),
				RewardAccount:                  repeatedBytes(28, 0x22),
				Pledge:                         dbtypes.Uint64(1_000_000),
				AddedSlot:                      5,
				LeiosKeyPublic:                 repeatedBytes(96, 0x66),
				LeiosKeyPossessionProof:        repeatedBytes(48, 0x77),
				LeiosKeyRegistrationAgeUnknown: c.ageUnknown,
			},
			nil,
		))
	}

	for _, c := range cases {
		got := runStakePoolState(t, ls, stakePoolStateQuery(false, [][]byte{c.pool}))
		entry, ok := got[gledger.PoolId(c.pool)]
		require.True(t, ok, c.name)
		require.Equal(t, uint64(1_000_000), entry.Pledge, c.name)
		require.Nil(t, entry.BlsKey, c.name)
	}
}

// seedStakePoolRetirement records a retirement certificate for the pool at
// slot, naming epoch.
func seedStakePoolRetirement(
	t *testing.T,
	ls *LedgerState,
	pool []byte,
	epoch uint64,
	slot uint64,
) {
	t.Helper()
	txHash := bytes.Repeat([]byte{byte(slot)}, lcommon.Blake2b256Size)
	input, err := mockledger.NewSimpleTransactionInput(txHash, 0)
	require.NoError(t, err)
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd").
		WithLovelace(1_000_000).
		Build()
	require.NoError(t, err)
	tb := mockledger.NewTransactionBuilder()
	tb.WithId(txHash)
	tb.WithType(gledger.TxTypeConway)
	tb.WithValid(true)
	tb.WithInputs(input)
	tb.WithOutputs(output)
	tb.WithCertificates(&lcommon.PoolRetirementCertificate{
		CertType:    uint(lcommon.CertificateTypePoolRetirement),
		PoolKeyHash: lcommon.PoolKeyHash(lcommon.NewBlake2b224(pool)),
		Epoch:       epoch,
	})
	tx, err := tb.Build()
	require.NoError(t, err)
	require.NoError(t, ls.db.SetTransactionMetadataOnly(
		context.Background(),
		tx,
		ocommon.Point{Slot: slot, Hash: txHash},
		0, map[int]uint64{0: 500_000_000}, nil, 0,
	))
}

// seedStakePoolBlsRegistration records a pool registration certificate at slot
// through the certificate write path the node runs while syncing, carrying a
// BLS key built from keySeed. The registered-in epoch is whatever that path
// records.
func seedStakePoolBlsRegistration(
	t *testing.T,
	ls *LedgerState,
	pool []byte,
	keySeed byte,
	slot uint64,
) {
	t.Helper()
	txHash := bytes.Repeat([]byte{byte(slot), byte(keySeed)}, lcommon.Blake2b256Size/2)
	input, err := mockledger.NewSimpleTransactionInput(txHash, 0)
	require.NoError(t, err)
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd").
		WithLovelace(1_000_000).
		Build()
	require.NoError(t, err)
	tb := mockledger.NewTransactionBuilder()
	tb.WithId(txHash)
	tb.WithType(gledger.TxTypeConway)
	tb.WithValid(true)
	tb.WithInputs(input)
	tb.WithOutputs(output)
	tb.WithCertificates(&lcommon.PoolRegistrationCertificate{
		CertType: uint(lcommon.CertificateTypePoolRegistration),
		Operator: lcommon.PoolKeyHash(lcommon.NewBlake2b224(pool)),
		VrfKeyHash: lcommon.VrfKeyHash(
			lcommon.NewBlake2b256(repeatedBytes(32, 0xAA)),
		),
		Pledge: 1_000_000,
		Cost:   340_000_000,
		Margin: cbor.Rat{Rat: big.NewRat(3, 10)},
		RewardAccount: lcommon.AddrKeyHash(
			lcommon.NewBlake2b224(repeatedBytes(28, 0x22)),
		),
		LeiosKey: &lcommon.LeiosKey{
			PublicKey:       repeatedBytes(96, keySeed),
			PossessionProof: repeatedBytes(48, keySeed+1),
		},
	})
	tx, err := tb.Build()
	require.NoError(t, err)
	require.NoError(t, ls.db.SetTransactionMetadataOnly(
		context.Background(),
		tx,
		ocommon.Point{Slot: slot, Hash: txHash},
		0, map[int]uint64{0: 500_000_000}, nil, 0,
	))
}

// TestQueryStakePoolState_BlsKeyRegisteredInFollowsLedgerRules registers BLS
// keys through the certificate write path and reads the registered-in epoch
// back through the query. A key registered by a first registration is
// registered in the transaction's epoch; a same-key re-registration keeps it;
// a rotated key takes the epoch after the re-registration's.
func TestQueryStakePoolState_BlsKeyRegisteredInFollowsLedgerRules(t *testing.T) {
	t.Parallel()
	ls := newStakePoolStateLedger(t)
	placeStakePoolStateTip(t, ls, 360)
	pp := &conway.ConwayProtocolParameters{}
	pp.ProtocolVersion.Major = 12
	ls.currentPParams = pp
	ls.publishSnapshotsLocked()
	kept := repeatedBytes(28, 0x11)
	rotated := repeatedBytes(28, 0x12)
	fresh := repeatedBytes(28, 0x13)

	seedStakePoolBlsRegistration(t, ls, kept, 0x66, 120)
	seedStakePoolBlsRegistration(t, ls, rotated, 0x66, 130)
	seedStakePoolBlsRegistration(t, ls, fresh, 0x66, 240)
	seedStakePoolBlsRegistration(t, ls, kept, 0x66, 250)
	seedStakePoolBlsRegistration(t, ls, rotated, 0x68, 260)

	got := runStakePoolState(
		t, ls, stakePoolStateQuery(false, [][]byte{kept, rotated, fresh}),
	)
	require.Len(t, got, 3)
	for _, c := range []struct {
		pool []byte
		want uint64
	}{{kept, 1}, {rotated, 3}, {fresh, 2}} {
		entry := got[gledger.PoolId(c.pool)]
		require.NotNil(t, entry.BlsKey, "pool %x", c.pool)
		require.Equal(t, c.want, entry.BlsKey.RegisteredIn, "pool %x", c.pool)
	}
}

// TestQueryStakePoolState_Retiring reports the epoch of a pending retirement.
func TestQueryStakePoolState_Retiring(t *testing.T) {
	t.Parallel()
	ls := newStakePoolStateLedger(t)
	pool := repeatedBytes(28, 0x11)
	seedStakePoolParamsPool(t, ls, pool, 0)
	seedStakePoolRetirement(t, ls, pool, 7, 5)

	got := runStakePoolState(t, ls, stakePoolStateQuery(false, [][]byte{pool}))
	entry, ok := got[gledger.PoolId(pool)]
	require.True(t, ok, "a retiring pool is still registered")
	require.NotNil(t, entry.Retiring)
	require.Equal(t, uint64(7), *entry.Retiring)
}

// TestQueryStakePoolState_WireEncoding compares the reply against literal
// bytes laid out from the ledger's QueryResultStakePoolState: the era wrapper,
// a map keyed by pool, and a 14-field record with nulls for what is absent.
func TestQueryStakePoolState_WireEncoding(t *testing.T) {
	t.Parallel()
	ls := newStakePoolStateLedger(t)
	pool := repeatedBytes(28, 0x11)
	require.NoError(t, ls.db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash:   pool,
			VrfKeyHash:    repeatedBytes(32, 0xAA),
			RewardAccount: repeatedBytes(28, 0x22),
		},
		&models.PoolRegistration{
			PoolKeyHash:   pool,
			VrfKeyHash:    repeatedBytes(32, 0xAA),
			RewardAccount: repeatedBytes(28, 0x22),
			Pledge:        dbtypes.Uint64(1),
			Cost:          dbtypes.Uint64(2),
			DepositAmount: dbtypes.Uint64(3),
			AddedSlot:     1,
		},
		nil,
	))
	got, err := ls.Query(
		t.Context(),
		stakePoolStateQuery(false, [][]byte{pool}),
		QueryPoint{},
	)
	require.NoError(t, err)
	gotCbor, err := cbor.Encode(got)
	require.NoError(t, err)
	want := hexBytes(t,
		"81", "a1",
		"581c"+strings.Repeat("11", 28),
		"8e",                            // 14 fields
		"5820"+strings.Repeat("aa", 32), // vrf
		"f6",                            // no BLS key
		"01", "02",                      // pledge, cost
		"d81e820001",                                // margin 0/1
		"82", "00", "581c"+strings.Repeat("22", 28), // account credential
		"d90102", "80", // owners: empty set
		"80", // relays
		"f6", // metadata
		"03", // deposit
		"00", // delegators counted
		"f6", // delegators not requested
		"f6", // not retiring
		"f6", // no future params
	)
	require.Equal(t, hex.EncodeToString(want), hex.EncodeToString(gotCbor))
}

// TestQueryStakePoolState_UnsupportedWithoutPrototypeGate keeps the query
// unanswered on a ledger that is not the Leios prototype's, as it is for any
// other query dingo does not implement: the call fails with the same
// "unsupported query type" error, whatever is registered.
func TestQueryStakePoolState_UnsupportedWithoutPrototypeGate(t *testing.T) {
	t.Parallel()
	ls := newStakePoolStateLedger(t)
	ls.config.StakePoolStateQuery = false
	seedStakePoolParamsPool(t, ls, repeatedBytes(28, 0x11), 0)

	for name, pools := range map[string][][]byte{
		"filtered":   {repeatedBytes(28, 0x11)},
		"unfiltered": nil,
		"empty":      {},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := ls.Query(
				t.Context(),
				stakePoolStateQuery(true, pools),
				QueryPoint{},
			)
			require.EqualError(
				t,
				err,
				"unsupported query type: *localstatequery.ShelleyStakePoolStateQuery",
			)
			require.Nil(t, got)
		})
	}
}

// TestQueryViewStakePoolStateUsesFrozenEpoch proves a held view answers
// GetStakePoolState from the epoch, tip and protocol version it froze, not
// from the live in-memory snapshots a boundary has since advanced. In the
// frozen epoch one pool is retiring, another has a re-registration pending and
// a BLS key the frozen protocol version predates; in the live epoch the
// retirement is over and nothing is pending.
func TestQueryViewStakePoolStateUsesFrozenEpoch(t *testing.T) {
	t.Parallel()

	ls, db := newDiskTestLedger(t)
	ls.config.CardanoNodeConfig = newTestEraHistoryCfg(t)
	ls.config.StakePoolStateQuery = true
	retiring := repeatedBytes(28, 0x11)
	pending := repeatedBytes(28, 0x12)

	// Epoch 3 starts at slot 300 and the frozen tip is slot 350.
	seedStakePoolParamsPool(t, ls, retiring, 0)
	registeredIn := uint64(0)
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash:   pending,
			VrfKeyHash:    repeatedBytes(32, 0xAA),
			RewardAccount: repeatedBytes(28, 0x22),
		},
		&models.PoolRegistration{
			PoolKeyHash:               pending,
			VrfKeyHash:                repeatedBytes(32, 0xAA),
			RewardAccount:             repeatedBytes(28, 0x22),
			Pledge:                    dbtypes.Uint64(1_000_000),
			AddedSlot:                 5,
			LeiosKeyPublic:            repeatedBytes(96, 0x66),
			LeiosKeyPossessionProof:   repeatedBytes(48, 0x77),
			LeiosKeyRegistrationEpoch: &registeredIn,
		},
		nil,
	))
	seedStakePoolParamsRegistration(t, ls, pending, 0, 320, 2_000_000)
	seedEpochs(t, ls, map[uint64]uint64{300: 3, 600: 6})
	seedStakePoolRetirement(t, ls, retiring, 5, 330)

	frozen := conwayPParamsWithCostModels(nil)
	frozen.ProtocolVersion.Major = 11
	frozenCbor, err := cbor.Encode(frozen)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		frozenCbor, 300, 3, eras.ConwayEraDesc.Id, nil,
	))
	view := freezeEpochThenCrossBoundary(
		t, ls, db, eras.ConwayEraDesc, eras.ConwayEraDesc,
	)
	livePParams := conwayPParamsWithCostModels(nil)
	livePParams.ProtocolVersion.Major = 12
	ls.currentPParams = livePParams
	ls.currentEpoch = models.Epoch{
		EpochId: 6, StartSlot: 600, LengthInSlots: 100,
	}
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0B)),
	}
	ls.publishSnapshotsLocked()
	query := stakePoolStateQuery(false, [][]byte{retiring, pending})

	// The direct query answers for the live epoch 6: the retirement of epoch
	// 5 has taken effect and the re-registration is no longer pending.
	live := runStakePoolState(t, ls, query)
	require.NotContains(t, live, gledger.PoolId(retiring))
	require.Nil(t, live[gledger.PoolId(pending)].FutureStakePoolParams)

	got, err := view.Query(t.Context(), query, 0)
	require.NoError(t, err)
	wire, err := cbor.Encode(got)
	require.NoError(t, err)
	var result olocalstatequery.StakePoolStateResult
	_, err = cbor.Decode(wire, &result)
	require.NoError(t, err)

	entry, ok := result.Results[gledger.PoolId(retiring)]
	require.True(t, ok, "registered in the frozen epoch")
	require.NotNil(t, entry.Retiring, "retiring in the frozen epoch")
	require.Equal(t, uint64(5), *entry.Retiring)

	entry, ok = result.Results[gledger.PoolId(pending)]
	require.True(t, ok)
	require.Equal(t, uint64(1_000_000), entry.Pledge, "in effect at the freeze")
	require.NotNil(t, entry.FutureStakePoolParams)
	require.Equal(t, uint64(2_000_000), entry.FutureStakePoolParams.Pledge)
	require.Nil(t, entry.BlsKey, "the frozen protocol version predates it")
	require.Nil(t, entry.FutureStakePoolParams.BlsKey)
}

// recordingStakePoolStateStore records the reads that decide how much of the
// account and pool tables GetStakePoolState walks.
type recordingStakePoolStateStore struct {
	metadata.MetadataStore
	mu                sync.Mutex
	activeAccountRead int
	activePoolsRead   int
	countPools        [][]lcommon.PoolKeyHash
	credentialPools   [][]lcommon.PoolKeyHash
}

func (s *recordingStakePoolStateStore) GetActiveAccountCredentials(
	txn dbtypes.Txn,
) ([]models.StakeCredentialRef, error) {
	s.mu.Lock()
	s.activeAccountRead++
	s.mu.Unlock()
	return s.MetadataStore.GetActiveAccountCredentials(txn)
}

func (s *recordingStakePoolStateStore) GetActivePoolKeyHashes(
	txn dbtypes.Txn,
) ([][]byte, error) {
	s.mu.Lock()
	s.activePoolsRead++
	s.mu.Unlock()
	return s.MetadataStore.GetActivePoolKeyHashes(txn)
}

func (s *recordingStakePoolStateStore) GetActiveDelegatorCountsByPool(
	pools []lcommon.PoolKeyHash,
	txn dbtypes.Txn,
) (map[string]uint64, error) {
	s.mu.Lock()
	s.countPools = append(s.countPools, slices.Clone(pools))
	s.mu.Unlock()
	return s.MetadataStore.GetActiveDelegatorCountsByPool(pools, txn)
}

func (s *recordingStakePoolStateStore) GetActiveDelegatorCredentialsByPool(
	pools []lcommon.PoolKeyHash,
	txn dbtypes.Txn,
) (map[string][]models.StakeCredentialRef, error) {
	s.mu.Lock()
	s.credentialPools = append(s.credentialPools, slices.Clone(pools))
	s.mu.Unlock()
	return s.MetadataStore.GetActiveDelegatorCredentialsByPool(pools, txn)
}

// TestQueryStakePoolState_ReadsOnlyRequestedPools pins what a filtered query
// reads: it never lists every active pool or account, counts the delegators of
// the requested pool alone, and fetches credentials only when the query asks
// for them, and then only for the requested pool.
func TestQueryStakePoolState_ReadsOnlyRequestedPools(t *testing.T) {
	t.Parallel()
	store := &recordingStakePoolStateStore{}
	db, err := dbtest.NewDatabaseWithMetadataWrapper(
		t,
		dbtest.Options{Config: &database.Config{DataDir: ""}},
		func(inner metadata.MetadataStore) metadata.MetadataStore {
			store.MetadataStore = inner
			return store
		},
	)
	require.NoError(t, err)
	ls := newPoolDistr2Ledger(t, db)
	ls.config.CardanoNodeConfig = newTestEraHistoryCfg(t)
	ls.config.StakePoolStateQuery = true
	placeStakePoolStateTip(t, ls, 10)
	pool := repeatedBytes(28, 0x11)
	other := repeatedBytes(28, 0x12)
	seedStakePoolParamsPool(t, ls, pool, 0)
	seedStakePoolParamsPool(t, ls, other, 0)
	seedActiveDelegator(t, ls, repeatedBytes(28, 0x61), 0, pool, true)
	seedActiveDelegator(t, ls, repeatedBytes(28, 0x62), 0, other, true)
	poolHash := lcommon.PoolKeyHash(lcommon.NewBlake2b224(pool))

	entry := runStakePoolState(
		t, ls, stakePoolStateQuery(false, [][]byte{pool}),
	)[gledger.PoolId(pool)]
	require.Equal(t, 1, entry.NumDelegators)
	require.Nil(t, entry.Delegators)
	require.Zero(t, store.activeAccountRead, "no walk of the account table")
	require.Zero(t, store.activePoolsRead, "no list of every active pool")
	require.Equal(t, [][]lcommon.PoolKeyHash{{poolHash}}, store.countPools)
	require.Empty(t, store.credentialPools, "the list was not requested")

	entry = runStakePoolState(
		t, ls, stakePoolStateQuery(true, [][]byte{pool}),
	)[gledger.PoolId(pool)]
	require.NotNil(t, entry.Delegators)
	require.Len(t, entry.Delegators.Items(), 1)
	require.Zero(t, store.activeAccountRead)
	require.Equal(
		t,
		[][]lcommon.PoolKeyHash{{poolHash}},
		store.credentialPools,
		"credentials only for the requested pool",
	)
}
