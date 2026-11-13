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

package ouroboros

import (
	"context"
	"testing"

	databaseTypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosfetch "github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	"github.com/stretchr/testify/require"
)

// withImmutableSlot makes the selected chain's immutable point sit at slot.
func withImmutableSlot(o *Ouroboros, slot uint64) {
	o.leiosImmutableSlot = func(context.Context) (uint64, bool, error) {
		return slot, true, nil
	}
}

type leiosRetentionRecord struct {
	slot uint64
	hash []byte
}

func leiosRetentionHash(seed byte) []byte {
	hash := make([]byte, 32)
	hash[0] = seed
	return hash
}

func requireLeiosEBPersisted(
	t *testing.T,
	o *Ouroboros,
	record leiosRetentionRecord,
	want bool,
) {
	t.Helper()
	_, err := o.leiosDatabase().GetLeiosEBManifest(record.hash, record.slot)
	if want {
		require.NoError(t, err, "slot %d should be retained", record.slot)
		return
	}
	require.ErrorIs(
		t,
		err,
		databaseTypes.ErrBlobKeyNotFound,
		"slot %d should be pruned",
		record.slot,
	)
}

// With no configured retention the frontier is the immutable point: an
// uncertified endorser block behind it is pruned, a certified one never is.
func TestLeiosPersistenceGCDefaultPrunesUncertifiedBehindImmutableTip(
	t *testing.T,
) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	withImmutableSlot(o, 100)
	db := o.leiosDatabase()
	txs := []cbor.RawMessage{mustCbor(t, "tx")}
	staleUncertified := leiosRetentionRecord{50, leiosRetentionHash(1)}
	staleCertified := leiosRetentionRecord{50, leiosRetentionHash(2)}
	atImmutable := leiosRetentionRecord{100, leiosRetentionHash(3)}
	recent := leiosRetentionRecord{150, leiosRetentionHash(4)}
	for _, record := range []leiosRetentionRecord{
		staleUncertified, staleCertified, atImmutable, recent,
	} {
		require.NoError(t, db.SetLeiosEB(
			record.slot, record.hash, []byte("manifest"), txs,
		))
	}
	require.NoError(
		t,
		db.SetLeiosEBCertified(staleCertified.slot, staleCertified.hash, nil),
	)

	o.runLeiosPersistenceGC(context.Background(), nil)

	requireLeiosEBPersisted(t, o, staleUncertified, false)
	requireLeiosEBPersisted(t, o, staleCertified, true)
	requireLeiosEBPersisted(t, o, atImmutable, true)
	requireLeiosEBPersisted(t, o, recent, true)
}

// A configured slot window never keeps pruning ahead of the immutable point:
// an uncertified endorser block at or after it can still be certified.
func TestLeiosPersistenceGCConfiguredWindowStopsAtImmutableTip(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	o.config.LeiosPersistenceRetentionSlots = 10
	withImmutableSlot(o, 100)
	db := o.leiosDatabase()
	behind := leiosRetentionRecord{90, leiosRetentionHash(1)}
	ahead := leiosRetentionRecord{150, leiosRetentionHash(2)}
	newest := leiosRetentionRecord{300, leiosRetentionHash(3)}
	for _, record := range []leiosRetentionRecord{behind, ahead, newest} {
		require.NoError(t, db.SetLeiosEB(
			record.slot, record.hash, []byte("manifest"), nil,
		))
	}

	o.runLeiosPersistenceGC(context.Background(), nil)

	requireLeiosEBPersisted(t, o, behind, false)
	requireLeiosEBPersisted(t, o, ahead, true)
	requireLeiosEBPersisted(t, o, newest, true)
}

// Without an immutable point to measure against, retention 0 prunes nothing.
func TestLeiosPersistenceGCDefaultWaitsForImmutablePoint(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	o.leiosImmutableSlot = func(context.Context) (uint64, bool, error) {
		return 0, false, nil
	}
	record := leiosRetentionRecord{50, leiosRetentionHash(1)}
	require.NoError(t, o.leiosDatabase().SetLeiosEB(
		record.slot, record.hash, []byte("manifest"), nil,
	))

	o.runLeiosPersistenceGC(context.Background(), nil)

	requireLeiosEBPersisted(t, o, record, true)
}

// A configured window is capped by the immutable point, so it prunes nothing
// while the ledger reports none: an uncertified endorser block there can still
// be certified, and a pruned one is not recoverable.
func TestLeiosPersistenceGCConfiguredWindowWaitsForImmutablePoint(
	t *testing.T,
) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	o.config.LeiosPersistenceRetentionSlots = 10
	o.leiosImmutableSlot = func(context.Context) (uint64, bool, error) {
		return 0, false, nil
	}
	db := o.leiosDatabase()
	old := leiosRetentionRecord{150, leiosRetentionHash(1)}
	newest := leiosRetentionRecord{300, leiosRetentionHash(2)}
	for _, record := range []leiosRetentionRecord{old, newest} {
		require.NoError(t, db.SetLeiosEB(
			record.slot, record.hash, []byte("manifest"), nil,
		))
	}

	o.runLeiosPersistenceGC(context.Background(), nil)

	requireLeiosEBPersisted(t, o, old, true)
	requireLeiosEBPersisted(t, o, newest, true)
	o.leiosPersistStoreMu.Lock()
	defer o.leiosPersistStoreMu.Unlock()
	require.Zero(t, o.leiosPersistPruneBeforeSlot)
}

// The writer drops an uncertified endorser block behind the frontier but
// still persists a certified one, which is re-fetched for replay.
func TestLeiosPersistenceWriterKeepsCertifiedBehindFrontier(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	withImmutableSlot(o, 100)
	o.leiosPersistGCOnce.Do(func() {})
	db := o.leiosDatabase()
	certifiedPoint, certifiedRaw, certifiedData, _ := leiosPersistTestEntry(
		t, 10, 1, 4,
	)
	require.NoError(
		t,
		db.SetLeiosEBCertified(certifiedPoint.Slot, certifiedPoint.Hash, nil),
	)
	uncertifiedPoint, uncertifiedRaw, uncertifiedData, _ := leiosPersistTestEntry(
		t, 11, 1, 4,
	)
	o.leiosPersistStoreMu.Lock()
	o.leiosPersistPruneBeforeSlot = 100
	o.leiosPersistStoreMu.Unlock()
	o.enqueueLeiosPersist(certifiedPoint, certifiedRaw, certifiedData)
	o.enqueueLeiosPersist(uncertifiedPoint, uncertifiedRaw, uncertifiedData)
	require.NoError(t, o.Close())

	_, err := db.GetLeiosEBManifest(certifiedPoint.Hash, certifiedPoint.Slot)
	require.NoError(t, err, "certified endorser block was not persisted")
	_, err = db.GetLeiosEBManifest(uncertifiedPoint.Hash, uncertifiedPoint.Slot)
	require.ErrorIs(t, err, databaseTypes.ErrBlobKeyNotFound)
}

// leios-fetch answers an uncertified endorser block behind the immutable point
// as unknown, whether the data is cached or only persisted; a certified one,
// and one at or after the immutable point, is still served.
func TestLeiosFetchServerRefusesUncertifiedBehindImmutableTip(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	withImmutableSlot(o, 100)
	db := o.leiosDatabase()
	type served struct {
		point  leiosRetentionRecord
		served bool
	}
	var cases []served
	for i, c := range []struct {
		slot      uint64
		certified bool
		served    bool
	}{
		{slot: 50, certified: false, served: false},
		{slot: 51, certified: true, served: true},
		{slot: 100, certified: false, served: true},
		{slot: 150, certified: false, served: true},
	} {
		tx, ref := testLeiosManifestTx(t, byte(i))
		raw, err := cbor.Encode(&lcommon.LeiosEndorserBlock{
			TransactionReferences: []lcommon.LeiosTransactionReference{ref},
		})
		require.NoError(t, err)
		hash := lcommon.Blake2b256Hash(raw).Bytes()
		require.NoError(t, db.SetLeiosEB(
			c.slot, hash, raw, []cbor.RawMessage{tx},
		))
		if c.certified {
			require.NoError(t, db.SetLeiosEBCertified(c.slot, hash, nil))
		}
		cases = append(cases, served{
			point:  leiosRetentionRecord{c.slot, hash},
			served: c.served,
		})
	}

	for _, c := range cases {
		point := ocommon.NewPoint(c.point.slot, c.point.hash)
		blockMsg, blockErr := o.leiosfetchServerBlockRequest(
			oleiosfetch.CallbackContext{},
			point,
		)
		txsMsg, txsErr := o.leiosfetchServerBlockTxsRequest(
			oleiosfetch.CallbackContext{},
			point,
			map[uint16]uint64{0: 1 << 63},
		)
		if c.served {
			require.NoError(t, blockErr, "slot %d", c.point.slot)
			require.NotNil(t, blockMsg)
			require.NoError(t, txsErr, "slot %d", c.point.slot)
			require.NotNil(t, txsMsg)
			continue
		}
		require.ErrorIs(
			t, blockErr, oleiosfetch.ErrBlockNotFound, "slot %d", c.point.slot,
		)
		require.Nil(t, blockMsg)
		require.ErrorIs(
			t, txsErr, oleiosfetch.ErrBlockTxsNotFound, "slot %d", c.point.slot,
		)
		require.Nil(t, txsMsg)
	}
}

// The refusal holds for an occurrence already loaded into the in-memory cache
// when the immutable point later passes it.
func TestLeiosFetchServerRefusesCachedUncertifiedBehindImmutableTip(
	t *testing.T,
) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	withImmutableSlot(o, 10)
	tx, ref := testLeiosManifestTx(t, 1)
	raw, err := cbor.Encode(&lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref},
	})
	require.NoError(t, err)
	point := ocommon.NewPoint(50, lcommon.Blake2b256Hash(raw).Bytes())
	require.NoError(t, o.leiosDatabase().SetLeiosEB(
		point.Slot, point.Hash, raw, []cbor.RawMessage{tx},
	))
	_, err = o.leiosfetchServerBlockRequest(
		oleiosfetch.CallbackContext{},
		point,
	)
	require.NoError(t, err)
	o.leiosMu.RLock()
	cached := o.leiosEndorserBlocks[leiosBlockKey(point.Slot, point.Hash)]
	o.leiosMu.RUnlock()
	require.NotNil(t, cached, "served endorser block was not cached")

	withImmutableSlot(o, 100)
	blockMsg, err := o.leiosfetchServerBlockRequest(
		oleiosfetch.CallbackContext{},
		point,
	)
	require.ErrorIs(t, err, oleiosfetch.ErrBlockNotFound)
	require.Nil(t, blockMsg)
	txsMsg, err := o.leiosfetchServerBlockTxsRequest(
		oleiosfetch.CallbackContext{},
		point,
		map[uint16]uint64{0: 1 << 63},
	)
	require.ErrorIs(t, err, oleiosfetch.ErrBlockTxsNotFound)
	require.Nil(t, txsMsg)
}
