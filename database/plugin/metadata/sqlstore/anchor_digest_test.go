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

package sqlstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type digestFixtureOutput struct {
	hash    []byte
	idx     int64
	added   int64
	deleted int64
	amount  string
}

func (o digestFixtureOutput) live(slot int64) bool {
	return o.added <= slot && (o.deleted == 0 || o.deleted > slot)
}

// seedDigestFixture inserts transactions in the reverse of hash order, so row
// id order differs from digest order, with spent outputs interleaved among
// the live ones.
func seedDigestFixture(
	t *testing.T,
	store *Store,
	txCount int,
) []digestFixtureOutput {
	t.Helper()
	ctx := context.Background()
	var all []digestFixtureOutput
	for i := txCount; i > 0; i-- {
		hash := bytes.Repeat([]byte{byte(i)}, 32)
		tx := fixtureIdentityRow(t, store, 0, hash)
		for idx := range 4 {
			o := digestFixtureOutput{
				hash:   hash,
				idx:    int64(idx),
				added:  int64(10 + idx),
				amount: fmt.Sprint(i*10 + idx),
			}
			switch (i + idx) % 4 {
			case 1:
				o.deleted = 40 // spent before the digest slot
			case 2:
				o.deleted = 90 // spent after it
			case 3:
				o.added = 70 // created after it
			}
			_, err := store.writeDB.ExecContext(
				ctx,
				store.dialect.Rebind(`
INSERT INTO utxo (tx_id, output_idx, added_slot, deleted_slot, amount)
VALUES (?, ?, ?, ?, ?)`),
				tx, o.idx, o.added, o.deleted, o.amount,
			)
			require.NoError(t, err)
			all = append(all, o)
		}
	}
	return all
}

func resolvedBytes(txHash []byte, idx uint32) []byte {
	return binary.BigEndian.AppendUint32(bytes.Clone(txHash[:4]), idx)
}

// referenceDigest folds the live outputs into a digester in Go, in the order
// the digest defines, without reading them back through SQL.
func referenceDigest(
	all []digestFixtureOutput,
	slot int64,
	resolve metadata.UtxoOutputResolver,
) (metadata.UtxoSetDigest, error) {
	var live []digestFixtureOutput
	for _, o := range all {
		if o.live(slot) {
			live = append(live, o)
		}
	}
	sort.Slice(live, func(a, b int) bool {
		if c := bytes.Compare(live[a].hash, live[b].hash); c != 0 {
			return c < 0
		}
		return live[a].idx < live[b].idx
	})
	d := metadata.NewUtxoDigester()
	for _, o := range live {
		var cbor []byte
		if resolve != nil {
			var err error
			cbor, err = resolve(o.hash, uint32(o.idx)) //nolint:gosec
			if err != nil {
				return metadata.UtxoSetDigest{}, err
			}
		}
		d.Add(
			o.hash, uint32(o.idx), o.amount, nil, nil, nil, //nolint:gosec
			resolve != nil, cbor,
		)
	}
	return d.Sum(), nil
}

func TestLiveUtxoDigestMatchesReferenceAcrossPageSizes(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	all := seedDigestFixture(t, store, 12)
	ctx := context.Background()
	resolve := func(h []byte, idx uint32) ([]byte, error) {
		return resolvedBytes(h, idx), nil
	}
	for _, withOutputs := range []bool{false, true} {
		var r metadata.UtxoOutputResolver
		if withOutputs {
			r = resolve
		}
		want, err := referenceDigest(all, 50, r)
		require.NoError(t, err)
		require.NotZero(t, want.Count)
		for _, page := range []int{1, 2, 5, 7, 12, 1000} {
			got, err := store.liveUtxoDigest(ctx, 50, r, nil, page)
			require.NoError(t, err)
			require.Equal(
				t, want, got,
				"page size %d with outputs %v", page, withOutputs,
			)
		}
	}
}

func TestLiveUtxoDigestResolvesOutputsConcurrently(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	all := seedDigestFixture(t, store, 12)
	ctx := context.Background()

	var (
		mu       sync.Mutex
		inFlight int
		peak     int
		overlap  = make(chan struct{})
		once     sync.Once
	)
	resolve := func(h []byte, idx uint32) ([]byte, error) {
		mu.Lock()
		inFlight++
		peak = max(peak, inFlight)
		if inFlight > 1 {
			once.Do(func() { close(overlap) })
		}
		mu.Unlock()
		defer func() {
			mu.Lock()
			inFlight--
			mu.Unlock()
		}()
		// Hold the first caller until another resolve is running beside it.
		select {
		case <-overlap:
		case <-time.After(2 * time.Second):
		}
		return resolvedBytes(h, idx), nil
	}
	got, err := store.liveUtxoDigest(ctx, 50, resolve, nil, 6)
	require.NoError(t, err)
	require.Greater(t, peak, 1, "outputs in a chunk resolve on several goroutines")

	serial := func(h []byte, idx uint32) ([]byte, error) {
		return resolvedBytes(h, idx), nil
	}
	want, err := referenceDigest(all, 50, serial)
	require.NoError(t, err)
	require.Equal(t, want, got, "parallel resolution digests in order")
}

func TestLiveUtxoDigestReportsTheOutputThatFailedToResolve(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	all := seedDigestFixture(t, store, 12)
	var target digestFixtureOutput
	var seen int
	for _, o := range all {
		if o.live(50) {
			if seen == 5 {
				target = o
			}
			seen++
		}
	}
	require.NotNil(t, target.hash)
	cause := errors.New("no block bytes")
	_, err := store.liveUtxoDigest(
		context.Background(), 50,
		func(h []byte, idx uint32) ([]byte, error) {
			if bytes.Equal(h, target.hash) && int64(idx) == target.idx {
				return nil, cause
			}
			return resolvedBytes(h, idx), nil
		},
		nil, 4,
	)
	require.ErrorIs(t, err, cause)
	require.ErrorContains(
		t, err, fmt.Sprintf("resolve output %x#%d", target.hash, target.idx),
	)
}

// legacyOutputCbor is an output in the pre-Babbage array form, which the
// digest canonicalizes to the map form a snapshot import stores.
func legacyOutputCbor(t *testing.T, txHash []byte, idx uint32) []byte {
	t.Helper()
	addr := make([]byte, 29)
	addr[0] = 0x60
	addr[1] = txHash[0]
	out, err := cbor.Encode([]any{addr, uint64(1_000_000) + uint64(idx)})
	require.NoError(t, err)
	return out
}

func TestLiveUtxoDigestCanonicalizesResolvedOutputs(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	all := seedDigestFixture(t, store, 12)
	resolve := func(h []byte, idx uint32) ([]byte, error) {
		return legacyOutputCbor(t, h, idx), nil
	}
	want, err := referenceDigest(all, 50, resolve)
	require.NoError(t, err)
	// The same outputs digested without canonicalizing must differ, or this
	// test would not notice a skipped canonicalization.
	raw := metadata.NewUtxoDigester()
	for _, o := range all {
		if o.live(50) {
			raw.AddCanonical(
				o.hash, uint32(o.idx), o.amount, nil, nil, nil, //nolint:gosec
				true, legacyOutputCbor(t, o.hash, uint32(o.idx)), //nolint:gosec
			)
		}
	}
	require.NotEqual(t, want.Digest, raw.Sum().Digest)
	for _, page := range []int{1, 5, 1000} {
		got, err := store.liveUtxoDigest(
			context.Background(), 50, resolve, nil, page,
		)
		require.NoError(t, err)
		require.Equal(t, want, got, "page size %d", page)
	}
}

func TestLiveUtxoDigestResolvesWithASingleReadConnection(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	store.readDB.SetMaxOpenConns(1)
	all := seedDigestFixture(t, store, 12)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// A production resolver begins its own read transaction, so the digest
	// must not hold a read connection while it resolves.
	resolve := func(h []byte, idx uint32) ([]byte, error) {
		txn := store.ReadTransaction(ctx)
		defer func() { _ = txn.Rollback() }()
		if st, ok := txn.(*sqlTxn); ok && st.beginErr != nil {
			return nil, st.beginErr
		}
		return resolvedBytes(h, idx), nil
	}
	got, err := store.liveUtxoDigest(ctx, 50, resolve, nil, 4)
	require.NoError(t, err)
	want, err := referenceDigest(all, 50, func(h []byte, idx uint32) ([]byte, error) {
		return resolvedBytes(h, idx), nil
	})
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestLiveUtxoSetQueryScansUtxoInsteadOfWalkingTheHashIndex(t *testing.T) {
	t.Parallel()
	s := newFilePlannerStatsStore(t, prometheus.NewRegistry())
	_, err := s.writeDB.Exec(`WITH RECURSIVE n(i) AS (
 SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<20000
) INSERT INTO "transaction"(id,hash) SELECT i,randomblob(32) FROM n`)
	require.NoError(t, err)
	// Half the outputs are live, which statistics make the index walk look
	// cheaper than scanning utxo.
	_, err = s.writeDB.Exec(`WITH RECURSIVE n(i) AS (
 SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<20000
) INSERT INTO utxo(tx_id,output_idx,added_slot,deleted_slot,amount)
SELECT i,0,1,CASE WHEN i%2=0 THEN 0 ELSE 2 END,'1' FROM n`)
	require.NoError(t, err)
	_, err = s.writeDB.Exec("ANALYZE")
	require.NoError(t, err)
	require.NoError(t, s.refreshReadPool(context.Background()))

	query := liveUtxoSetSQL("sqlite", `"transaction"`)
	legacy := strings.ReplaceAll(query, "+t.hash", "t.hash")
	require.Contains(
		t, explainQueryPlan(t, s, legacy, 50, 50), "idx_transaction_hash",
		"fixture must reproduce the index walk",
	)
	plan := explainQueryPlan(t, s, query, 50, 50)
	require.NotContains(t, plan, "idx_transaction_hash")
	require.Contains(t, plan, "SCAN u")
}

func TestLiveUtxoSetQueryKeepsNonSQLiteOrderKey(t *testing.T) {
	t.Parallel()
	for _, d := range []string{"postgres", "mysql"} {
		q := liveUtxoSetSQL(d, "transaction")
		require.Contains(t, q, "ORDER BY t.hash, u.output_idx", d)
		require.NotContains(t, q, "+t.hash", d)
	}
}
