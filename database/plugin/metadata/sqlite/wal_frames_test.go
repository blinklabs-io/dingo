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

package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// walWorkload describes a block mix applied through the store's write path.
type walWorkload struct {
	name        string
	storageMode string
	// overlay selects the trusted batch UTxO overlay, which is what genesis
	// sync uses on core-mode SQLite.
	overlay       bool
	txsPerBlock   int
	inputsPerTx   int
	outputsPerTx  int
	blocksPerTxn  int
	freshAddrProb float64 // probability an output goes to a never-seen address
	addrPool      int     // reusable address population
	stakeProb     float64 // probability an address carries a stake credential
	scriptProb    float64 // probability a tx is a script tx (ref input + collateral)
	invalidProb   float64 // probability a script tx fails phase 2
	warmTxs       int
	measureBlocks int
}

type walRef struct {
	txid []byte
	idx  uint32
}

type walGen struct {
	rng     *rand.Rand
	counter uint64
	pool    []walRef
	hotRefs []walRef
	hotCol  []walRef
	addrs   []string
	w       walWorkload
}

func newWalGen(w walWorkload) *walGen {
	g := &walGen{rng: rand.New(rand.NewPCG(1, 2)), w: w}
	for range w.addrPool {
		g.addrs = append(g.addrs, g.newAddr(g.rng.Float64() < w.stakeProb, false))
	}
	return g
}

func (g *walGen) hash() []byte {
	g.counter++
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], g.counter)
	binary.BigEndian.PutUint64(buf[8:], 0xd1d90)
	sum := sha256.Sum256(buf[:])
	return sum[:]
}

func (g *walGen) newAddr(withStake, script bool) string {
	pay := make([]byte, 28)
	for i := range pay {
		pay[i] = byte(g.rng.Uint32())
	}
	var stake []byte
	var typ uint8 = lcommon.AddressTypeKeyNone
	switch {
	case withStake && script:
		typ = lcommon.AddressTypeScriptKey
	case withStake:
		typ = lcommon.AddressTypeKeyKey
	case script:
		typ = lcommon.AddressTypeScriptNone
	}
	if withStake {
		stake = make([]byte, 28)
		for i := range stake {
			stake[i] = byte(g.rng.Uint32())
		}
	}
	addr, err := lcommon.NewAddressFromParts(
		typ, lcommon.AddressNetworkMainnet, pay, stake,
	)
	if err != nil {
		panic(err)
	}
	return addr.String()
}

func (g *walGen) pickAddr() string {
	if len(g.addrs) == 0 || g.rng.Float64() < g.w.freshAddrProb {
		return g.newAddr(g.rng.Float64() < g.w.stakeProb, false)
	}
	return g.addrs[g.rng.IntN(len(g.addrs))]
}

func (g *walGen) takeInput() (walRef, bool) {
	if len(g.pool) == 0 {
		return walRef{}, false
	}
	i := g.rng.IntN(len(g.pool))
	r := g.pool[i]
	g.pool[i] = g.pool[len(g.pool)-1]
	g.pool = g.pool[:len(g.pool)-1]
	return r, true
}

func mustInput(r walRef) lcommon.TransactionInput {
	in, err := mockledger.NewTransactionInputBuilder().
		WithTxId(r.txid).WithIndex(r.idx).Build()
	if err != nil {
		panic(err)
	}
	return in
}

func mustOutput(addr string, lovelace uint64) lcommon.TransactionOutput {
	out, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(addr).WithLovelace(lovelace).Build()
	if err != nil {
		panic(err)
	}
	return out
}

type walTx struct {
	tx       lcommon.Transaction
	consumed []walRef
}

// next builds one transaction. warm transactions only create outputs.
func (g *walGen) next(warm bool) walTx {
	hash := g.hash()
	b := mockledger.NewTransactionBuilder()
	b.WithId(hash)
	var consumed []walRef
	var inputs []lcommon.TransactionInput
	script := !warm && g.rng.Float64() < g.w.scriptProb
	invalid := script && g.rng.Float64() < g.w.invalidProb
	if !warm {
		for range g.w.inputsPerTx {
			r, ok := g.takeInput()
			if !ok {
				break
			}
			consumed = append(consumed, r)
			inputs = append(inputs, mustInput(r))
		}
	}
	if len(inputs) == 0 {
		// A synthetic input never produced by the store: the consumed-input
		// path treats it as an absent producer, as for imported history.
		r := walRef{txid: g.hash(), idx: 0}
		inputs = append(inputs, mustInput(r))
	}
	var outputs []lcommon.TransactionOutput
	for range g.w.outputsPerTx {
		outputs = append(outputs, mustOutput(g.pickAddr(), 2_000_000))
	}
	b.WithInputs(inputs...)
	b.WithOutputs(outputs...)
	b.WithValid(!invalid)
	if script && len(g.hotRefs) > 0 && len(g.hotCol) > 0 {
		b.WithReferenceInputs(mustInput(g.hotRefs[g.rng.IntN(len(g.hotRefs))]))
		b.WithCollateral(mustInput(g.hotCol[g.rng.IntN(len(g.hotCol))]))
	}
	if invalid {
		b.WithCollateralReturn(mustOutput(g.pickAddr(), 1_000_000))
	}
	if !invalid {
		for i := range outputs {
			g.pool = append(g.pool, walRef{txid: hash, idx: uint32(i)}) //nolint:gosec
		}
	}
	return walTx{tx: b, consumed: consumed}
}

type walBlockBatch struct {
	txs   []walTx
	slots []uint64
	idxs  []uint32
}

// walFrameResult is the per-b-tree frame attribution of one run.
type walFrameResult struct {
	frames     int
	commits    int
	blocks     int
	txs        int
	perBTree   map[string]int
	totalBytes int64
}

func (r walFrameResult) report(title string) string {
	type row struct {
		name   string
		frames int
	}
	rows := make([]row, 0, len(r.perBTree))
	for n, f := range r.perBTree {
		rows = append(rows, row{n, f})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].frames > rows[j].frames })
	var sb strings.Builder
	fmt.Fprintf(&sb, "== %s: %d frames, %d commits, %d blocks, %d txs; "+
		"%.1f frames/block, %.1f frames/commit, %.1f frames/tx\n",
		title, r.frames, r.commits, r.blocks, r.txs,
		float64(r.frames)/float64(r.blocks),
		float64(r.frames)/float64(max(r.commits, 1)),
		float64(r.frames)/float64(max(r.txs, 1)))
	for _, rw := range rows {
		fmt.Fprintf(&sb, "  %-52s %7d %5.1f%% %7.2f/block\n",
			rw.name, rw.frames,
			100*float64(rw.frames)/float64(r.frames),
			float64(rw.frames)/float64(r.blocks))
	}
	return sb.String()
}

func walFrameNumbers(t *testing.T, walPath string, skipFrames int) ([]uint32, int, int64) {
	t.Helper()
	data, err := os.ReadFile(walPath)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(data), 32)
	pageSize := int(binary.BigEndian.Uint32(data[8:12]))
	frameSize := 24 + pageSize
	n := (len(data) - 32) / frameSize
	pages := make([]uint32, 0, n)
	commits := 0
	for i := skipFrames; i < n; i++ {
		off := 32 + i*frameSize
		if string(data[off+8:off+16]) != string(data[16:24]) {
			break
		}
		pages = append(pages, binary.BigEndian.Uint32(data[off:off+4]))
		if binary.BigEndian.Uint32(data[off+4:off+8]) != 0 {
			commits++
		}
	}
	return pages, commits, int64(len(data))
}

func walFrameCount(t *testing.T, walPath string) int {
	t.Helper()
	data, err := os.ReadFile(walPath)
	require.NoError(t, err)
	if len(data) < 32 {
		return 0
	}
	pageSize := int(binary.BigEndian.Uint32(data[8:12]))
	frameSize := 24 + pageSize
	n := 0
	for off := 32; off+frameSize <= len(data); off += frameSize {
		if string(data[off+8:off+16]) != string(data[16:24]) {
			break
		}
		n++
	}
	return n
}

// runWALWorkload applies the workload through the store's real batched write
// path against a file-backed WAL database and attributes the WAL frames of
// the measured phase to b-trees with dbstat.
func runWALWorkload(t *testing.T, w walWorkload) walFrameResult {
	t.Helper()
	dir := t.TempDir()
	store, writeDB, readDB, err := openSQLStore(
		Config{DataDir: dir},
		metadata.ProviderDependencies{StorageMode: w.storageMode},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() { _ = store.Close() })
	dbPath := filepath.Join(dir, "metadata.sqlite")
	walPath := dbPath + "-wal"

	gen := newWalGen(w)
	ctx := context.Background()
	slot := uint64(1)
	apply := func(blocks [][]walTx, measure bool) {
		txn := store.Transaction(ctx)
		acc := store.NewBatchAccumulator()
		if w.overlay {
			produced := map[string]struct{}{}
			var inputs []models.UtxoId
			for _, blk := range blocks {
				for _, tx := range blk {
					for _, in := range tx.tx.Consumed() {
						if _, ok := produced[string(in.Id().Bytes())+"/"+strconv.Itoa(int(in.Index()))]; ok {
							continue
						}
						inputs = append(inputs, models.UtxoId{
							Hash: in.Id().Bytes(), Idx: in.Index(),
						})
					}
					for _, u := range tx.tx.Produced() {
						produced[string(u.Id.Id().Bytes())+"/"+strconv.Itoa(int(u.Id.Index()))] = struct{}{}
					}
				}
			}
			enabled, err := store.EnableBatchUtxoOverlay(acc, txn, inputs, nil, nil)
			require.NoError(t, err)
			require.True(t, enabled)
		}
		for _, blk := range blocks {
			slot += 20
			blockHash := gen.hash()
			for i, tx := range blk {
				require.NoError(t, store.SetTransactionBatched(
					tx.tx,
					ocommon.Point{Slot: slot, Hash: blockHash},
					uint32(i), nil, false, acc, txn, 0, //nolint:gosec
				))
			}
			require.NoError(t, store.FlushBatchStakeDeltas(acc, txn))
		}
		require.NoError(t, store.FlushBatch(acc, txn))
		require.NoError(t, txn.Commit())
	}

	// Reference and collateral hot sets: outputs that are never spent.
	hotTx := make([]walTx, 0, 8)
	for range 8 {
		hotTx = append(hotTx, gen.next(true))
	}
	apply([][]walTx{hotTx}, false)
	for _, tx := range hotTx {
		for _, u := range tx.tx.Produced() {
			ref := walRef{txid: u.Id.Id().Bytes(), idx: u.Id.Index()}
			gen.hotRefs = append(gen.hotRefs, ref)
			gen.hotCol = append(gen.hotCol, ref)
		}
	}
	// Remove the hot outputs from the spendable pool.
	gen.pool = gen.pool[:0]

	warmPerBlock := w.txsPerBlock
	for done := 0; done < w.warmTxs; {
		var blocks [][]walTx
		for range max(w.blocksPerTxn, 1) {
			var blk []walTx
			for range warmPerBlock {
				blk = append(blk, gen.next(done < w.warmTxs/4))
				done++
			}
			blocks = append(blocks, blk)
		}
		apply(blocks, false)
	}

	// A reader holding an old snapshot keeps the WAL from resetting, so the
	// frame count is the exact number of pages the measured commits wrote.
	readerDB, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=busy_timeout(30000)&_pragma=synchronous(OFF)")
	require.NoError(t, err)
	t.Cleanup(func() { _ = readerDB.Close() })
	readerTx, err := readerDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	var one int
	require.NoError(t, readerTx.QueryRow(`SELECT 1 FROM sqlite_master LIMIT 1`).Scan(&one))
	skip := walFrameCount(t, walPath)

	res := walFrameResult{perBTree: map[string]int{}}
	for done := 0; done < w.measureBlocks; {
		var blocks [][]walTx
		for range max(w.blocksPerTxn, 1) {
			var blk []walTx
			for range w.txsPerBlock {
				blk = append(blk, gen.next(false))
				res.txs++
			}
			blocks = append(blocks, blk)
			done++
			res.blocks++
		}
		apply(blocks, true)
	}

	pages, commits, size := walFrameNumbers(t, walPath, skip)
	res.frames = len(pages)
	res.commits = commits
	res.totalBytes = size

	owner := map[uint32]string{}
	rows, err := readDB.QueryContext(ctx, `SELECT name, pageno FROM dbstat`)
	require.NoError(t, err)
	for rows.Next() {
		var name string
		var pageno int64
		require.NoError(t, rows.Scan(&name, &pageno))
		owner[uint32(pageno)] = name //nolint:gosec
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	_ = writeDB
	for _, p := range pages {
		name, ok := owner[p]
		if !ok {
			name = "(unattributed: freelist/ptrmap)"
		}
		res.perBTree[name]++
	}
	require.NoError(t, readerTx.Rollback())
	return res
}

func walEnvInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return def
}

// walWorkloads are the block mixes the frame measurement runs.
func walWorkloads() []walWorkload {
	warm := walEnvInt("DINGO_WAL_WARM_TXS", 20000)
	blocks := walEnvInt("DINGO_WAL_BLOCKS", 100)
	perTxn := walEnvInt("DINGO_WAL_BLOCKS_PER_TXN", 1)
	return []walWorkload{
		{
			name: "byron-core-overlay", storageMode: types.StorageModeCore,
			overlay: true, txsPerBlock: 10, inputsPerTx: 2, outputsPerTx: 2,
			blocksPerTxn: perTxn, freshAddrProb: 0.9, addrPool: 2000,
			warmTxs: warm, measureBlocks: blocks,
		},
		{
			name: "byron-core-perrow", storageMode: types.StorageModeCore,
			txsPerBlock: 10, inputsPerTx: 2, outputsPerTx: 2,
			blocksPerTxn: perTxn, freshAddrProb: 0.9, addrPool: 2000,
			warmTxs: warm, measureBlocks: blocks,
		},
		{
			name: "alonzo-core-overlay", storageMode: types.StorageModeCore,
			overlay: true, txsPerBlock: 20, inputsPerTx: 2, outputsPerTx: 3,
			blocksPerTxn: perTxn, freshAddrProb: 0.2, addrPool: 5000,
			stakeProb: 0.8, scriptProb: 0.3, invalidProb: 0.03,
			warmTxs: warm, measureBlocks: blocks,
		},
		{
			name: "alonzo-api-batched", storageMode: types.StorageModeAPI,
			txsPerBlock: 20, inputsPerTx: 2, outputsPerTx: 3,
			blocksPerTxn: perTxn, freshAddrProb: 0.2, addrPool: 5000,
			stakeProb: 0.8, scriptProb: 0.3, invalidProb: 0.03,
			warmTxs: warm, measureBlocks: blocks,
		},
	}
}

var _ = sqlstore.SQLiteDialect

// TestWALFramesPerBlock reports the WAL frames each b-tree receives per
// block. It only measures when DINGO_WAL_FRAMES=1 because the warm-up is
// slow; the report goes to the test log.
func TestWALFramesPerBlock(t *testing.T) {
	if os.Getenv("DINGO_WAL_FRAMES") == "" {
		t.Skip("set DINGO_WAL_FRAMES=1 to run the WAL frame measurement")
	}
	for _, w := range walWorkloads() {
		t.Run(w.name, func(t *testing.T) {
			t.Parallel()
			res := runWALWorkload(t, w)
			t.Log("\n" + res.report(w.name))
			if out := os.Getenv("DINGO_WAL_REPORT_DIR"); out != "" {
				require.NoError(t, os.WriteFile(
					filepath.Join(out, w.name+".txt"),
					[]byte(res.report(w.name)), 0o644,
				))
			}
		})
	}
}
