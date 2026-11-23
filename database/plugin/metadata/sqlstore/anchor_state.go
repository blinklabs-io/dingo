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
	"bufio"
	"bytes"
	"cmp"
	"container/heap"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
)

var _ metadata.AnchorStateStore = (*Store)(nil)

// utxoDigestPage bounds the outputs resolved and hashed per chunk while
// digesting.
const utxoDigestPage = 20_000

// ReadImportedAccounts implements metadata.AnchorStateStore.
func (s *Store) ReadImportedAccounts(
	ctx context.Context,
	anchor uint64,
	cursor int64,
	limit int,
	txn types.Txn,
) ([]metadata.ImportedAccount, int64, error) {
	if limit <= 0 {
		return nil, 0, errors.New(
			"sqlstore: imported account read needs a positive limit",
		)
	}
	slot, err := checkedInt64(anchor)
	if err != nil {
		return nil, 0, err
	}
	db, txCtx, err := s.readDBFromTxn(txn) //nolint:contextcheck
	if err != nil {
		return nil, 0, fmt.Errorf("read imported accounts: %w", err)
	}
	if txn == nil {
		txCtx = ctx
	}
	rs, err := db.QueryContext(
		txCtx,
		s.dialect.Rebind(`-- name: ReadImportedAccounts :many
SELECT a.id, b.credential_tag, b.staking_key, b.pool, b.drep, b.drep_type,
    b.active, b.added_slot, b.deposit_amount, a.reward
FROM account_import_baseline b
JOIN account a
  ON a.credential_tag = b.credential_tag AND a.staking_key = b.staking_key
WHERE b.added_slot >= ? AND a.id > ?
ORDER BY a.id
LIMIT ?`),
		slot,
		cursor,
		limit,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("read imported accounts: %w", err)
	}
	defer rs.Close()
	var (
		out  []metadata.ImportedAccount
		last int64
	)
	for rs.Next() {
		var (
			id, drepType, added int64
			tag                 uint8
			active              bool
			pool, drep, key     []byte
			deposit, reward     sql.NullString
		)
		if err := rs.Scan(
			&id, &tag, &key, &pool, &drep, &drepType, &active, &added,
			&deposit, &reward,
		); err != nil {
			return nil, 0, fmt.Errorf("read imported accounts: %w", err)
		}
		acct := metadata.ImportedAccount{
			CredentialTag: tag, StakingKey: key, Pool: pool, Drep: drep,
			Active: active,
		}
		if acct.DrepType, err = checkedUint64(drepType); err != nil {
			return nil, 0, err
		}
		if acct.AddedSlot, err = checkedUint64(added); err != nil {
			return nil, 0, err
		}
		if deposit.Valid && deposit.String != "" {
			d, err := parseUint64("import deposit", deposit.String)
			if err != nil {
				return nil, 0, err
			}
			v := types.Uint64(d)
			acct.DepositAmount = &v
		}
		r, err := parseNullUint64("account reward", reward)
		if err != nil {
			return nil, 0, err
		}
		acct.Reward = types.Uint64(r)
		out = append(out, acct)
		last = id
	}
	if err := rs.Err(); err != nil {
		return nil, 0, err
	}
	if len(out) < limit {
		last = 0
	}
	return out, last, nil
}

// ReadRewardJournalAfter implements metadata.AnchorStateStore.
func (s *Store) ReadRewardJournalAfter(
	ctx context.Context,
	slot uint64,
	cursor int64,
	limit int,
	txn types.Txn,
) ([]metadata.RewardJournalEntry, int64, error) {
	if limit <= 0 {
		return nil, 0, errors.New(
			"sqlstore: reward journal read needs a positive limit",
		)
	}
	after, err := checkedInt64(slot)
	if err != nil {
		return nil, 0, err
	}
	db, txCtx, err := s.readDBFromTxn(txn) //nolint:contextcheck
	if err != nil {
		return nil, 0, fmt.Errorf("read reward journal: %w", err)
	}
	if txn == nil {
		txCtx = ctx
	}
	rs, err := db.QueryContext(
		txCtx,
		s.dialect.Rebind(`-- name: ReadRewardJournalAfter :many
SELECT id, credential_tag, staking_key, amount, withdrawal
FROM account_reward_delta
WHERE added_slot > ? AND id > ?
ORDER BY id
LIMIT ?`),
		after,
		cursor,
		limit,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("read reward journal: %w", err)
	}
	defer rs.Close()
	var (
		out  []metadata.RewardJournalEntry
		last int64
	)
	for rs.Next() {
		var (
			id         int64
			tag        uint8
			key        []byte
			amount     sql.NullString
			withdrawal bool
		)
		if err := rs.Scan(&id, &tag, &key, &amount, &withdrawal); err != nil {
			return nil, 0, fmt.Errorf("read reward journal: %w", err)
		}
		n, err := parseNullUint64("reward journal amount", amount)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, metadata.RewardJournalEntry{
			CredentialTag: tag, StakingKey: key, Amount: n, Withdrawal: withdrawal,
		})
		last = id
	}
	if err := rs.Err(); err != nil {
		return nil, 0, err
	}
	if len(out) < limit {
		last = 0
	}
	return out, last, nil
}

// WriteImportedAccounts implements metadata.AnchorStateStore.
func (s *Store) WriteImportedAccounts(
	accounts []metadata.ImportedAccount,
	txn types.Txn,
) error {
	if len(accounts) == 0 {
		return nil
	}
	if txn == nil {
		return errors.New(
			"sqlstore: imported accounts are written inside a transaction",
		)
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			for _, a := range accounts {
				drepType, err := checkedInt64(a.DrepType)
				if err != nil {
					return err
				}
				added, err := checkedInt64(a.AddedSlot)
				if err != nil {
					return err
				}
				var deposit any
				if a.DepositAmount != nil {
					deposit = decimalUint64(*a.DepositAmount)
				}
				if _, err := db.ExecContext(ctx, s.dialect.Rebind(`-- name: UpsertAccountImportBaseline :exec
INSERT INTO account_import_baseline (
    credential_tag, staking_key, pool, drep, drep_type, active, added_slot,
    deposit_amount
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (credential_tag, staking_key) DO UPDATE SET
    pool = excluded.pool,
    drep = excluded.drep,
    drep_type = excluded.drep_type,
    active = excluded.active,
    added_slot = excluded.added_slot,
    deposit_amount = excluded.deposit_amount`),
					a.CredentialTag, a.StakingKey, nullBytes(a.Pool),
					nullBytes(a.Drep), drepType, a.Active, added, deposit,
				); err != nil {
					return fmt.Errorf(
						"write imported account baseline: %w",
						err,
					)
				}
				if _, err := db.ExecContext(ctx, s.dialect.Rebind(`-- name: SetImportedAccountReward :exec
UPDATE account SET reward = ? WHERE credential_tag = ? AND staking_key = ?`),
					decimalUint64(a.Reward), a.CredentialTag, a.StakingKey,
				); err != nil {
					return fmt.Errorf("write imported account reward: %w", err)
				}
			}
			return nil
		},
	)
}

// LiveUtxoDigest implements metadata.AnchorStateStore.
func (s *Store) LiveUtxoDigest(
	ctx context.Context,
	atSlot uint64,
	txn types.Txn,
) (metadata.UtxoSetDigest, error) {
	return s.liveUtxoDigest(ctx, atSlot, nil, txn, utxoDigestPage)
}

// LiveUtxoDigestWithOutputs implements metadata.AnchorStateStore.
func (s *Store) LiveUtxoDigestWithOutputs(
	ctx context.Context,
	atSlot uint64,
	resolve metadata.UtxoOutputResolver,
	txn types.Txn,
) (metadata.UtxoSetDigest, error) {
	return s.liveUtxoDigest(ctx, atSlot, resolve, txn, utxoDigestPage)
}

// digestScratchDir is the directory the digest spool is created in:
// Config.ScratchDir, else the SQLite database's directory, else empty for the
// operating system's temporary directory.
func (s *Store) digestScratchDir() string {
	if s.scratchDir != "" {
		return s.scratchDir
	}
	if s.sqlitePath != "" {
		return filepath.Dir(s.sqlitePath)
	}
	return ""
}

func (s *Store) liveUtxoDigest(
	ctx context.Context,
	atSlot uint64,
	resolve metadata.UtxoOutputResolver,
	txn types.Txn,
	page int,
) (metadata.UtxoSetDigest, error) {
	slot, err := checkedInt64(atSlot)
	if err != nil {
		return metadata.UtxoSetDigest{}, err
	}
	db, txCtx, err := s.readDBFromTxn(txn) //nolint:contextcheck
	if err != nil {
		return metadata.UtxoSetDigest{}, fmt.Errorf("digest utxo set: %w", err)
	}
	if txn == nil {
		txCtx = ctx
	}
	runBytes := utxoDigestRunBytes
	if s.utxoDigestRunBytes > 0 {
		runBytes = s.utxoDigestRunBytes
	}
	spool, err := spoolLiveOutputs(txCtx, db, s.dialect.Rebind(
		liveUtxoSetSQL(s.dialect.QuoteIdentifier("transaction")),
	), slot, s.digestScratchDir(), runBytes)
	if err != nil {
		return metadata.UtxoSetDigest{}, fmt.Errorf("digest utxo set: %w", err)
	}
	defer spool.close()
	d := metadata.NewUtxoDigester()
	for {
		if err := txCtx.Err(); err != nil {
			return metadata.UtxoSetDigest{}, err
		}
		outputs, err := spool.next(page)
		if err != nil {
			return metadata.UtxoSetDigest{}, fmt.Errorf(
				"digest utxo set: %w", err,
			)
		}
		if len(outputs) == 0 {
			break
		}
		if err := digestOutputs(txCtx, d, resolve, outputs); err != nil {
			return metadata.UtxoSetDigest{}, fmt.Errorf(
				"digest utxo set: %w", err,
			)
		}
	}
	return d.Sum(), nil
}

// liveUtxoSetSQL selects the outputs live at a slot, in no particular order;
// the spool sorts them. With no ORDER BY to satisfy, the planner scans utxo
// for the live rows and reads the table once, rather than walking the
// transaction hash index and fetching every output ever created by row id.
func liveUtxoSetSQL(transactionTable string) string {
	return `-- name: LiveUtxoSet :many
SELECT t.hash, u.output_idx, u.amount, u.payment_key, u.staking_key,
    u.datum_hash
FROM utxo u
JOIN ` + transactionTable + ` t ON t.id = u.tx_id
WHERE u.added_slot <= ?
  AND (u.deleted_slot = 0 OR u.deleted_slot > ?)`
}

// AppliedTransactionCount implements metadata.AnchorStateStore.
func (s *Store) AppliedTransactionCount(
	ctx context.Context,
	txn types.Txn,
) (uint64, error) {
	db, txCtx, err := s.readDBFromTxn(txn) //nolint:contextcheck
	if err != nil {
		return 0, err
	}
	if txn == nil {
		txCtx = ctx
	}
	var n int64
	if err := db.QueryRowContext(
		txCtx, `SELECT COUNT(*) FROM applied_transaction`,
	).Scan(&n); err != nil {
		return 0, fmt.Errorf("count applied transactions: %w", err)
	}
	return checkedUint64(n)
}

// ReadOpCertCountersAt implements metadata.AnchorStateStore.
func (s *Store) ReadOpCertCountersAt(
	ctx context.Context,
	slot uint64,
	txn types.Txn,
) ([]metadata.OpCertCounter, error) {
	if s.tableDrained("pool_opcert_sequence") {
		return nil, errTableDrained("pool_opcert_sequence")
	}
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return nil, err
	}
	db, txCtx, err := s.readDBFromTxn(txn) //nolint:contextcheck
	if err != nil {
		return nil, err
	}
	if txn == nil {
		txCtx = ctx
	}
	rs, err := db.QueryContext(
		txCtx,
		s.dialect.Rebind(`-- name: ReadOpCertCountersAt :many
SELECT pool_key_hash, sequence FROM pool_opcert_sequence
WHERE slot = ? ORDER BY pool_key_hash`),
		slotValue,
	)
	if err != nil {
		return nil, fmt.Errorf("read op-cert counters at slot %d: %w", slot, err)
	}
	defer rs.Close()
	var out []metadata.OpCertCounter
	for rs.Next() {
		var (
			pool []byte
			seq  int64
		)
		if err := rs.Scan(&pool, &seq); err != nil {
			return nil, fmt.Errorf(
				"read op-cert counters at slot %d: %w", slot, err,
			)
		}
		out = append(out, metadata.OpCertCounter{
			PoolKeyHash: pool, Sequence: decodeOpCertSequence(seq),
		})
	}
	return out, rs.Err()
}

type liveOutput struct {
	txHash, payment, staking, datum []byte
	idx                             int64
	amount                          sql.NullString
}

// digestOutputs folds outputs, in order, into d. Resolving an output reads
// its block bytes and canonicalizing it re-encodes them; both are independent
// per output, so they run on several goroutines and only the hash needs the
// outputs in order.
func digestOutputs(
	ctx context.Context,
	d *metadata.UtxoDigester,
	resolve metadata.UtxoOutputResolver,
	outputs []liveOutput,
) error {
	if resolve == nil {
		for _, o := range outputs {
			d.Add(
				o.txHash, uint32(o.idx), o.amount.String, //nolint:gosec
				o.payment, o.staking, o.datum, false, nil,
			)
		}
		return nil
	}
	cbors := make([][]byte, len(outputs))
	errs := make([]error, len(outputs))
	var next atomic.Int64
	var wg sync.WaitGroup
	for range min(digestResolveWorkers, len(outputs)) {
		wg.Go(func() {
			for {
				i := int(next.Add(1) - 1)
				if i >= len(outputs) || ctx.Err() != nil {
					return
				}
				o := outputs[i]
				cbor, err := resolve(o.txHash, uint32(o.idx)) //nolint:gosec
				if err != nil {
					errs[i] = fmt.Errorf(
						"resolve output %x#%d: %w", o.txHash, o.idx, err,
					)
					continue
				}
				cbors[i] = metadata.CanonicalOutputCbor(cbor)
			}
		})
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return err
	}
	for i, o := range outputs {
		if errs[i] != nil {
			return errs[i]
		}
		d.AddCanonical(
			o.txHash, uint32(o.idx), o.amount.String, //nolint:gosec
			o.payment, o.staking, o.datum, true, cbors[i],
		)
	}
	return nil
}

// digestResolveWorkers bounds the goroutines resolving one digest chunk.
const digestResolveWorkers = 8

// outputSpool holds the live outputs on disk and returns them in digest
// order. The scan is read to its end and its cursor closed before any output
// is resolved: a resolver begins its own read transaction, so a cursor held
// open across resolves would need a second read connection per resolving
// goroutine and would stall a read pool of one. The scan also pins its read
// snapshot only for the time it takes to read, not for the whole digest.
//
// The spool sorts the rows itself rather than asking the database to: it is
// an external merge sort whose runs are written to one file in the store's
// scratch directory, so all of the digest's disk use lands where the store
// keeps its data. SQLite would spill an ORDER BY of the whole set into the
// process's temporary directory, and only a process-wide setting moves that.
type outputSpool struct {
	file     *os.File
	unlinked bool
	runs     outputRunHeap
}

func spoolLiveOutputs(
	ctx context.Context,
	db queryer,
	query string,
	slot int64,
	dir string,
	runBytes int,
) (*outputSpool, error) {
	if dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	file, err := os.CreateTemp(dir, ".dingo-utxo-digest-*")
	if err != nil {
		return nil, err
	}
	// Unlinking the open file means a process killed mid-digest leaves no
	// spool behind. Windows refuses, so close removes it there.
	sp := &outputSpool{file: file, unlinked: os.Remove(file.Name()) == nil}
	if err := sp.fill(ctx, db, query, slot, runBytes); err != nil {
		sp.close()
		return nil, err
	}
	return sp, nil
}

// fill writes the scan as sorted runs of about runBytes encoded bytes each,
// then opens a reader on every run for the merge.
func (sp *outputSpool) fill(
	ctx context.Context,
	db queryer,
	query string,
	slot int64,
	runBytes int,
) error {
	rs, err := db.QueryContext(ctx, query, slot, slot)
	if err != nil {
		return err
	}
	defer rs.Close()
	w := bufio.NewWriterSize(sp.file, spoolBufferSize)
	var (
		arena   []byte
		spans   [][2]int
		offset  int64
		bounds  [][2]int64
		scratch [][]byte
	)
	flush := func() error {
		if len(spans) == 0 {
			return nil
		}
		slices.SortFunc(spans, func(a, b [2]int) int {
			return compareSpoolRecords(arena[a[0]:a[1]], arena[b[0]:b[1]])
		})
		for _, span := range spans {
			if _, err := w.Write(arena[span[0]:span[1]]); err != nil {
				return err
			}
		}
		bounds = append(bounds, [2]int64{offset, int64(len(arena))})
		offset += int64(len(arena))
		arena, spans = arena[:0], spans[:0]
		return nil
	}
	for rs.Next() {
		var o liveOutput
		if err := rs.Scan(
			&o.txHash, &o.idx, &o.amount, &o.payment, &o.staking, &o.datum,
		); err != nil {
			return err
		}
		start := len(arena)
		arena = binary.AppendUvarint(arena, uint64(len(o.txHash)))
		arena = append(arena, o.txHash...)
		arena = binary.AppendUvarint(arena, uint64(o.idx)) //nolint:gosec
		scratch = append(scratch[:0],
			[]byte(o.amount.String), o.payment, o.staking, o.datum,
		)
		for _, field := range scratch {
			arena = binary.AppendUvarint(arena, uint64(len(field)))
			arena = append(arena, field...)
		}
		spans = append(spans, [2]int{start, len(arena)})
		if len(arena) >= runBytes {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := rs.Err(); err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	for _, b := range bounds {
		run := &outputRun{r: bufio.NewReaderSize(
			io.NewSectionReader(sp.file, b[0], b[1]), spoolRunReadBuffer,
		)}
		ok, err := run.advance()
		if err != nil {
			return err
		}
		if ok {
			sp.runs = append(sp.runs, run)
		}
	}
	heap.Init(&sp.runs)
	return nil
}

// compareSpoolRecords orders two encoded records by transaction hash, then
// output index, the order the digest defines. A record starts with the
// length-prefixed hash and the index, so only that prefix is decoded.
func compareSpoolRecords(a, b []byte) int {
	ha, ia := spoolRecordKey(a)
	hb, ib := spoolRecordKey(b)
	if c := bytes.Compare(ha, hb); c != 0 {
		return c
	}
	return cmp.Compare(ia, ib)
}

func spoolRecordKey(rec []byte) ([]byte, uint64) {
	size, n := binary.Uvarint(rec)
	hash := rec[n : n+int(size)] //nolint:gosec
	idx, _ := binary.Uvarint(rec[n+int(size):])
	return hash, idx
}

// next returns up to n outputs, or none once the spool is exhausted.
func (sp *outputSpool) next(n int) ([]liveOutput, error) {
	outputs := make([]liveOutput, 0, n)
	for len(outputs) < n && len(sp.runs) > 0 {
		run := sp.runs[0]
		outputs = append(outputs, run.head)
		ok, err := run.advance()
		if err != nil {
			return nil, err
		}
		if ok {
			heap.Fix(&sp.runs, 0)
		} else {
			heap.Pop(&sp.runs)
		}
	}
	return outputs, nil
}

func (sp *outputSpool) close() {
	_ = sp.file.Close()
	if !sp.unlinked {
		_ = os.Remove(sp.file.Name())
	}
}

// outputRun reads one sorted run back; head is its next output.
type outputRun struct {
	r    *bufio.Reader
	head liveOutput
}

// advance reads the run's next output into head, reporting false at the end
// of the run.
func (run *outputRun) advance() (bool, error) {
	readField := func(first bool) ([]byte, error) {
		size, err := binary.ReadUvarint(run.r)
		if err != nil {
			if first && errors.Is(err, io.EOF) {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("read spooled output: %w", err)
		}
		field := make([]byte, size)
		if _, err := io.ReadFull(run.r, field); err != nil {
			return nil, fmt.Errorf("read spooled output: %w", err)
		}
		return field, nil
	}
	var o liveOutput
	var err error
	if o.txHash, err = readField(true); err != nil {
		if errors.Is(err, io.EOF) {
			return false, nil
		}
		return false, err
	}
	idx, err := binary.ReadUvarint(run.r)
	if err != nil {
		return false, fmt.Errorf("read spooled output: %w", err)
	}
	o.idx = int64(idx) //nolint:gosec
	var amount []byte
	for _, dst := range []*[]byte{&amount, &o.payment, &o.staking, &o.datum} {
		if *dst, err = readField(false); err != nil {
			return false, err
		}
	}
	o.amount = sql.NullString{String: string(amount), Valid: true}
	run.head = o
	return true, nil
}

// outputRunHeap orders the runs by their head output, for the k-way merge.
type outputRunHeap []*outputRun

func (h *outputRunHeap) Len() int { return len(*h) }

func (h *outputRunHeap) Less(i, j int) bool {
	a, b := (*h)[i].head, (*h)[j].head
	if c := bytes.Compare(a.txHash, b.txHash); c != 0 {
		return c < 0
	}
	return a.idx < b.idx
}

func (h *outputRunHeap) Swap(i, j int) { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }

func (h *outputRunHeap) Push(x any) {
	*h = append(*h, x.(*outputRun)) //nolint:forcetypeassert
}

func (h *outputRunHeap) Pop() any {
	old := *h
	run := old[len(old)-1]
	*h = old[:len(old)-1]
	return run
}

const (
	spoolBufferSize = 1 << 20
	// spoolRunReadBuffer is each run's read buffer during the merge, kept
	// small because a mainnet set spools as dozens of runs.
	spoolRunReadBuffer = 64 << 10
	// utxoDigestRunBytes is the encoded size of one sorted run, which bounds
	// the rows the spool holds in memory to sort.
	utxoDigestRunBytes = 16 << 20
)
