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

package database

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/database/leiosdb"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
)

// Migration crash points; see LeiosMigrationOptions.Hook.
const (
	// LeiosMigrationHookCopied fires after every record has been copied and
	// the Copied state is recorded, before the first source delete.
	LeiosMigrationHookCopied = "leios-migration-copied"
	// LeiosMigrationHookDeleteBatch fires after each committed batch of
	// source deletes.
	LeiosMigrationHookDeleteBatch = "leios-migration-delete-batch"
)

// leiosMigrationDeleteBatch bounds one source-delete transaction. Cloud blob
// stores spool prior values to compensate a partial commit, so the batch stays
// small.
const leiosMigrationDeleteBatch = 32

// LeiosMigrationOptions configures MigrateLeiosStore.
type LeiosMigrationOptions struct {
	// CertifiedByChain reports whether the selected chain certifies the
	// endorser block occurrence, and the ranking block that does. It is
	// consulted only for a record without a certification marker that lies
	// below the immutable point. Nil treats such a record as uncertified.
	CertifiedByChain func(
		ctx context.Context,
		eb leiosdb.Ref,
	) (certifier leiosdb.Ref, certified bool, err error)
	// Hook, when set, is called at each crash point and aborts the migration
	// with its error. It exists so tests can interrupt between commits.
	Hook func(point string) error
	// ImmutablePoint is the selected chain's immutable point. It is only
	// meaningful when ImmutableKnown is set.
	ImmutablePoint leiosdb.Ref
	// ImmutableKnown is false while the chain is too short to have one; every
	// record is then kept volatile.
	ImmutableKnown bool
	// BatchEntries bounds how many occurrences' values one read transaction
	// loads. Zero selects a default; a batch also ends once it holds
	// leiosMigrationBatchBytes of values.
	BatchEntries int
	// OnBatch, when set, is called after each batch of values is loaded with
	// the number of occurrences in it. It exists so tests can observe the
	// bound.
	OnBatch func(entries int)
}

// leiosMigrationBatchEntries and leiosMigrationBatchBytes bound the values one
// migration read transaction loads.
const (
	leiosMigrationBatchEntries = 64
	leiosMigrationBatchBytes   = 32 << 20
)

// LeiosMigrationResult counts what a migration pass did.
type LeiosMigrationResult struct {
	// Volatile, Pinned, Dropped and Malformed count endorser-block
	// occurrences copied to the volatile partition, pinned to the immutable
	// one, left behind as uncertified below the immutable point, and skipped
	// because a legacy record carried no readable slot.
	Volatile  int
	Pinned    int
	Dropped   int
	Malformed int
	// Deleted counts source keys removed from the shared blob store.
	Deleted int
	// AlreadyDone is set when the migration had finished before the call.
	AlreadyDone bool
}

// legacyLeiosEntry is the index of one occurrence: the keys its values are read
// from, not the values.
type legacyLeiosEntry struct {
	manifestKey    []byte
	txsKey         []byte
	legacyManifest bool
	hasManifest    bool
	hasTxs         bool
	marker         bool
}

type legacyLeiosKey struct {
	hash [leiosdb.HashSize]byte
	slot uint64
}

// MigrateLeiosStore moves the endorser-block records of the shared blob store
// into the LeiosDB. It is resumable and idempotent: it copies every record,
// records MigrationCopied, deletes the sources in batches, then records
// MigrationDone, and each step is safe to repeat after an interruption.
//
// A record below the immutable point that no certification marker or the
// chain certifies is not copied, because the collector would remove it
// anyway; one at or above the point stays volatile and the pin walk decides it
// later. A certified record below the point is pinned. Records of the legacy
// hash-only key format carry their slot in the manifest value and migrate
// through it; a legacy manifest without a readable slot is dropped, since
// nothing could look it up.
//
// Synthetic closure blocks in the shared store are not touched: the offset
// resolver still reads them there.
func (d *Database) MigrateLeiosStore(
	ctx context.Context,
	opts LeiosMigrationOptions,
) (LeiosMigrationResult, error) {
	var res LeiosMigrationResult
	store, err := d.LeiosDB()
	if err != nil {
		return res, err
	}
	state, err := store.MigrationState()
	if err != nil {
		return res, err
	}
	switch state {
	case leiosdb.MigrationDone:
		d.leios.migrated.Store(true)
		res.AlreadyDone = true
		return res, nil
	case leiosdb.MigrationNone:
		if err := d.copyLegacyLeios(ctx, store, opts, &res); err != nil {
			return res, err
		}
		if err := store.SetMigrationState(leiosdb.MigrationCopied); err != nil {
			return res, err
		}
	case leiosdb.MigrationCopied:
		// The records are already copied; only the source deletes remain.
	}
	if opts.Hook != nil {
		if err := opts.Hook(LeiosMigrationHookCopied); err != nil {
			return res, err
		}
	}
	deleted, err := d.deleteLegacyLeios(ctx, opts)
	res.Deleted = deleted
	if err != nil {
		return res, err
	}
	if err := store.SetMigrationState(leiosdb.MigrationDone); err != nil {
		return res, err
	}
	d.leios.migrated.Store(true)
	return res, nil
}

func (d *Database) copyLegacyLeios(
	ctx context.Context,
	store leiosdb.Store,
	opts LeiosMigrationOptions,
	res *LeiosMigrationResult,
) error {
	entries, malformed, err := d.indexLegacyLeios(ctx)
	if err != nil {
		return err
	}
	res.Malformed += malformed
	refs := make([]legacyLeiosKey, 0, len(entries))
	for key := range entries {
		refs = append(refs, key)
	}
	slices.SortFunc(refs, func(a, b legacyLeiosKey) int {
		if a.slot != b.slot {
			if a.slot < b.slot {
				return -1
			}
			return 1
		}
		return bytes.Compare(a.hash[:], b.hash[:])
	})
	maxEntries := opts.BatchEntries
	if maxEntries <= 0 {
		maxEntries = leiosMigrationBatchEntries
	}
	for start := 0; start < len(refs); {
		if err := ctx.Err(); err != nil {
			return err
		}
		values, n, err := d.loadLegacyLeiosBatch(
			refs[start:], entries, maxEntries,
		)
		if err != nil {
			return err
		}
		if opts.OnBatch != nil {
			opts.OnBatch(n)
		}
		if err := d.migrateLegacyLeiosBatch(
			ctx, store, opts, res, refs[start:start+n], entries, values,
		); err != nil {
			return err
		}
		start += n
	}
	// Everything below the immutable point has been classified, so the pin
	// walk resumes from it. Without an immutable point the walk starts at
	// the beginning of a chain too short to be costly.
	if opts.ImmutableKnown {
		if err := store.AdvancePinnedThrough(opts.ImmutablePoint); err != nil {
			return err
		}
	}
	return nil
}

type legacyLeiosValues struct {
	manifest []byte
	txs      []byte
}

// loadLegacyLeiosBatch reads the values of the next occurrences in a short
// read transaction of its own, stopping at maxEntries occurrences or
// leiosMigrationBatchBytes of values, whichever comes first, and at least one.
func (d *Database) loadLegacyLeiosBatch(
	refs []legacyLeiosKey,
	entries map[legacyLeiosKey]*legacyLeiosEntry,
	maxEntries int,
) (map[legacyLeiosKey]legacyLeiosValues, int, error) {
	readTxn := d.BlobTxn(false)
	defer readTxn.Release()
	blob := readTxn.BlobStore()
	blobTxn := readTxn.Blob()
	if blob == nil {
		return nil, 0, types.ErrBlobStoreUnavailable
	}
	if blobTxn == nil {
		return nil, 0, types.ErrNilTxn
	}
	values := map[legacyLeiosKey]legacyLeiosValues{}
	loaded, bytesLoaded := 0, 0
	for _, key := range refs {
		if loaded >= maxEntries ||
			(loaded > 0 && bytesLoaded >= leiosMigrationBatchBytes) {
			break
		}
		loaded++
		entry := entries[key]
		if entry == nil || !entry.hasManifest {
			continue
		}
		var v legacyLeiosValues
		manifest, err := blob.Get(blobTxn, entry.manifestKey)
		if err != nil {
			return nil, 0, fmt.Errorf("read Leios manifest: %w", err)
		}
		if entry.legacyManifest {
			// The legacy value is the slot followed by the manifest.
			if len(manifest) < 8 {
				return nil, 0, errors.New("legacy Leios manifest lost its slot")
			}
			manifest = manifest[8:]
		}
		v.manifest = manifest
		bytesLoaded += len(manifest)
		if entry.hasTxs {
			txs, err := blob.Get(blobTxn, entry.txsKey)
			if err != nil {
				return nil, 0, fmt.Errorf("read Leios transactions: %w", err)
			}
			v.txs = txs
			bytesLoaded += len(txs)
		}
		values[key] = v
	}
	return values, loaded, nil
}

func (d *Database) migrateLegacyLeiosBatch(
	ctx context.Context,
	store leiosdb.Store,
	opts LeiosMigrationOptions,
	res *LeiosMigrationResult,
	refs []legacyLeiosKey,
	entries map[legacyLeiosKey]*legacyLeiosEntry,
	values map[legacyLeiosKey]legacyLeiosValues,
) error {
	for _, key := range refs {
		if err := ctx.Err(); err != nil {
			return err
		}
		entry := entries[key]
		if entry == nil || !entry.hasManifest {
			// A certification marker or transactions with no manifest hold
			// nothing a reader could serve.
			continue
		}
		ref := leiosdb.Ref{Slot: key.slot, Hash: bytes.Clone(key.hash[:])}
		below := opts.ImmutableKnown && key.slot < opts.ImmutablePoint.Slot
		var (
			certified bool
			certifier leiosdb.Ref
		)
		if below {
			certified = entry.marker
			if !certified && opts.CertifiedByChain != nil {
				var cerr error
				certifier, certified, cerr = opts.CertifiedByChain(ctx, ref)
				if cerr != nil {
					return fmt.Errorf(
						"derive certification of endorser block at slot %d: %w",
						key.slot,
						cerr,
					)
				}
			}
			if !certified {
				res.Dropped++
				continue
			}
		}
		val := values[key]
		content := leiosdb.Content{Manifest: val.manifest}
		if entry.hasTxs {
			txs, derr := decodeLeiosEBTxs(val.txs)
			if derr != nil {
				return fmt.Errorf(
					"decode legacy endorser block transactions at slot %d: %w",
					key.slot,
					derr,
				)
			}
			if txs == nil {
				txs = []cbor.RawMessage{}
			}
			content.Txs = txs
		}
		if err := store.PutVolatile(ref, content); err != nil &&
			!errors.Is(err, leiosdb.ErrBehindImmutableTip) {
			return err
		}
		if !below {
			res.Volatile++
			continue
		}
		if _, err := store.Pin(ref, certifier); err != nil {
			return err
		}
		res.Pinned++
	}
	return nil
}

// indexLegacyLeios lists every endorser-block record of the shared store
// without reading the values the copy needs, so the index holds keys only. A
// legacy manifest's value is read to take its slot, which it carries, and
// dropped.
func (d *Database) indexLegacyLeios(
	ctx context.Context,
) (entries map[legacyLeiosKey]*legacyLeiosEntry, malformed int, err error) {
	readTxn := d.BlobTxn(false)
	defer readTxn.Release()
	blob := readTxn.BlobStore()
	blobTxn := readTxn.Blob()
	if blob == nil {
		return nil, 0, types.ErrBlobStoreUnavailable
	}
	if blobTxn == nil {
		return nil, 0, types.ErrNilTxn
	}
	entries = map[legacyLeiosKey]*legacyLeiosEntry{}
	// Legacy hash-only transaction lists wait for their manifest's slot.
	legacyTxs := map[[leiosdb.HashSize]byte][]byte{}
	legacySlots := map[[leiosdb.HashSize]byte]uint64{}
	get := func(key legacyLeiosKey) *legacyLeiosEntry {
		if entries[key] == nil {
			entries[key] = &legacyLeiosEntry{}
		}
		return entries[key]
	}
	prefix := []byte(types.LeiosEBManifestKeyPrefix[:1])
	it := blob.NewIterator(blobTxn, types.BlobIteratorOptions{Prefix: prefix})
	if it == nil {
		return nil, 0, types.ErrBlobStoreUnavailable
	}
	defer it.Close()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		item := it.Item()
		if item == nil {
			continue
		}
		key := item.Key()
		if len(key) < 2 {
			continue
		}
		switch string(key[:2]) {
		case types.LeiosEBManifestKeyPrefix:
			switch len(key) {
			case 2 + leiosdb.HashSize + 8:
				var k legacyLeiosKey
				copy(k.hash[:], key[2:2+leiosdb.HashSize])
				k.slot = binary.BigEndian.Uint64(key[len(key)-8:])
				e := get(k)
				e.manifestKey, e.hasManifest = bytes.Clone(key), true
			case 2 + leiosdb.HashSize:
				val, err := item.ValueCopy(nil)
				if err != nil {
					return nil, 0, fmt.Errorf(
						"read legacy Leios manifest: %w",
						err,
					)
				}
				var h [leiosdb.HashSize]byte
				copy(h[:], key[2:])
				if len(val) < 8 {
					// No slot to file it under.
					malformed++
					continue
				}
				slot := binary.BigEndian.Uint64(val[:8])
				legacySlots[h] = slot
				e := get(legacyLeiosKey{hash: h, slot: slot})
				e.manifestKey, e.hasManifest = bytes.Clone(key), true
				e.legacyManifest = true
			}
		case types.LeiosEBTxsKeyPrefix:
			switch len(key) {
			case 2 + leiosdb.HashSize + 8:
				var k legacyLeiosKey
				copy(k.hash[:], key[2:2+leiosdb.HashSize])
				k.slot = binary.BigEndian.Uint64(key[len(key)-8:])
				e := get(k)
				e.txsKey, e.hasTxs = bytes.Clone(key), true
			case 2 + leiosdb.HashSize:
				var h [leiosdb.HashSize]byte
				copy(h[:], key[2:])
				legacyTxs[h] = bytes.Clone(key)
			}
		case types.LeiosEBCertifiedKeyPrefix:
			if len(key) == 2+8+leiosdb.HashSize {
				var k legacyLeiosKey
				k.slot = binary.BigEndian.Uint64(key[2:10])
				copy(k.hash[:], key[10:])
				get(k).marker = true
			}
		}
	}
	if err := it.Err(); err != nil {
		return nil, 0, fmt.Errorf("scan legacy Leios records: %w", err)
	}
	for h, txsKey := range legacyTxs {
		slot, ok := legacySlots[h]
		if !ok {
			// No legacy manifest can name these bodies.
			continue
		}
		e := get(legacyLeiosKey{hash: h, slot: slot})
		if !e.hasTxs {
			e.txsKey, e.hasTxs = txsKey, true
		}
	}
	return entries, malformed, nil
}

// deleteLegacyLeios removes every endorser-block record of the shared store in
// small blob-only transactions. It rescans the prefix, so an interrupted pass
// resumes where it stopped.
func (d *Database) deleteLegacyLeios(
	ctx context.Context,
	opts LeiosMigrationOptions,
) (int, error) {
	deleted := 0
	for {
		if err := ctx.Err(); err != nil {
			return deleted, err
		}
		keys, err := d.legacyLeiosKeys(ctx, leiosMigrationDeleteBatch)
		if err != nil {
			return deleted, err
		}
		if len(keys) == 0 {
			return deleted, nil
		}
		txn := d.BlobTxn(true)
		blob := txn.BlobStore()
		if blob == nil {
			txn.Release()
			return deleted, types.ErrBlobStoreUnavailable
		}
		err = txn.Do(func(txn *Txn) error {
			for _, key := range keys {
				if err := blob.Delete(txn.Blob(), key); err != nil &&
					!errors.Is(err, types.ErrBlobKeyNotFound) {
					return fmt.Errorf("delete legacy Leios key: %w", err)
				}
			}
			return nil
		})
		if err != nil {
			return deleted, err
		}
		deleted += len(keys)
		if opts.Hook != nil {
			if err := opts.Hook(LeiosMigrationHookDeleteBatch); err != nil {
				return deleted, err
			}
		}
	}
}

// legacyLeiosKeys lists up to limit keys of the legacy record families.
func (d *Database) legacyLeiosKeys(
	ctx context.Context,
	limit int,
) ([][]byte, error) {
	txn := d.BlobTxn(false)
	defer txn.Release()
	blob := txn.BlobStore()
	blobTxn := txn.Blob()
	if blob == nil {
		return nil, types.ErrBlobStoreUnavailable
	}
	if blobTxn == nil {
		return nil, types.ErrNilTxn
	}
	prefix := []byte(types.LeiosEBManifestKeyPrefix[:1])
	it := blob.NewIterator(blobTxn, types.BlobIteratorOptions{Prefix: prefix})
	if it == nil {
		return nil, types.ErrBlobStoreUnavailable
	}
	defer it.Close()
	var keys [][]byte
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		item := it.Item()
		if item == nil {
			continue
		}
		key := item.Key()
		if len(key) < 2 {
			continue
		}
		switch string(key[:2]) {
		case types.LeiosEBManifestKeyPrefix,
			types.LeiosEBTxsKeyPrefix,
			types.LeiosEBCertifiedKeyPrefix:
			keys = append(keys, bytes.Clone(key))
			if len(keys) >= limit {
				return keys, nil
			}
		}
	}
	return keys, it.Err()
}

// legacyLeiosEBManifest reads a manifest from the shared blob store's legacy
// records while the migration is unfinished. miss is the error to return when
// nothing is found.
func (d *Database) legacyLeiosEBManifest(
	store leiosdb.Store,
	hash []byte,
	slot uint64,
	miss error,
) ([]byte, error) {
	if d.leiosLegacyReadsDone(store) {
		return nil, miss
	}
	txn := d.BlobTxn(false)
	defer txn.Rollback() //nolint:errcheck
	blob := txn.BlobStore()
	if blob == nil {
		return nil, miss
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return nil, types.ErrNilTxn
	}
	val, err := blob.Get(blobTxn, types.LeiosEBManifestKey(hash, slot))
	if err == nil {
		return val, nil
	}
	if !errors.Is(err, types.ErrBlobKeyNotFound) {
		return nil, err
	}
	legacyVal, legacyErr := blob.Get(
		blobTxn,
		types.LegacyLeiosEBManifestKey(hash),
	)
	if legacyErr != nil {
		// A real failure reading the legacy record must not be hidden behind
		// the not-found error.
		if !errors.Is(legacyErr, types.ErrBlobKeyNotFound) {
			return nil, legacyErr
		}
		return nil, miss
	}
	if len(legacyVal) < 8 || binary.BigEndian.Uint64(legacyVal[:8]) != slot {
		return nil, miss
	}
	return legacyVal[8:], nil
}

// legacyLeiosEBTxs is legacyLeiosEBManifest for the transaction list. The
// legacy format paired one manifest and one transaction record per hash, so
// the manifest's embedded slot must match before its transactions are used.
func (d *Database) legacyLeiosEBTxs(
	store leiosdb.Store,
	hash []byte,
	slot uint64,
	miss error,
) ([]cbor.RawMessage, error) {
	if d.leiosLegacyReadsDone(store) {
		return nil, miss
	}
	txn := d.BlobTxn(false)
	defer txn.Rollback() //nolint:errcheck
	blob := txn.BlobStore()
	if blob == nil {
		return nil, miss
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return nil, types.ErrNilTxn
	}
	val, err := blob.Get(blobTxn, types.LeiosEBTxsKey(hash, slot))
	if err != nil {
		if !errors.Is(err, types.ErrBlobKeyNotFound) {
			return nil, err
		}
		legacyManifest, legacyErr := blob.Get(
			blobTxn,
			types.LegacyLeiosEBManifestKey(hash),
		)
		if legacyErr != nil {
			if !errors.Is(legacyErr, types.ErrBlobKeyNotFound) {
				return nil, legacyErr
			}
			return nil, miss
		}
		if len(legacyManifest) < 8 ||
			binary.BigEndian.Uint64(legacyManifest[:8]) != slot {
			return nil, miss
		}
		val, err = blob.Get(blobTxn, types.LegacyLeiosEBTxsKey(hash))
		if err != nil {
			return nil, err
		}
	}
	txsRaw, err := decodeLeiosEBTxs(val)
	if err != nil {
		return nil, fmt.Errorf("GetLeiosEBTxs: decode: %w", err)
	}
	return txsRaw, nil
}

// leiosLegacyReadsDone reports whether the legacy records are gone, so a miss
// in the LeiosDB is final.
func (d *Database) leiosLegacyReadsDone(store leiosdb.Store) bool {
	if d.leios.migrated.Load() {
		return true
	}
	state, err := store.MigrationState()
	if err != nil {
		return false
	}
	if state == leiosdb.MigrationDone {
		d.leios.migrated.Store(true)
		return true
	}
	return false
}

// deleteLeiosEBCertifiedFromSlot removes the certification markers of every
// endorser block at or after slot, staged in the rollback transaction txn.
// An endorser block shares its announcing ranking block's slot and is
// certified by that block's child, so a rollback to slot removes the
// certifying block of every such occurrence, including one announced by the
// block at slot itself. A re-applied certifying block writes the marker again.
// Markers left unstaged past the transaction's budget only over-retain.
//
// Only a database whose migration has not finished holds markers; the
// LeiosDB derives certification from the chain and writes none.
func (d *Database) deleteLeiosEBCertifiedFromSlot(
	slot uint64,
	txn *Txn,
) error {
	blob := txn.BlobStore()
	if blob == nil {
		return types.ErrBlobStoreUnavailable
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return types.ErrNilTxn
	}
	prefix := []byte(types.LeiosEBCertifiedKeyPrefix)
	it := blob.NewIterator(blobTxn, types.BlobIteratorOptions{Prefix: prefix})
	if it == nil {
		return types.ErrBlobStoreUnavailable
	}
	var keys [][]byte
	it.Seek(types.LeiosEBCertifiedSlotPrefix(slot))
	for ; it.ValidForPrefix(prefix); it.Next() {
		if item := it.Item(); item != nil {
			keys = append(keys, bytes.Clone(item.Key()))
		}
	}
	err := it.Err()
	it.Close()
	if err != nil {
		return fmt.Errorf("list Leios certification markers: %w", err)
	}
	if len(keys) == 0 {
		return nil
	}
	staged := stagedBlobDeleteLimit(blob, blobTxn, len(keys[0]), len(keys))
	for _, key := range keys[:staged] {
		if err := blob.Delete(blobTxn, key); err != nil &&
			!errors.Is(err, types.ErrBlobKeyNotFound) {
			return fmt.Errorf("delete Leios certification marker: %w", err)
		}
	}
	if skipped := len(keys) - staged; skipped > 0 && d.logger != nil {
		d.logger.Warn(
			"Leios certification markers left past the rollback budget",
			"component", "database",
			"skipped", skipped,
			"total", len(keys),
		)
	}
	return nil
}
