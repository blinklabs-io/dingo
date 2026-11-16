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

package utxoset

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// MigrationSyncKey is the sync_state key recording how far the move of a
// database's UTxO set from the metadata store into the live UTxO set has
// gone. It is absent before the move starts and after it ends.
const MigrationSyncKey = "ledger.utxo_set_migration"

const (
	migrationBootstrapped = "bootstrapped:"
	migrationCommitted    = "committed"
)

// defaultMigrationChunk is how many utxo rows one delete statement removes.
const defaultMigrationChunk = 5000

// MigrationStore is the metadata store as the UTxO set migration uses it.
type MigrationStore interface {
	// SQLTip returns the metadata store's tip.
	SQLTip(ctx context.Context) (ocommon.Point, error)
	GetSyncState(ctx context.Context, key string) (string, error)
	SetSyncState(ctx context.Context, key, value string) error
	DeleteSyncState(ctx context.Context, key string) error
	// DeleteUtxoRows deletes up to limit utxo rows and returns how many it
	// deleted; fewer than limit means none remain.
	DeleteUtxoRows(ctx context.Context, limit int) (int64, error)
}

// MigrationEnv carries what MigrateFromSQL needs.
type MigrationEnv struct {
	Store MigrationStore
	// Mark records that the metadata store holds no UTxO set. It is called
	// once the set is verified and again on every resumed run, so it must be
	// idempotent.
	Mark func(ctx context.Context) error
	// Marked reports whether the set-only mark is already written. A run that
	// finds the commit recorded and no mark checks the set again, since the
	// metadata store may have moved on; nil reads as not marked.
	Marked func(ctx context.Context) (bool, error)
	// FreeBytes returns the bytes available for the set's directory; nil
	// skips the space check.
	FreeBytes func() (uint64, error)
	// ChunkRows overrides the rows one delete statement removes.
	ChunkRows int
	// Hook, when non-nil, is called after each step with the step's name; an
	// error it returns stops the migration there, as a crash would. Tests
	// use it to stop between steps.
	Hook   func(step string) error
	Logger *slog.Logger
}

// ErrMigrationInsufficientSpace is wrapped by the error MigrateFromSQL
// returns when the filesystem cannot hold the set.
var ErrMigrationInsufficientSpace = errors.New(
	"not enough free disk space to build the live UTxO set",
)

// spaceFactor is how many times the live outputs' CBOR the set's directory is
// allowed to need: Badger keeps a value log and levels beside the live data.
const spaceFactor = 2

// MigrateFromSQL moves a database that holds its UTxO set in the metadata
// store to the set-only layout in place. The metadata store stays the
// authority until the end: the set is built from its live utxo rows, checked
// against them by count and a streamed digest of every output, the set-only
// mark is written, and only then are the utxo rows deleted in bounded
// batches. Every step is resumable: a crash before the mark leaves the
// metadata store whole and the next run rebuilds or reuses the set after
// checking it again; a crash after the mark resumes the deletes.
func (s *Set) MigrateFromSQL(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	env MigrationEnv,
) error {
	if s.sqlDrained.Load() {
		return errors.New(
			"utxoset: the metadata store has drained ledger-state families, so its UTxO rows cannot be the source of a migration",
		)
	}
	s.migrating.Store(true)
	defer s.migrating.Store(false)
	log := env.Logger
	if log == nil {
		log = s.logger
	}
	hook := func(step string) error {
		if env.Hook == nil {
			return nil
		}
		return env.Hook(step)
	}
	progress, err := env.Store.GetSyncState(ctx, MigrationSyncKey)
	if err != nil {
		return fmt.Errorf("utxoset: read migration progress: %w", err)
	}
	marked := false
	if env.Marked != nil {
		if marked, err = env.Marked(ctx); err != nil {
			return fmt.Errorf("utxoset: read the UTxO set mark: %w", err)
		}
	}
	// Once the mark is written the rows are being deleted and cannot be
	// checked against; before it, a commit recorded by an earlier run says
	// nothing of what the metadata store has been given since.
	if progress != migrationCommitted || !marked {
		if err := s.buildAndVerify(
			ctx, ldb, env, log, hook,
		); err != nil {
			return err
		}
		if err := env.Store.SetSyncState(
			ctx, MigrationSyncKey, migrationCommitted,
		); err != nil {
			return fmt.Errorf("utxoset: record migration commit: %w", err)
		}
		if err := hook("committed"); err != nil {
			return err
		}
	}
	if err := env.Mark(ctx); err != nil {
		return fmt.Errorf("utxoset: record the UTxO set mark: %w", err)
	}
	if err := hook("marked"); err != nil {
		return err
	}
	if err := s.drainUtxoRows(ctx, env, log, hook); err != nil {
		return err
	}
	if err := env.Store.DeleteSyncState(ctx, MigrationSyncKey); err != nil {
		return fmt.Errorf("utxoset: clear migration progress: %w", err)
	}
	log.Info("moved the UTxO set out of the metadata store")
	return hook("done")
}

func (s *Set) buildAndVerify(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	env MigrationEnv,
	log *slog.Logger,
	hook func(string) error,
) error {
	sqlTip, err := env.Store.SQLTip(ctx)
	if err != nil {
		return fmt.Errorf("utxoset: read metadata tip: %w", err)
	}
	log.Info("scanning the metadata store's live UTxO set",
		"slot", sqlTip.Slot)
	want, err := scanSQLUtxos(ctx, ldb)
	if err != nil {
		return fmt.Errorf("utxoset: scan metadata UTxO set: %w", err)
	}
	log.Info("scanned the metadata store's live UTxO set",
		"live_outputs", want.count, "cbor_bytes", want.bytes)
	if err := hook("scanned"); err != nil {
		return err
	}
	marker := migrationBootstrapped + pointKey(sqlTip)
	setTip, hadTip, err := s.store.Tip()
	if err != nil {
		return fmt.Errorf("utxoset: read tip: %w", err)
	}
	stale, err := s.store.Stale()
	if err != nil {
		return fmt.Errorf("utxoset: read format: %w", err)
	}
	// A set already at the metadata store's tip is kept, with its journal and
	// rollback window, when its digest matches below; only one that differs
	// is rebuilt.
	reuse := hadTip && !stale && samePoint(setTip, sqlTip)
	build := func() error {
		if env.FreeBytes != nil {
			free, err := env.FreeBytes()
			if err != nil {
				return fmt.Errorf("utxoset: read free disk space: %w", err)
			}
			// What the set already holds is rebuilt in place, so it is not
			// needed again.
			need := want.bytes * spaceFactor
			if have := s.store.DataBytes(); have > 0 {
				need -= min(need, uint64(have))
			}
			if free < need {
				return fmt.Errorf(
					"utxoset: %w: %d bytes free, about %d needed for %d outputs",
					ErrMigrationInsufficientSpace, free, need, want.count,
				)
			}
		}
		log.Info("building the live UTxO set from the metadata store",
			"slot", sqlTip.Slot)
		if err := s.rebuild(ctx, ldb, nil, sqlTip, hadTip, setTip); err != nil {
			return err
		}
		if err := env.Store.SetSyncState(
			ctx, MigrationSyncKey, marker,
		); err != nil {
			return fmt.Errorf("utxoset: record migration progress: %w", err)
		}
		return hook("bootstrapped")
	}
	if !reuse {
		if err := build(); err != nil {
			return err
		}
	}
	got, err := s.scanSet()
	if err != nil {
		return fmt.Errorf("utxoset: scan the live UTxO set: %w", err)
	}
	if got != want && reuse {
		// A set kept from an earlier run that no longer matches is
		// rebuilt once; the metadata store is still the authority.
		log.Warn("the kept live UTxO set differs from the metadata store; rebuilding it")
		setTip, hadTip, err = s.store.Tip()
		if err != nil {
			return fmt.Errorf("utxoset: read tip: %w", err)
		}
		if err := build(); err != nil {
			return err
		}
		if got, err = s.scanSet(); err != nil {
			return fmt.Errorf("utxoset: scan the live UTxO set: %w", err)
		}
	}
	if got != want {
		return fmt.Errorf(
			"utxoset: the live UTxO set built from the metadata store does not match it: %d outputs against %d, digest %s against %s",
			got.count, want.count, got.hex(), want.hex(),
		)
	}
	log.Info("verified the live UTxO set against the metadata store",
		"live_outputs", got.count, "digest", got.hex())
	return hook("verified")
}

func (s *Set) drainUtxoRows(
	ctx context.Context,
	env MigrationEnv,
	log *slog.Logger,
	hook func(string) error,
) error {
	chunk := env.ChunkRows
	if chunk <= 0 {
		chunk = defaultMigrationChunk
	}
	var total int64
	for batches := 1; ; batches++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := env.Store.DeleteUtxoRows(ctx, chunk)
		if err != nil {
			return fmt.Errorf("utxoset: delete utxo rows: %w", err)
		}
		total += n
		if batches%100 == 0 {
			log.Info("deleting the metadata store's utxo rows",
				"deleted", total)
		}
		if err := hook(fmt.Sprintf("drain:%d", batches)); err != nil {
			return err
		}
		if n < int64(chunk) {
			break
		}
	}
	log.Info("deleted the metadata store's utxo rows", "deleted", total)
	return nil
}

// utxoDigest summarizes a set of outputs independently of their order: the
// count, the CBOR bytes and the lane-wise sum of each output's SHA-256 over
// its reference and CBOR.
type utxoDigest struct {
	count uint64
	bytes uint64
	lanes [4]uint64
}

func (d *utxoDigest) add(txID []byte, idx uint32, cbor []byte) {
	h := sha256.New()
	h.Write(txID)
	var n [8]byte
	binary.BigEndian.PutUint32(n[:4], idx)
	binary.BigEndian.PutUint32(n[4:], uint32(len(cbor))) //nolint:gosec // output CBOR is far below 4 GiB
	h.Write(n[:])
	h.Write(cbor)
	sum := h.Sum(nil)
	for i := range d.lanes {
		d.lanes[i] += binary.BigEndian.Uint64(sum[i*8:])
	}
	d.count++
	d.bytes += uint64(len(cbor))
}

func (d *utxoDigest) hex() string {
	var b [32]byte
	for i, l := range d.lanes {
		binary.BigEndian.PutUint64(b[i*8:], l)
	}
	return hex.EncodeToString(b[:])
}

func scanSQLUtxos(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) (utxoDigest, error) {
	var d utxoDigest
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	err := ldb.IterateLiveUtxos(ctx, txn, func(u *models.Utxo) error {
		d.add(u.TxId, u.OutputIdx, u.Cbor)
		return nil
	})
	return d, err
}

func (s *Set) scanSet() (utxoDigest, error) {
	var d utxoDigest
	err := s.EachUtxo(func(txID []byte, idx uint32, cbor []byte) error {
		d.add(txID, idx, cbor)
		return nil
	})
	return d, err
}

func pointKey(p ocommon.Point) string {
	return fmt.Sprintf("%d:%s", p.Slot, hex.EncodeToString(p.Hash))
}
