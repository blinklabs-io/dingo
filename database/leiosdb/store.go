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

// Package leiosdb holds Leios endorser blocks in a store of their own, apart
// from the ranking-block blob store.
//
// Records are keyed slot first and split into two partitions. The volatile
// partition holds every endorser block seen for a slot range that can still be
// rolled back, and is garbage-collected by slot once the immutable tip has
// passed it. The immutable partition holds the endorser blocks certified by a
// ranking block that has itself become immutable, and keeps them permanently.
// An endorser block is moved from one to the other by Pin.
package leiosdb

import (
	"context"
	"errors"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
)

var (
	// ErrNotFound reports that the store holds no such record. It is the blob
	// store's missing-key error so readers that already test for a missing
	// endorser block keep working whichever store answers.
	ErrNotFound = types.ErrBlobKeyNotFound
	// ErrBehindImmutableTip reports a volatile write for an endorser block
	// whose slot the garbage collector has already passed and that is not
	// pinned. Such a block was either never certified or lost its fork, and
	// storing it would only leave a record for the next collection to remove.
	ErrBehindImmutableTip = errors.New(
		"leiosdb: endorser block is behind the immutable tip",
	)
	// ErrClosed reports use of a closed store.
	ErrClosed = errors.New("leiosdb: store closed")
)

// HashSize is the byte length of an endorser-block hash.
const HashSize = 32

// Ref names one occurrence of an endorser block: the announcing ranking
// block's slot and the manifest hash. The same hash can occur at more than one
// slot, and each occurrence is stored and garbage-collected on its own.
type Ref struct {
	Hash []byte
	Slot uint64
}

// Content is the persisted payload of an endorser block.
type Content struct {
	// Manifest is the raw endorser-block manifest CBOR.
	Manifest []byte
	// Txs holds the raw block_transaction CBOR values. A nil slice means the
	// transaction list was not stored (an incomplete endorser block); an empty
	// non-nil slice is a stored, empty list.
	Txs []cbor.RawMessage
}

// Complete reports whether the transaction list is present.
func (c Content) Complete() bool {
	return c.Txs != nil
}

// Watermarks are the two persisted frontiers that make pinning and collection
// resumable.
type Watermarks struct {
	// PinnedThrough is the newest ranking block whose certification has been
	// pinned. It is the zero Ref until the first advance.
	PinnedThrough Ref
	// GCBefore is the slot below which unpinned volatile records have been, or
	// are being, collected. Volatile writes below it are refused.
	GCBefore uint64
	// PinnedThroughSet distinguishes an unset PinnedThrough from the origin.
	PinnedThroughSet bool
}

// PinResult describes the outcome of a Pin call.
type PinResult struct {
	// AlreadyPinned is set when the endorser block was pinned before the call.
	AlreadyPinned bool
	// ContentPending is set when the pin holds no complete content: the
	// manifest or the transaction list was not in the store. The pin is
	// recorded anyway and the content can be supplied later through
	// PutVolatile or PendingPins repair.
	ContentPending bool
}

// PendingPin is a pinned endorser block still missing manifest or
// transactions.
type PendingPin struct {
	EB        Ref
	Certifier Ref
}

// MigrationState records progress of the move of legacy records out of the
// shared blob store.
type MigrationState uint8

const (
	// MigrationNone means no migration has run.
	MigrationNone MigrationState = 0
	// MigrationCopied means every source record has been copied and the
	// sources may be deleted.
	MigrationCopied MigrationState = 1
	// MigrationDone means the sources have been deleted.
	MigrationDone MigrationState = 2
)

// Store is the endorser-block store.
type Store interface {
	// PutVolatile stores content for the endorser block. It returns
	// ErrBehindImmutableTip when the slot is below the collection watermark and
	// the block is not pinned. A pinned block's content goes to the immutable
	// partition, which is how a content-pending pin is repaired.
	PutVolatile(eb Ref, content Content) error
	// PutClosure stores the certified closure CBOR that transaction offsets
	// resolve into. It follows the same partition rule as PutVolatile except
	// that it is never refused: the closure belongs to a block certification
	// has already accepted.
	PutClosure(eb Ref, closure []byte) error
	// Get returns the endorser block's content from the immutable partition,
	// then the volatile one. ErrNotFound when neither holds a manifest.
	Get(eb Ref) (Content, error)
	// GetClosure returns the closure CBOR, immutable partition first.
	GetClosure(eb Ref) ([]byte, error)
	// IsPinned reports whether the endorser block has been pinned.
	IsPinned(eb Ref) (bool, error)
	// Pin moves the endorser block into the immutable partition, recording the
	// ranking block that certifies it. It is idempotent and never fails for
	// missing content; see PinResult.
	Pin(eb, certifier Ref) (PinResult, error)
	// AdvancePinnedThrough records that every certification up to and
	// including the ranking block has been pinned. It never moves backwards.
	AdvancePinnedThrough(through Ref) error
	// GarbageCollect deletes unpinned volatile records with a slot strictly
	// below beforeSlot and raises the collection watermark to it. The
	// watermark is persisted before any delete, so an interrupted collection
	// is finished by the next call. It returns the number of keys deleted.
	GarbageCollect(ctx context.Context, beforeSlot uint64) (int, error)
	// Watermarks returns the persisted frontiers.
	Watermarks() (Watermarks, error)
	// MaxSlot returns the highest slot holding a manifest in either
	// partition. found is false when the store holds none.
	MaxSlot() (slot uint64, found bool, err error)
	// PendingPins lists pins that still lack complete content.
	PendingPins() ([]PendingPin, error)
	// RollbackPins unpins the endorser blocks certified by a ranking block
	// above the target's slot, moving their records back to the volatile
	// partition, then sets the pin
	// watermark to target and lowers the collection watermark to the target's
	// slot so the re-applied chain can store and pin again. A pin whose
	// certifier is unknown (the zero Ref) is kept. It returns the number of
	// pins removed.
	RollbackPins(target Ref) (int, error)
	// MigrationState returns the recorded migration progress.
	MigrationState() (MigrationState, error)
	// SetMigrationState records migration progress.
	SetMigrationState(MigrationState) error
	// Close releases the store.
	Close() error
}
