// Copyright 2025 Blink Labs Software
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

package types

import (
	"bytes"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"strconv"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
)

//nolint:recvcheck
type Rat struct {
	*big.Rat
}

func (r Rat) Value() (driver.Value, error) {
	if r.Rat == nil {
		return "", nil
	}
	return r.String(), nil
}

func (r *Rat) Scan(val any) error {
	if r.Rat == nil {
		r.Rat = new(big.Rat)
	}
	switch v := val.(type) {
	case nil:
		r.SetInt64(0)
		return nil
	case string:
		if v == "" {
			r.SetInt64(0)
			return nil
		}
		if _, ok := r.SetString(v); !ok {
			return fmt.Errorf("failed to set big.Rat value from string: %s", v)
		}
		return nil
	case []byte:
		if len(v) == 0 {
			r.SetInt64(0)
			return nil
		}
		if _, ok := r.SetString(string(v)); !ok {
			return fmt.Errorf("failed to set big.Rat value from string: %s", string(v))
		}
		return nil
	default:
		return fmt.Errorf(
			"value was not expected type, wanted string/[]byte, got %T",
			val,
		)
	}
}

//nolint:recvcheck
type Uint64 uint64

func (u Uint64) Value() (driver.Value, error) {
	return strconv.FormatUint(uint64(u), 10), nil
}

func (u *Uint64) Scan(val any) error {
	switch v := val.(type) {
	case nil:
		*u = 0
		return nil
	case uint64:
		*u = Uint64(v)
		return nil
	case int64:
		if v < 0 {
			return fmt.Errorf("invalid negative value for Uint64: %d", v)
		}
		*u = Uint64(v)
		return nil
	case []byte:
		if len(v) == 0 {
			*u = 0
			return nil
		}
		tmpUint, err := strconv.ParseUint(string(v), 10, 64)
		if err != nil {
			return err
		}
		*u = Uint64(tmpUint)
		return nil
	case string:
		if v == "" {
			*u = 0
			return nil
		}
		tmpUint, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return err
		}
		*u = Uint64(tmpUint)
		return nil
	default:
		return fmt.Errorf(
			"value was not expected type, wanted string/[]byte/uint64/int64, got %T",
			val,
		)
	}
}

// Storage mode constants shared by the metadata plugins.
const (
	// StorageModeCore stores only consensus and chain state data.
	StorageModeCore = "core"
	// StorageModeAPI stores everything needed for API queries.
	StorageModeAPI = "api"
)

// NodeSettings holds immutable node configuration that is persisted on first
// sync and enforced on every subsequent startup.
type NodeSettings struct {
	StorageMode string
	Network     string
}

// ErrBlobKeyNotFound is returned by blob operations when a key is missing
var ErrBlobKeyNotFound = errors.New("blob key not found")

// ErrHistoryExpired is returned when a block's local CBOR has expired from
// history storage. Callers can use errors.Is(err, ErrHistoryExpired) for
// existence checks. To get the (slot, hash), use errors.As to extract a
// *HistoryExpiredError. Archive-proxy wrappers can intercept this typed
// error and resolve the block from remote history; unwrapped consumers should
// surface the condition as unavailable expired history.
var ErrHistoryExpired = errors.New("history expired")

// HistoryExpiredError is the typed form of ErrHistoryExpired, returned by
// blob plugins when GetBlock or iterator ValueCopy observe an expired block.
// It carries the (slot, hash) so archive-proxy wrappers can resolve it without
// parsing blob keys — the wrapper extracts it via:
//
//	var h *types.HistoryExpiredError
//	if errors.As(err, &h) { fetchFromArchive(h.Slot, h.Hash) }
type HistoryExpiredError struct {
	Slot uint64
	Hash []byte
}

func (e *HistoryExpiredError) Error() string {
	return fmt.Sprintf(
		"%s: slot=%d hash=%s",
		ErrHistoryExpired, e.Slot, hex.EncodeToString(e.Hash),
	)
}

// Unwrap returns ErrHistoryExpired so errors.Is(err, ErrHistoryExpired) keeps
// working for callers that only need to detect the condition.
func (e *HistoryExpiredError) Unwrap() error { return ErrHistoryExpired }

// ErrBlockTombstoned and BlockTombstonedError are retained as aliases for
// older callers. New code should use ErrHistoryExpired and HistoryExpiredError.
var ErrBlockTombstoned = ErrHistoryExpired

type BlockTombstonedError = HistoryExpiredError

// ErrTxnWrongType is returned when a transaction has the wrong type
var ErrTxnWrongType = errors.New("invalid transaction type")

// ErrNilTxn is returned when a nil transaction is provided where a valid transaction is required
var ErrNilTxn = errors.New("nil transaction")

// ErrNoStoreAvailable is returned when no blob or metadata store is available
var ErrNoStoreAvailable = errors.New("no store available")

// ErrBlobStoreUnavailable is returned when blob store cannot be accessed
var ErrBlobStoreUnavailable = errors.New("blob store unavailable")

// ErrNoEpochData is returned when epoch data has not been synced yet for the
// requested slot. Callers should distinguish this from "no active pools" and
// handle it appropriately (e.g., retry after sync progresses).
var ErrNoEpochData = errors.New(
	"no epoch data available for requested slot",
)

// ErrPartialCommit is returned when blob commits but metadata fails,
// leaving the database in an inconsistent state that requires recovery.
var ErrPartialCommit = errors.New(
	"partial commit: blob committed but metadata failed",
)

// ErrUtxoConflict is returned when a UTxO spend fails because the UTxO
// was already consumed by another transaction or restored by a rollback.
// This sentinel error allows callers to distinguish optimistic locking
// conflicts from other errors and retry or reject accordingly.
var ErrUtxoConflict = errors.New("UTxO already spent")

// ErrUtxoNotFound is returned when a requested UTxO row does not exist.
var ErrUtxoNotFound = errors.New("utxo not found")

// UtxoKey identifies a UTxO row by its transaction id and output
// index. Used as a parameter type for batch UTxO operations across
// the metadata-store interface (e.g. MarkUtxosDeletedAtSlot). The
// fixed-size UtxoRef in database/cbor_cache.go is for in-memory map
// keys; this variable-length form matches the byte slices stored on
// the UTxO row directly.
type UtxoKey struct {
	TxId      []byte
	OutputIdx uint32
}

// BlobItem represents a value returned by an iterator
type BlobItem interface {
	Key() []byte
	ValueCopy(dst []byte) ([]byte, error)
}

// BlobIterator provides key iteration over the blob store.
//
// Important lifecycle constraint: items returned by `Item()` must only be
// accessed while the underlying transaction used to create the iterator is
// still active. Implementations may validate transaction state at access
// time (for example `ValueCopy` may fail if the transaction has been committed
// or rolled back). Typical usage iterates and accesses item values within the
// same transaction scope.
type BlobIterator interface {
	Rewind()
	Seek(prefix []byte)
	Valid() bool
	ValidForPrefix(prefix []byte) bool
	Next()
	Item() BlobItem
	Close()
	Err() error
}

// BlobIteratorOptions configures blob iterator creation
type BlobIteratorOptions struct {
	Prefix  []byte
	Reverse bool
}

// Txn is a simple transaction handle for commit/rollback only.
// Database layer (Txn) coordinates metadata and blob operations separately.
type Txn interface {
	Commit() error
	Rollback() error
}

// MetadataBatchAccumulator is an opaque plugin-owned accumulator used by
// metadata stores that support batched transaction ingestion.
type MetadataBatchAccumulator interface {
	Reset()
}

// BlockMetadata contains metadata for a block stored in blob.
// IMPORTANT: Field order must remain [ID, Type, Height, PrevHash] because
// cbor.StructAsArray encodes/decodes by position. Changing the order would
// break deserialization of existing stored data.
type BlockMetadata struct {
	cbor.StructAsArray
	ID       uint64
	Type     uint
	Height   uint64
	PrevHash []byte
}

// SignedURL is a url that has been pre-signed and has an expiration time
type SignedURL struct {
	URL     url.URL
	Expires time.Time
}

// BlockTombstoneMagic is the four-byte prefix that identifies an expired
// history marker stored at a block's bp key. The bytes "DBT1" do not appear
// at the start of any valid Cardano block CBOR (blocks begin with a CBOR
// array header byte such as 0x82/0x83), so detection is unambiguous.
var BlockTombstoneMagic = [4]byte{'D', 'B', 'T', '1'}

// BlockTombstone returns a fresh copy of the tombstone marker that
// replaces a block's CBOR when the block has expired from local history.
// The marker carries no embedded (slot, hash) — the bp key already does,
// and surfacing the typed HistoryExpiredError is the responsibility of
// each blob plugin (which knows its own key format).
func BlockTombstone() []byte {
	out := make([]byte, len(BlockTombstoneMagic))
	copy(out, BlockTombstoneMagic[:])
	return out
}

// IsBlockTombstone reports whether the given bytes are a tombstone
// marker. Tested by prefix so any payload starting with the magic is
// treated as a tombstone — a partial or unexpectedly extended marker
// must never be misread as block CBOR.
func IsBlockTombstone(data []byte) bool {
	return bytes.HasPrefix(data, BlockTombstoneMagic[:])
}
