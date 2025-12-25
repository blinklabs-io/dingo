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
	"database/sql/driver"
	"errors"
	"fmt"
	"math/big"
	"strconv"

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
	v, ok := val.(string)
	if !ok {
		return fmt.Errorf(
			"value was not expected type, wanted string, got %T",
			val,
		)
	}
	if _, ok := r.SetString(v); !ok {
		return fmt.Errorf("failed to set big.Rat value from string: %s", v)
	}
	return nil
}

//nolint:recvcheck
type Uint64 uint64

func (u Uint64) Value() (driver.Value, error) {
	return strconv.FormatUint(uint64(u), 10), nil
}

func (u *Uint64) Scan(val any) error {
	v, ok := val.(string)
	if !ok {
		return fmt.Errorf(
			"value was not expected type, wanted string, got %T",
			val,
		)
	}
	tmpUint, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return err
	}
	*u = Uint64(tmpUint)
	return nil
}

// ErrBlobKeyNotFound is returned by blob operations when a key is missing
var ErrBlobKeyNotFound = errors.New("blob key not found")

// ErrTxnWrongType is returned when a transaction has the wrong type
var ErrTxnWrongType = errors.New("invalid transaction type")

// ErrNilTxn is returned when a nil transaction is provided where a valid transaction is required
var ErrNilTxn = errors.New("nil transaction")

// ErrNoStoreAvailable is returned when no blob or metadata store is available
var ErrNoStoreAvailable = errors.New("no store available")

// ErrBlobStoreUnavailable is returned when blob store cannot be accessed
var ErrBlobStoreUnavailable = errors.New("blob store unavailable")

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

// BlockMetadata contains metadata for a block stored in blob
type BlockMetadata struct {
	cbor.StructAsArray
	ID       uint64
	Type     uint
	Height   uint64
	PrevHash []byte
}
