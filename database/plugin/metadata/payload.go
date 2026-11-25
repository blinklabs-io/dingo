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

package metadata

import (
	"context"

	"github.com/blinklabs-io/dingo/database/types"
)

// PayloadBlockSource finds where the block that carried a stored payload
// holds it.
type PayloadBlockSource interface {
	// LocatorAtSlot returns the locator of the canonical block at slot. A
	// block that expired, and that no archive serves, is reported as a
	// *types.HistoryExpiredError naming it. A slot with no canonical block
	// is reported as an error wrapping models.ErrBlockNotFound.
	LocatorAtSlot(ctx context.Context, slot uint64) (types.PayloadLocator, error)
}

// MidnightRollbackDepth is the number of blocks a Midnight rollback can reach
// back: the Cardano security parameter k. The rows a rollback restores stay
// until the tip is this far past the block that wrote them.
const MidnightRollbackDepth uint64 = 2160

// ContentKey names a datum or script by the hash its table is keyed on.
type ContentKey struct {
	// Table is "datum" or "script".
	Table string
	// Hash is the datum hash or script hash.
	Hash string
}

// PayloadCarrierScanner is implemented by a PayloadBlockSource that can read
// the canonical blocks' transactions, for content that no indexed table links
// to the transaction carrying it: a script in auxiliary data, and a datum or
// reference script in a sub-transaction output.
type PayloadCarrierScanner interface {
	// FindCarriers returns, for each wanted key a canonical transaction
	// carries, the slot of the first canonical block that does. A key absent
	// from the result is carried by no block the scan could read. The error
	// names the first canonical block the scan could not read because it
	// expired with no archive, and is nil when every block was read or every
	// key was found: an absent key might be carried in that block.
	FindCarriers(
		ctx context.Context,
		wanted map[ContentKey]struct{},
	) (map[ContentKey]uint64, *types.HistoryExpiredError, error)
}

// PayloadColumnMigrator is implemented by metadata stores that can replace
// the payload columns of a store written before they held offsets, in place.
type PayloadColumnMigrator interface {
	// MigratePayloadColumns replaces every payload column that holds a copy
	// of its bytes with the CBOR offset of those bytes in the block that
	// carried them, read from blocks. A column that already holds an offset is
	// left as it is, so the migration resumes where it stopped. A datum or
	// script row whose block is not canonical or lacks its bytes is re-pointed
	// to a canonical transaction that carries them, or deleted when none
	// does. Otherwise it fails when a block cannot be read for a reason other
	// than expiry or does not contain a payload, and leaves the rows it had
	// converted converted. A payload whose block expired, and that no live
	// output holds, is kept as an expired-payload marker naming the block.
	MigratePayloadColumns(ctx context.Context, blocks PayloadBlockSource) error
}
