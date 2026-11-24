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

// PayloadColumnMigrator is implemented by metadata stores that can replace
// the payload columns of a store written before they held offsets, in place.
type PayloadColumnMigrator interface {
	// MigratePayloadColumns replaces every payload column that holds a copy
	// of its bytes with the CBOR offset of those bytes in the block that
	// carried them, read from blocks. A column that already holds an offset is
	// left as it is, so the migration resumes where it stopped. It fails when
	// a block cannot be read or does not contain a payload, and leaves the
	// rows it had converted converted. A payload whose block expired is kept
	// as an expired-payload marker naming the block.
	MigratePayloadColumns(ctx context.Context, blocks PayloadBlockSource) error
}
