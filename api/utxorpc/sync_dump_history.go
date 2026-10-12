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

package utxorpc

import (
	"context"
	"errors"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	sync "github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync"
	"google.golang.org/protobuf/proto"
)

// dumpHistoryIterator is implemented by *chain.ChainIterator and by test fakes
// that supply the same Next behavior.
type dumpHistoryIterator interface {
	Next(blocking bool) (*chain.ChainIteratorResult, error)
}

// effectiveDumpHistoryMaxItems maps DumpHistoryRequest.max_items to a page size.
// Protobuf uses 0 when the client omits the field; that is treated as
// defaultItems, the page size for requests that did not choose one. If
// defaultItems is 0, the result is 0 (empty page).
func effectiveDumpHistoryMaxItems(requested, defaultItems uint32) uint32 {
	if requested != 0 {
		return requested
	}
	return defaultItems
}

// collectDumpHistoryPage reads up to maxItems forward blocks from the iterator
// using non-blocking Next. Skips rollback markers. If the page is full, peeks
// one more forward block to set hasMore (without including that block in out).
// Pass requested maxItems and defaultItems; unset (0) is resolved via
// effectiveDumpHistoryMaxItems.
//
// Collection also stops once the serialized blocks reach maxBytes. A block that
// would cross the budget is left out, so the page never exceeds it, except that
// a first block larger than maxBytes is still returned: the continuation token
// must advance.
func collectDumpHistoryPage(
	ctx context.Context,
	iter dumpHistoryIterator,
	maxItems uint32,
	defaultItems uint32,
	maxBytes int,
) (out []*sync.AnyChainBlock, lastModel *models.Block, hasMore bool, err error) {
	maxItems = effectiveDumpHistoryMaxItems(maxItems, defaultItems)
	if maxItems == 0 {
		return nil, nil, false, nil
	}
	pageBytes := 0
	for len(out) < int(maxItems) {
		if err := ctx.Err(); err != nil {
			return nil, nil, false, err
		}
		next, err := iter.Next(false)
		if err != nil {
			if errors.Is(err, chain.ErrIteratorChainTip) {
				return out, lastModel, false, nil
			}
			return nil, nil, false, err
		}
		if next == nil {
			continue
		}
		if next.Rollback {
			continue
		}
		acb, err := anyChainBlockFromModel(next.Block)
		if err != nil {
			return nil, nil, false, err
		}
		pageBytes += proto.Size(acb)
		if len(out) > 0 && pageBytes > maxBytes {
			return out, lastModel, true, nil
		}
		out = append(out, acb)
		lm := next.Block
		lastModel = &lm
	}
	// Page full: peek for more forward blocks.
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, false, err
		}
		peek, err := iter.Next(false)
		if err != nil {
			if errors.Is(err, chain.ErrIteratorChainTip) {
				return out, lastModel, false, nil
			}
			return nil, nil, false, err
		}
		if peek == nil {
			continue
		}
		if peek.Rollback {
			continue
		}
		return out, lastModel, true, nil
	}
}
