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
)

// dumpHistoryIterator is implemented by *chain.ChainIterator and by test fakes
// that supply the same Next behavior.
type dumpHistoryIterator interface {
	Next(blocking bool) (*chain.ChainIteratorResult, error)
}

// collectDumpHistoryPage reads up to maxItems forward blocks from the iterator
// using non-blocking Next. Skips rollback markers. If the page is full, peeks
// one more forward block to set hasMore (without including that block in out).
func collectDumpHistoryPage(
	ctx context.Context,
	iter dumpHistoryIterator,
	maxItems uint32,
) (out []*sync.AnyChainBlock, lastModel *models.Block, hasMore bool, err error) {
	if maxItems == 0 {
		return nil, nil, false, nil
	}
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
