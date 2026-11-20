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

package server

import (
	"context"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
)

// TipFunc reports the newest block the served rows cover. ok is false when
// nothing has been indexed yet.
type TipFunc func(ctx context.Context) (slot, number uint64, ok bool, err error)

// boundedDatabase hides blocks above a tip. Block bodies are shared by the
// ledger and the IndexerDB through one blob store, so block lookups would
// otherwise answer beyond what the IndexerDB's other rows cover.
type boundedDatabase struct {
	MidnightDatabase
	tip TipFunc
}

// NewBoundedDatabase returns db with every block lookup limited to the tip
// reported by tip.
func NewBoundedDatabase(db MidnightDatabase, tip TipFunc) MidnightDatabase {
	return &boundedDatabase{MidnightDatabase: db, tip: tip}
}

func (b *boundedDatabase) BlockByHash(
	ctx context.Context,
	hash []byte,
) (models.Block, error) {
	_, number, ok, err := b.tip(ctx)
	if err != nil {
		return models.Block{}, err
	}
	if !ok {
		return models.Block{}, models.ErrBlockNotFound
	}
	block, err := b.MidnightDatabase.BlockByHash(ctx, hash)
	if err != nil {
		return models.Block{}, err
	}
	if block.Number > number {
		return models.Block{}, models.ErrBlockNotFound
	}
	return block, nil
}

func (b *boundedDatabase) BlockByNumber(
	ctx context.Context,
	n uint64,
) (models.Block, error) {
	_, number, ok, err := b.tip(ctx)
	if err != nil {
		return models.Block{}, err
	}
	if !ok || n > number {
		return models.Block{}, fmt.Errorf(
			"block number %d is beyond the indexed tip: %w",
			n, models.ErrBlockNotFound,
		)
	}
	return b.MidnightDatabase.BlockByNumber(ctx, n)
}

func (b *boundedDatabase) BlocksRecent(
	ctx context.Context,
	count int,
) ([]models.Block, error) {
	slot, number, ok, err := b.tip(ctx)
	if err != nil {
		return nil, err
	}
	if !ok || count <= 0 {
		return nil, nil
	}
	first, err := b.MidnightDatabase.BlockBeforeSlot(ctx, slot+1)
	if err != nil {
		return nil, err
	}
	if first.Number > number {
		return nil, models.ErrBlockNotFound
	}
	blocks := []models.Block{first}
	for i := 1; i < count && uint64(i) <= first.Number; i++ {
		block, err := b.MidnightDatabase.BlockByNumber(
			ctx,
			first.Number-uint64(i),
		)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, block)
	}
	return blocks, nil
}

func (b *boundedDatabase) BlockBeforeSlot(
	ctx context.Context,
	slot uint64,
) (models.Block, error) {
	tipSlot, _, ok, err := b.tip(ctx)
	if err != nil {
		return models.Block{}, err
	}
	if !ok {
		return models.Block{}, models.ErrBlockNotFound
	}
	if slot > tipSlot+1 {
		slot = tipSlot + 1
	}
	return b.MidnightDatabase.BlockBeforeSlot(ctx, slot)
}
