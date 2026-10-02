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
	"context"
	"testing"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// Every Database entry point that opens its own transaction binds that
// transaction to the caller's context, so a cancelled caller sees the
// cancellation instead of a completed read.
func TestDatabaseOwnTransactionsHonorCancelledContext(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	insertTestBlock(t, db, 1, randomHash(t), []byte("cbor"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	point := ocommon.NewPoint(1, randomHash(t))
	for name, call := range map[string]func() error{
		"BlockByPoint": func() error {
			_, err := BlockByPoint(ctx, db, point)
			return err
		},
		"BlockByHash": func() error {
			_, err := BlockByHash(ctx, db, point.Hash)
			return err
		},
		"BlockBySlot": func() error {
			_, err := BlockBySlot(ctx, db, 1)
			return err
		},
		"BlocksRecent": func() error {
			_, err := BlocksRecent(ctx, db, 1)
			return err
		},
		"BlockBeforeSlot": func() error {
			_, err := BlockBeforeSlot(ctx, db, 2)
			return err
		},
		"ResolveBlockNumberBound": func() error {
			_, err := ResolveBlockNumberBound(ctx, db)
			return err
		},
		"GetActiveDreps": func() error {
			_, err := db.GetActiveDreps(ctx, nil)
			return err
		},
		"GetCommitteeMembers": func() error {
			_, err := db.GetCommitteeMembers(ctx, nil)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.ErrorIs(t, call(), context.Canceled)
		})
	}
}
