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

package sqlstore

import (
	"math"
	"math/rand/v2"
	"regexp"
	"strings"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// LedgerView.PoolCurrentState makes two store calls, GetPoolStatus and
// GetPoolRegistrationsEffectiveForEpoch. Every statement they issue for a pool
// with a pending retirement is recorded and planned: none reads
// pool_retirement, none scans a table other than epoch, and pool_registration
// is reached only by primary key for the selected registration.
func TestPoolCurrentStateStatementsReadNoHistory(t *testing.T) {
	t.Parallel()
	store, recorder := newRecordingSQLiteStore(t)
	depositHeldEpochs(t, store, 12)
	pool := depositHeldPoolKey(0xc1)
	w := &poolCurrentWorkload{
		t:     t,
		store: store,
		rng:   rand.New(rand.NewPCG(1, 1)),
		pools: []lcommon.PoolKeyHash{pool},
		used:  map[[2]uint64]struct{}{},
	}
	w.apply(1_000, 0, depositHeldRegistration(pool), nil)
	w.apply(2_000, 0, depositHeldRetirement(pool, 9), nil)
	require.NoError(t, store.SetTip(ochainsync.Tip{
		Point: ocommon.Point{Slot: 5_000, Hash: make([]byte, 32)},
	}, nil))

	mark := len(recorder.snapshot())
	status, err := store.GetPoolStatus(pool, nil)
	require.NoError(t, err)
	regs, err := store.GetPoolRegistrationsEffectiveForEpoch(
		w.pools, 5_000, 4, math.MaxInt64, nil,
	)
	require.NoError(t, err)
	statements := recorder.snapshot()[mark:]

	require.True(t, status.Active)
	require.Equal(t, new(uint64(9)), status.RetirementEpoch)
	require.Len(t, regs, 1)
	require.Equal(t, uint64(1_000), regs[0].AddedSlot)
	// Status, current epoch, stored events, the registration row, and its
	// owners and relays.
	require.Len(t, statements, 6)
	scan := regexp.MustCompile(`(?m)^SCAN (\w+)`)
	registrationRead := regexp.MustCompile(
		`(?m)^SEARCH p USING INTEGER PRIMARY KEY`,
	)
	for _, statement := range statements {
		require.NotContains(t, statement, "pool_retirement", statement)
		plan := queryPlan(
			t, store.writeDB, statement,
			make([]any, strings.Count(statement, "?"))...,
		)
		for _, match := range scan.FindAllStringSubmatch(plan, -1) {
			require.Equal(t, "epoch", match[1], "statement: %s\n%s", statement, plan)
		}
		if strings.Contains(statement, "FROM pool_registration p") {
			require.Regexp(t, registrationRead, plan, statement)
		}
	}
}
