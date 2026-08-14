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

package ledgerstate

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// The reward basis is derived from the pool registrations in the database, so
// it can only be right if every registration the import is going to create
// already exists when it runs. importSnapShots creates registrations in two
// later stages -- the fallback pool import, and the retired-but-scheduled
// synthesis at the end -- and the seeding used to sit ahead of both. Whatever
// those stages added was therefore invisible to it, and which pools the
// seeding could describe depended on where in the function it happened to sit
// rather than on what the import knew.
//
// The fallback path shows it plainly: it exists for a resume where cert state
// completed in an earlier run, so it is exactly the case where the pools come
// from that later stage and nowhere else. Seeded ahead of it, the seeding sees
// an empty pool table.
//
// Ordering is the assertion because it is the invariant -- the seeding must be
// downstream of every stage that writes a pool. Asserting on the resulting
// rows instead would tie this test to the fixture's snapshot format: these are
// compact UTxO-HD snapshots carrying no reward accounts, so the basis is
// dropped by the gate either way, and a row-level assertion would pass for
// reasons that have nothing to do with the ordering.
func TestImportSnapShotsSeedsAfterEveryPoolImportStage(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)
	require.NotNil(t, state.Tip)

	recorder := &messageRecorder{}
	cfg := ImportConfig{
		Database: db,
		State:    state,
		Logger:   slog.New(recorder),
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 500, nil
		},
	}

	// No importCertState: this is the resume the fallback exists for, where
	// the only pools this run learns about come from the fallback itself.
	require.NoError(t, importSnapShots(
		context.Background(),
		cfg,
		state.Tip.Slot,
		func(ImportProgress) {},
		true,
	))

	// Matched against the two messages seedImportedRewardInputs emits, in
	// full, rather than a shared fragment of them: "not seeding ..."
	// contains "seeding ...", so a fragment would match both by accident and
	// leave it unclear which one the fixture actually produces.
	const (
		seededMsg  = "seeded reward inputs for an imported epoch"
		droppedMsg = "not seeding reward inputs for an imported epoch: " +
			"the derived basis does not reconcile, so that epoch's reward " +
			"round will be skipped and its rewards never credited"
	)
	messages := recorder.snapshot()
	seedIdx := firstIndexContaining(messages, seededMsg, droppedMsg)
	require.GreaterOrEqual(t, seedIdx, 0,
		"the seeding did not run at all, so this test proves nothing")
	// Which one fires is the claim the comment above rests on. Pin it, so
	// that if a future fixture does carry reward accounts this test says so
	// instead of quietly resting on a stale rationale.
	require.Equal(t, droppedMsg, messages[seedIdx],
		"the fixture's compact snapshots carry no reward accounts, so the "+
			"gate must drop the basis; a seeded basis here means the "+
			"reason this test asserts ordering rather than rows no longer "+
			"holds")

	for _, stage := range []string{
		// The fallback pool import, which writes the registrations the
		// seeding reads.
		"importing pools",
		// The last stage that can add a pool: retired-but-scheduled
		// synthesis runs immediately after this one.
		"imported active pool distribution",
	} {
		idx := firstIndexContaining(messages, stage)
		require.GreaterOrEqual(t, idx, 0,
			"stage %q did not run, so the ordering it anchors is untested",
			stage)
		require.Less(t, idx, seedIdx,
			"the reward basis was seeded before %q, so any pool that stage "+
				"created was invisible to it", stage)
	}
}

func firstIndexContaining(messages []string, needles ...string) int {
	for i, msg := range messages {
		if slices.ContainsFunc(needles, func(n string) bool {
			return strings.Contains(msg, n)
		}) {
			return i
		}
	}
	return -1
}

// messageRecorder is a slog.Handler that keeps messages in emission order.
// Ordering is the thing under test, so the handler records sequence rather
// than the test scraping a formatted buffer.
type messageRecorder struct {
	mu       sync.Mutex
	messages []string
}

func (r *messageRecorder) Enabled(context.Context, slog.Level) bool {
	return true
}

func (r *messageRecorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, rec.Message)
	return nil
}

func (r *messageRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *messageRecorder) WithGroup(string) slog.Handler      { return r }

func (r *messageRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.messages)
}
