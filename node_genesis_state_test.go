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

package dingo

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecideInitialGenesisModeStartupMatrix(t *testing.T) {
	t.Parallel()
	epoch := time.Unix(1_700_000_000, 0)
	slotTime := func(slot uint64) (time.Time, error) {
		return epoch.Add(time.Duration(slot) * time.Second), nil
	}
	failing := func(uint64) (time.Time, error) {
		return time.Time{}, errors.New("past horizon")
	}
	tip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 1000, Hash: []byte("tip")},
		BlockNumber: 10,
	}
	maxAge := chainselection.DefaultMaxCaughtUpAge
	young := epoch.Add(1000*time.Second + maxAge - time.Second)
	old := epoch.Add(1000*time.Second + maxAge)

	tests := []struct {
		name       string
		eligible   bool
		marker     bool
		tip        ochainsync.Tip
		slotTime   func(uint64) (time.Time, error)
		now        time.Time
		wantGen    bool
		wantMarker bool
	}{
		{"no marker starts in Genesis", true, false, tip, slotTime, young, true, false},
		{"marker with a young tip starts in Praos", true, true, tip, slotTime, young, false, true},
		{"marker with an old tip is removed and starts in Genesis", true, true, tip, slotTime, old, true, false},
		{"marker with an unconvertible tip is too old", true, true, tip, failing, young, true, false},
		{"marker at the origin is too old", true, true, ochainsync.Tip{}, slotTime, young, true, false},
		{"override forces Praos without a marker", false, false, tip, slotTime, young, false, false},
		{"override forces Praos and leaves the marker", false, true, tip, slotTime, old, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), caughtUpMarkerName)
			if tt.marker {
				require.NoError(t, os.WriteFile(path, nil, 0o600))
			}
			got := decideInitialGenesisMode(
				tt.eligible, path, tt.tip, tt.slotTime, tt.now, maxAge, nil,
			)
			assert.Equal(t, tt.wantGen, got)
			_, err := os.Stat(path)
			assert.Equal(t, tt.wantMarker, err == nil)
		})
	}
}

// A marker that cannot be removed is logged and ignored: the node still
// starts in Genesis because the tip-age check, not the file, decides.
func TestDecideInitialGenesisModeUnremovableMarkerStillStartsInGenesis(
	t *testing.T,
) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), caughtUpMarkerName)
	require.NoError(t, os.MkdirAll(filepath.Join(path, "child"), 0o700))
	got := decideInitialGenesisMode(
		true, path, ochainsync.Tip{}, nil, time.Now(),
		chainselection.DefaultMaxCaughtUpAge, nil,
	)
	assert.True(t, got)
}

func TestHistoricityCutoffForMatchesReference(t *testing.T) {
	t.Parallel()
	// 3k/f = 129600 slots of one second plus one hour.
	assert.Equal(
		t,
		(129600+3600)*time.Second,
		historicityCutoffFor(129600, big.NewRat(1, 1)),
	)
	// A Byron-length slot scales the window.
	assert.Equal(
		t,
		(2*20+3600)*time.Second,
		historicityCutoffFor(2, big.NewRat(20, 1)),
	)
}

func TestCaughtUpMarkerFollowsSelectorModeEvents(t *testing.T) {
	t.Parallel()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	dir := t.TempDir()
	n := &Node{
		eventBus: bus,
		config:   NewConfig(WithDatabasePath(dir)),
	}
	n.subscribeCaughtUpMarkerEvents()
	path := filepath.Join(dir, caughtUpMarkerName)

	bus.Publish(
		chainselection.GenesisModeExitedEventType,
		event.NewEvent(
			chainselection.GenesisModeExitedEventType,
			chainselection.GenesisModeExitedEvent{},
		),
	)
	testutil.WaitForCondition(t, func() bool {
		_, err := os.Stat(path)
		return err == nil
	}, 2*time.Second, "exit must write the marker")

	bus.Publish(
		chainselection.GenesisModeEnteredEventType,
		event.NewEvent(
			chainselection.GenesisModeEnteredEventType,
			chainselection.GenesisModeEnteredEvent{},
		),
	)
	testutil.WaitForCondition(t, func() bool {
		_, err := os.Stat(path)
		return os.IsNotExist(err)
	}, 2*time.Second, "entry must remove the marker")
}

// A live truncate changes the tip a marker vouched for, so it must go.
func TestLiveTruncateRemovesCaughtUpMarker(t *testing.T) {
	t.Parallel()
	const numBlocks = 20
	n, points := newLiveLifecycleTestNode(t, numBlocks)
	path := filepath.Join(n.config.dataDir, caughtUpMarkerName)
	require.NoError(t, os.WriteFile(path, nil, 0o600))

	targetSlot := points[numBlocks/2].Slot
	_, err := n.Truncate(
		context.Background(),
		dblifecycle.TruncateTarget{Slot: &targetSlot},
	)
	require.NoError(t, err)

	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err), "truncate must remove the marker")
}

// The historicity cutoff is the stability window 3k/f of the network plus one
// hour. A configured Genesis density window changes how density is compared,
// not how old an honest caught-up peer's tip may be, so it must not move the
// cutoff.
func TestHistoricityCutoffIgnoresConfiguredGenesisWindow(t *testing.T) {
	t.Parallel()
	n := &Node{config: Config{
		logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		cardanoNodeConfig: shelleyGenesisCfgForBP(t, time.Now()),
	}}
	// The fixture genesis has k = 10, f = 1/2 and one-second slots: 3k/f is
	// 60 slots.
	want := 60*time.Second + time.Hour
	for _, configured := range []uint64{0, 6, 5000} {
		assert.Equal(
			t,
			want,
			n.buildChainSelectorConfig(10, true, configured).HistoricityCutoff,
			"configured window %d", configured,
		)
	}
}
