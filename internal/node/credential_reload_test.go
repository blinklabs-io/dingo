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

package node

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/internal/health"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

// TestReloadOnSignalReloadsPerSignalAndSurvivesFailure delivers two signals to
// the loop: the first reload fails, the second succeeds. A failed reload must
// not end the loop, since the node keeps forging on its loaded credentials and
// the operator retries after fixing the files, and the loop must end with its
// context.
func TestReloadOnSignalReloadsPerSignalAndSurvivesFailure(t *testing.T) {
	t.Parallel()

	sigs := make(chan os.Signal, 2)
	calls := make(chan int)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attempts := 0
	reload := func() error {
		attempts++
		calls <- attempts
		if attempts == 1 {
			return errors.New("opcert counter below the loaded counter")
		}
		return nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		reloadOnSignal(
			ctx, sigs, reload, slog.New(slog.NewTextHandler(io.Discard, nil)),
		)
	}()

	for want := 1; want <= 2; want++ {
		sigs <- syscall.SIGHUP
		select {
		case got := <-calls:
			if got != want {
				t.Fatalf("reload attempt %d, want %d", got, want)
			}
		case <-done:
			t.Fatalf("loop ended before reload attempt %d", want)
		case <-time.After(testutil.AsyncWait):
			t.Fatalf("reload attempt %d never ran", want)
		}
	}

	cancel()
	testutil.RequireReceive(
		t, done, testutil.AsyncWait, "loop did not end with its context",
	)
}

// TestNodeHealthChecksSplitLivenessFromReadiness pins what Run hands the
// probes: only the slot-clock heartbeat may fail liveness, while the database
// and forging checks hold readiness alone. A database check marked as a
// liveness check would have an orchestrator restart a node over a condition a
// restart does not repair.
func TestNodeHealthChecksSplitLivenessFromReadiness(t *testing.T) {
	t.Parallel()

	checks := nodeHealthChecks(&dingo.Node{})
	var live, ready int
	for _, check := range checks {
		if check.Liveness {
			live++
		} else {
			ready++
		}
	}
	if live != 1 || ready != 2 {
		t.Fatalf(
			"got %d liveness and %d readiness checks, want 1 and 2",
			live,
			ready,
		)
	}

	// A node with no database open is live and caught up but not ready, and
	// says why.
	status := health.Evaluate(
		func() (uint64, bool) { return 0, true }, 1000, checks...,
	)
	if !status.Live || status.Ready {
		t.Fatalf(
			"live=%v ready=%v, want live and not ready",
			status.Live,
			status.Ready,
		)
	}
	if !strings.Contains(status.Reason, "database") {
		t.Fatalf("reason %q does not name the database", status.Reason)
	}
}
