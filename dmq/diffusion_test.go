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

package dmq

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// diffusionLatencyBound is the end-to-end delivery bound for a message
// crossing two hops.
const diffusionLatencyBound = 5 * time.Second

// timeToReach waits until the stack's pool holds wantLen messages and returns
// how long that took.
func timeToReach(
	t *testing.T,
	s *Stack,
	wantLen int,
) time.Duration {
	t.Helper()
	start := time.Now()
	testutil.WaitForCondition(t, func() bool {
		return s.pool.Len() >= wantLen
	}, diffusionLatencyBound, "the pool must fill within the latency bound")
	return time.Since(start)
}

// publishBurst adds n distinct messages to the stack's pool and reports the
// first one the pool does not admit. It is safe to call from any goroutine.
func publishBurst(s *Stack, prefix string, n int) error {
	for i := range n {
		added, err := s.pool.Add(newTestMessage(
			[]byte(fmt.Sprintf("%s-%d", prefix, i)), soonExpiry(),
		))
		if err != nil {
			return fmt.Errorf("adding %s-%d: %w", prefix, i, err)
		}
		if !added {
			return fmt.Errorf("%s-%d was not admitted", prefix, i)
		}
	}
	return nil
}

// Three stacks joined in a line, A to B and B to C, so a message from either
// end reaches the other only by crossing B.
func TestDiffusionAcrossThreeNodesInALine(t *testing.T) {
	t.Parallel()
	a, _ := newTestStack(t, StackConfig{})
	b, _ := newListeningStack(t, PeerConfig{})
	c, _ := newListeningStack(t, PeerConfig{})

	dialStack(t, a, b)
	dialStack(t, b, c)
	waitFor(t, "the line is connected", func() bool {
		return b.PeerCount() == 2 && c.PeerCount() == 1
	})

	addMessages(t, a, "from-a", 1)
	took := timeToReach(t, c, 1)
	t.Logf("A to C across B: %s", took)
	assert.Less(t, took, diffusionLatencyBound)

	addMessages(t, c, "from-c", 1)
	took = timeToReach(t, a, 2)
	t.Logf("C to A across B: %s", took)
	assert.Less(t, took, diffusionLatencyBound)

	for name, s := range map[string]*Stack{"a": a, "b": b, "c": c} {
		assert.Equal(t, 2, s.pool.Len(), "node %s", name)
	}
}

// A burst of messages from every node reaches every other node within the
// latency bound.
func TestDiffusionOfABurstAcrossThreeNodes(t *testing.T) {
	t.Parallel()
	const perNode = 50
	a, _ := newTestStack(t, StackConfig{})
	b, _ := newListeningStack(t, PeerConfig{})
	c, _ := newListeningStack(t, PeerConfig{})
	dialStack(t, a, b)
	dialStack(t, b, c)
	dialStack(t, a, c)
	waitFor(t, "the triangle is connected", func() bool {
		return b.PeerCount() == 2 && c.PeerCount() == 2
	})

	// The publishers run off the test goroutine, so they return their
	// errors instead of failing the test themselves.
	errs := make(chan error, 3)
	var wg sync.WaitGroup
	for name, s := range map[string]*Stack{"a": a, "b": b, "c": c} {
		wg.Go(func() { errs <- publishBurst(s, "burst-"+name, perNode) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	for name, s := range map[string]*Stack{"a": a, "b": b, "c": c} {
		took := timeToReach(t, s, 3*perNode)
		t.Logf("node %s complete after %s", name, took)
	}
	for name, s := range map[string]*Stack{"a": a, "b": b, "c": c} {
		assert.Equal(t, 3*perNode, s.pool.Len(), "node %s", name)
	}
}

// Traffic from signers each publishing once a minute under a 30 minute TTL
// plateaus at one TTL's worth of messages, stays inside the capacity even at
// the largest body size, and empties once the TTL passes. The signer count and
// capacity are one tenth of mainnet's 1,550 signers and 128 MiB, which keeps
// the same capacity-to-traffic ratio and the same plateau shape.
func TestMempoolStaysBoundedAtMainnetSignerScale(t *testing.T) {
	t.Parallel()
	const (
		signers  = 155
		ttl      = 30 * time.Minute
		capacity = int64(128) << 20 / 10
		minBody  = 90
		maxBody  = 2000
	)
	var mu sync.Mutex
	now := time.Unix(2_000_000_000, 0)
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		now = now.Add(d)
	}
	pool := NewMessageMempool(Config{Capacity: capacity, Now: clock})

	publish := func(body int, round int) (admitted, full int) {
		for signer := range signers {
			prefix := fmt.Sprintf("s%04d-r%04d-", signer, round)
			payload := prefix + strings.Repeat("x", body-len(prefix))
			// #nosec G115 -- fixed test timestamp
			expires := uint32(clock().Add(ttl).Unix())
			ok, err := pool.Add(newTestMessage([]byte(payload), expires))
			switch {
			case err == nil && ok:
				admitted++
			case err != nil && assert.ErrorIs(t, err, ErrFull):
				full++
			}
			require.LessOrEqual(t, pool.SizeBytes(), capacity)
		}
		return admitted, full
	}

	// Every signer publishes once a minute for two TTL windows at the
	// smallest body, expiring as it goes: the pool plateaus at one TTL's
	// worth of traffic rather than growing with time.
	var peak int
	for round := range 40 {
		admitted, full := publish(minBody, round)
		require.Equal(t, signers, admitted)
		require.Zero(t, full)
		advance(time.Minute)
		pool.removeExpired()
		peak = max(peak, pool.Len())
	}
	t.Logf("one message a minute at %d bytes: peak %d messages, %d bytes",
		minBody, peak, pool.SizeBytes())
	assert.Equal(t, signers*int(ttl/time.Minute), peak)

	advance(ttl + time.Minute)
	pool.removeExpired()
	require.Zero(t, pool.Len())
	require.Zero(t, pool.SizeBytes())

	// At the largest body the plateau is about 9.3 MB, which fits in the
	// 12.8 MiB capacity with room to spare, so nothing is refused.
	var refused int
	for round := range 30 {
		_, full := publish(maxBody, round)
		refused += full
		advance(time.Minute)
		pool.removeExpired()
	}
	t.Logf("one message a minute at %d bytes: %d refused, %d bytes held",
		maxBody, refused, pool.SizeBytes())
	assert.Zero(t, refused)
	assert.LessOrEqual(t, pool.SizeBytes(), capacity)

	advance(ttl + time.Minute)
	pool.removeExpired()
	assert.Zero(t, pool.Len())
	assert.Zero(t, pool.SizeBytes())
}
