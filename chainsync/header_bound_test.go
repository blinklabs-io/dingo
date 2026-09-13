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

package chainsync

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestHeaderAlternativeBound(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	_, events := bus.Subscribe(ForkDetectedEventType)
	cfg := DefaultConfig()
	cfg.PromRegistry = prometheus.NewRegistry()
	s := NewStateWithConfig(bus, nil, cfg)
	conn := ouroboros.ConnectionId{}
	point := func(n uint64) ocommon.Point {
		hash := make([]byte, 32)
		binary.BigEndian.PutUint64(hash, n)
		return ocommon.NewPoint(100, hash)
	}
	const bound = 32
	// Consume each initial event so subscriber capacity cannot hide overflows.
	for n := range uint64(bound) {
		require.True(t, s.RecordHeader(conn, point(n)))
		require.False(t, s.RecordHeader(conn, point(n)))
		if n > 0 {
			testutil.RequireReceive(t, events, time.Second, "fork event below capacity")
		}
	}
	require.Equal(t, bound, len(s.seenHeaders[100]))
	for n := uint64(bound); n < 1024; n++ {
		require.True(t, s.RecordHeader(conn, point(n)), "capacity must not suppress an unseen eligible header")
		require.False(t, s.RecordHeader(conn, point(n)), "latest overflow header remains deduplicated")
	}
	require.Equal(t, bound, len(s.seenHeaders[100]), "same-slot header retention must remain bounded")
	testutil.RequireNoReceive(t, events, 50*time.Millisecond, "saturated slot must not emit more fork events")
	// Alternating overflow identities may miss the bounded cache, but cannot
	// grow retention or reset the fork-event budget.
	for n := range 100 {
		s.RecordHeader(conn, point(2000+uint64(n%2)))
	}
	require.Equal(t, bound, len(s.seenHeaders[100]))
	testutil.RequireNoReceive(t, events, 50*time.Millisecond, "overflow replacement must not replenish event budget")
	require.False(t, s.RecordHeader(conn, point(0)), "first observation remains deduplicated")
	for _, clear := range []struct {
		name string
		fn   func()
	}{
		{"recovery", func() { s.ClearSeenHeadersFrom(99) }},
		{"pruning", func() { s.PruneSeenHeaders(101) }},
		{"reset", s.ClearSeenHeaders},
	} {
		t.Run(clear.name, func(t *testing.T) {
			clear.fn()
			require.Empty(t, s.seenHeaders)
			require.True(t, s.RecordHeader(conn, point(0)))
			require.True(t, s.RecordHeader(conn, point(1)))
			testutil.RequireReceive(t, events, time.Second, "cleared slot regains fork detection")
		})
	}
}
