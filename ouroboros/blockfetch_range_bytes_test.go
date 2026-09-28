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

package ouroboros

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"

	ouroboros "github.com/blinklabs-io/gouroboros"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
)

// capturingRangeRequester records the RangeRequest it is handed.
type capturingRangeRequester struct {
	mu   sync.Mutex
	reqs []blockfetch.RangeRequest
}

func (f *capturingRangeRequester) RequestRange(
	_ context.Context,
	req blockfetch.RangeRequest,
) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs = append(f.reqs, req)
	return uint64(len(f.reqs)), nil
}

func TestBlockfetchClientRequestRangeSendsExpectedBytes(t *testing.T) {
	t.Parallel()

	start := ocommon.NewPoint(1, make([]byte, lcommon.Blake2b256Size))
	end := ocommon.NewPoint(2, make([]byte, lcommon.Blake2b256Size))

	for _, tc := range []struct {
		name string
		want uint64
	}{
		{name: "estimate", want: 123456},
		{name: "no estimate", want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fake := &capturingRangeRequester{}
			o := newOuroboros(OuroborosConfig{
				Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			})
			o.blockfetchConnClient = func(
				ouroboros.ConnectionId,
			) (blockfetchRangeRequester, error) {
				return fake, nil
			}
			var gotStart, gotEnd ocommon.Point
			o.blockfetchRangeBytes = func(s, e ocommon.Point) uint64 {
				gotStart, gotEnd = s, e
				return tc.want
			}

			_, err := o.BlockfetchClientRequestRange(testConnId(), start, end)
			require.NoError(t, err)

			require.Len(t, fake.reqs, 1)
			require.Equal(t, tc.want, fake.reqs[0].ExpectedBytes)
			require.Equal(t, start, gotStart)
			require.Equal(t, end, gotEnd)
		})
	}
}

// TestBlockfetchClientWiringKeepsRangesPipelinedUnderRealEstimates drives
// dingo's real client wiring with range requests sized at the ledger's
// per-range cap, the size real estimates produce on a heavy region. With no
// configured budget gouroboros' 100 x 88 KiB default admits only one such
// range at a time; the configured budget must keep several in flight.
func TestBlockfetchClientWiringKeepsRangesPipelinedUnderRealEstimates(
	t *testing.T,
) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	peer := newBlockfetchPeerWithOpts(t, o.blockfetchClientConnOpts()...)

	start := ocommon.NewPoint(1, make([]byte, lcommon.Blake2b256Size))
	end := ocommon.NewPoint(2, make([]byte, lcommon.Blake2b256Size))
	request := func() chan error {
		done := make(chan error, 1)
		go func() {
			_, err := peer.client.RequestRange(
				context.Background(),
				blockfetch.RangeRequest{
					Start:         start,
					End:           end,
					ExpectedBytes: ledger.BlockfetchMaxRangeBytes,
				},
			)
			done <- err
		}()
		return done
	}

	const pipelined = 3
	for i := range pipelined {
		select {
		case err := <-request():
			require.NoError(t, err, "request %d", i)
		case <-time.After(5 * time.Second):
			t.Fatalf(
				"range %d of %d blocked on the in-flight byte budget",
				i+1,
				pipelined,
			)
		}
	}

	// The budget is a bound, not absent: nothing answers on the wire here,
	// so one more range must wait for capacity.
	testutil.RequireNoReceive(
		t,
		request(),
		200*time.Millisecond,
		"a range beyond the in-flight budget was admitted",
	)
}
