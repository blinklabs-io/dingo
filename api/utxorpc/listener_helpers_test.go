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

package utxorpc

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

func startOnFreePort(
	t *testing.T,
	ctx context.Context,
	tlsCfg apiconfig.EffectiveTLS,
	opts ...func(*UtxorpcConfig),
) (*Utxorpc, string) {
	t.Helper()
	var lastErr error
	for range testutil.BindAttempts {
		addr := testutil.FreePort(t)
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			t.Fatal(err)
		}
		portNum, err := strconv.ParseUint(port, 10, 16)
		if err != nil {
			t.Fatal(err)
		}
		bus := event.NewEventBus(nil, nil)
		t.Cleanup(bus.Close)
		cfg := UtxorpcConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: bus,
			Host:     host,
			Port:     uint(portNum),
			TLS:      tlsCfg,
		}
		for _, opt := range opts {
			opt(&cfg)
		}
		u := NewUtxorpc(cfg)
		attemptCtx, cancel := context.WithCancel(ctx)
		lastErr = u.Start(attemptCtx)
		if lastErr == nil {
			t.Cleanup(cancel)
			return u, addr
		}
		cancel()
	}
	t.Fatalf("could not start on a free loopback port: %v", lastErr)
	return nil, ""
}

func stopUtxorpc(t *testing.T, u *Utxorpc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := u.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}
