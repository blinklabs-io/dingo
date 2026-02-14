// Copyright 2025 Blink Labs Software
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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/event"
)

func TestUtxorpc_StartStop(t *testing.T) {
	// Start server on ephemeral port by setting Port to 0
	u := NewUtxorpc(UtxorpcConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: event.NewEventBus(nil, nil),
		Host:     "127.0.0.1",
		Port:     0,
	})
	err := u.Start(context.Background())
	require.NoError(t, err, "failed to start utxorpc")

	// Server is already listening after Start returns successfully
	// (startServer waits internally for startup or error)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = u.Stop(ctx)
	require.NoError(t, err, "failed to stop utxorpc")
}
