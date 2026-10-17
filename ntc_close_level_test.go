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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/gouroboros/muxer"
	"github.com/stretchr/testify/require"
)

// A local client closing its connection is routine and logged at debug; any
// other reason a node-to-client connection ended stays a warning.
func TestNtCCloseLogLevel(t *testing.T) {
	peerClosed := fmt.Errorf("wrapped: %w", &muxer.ConnectionClosedError{
		Context: "reading header",
		Err:     io.EOF,
	})
	require.Equal(t, slog.LevelDebug, ntcCloseLogLevel(peerClosed))
	require.Equal(
		t,
		slog.LevelWarn,
		ntcCloseLogLevel(errors.New("muxer: protocol violation")),
	)
}
