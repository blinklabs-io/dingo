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

package connmanager

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestEnableTCPLingerZeroOnNonTCPIsNoop verifies that calling the
// helper on a non-TCP connection (e.g. unix-socket NtC) is a no-op
// and never returns an error. The listener wires the helper before
// the unix-socket wrapping, so this guard prevents NtC paths from
// erroring on accept. net.Pipe gives us a non-TCP net.Conn pair
// without a filesystem path — important on macOS where t.TempDir
// paths exceed the unix-socket sun_path length limit.
func TestEnableTCPLingerZeroOnNonTCPIsNoop(t *testing.T) {
	t.Parallel()

	srv, cli := net.Pipe()
	defer srv.Close()
	defer cli.Close()

	assert.NoError(t, enableTCPLingerZero(srv))
	assert.NoError(t, enableTCPLingerZero(cli))
}
