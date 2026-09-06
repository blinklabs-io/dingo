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

package main

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newKoiosURLFlagCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "test"}
	addKoiosURLFlag(cmd)
	return cmd
}

// TestKoiosAllowInsecureHTTPFlagBeatsEnv pins CLAUDE.md's CLI > env rule on the
// security-relevant switch. Reading only the flag's *value* would let
// KOIOS_ALLOW_INSECURE_HTTP=true defeat an explicit
// --koios-allow-insecure-http=false, silently keeping the plain-HTTP escape
// hatch open against the operator's stated intent.
func TestKoiosAllowInsecureHTTPFlagBeatsEnv(t *testing.T) {
	t.Setenv("KOIOS_ALLOW_INSECURE_HTTP", "true")

	cmd := newKoiosURLFlagCmd()
	require.NoError(t, cmd.ParseFlags([]string{"--koios-allow-insecure-http=false"}))
	assert.False(t, koiosAllowInsecureHTTP(cmd),
		"an explicit false must win over the environment")

	cmd = newKoiosURLFlagCmd()
	require.NoError(t, cmd.ParseFlags([]string{"--koios-allow-insecure-http=true"}))
	assert.True(t, koiosAllowInsecureHTTP(cmd))

	// Unset flag falls back to the environment.
	cmd = newKoiosURLFlagCmd()
	require.NoError(t, cmd.ParseFlags(nil))
	assert.True(t, koiosAllowInsecureHTTP(cmd))

	t.Setenv("KOIOS_ALLOW_INSECURE_HTTP", "false")
	cmd = newKoiosURLFlagCmd()
	require.NoError(t, cmd.ParseFlags(nil))
	assert.False(t, koiosAllowInsecureHTTP(cmd))
}

// TestKoiosBaseURLFlagBeatsEnv pins the same rule for the host itself,
// including an explicit empty value meaning "use the public host".
func TestKoiosBaseURLFlagBeatsEnv(t *testing.T) {
	t.Setenv("KOIOS_URL", "https://env.example/api/v1")

	cmd := newKoiosURLFlagCmd()
	require.NoError(t, cmd.ParseFlags([]string{"--koios-url=https://flag.example/api/v1"}))
	assert.Equal(t, "https://flag.example/api/v1", koiosBaseURL(cmd))

	cmd = newKoiosURLFlagCmd()
	require.NoError(t, cmd.ParseFlags([]string{"--koios-url="}))
	assert.Empty(t, koiosBaseURL(cmd),
		"an explicit empty flag selects the public host over the environment")

	cmd = newKoiosURLFlagCmd()
	require.NoError(t, cmd.ParseFlags(nil))
	assert.Equal(t, "https://env.example/api/v1", koiosBaseURL(cmd))
}
