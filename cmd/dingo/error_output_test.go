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
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Remote values reach errors through "%s" verbs; the final error is logged
// with slog.Error(err.Error()), so the handler must not emit raw control
// bytes for either log format.
func TestFinalErrorEscapesRemoteControlBytes(t *testing.T) {
	t.Parallel()
	err := fmt.Errorf(
		"unsupported signed entity type: %s",
		"x\x1b[2J\x1b]0;pwned\x07\r\u009b",
	)
	for _, format := range []string{"text", "json"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			logger, _, _ := newLogger(&buf, format, "info", false)
			logExecuteError(logger, err)
			out := buf.String()
			for _, b := range []byte(out[:len(out)-1]) {
				assert.False(
					t, b < 0x20 || b == 0x7f,
					"raw control byte %#x in %s output %q", b, format, out,
				)
			}
			assert.NotContains(t, out, "\u009b")
		})
	}
}
