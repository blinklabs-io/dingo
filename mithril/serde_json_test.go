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

package mithril

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSerdeJSONCopiesNestedNullInAny(t *testing.T) {
	t.Parallel()

	for _, in := range []string{
		`{"a":null}`,
		`[1,null]`,
		`[null]`,
		`{"a":[null,{"b":null}],"c":true}`,
	} {
		out, err := serdeJSON([]byte(in), &serdeField{kind: serdeAny})
		require.NoError(t, err, in)
		require.Equal(t, in, string(out))
	}
}

// The output feeds decoders whose bound is the input size, so the copy
// must not grow beyond a small constant factor of it: HTML escaping would
// turn each '<' into six bytes.
func TestSerdeJSONOutputBoundedByInput(t *testing.T) {
	t.Parallel()

	for _, s := range []string{
		strings.Repeat("<>&", 100),
		strings.Repeat(" ", 100),
		strings.Repeat("\xff", 100),
	} {
		in := []byte(`{"a":"` + s + `"}`)
		out, err := serdeJSON(in, &serdeField{kind: serdeAny})
		require.NoError(t, err)
		require.LessOrEqual(t, len(out), 3*len(in), "%q", s)
		require.True(t, json.Valid(out))
		var got, want map[string]string
		require.NoError(t, json.Unmarshal(out, &got))
		require.NoError(t, json.Unmarshal(in, &want))
		require.Equal(t, want, got)
	}
}
