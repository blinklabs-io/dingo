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

package txresponse

import (
	"slices"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

func TestCloneOwnsPayloadAndPreservesEmptyItems(t *testing.T) {
	t.Parallel()
	require.Nil(t, Clone(nil))
	require.NotNil(t, Clone([]cbor.RawMessage{}))
	in := []cbor.RawMessage{nil, {}, {1}, {2}}
	out := Clone(in)
	require.Equal(t, in, out)
	appended := append(out[2], 99)
	require.Equal(t, cbor.RawMessage{1, 99}, appended)
	require.Equal(t, cbor.RawMessage{2}, out[3])
	out[3][0] = 77
	require.Equal(t, cbor.RawMessage{2}, in[3])
	out[2] = nil
	require.Equal(t, cbor.RawMessage{1}, in[2])
}

var benchmarkResponse []cbor.RawMessage

func BenchmarkCloneTransactionResponse(b *testing.B) {
	in := make([]cbor.RawMessage, 1302)
	for i := range in {
		in[i] = make([]byte, 350)
	}
	for _, name := range []string{"individual", "packed"} {
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(in) * 350))
			b.ReportAllocs()
			for b.Loop() {
				if name == "packed" {
					benchmarkResponse = Clone(in)
				} else {
					out := make([]cbor.RawMessage, len(in))
					for i, tx := range in {
						out[i] = slices.Clone(tx)
					}
					benchmarkResponse = out
				}
			}
		})
	}
}
