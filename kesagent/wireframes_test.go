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

package kesagent

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// TestWireGoldenFrames decodes the frames bursa publishes for its KES agent
// protocol (kesagent/wire/testdata/frames, vendored into
// testdata/wireframes) with this package's own framing, checks each against
// its decoded JSON, and requires re-encoding to give the same bytes. A
// change on either side of the protocol fails here.
func TestWireGoldenFrames(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		maxSize int
		fresh   func() any
	}{
		{"hello_sign", MaxHelloFrameLen, func() any { return new(Hello) }},
		{"hello_serve_key", MaxHelloFrameLen, func() any { return new(Hello) }},
		{
			"hello_attested_sign",
			MaxHelloFrameLen,
			func() any { return new(Hello) },
		},
		{
			"attestation_request",
			MaxSignFrameLen,
			func() any { return new(attestationRequest) },
		},
		{
			"attestation_response_none",
			maxAttestationFrameLen,
			func() any { return new(attestationResponse) },
		},
		{
			"attestation_response_evidence",
			maxAttestationFrameLen,
			func() any { return new(attestationResponse) },
		},
		{
			"attestation_response_error",
			maxAttestationFrameLen,
			func() any { return new(attestationResponse) },
		},
		{"sign_request", MaxSignFrameLen, func() any { return new(SignRequest) }},
		{
			"sign_request_profile",
			MaxSignFrameLen,
			func() any { return new(SignRequest) },
		},
		{
			"sign_response",
			MaxSignFrameLen,
			func() any { return new(SignResponse) },
		},
		{
			"sign_response_error",
			MaxSignFrameLen,
			func() any { return new(SignResponse) },
		},
		{"key_push", MaxKeyPushFrameLen, func() any { return new(KeyPush) }},
	}
	dir := filepath.Join("testdata", "wireframes")
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw, err := os.ReadFile(filepath.Join(dir, tc.name+".hex"))
			if err != nil {
				t.Fatal(err)
			}
			frame, err := hex.DecodeString(strings.TrimSpace(string(raw)))
			if err != nil {
				t.Fatalf("fixture is not hex: %v", err)
			}
			jsonFixture, err := os.ReadFile(filepath.Join(dir, tc.name+".json"))
			if err != nil {
				t.Fatal(err)
			}
			want := tc.fresh()
			if err := json.Unmarshal(jsonFixture, want); err != nil {
				t.Fatalf("decode json fixture: %v", err)
			}

			in := bytes.NewReader(frame)
			got := tc.fresh()
			if err := readFrame(in, tc.maxSize, got); err != nil {
				t.Fatalf("readFrame: %v", err)
			}
			if in.Len() != 0 {
				t.Fatalf("%d bytes left after the frame", in.Len())
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("decoded %+v, want %+v", got, want)
			}

			var out bytes.Buffer
			if err := writeFrame(
				&out,
				tc.maxSize,
				reflect.ValueOf(got).Elem().Interface(),
			); err != nil {
				t.Fatalf("writeFrame: %v", err)
			}
			if !bytes.Equal(out.Bytes(), frame) {
				t.Fatalf(
					"re-encoded frame differs\n got %x\nwant %x",
					out.Bytes(),
					frame,
				)
			}
		})
	}
}
