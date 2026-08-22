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
	"encoding/binary"
	"encoding/json"
	"strings"
	"testing"
)

// frameBytes builds a length-prefixed frame around an arbitrary payload,
// bypassing writeFrame so a test can produce payloads writeFrame would refuse.
func frameBytes(payload []byte) []byte {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	return append(hdr[:], payload...)
}

func TestReadFrameRejectsMalformedJSON(t *testing.T) {
	r := bytes.NewReader(frameBytes([]byte("{\"type\":")))
	var kp KeyPush
	err := readFrame(r, &kp)
	if err == nil {
		t.Fatal("expected an error for a truncated JSON payload")
	}
	if !strings.Contains(err.Error(), "unmarshal frame") {
		t.Fatalf("expected an unmarshal error, got %v", err)
	}
}

func TestReadFrameRejectsNonObjectPayload(t *testing.T) {
	// Valid JSON, wrong shape. Silently accepting it would leave a zero-valued
	// KeyPush that later checks have to catch instead.
	r := bytes.NewReader(frameBytes([]byte(`["key_push"]`)))
	var kp KeyPush
	if err := readFrame(r, &kp); err == nil {
		t.Fatal("expected an error for a JSON array payload")
	}
}

func TestReadFrameRejectsOversizedLengthHeader(t *testing.T) {
	// A length header past the cap must be refused from the header alone: the
	// allocation it asks for is the attack, so it must not be made before the
	// body is read.
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], maxFrameLen+1)
	var kp KeyPush
	err := readFrame(bytes.NewReader(hdr[:]), &kp)
	if err == nil {
		t.Fatal("expected an error for an oversized length header")
	}
	if !strings.Contains(err.Error(), "frame too large") {
		t.Fatalf("expected a frame-too-large error, got %v", err)
	}
}

func TestReadFrameRejectsZeroLengthFrame(t *testing.T) {
	var kp KeyPush
	err := readFrame(bytes.NewReader(frameBytes([]byte{})), &kp)
	if err == nil {
		t.Fatal("expected an error for a zero-length frame")
	}
	if !strings.Contains(err.Error(), "zero-length frame") {
		t.Fatalf("expected a zero-length-frame error, got %v", err)
	}
}

func TestReadFrameTruncatedPayloadDoesNotDecode(t *testing.T) {
	// A header promising more bytes than follow must fail rather than decode a
	// short buffer.
	frame := frameBytes([]byte(`{"type":"key_push","period":7}`))
	var kp KeyPush
	err := readFrame(bytes.NewReader(frame[:len(frame)-5]), &kp)
	if err == nil {
		t.Fatal("expected an error for a truncated payload")
	}
	if kp.Period != 0 {
		t.Fatalf("a truncated frame populated the value: period %d", kp.Period)
	}
}

// TestReadFrameWipesPayload pins that the frame buffer is zeroed before
// readFrame returns.
//
// A serve-key frame carries the base64-encoded KES signing key, and a push
// recurs on every evolve and every reconnect, so an unwiped buffer leaves a
// trivially decodable copy of the pool's signing key in freed heap memory on
// every one. The buffer is local to readFrame and unreachable afterwards, so
// the only way to observe the wipe is to hand readFrame a buffer the test keeps
// a reference to.
func TestReadFrameWipesPayload(t *testing.T) {
	secret := bytes.Repeat([]byte{0x5a}, secretKeySize(6))
	payload, err := json.Marshal(KeyPush{
		Type:       KeyPushType,
		Period:     4,
		Depth:      6,
		KESSignKey: secret,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var retained []byte
	original := framePayloadBuffer
	framePayloadBuffer = func(n uint32) []byte {
		retained = make([]byte, n)
		return retained
	}
	t.Cleanup(func() { framePayloadBuffer = original })

	var kp KeyPush
	if err := readFrame(bytes.NewReader(frameBytes(payload)), &kp); err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	// The decoded value is unaffected by the wipe: json.Unmarshal base64-decodes
	// into its own allocation.
	if !bytes.Equal(kp.KESSignKey, secret) {
		t.Fatal("the wipe corrupted the decoded signing key")
	}
	if retained == nil {
		t.Fatal("the payload allocator was not used")
	}
	for i, b := range retained {
		if b != 0 {
			t.Fatalf(
				"frame payload was not wiped: byte %d is %#x; the frame holds the base64 KES signing key",
				i,
				b,
			)
		}
	}
}

// TestReadFrameWipesPayloadOnDecodeFailure covers the rejected push. A frame
// refused for any reason still arrived carrying key material.
func TestReadFrameWipesPayloadOnDecodeFailure(t *testing.T) {
	var retained []byte
	original := framePayloadBuffer
	framePayloadBuffer = func(n uint32) []byte {
		retained = make([]byte, n)
		return retained
	}
	t.Cleanup(func() { framePayloadBuffer = original })

	var kp KeyPush
	if err := readFrame(
		bytes.NewReader(frameBytes([]byte(`{"kes_sign_key":`))),
		&kp,
	); err == nil {
		t.Fatal("expected a decode error")
	}
	if retained == nil {
		t.Fatal("the payload allocator was not used")
	}
	if !bytes.Equal(retained, make([]byte, len(retained))) {
		t.Fatalf("payload was not wiped after a decode failure: %q", retained)
	}
}

func TestWriteFrameRoundTrips(t *testing.T) {
	var buf bytes.Buffer
	want := SignResponse{
		Type:      SignResponseType,
		Period:    9,
		Signature: []byte{0x01, 0x02, 0x03},
	}
	if err := writeFrame(&buf, want); err != nil {
		t.Fatalf("writeFrame: %v", err)
	}
	var got SignResponse
	if err := readFrame(&buf, &got); err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if got.Type != want.Type || got.Period != want.Period ||
		!bytes.Equal(got.Signature, want.Signature) {
		t.Fatalf("round trip mismatch: got %+v want %+v", got, want)
	}
}
