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

package version

import "testing"

func TestEncodeHeaderVersion(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		version string
		minor   uint64
	}{
		{"v0.123.4", 0x007b0445},
		{"1.2.3", 0x01020345},
		{"v1.2.3-rc.1+build.5", 0x01020345},
		{"63.255.255", 0x3fffff45},
		{"0.0.0", 69},
	} {
		t.Run(tt.version, func(t *testing.T) {
			got, err := encodeHeaderVersion(tt.version)
			if err != nil || got != tt.minor {
				t.Fatalf("got %x, %v; want %x", got, err, tt.minor)
			}
		})
	}
	for _, value := range []string{"", "devel", "1.2", "01.2.3", "64.0.0", "0.256.0", "0.0.256", "1.2.3-01", "1.2.3-rc..1", "1.2.3+", "1.2.3+..", "1.2.3.4", "18446744073709551616.0.0"} {
		if _, err := encodeHeaderVersion(value); err == nil {
			t.Errorf("accepted %q", value)
		}
	}
}

func TestHeaderProtocolMinor(t *testing.T) {
	t.Parallel()
	got, err := HeaderProtocolMinor(false)
	if err != nil || got != 69 {
		t.Fatalf("disabled: got %d, %v", got, err)
	}
	want, wantErr := encodeHeaderVersion(Version)
	got, err = HeaderProtocolMinor(true)
	if (err == nil) != (wantErr == nil) || got != want {
		t.Fatalf("enabled: got %d, %v; want %d, %v", got, err, want, wantErr)
	}
}
