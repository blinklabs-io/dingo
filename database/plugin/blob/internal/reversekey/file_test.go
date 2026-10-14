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

package reversekey

import (
	"encoding/binary"
	"os"
	"slices"
	"testing"
)

func TestReverseKeyFile(t *testing.T) {
	t.Parallel()
	file, err := os.CreateTemp(t.TempDir(), "reverse-")
	if err != nil {
		t.Fatal(err)
	}
	f := New(file)
	t.Cleanup(f.Close)
	for _, key := range []string{"a", "b", "c"} {
		if err := Write(file, key); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		f.Reset()
		for _, want := range []string{"c", "b", "a"} {
			got, valid, err := f.NextReverse()
			if err != nil || !valid || got != want {
				t.Fatalf("reverse key = %q, %v, %v", got, valid, err)
			}
		}
		if _, valid, err := f.NextReverse(); err != nil || valid {
			t.Fatalf(
				"reverse iterator should be exhausted: valid=%v err=%v",
				valid,
				err,
			)
		}
	}
	f.Close()
	if _, err := os.Stat(file.Name()); !os.IsNotExist(err) {
		t.Fatalf("closed spool remains on disk: %v", err)
	}
	f.Close()
}

func TestReverseKeyFileRejectsMalformedRecords(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"short header", []byte{0, 0, 0}},
		{"missing prefix", []byte{0, 0, 0, 4}},
		{"truncated payload", []byte{0, 0, 0, 2, 'a', 0, 0, 0, 2}},
		{"mismatched lengths", []byte{0, 0, 0, 2, 'a', 0, 0, 0, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "reverse-")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			if _, err := file.Write(tc.data); err != nil {
				t.Fatal(err)
			}
			f := New(file)
			key, valid, err := f.NextReverse()
			if err == nil || valid || key != "" {
				t.Fatalf(
					"malformed spool yielded key %q, valid=%v, err=%v",
					key,
					valid,
					err,
				)
			}
		})
	}
}

func TestReverseKeyFileEmptyAndBinaryKeys(t *testing.T) {
	t.Parallel()
	file, err := os.CreateTemp(t.TempDir(), "reverse-")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	keys := []string{
		"",
		string(binary.BigEndian.AppendUint32(nil, 0xffffffff)),
		"last",
	}
	for _, key := range keys {
		if err := Write(file, key); err != nil {
			t.Fatal(err)
		}
	}
	f := New(file)
	for _, want := range slices.Backward(keys) {
		key, valid, err := f.NextReverse()
		if err != nil || !valid || key != want {
			t.Fatalf(
				"reverse key = %q, valid=%v, err=%v; want %q",
				key,
				valid,
				err,
				want,
			)
		}
	}
	if _, valid, err := f.NextReverse(); err != nil || valid {
		t.Fatalf("exhausted iterator: valid=%v err=%v", valid, err)
	}
}
