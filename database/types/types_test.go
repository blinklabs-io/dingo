// Copyright 2025 Blink Labs Software
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

package types_test

import (
	"database/sql"
	"database/sql/driver"
	"math/big"
	"reflect"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
)

func TestTypesScanValue(t *testing.T) {
	testDefs := []struct {
		origValue     any
		expectedValue any
	}{
		{
			origValue: func(v types.Uint64) *types.Uint64 { return &v }(
				types.Uint64(123),
			),
			expectedValue: "123",
		},
		{
			origValue: func(v types.Rat) *types.Rat { return &v }(
				types.Rat{
					Rat: big.NewRat(3, 5),
				},
			),
			expectedValue: "3/5",
		},
	}
	var ok bool
	var tmpScanner sql.Scanner
	var tmpValuer driver.Valuer
	for _, testDef := range testDefs {
		tmpValuer, ok = testDef.origValue.(driver.Valuer)
		if !ok {
			t.Fatalf("test original value does not implement driver.Valuer")
		}
		valueOut, err := tmpValuer.Value()
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if !reflect.DeepEqual(valueOut, testDef.expectedValue) {
			t.Fatalf(
				"did not get expected value from Value(): got %#v, expected %#v",
				valueOut,
				testDef.expectedValue,
			)
		}
		tmpScanner, ok = testDef.origValue.(sql.Scanner)
		if !ok {
			t.Fatalf(
				"test original value does not implement sql.Scanner (it must be a pointer)",
			)
		}
		if err := tmpScanner.Scan(valueOut); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if !reflect.DeepEqual(tmpScanner, testDef.origValue) {
			t.Fatalf(
				"did not get expected value after Scan(): got %#v, expected %#v",
				tmpScanner,
				testDef.origValue,
			)
		}
	}
}
