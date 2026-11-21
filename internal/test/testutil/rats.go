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

package testutil

import (
	"math/big"
	"reflect"

	"github.com/blinklabs-io/gouroboros/cbor"
)

// CompleteRats gives every unset rational of a protocol-parameter fixture a
// zero value and returns the fixture. Encoding a parameter set with an unset
// rational panics, and an epoch boundary encodes its post-ENACT parameters.
func CompleteRats[T any](params T) T {
	var fill func(reflect.Value)
	fill = func(v reflect.Value) {
		switch v.Kind() { //nolint:exhaustive // only pointers and structs hold rationals
		case reflect.Pointer:
			if !v.IsNil() {
				fill(v.Elem())
			}
		case reflect.Struct:
			if r, ok := v.Addr().Interface().(*cbor.Rat); ok {
				if r.Rat == nil {
					r.Rat = big.NewRat(0, 1)
				}
				return
			}
			for i := range v.NumField() {
				if v.Field(i).CanSet() {
					fill(v.Field(i))
				}
			}
		}
	}
	fill(reflect.ValueOf(params))
	return params
}
