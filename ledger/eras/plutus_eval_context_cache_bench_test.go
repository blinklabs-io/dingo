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

package eras

import (
	"testing"

	"github.com/blinklabs-io/plutigo/cek"
	"github.com/blinklabs-io/plutigo/lang"
)

// BenchmarkPlutusEvalContextPerBlock measures EvalContext construction for
// one block's worth of same-version PlutusV3 redeemers. The cached case
// starts every block from an empty cache, so it pays one build per block and
// never benefits from reuse across blocks.
func BenchmarkPlutusEvalContextPerBlock(b *testing.B) {
	const redeemersPerBlock = 16
	costModel := defaultMachineCostModel(b, lang.LanguageVersionV3)
	protoVersion := cek.ProtoVersion{Major: 10}

	b.Run("uncached", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			for range redeemersPerBlock {
				if _, err := cek.NewEvalContext(
					lang.LanguageVersionV3,
					protoVersion,
					costModel,
				); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("cached", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			cache := NewPlutusEvalContextCache()
			for range redeemersPerBlock {
				if _, err := cache.get(
					lang.LanguageVersionV3,
					protoVersion,
					costModel,
					false,
				); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}
