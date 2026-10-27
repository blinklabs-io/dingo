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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecide(t *testing.T) {
	t.Parallel()
	target := func(n uint64) CertifiedTarget {
		return CertifiedTarget{Immutable: n}
	}
	for _, tc := range []struct {
		name string
		in   DecisionInput
		want bool
	}{
		{
			name: "gap below min",
			in: DecisionInput{
				LocalImmutable: 10, Target: target(13), MinGapChunks: 4,
			},
			want: false,
		},
		{
			name: "local ahead of target",
			in: DecisionInput{
				LocalImmutable: 20, Target: target(13), MinGapChunks: 4,
			},
			want: false,
		},
		{
			name: "unknown rates with a big gap",
			in: DecisionInput{
				LocalImmutable: 10, Target: target(500), MinGapChunks: 4,
			},
			want: true,
		},
		{
			name: "unknown ouroboros rate",
			in: DecisionInput{
				LocalImmutable: 10, Target: target(14), MinGapChunks: 4,
				MithrilBPS: 10,
			},
			want: true,
		},
		{
			name: "unknown mithril rate",
			in: DecisionInput{
				LocalImmutable: 10, Target: target(14), MinGapChunks: 4,
				OuroborosBPS: 1000,
			},
			want: true,
		},
		{
			name: "mithril 1.2x",
			in: DecisionInput{
				LocalImmutable: 10, Target: target(500), MinGapChunks: 4,
				OuroborosBPS: 100, MithrilBPS: 120,
			},
			want: false,
		},
		{
			name: "mithril 1.3x",
			in: DecisionInput{
				LocalImmutable: 10, Target: target(500), MinGapChunks: 4,
				OuroborosBPS: 100, MithrilBPS: 130,
			},
			want: true,
		},
		{
			name: "zero min gap and no gap",
			in: DecisionInput{
				LocalImmutable: 13, Target: target(13),
			},
			want: false,
		},
		{
			name: "zero min gap and a one-file gap",
			in: DecisionInput{
				LocalImmutable: 12, Target: target(13),
			},
			want: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, Decide(tc.in))
		})
	}
}

func TestImmutableFileForSlot(t *testing.T) {
	t.Parallel()
	got, ok := ImmutableFileForSlot(43_199, 21_600)
	assert.True(t, ok)
	assert.Equal(t, uint64(1), got)
	got, ok = ImmutableFileForSlot(43_200, 21_600)
	assert.True(t, ok)
	assert.Equal(t, uint64(2), got)
	_, ok = ImmutableFileForSlot(43_200, 0)
	assert.False(t, ok, "no chunk size gives no estimate")
}
