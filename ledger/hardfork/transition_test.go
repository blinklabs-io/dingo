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

package hardfork_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/stretchr/testify/assert"
)

func TestTransitionInfo_ZeroIsUnknown(t *testing.T) {
	var ti hardfork.TransitionInfo
	assert.Equal(t, hardfork.TransitionUnknown, ti.State)
	assert.Equal(t, uint64(0), ti.KnownEpoch)
}

func TestTransitionState_String(t *testing.T) {
	tests := []struct {
		state hardfork.TransitionState
		want  string
	}{
		{hardfork.TransitionUnknown, "TransitionUnknown"},
		{hardfork.TransitionKnown, "TransitionKnown"},
		{hardfork.TransitionImpossible, "TransitionImpossible"},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.want, tc.state.String())
	}
}

func TestTransitionInfo_Constructors(t *testing.T) {
	u := hardfork.NewTransitionUnknown()
	assert.Equal(t, hardfork.TransitionUnknown, u.State)

	k := hardfork.NewTransitionKnown(501)
	assert.Equal(t, hardfork.TransitionKnown, k.State)
	assert.Equal(t, uint64(501), k.KnownEpoch)

	i := hardfork.NewTransitionImpossible()
	assert.Equal(t, hardfork.TransitionImpossible, i.State)
}
