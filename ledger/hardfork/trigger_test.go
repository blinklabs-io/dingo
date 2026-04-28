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

	"github.com/stretchr/testify/assert"

	"github.com/blinklabs-io/dingo/ledger/hardfork"
)

func TestTriggerHardFork_Constructors(t *testing.T) {
	v := hardfork.NewTriggerAtVersion(7)
	assert.Equal(t, hardfork.TriggerAtVersion, v.Kind)
	assert.Equal(t, uint(7), v.Version)
	assert.Equal(t, uint64(0), v.Epoch)

	e := hardfork.NewTriggerAtEpoch(500)
	assert.Equal(t, hardfork.TriggerAtEpoch, e.Kind)
	assert.Equal(t, uint64(500), e.Epoch)
	assert.Equal(t, uint(0), e.Version)

	n := hardfork.NewTriggerNotDuringThisExecution()
	assert.Equal(t, hardfork.TriggerNotDuringThisExecution, n.Kind)
	assert.Equal(t, uint(0), n.Version)
	assert.Equal(t, uint64(0), n.Epoch)
}

func TestTriggerHardFork_String(t *testing.T) {
	cases := []struct {
		in   hardfork.TriggerHardFork
		want string
	}{
		{hardfork.NewTriggerAtVersion(4), "AtVersion(4)"},
		{hardfork.NewTriggerAtEpoch(123), "AtEpoch(123)"},
		{hardfork.NewTriggerNotDuringThisExecution(), "NotDuringThisExecution"},
		{hardfork.TriggerHardFork{Kind: 99}, "TriggerHardFork(?kind=99)"},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, tc.in.String())
	}
}
