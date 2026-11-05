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

package dingo

import (
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The selector's ConnectionEligible hook must follow chainsync ingress
// admission: a peer demoted to observability-only has its headers dropped, so
// it has to leave the Limit on Eagerness candidates, and a promoted peer has
// to return.
func TestChainSelectorConnectionEligibleFollowsIngressAdmission(t *testing.T) {
	t.Parallel()
	n := &Node{}
	n.publishChainsyncState(chainsync.NewState(nil, nil))
	hook := n.buildChainSelectorConfig(2, true, 0).ConnectionEligible
	require.NotNil(t, hook,
		"the node must wire the selector's ConnectionEligible hook")

	conn := newNodeTestConnId(9201)
	assert.True(t, hook(conn), "an untracked connection stays eligible")

	require.True(t, n.chainsyncState.AddClientConnId(conn))
	assert.True(t, hook(conn), "an ingress client is eligible")

	require.True(t, n.chainsyncState.SetClientObservabilityOnly(conn, true))
	assert.False(t, hook(conn),
		"a demoted observer must not be a limit-on-eagerness candidate")

	require.True(t, n.chainsyncState.SetClientObservabilityOnly(conn, false))
	assert.True(t, hook(conn), "a re-promoted peer is eligible again")

	observed := newNodeTestConnId(9202)
	require.True(t, n.chainsyncState.TryAddObservedClientConnId(observed))
	assert.False(t, hook(observed),
		"a connection admitted as an observer is not eligible")
}

func TestChainsyncIngressAdmittedWithoutChainsyncState(t *testing.T) {
	t.Parallel()
	n := &Node{}
	assert.True(t, n.chainsyncIngressAdmitted(newNodeTestConnId(9203)))
}

// The selector outlives a live Restore, which republishes the chainsync state
// while the hook is still being called under the selector lock.
func TestChainsyncIngressAdmittedDuringStateRepublish(t *testing.T) {
	t.Parallel()
	n := &Node{}
	n.publishChainsyncState(chainsync.NewState(nil, nil))
	conn := newNodeTestConnId(9204)
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 200 {
			n.publishChainsyncState(nil)
			n.publishChainsyncState(chainsync.NewState(nil, nil))
		}
	})
	for range 200 {
		assert.True(t, n.chainsyncIngressAdmitted(conn))
	}
	wg.Wait()
}
