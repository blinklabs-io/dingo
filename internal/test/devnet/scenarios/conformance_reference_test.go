//go:build devnet && devnet_conformance

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

package scenarios

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/devnet"
	"github.com/stretchr/testify/require"
)

// TestCardanoProducerChainAdvances verifies the cardano-node producer
// is also forging blocks, serving as a reference baseline. Conformance
// mode only.
func TestCardanoProducerChainAdvances(t *testing.T) {
	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(t, err, "failed to load devnet config from testnet.yaml")

	endpoints := devnet.LoadEndpoints()
	h := devnet.NewTestHarness(
		t, endpoints,
		devnet.WithNetworkMagic(cfg.NetworkMagic),
	)

	cardanoEndpoint, ok := h.ReferenceNode()
	require.True(t, ok, "conformance mode must have a reference node")

	h.WaitForNodeSlot(cardanoEndpoint, 0, 60*time.Second)

	initialTip, err := h.GetChainTip(cardanoEndpoint)
	require.NoError(t, err, "failed to get initial cardano-producer tip")

	const advanceSlots = 10
	targetSlot := initialTip.SlotNumber + advanceSlots
	timeout := time.Duration(advanceSlots)*cfg.SlotDuration() +
		cfg.ExpectedBlockTime()*5
	h.WaitForNodeSlot(cardanoEndpoint, targetSlot, timeout)

	newTip, err := h.GetChainTip(cardanoEndpoint)
	require.NoError(t, err, "failed to get new cardano-producer tip")
	require.Greater(t, newTip.BlockNumber, initialTip.BlockNumber,
		"cardano-producer should have forged new blocks",
	)

	t.Logf(
		"cardano-producer chain advanced from slot %d to %d"+
			" (blocks: %d -> %d)",
		initialTip.SlotNumber, newTip.SlotNumber,
		initialTip.BlockNumber, newTip.BlockNumber,
	)
}
