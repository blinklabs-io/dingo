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

package ouroboros_test

import (
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/ouroboros"
	"github.com/blinklabs-io/dingo/peergov"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
)

// mockNodeInterface is a simple mock implementation of NodeInterface for testing.
type mockNodeInterface struct{}

func (m *mockNodeInterface) ConnManager() *connmanager.ConnectionManager {
	return &connmanager.ConnectionManager{}
}

func (m *mockNodeInterface) LedgerState() *ledger.LedgerState { return &ledger.LedgerState{} }

func (m *mockNodeInterface) Mempool() *mempool.Mempool { return &mempool.Mempool{} }

func (m *mockNodeInterface) Config() ouroboros.ConfigInterface { return &mockConfig{} }

func (m *mockNodeInterface) ChainsyncState() *chainsync.State { return &chainsync.State{} }

func (m *mockNodeInterface) PeerGov() *peergov.PeerGovernor { return &peergov.PeerGovernor{} }

func (m *mockNodeInterface) EventBus() *event.EventBus { return &event.EventBus{} }

// mockConfig implements ConfigInterface for testing.
type mockConfig struct{}

func (m *mockConfig) IntersectTip() bool               { return false }
func (m *mockConfig) IntersectPoints() []ocommon.Point { return nil }
func (m *mockConfig) Logger() *slog.Logger             { return nil }

func TestNewOuroboros(t *testing.T) {
	mockNode := &mockNodeInterface{}
	config := ouroboros.OuroborosConfig{
		NetworkMagic: 764824073,
		Node:         mockNode,
	}

	o, err := ouroboros.NewOuroboros(config)

	assert.NoError(t, err)
	assert.NotNil(t, o)
}

func TestNewOuroboros_ErrorOnNilNode(t *testing.T) {
	config := ouroboros.OuroborosConfig{
		NetworkMagic: 764824073,
		Node:         nil,
	}

	_, err := ouroboros.NewOuroboros(config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "OuroborosConfig.Node must not be nil")
}
