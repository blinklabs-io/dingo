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

package consensus

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/plugin"
	ledgerpkg "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/require"
)

type recordingHeaderVerifier struct {
	called     bool
	advance    bool
	deferState bool
}

func (v *recordingHeaderVerifier) ValidatePraosHeader(
	_ context.Context,
	_ ledgerpkg.Block,
	advance bool,
	deferState bool,
) error {
	v.called = true
	v.advance = advance
	v.deferState = deferState
	return nil
}

func (v *recordingHeaderVerifier) ValidateTachysHeader(
	_ context.Context,
	_ ledgerpkg.Block,
	advance bool,
	deferState bool,
) error {
	v.called = true
	v.advance = advance
	v.deferState = deferState
	return nil
}

type testScheduler struct{}

func (*testScheduler) Start(context.Context) error          { return nil }
func (*testScheduler) Stop() error                          { return nil }
func (*testScheduler) ShouldProduceBlock(uint64) bool       { return false }
func (*testScheduler) NextLeaderSlot(uint64) (uint64, bool) { return 0, false }
func (*testScheduler) ConsensusStateRestored()              {}

func TestPraosProviderRoutesConsensusDecisions(t *testing.T) {
	t.Parallel()

	provider := NewPraosProvider()
	verifier := &recordingHeaderVerifier{}
	require.NoError(t, provider.ValidateHeader(
		context.Background(), verifier, nil, true, false,
	))
	require.True(t, verifier.called)
	require.True(t, verifier.advance)
	require.False(t, verifier.deferState)

	var gotIdentity string
	scheduler, err := provider.NewLeaderScheduler(
		func(identity string, kind LeaderScheduleKind) (LeaderScheduler, error) {
			gotIdentity = identity
			require.Equal(t, LeaderSchedulePraos, kind)
			return &testScheduler{}, nil
		},
	)
	require.NoError(t, err)
	require.NotNil(t, scheduler)
	require.Equal(t, PraosIdentity, gotIdentity)
}

func TestTachysProviderRoutesConsensusDecisions(t *testing.T) {
	t.Parallel()

	provider := NewTachysProvider()
	verifier := &recordingHeaderVerifier{}
	require.NoError(t, provider.ValidateHeader(
		context.Background(), verifier, nil, false, true,
	))
	require.True(t, verifier.called)
	require.False(t, verifier.advance)
	require.True(t, verifier.deferState)
	require.Equal(t, TachysIdentity, provider.NetworkIdentity())
	require.Equal(t, TachysIdentity, provider.DatabaseIdentity())

	var gotIdentity string
	var gotKind LeaderScheduleKind
	scheduler, err := provider.NewLeaderScheduler(
		func(identity string, kind LeaderScheduleKind) (LeaderScheduler, error) {
			gotIdentity = identity
			gotKind = kind
			return &testScheduler{}, nil
		},
	)
	require.NoError(t, err)
	require.NotNil(t, scheduler)
	require.Equal(t, TachysIdentity, gotIdentity)
	require.Equal(t, LeaderScheduleTachys, gotKind)
}

func TestPraosPluginInstancesAreNodeOwned(t *testing.T) {
	t.Parallel()

	host := plugin.NewHost()
	require.NoError(t, RegisterProvider(host))
	require.Error(t, host.ValidateSelection(
		plugin.CapabilityConsensus,
		PraosProviderName,
		map[string]any{"unknown": true},
	))

	first, err := plugin.Resolve[Provider](
		context.Background(), host, plugin.CapabilityConsensus,
		PraosProviderName, nil, struct{}{},
	)
	require.NoError(t, err)
	second, err := plugin.Resolve[Provider](
		context.Background(), host, plugin.CapabilityConsensus,
		PraosProviderName, nil, struct{}{},
	)
	require.NoError(t, err)
	require.NotSame(t, first, second)
	require.NoError(t, host.Stop(context.Background()))
}

func TestTachysPluginInstancesAreNodeOwned(t *testing.T) {
	t.Parallel()

	host := plugin.NewHost()
	require.NoError(t, RegisterProvider(host))
	provider, err := plugin.Resolve[Provider](
		context.Background(), host, plugin.CapabilityConsensus,
		TachysProviderName, nil, struct{}{},
	)
	require.NoError(t, err)
	require.Equal(t, TachysIdentity, provider.DatabaseIdentity())
	require.NoError(t, host.Stop(context.Background()))
}
