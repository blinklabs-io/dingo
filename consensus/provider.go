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

// Package consensus defines the node-wide consensus provider contract.
package consensus

import (
	"context"
	"errors"

	"github.com/blinklabs-io/dingo/consensus/praos"
	"github.com/blinklabs-io/dingo/plugin"
	ledgerpkg "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

const (
	// PraosProviderName is the provider name used by existing networks and
	// databases.
	PraosProviderName = "praos"
	// PraosIdentity versions persisted and network-visible Praos semantics.
	PraosIdentity = "praos/v1"
)

// HeaderVerifier exposes the ledger-owned Praos header checks to a provider.
// The provider chooses the algorithm; the ledger retains all state access.
type HeaderVerifier interface {
	ValidatePraosHeader(
		context.Context,
		ledgerpkg.Block,
		bool,
		bool,
	) error
}

// LeaderScheduler is the production schedule selected by a Provider.
type LeaderScheduler interface {
	Start(context.Context) error
	Stop() error
	ShouldProduceBlock(uint64) bool
	NextLeaderSlot(uint64) (uint64, bool)
}

// LeaderSchedulerFactory lets application composition construct the concrete
// scheduler chosen by a provider without making the provider contract depend
// on ledger implementation packages.
type LeaderSchedulerFactory func(scheduleIdentity string) (LeaderScheduler, error)

// Provider makes every consensus decision for one node instance.
type Provider interface {
	Name() string
	NetworkIdentity() string
	DatabaseIdentity() string
	ScheduleIdentity() string
	ValidateHeader(
		context.Context,
		HeaderVerifier,
		ledgerpkg.Block,
		bool,
		bool,
	) error
	CompareTips(
		ochainsync.Tip,
		ochainsync.Tip,
		praos.PraosTiebreakerView,
		praos.PraosTiebreakerView,
	) praos.ChainComparisonResult
	NewLeaderScheduler(LeaderSchedulerFactory) (LeaderScheduler, error)
}

type praosProvider struct {
	_ byte
}

// NewPraosProvider returns the built-in Praos provider.
func NewPraosProvider() Provider { return &praosProvider{} }

func (praosProvider) Name() string             { return PraosProviderName }
func (praosProvider) NetworkIdentity() string  { return PraosIdentity }
func (praosProvider) DatabaseIdentity() string { return PraosIdentity }
func (praosProvider) ScheduleIdentity() string { return PraosIdentity }

func (praosProvider) ValidateHeader(
	ctx context.Context,
	verifier HeaderVerifier,
	block ledgerpkg.Block,
	allowEpochCacheAdvance bool,
	allowStateDefer bool,
) error {
	return verifier.ValidatePraosHeader(
		ctx,
		block,
		allowEpochCacheAdvance,
		allowStateDefer,
	)
}

func (praosProvider) CompareTips(
	tipA, tipB ochainsync.Tip,
	viewA, viewB praos.PraosTiebreakerView,
) praos.ChainComparisonResult {
	return praos.ComparePraosTips(tipA, tipB, viewA, viewB)
}

func (p praosProvider) NewLeaderScheduler(
	factory LeaderSchedulerFactory,
) (LeaderScheduler, error) {
	if factory == nil {
		return nil, errors.New("leader scheduler factory is nil")
	}
	return factory(p.ScheduleIdentity())
}

// RegisterProvider registers the built-in Praos provider on host.
func RegisterProvider(host *plugin.Host) error {
	return plugin.Register[Provider](
		host,
		plugin.Descriptor{
			Capability:  plugin.CapabilityConsensus,
			Name:        PraosProviderName,
			Description: "Ouroboros Praos consensus",
		},
		func() struct{} { return struct{}{} },
		func(context.Context, struct{}, struct{}) (Provider, plugin.Instance, error) {
			return NewPraosProvider(), plugin.Lifecycle{}, nil
		},
	)
}
