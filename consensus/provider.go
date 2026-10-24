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
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/blinklabs-io/dingo/consensus/praos"
	"github.com/blinklabs-io/dingo/plugin"
	gconsensus "github.com/blinklabs-io/gouroboros/consensus"
	ledgerpkg "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

const (
	// PraosProviderName is the provider name used by existing networks and
	// databases.
	PraosProviderName = "praos"
	// PraosIdentity versions persisted and network-visible Praos semantics.
	PraosIdentity = "praos/v1"
	// TachysProviderName is the built-in CIP-0177 provider.
	TachysProviderName = "tachys"
	// TachysIdentity binds network, database, and schedule state to the
	// supported CIP-0177 semantics.
	TachysIdentity = "tachys/cip177/" + gconsensus.TachysSpecificationRevision
	// DatabaseIdentitySyncKey is the sync-state key that binds a database to
	// the DatabaseIdentity of the provider that created it. It must outlive
	// any bulk sync-state cleanup: a database that loses it is read as an
	// unmarked Praos database on the next start.
	DatabaseIdentitySyncKey = "consensus_provider_identity"
)

const networkMagicDomain = "dingo/consensus-network/v1"

// NetworkMagic derives the node-to-node network identity for a consensus
// provider while preserving Cardano's magic for Praos interoperability.
func NetworkMagic(base uint32, identity string) uint32 {
	if identity == "" || identity == PraosIdentity {
		return base
	}
	input := make([]byte, len(networkMagicDomain)+4+len(identity))
	copy(input, networkMagicDomain)
	binary.BigEndian.PutUint32(input[len(networkMagicDomain):], base)
	copy(input[len(networkMagicDomain)+4:], identity)
	digest := sha256.Sum256(input)
	for offset := 0; offset < len(digest); offset += 4 {
		candidate := binary.BigEndian.Uint32(digest[offset : offset+4])
		if candidate != 0 && candidate != base {
			return candidate
		}
	}
	candidate := base + 1
	if candidate == 0 {
		candidate = 1
	}
	return candidate
}

// HeaderVerifier exposes the ledger-owned Praos header checks to a provider.
// The provider chooses the algorithm; the ledger retains all state access.
type HeaderVerifier interface {
	ValidatePraosHeader(
		context.Context,
		ledgerpkg.Block,
		bool,
		bool,
	) error
	ValidateTachysHeader(
		context.Context,
		ledgerpkg.Block,
		bool,
		bool,
	) error
}

// LeaderScheduleKind identifies the algorithm used to construct a production
// schedule. Providers pass it through composition so one selected provider
// governs both incoming authorization and local forging.
type LeaderScheduleKind uint8

const (
	LeaderSchedulePraos LeaderScheduleKind = iota
	LeaderScheduleTachys
)

// LeaderScheduler is the production schedule selected by a Provider.
type LeaderScheduler interface {
	Start(context.Context) error
	Stop() error
	ConsensusStateRestored()
	ShouldProduceBlock(uint64) bool
	NextLeaderSlot(uint64) (uint64, bool)
}

// LeaderSchedulerFactory lets application composition construct the concrete
// scheduler chosen by a provider without making the provider contract depend
// on ledger implementation packages.
type LeaderSchedulerFactory func(
	scheduleIdentity string,
	kind LeaderScheduleKind,
) (LeaderScheduler, error)

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

type tachysProvider struct {
	_ byte
}

// NewPraosProvider returns the built-in Praos provider.
func NewPraosProvider() Provider { return &praosProvider{} }

// NewTachysProvider returns the built-in CIP-0177 provider.
func NewTachysProvider() Provider { return &tachysProvider{} }

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
	return factory(p.ScheduleIdentity(), LeaderSchedulePraos)
}

func (tachysProvider) Name() string             { return TachysProviderName }
func (tachysProvider) NetworkIdentity() string  { return TachysIdentity }
func (tachysProvider) DatabaseIdentity() string { return TachysIdentity }
func (tachysProvider) ScheduleIdentity() string { return TachysIdentity }

func (tachysProvider) ValidateHeader(
	ctx context.Context,
	verifier HeaderVerifier,
	block ledgerpkg.Block,
	allowEpochCacheAdvance bool,
	allowStateDefer bool,
) error {
	return verifier.ValidateTachysHeader(
		ctx,
		block,
		allowEpochCacheAdvance,
		allowStateDefer,
	)
}

func (tachysProvider) CompareTips(
	tipA, tipB ochainsync.Tip,
	viewA, viewB praos.PraosTiebreakerView,
) praos.ChainComparisonResult {
	return praos.ComparePraosTips(tipA, tipB, viewA, viewB)
}

func (p tachysProvider) NewLeaderScheduler(
	factory LeaderSchedulerFactory,
) (LeaderScheduler, error) {
	if factory == nil {
		return nil, errors.New("leader scheduler factory is nil")
	}
	return factory(p.ScheduleIdentity(), LeaderScheduleTachys)
}

// RegisterProvider registers the built-in Praos and Tachys providers on host.
func RegisterProvider(host *plugin.Host) error {
	if err := plugin.Register[Provider](
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
	); err != nil {
		return err
	}
	return plugin.Register[Provider](
		host,
		plugin.Descriptor{
			Capability:  plugin.CapabilityConsensus,
			Name:        TachysProviderName,
			Description: "CIP-0177 Tachys consensus",
		},
		func() struct{} { return struct{}{} },
		func(context.Context, struct{}, struct{}) (Provider, plugin.Instance, error) {
			return NewTachysProvider(), plugin.Lifecycle{}, nil
		},
	)
}
