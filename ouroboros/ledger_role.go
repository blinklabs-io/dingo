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

package ouroboros

import (
	"context"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
	gouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// ProtocolLedger provides the Cardano-specific ledger services that Ouroboros
// needs in addition to the neutral SDK ledger role. Composition supplies both
// interfaces over the same ledger implementation.
type ProtocolLedger interface {
	AcquireQueryView(
		context.Context,
		ledger.QueryPoint,
	) (*ledger.QueryView, error)
	AdvanceLeiosPins(context.Context) error
	AwaitAppliedSlot(context.Context, uint64) error
	AwaitChainsyncHeaderAdmission(
		context.Context,
		ledger.ChainsyncEvent,
	) (bool, error)
	AwaitChainsyncHeaderCapacity(context.Context, int) error
	AwaitObserverHeaderHorizon(context.Context, ledger.ChainsyncEvent) error
	BlockByHash(context.Context, []byte) (models.Block, error)
	BlockfetchRangeExpectedBytes(ocommon.Point, ocommon.Point) uint64
	Chain() *chain.Chain
	Database() *database.Database
	EndorserBlockWaitDuration() time.Duration
	EpochBoundaryJobPending() bool
	GetBlock(ocommon.Point) (models.Block, error)
	GetChainFromPoint(ocommon.Point, bool) (*chain.ChainIterator, error)
	GetCurrentPParams() lcommon.ProtocolParameters
	GetIntersectPoint(context.Context, []ocommon.Point) (*ocommon.Point, error)
	ImmutablePoint(context.Context) (ocommon.Point, bool, error)
	IntersectPoints(context.Context, int) ([]ocommon.Point, error)
	IsAtTip() bool
	LedgerTipIntersectPoints(context.Context, int) ([]ocommon.Point, error)
	MaxTxSize(uint) (uint, bool)
	NetworkIngestHeld() bool
	PinAcquiredPoint(uint64) func()
	QueryWithProtocolVersion(
		context.Context,
		any,
		ledger.QueryPoint,
		uint16,
	) (any, error)
	RecoverAfterLocalRollback(
		context.Context,
		[]gouroboros.ConnectionId,
		ocommon.Point,
	) ledger.LocalRollbackRecoveryResult
	RollbackWindowIntersectAnchor(context.Context) (ocommon.Point, bool, error)
	ShouldVerifyChainSelectionHeaderCrypto(ocommon.Point) bool
	SlotToTime(uint64) (time.Time, error)
	SlotsBehindHead() uint64
	ValidateHeaderCheckpoint(gledger.BlockHeader) error
	ValidatePeerChainSelectionHeaderCryptoWithGeneration(
		gouroboros.ConnectionId,
		gledger.BlockHeader,
	) (uint64, error)
}

func (o *Ouroboros) ledgerTip() ochainsync.Tip {
	tip := o.chainLedger.Tip()
	return ochainsync.Tip{
		Point: ocommon.Point{
			Slot: tip.Point.Slot,
			Hash: tip.Point.Hash,
		},
		BlockNumber: tip.BlockNumber,
	}
}

func (o *Ouroboros) ledgerSecurityParam() int {
	return o.chainLedger.SecurityParam()
}
