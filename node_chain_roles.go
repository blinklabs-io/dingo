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
	"context"
	"errors"

	"github.com/blinklabs-io/dingo/chainselection"
	sdkcardano "github.com/blinklabs-io/dingo/sdk/cardano"
	sdkchain "github.com/blinklabs-io/dingo/sdk/chain"
	cardanochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type nodeChainConsensusRole struct {
	node *Node
}

var _ sdkchain.Consensus = nodeChainConsensusRole{}

func (r nodeChainConsensusRole) Compare(a, b sdkchain.Tip) sdkchain.Comparison {
	cardanoA := toCardanoTip(a)
	cardanoB := toCardanoTip(b)
	viewA, _ := r.node.selectorLocalTipPraosView(cardanoA)
	viewB, _ := r.node.selectorLocalTipPraosView(cardanoB)
	return r.compareCardano(cardanoA, cardanoB, viewA, viewB)
}

// CompareObservedTips retains the Cardano select views already collected by
// chain sync while the selector itself depends only on the SDK role.
func (r nodeChainConsensusRole) CompareObservedTips(
	a, b sdkchain.Tip,
	viewA, viewB chainselection.PraosTiebreakerView,
) sdkchain.Comparison {
	return r.compareCardano(toCardanoTip(a), toCardanoTip(b), viewA, viewB)
}

func (r nodeChainConsensusRole) compareCardano(
	a, b cardanochainsync.Tip,
	viewA, viewB chainselection.PraosTiebreakerView,
) sdkchain.Comparison {
	return sdkchain.Comparison(r.node.selectedConsensusProvider().CompareTips(
		a,
		b,
		viewA,
		viewB,
	))
}

func toCardanoTip(tip sdkchain.Tip) cardanochainsync.Tip {
	return cardanochainsync.Tip{
		Point: ocommon.Point{
			Slot: tip.Point.Slot,
			Hash: tip.Point.Hash,
		},
		BlockNumber: tip.BlockNumber,
	}
}

func (r nodeChainConsensusRole) ShouldProduceBlock(slot uint64) bool {
	ref := r.node.leaderSchedulerRef.Load()
	return ref != nil && ref.scheduler != nil &&
		ref.scheduler.ShouldProduceBlock(slot)
}

func (r nodeChainConsensusRole) NextLeaderSlot(fromSlot uint64) (uint64, bool) {
	ref := r.node.leaderSchedulerRef.Load()
	if ref == nil || ref.scheduler == nil {
		return 0, false
	}
	return ref.scheduler.NextLeaderSlot(fromSlot)
}

func (r nodeChainConsensusRole) ValidateForgedBlock(
	ctx context.Context,
	block sdkchain.Block,
) error {
	ls := r.node.wallClockLedger.Load()
	if ls == nil {
		return errors.New("ledger unavailable")
	}
	decoded, err := sdkcardano.BlockOf(block)
	if err != nil {
		return err
	}
	raw, err := block.Bytes()
	if err != nil {
		return err
	}
	return ls.ValidateForgedBlock(ctx, decoded, raw)
}

func (n *Node) configureChainLedgerRole() error {
	role, err := sdkcardano.NewLedgerRole(n.ledgerState)
	if err != nil {
		return err
	}
	return n.chainManager.SetLedgerRole(role)
}
