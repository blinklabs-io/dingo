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

package cardano

import (
	"context"
	"errors"

	"github.com/blinklabs-io/dingo/sdk/chain"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

// ErrNilSource is returned by a role constructor given a nil source or
// comparer.
var ErrNilSource = errors.New("cardano: role source is nil")

// LedgerSource is what the Cardano ledger role reads from the node's ledger
// state. It is the security parameter the chain manager takes from the ledger,
// the applied tip, and the transaction validator the mempool and forger call.
type LedgerSource interface {
	SecurityParam() int
	Tip() ochainsync.Tip
	ValidateTx(tx lcommon.Transaction) error
}

// LeaderSource is the forger's leader check, which the node's leader schedule
// provides.
type LeaderSource interface {
	ShouldProduceBlock(slot uint64) bool
	NextLeaderSlot(fromSlot uint64) (uint64, bool)
}

// ForgedBlockValidator is the forger's check of a block it has built, run
// before the block is adopted. The node's ledger state provides it.
type ForgedBlockValidator interface {
	ValidateForgedBlock(
		ctx context.Context,
		block lcommon.Block,
		blockCbor []byte,
	) error
}

// TipComparer compares two tips the way chain selection does. A tip carries
// only its point and block number, so a comparer that breaks equal-length ties
// as chain selection does, by operational certificate or VRF output, looks up
// that data itself.
type TipComparer func(a, b ochainsync.Tip) chain.Comparison

// NetworkSink is the forger's block broadcaster: it adds a block to the local
// chain and propagates it.
type NetworkSink interface {
	AddBlock(ctx context.Context, block lcommon.Block, cbor []byte) error
}

// MempoolSource is the part of the node's mempool the Cardano mempool role
// calls. The transaction type is the Cardano era the pool takes with the
// transaction bytes.
type MempoolSource interface {
	AddTransaction(ctx context.Context, txType uint, txBytes []byte) error
	RemoveTransaction(txHash string)
}

func toChainTip(t ochainsync.Tip) chain.Tip {
	return chain.Tip{
		Point:       chain.Point{Slot: t.Point.Slot, Hash: t.Point.Hash},
		BlockNumber: t.BlockNumber,
	}
}

func toCardanoTip(t chain.Tip) ochainsync.Tip {
	tip := ochainsync.Tip{BlockNumber: t.BlockNumber}
	tip.Point.Slot = t.Point.Slot
	tip.Point.Hash = t.Point.Hash
	return tip
}

// LedgerRole implements chain.Ledger for Cardano.
type LedgerRole struct {
	src LedgerSource
}

var _ chain.Ledger = (*LedgerRole)(nil)

// NewLedgerRole returns the Cardano ledger role over src.
func NewLedgerRole(src LedgerSource) (*LedgerRole, error) {
	if src == nil {
		return nil, ErrNilSource
	}
	return &LedgerRole{src: src}, nil
}

// SecurityParam implements chain.Ledger.
func (r *LedgerRole) SecurityParam() int { return r.src.SecurityParam() }

// Tip implements chain.Ledger.
func (r *LedgerRole) Tip() chain.Tip { return toChainTip(r.src.Tip()) }

// ValidateTx implements chain.Ledger. A transaction carrying its decoded value
// is validated without being decoded again.
func (r *LedgerRole) ValidateTx(tx chain.Tx) error {
	decoded, err := TxOf(tx)
	if err != nil {
		return err
	}
	return r.src.ValidateTx(decoded)
}

// ConsensusRole implements chain.Consensus for Cardano.
type ConsensusRole struct {
	leader    LeaderSource
	validator ForgedBlockValidator
	compare   TipComparer
}

var _ chain.Consensus = (*ConsensusRole)(nil)

// NewConsensusRole returns the Cardano consensus role over the node's leader
// schedule, its forged-block validator and a tip comparer. Every argument is
// required: a role that could not order tips would never prefer another chain.
func NewConsensusRole(
	leader LeaderSource,
	validator ForgedBlockValidator,
	compare TipComparer,
) (*ConsensusRole, error) {
	if leader == nil || validator == nil || compare == nil {
		return nil, ErrNilSource
	}
	return &ConsensusRole{
		leader:    leader,
		validator: validator,
		compare:   compare,
	}, nil
}

// Compare implements chain.Consensus.
func (r *ConsensusRole) Compare(a, b chain.Tip) chain.Comparison {
	return r.compare(toCardanoTip(a), toCardanoTip(b))
}

// ShouldProduceBlock implements chain.Consensus.
func (r *ConsensusRole) ShouldProduceBlock(slot uint64) bool {
	return r.leader.ShouldProduceBlock(slot)
}

// NextLeaderSlot implements chain.Consensus.
func (r *ConsensusRole) NextLeaderSlot(fromSlot uint64) (uint64, bool) {
	return r.leader.NextLeaderSlot(fromSlot)
}

// ValidateForgedBlock implements chain.Consensus.
func (r *ConsensusRole) ValidateForgedBlock(
	ctx context.Context,
	block chain.Block,
) error {
	decoded, err := BlockOf(block)
	if err != nil {
		return err
	}
	return r.validator.ValidateForgedBlock(ctx, decoded, decoded.Cbor())
}

// NetworkRole implements chain.Network for Cardano.
type NetworkRole struct {
	sink NetworkSink
}

var _ chain.Network = (*NetworkRole)(nil)

// NewNetworkRole returns the Cardano network role over sink.
func NewNetworkRole(sink NetworkSink) (*NetworkRole, error) {
	if sink == nil {
		return nil, ErrNilSource
	}
	return &NetworkRole{sink: sink}, nil
}

// Broadcast implements chain.Network.
func (r *NetworkRole) Broadcast(ctx context.Context, block chain.Block) error {
	decoded, err := BlockOf(block)
	if err != nil {
		return err
	}
	return r.sink.AddBlock(ctx, decoded, decoded.Cbor())
}

// MempoolRole implements chain.Mempool for Cardano.
type MempoolRole struct {
	src MempoolSource
}

var _ chain.Mempool = (*MempoolRole)(nil)

// NewMempoolRole returns the Cardano mempool role over src.
func NewMempoolRole(src MempoolSource) (*MempoolRole, error) {
	if src == nil {
		return nil, ErrNilSource
	}
	return &MempoolRole{src: src}, nil
}

// AddTx implements chain.Mempool. The pool takes the transaction's bytes, so a
// payload that already carries them is passed on untouched.
func (r *MempoolRole) AddTx(ctx context.Context, tx chain.Tx) error {
	raw, err := tx.Bytes()
	if err != nil {
		return err
	}
	return r.src.AddTransaction(ctx, tx.View().Type, raw)
}

// RemoveTx implements chain.Mempool.
func (r *MempoolRole) RemoveTx(
	txHash string,
) {
	r.src.RemoveTransaction(txHash)
}
