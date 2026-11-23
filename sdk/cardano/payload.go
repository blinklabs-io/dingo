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

// Package cardano is the Cardano implementation of the chain role contracts in
// sdk/chain. It is the only part of the sdk that exposes gouroboros types.
//
// Payloads built here carry the decoded gouroboros value, so two built-in
// roles exchange it without encoding. A payload that arrives as bytes only,
// from an external role, is decoded when a role needs the value.
//
// The roles do not depend on the node. Each wraps small source interfaces
// whose methods are the ones the node's ledger state, leader schedule, forger
// and mempool already have, so the node's own types satisfy them as they are.
package cardano

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/sdk/chain"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// ChainID is the identifier of the Cardano chain.
const ChainID chain.ChainID = "cardano"

// ErrNotBlock, ErrNotHeader and ErrNotTx wrap a decode failure of a payload
// that carried no native value.
var (
	ErrNotBlock  = errors.New("cardano: payload is not a decodable block")
	ErrNotHeader = errors.New("cardano: payload is not a decodable header")
	ErrNotTx     = errors.New("cardano: payload is not a decodable transaction")
)

func hashBytes(h lcommon.Blake2b256) []byte {
	return h.Bytes()
}

// encoder returns v's retained encoding, and otherwise encodes v. A value
// assembled rather than decoded, such as a freshly built block, retains none.
func encoder(v interface{ Cbor() []byte }) func() ([]byte, error) {
	return func() ([]byte, error) {
		if raw := v.Cbor(); len(raw) > 0 {
			return raw, nil
		}
		return cbor.Encode(v)
	}
}

func headerView(h lcommon.BlockHeader, typ uint) chain.HeaderView {
	return chain.HeaderView{
		Type:     typ,
		Hash:     hashBytes(h.Hash()),
		PrevHash: hashBytes(h.PrevHash()),
		Slot:     h.SlotNumber(),
		Number:   h.BlockNumber(),
	}
}

// NewHeader wraps a decoded header of block type typ. The payload holds the
// header itself and encodes only when its bytes are requested. A block, which
// also satisfies lcommon.BlockHeader, is reduced to its header.
func NewHeader(typ uint, h lcommon.BlockHeader) chain.Header {
	if b, ok := h.(lcommon.Block); ok {
		h = b.Header()
	}
	return chain.NewNativePayload(headerView(h, typ), h, encoder(h))
}

// HeaderOf returns the decoded header of p: the native value when p carries
// one, and otherwise the header decoded from p's bytes.
func HeaderOf(p chain.Header) (lcommon.BlockHeader, error) {
	if h, ok := chain.NativeOf[lcommon.BlockHeader](p); ok {
		return h, nil
	}
	raw, err := p.Bytes()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNotHeader, err)
	}
	h, err := gledger.NewBlockHeaderFromCbor(p.View().Type, raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNotHeader, err)
	}
	return h, nil
}

// NewBlock wraps a decoded block.
func NewBlock(b lcommon.Block) chain.Block {
	return chain.NewNativePayload(
		chain.BlockView{
			HeaderView: headerView(b, uint(b.Type())),
			TxCount:    len(b.Transactions()),
		},
		b,
		encoder(b),
	)
}

// BlockOf returns the decoded block of p: the native value when p carries one,
// and otherwise the block decoded from p's bytes.
func BlockOf(p chain.Block) (lcommon.Block, error) {
	if b, ok := chain.NativeOf[lcommon.Block](p); ok {
		return b, nil
	}
	raw, err := p.Bytes()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNotBlock, err)
	}
	b, err := gledger.NewBlockFromCbor(p.View().Type, raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNotBlock, err)
	}
	return b, nil
}

// NewTx wraps a decoded transaction.
func NewTx(tx lcommon.Transaction) chain.Tx {
	return chain.NewNativePayload(
		chain.TxView{
			Type: uint(tx.Type()),
			Hash: hashBytes(tx.Hash()),
		},
		tx,
		encoder(tx),
	)
}

// TxOf returns the decoded transaction of p: the native value when p carries
// one, and otherwise the transaction decoded from p's bytes.
func TxOf(p chain.Tx) (lcommon.Transaction, error) {
	if tx, ok := chain.NativeOf[lcommon.Transaction](p); ok {
		return tx, nil
	}
	raw, err := p.Bytes()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNotTx, err)
	}
	tx, err := gledger.NewTransactionFromCbor(p.View().Type, raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNotTx, err)
	}
	return tx, nil
}
