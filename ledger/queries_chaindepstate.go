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

package ledger

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

// chainDepStateVersionPraos is the `encodeVersion` tag the Haskell consensus
// layer writes for the Praos serialisation, used from Babbage onwards. TPraos
// (Shelley through Alonzo) uses 1 and a different layout; dingo serves the
// Praos form because the eras it supports all run Praos.
const chainDepStateVersionPraos = 0

// praosChainDepState mirrors ouroboros-consensus' PraosState record. Field
// order is load-bearing: the Haskell decoder reads a fixed 8-element array, so
// a reordering still produces valid CBOR that deserialises into the wrong
// fields.
type praosChainDepState struct {
	cbor.StructAsArray
	LastSlot            olocalstatequery.WithOriginSlot
	OpCertCounters      map[lcommon.Blake2b224]uint64
	EvolvingNonce       lcommon.Nonce
	CandidateNonce      lcommon.Nonce
	EpochNonce          lcommon.Nonce
	PreviousEpochNonce  lcommon.Nonce
	LabNonce            lcommon.Nonce
	LastEpochBlockNonce lcommon.Nonce
}

// versionedChainDepState is the `encodeVersion N` envelope both protocol
// serialisations share: a 2-element array of version and payload.
type versionedChainDepState struct {
	cbor.StructAsArray
	Version uint64
	Inner   praosChainDepState
}

// nonceFromBytes converts a stored nonce into its wire form. An absent or
// empty value is the neutral nonce, which is how the ledger represents "no
// nonce yet" — notably at genesis and before the first epoch boundary.
func nonceFromBytes(b []byte) lcommon.Nonce {
	if len(b) != lcommon.Blake2b256Size {
		return lcommon.Nonce{Type: lcommon.NonceTypeNeutral}
	}
	nonce := lcommon.Nonce{Type: lcommon.NonceTypeNonce}
	copy(nonce.Value[:], b)
	return nonce
}

// queryShelleyDebugChainDepState answers GetChainDepState, the consensus
// chain-dependent state at the acquired point.
//
// cardano-cli reads the epoch nonce from here when computing a leadership
// schedule, so leaving it unhandled does not merely fail one query: an
// unsupported query aborts the LocalStateQuery protocol, the node drops the
// connection, and the caller sees only a closed bearer.
func (ls *LedgerState) queryShelleyDebugChainDepState() (any, error) {
	// Every value in the reply is read from this one transaction, tip
	// included. The in-memory tip and epoch snapshots would be the cheaper
	// source, but they are published after the database write that advances
	// the chain, so pairing a slot or epoch number from them with nonces read
	// here can straddle an epoch boundary. The worst of that is silent: an
	// epoch the snapshot has reached but this transaction has not yet seen has
	// no record to read, and the reply would carry a populated last slot
	// beside a neutral epoch nonce -- from which cardano-cli computes a
	// leadership schedule that is wrong rather than absent.
	txn := ls.db.Transaction(false)
	defer txn.Release()

	tip, err := ls.db.GetTip(txn)
	if err != nil {
		return nil, err
	}
	lastSlot := olocalstatequery.WithOriginSlot{}
	if len(tip.Point.Hash) > 0 {
		lastSlot.HasSlot = true
		lastSlot.Slot = tip.Point.Slot
	}

	counters, err := ls.chainDepStateOpCertCounters(txn)
	if err != nil {
		return nil, err
	}

	state := praosChainDepState{
		LastSlot:       lastSlot,
		OpCertCounters: counters,
	}

	// The epoch containing the tip, so the nonces belong to the same slot the
	// reply reports. Resolving it from the slot rather than from an epoch
	// number read elsewhere is what makes the pairing exact.
	//
	// A tip with no epoch record covering it would leave every nonce neutral,
	// but the chain cannot reach that state: applying a block in an epoch
	// requires that epoch's nonce to check the producer's leader VRF, and the
	// nonce lives in the record. The record therefore exists before the tip
	// can enter the epoch. What remains is the chain that has applied no
	// blocks at all, where neutral is the right answer.
	current, err := ls.db.GetEpochBySlot(tip.Point.Slot, txn)
	if err != nil {
		return nil, err
	}
	var carriedLabNonce []byte
	if current != nil {
		carriedLabNonce = current.LastEpochBlockNonce
		state.EvolvingNonce = nonceFromBytes(current.EvolvingNonce)
		state.CandidateNonce = nonceFromBytes(current.CandidateNonce)
		state.EpochNonce = nonceFromBytes(current.Nonce)
		// The lab carried into this epoch: the parent hash of the last block
		// of the previous one.
		state.LastEpochBlockNonce = nonceFromBytes(
			current.LastEpochBlockNonce,
		)
		// Epoch 0 has no predecessor; its previous-epoch nonce stays neutral.
		if current.EpochId > 0 {
			previous, err := ls.db.GetEpoch(current.EpochId-1, txn)
			if err != nil {
				return nil, err
			}
			if previous != nil {
				state.PreviousEpochNonce = nonceFromBytes(previous.Nonce)
			}
		}
	}

	state.LabNonce, err = ls.chainDepStateLabNonce(txn, tip, carriedLabNonce)
	if err != nil {
		return nil, err
	}

	return []any{
		versionedChainDepState{
			Version: chainDepStateVersionPraos,
			Inner:   state,
		},
	}, nil
}

// chainDepStateLabNonce derives the nonce of the last block applied.
//
// Unlike every other field of the record, the lab moves with each block rather
// than only at an epoch boundary. Praos sets it to prevHashToNonce of the
// applied block's parent hash -- the PARENT, not the block's own hash, a
// deliberate one-block lag that keeps the final block of an epoch out of the
// nonce it seeds. See epochLabNonce, which computes the same value at a
// boundary, and #2734 for what a shift by one costs.
//
// Serving the epoch's carried last-epoch-block nonce instead is right only
// until the epoch's first block lands, since that carried value is what the
// lag had reached when the epoch opened. After that it is a stale answer for a
// field the chain has already moved on from.
func (ls *LedgerState) chainDepStateLabNonce(
	txn *database.Txn,
	tip ochainsync.Tip,
	carriedLabNonce []byte,
) (lcommon.Nonce, error) {
	if len(tip.Point.Hash) == 0 {
		// No block applied: consensus has never moved the lab off the value
		// the epoch opened with.
		return nonceFromBytes(carriedLabNonce), nil
	}
	block, err := database.BlockByHashTxn(txn, tip.Point.Hash)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			// The tip names a block this transaction cannot read. Reporting
			// the carried value is the same answer as before the epoch's
			// first block, which is wrong by at most one block; failing would
			// abort the protocol and take the whole query with it.
			return nonceFromBytes(carriedLabNonce), nil
		}
		return lcommon.Nonce{}, err
	}
	prevHash, err := blockPrevHash(block)
	if err != nil {
		return lcommon.Nonce{}, fmt.Errorf(
			"derive tip parent hash at slot %d: %w",
			block.Slot,
			err,
		)
	}
	if len(prevHash) != lcommon.Blake2b256Size {
		return nonceFromBytes(carriedLabNonce), nil
	}
	// prevHashToNonce maps GenesisHash to the neutral nonce rather than to the
	// genesis hash bytes, so the chain's first block leaves the lab as it was.
	if genesisHash, gErr := GenesisBlockHash(
		ls.config.CardanoNodeConfig,
	); gErr == nil && bytes.Equal(prevHash, genesisHash[:]) {
		return nonceFromBytes(carriedLabNonce), nil
	}
	return nonceFromBytes(prevHash), nil
}

// chainDepStateOpCertCounters collects the highest operational-certificate
// issue number the chain has accepted for each block issuer.
//
// The set is every pool that has issued a block, taken from the issuer record
// itself rather than from the currently registered pools. Those two differ in
// both directions, and each difference matters: a registered pool that has
// never minted has no accepted number to report, and a pool that minted and
// has since left the active set still has one the chain enforces against any
// block claiming its cold key.
func (ls *LedgerState) chainDepStateOpCertCounters(txn *database.Txn) (
	map[lcommon.Blake2b224]uint64,
	error,
) {
	sequences, err := ls.db.LatestPoolOpCertSequences(txn)
	if err != nil {
		return nil, err
	}
	// Non-nil even when empty: the client-side decoder normalises a missing
	// map, but emitting CBOR null here would differ from the node's output.
	counters := make(map[lcommon.Blake2b224]uint64, len(sequences))
	for keyHash, sequence := range sequences {
		if len(keyHash) != lcommon.Blake2b224Size {
			continue
		}
		counters[lcommon.NewBlake2b224([]byte(keyHash))] = sequence
	}
	return counters, nil
}
