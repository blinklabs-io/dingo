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
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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
	// Every read below belongs to one view of the chain. Taken separately,
	// each opens its own transaction, so an epoch boundary landing part-way
	// through would pair one epoch's nonces with another's, and the opcert
	// counters with neither.
	txn := ls.db.Transaction(false)
	defer txn.Release()

	tip := ls.loadTipSnapshot().currentTip
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

	epochID := ls.loadConsensusSnapshot().currentEpoch.EpochId
	current, err := ls.db.GetEpoch(epochID, txn)
	if err != nil {
		return nil, err
	}
	if current != nil {
		state.EvolvingNonce = nonceFromBytes(current.EvolvingNonce)
		state.CandidateNonce = nonceFromBytes(current.CandidateNonce)
		state.EpochNonce = nonceFromBytes(current.Nonce)
		// The ledger records the lab carried into this epoch, which is the
		// nonce of the last block of the previous one.
		state.LastEpochBlockNonce = nonceFromBytes(
			current.LastEpochBlockNonce,
		)
		// PraosState's lab is the nonce of the last block applied so far.
		// Within an epoch that is the evolving nonce's most recent input, and
		// the ledger keeps no separate value for it.
		state.LabNonce = nonceFromBytes(current.LastEpochBlockNonce)
	}
	// Epoch 0 has no predecessor; its previous-epoch nonce stays neutral.
	if epochID > 0 {
		previous, err := ls.db.GetEpoch(epochID-1, txn)
		if err != nil {
			return nil, err
		}
		if previous != nil {
			state.PreviousEpochNonce = nonceFromBytes(previous.Nonce)
		}
	}

	return []any{
		versionedChainDepState{
			Version: chainDepStateVersionPraos,
			Inner:   state,
		},
	}, nil
}

// chainDepStateOpCertCounters collects the highest operational-certificate
// issue number the chain has accepted for each block issuer.
//
// The set is drawn from currently registered pools. A pool that minted and has
// since retired is therefore absent, which differs from the Haskell node's
// state; the counters exist to let an operator check their own pool's
// certificate against the chain, and a retired pool has none to check.
func (ls *LedgerState) chainDepStateOpCertCounters(txn *database.Txn) (
	map[lcommon.Blake2b224]uint64,
	error,
) {
	// Non-nil even when empty: the client-side decoder normalises a missing
	// map, but emitting CBOR null here would differ from the node's output.
	counters := map[lcommon.Blake2b224]uint64{}
	keyHashes, err := ls.db.GetActivePoolKeyHashes(txn)
	if err != nil {
		return nil, err
	}
	for _, pkh := range keyHashes {
		poolKeyHash := lcommon.PoolKeyHash(lcommon.NewBlake2b224(pkh))
		sequence, found, err := ls.db.LatestPoolOpCertSequence(
			poolKeyHash, txn,
		)
		if err != nil {
			return nil, err
		}
		if !found {
			// A registered pool that has never minted has no counter, which
			// the Haskell state also omits rather than reporting as zero.
			continue
		}
		counters[lcommon.Blake2b224(poolKeyHash)] = sequence
	}
	return counters, nil
}
