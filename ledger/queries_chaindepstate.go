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
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/consensus"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

// The `encodeVersion` tags the Haskell consensus layer writes for the two
// chain-dependent-state serialisations. The tag is not decoration: it selects
// which record follows, and the two differ in nesting and arity, so emitting
// the wrong one hands the client a payload that does not match what the
// version promises.
const (
	chainDepStateVersionPraos  uint64 = 0
	chainDepStateVersionTPraos uint64 = 1
)

// chainDepStateUsesTPraos reports whether an era serialises its
// chain-dependent state as TPraos.
//
// Shelley through Alonzo run TPraos; Babbage onwards run Praos. dingo supports
// all of them, and a node syncing from genesis spends a long stretch in the
// TPraos eras while still answering queries.
//
// The answer is taken from consensusModeForEraID rather than from an era list
// of this query's own. The two would be answering the same question — which
// protocol the era runs — and a second list is a second place for a new era to
// go unlisted, from which the wire layout and the protocol the node actually
// elects leaders under can disagree. Byron is neither TPraos nor Praos (it ran
// PBFT and has no chain-dependent state of this shape), and maps to CPraos
// there, so it takes the modern layout along with every era since Babbage.
func chainDepStateUsesTPraos(eraId uint) bool {
	return consensusModeForEraID(eraId) == consensus.ConsensusModeTPraos
}

// tpraosPrtclState is the PrtclState record TPraos nests inside its
// chain-dependent state. Praos carries these three fields flat; TPraos groups
// them, which is the structural difference between the two layouts.
type tpraosPrtclState struct {
	cbor.StructAsArray
	OpCertCounters map[lcommon.Blake2b224]uint64
	EvolvingNonce  lcommon.Nonce
	CandidateNonce lcommon.Nonce
}

// tpraosChainDepState mirrors ouroboros-consensus' TPraosState record: the
// last slot beside the nested PrtclState. The epoch, previous-epoch, lab and
// last-epoch-block nonces are Praos-only and have no place here.
type tpraosChainDepState struct {
	cbor.StructAsArray
	LastSlot   olocalstatequery.WithOriginSlot
	PrtclState tpraosPrtclState
}

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

// praos2ChainDepState mirrors the Leios overlay's Praos2 chain-dependent
// state: the eight PraosState fields, then the announcement of the endorser
// block the last applied header carried. The announcement is a StrictMaybe, a
// list of zero or one element, so an absent one is the empty list (0x80) and
// never a CBOR null. Praos2 writes serialisation version 1, the number TPraos
// uses, because each protocol numbers its versions separately; the field
// count tells the layouts apart.
type praos2ChainDepState struct {
	cbor.StructAsArray
	LastSlot            olocalstatequery.WithOriginSlot
	OpCertCounters      map[lcommon.Blake2b224]uint64
	EvolvingNonce       lcommon.Nonce
	CandidateNonce      lcommon.Nonce
	EpochNonce          lcommon.Nonce
	PreviousEpochNonce  lcommon.Nonce
	LabNonce            lcommon.Nonce
	LastEpochBlockNonce lcommon.Nonce
	Announcement        []olocalstatequery.ChainDepStateAnnouncement
}

// chainDepStateUsesPraos2 reports whether the reply uses the Praos2 layout.
//
// Released consensus runs the Dijkstra era on plain Praos, so the era alone
// must not select Praos2: a mainnet, preprod, preview or sancho node answering
// with it would hand every client a state it cannot decode. It takes the
// Dijkstra era AND LedgerStateConfig.Praos2ChainDepState, which the node sets
// only for the Leios prototype network (Musashi) under its strict identity
// check (Config.musashiPrototypeIdentity): the name "musashi" or the
// magic 164, with no conflicting standard network. A devnet is not a conflict,
// so the name "musashi" with the devnet magic 42, or the devnet name with magic
// 164, selects Praos2 as well. A network named "devnet" with magic 42 carries
// neither half of the identity and keeps the Praos layout.
func (ls *LedgerState) chainDepStateUsesPraos2(eraId uint) bool {
	return ls.config.Praos2ChainDepState && eraId == eras.DijkstraEraDesc.Id
}

// versionedChainDepState is the `encodeVersion N` envelope both protocol
// serialisations share: a 2-element array of version and payload. The payload
// is whichever of the two records the version names.
type versionedChainDepState struct {
	cbor.StructAsArray
	Version uint64
	Inner   any
}

// epochAtTip reads the tip and the epoch record containing it from one
// transaction, for a query that has to describe both together.
//
// The in-memory tip and epoch snapshots are the cheaper source, but they are
// published after the database write that advances the chain, so a slot or
// epoch number taken from them and paired with rows read here can straddle an
// epoch boundary. What that costs is silent in both queries that need it: the
// chain-dependent state would carry a populated last slot beside a neutral
// epoch nonce, and the pool distribution would carry stake rows belonging to an
// epoch other than the one it names. Either way cardano-cli computes a
// leadership schedule that is wrong rather than absent.
//
// Resolving the epoch from the tip's slot rather than from an epoch number read
// elsewhere is what makes the pairing exact.
//
// A nil epoch record means no record covers the tip. The chain cannot reach
// that state part-way through an epoch -- applying a block requires that
// epoch's nonce to check the producer's leader VRF, and the nonce lives in the
// record, so the record exists before the tip can enter the epoch. What remains
// is the chain that has applied no blocks at all, where the callers' neutral
// and zero defaults are the right answer.
func (ls *LedgerState) epochAtTip(
	txn *database.Txn,
) (ochainsync.Tip, *models.Epoch, error) {
	tip, err := ls.db.GetTip(txn)
	if err != nil {
		return ochainsync.Tip{}, nil, err
	}
	current, err := ls.db.GetEpochBySlot(tip.Point.Slot, txn)
	if err != nil {
		return ochainsync.Tip{}, nil, err
	}
	return tip, current, nil
}

// epochAtPoint is epochAtTip for a pinned at: the acquired block stands in
// for the tip, and the epoch is the one containing its slot.
func (ls *LedgerState) epochAtPoint(
	at QueryPoint,
	txn *database.Txn,
) (ochainsync.Tip, *models.Epoch, error) {
	if !at.pinned() {
		return ls.epochAtTip(txn)
	}
	current, err := ls.db.GetEpochBySlot(at.Slot, txn)
	if err != nil {
		return ochainsync.Tip{}, nil, err
	}
	return ochainsync.Tip{Point: ocommon.NewPoint(at.Slot, at.Hash)}, current, nil
}

// nonceFromBytes converts a stored nonce into its wire form. An absent or
// empty value is the neutral nonce, which is how the ledger represents "no
// nonce yet" — notably at genesis and before the first epoch boundary. Any
// other length is a malformed stored nonce and fails rather than being
// reported as neutral.
func nonceFromBytes(b []byte) (lcommon.Nonce, error) {
	if len(b) == 0 {
		return lcommon.Nonce{Type: lcommon.NonceTypeNeutral}, nil
	}
	value, err := lcommon.NewBlake2b256Checked(b)
	if err != nil {
		return lcommon.Nonce{}, fmt.Errorf("stored nonce: %w", err)
	}
	return lcommon.Nonce{Type: lcommon.NonceTypeNonce, Value: value}, nil
}

// queryShelleyDebugChainDepState answers GetChainDepState, the consensus
// chain-dependent state at the acquired point.
//
// cardano-cli reads the epoch nonce from here when computing a leadership
// schedule, so leaving it unhandled does not merely fail one query: an
// unsupported query aborts the LocalStateQuery protocol, the node drops the
// connection, and the caller sees only a closed bearer.
//
// A pinned at answers at that block instead of the tip: the epoch rows of its
// epoch, the nonce fold stopped at it, the lab from its parent hash, and the
// op-cert counters observed at or before it. Epoch rows and op-cert rows are
// removed only by rollback. Block nonce rows keep the last three epochs; for a
// point older than that (API storage mode accepts one) the nonce fold
// recomputes from the stored blocks instead.
func (ls *LedgerState) queryShelleyDebugChainDepState(
	ctx context.Context,
	at QueryPoint,
	txn *database.Txn,
) (any, error) {
	// Every value in the reply is read from this one transaction, tip and epoch
	// included; see epochAtTip for why neither may come from the in-memory
	// snapshots.
	txn, release := ls.readTxn(ctx, txn)
	defer release()

	tip, current, err := ls.epochAtPoint(at, txn)
	if err != nil {
		return nil, err
	}
	lastSlot := olocalstatequery.WithOriginSlot{}
	if len(tip.Point.Hash) > 0 {
		lastSlot.HasSlot = true
		lastSlot.Slot = tip.Point.Slot
	}

	counters, err := ls.chainDepStateOpCertCounters(ctx, txn, at)
	if err != nil {
		return nil, err
	}

	var (
		eraId                         uint
		evolvingNonce, candidateNonce lcommon.Nonce
		carriedLabNonce               []byte
	)
	if current != nil {
		eraId = current.EraId
		carriedLabNonce = current.LastEpochBlockNonce
		// The epoch row's evolving and candidate nonces are the values the
		// epoch OPENED with, and nothing rewrites them as blocks land. Both
		// move with every block, so serving them directly would describe the
		// chain as it stood at the boundary while the rest of this reply
		// describes it at the tip. Recomputed here through the same function
		// the consensus path uses at a boundary, stopped at the tip.
		candidate, evolving, err := ls.computeCandidateNonceAsOf(
			ctx,
			txn,
			current.EraId,
			current.EvolvingNonce,
			current.CandidateNonce,
			current.StartSlot,
			uint64(current.LengthInSlots),
			foldEndSlotForTip(tip.Point.Slot),
			chainDepStateFoldTip(tip),
		)
		if err != nil {
			return nil, fmt.Errorf(
				"compute chain dep state nonces at slot %d: %w",
				tip.Point.Slot,
				err,
			)
		}
		evolvingNonce, err = nonceFromBytes(evolving)
		if err != nil {
			return nil, fmt.Errorf("chain dep state evolving nonce: %w", err)
		}
		candidateNonce, err = nonceFromBytes(candidate)
		if err != nil {
			return nil, fmt.Errorf("chain dep state candidate nonce: %w", err)
		}
	}

	// The era at the tip decides the layout. Both records carry the last slot,
	// the counters and these two nonces; the rest is Praos-only, so a TPraos
	// reply is complete here and does not pay for reads it cannot report.
	if chainDepStateUsesTPraos(eraId) {
		return []any{
			versionedChainDepState{
				Version: chainDepStateVersionTPraos,
				Inner: tpraosChainDepState{
					LastSlot: lastSlot,
					PrtclState: tpraosPrtclState{
						OpCertCounters: counters,
						EvolvingNonce:  evolvingNonce,
						CandidateNonce: candidateNonce,
					},
				},
			},
		}, nil
	}

	state := praosChainDepState{
		LastSlot:       lastSlot,
		OpCertCounters: counters,
		EvolvingNonce:  evolvingNonce,
		CandidateNonce: candidateNonce,
	}
	if current != nil {
		state.EpochNonce, err = nonceFromBytes(current.Nonce)
		if err != nil {
			return nil, fmt.Errorf("chain dep state epoch nonce: %w", err)
		}
		// The lab carried into this epoch: the parent hash of the last block
		// of the previous one.
		state.LastEpochBlockNonce, err = nonceFromBytes(
			current.LastEpochBlockNonce,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"chain dep state last epoch block nonce: %w",
				err,
			)
		}
		// Epoch 0 has no predecessor; its previous-epoch nonce stays neutral.
		if current.EpochId > 0 {
			previous, err := ls.db.GetEpoch(current.EpochId-1, txn)
			if err != nil {
				return nil, err
			}
			if previous != nil {
				state.PreviousEpochNonce, err = nonceFromBytes(previous.Nonce)
				if err != nil {
					return nil, fmt.Errorf(
						"chain dep state previous epoch nonce: %w",
						err,
					)
				}
			}
		}
	}

	state.LabNonce, err = ls.chainDepStateLabNonce(txn, tip, carriedLabNonce)
	if err != nil {
		return nil, err
	}

	if ls.chainDepStateUsesPraos2(eraId) {
		announcement, err := ls.chainDepStateAnnouncement(txn, tip)
		if err != nil {
			return nil, err
		}
		return []any{
			versionedChainDepState{
				Version: chainDepStateVersionTPraos,
				Inner:   newPraos2ChainDepState(state, announcement),
			},
		}, nil
	}

	return []any{
		versionedChainDepState{
			Version: chainDepStateVersionPraos,
			Inner:   state,
		},
	}, nil
}

// newPraos2ChainDepState extends a Praos state with the announcement of the
// last applied header. A nil announcement is written as the empty list.
func newPraos2ChainDepState(
	state praosChainDepState,
	announcement *olocalstatequery.ChainDepStateAnnouncement,
) praos2ChainDepState {
	// Non-nil even when empty: a nil slice would encode as a CBOR null.
	list := make([]olocalstatequery.ChainDepStateAnnouncement, 0, 1)
	if announcement != nil {
		list = append(list, *announcement)
	}
	return praos2ChainDepState{
		LastSlot:            state.LastSlot,
		OpCertCounters:      state.OpCertCounters,
		EvolvingNonce:       state.EvolvingNonce,
		CandidateNonce:      state.CandidateNonce,
		EpochNonce:          state.EpochNonce,
		PreviousEpochNonce:  state.PreviousEpochNonce,
		LabNonce:            state.LabNonce,
		LastEpochBlockNonce: state.LastEpochBlockNonce,
		Announcement:        list,
	}
}

// chainDepStateAnnouncement reads the Leios announcement the tip block's
// header carried, with the key hash of its issuer, or nil when the header
// announced nothing.
//
// Praos2 replaces the announcement with each applied header's own, so the
// state at a block is that block's announcement alone: nothing is carried over
// from earlier blocks, and none has to be reconstructed. Reading it from the
// acquired block gives the pinned and rollback semantics for free. The query
// reports the state after the last applied header, before any tick, so the
// clearing the ledger does when it ticks into a new epoch is not visible
// here; a tip in a later epoch than the block that announced simply has a
// later block that replaced it.
//
// A tip with no stored block, such as a Mithril-bootstrapped node whose tip
// body was never stored, reports no announcement, the same fallback the lab
// nonce takes. A header of an era without the Leios extension reports none.
func (ls *LedgerState) chainDepStateAnnouncement(
	txn *database.Txn,
	tip ochainsync.Tip,
) (*olocalstatequery.ChainDepStateAnnouncement, error) {
	if len(tip.Point.Hash) != lcommon.Blake2b256Size {
		return nil, nil
	}
	block, err := database.BlockByPointTxn(txn, tip.Point)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if block.Type != dijkstra.BlockTypeDijkstra {
		return nil, nil
	}
	// A block is [header, ...]: read the header alone rather than decoding
	// and validating every transaction for a query that needs one field.
	var parts []cbor.RawMessage
	if _, err := cbor.Decode(block.Cbor, &parts); err != nil {
		return nil, fmt.Errorf(
			"decode Dijkstra block at slot %d: %w",
			block.Slot,
			err,
		)
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("dijkstra block at slot %d has no header", block.Slot)
	}
	var header dijkstra.DijkstraBlockHeader
	if _, err := cbor.Decode(parts[0], &header); err != nil {
		return nil, fmt.Errorf(
			"decode Dijkstra header at slot %d: %w",
			block.Slot,
			err,
		)
	}
	ebHash, ebSize, ok := header.LeiosAnnouncement()
	if !ok {
		return nil, nil
	}
	if ebSize > math.MaxUint32 {
		return nil, fmt.Errorf(
			"announced endorser block size %d at slot %d exceeds Word32",
			ebSize,
			block.Slot,
		)
	}
	vkey := header.IssuerVkey()
	return &olocalstatequery.ChainDepStateAnnouncement{
		Issuer: lcommon.Blake2b224Hash(vkey[:]),
		EbHash: ebHash,
		EbSize: uint32(ebSize), // #nosec G115 -- bounded above
	}, nil
}

// chainDepStateFoldTip is the block a GetChainDepState fold ends at, when the
// tip names one: the acquired block, or the tip.
func chainDepStateFoldTip(tip ochainsync.Tip) *ocommon.Point {
	if len(tip.Point.Hash) != lcommon.Blake2b256Size {
		return nil
	}
	point := tip.Point
	return &point
}

// foldEndSlotForTip converts a tip slot into the exclusive end bound that
// includes the tip's own block.
//
// It saturates rather than wrapping. A wrap to zero would fold no blocks at
// all and report the epoch's opening values as though they were current, which
// is the silent-wrong-answer failure this whole reply is written to avoid.
// Saturating instead loses only the tip block itself, and only at a slot no
// chain reaches: computeCandidateNonceAsOf clamps any bound past the epoch's
// end back to it, so the fold still covers the epoch.
func foldEndSlotForTip(tipSlot uint64) uint64 {
	if tipSlot == ^uint64(0) {
		return tipSlot
	}
	return tipSlot + 1
}

// chainDepStateLabNonce derives the nonce of the last block applied.
//
// Unlike every other field of the record, the lab moves with each block rather
// than only at an epoch boundary. Praos sets it to prevHashToNonce of the
// applied block's parent hash -- the PARENT, not the block's own hash, a
// deliberate one-block lag that keeps the final block of an epoch out of the
// nonce it seeds. See epochLabNonce, which computes the same value at a
// boundary; a shift by one block makes every leader-VRF check in the next
// epoch fail.
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
	if len(tip.Point.Hash) != lcommon.Blake2b256Size {
		// No block applied, so consensus has never moved the lab off the
		// value the epoch opened with -- or a tip whose hash cannot name a
		// block at all, which is the same answer and, more to the point, not
		// a reason to abort the protocol and drop the connection.
		return nonceFromBytes(carriedLabNonce)
	}
	// By point, not by hash. A hash lookup goes through the block hash index,
	// which has only been populated for newer blocks and reports a miss rather
	// than scanning for a block predating it -- so a database still carrying
	// those blocks answers "no such block" for one it holds, and a node
	// restarted on such a database has exactly one of them as its tip. The
	// tip's slot and hash address the block's blob directly, which needs no
	// index.
	block, err := database.BlockByPointTxn(txn, tip.Point)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			// The tip names a block this transaction cannot read at all --
			// a Mithril-bootstrapped node whose tip body was never stored.
			// Reporting the carried value is the same answer as before the
			// epoch's first block, which is wrong by at most one block;
			// failing would abort the protocol and take the whole query
			// with it.
			return nonceFromBytes(carriedLabNonce)
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
		return nonceFromBytes(carriedLabNonce)
	}
	// prevHashToNonce maps GenesisHash to the neutral nonce rather than to the
	// genesis hash bytes, so the chain's first block leaves the lab as it was.
	if genesisHash, gErr := GenesisBlockHash(
		ls.config.CardanoNodeConfig,
	); gErr == nil && bytes.Equal(prevHash, genesisHash[:]) {
		return nonceFromBytes(carriedLabNonce)
	}
	return nonceFromBytes(prevHash)
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
func (ls *LedgerState) chainDepStateOpCertCounters(
	ctx context.Context,
	txn *database.Txn,
	at QueryPoint,
) (
	map[lcommon.Blake2b224]uint64,
	error,
) {
	var sequences map[string]uint64
	var err error
	if at.pinned() {
		sequences, err = ls.db.LatestPoolOpCertSequencesAtOrBefore(
			ctx,
			at.Slot,
			txn,
		)
	} else {
		sequences, err = ls.db.LatestPoolOpCertSequences(ctx, txn)
	}
	if err != nil {
		return nil, err
	}
	// Non-nil even when empty: the client-side decoder normalises a missing
	// map, but emitting CBOR null here would differ from the node's output.
	counters := make(map[lcommon.Blake2b224]uint64, len(sequences))
	for keyHash, sequence := range sequences {
		// A stored issuer key that is not a pool key hash fails the query.
		// Padding or truncating it would report a counter against a cold key
		// the row did not mean, and dropping it would report "no certificate
		// accepted yet" for a key the chain enforces a counter against. The
		// cost is availability: the error aborts GetChainDepState and drops
		// the client's LocalStateQuery connection, which is accepted because
		// a degraded answer here would be a wrong one.
		issuer, err := lcommon.NewBlake2b224Checked([]byte(keyHash))
		if err != nil {
			return nil, fmt.Errorf("op-cert counter issuer key: %w", err)
		}
		counters[issuer] = sequence
	}
	return counters, nil
}
