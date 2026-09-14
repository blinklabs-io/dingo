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

package nodeparity

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"sort"
	"strings"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	utxorpccardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

// Snapshot is a normalized, comparison-friendly view of the ledger state
// exposed by a single node's LocalStateQuery interface: current protocol
// parameters, stake distribution, and the whole UTxO set.
//
// Fields are already canonicalized (sorted, stringified) so that two
// Snapshot values built from independently-decoded LocalStateQuery
// responses (one per node) can be compared directly, regardless of map
// iteration order or which node produced them.
type Snapshot struct {
	// ProtocolParams is the current protocol parameter set, converted to
	// its utxorpc representation. Utxorpc() gives a stable, era-neutral
	// proto.Message we can diff with proto.Equal instead of hand-rolling
	// a comparison across every gouroboros *ProtocolParameters type.
	ProtocolParams *utxorpccardano.PParams

	// StakeDistribution maps pool ID to its stake fraction and registered
	// VRF key hash, as reported by GetStakeDistribution -- both fields, so
	// two nodes agreeing on every pool's stake share while disagreeing on
	// a registered VRF key (a real, distinct leader-election divergence)
	// is not masked by comparing the fraction alone.
	StakeDistribution map[lcommon.PoolId]StakeDistributionEntry

	// UTxOEntries maps "<txHash>#<outputIndex>" to a canonical string
	// encoding of that output's address, lovelace amount, any
	// multi-asset tokens (sorted by policy then asset name so the
	// encoding is deterministic), datum, and reference script.
	UTxOEntries map[string]string
}

// StakeDistributionEntry pairs a pool's stake fraction with its registered
// VRF key hash, the two fields GetStakeDistribution reports per pool.
type StakeDistributionEntry struct {
	StakeFraction *big.Rat
	VrfHash       ledger.Blake2b256
}

// QuerySnapshot acquires point (or the volatile tip, if point is nil) on an
// already-dialed connection and queries protocol parameters, stake
// distribution, and the whole UTxO set as a single LocalStateQuery session,
// via the standard GetUTxOWhole every Ouroboros LocalStateQuery server
// (Dingo or a real cardano-node) answers directly.
//
// point pins the session to a specific historical block instead of
// whatever's live when each individual query runs (blinklabs-io/dingo#382)
// -- the whole-UTxO walk can take on the order of minutes against Dingo's
// disk-backed store, long enough for a live testnet's tip to advance many
// blocks before it finishes. A real cardano-node's Acquire(point) genuinely
// pins its whole reply (every query on the session), but Dingo's
// server-side Acquire (ouroboros/localstatequery.go) only recognizes the
// pinned point for GetStakeDistribution/GetPoolDistr2, not GetUTxOWhole:
// GetUTxOWhole always answers at Dingo's live tip regardless of what was
// acquired, an accepted MVP gap tracked separately (not part of this
// change) -- there is no tip-sandwich or other before/after check
// discarding a result if Dingo's live tip moved during the walk, so a
// UTxO comparison against Dingo should be read as approximate rather
// than exactly pinned. GetCurrentProtocolParams and GetStakeDistribution
// are single round-trip queries issued immediately after Acquire, before
// the slow UTxO walk even starts, so the window for Dingo's live tip to
// move underneath them is negligible in comparison, and GetStakeDistribution
// does honor the acquired point (see ledger/queries_stakedistribution.go).
func QuerySnapshot(
	conn *ouroboros.Connection,
	point *pcommon.Point,
) (*Snapshot, error) {
	snap, _, err := querySnapshot(conn, point)
	return snap, err
}

// querySnapshot is QuerySnapshot's implementation, additionally returning
// every UTxO ref it saw. Check uses those refs to drive
// QueryReferenceUTxOSnapshot against the reference cardano-node instead of
// asking it for its own whole UTxO set -- see that function's doc comment
// for why.
func querySnapshot(
	conn *ouroboros.Connection,
	point *pcommon.Point,
) (*Snapshot, []localstatequery.UtxoId, error) {
	lsq := conn.LocalStateQuery()
	if lsq == nil || lsq.Client == nil {
		return nil, nil, errors.New("LocalStateQuery client unavailable")
	}
	client := lsq.Client
	if err := client.Acquire(point); err != nil {
		return nil, nil, fmt.Errorf("acquire point: %w", err)
	}
	defer client.Release() //nolint:errcheck

	ppProto, stakeDist, err := queryProtocolParamsAndStakeDistribution(client)
	if err != nil {
		return nil, nil, err
	}

	entries, refs, err := queryUTxOWhole(client)
	if err != nil {
		return nil, nil, err
	}

	return &Snapshot{
		ProtocolParams:    ppProto,
		StakeDistribution: stakeDist,
		UTxOEntries:       entries,
	}, refs, nil
}

// queryProtocolParams runs GetCurrentProtocolParams and converts the result
// to its utxorpc representation. Split out from
// queryProtocolParamsAndStakeDistribution so incremental.go's per-block
// check (queryIncrementalHalf) -- which cannot also query stake
// distribution, since Dingo's GetStakeDistribution handler only answers
// when the pinned point equals its live tip, never true for a per-block
// walk that is behind tip by design (see
// ledger/queries_stakedistribution.go's queryShelleyStakeDistribution;
// blinklabs-io/dingo#1900 incremental-mode audit finding) -- can reuse just
// this half without also querying stake distribution.
func queryProtocolParams(
	client *localstatequery.Client,
) (*utxorpccardano.PParams, error) {
	pp, err := client.GetCurrentProtocolParams()
	if err != nil {
		return nil, fmt.Errorf("protocol params query: %w", err)
	}
	ppProto, err := pp.Utxorpc()
	if err != nil {
		return nil, fmt.Errorf(
			"converting protocol params to utxorpc: %w", err,
		)
	}
	return ppProto, nil
}

// queryProtocolParamsAndStakeDistribution runs the two small, fast queries
// every full Snapshot needs beyond its UTxO half -- shared by querySnapshot
// and QueryReferenceUTxOSnapshot so both build these two fields identically.
// Both are answered at whatever point the session's Acquire call pinned
// (live-tip mode pins the tip both nodes just agreed on; an explicit
// historical check pins the caller-supplied point directly) -- see
// queryProtocolParams's doc comment for the per-block case that cannot use
// this.
func queryProtocolParamsAndStakeDistribution(
	client *localstatequery.Client,
) (*utxorpccardano.PParams, map[lcommon.PoolId]StakeDistributionEntry, error) {
	ppProto, err := queryProtocolParams(client)
	if err != nil {
		return nil, nil, err
	}

	sd, err := client.GetStakeDistribution()
	if err != nil {
		return nil, nil, fmt.Errorf("stake distribution query: %w", err)
	}
	stakeDist := make(
		map[lcommon.PoolId]StakeDistributionEntry,
		len(sd.Results),
	)
	for poolID, entry := range sd.Results {
		if entry.StakeFraction == nil {
			continue
		}
		stakeDist[poolID] = StakeDistributionEntry{
			StakeFraction: entry.StakeFraction.Rat,
			VrfHash:       entry.VrfHash,
		}
	}
	return ppProto, stakeDist, nil
}

// utxoByTxInBatchSize bounds how many UTxO refs QueryReferenceUTxOSnapshot
// asks for per GetUTxOByTxIn call. GetUTxOByTxIn is a real, standard
// LocalStateQuery type every cardano-node answers directly; keeping each
// batch bounded keeps every round trip small regardless of how large the
// caller's whole ref list is.
const utxoByTxInBatchSize = 5_000

// QueryReferenceUTxOSnapshot builds a Snapshot for a reference node (a real
// cardano-node) the same way querySnapshot does for protocol parameters and
// stake distribution, but resolves the UTxO half from exactly the refs
// named by knownRefs -- via batched GetUTxOByTxIn calls -- rather than
// asking the node for its own whole UTxO set.
//
// This exists because a real cardano-node's plain GetUTxOWhole was found,
// live against Preview, to silently close the connection partway through
// assembling a reply at the network's current UTxO-set scale (~3.17M
// entries): confirmed to fail consistently after roughly 11 seconds,
// independent of any bridge/proxy in the connection path (reproduced over
// a direct Unix-domain-socket connection to the node, no intermediary at
// all) and independent of gouroboros's own 120s mux read timeout -- the
// server itself is what ends the session, not a client-side timeout.
// GetUTxOByTxIn has no such problem: it is a real, standard, bounded query
// every cardano-node answers directly, confirmed working live at every
// batch size tried.
//
// The comparison this produces is intentionally asymmetric: it can only
// tell you whether the reference node agrees on the UTxOs the caller
// already knows about (typically Dingo's own paginated walk, via
// querySnapshot's second return value) -- it cannot discover a UTxO the
// reference node has that the caller's own list never named, since there
// is no cheap, reliable way left to ask a real cardano-node for its total
// UTxO set at this scale. That gap is accepted: this still catches the
// more likely and more actionable class of divergence (the caller
// computing wrong or stale state for a UTxO it does have), without
// requiring the one query that does not work.
func QueryReferenceUTxOSnapshot(
	conn *ouroboros.Connection,
	point *pcommon.Point,
	knownRefs []localstatequery.UtxoId,
) (*Snapshot, error) {
	lsq := conn.LocalStateQuery()
	if lsq == nil || lsq.Client == nil {
		return nil, errors.New("LocalStateQuery client unavailable")
	}
	client := lsq.Client
	if err := client.Acquire(point); err != nil {
		return nil, fmt.Errorf("acquire point: %w", err)
	}
	defer client.Release() //nolint:errcheck

	ppProto, stakeDist, err := queryProtocolParamsAndStakeDistribution(client)
	if err != nil {
		return nil, err
	}

	entries, err := queryUTxOByRefs(client, knownRefs)
	if err != nil {
		return nil, err
	}

	return &Snapshot{
		ProtocolParams:    ppProto,
		StakeDistribution: stakeDist,
		UTxOEntries:       entries,
	}, nil
}

// queryUTxOByRefs asks for exactly the named UTxO refs, in bounded batches
// via the real, standard GetUTxOByTxIn query -- see
// QueryReferenceUTxOSnapshot's doc comment for why this exists instead of
// GetUTxOWhole. A ref the server does not return (already spent, or never
// existed on its chain) is simply absent from the result map; the caller's
// diff already reports "present in a, missing in b" for that case, so no
// special handling is needed here for a missing ref.
func queryUTxOByRefs(
	client *localstatequery.Client,
	refs []localstatequery.UtxoId,
) (map[string]string, error) {
	entries := make(map[string]string, len(refs))
	for start := 0; start < len(refs); start += utxoByTxInBatchSize {
		end := min(start+utxoByTxInBatchSize, len(refs))
		batch := refs[start:end]
		txIns := make([]lcommon.TransactionInput, len(batch))
		for i, ref := range batch {
			txIns[i] = shelley.NewShelleyTransactionInput(
				ref.Hash.String(), ref.Idx,
			)
		}
		page, err := client.GetUTxOByTxIn(txIns)
		if err != nil {
			return nil, fmt.Errorf(
				"utxo by txin batch [%d:%d]: %w", start, end, err,
			)
		}
		for id, out := range page.Results {
			key := fmt.Sprintf("%s#%d", id.Hash.String(), id.Idx)
			entries[key] = canonicalUTxOEntry(out)
		}
	}
	return entries, nil
}

// queryUTxOWhole fetches the whole live UTxO set in one shot -- the
// standard query any Ouroboros LocalStateQuery server (including a real
// cardano-node) answers. Also returns every ref it saw, so a caller (Check)
// can drive QueryReferenceUTxOSnapshot's batched GetUTxOByTxIn comparison
// against the reference node from Dingo's own walk instead of asking the
// reference node for its own whole set -- see QueryReferenceUTxOSnapshot's
// doc comment for why the direct route doesn't work.
func queryUTxOWhole(
	client *localstatequery.Client,
) (map[string]string, []localstatequery.UtxoId, error) {
	start := time.Now()
	utxos, err := client.GetUTxOWhole()
	elapsed := time.Since(start)
	if err != nil {
		slog.Error(
			"whole UTxO query failed",
			"elapsed", elapsed,
			"error", err,
		)
		return nil, nil, fmt.Errorf("whole UTxO query: %w", err)
	}
	slog.Info(
		"whole UTxO query complete",
		"elapsed", elapsed,
		"entries", len(utxos.Results),
	)
	entries := make(map[string]string, len(utxos.Results))
	refs := make([]localstatequery.UtxoId, 0, len(utxos.Results))
	for id, out := range utxos.Results {
		key := fmt.Sprintf("%s#%d", id.Hash.String(), id.Idx)
		entries[key] = canonicalUTxOEntry(out)
		refs = append(refs, id)
	}
	return entries, refs, nil
}

// SnapshotAtTip dials addr and calls QuerySnapshot in one step, closing the
// connection before returning. Use this for a one-off look at a single
// node; Check manages its own connections directly so it can interleave tip
// reads around the query. See Dial for ctx's role.
func SnapshotAtTip(
	ctx context.Context,
	addr string,
	magic uint32,
) (*Snapshot, error) {
	conn, err := Dial(ctx, addr, magic)
	if err != nil {
		return nil, err
	}
	defer conn.Close() //nolint:errcheck
	return QuerySnapshot(conn, nil)
}

// canonicalUTxOEntry builds a deterministic string encoding of a UTxO's
// address, lovelace amount, any multi-asset tokens (sorted by policy then
// asset name), datum, and reference script, so two independently-decoded
// outputs with identical content produce identical strings regardless of
// map iteration order.
//
// The output is read through the ledger.TransactionOutput interface rather
// than a concrete era-specific struct so this keeps working if gouroboros
// ever decodes GetUTxOWhole into a different era-specific type.
//
// Datum and reference script are each folded into a single content hash
// (DatumHash() and ScriptRef().Hash() respectively) rather than re-encoded
// byte-for-byte: both are already content-addressed by construction, so two
// hashes matching is exactly the "same content" signal this comparison
// needs, without pulling in a canonical CBOR/Plutus re-encoding of
// arbitrary datum or script bytes.
func canonicalUTxOEntry(out ledger.TransactionOutput) string {
	var sb strings.Builder
	sb.WriteString(out.Address().String())
	sb.WriteString("|")
	sb.WriteString(out.Amount().String())

	if assets := out.Assets(); assets != nil {
		policies := assets.Policies()
		sort.Slice(policies, func(i, j int) bool {
			return bytes.Compare(policies[i].Bytes(), policies[j].Bytes()) < 0
		})
		for _, policy := range policies {
			names := assets.Assets(policy)
			sort.Slice(names, func(i, j int) bool {
				return bytes.Compare(names[i], names[j]) < 0
			})
			for _, name := range names {
				amount := assets.Asset(policy, name)
				fmt.Fprintf(
					&sb, "|%s.%s=%s",
					policy.String(), hex.EncodeToString(name), amount.String(),
				)
			}
		}
	}

	// DatumHash() alone covers a real content mismatch (different hash),
	// but not a form mismatch: an explicit datum-hash reference and an
	// inline datum carrying that same content hash to the same value here,
	// even though they are different wire forms -- one node reporting a
	// UTxO's datum as inline while the other reports only its hash would
	// be a genuine indexing/decoding divergence between the two
	// implementations, exactly what this tool exists to catch, and must
	// not be masked by a matching hash. Datum() is non-nil only for the
	// inline form, so folding that into the encoded form distinguishes it
	// from a hash-only reference with the identical content hash.
	if dh := out.DatumHash(); dh != nil {
		form := "hash"
		if out.Datum() != nil {
			form = "inline"
		}
		fmt.Fprintf(&sb, "|datum=%s:%s", form, dh.String())
	}
	if sr := out.ScriptRef(); sr != nil {
		fmt.Fprintf(&sb, "|scriptref=%s", sr.Hash().String())
	}
	return sb.String()
}
