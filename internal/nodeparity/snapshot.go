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
	"math/big"
	"sort"
	"strings"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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

// QuerySnapshot acquires the volatile tip on an already-dialed connection
// and queries protocol parameters, stake distribution, and the whole UTxO
// set as a single LocalStateQuery session, so all three reflect one
// consistent view of that node's ledger state.
//
// This intentionally does not support pinning an exact historical block via
// Acquire(point): Dingo's LocalStateQuery server (ouroboros/localstatequery.go)
// currently answers every Acquire against its live tip regardless of the
// requested point (no point-specific ledger view yet; see
// blinklabs-io/dingo#382), so Check sandwiches this call between two tip
// reads and discards the result if the tip moved in between, rather than
// relying on Acquire(point) to pin a block itself.
func QuerySnapshot(conn *ouroboros.Connection) (*Snapshot, error) {
	lsq := conn.LocalStateQuery()
	if lsq == nil || lsq.Client == nil {
		return nil, errors.New("LocalStateQuery client unavailable")
	}
	client := lsq.Client
	if err := client.AcquireVolatileTip(); err != nil {
		return nil, fmt.Errorf("acquire tip: %w", err)
	}
	defer client.Release() //nolint:errcheck

	pp, err := client.GetCurrentProtocolParams()
	if err != nil {
		return nil, fmt.Errorf("protocol params query: %w", err)
	}
	ppProto, err := pp.Utxorpc()
	if err != nil {
		return nil, fmt.Errorf("converting protocol params to utxorpc: %w", err)
	}

	sd, err := client.GetStakeDistribution()
	if err != nil {
		return nil, fmt.Errorf("stake distribution query: %w", err)
	}
	stakeDist := make(map[lcommon.PoolId]StakeDistributionEntry, len(sd.Results))
	for poolID, entry := range sd.Results {
		if entry.StakeFraction == nil {
			continue
		}
		stakeDist[poolID] = StakeDistributionEntry{
			StakeFraction: entry.StakeFraction.Rat,
			VrfHash:       entry.VrfHash,
		}
	}

	utxos, err := client.GetUTxOWhole()
	if err != nil {
		return nil, fmt.Errorf("whole UTxO query: %w", err)
	}
	entries := make(map[string]string, len(utxos.Results))
	for id, out := range utxos.Results {
		key := fmt.Sprintf("%s#%d", id.Hash.String(), id.Idx)
		entries[key] = canonicalUTxOEntry(out)
	}

	return &Snapshot{
		ProtocolParams:    ppProto,
		StakeDistribution: stakeDist,
		UTxOEntries:       entries,
	}, nil
}

// SnapshotAtTip dials addr and calls QuerySnapshot in one step, closing the
// connection before returning. Use this for a one-off look at a single
// node; Check manages its own connections directly so it can interleave tip
// reads around the query. See Dial for ctx's role.
func SnapshotAtTip(ctx context.Context, addr string, magic uint32) (*Snapshot, error) {
	conn, err := Dial(ctx, addr, magic)
	if err != nil {
		return nil, err
	}
	defer conn.Close() //nolint:errcheck
	return QuerySnapshot(conn)
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
