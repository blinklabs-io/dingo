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

//go:build devnet

package devnet

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	utxorpccardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// LedgerState is a normalized, comparison-friendly snapshot of the ledger
// state exposed by a single node's LocalStateQuery interface: current
// protocol parameters, stake distribution, and the whole UTxO set.
//
// Fields are already canonicalized (sorted, stringified) so that two
// LedgerState values built from independently-decoded LocalStateQuery
// responses (one per node) can be compared directly, regardless of map
// iteration order or which node produced them.
type LedgerState struct {
	// ProtocolParams is the current protocol parameter set, converted to
	// its utxorpc representation. Utxorpc() gives a stable, era-neutral
	// proto.Message we can diff with proto.Equal instead of hand-rolling
	// a comparison across every gouroboros *ProtocolParameters type.
	ProtocolParams *utxorpccardano.PParams

	// StakeDistribution maps pool ID to stake fraction, as reported by
	// GetStakeDistribution.
	StakeDistribution map[lcommon.PoolId]*big.Rat

	// UTxOEntries maps "<txHash>#<outputIndex>" to a canonical string
	// encoding of that output's address, lovelace amount, any
	// multi-asset tokens (sorted by policy then asset name so the
	// encoding is deterministic), datum, and reference script.
	UTxOEntries map[string]string
}

// LedgerStateAtTip dials addr over node-to-client (NtC) TCP, acquires the
// volatile tip, and queries protocol parameters, stake distribution, and
// the whole UTxO set as a single LocalStateQuery session so all three
// reflect one consistent view of that node's ledger state.
//
// This intentionally does not support pinning an exact historical block
// via Acquire(point): Dingo's LocalStateQuery server
// (ouroboros/localstatequery.go) currently answers every Acquire against
// its live tip regardless of the requested point (no point-specific
// ledger view yet; see blinklabs-io/dingo#382), so a caller that needs to
// compare two nodes at the same point must instead confirm via NtN
// chain-tip polling that both nodes report an identical tip immediately
// before and after calling this function, and retry the sample if not.
func LedgerStateAtTip(addr string, magic uint32) (*LedgerState, error) {
	conn, err := ouroboros.New(
		ouroboros.WithNetworkMagic(magic),
		ouroboros.WithNodeToNode(false),
	)
	if err != nil {
		return nil, fmt.Errorf("ouroboros.New: %w", err)
	}
	defer conn.Close() //nolint:errcheck

	if err := conn.DialTimeout("tcp", addr, 10*time.Second); err != nil {
		return nil, fmt.Errorf("dial tcp %s: %w", addr, err)
	}

	lsq := conn.LocalStateQuery()
	if lsq == nil || lsq.Client == nil {
		return nil, fmt.Errorf("LocalStateQuery client unavailable on %s", addr)
	}
	client := lsq.Client
	if err := client.AcquireVolatileTip(); err != nil {
		return nil, fmt.Errorf("acquire tip on %s: %w", addr, err)
	}
	defer client.Release() //nolint:errcheck

	pp, err := client.GetCurrentProtocolParams()
	if err != nil {
		return nil, fmt.Errorf("protocol params query on %s: %w", addr, err)
	}
	ppProto, err := pp.Utxorpc()
	if err != nil {
		return nil, fmt.Errorf(
			"converting protocol params to utxorpc on %s: %w", addr, err,
		)
	}

	sd, err := client.GetStakeDistribution()
	if err != nil {
		return nil, fmt.Errorf("stake distribution query on %s: %w", addr, err)
	}
	stakeDist := make(map[lcommon.PoolId]*big.Rat, len(sd.Results))
	for poolID, entry := range sd.Results {
		if entry.StakeFraction == nil {
			continue
		}
		stakeDist[poolID] = entry.StakeFraction.Rat
	}

	utxos, err := client.GetUTxOWhole()
	if err != nil {
		return nil, fmt.Errorf("whole UTxO query on %s: %w", addr, err)
	}
	entries := make(map[string]string, len(utxos.Results))
	for id, out := range utxos.Results {
		key := fmt.Sprintf("%s#%d", id.Hash.String(), id.Idx)
		entries[key] = canonicalUTxOEntry(out)
	}

	return &LedgerState{
		ProtocolParams:    ppProto,
		StakeDistribution: stakeDist,
		UTxOEntries:       entries,
	}, nil
}

// canonicalUTxOEntry builds a deterministic string encoding of a UTxO's
// address, lovelace amount, any multi-asset tokens (sorted by policy then
// asset name), datum, and reference script, so two independently-decoded
// outputs with identical content produce identical strings regardless of
// map iteration order.
//
// The output is read through the ledger.TransactionOutput interface
// rather than the concrete babbage.BabbageTransactionOutput struct so this
// keeps working if gouroboros ever decodes GetUTxOWhole into a different
// era-specific type.
//
// Datum and reference script are each folded into a single content hash
// (DatumHash()/Datum().Hash() and ScriptRef().Hash() respectively) rather
// than re-encoded byte-for-byte: both are already content-addressed by
// construction, so two hashes matching is exactly the "same content"
// signal this comparison needs, without pulling in a canonical CBOR/Plutus
// re-encoding of arbitrary datum or script bytes.
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

	// DatumHash() covers both wire forms of a datum option: an explicit
	// hash, or an inline datum (in which case DatumHash() returns the
	// hash of that inline content) — see
	// babbage.BabbageTransactionOutput.DatumHash(). One field is enough;
	// there is no case where a real hash mismatch would hide behind a
	// matching inline datum or vice versa.
	if dh := out.DatumHash(); dh != nil {
		fmt.Fprintf(&sb, "|datum=%s", dh.String())
	}
	if sr := out.ScriptRef(); sr != nil {
		fmt.Fprintf(&sb, "|scriptref=%s", sr.Hash().String())
	}
	return sb.String()
}

// DiffLedgerStates compares two LedgerState snapshots and returns a
// human-readable description of every divergence found: protocol
// parameter differences (as a single unified-looking JSON diff), stake
// distribution differences per pool, and UTxO set differences per output.
// It returns an empty slice when the two snapshots are equal.
//
// UTxO differences are capped at maxUTxODiffReports individual entries to
// keep failure output readable on a diverged network with many UTxOs; a
// trailing summary line reports the total count when the cap is hit.
func DiffLedgerStates(a, b *LedgerState) []string {
	var diffs []string

	if !proto.Equal(a.ProtocolParams, b.ProtocolParams) {
		diffs = append(diffs, fmt.Sprintf(
			"protocol parameters differ:\n--- a ---\n%s\n--- b ---\n%s",
			protojson.Format(a.ProtocolParams),
			protojson.Format(b.ProtocolParams),
		))
	}

	for poolID, aFrac := range a.StakeDistribution {
		bFrac, ok := b.StakeDistribution[poolID]
		switch {
		case !ok:
			diffs = append(diffs, fmt.Sprintf(
				"stake distribution: pool %s present in a, missing in b",
				poolID,
			))
		case aFrac.Cmp(bFrac) != 0:
			diffs = append(diffs, fmt.Sprintf(
				"stake distribution: pool %s fraction differs: %s (a) vs %s (b)",
				poolID,
				aFrac.RatString(),
				bFrac.RatString(),
			))
		}
	}
	for poolID := range b.StakeDistribution {
		if _, ok := a.StakeDistribution[poolID]; !ok {
			diffs = append(diffs, fmt.Sprintf(
				"stake distribution: pool %s present in b, missing in a",
				poolID,
			))
		}
	}

	const maxUTxODiffReports = 20
	utxoDiffCount := 0
	report := func(format string, args ...any) {
		utxoDiffCount++
		if utxoDiffCount <= maxUTxODiffReports {
			diffs = append(diffs, fmt.Sprintf(format, args...))
		}
	}
	for key, aVal := range a.UTxOEntries {
		bVal, ok := b.UTxOEntries[key]
		switch {
		case !ok:
			report("utxo %s present in a, missing in b: %s", key, aVal)
		case aVal != bVal:
			report("utxo %s differs: %s (a) vs %s (b)", key, aVal, bVal)
		}
	}
	for key, bVal := range b.UTxOEntries {
		if _, ok := a.UTxOEntries[key]; !ok {
			report("utxo %s present in b, missing in a: %s", key, bVal)
		}
	}
	if utxoDiffCount > maxUTxODiffReports {
		diffs = append(diffs, fmt.Sprintf(
			"... %d more utxo differences omitted",
			utxoDiffCount-maxUTxODiffReports,
		))
	}

	return diffs
}
