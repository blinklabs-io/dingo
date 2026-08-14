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

package ledgerstate

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// refPool is one pool's entry in `cardano-cli query stake-snapshot`.
type refPool struct {
	StakeMark uint64 `json:"stakeMark"`
	StakeSet  uint64 `json:"stakeSet"`
	StakeGo   uint64 `json:"stakeGo"`
}

// The gate on the derived reward basis proves it reconciles with itself. It
// cannot prove the derivation agrees with the reference implementation: a
// systematically different aggregation would satisfy every internal identity
// and still produce wrong rewards.
//
// This closes that gap by deriving from a real cardano-node ledger-state
// snapshot and comparing the per-pool result against what that same node
// reports through `cardano-cli query stake-snapshot --all-stake-pools`. The
// CLI's stakeMark/stakeSet/stakeGo are the reference's own view of the three
// snapshots this seeding reads, so agreement is agreement with cardano-node
// rather than with my reading of it.
//
// It is opt-in because it needs both artifacts from a running node:
//
//	DINGO_REF_LEDGER_SNAPSHOT  path to a cardano-node ledger snapshot file
//	                           (written on graceful shutdown, under
//	                           <db>/ledger/<slot>)
//	DINGO_REF_STAKE_SNAPSHOT   path to the JSON emitted by
//	                           `cardano-cli query stake-snapshot
//	                            --all-stake-pools --output-json`
//	DINGO_REF_EPOCH            the epoch the node was in when that query ran
//
// The epoch matters because the two artifacts are rarely from the same point:
// a ledger snapshot is written at the node's immutable tip, which lags the
// tip the CLI query sees, so the snapshot is often an epoch behind. Snapshots
// rotate at the boundary, so an offset of one means this snapshot's mark is
// the reference's set and its set is the reference's go. Getting that wrong
// makes a correct derivation look like an off-by-one bug -- it did here on
// the first run -- so the offset is stated rather than assumed.
func TestDerivedRewardInputsMatchReferenceStakeSnapshot(t *testing.T) {
	snapshotPath := os.Getenv("DINGO_REF_LEDGER_SNAPSHOT")
	referencePath := os.Getenv("DINGO_REF_STAKE_SNAPSHOT")
	if snapshotPath == "" || referencePath == "" {
		t.Skip(
			"set DINGO_REF_LEDGER_SNAPSHOT and DINGO_REF_STAKE_SNAPSHOT to " +
				"compare the derived reward basis against cardano-node",
		)
	}

	state, err := ParseSnapshot(snapshotPath)
	require.NoError(t, err, "parsing the reference ledger snapshot")
	require.NotNil(t, state.SnapShotsData,
		"the snapshot carries no stake snapshots")

	snapshots, err := ParseSnapShots(state.SnapShotsData)
	if err != nil {
		require.NotNil(t, snapshots, "parsing stake snapshots: %v", err)
		t.Logf("stake snapshot parse warnings: %v", err)
	}

	// Pool parameters come from cert state, the same place the import takes
	// them: current snapshots carry only the compact pool-distr shape inside
	// SnapShots, with no margin, cost, pledge, reward account or owners.
	require.NotNil(t, state.CertStateData,
		"the snapshot carries no cert state to take pool parameters from")
	certState, err := ParseCertState(state.CertStateData)
	require.NoError(t, err, "parsing cert state")
	params := make(map[string]*ParsedPool, len(certState.Pools))
	for i := range certState.Pools {
		pool := certState.Pools[i]
		params[hex.EncodeToString(pool.PoolKeyHash)] = &pool
	}
	require.NotEmpty(t, params, "cert state carries no pool registrations")

	raw, err := os.ReadFile(referencePath)
	require.NoError(t, err, "reading the reference stake snapshot")
	var reference struct {
		Pools map[string]refPool `json:"pools"`
	}
	require.NoError(t, json.Unmarshal(raw, &reference))
	require.NotEmpty(t, reference.Pools, "reference reports no pools")

	refEpochRaw := os.Getenv("DINGO_REF_EPOCH")
	require.NotEmpty(t, refEpochRaw,
		"set DINGO_REF_EPOCH to the epoch the stake-snapshot query ran in")
	refEpoch, err := strconv.ParseUint(refEpochRaw, 10, 64)
	require.NoError(t, err, "parsing DINGO_REF_EPOCH")
	require.GreaterOrEqual(t, refEpoch, state.Epoch,
		"the reference query cannot predate the ledger snapshot")
	offset := refEpoch - state.Epoch
	require.LessOrEqual(t, offset, uint64(2),
		"a snapshot more than two epochs behind the query shares no "+
			"positions with it")
	t.Logf("ledger snapshot epoch %d, reference epoch %d (offset %d)",
		state.Epoch, refEpoch, offset)

	// Position i of this snapshot is position i+offset of the reference.
	columns := []func(v refPool) uint64{
		func(v refPool) uint64 { return v.StakeMark },
		func(v refPool) uint64 { return v.StakeSet },
		func(v refPool) uint64 { return v.StakeGo },
	}
	pick := func(i int) func(string) (uint64, bool) {
		idx := i + int(offset)
		if idx >= len(columns) {
			return nil // no counterpart in the reference at this offset
		}
		return func(p string) (uint64, bool) {
			v, ok := reference.Pools[p]
			return columns[idx](v), ok
		}
	}

	for _, c := range []struct {
		name string
		snap *ParsedSnapShot
		want func(poolHex string) (uint64, bool)
	}{
		{"mark", &snapshots.Mark, pick(0)},
		{"set", &snapshots.Set, pick(1)},
		{"go", &snapshots.Go, pick(2)},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.want == nil {
				t.Skipf(
					"the reference has no column for the %s snapshot at "+
						"offset %d", c.name, offset,
				)
			}
			bundle := deriveRewardInputs(c.snap, params, state.Epoch, 1, 0)
			require.NotNil(t, bundle)
			require.NoError(t, bundle.validate(),
				"a basis derived from a real snapshot must reconcile")

			derived := make(map[string]uint64, len(bundle.poolInputs))
			for _, pool := range bundle.poolInputs {
				derived[hex.EncodeToString(pool.PoolKeyHash)] =
					uint64(pool.DelegatedStake)
			}

			pools := make([]string, 0, len(derived))
			for pool := range derived {
				pools = append(pools, pool)
			}
			sort.Strings(pools)

			var compared int
			for _, pool := range pools {
				want, ok := c.want(pool)
				if !ok {
					// The reference lists only pools it holds stake for; a
					// pool absent there with zero derived stake is agreement.
					require.Zero(t, derived[pool],
						"pool %s has derived stake but is absent from the "+
							"reference snapshot", pool)
					continue
				}
				compared++
				require.Equal(t, want, derived[pool],
					"pool %s: derived delegated stake disagrees with "+
						"cardano-node's %s snapshot", pool, c.name)
			}
			t.Logf("%s: %d pools compared against cardano-node, all equal",
				c.name, compared)
		})
	}
}
