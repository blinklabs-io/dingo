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
	"encoding/hex"
	"io"
	"log/slog"
	"strings"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
)

// TestCreateGenesisBlockSkipsGenesisStakingAfterMithrilBootstrap is the
// same-bug-class regression test the #4151 PR review recommended: it
// found that SetGenesisStaking (and SetGenesisGovernance, covered by
// TestCreateGenesisBlockSkipsGenesisGovernanceAfterMithrilBootstrap below)
// weren't gated by the same bootstrappedFromMithril guard as genesis UTxO
// insertion. Both upsert current-state rows (ON CONFLICT ... DO UPDATE),
// and a Mithril-bootstrapped node's imported ledger snapshot
// (ledgerstate/import.go's importCertState) already reflects the correct
// current pool/delegation state as of the bootstrap point -- reapplying
// stale genesis-config values would silently resurrect a pool genuinely
// retired (or a delegation genuinely changed) before the bootstrap point.
//
// Uses the embedded devnet config, the only bundled network that declares
// a nonzero genesis pool + stake delegation (mainnet/preview/preprod/
// musashi all declare zero, so the bug was unreachable there, but live on
// devnet).
func TestCreateGenesisBlockSkipsGenesisStakingAfterMithrilBootstrap(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"devnet/config.json",
		"devnet",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)

	genesisPools, _, err := nodeCfg.ShelleyGenesis().InitialPools()
	require.NoError(t, err)
	require.NotEmpty(
		t, genesisPools,
		"devnet must declare at least one genesis pool for this test to "+
			"be meaningful",
	)
	var poolIdHex string
	for k := range genesisPools {
		poolIdHex = k
		break
	}
	poolKeyHash, err := hex.DecodeString(poolIdHex)
	require.NoError(t, err)

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Database:          db,
			CardanoNodeConfig: nodeCfg,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
		},
	}
	// Same bootstrap shape as
	// TestCreateGenesisBlockSkipsUtxoInsertionAfterMithrilBootstrap: a
	// currentTip past slot 0 with no genesis CBOR yet stored.
	ls.currentTip.Point.Slot = 42
	require.NoError(t, ls.createGenesisBlock())

	_, err = db.GetPool(lcommon.PoolKeyHash(poolKeyHash), true, nil)
	require.ErrorIs(
		t, err, models.ErrPoolNotFound,
		"a genesis pool must not be (re-)inserted after a Mithril "+
			"bootstrap -- the imported ledger snapshot is the only "+
			"authority on whether it is still registered or was already "+
			"retired before the bootstrap point",
	)

	// Re-running (as a real startup would on every restart) must remain
	// idempotent and continue to skip insertion.
	require.NoError(t, ls.createGenesisBlock())
	_, err = db.GetPool(lcommon.PoolKeyHash(poolKeyHash), true, nil)
	require.ErrorIs(t, err, models.ErrPoolNotFound)
}

// TestCreateGenesisBlockSkipsGenesisGovernanceAfterMithrilBootstrap covers
// the same guard for SetGenesisGovernance. No bundled network config
// declares a genesis DRep today (devnet's conway-genesis.json has an
// empty initialDReps), so this synthesizes a minimal one via
// LoadConwayGenesisFromReader, the same test-only escape hatch
// config/cardano/node.go documents "mostly for tests".
func TestCreateGenesisBlockSkipsGenesisGovernanceAfterMithrilBootstrap(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"devnet/config.json",
		"devnet",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)

	const drepKeyHashHex = "00112233445566778899aabbccddeeff001122334455667788990011"
	require.Len(t, drepKeyHashHex, 56, "must decode to 28 bytes")
	conwayGenesisJson := `{
		"poolVotingThresholds": {},
		"dRepVotingThresholds": {},
		"committeeMinSize": 0,
		"committeeMaxTermLength": 0,
		"govActionLifetime": 0,
		"govActionDeposit": 0,
		"dRepDeposit": 0,
		"dRepActivity": 0,
		"minFeeRefScriptCostPerByte": null,
		"plutusV3CostModel": [],
		"constitution": {"anchor": {"dataHash": "", "url": ""}, "script": ""},
		"committee": {"members": {}, "threshold": null},
		"delegs": {},
		"initialDReps": {
			"keyHash-` + drepKeyHashHex + `": {
				"expiry": 500,
				"deposit": 500000000,
				"anchor": null
			}
		}
	}`
	require.NoError(
		t,
		nodeCfg.LoadConwayGenesisFromReader(
			strings.NewReader(conwayGenesisJson),
		),
	)

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Database:          db,
			CardanoNodeConfig: nodeCfg,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
		},
	}
	ls.currentTip.Point.Slot = 42
	require.NoError(t, ls.createGenesisBlock())

	drepKeyHash, err := hex.DecodeString(drepKeyHashHex)
	require.NoError(t, err)
	_, err = db.GetDrep(drepKeyHash, true, nil)
	require.ErrorIs(
		t, err, models.ErrDrepNotFound,
		"a genesis DRep must not be (re-)inserted after a Mithril "+
			"bootstrap -- the imported ledger snapshot is the only "+
			"authority on current DRep/delegation state",
	)

	require.NoError(t, ls.createGenesisBlock())
	_, err = db.GetDrep(drepKeyHash, true, nil)
	require.ErrorIs(t, err, models.ErrDrepNotFound)
}
