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
	"encoding/hex"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// The constitution the Musashi Conway genesis declares. Genesis
// initialization must record exactly these bytes, since guardrails
// validation compares the script hash against every parameter-change and
// treasury-withdrawal proposal's policy hash.
const (
	musashiConstitutionURL = "ipfs://" +
		"bafkreiazhhawe7sjwuthcfgl3mmv2swec7sukvclu3oli7qdyz4uhhuvmy"
	musashiConstitutionAnchorHash = "2a61e2f4b63442978140c77a70daab396" +
		"1b22b12b63b13949a390c097214d1c5"
	musashiConstitutionScriptHash = "fa24fb305126805cf2164c161d852a0e" +
		"7330cf988f1fe558cf7d4a64"
)

// genesisConstitutionTestState builds a LedgerState over a file-backed test
// database with the Musashi configuration, whose Conway genesis declares a
// constitution with a guardrails script.
func genesisConstitutionTestState(
	t *testing.T,
) (*LedgerState, *database.Database) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"musashi/config.json",
		"musashi",
		cardano.EmbeddedConfigFS,
	)
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
	return ls, db
}

// requireGenesisConstitution asserts the view reports exactly the
// constitution the Musashi Conway genesis declares.
func requireGenesisConstitution(t *testing.T, lv *LedgerView) {
	t.Helper()
	got, err := lv.Constitution()
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, musashiConstitutionURL, got.Anchor.Url)
	require.Equal(
		t,
		musashiConstitutionAnchorHash,
		hex.EncodeToString(got.Anchor.DataHash[:]),
	)
	require.Equal(
		t,
		musashiConstitutionScriptHash,
		hex.EncodeToString(got.ScriptHash),
	)
}

// TestCreateGenesisBlockSeedsConstitution proves a node initialized from
// Conway genesis reports the genesis constitution, so guardrails validation
// accepts a treasury-withdrawal proposal carrying the genesis guardrails
// script hash and rejects one carrying none. Without the seed the lookup
// fails closed and every such proposal is rejected until a NewConstitution
// action is enacted.
func TestCreateGenesisBlockSeedsConstitution(t *testing.T) {
	ls, _ := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock())

	lv := &LedgerView{ls: ls}
	requireGenesisConstitution(t, lv)

	scriptHash, err := hex.DecodeString(musashiConstitutionScriptHash)
	require.NoError(t, err)
	require.NoError(t, constitutionTestGuardrails(t, lv, scriptHash))

	err = constitutionTestGuardrails(t, lv, nil)
	require.Error(t, err)
	var mismatch conway.InvalidGuardrailsScriptHashError
	require.ErrorAs(t, err, &mismatch)
	require.Equal(t, scriptHash, mismatch.Expected)
}

// TestCreateGenesisBlockConstitutionReplayIdempotent proves re-running
// genesis initialization over a store that already holds the genesis
// constitution leaves a single slot-0 row rather than a duplicate.
func TestCreateGenesisBlockConstitutionReplayIdempotent(t *testing.T) {
	ls, db := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock())
	require.NoError(t, ls.createGenesisBlock())

	requireGenesisConstitution(t, &LedgerView{ls: ls})
	require.Equal(t, 1, constitutionRowCount(t, db))
}

// TestCreateGenesisBlockConstitutionSeededOnRestart proves the restart path
// -- an existing database whose genesis CBOR already matches, which returns
// before genesis storage is rewritten -- still records the genesis
// constitution. A database created before the constitution was seeded
// reaches genesis initialization only through that path.
func TestCreateGenesisBlockConstitutionSeededOnRestart(t *testing.T) {
	ls, db := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock())

	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec("DELETE FROM constitution")
	require.NoError(t, err)
	require.NoError(t, raw.Close())
	require.Equal(t, 0, constitutionRowCount(t, db))

	// Advance past genesis so the second run takes the existing-database
	// path instead of rewriting genesis storage.
	ls.currentTip.Point = ocommon.Point{Slot: 100}
	require.NoError(t, ls.createGenesisBlock())

	requireGenesisConstitution(t, &LedgerView{ls: ls})
}

// TestCreateGenesisBlockConstitutionEnactmentWins proves an enacted
// NewConstitution action outranks the slot-0 genesis seed, and that a later
// genesis initialization pass does not restore the genesis constitution over
// it.
func TestCreateGenesisBlockConstitutionEnactmentWins(t *testing.T) {
	ls, db := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock())

	enactedAnchor := bytes.Repeat([]byte{0xe1}, lcommon.Blake2b256Size)
	enactedScript := bytes.Repeat([]byte{0xe2}, lcommon.Blake2b224Size)
	require.NoError(t, db.SetConstitution(&models.Constitution{
		AnchorURL:  "https://example.invalid/enacted",
		AnchorHash: enactedAnchor,
		PolicyHash: enactedScript,
		AddedSlot:  100,
	}, nil))

	ls.currentTip.Point = ocommon.Point{Slot: 200}
	require.NoError(t, ls.createGenesisBlock())

	lv := &LedgerView{ls: ls}
	got, err := lv.Constitution()
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "https://example.invalid/enacted", got.Anchor.Url)
	require.Equal(t, enactedAnchor, got.Anchor.DataHash[:])
	require.Equal(t, enactedScript, got.ScriptHash)

	require.NoError(t, constitutionTestGuardrails(t, lv, enactedScript))
}

// constitutionRowCount returns the number of stored constitution rows.
func constitutionRowCount(t *testing.T, db *database.Database) int {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	defer func() { require.NoError(t, raw.Close()) }()
	var count int
	require.NoError(
		t,
		raw.QueryRow("SELECT COUNT(*) FROM constitution").Scan(&count),
	)
	return count
}
