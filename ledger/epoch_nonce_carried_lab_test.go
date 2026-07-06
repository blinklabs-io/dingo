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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// TestEpochNonceUsesCarriedLastEpochBlockNonce verifies the cardano-ledger
// epoch-nonce assembly (#2734): the epoch nonce mixes the frozen candidate with
// the CARRIED last-block-of-previous-epoch nonce
// (prevEpoch.LastEpochBlockNonce == cardano praosStateLastEpochBlockNonce),
// NOT the hash of the last block of the epoch being closed. The closing epoch's
// last block hash is stored on the new epoch record for use at the NEXT
// boundary.
//
// Confirmed against preview mainnet: for the 1347->1348 boundary, koios
// eta_1348 = blake2b(frozenCandidate || epoch1347.LastEpochBlockNonce), where
// epoch1347.LastEpochBlockNonce is the last block of epoch 1346 (the carried
// value), not the last block of epoch 1347.
func TestEpochNonceUsesCarriedLastEpochBlockNonce(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	defer db.Close()

	cfg := newConwayBootstrapStabilityCfg(t)

	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		preCutSlot  uint64 = 1014
		postCutSlot uint64 = 1070
	)

	importedNonce := bytes.Repeat([]byte{0xaa}, 32)
	nonceAtPreCut := bytes.Repeat([]byte{0xbb}, 32) // frozen candidate
	nonceAtPostCut := bytes.Repeat([]byte{0xcc}, 32)
	carriedLab := bytes.Repeat([]byte{0xfa}, 32) // = last block of the PREVIOUS epoch

	hashAtPreCut := bytes.Repeat([]byte{0x14}, 32)
	hashAtPostCut := bytes.Repeat([]byte{0x70}, 32) // last block of the CLOSING epoch
	prevHashAtPreCut := bytes.Repeat([]byte{0x09}, 32)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot: preCutSlot, Hash: hashAtPreCut, PrevHash: prevHashAtPreCut,
			Cbor: []byte{0x80}, Number: 1, Type: conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot: postCutSlot, Hash: hashAtPostCut, PrevHash: hashAtPreCut,
			Cbor: []byte{0x80}, Number: 2, Type: conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.SetBlockNonce(
			hashAtPreCut, preCutSlot, nonceAtPreCut, false, txn); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAtPostCut, postCutSlot, nonceAtPostCut, false, txn)
	}))

	prevEpoch := models.Epoch{
		EpochId:             100,
		StartSlot:           epochStart,
		LengthInSlots:       uint(epochLength),
		SlotLength:          1000,
		EraId:               eras.ConwayEraDesc.Id,
		Nonce:               bytes.Repeat([]byte{0xee}, 32),
		EvolvingNonce:       importedNonce,
		CandidateNonce:      importedNonce,
		LastEpochBlockNonce: carriedLab,
	}

	ls := &LedgerState{
		db:           db,
		currentEra:   eras.ConwayEraDesc,
		currentEpoch: prevEpoch,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	hvNonce, _, hvCandidate, hvLab, err :=
		ls.computeEpochNonceForSlot(epochEnd, prevEpoch)
	require.NoError(t, err)

	var rNonce, rCandidate, rLab []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		n, _, c, lab, err := ls.calculateEpochNonce(
			txn, epochEnd, eras.ConwayEraDesc, prevEpoch)
		rNonce, rCandidate, rLab = n, c, lab
		return err
	}))

	// Correct assembly: eta = candidate ⭒ carriedLab (prevEpoch.LastEpochBlockNonce).
	wantEta, err := lcommon.CalculateEpochNonce(nonceAtPreCut, carriedLab, nil)
	require.NoError(t, err)
	// Old (buggy) assembly: eta = candidate ⭒ last block of closing epoch.
	oldEta, err := lcommon.CalculateEpochNonce(nonceAtPreCut, hashAtPostCut, nil)
	require.NoError(t, err)

	require.Equal(t, hex.EncodeToString(nonceAtPreCut),
		hex.EncodeToString(rCandidate), "candidate is the frozen pre-cutoff nonce")
	require.Equal(t, hex.EncodeToString(wantEta.Bytes()),
		hex.EncodeToString(rNonce),
		"epoch nonce must mix the candidate with the CARRIED lab, not the closing epoch's last block")
	require.NotEqual(t, hex.EncodeToString(oldEta.Bytes()),
		hex.EncodeToString(rNonce),
		"epoch nonce must NOT use the closing epoch's own last block (the #2734 bug)")
	// The carried lab stored for the NEXT boundary is the PARENT hash of the
	// closing epoch's last block (prevHashToNonce(lastBlock.prevHash) ==
	// hashAtPreCut, the post-cutoff block's PrevHash), NOT the last block's own
	// hash — a one-block Praos lag (#2734 eta_1349 root cause).
	require.Equal(t, hex.EncodeToString(hashAtPreCut),
		hex.EncodeToString(rLab),
		"stored lastEpochBlockNonce must be the closing epoch's last-block PrevHash, for the next boundary")
	require.NotEqual(t, hex.EncodeToString(hashAtPostCut),
		hex.EncodeToString(rLab),
		"stored lastEpochBlockNonce must NOT be the closing epoch's last block's own hash")

	// The eager header-verification path must agree with the rollover path.
	require.Equal(t, hex.EncodeToString(rNonce), hex.EncodeToString(hvNonce),
		"eager epoch nonce must match rollover")
	require.Equal(t, hex.EncodeToString(rCandidate), hex.EncodeToString(hvCandidate),
		"eager candidate must match rollover")
	require.Equal(t, hex.EncodeToString(rLab), hex.EncodeToString(hvLab),
		"eager lab must match rollover")
}

// TestEpochNonceGenesisEdgeUsesNeutralLab pins the from-genesis initialization
// of the carried lastEpochBlockNonce (#2734). When the initial epoch is created
// (no prior nonce), the epoch/evolving/candidate nonces are the genesis nonce
// but the carried lab is Neutral (nil) — NOT the genesis nonce. cardano-ledger
// initializes praosStateLastEpochBlockNonce to NeutralNonce at genesis
// (Cardano.Ledger.Shelley.API.Protocol.initialChainDepState: csLabNonce =
// NeutralNonce; Cardano.Protocol.TPraos.BHeader.prevHashToNonce GenesisHash =
// NeutralNonce), so the FIRST from-genesis boundary uses the identity
// (eta_1 = candidate ⭒ NeutralNonce = candidate). Devnet confirmed cardano's
// epoch-1 epochNonce == its candidate. Seeding the lab with the genesis nonce
// instead combines and diverges at the first boundary. The Mithril bootstrap
// path is unaffected (bootstrap epoch imports a non-nil lastEpochBlockNonce and
// never takes this branch).
func TestEpochNonceGenesisEdgeUsesNeutralLab(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	defer db.Close()

	cfg := newConwayBootstrapStabilityCfg(t)
	// ShelleyGenesisHash set by the helper is 32 bytes of 0x11.
	genesisHash := bytes.Repeat([]byte{0x11}, 32)

	// Initial epoch: no nonce/evolving/candidate yet (from-genesis creation).
	initialEpoch := models.Epoch{
		EpochId: 0,
		EraId:   eras.ConwayEraDesc.Id,
	}

	ls := &LedgerState{
		db:           db,
		currentEra:   eras.ConwayEraDesc,
		currentEpoch: initialEpoch,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	hvNonce, hvEvolving, hvCandidate, hvLab, err :=
		ls.computeEpochNonceForSlot(500, initialEpoch)
	require.NoError(t, err)

	var rNonce, rEvolving, rCandidate, rLab []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		n, ev, c, lab, err := ls.calculateEpochNonce(
			txn, 500, eras.ConwayEraDesc, initialEpoch)
		rNonce, rEvolving, rCandidate, rLab = n, ev, c, lab
		return err
	}))

	// Epoch/evolving/candidate are the genesis nonce for the initial epoch.
	require.Equal(t, hex.EncodeToString(genesisHash), hex.EncodeToString(rNonce),
		"initial epoch nonce is the genesis nonce")
	require.Equal(t, hex.EncodeToString(genesisHash), hex.EncodeToString(rEvolving),
		"initial evolving nonce is the genesis nonce")
	require.Equal(t, hex.EncodeToString(genesisHash), hex.EncodeToString(rCandidate),
		"initial candidate nonce is the genesis nonce")
	// The key #2734 assertion: the carried lab is Neutral (nil), NOT the genesis
	// nonce, so the first from-genesis boundary uses the identity.
	require.Empty(t, rLab,
		"initial carried lastEpochBlockNonce must be Neutral (nil), not the genesis nonce")

	// The eager header-verification path must agree with the rollover path.
	require.Equal(t, hex.EncodeToString(rNonce), hex.EncodeToString(hvNonce))
	require.Equal(t, hex.EncodeToString(rEvolving), hex.EncodeToString(hvEvolving))
	require.Equal(t, hex.EncodeToString(rCandidate), hex.EncodeToString(hvCandidate))
	require.Empty(t, hvLab,
		"eager path initial carried lab must also be Neutral (nil)")
}
