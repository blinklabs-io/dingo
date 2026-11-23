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
	"log/slog"
	"slices"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

// genesisUtxoSet is the genesis UTxO set grouped into the synthetic genesis
// transactions, with the synthetic block that carries their outputs.
type genesisUtxoSet struct {
	txUtxos   map[[32]byte][]lcommon.Utxo
	offsets   map[database.UtxoRef]database.CborOffset
	utxos     []lcommon.Utxo
	blockCbor []byte
	reserves  uint64
}

// buildGenesisUtxoSet derives the Byron and Shelley genesis UTxOs, the
// reserves they leave of the maximum supply, and the synthetic genesis block
// whose byte offsets locate each output.
func buildGenesisUtxoSet(
	nodeCfg *cardano.CardanoNodeConfig,
	genesisHash [32]byte,
	logger *slog.Logger,
) (*genesisUtxoSet, error) {
	byronGenesisUtxos, err := nodeCfg.ByronGenesis().GenesisUtxos()
	if err != nil {
		return nil, fmt.Errorf("generate Byron genesis UTxOs: %w", err)
	}
	// The reference Byron to Shelley UTxO translation drops every output
	// whose coin is zero, so the Shelley ledger state, and the state a
	// Mithril snapshot imports, never holds them.
	_, byronGenesisUtxos = splitZeroValueByron(byronGenesisUtxos)
	shelleyGenesis := nodeCfg.ShelleyGenesis()
	shelleyGenesisUtxos, err := shelleyGenesis.GenesisUtxos()
	if err != nil {
		return nil, fmt.Errorf("generate Shelley genesis UTxOs: %w", err)
	}
	if len(byronGenesisUtxos)+len(shelleyGenesisUtxos) == 0 {
		return nil, errors.New("failed to generate genesis UTxOs")
	}
	if logger != nil {
		logger.Info(
			fmt.Sprintf("creating %d genesis UTxOs (%d Byron, %d Shelley)",
				len(byronGenesisUtxos)+len(shelleyGenesisUtxos),
				len(byronGenesisUtxos),
				len(shelleyGenesisUtxos),
			),
			"component", "ledger",
		)
	}
	genesisUtxos := slices.Concat(byronGenesisUtxos, shelleyGenesisUtxos)
	if err := rejectDuplicateGenesisUtxos(genesisUtxos); err != nil {
		return nil, fmt.Errorf("validate genesis UTxOs: %w", err)
	}
	reserves, err := genesisReserveBalance(
		shelleyGenesis.MaxLovelaceSupply, genesisUtxos,
	)
	if err != nil {
		return nil, fmt.Errorf("calculate genesis reserves: %w", err)
	}
	txUtxos := make(map[[32]byte][]lcommon.Utxo)
	for i := range genesisUtxos {
		var txHashArray [32]byte
		copy(txHashArray[:], genesisUtxos[i].Id.Id().Bytes())
		// Genesis outputs have no original CBOR, so encode them.
		cborData, err := cbor.Encode(genesisUtxos[i].Output)
		if err != nil {
			return nil, fmt.Errorf(
				"encode genesis UTxO output to CBOR: %w",
				err,
			)
		}
		var newOutput lcommon.TransactionOutput
		switch output := genesisUtxos[i].Output.(type) {
		case byron.ByronTransactionOutput:
			newByronOutput := output
			(&newByronOutput).SetCbor(cborData)
			newOutput = newByronOutput
		case shelley.ShelleyTransactionOutput:
			newShelleyOutput := output
			(&newShelleyOutput).SetCbor(cborData)
			newOutput = newShelleyOutput
		default:
			return nil, fmt.Errorf(
				"unsupported genesis UTxO output type: %T", genesisUtxos[i].Output,
			)
		}
		txUtxos[txHashArray] = append(txUtxos[txHashArray], lcommon.Utxo{
			Id:     genesisUtxos[i].Id,
			Output: newOutput,
		})
	}
	// The block is an array of {0: tx_hash, 1: [output, ...]} entries. Sorted
	// transaction hashes keep the layout, and so the offsets, deterministic.
	utxoOffsets := make(map[database.UtxoRef]database.CborOffset)
	txHashes := make([][32]byte, 0, len(txUtxos))
	for txHash := range txUtxos {
		txHashes = append(txHashes, txHash)
	}
	slices.SortFunc(txHashes, func(a, b [32]byte) int {
		return bytes.Compare(a[:], b[:])
	})
	blockCbor, err := buildGenesisBlockCbor(
		txHashes, txUtxos, utxoOffsets, genesisHash,
	)
	if err != nil {
		return nil, fmt.Errorf("build genesis block cbor: %w", err)
	}
	return &genesisUtxoSet{
		txUtxos: txUtxos, offsets: utxoOffsets, utxos: genesisUtxos,
		blockCbor: blockCbor, reserves: reserves,
	}, nil
}

// SeedIndexerGenesis writes the from-genesis state block replay does not
// carry into db: the genesis transactions with their UTxOs, the genesis
// pools, delegations and governance, the genesis constitution and the
// genesis committee. It is the part of the ledger's own genesis creation that
// lives in the metadata store, so an IndexerDB filled from the first block
// holds the rows the ledger's store holds. The slot-0 network state and ADA
// pots are ledger facts and are copied with the others.
//
// Call it only for a chain that starts at genesis: a Mithril-bootstrapped
// chain already reflects the genesis outputs' current status.
func SeedIndexerGenesis(
	ctx context.Context,
	nodeCfg *cardano.CardanoNodeConfig,
	db *database.Database,
	logger *slog.Logger,
	txn *database.Txn,
) error {
	if logger == nil {
		logger = slog.Default()
	}
	genesisHash, err := GenesisBlockHash(nodeCfg)
	if err != nil {
		return fmt.Errorf("get genesis block hash: %w", err)
	}
	set, err := buildGenesisUtxoSet(nodeCfg, genesisHash, logger)
	if err != nil {
		return err
	}
	// Transactions go in by hash order so the surrogate ids come out the
	// same on every store seeded this way.
	hashes := make([][32]byte, 0, len(set.txUtxos))
	for h := range set.txUtxos {
		hashes = append(hashes, h)
	}
	slices.SortFunc(
		hashes,
		func(a, b [32]byte) int { return bytes.Compare(a[:], b[:]) },
	)
	for _, h := range hashes {
		if err := db.SetGenesisTransaction(
			ctx, h[:], genesisHash[:], set.txUtxos[h], set.offsets, txn,
		); err != nil {
			return fmt.Errorf("set genesis transaction %x: %w", h[:8], err)
		}
	}
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeCfg,
			Logger:            logger,
		},
	}
	shelleyGenesis := nodeCfg.ShelleyGenesis()
	pools, delegators, err := initialPools(shelleyGenesis)
	if err != nil {
		return fmt.Errorf("parse genesis staking: %w", err)
	}
	stake, err := genesisStakeDelegations(delegators)
	if err != nil {
		return fmt.Errorf("parse genesis stake delegations: %w", err)
	}
	if len(pools) > 0 || len(stake) > 0 {
		if err := db.SetGenesisStakingWithDeposits(
			pools, stake,
			uint64(shelleyGenesis.ProtocolParameters.KeyDeposit),
			uint64(shelleyGenesis.ProtocolParameters.PoolDeposit),
			genesisHash[:], txn,
		); err != nil {
			return fmt.Errorf("set genesis staking: %w", err)
		}
	}
	if conway := nodeCfg.ConwayGenesis(); conway != nil &&
		(len(conway.InitialDReps) > 0 || len(conway.Delegs) > 0) {
		if err := db.SetGenesisGovernance(
			conway.InitialDReps, conway.Delegs, genesisHash[:], txn,
		); err != nil {
			return fmt.Errorf("set genesis governance: %w", err)
		}
	}
	if err := ls.ensureGenesisConstitution(txn); err != nil {
		return err
	}
	return ls.ensureGenesisCommittee(ctx, txn)
}
