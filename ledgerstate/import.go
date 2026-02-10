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
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// EpochLengthFunc resolves the slot length (ms) and epoch length
// (in slots) for a given era ID using the Cardano node config.
// Returns (0, 0, err) if the era is unknown or the config does
// not contain the needed genesis parameters.
type EpochLengthFunc func(eraId uint) (
	slotLength, epochLength uint, err error,
)

// ImportConfig holds configuration for the ledger state import.
type ImportConfig struct {
	Database   *database.Database
	State      *RawLedgerState
	Logger     *slog.Logger
	OnProgress func(ImportProgress)
	// EpochLength resolves era parameters from the node config.
	// If nil, epoch generation falls back to computing from
	// era bounds.
	EpochLength EpochLengthFunc
	// ImportKey identifies this import for resume tracking.
	// Format: "{digest}:{slot}". If empty, resume is disabled.
	ImportKey string
}

// ImportLedgerState orchestrates the full import of parsed ledger
// state data into Dingo's metadata store. If ImportKey is set,
// completed phases are checkpointed so a failed import can resume
// from the last successful phase.
func ImportLedgerState(
	ctx context.Context,
	cfg ImportConfig,
) error {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Database == nil {
		return errors.New("database is nil")
	}
	if cfg.State == nil {
		return errors.New("ledger state is nil")
	}

	progress := func(p ImportProgress) {
		if cfg.OnProgress != nil {
			cfg.OnProgress(p)
		}
	}

	if cfg.State.Tip == nil {
		return errors.New("snapshot tip is nil")
	}
	slot := cfg.State.Tip.Slot

	// Check for existing checkpoint to enable resume
	completedPhase := ""
	if cfg.ImportKey != "" {
		cp, err := cfg.Database.Metadata().GetImportCheckpoint(
			cfg.ImportKey, nil,
		)
		if err != nil {
			cfg.Logger.Warn(
				"failed to read import checkpoint, "+
					"starting from scratch",
				"component", "ledgerstate",
				"error", err,
			)
		} else if cp != nil {
			completedPhase = cp.Phase
			cfg.Logger.Info(
				"resuming import from checkpoint",
				"component", "ledgerstate",
				"completed_phase", completedPhase,
				"import_key", cfg.ImportKey,
			)
		}
	}

	cfg.Logger.Info(
		"starting ledger state import",
		"component", "ledgerstate",
		"epoch", cfg.State.Epoch,
		"era", EraName(cfg.State.EraIndex),
		"slot", slot,
	)

	// Import UTxOs (from UTxO table file or inline data)
	if !models.IsPhaseCompleted(
		completedPhase,
		models.ImportPhaseUTxO,
	) {
		if cfg.State.UTxOTablePath != "" ||
			cfg.State.UTxOData != nil {
			if err := importUTxOs(
				ctx, cfg, slot, progress,
			); err != nil {
				return fmt.Errorf("importing UTxOs: %w", err)
			}
		}
		if err := setCheckpoint(
			cfg, models.ImportPhaseUTxO,
		); err != nil {
			return err
		}
	} else {
		cfg.Logger.Info(
			"skipping UTxO import (already completed)",
			"component", "ledgerstate",
		)
	}

	// Import cert state (accounts, pools, DReps)
	if !models.IsPhaseCompleted(
		completedPhase,
		models.ImportPhaseCertState,
	) {
		if cfg.State.CertStateData != nil {
			if err := importCertState(
				ctx, cfg, slot, progress,
			); err != nil {
				return fmt.Errorf(
					"importing cert state: %w",
					err,
				)
			}
		}
		if err := setCheckpoint(
			cfg, models.ImportPhaseCertState,
		); err != nil {
			return err
		}
	} else {
		cfg.Logger.Info(
			"skipping cert state import (already completed)",
			"component", "ledgerstate",
		)
	}

	// Import stake snapshots
	if !models.IsPhaseCompleted(
		completedPhase,
		models.ImportPhaseSnapshots,
	) {
		if cfg.State.SnapShotsData != nil {
			if err := importSnapShots(
				ctx, cfg, slot, progress,
			); err != nil {
				return fmt.Errorf(
					"importing stake snapshots: %w",
					err,
				)
			}
		}
		if err := setCheckpoint(
			cfg, models.ImportPhaseSnapshots,
		); err != nil {
			return err
		}
	} else {
		cfg.Logger.Info(
			"skipping snapshot import (already completed)",
			"component", "ledgerstate",
		)
	}

	// Import protocol parameters
	if !models.IsPhaseCompleted(
		completedPhase,
		models.ImportPhasePParams,
	) {
		if err := importPParams(cfg); err != nil {
			return fmt.Errorf(
				"importing protocol parameters: %w",
				err,
			)
		}
		if err := setCheckpoint(
			cfg, models.ImportPhasePParams,
		); err != nil {
			return err
		}
	} else {
		cfg.Logger.Info(
			"skipping pparams import (already completed)",
			"component", "ledgerstate",
		)
	}

	// Import governance state (constitution, committee, proposals)
	if !models.IsPhaseCompleted(
		completedPhase,
		models.ImportPhaseGovState,
	) {
		if cfg.State.GovStateData != nil &&
			cfg.State.EraIndex >= EraConway {
			if err := importGovState(
				ctx, cfg, slot, progress,
			); err != nil {
				return fmt.Errorf(
					"importing governance state: %w",
					err,
				)
			}
		}
		if err := setCheckpoint(
			cfg, models.ImportPhaseGovState,
		); err != nil {
			return err
		}
	} else {
		cfg.Logger.Info(
			"skipping governance state import "+
				"(already completed)",
			"component", "ledgerstate",
		)
	}

	// Set the chain tip
	if !models.IsPhaseCompleted(
		completedPhase,
		models.ImportPhaseTip,
	) {
		if err := importTip(ctx, cfg); err != nil {
			return fmt.Errorf("setting tip: %w", err)
		}
		if err := setCheckpoint(
			cfg, models.ImportPhaseTip,
		); err != nil {
			return err
		}
	}

	cfg.Logger.Info(
		"ledger state import complete",
		"component", "ledgerstate",
		"epoch", cfg.State.Epoch,
		"slot", slot,
	)

	return nil
}

// setCheckpoint persists the completed phase if resume tracking
// is enabled (ImportKey is set).
func setCheckpoint(cfg ImportConfig, phase string) error {
	if cfg.ImportKey == "" {
		return nil
	}
	store := cfg.Database.Metadata()
	txn := cfg.Database.MetadataTxn(true)
	defer txn.Release()

	err := store.SetImportCheckpoint(
		&models.ImportCheckpoint{
			ImportKey: cfg.ImportKey,
			Phase:     phase,
		},
		txn.Metadata(),
	)
	if err != nil {
		return fmt.Errorf(
			"saving checkpoint for phase %q: %w",
			phase,
			err,
		)
	}
	if err := txn.Commit(); err != nil {
		return fmt.Errorf(
			"committing checkpoint for phase %q: %w",
			phase,
			err,
		)
	}
	cfg.Logger.Info(
		"checkpoint saved",
		"component", "ledgerstate",
		"phase", phase,
		"import_key", cfg.ImportKey,
	)
	return nil
}

// importUTxOs streams the UTxO map and imports in batches. Supports
// both inline UTxO data and file-based UTxO-HD tvar format.
func importUTxOs(
	ctx context.Context,
	cfg ImportConfig,
	slot uint64,
	progress func(ImportProgress),
) error {
	source := "inline"
	if cfg.State.UTxOTablePath != "" {
		source = cfg.State.UTxOTablePath
	}
	cfg.Logger.Info(
		"importing UTxOs from ledger state",
		"component", "ledgerstate",
		"source", source,
	)

	store := cfg.Database.Metadata()
	totalImported := 0

	batchCallback := func(batch []ParsedUTxO) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		utxos := make([]models.Utxo, 0, len(batch))
		for i := range batch {
			utxos = append(
				utxos,
				UTxOToModel(&batch[i], slot),
			)
		}

		txn := cfg.Database.MetadataTxn(true)
		defer txn.Release()

		if err := store.ImportUtxos(
			utxos, txn.Metadata(),
		); err != nil {
			return fmt.Errorf(
				"inserting UTxO batch: %w",
				err,
			)
		}

		if err := txn.Commit(); err != nil {
			return fmt.Errorf(
				"committing UTxO batch: %w",
				err,
			)
		}

		prevTotal := totalImported
		totalImported += len(batch)

		if totalImported/100000 > prevTotal/100000 {
			cfg.Logger.Info(
				"UTxO import progress",
				"component", "ledgerstate",
				"count", totalImported,
			)
		}

		progress(ImportProgress{
			Stage:   "utxo",
			Current: totalImported,
			Description: fmt.Sprintf(
				"%d UTxOs imported",
				totalImported,
			),
		})

		return nil
	}

	var err error
	if cfg.State.UTxOTablePath != "" {
		// UTxO-HD format: stream from tvar file
		_, err = ParseUTxOsFromFile(
			cfg.State.UTxOTablePath,
			batchCallback,
		)
		if err != nil {
			return fmt.Errorf(
				"parsing UTxOs from file %s: %w",
				cfg.State.UTxOTablePath, err,
			)
		}
	} else {
		// Legacy format: decode from inline CBOR
		_, err = ParseUTxOsStreaming(
			cfg.State.UTxOData,
			batchCallback,
		)
		if err != nil {
			return fmt.Errorf(
				"parsing inline UTxOs: %w", err,
			)
		}
	}

	cfg.Logger.Info(
		"finished importing UTxOs",
		"component", "ledgerstate",
		"count", totalImported,
	)

	return nil
}

// importCertState imports accounts, pools, and DReps from the
// cert state data.
func importCertState(
	ctx context.Context,
	cfg ImportConfig,
	slot uint64,
	progress func(ImportProgress),
) error {
	cfg.Logger.Info(
		"parsing cert state",
		"component", "ledgerstate",
	)

	certState, err := ParseCertState(cfg.State.CertStateData)
	if err != nil {
		if certState == nil {
			return fmt.Errorf(
				"parsing cert state: %w", err,
			)
		}
		cfg.Logger.Warn(
			"cert state parse warnings",
			"component", "ledgerstate",
			"warning", err.Error(),
		)
	}

	// Import accounts
	if len(certState.Accounts) > 0 {
		if err := importAccounts(
			ctx, cfg, certState.Accounts, slot,
		); err != nil {
			return fmt.Errorf(
				"importing accounts: %w",
				err,
			)
		}
		progress(ImportProgress{
			Stage:   "accounts",
			Current: len(certState.Accounts),
			Total:   len(certState.Accounts),
			Description: fmt.Sprintf(
				"%d accounts imported",
				len(certState.Accounts),
			),
		})
	}

	// Import pools
	if len(certState.Pools) > 0 {
		if err := importPools(
			ctx, cfg, certState.Pools, slot,
		); err != nil {
			return fmt.Errorf("importing pools: %w", err)
		}
		progress(ImportProgress{
			Stage:   "pools",
			Current: len(certState.Pools),
			Total:   len(certState.Pools),
			Description: fmt.Sprintf(
				"%d pools imported",
				len(certState.Pools),
			),
		})
	}

	// Import DReps
	if len(certState.DReps) > 0 {
		if err := importDReps(
			ctx, cfg, certState.DReps, slot,
		); err != nil {
			return fmt.Errorf("importing DReps: %w", err)
		}
		progress(ImportProgress{
			Stage:   "dreps",
			Current: len(certState.DReps),
			Total:   len(certState.DReps),
			Description: fmt.Sprintf(
				"%d DReps imported",
				len(certState.DReps),
			),
		})
	}

	return nil
}

// importAccounts imports parsed accounts into the metadata store.
func importAccounts(
	ctx context.Context,
	cfg ImportConfig,
	accounts []ParsedAccount,
	slot uint64,
) error {
	cfg.Logger.Info(
		"importing accounts",
		"component", "ledgerstate",
		"count", len(accounts),
	)

	const accountBatchSize = 10000

	store := cfg.Database.Metadata()
	txn := cfg.Database.MetadataTxn(true)
	defer func() { txn.Release() }()

	inBatch := 0
	for _, acct := range accounts {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		model := &models.Account{
			StakingKey: acct.StakingKey,
			Pool:       acct.PoolKeyHash,
			Drep:       acct.DRepCred,
			AddedSlot:  slot,
			Reward:     types.Uint64(acct.Reward),
			Active:     acct.Active,
		}

		if err := store.ImportAccount(
			model, txn.Metadata(),
		); err != nil {
			return fmt.Errorf(
				"importing account %x: %w",
				acct.StakingKey,
				err,
			)
		}

		inBatch++
		if inBatch >= accountBatchSize {
			if err := txn.Commit(); err != nil {
				return fmt.Errorf(
					"committing account batch: %w",
					err,
				)
			}
			inBatch = 0
			txn.Release()
			txn = cfg.Database.MetadataTxn(true)
		}
	}

	if inBatch > 0 {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf(
				"committing account batch: %w",
				err,
			)
		}
	}
	return nil
}

// importPools imports parsed pools into the metadata store.
func importPools(
	ctx context.Context,
	cfg ImportConfig,
	pools []ParsedPool,
	slot uint64,
) error {
	cfg.Logger.Info(
		"importing pools",
		"component", "ledgerstate",
		"count", len(pools),
	)

	const poolBatchSize = 5000

	store := cfg.Database.Metadata()
	txn := cfg.Database.MetadataTxn(true)
	defer func() { txn.Release() }()

	inBatch := 0
	for _, pool := range pools {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var margin *types.Rat
		if pool.MarginDen > 0 {
			r := new(big.Rat).SetFrac(
				new(big.Int).SetUint64(pool.MarginNum),
				new(big.Int).SetUint64(pool.MarginDen),
			)
			margin = &types.Rat{Rat: r}
		}

		model := &models.Pool{
			PoolKeyHash:   pool.PoolKeyHash,
			VrfKeyHash:    pool.VrfKeyHash,
			Pledge:        types.Uint64(pool.Pledge),
			Cost:          types.Uint64(pool.Cost),
			Margin:        margin,
			RewardAccount: pool.RewardAccount,
		}

		var owners []models.PoolRegistrationOwner
		for _, owner := range pool.Owners {
			owners = append(
				owners,
				models.PoolRegistrationOwner{KeyHash: owner},
			)
		}

		var relays []models.PoolRegistrationRelay
		for _, r := range pool.Relays {
			relay := models.PoolRegistrationRelay{
				Port:     uint(r.Port),
				Hostname: r.Hostname,
			}
			if r.IPv4 != nil {
				ip := net.IP(r.IPv4)
				relay.Ipv4 = &ip
			}
			if r.IPv6 != nil {
				ip := net.IP(r.IPv6)
				relay.Ipv6 = &ip
			}
			relays = append(relays, relay)
		}

		reg := &models.PoolRegistration{
			PoolKeyHash:   pool.PoolKeyHash,
			VrfKeyHash:    pool.VrfKeyHash,
			Pledge:        types.Uint64(pool.Pledge),
			Cost:          types.Uint64(pool.Cost),
			Margin:        margin,
			RewardAccount: pool.RewardAccount,
			AddedSlot:     slot,
			DepositAmount: types.Uint64(pool.Deposit),
			Owners:        owners,
			Relays:        relays,
			MetadataUrl:   pool.MetadataUrl,
			MetadataHash:  pool.MetadataHash,
		}

		if err := store.ImportPool(
			model, reg, txn.Metadata(),
		); err != nil {
			return fmt.Errorf(
				"importing pool %x: %w",
				pool.PoolKeyHash,
				err,
			)
		}

		inBatch++
		if inBatch >= poolBatchSize {
			if err := txn.Commit(); err != nil {
				return fmt.Errorf(
					"committing pool batch: %w",
					err,
				)
			}
			inBatch = 0
			txn.Release()
			txn = cfg.Database.MetadataTxn(true)
		}
	}

	if inBatch > 0 {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf(
				"committing pool batch: %w",
				err,
			)
		}
	}
	return nil
}

// importDReps imports parsed DReps into the metadata store.
func importDReps(
	ctx context.Context,
	cfg ImportConfig,
	dreps []ParsedDRep,
	slot uint64,
) error {
	cfg.Logger.Info(
		"importing DReps",
		"component", "ledgerstate",
		"count", len(dreps),
	)

	const drepBatchSize = 10000

	store := cfg.Database.Metadata()
	txn := cfg.Database.MetadataTxn(true)
	defer func() { txn.Release() }()

	inBatch := 0
	for _, drep := range dreps {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		model := &models.Drep{
			Credential: drep.Credential,
			AnchorUrl:  drep.AnchorUrl,
			AnchorHash: drep.AnchorHash,
			AddedSlot:  slot,
			Active:     drep.Active,
		}

		reg := &models.RegistrationDrep{
			DrepCredential: drep.Credential,
			AnchorUrl:      drep.AnchorUrl,
			AnchorHash:     drep.AnchorHash,
			AddedSlot:      slot,
			DepositAmount:  types.Uint64(drep.Deposit),
		}

		if err := store.ImportDrep(
			model, reg, txn.Metadata(),
		); err != nil {
			return fmt.Errorf(
				"importing DRep %x: %w",
				drep.Credential,
				err,
			)
		}

		inBatch++
		if inBatch >= drepBatchSize {
			if err := txn.Commit(); err != nil {
				return fmt.Errorf(
					"committing DRep batch: %w",
					err,
				)
			}
			inBatch = 0
			txn.Release()
			txn = cfg.Database.MetadataTxn(true)
		}
	}

	if inBatch > 0 {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf(
				"committing DRep batch: %w",
				err,
			)
		}
	}
	return nil
}

// importSnapShots imports the mark/set/go stake snapshots.
func importSnapShots(
	ctx context.Context,
	cfg ImportConfig,
	slot uint64,
	progress func(ImportProgress),
) error {
	cfg.Logger.Info(
		"parsing stake snapshots",
		"component", "ledgerstate",
	)

	snapshots, err := ParseSnapShots(cfg.State.SnapShotsData)
	if err != nil {
		if snapshots == nil {
			return fmt.Errorf(
				"parsing stake snapshots: %w",
				err,
			)
		}
		// Non-fatal: some entries skipped during parsing
		cfg.Logger.Warn(
			"stake snapshot parse warnings",
			"component", "ledgerstate",
			"warning", err.Error(),
		)
	}

	store := cfg.Database.Metadata()
	epoch := cfg.State.Epoch

	// Import each snapshot type, saving the "go" result for the
	// epoch summary to avoid recomputing it.
	var goSnapshots []*models.PoolStakeSnapshot
	for _, st := range []struct {
		name string
		snap *ParsedSnapShot
	}{
		{"mark", &snapshots.Mark},
		{"set", &snapshots.Set},
		{"go", &snapshots.Go},
	} {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		poolSnapshots := AggregatePoolStake(
			st.snap, epoch, st.name, slot,
		)

		if st.name == "go" {
			goSnapshots = poolSnapshots
		}

		if len(poolSnapshots) == 0 {
			continue
		}

		if err := func() error {
			txn := cfg.Database.MetadataTxn(true)
			defer txn.Release()
			metaTxn := txn.Metadata()

			if err := store.DeletePoolStakeSnapshotsForEpoch(
				epoch, st.name, metaTxn,
			); err != nil {
				return fmt.Errorf(
					"deleting existing %s snapshots: %w",
					st.name,
					err,
				)
			}

			if err := store.SavePoolStakeSnapshots(
				poolSnapshots, metaTxn,
			); err != nil {
				return fmt.Errorf(
					"saving %s snapshots: %w",
					st.name,
					err,
				)
			}

			if err := txn.Commit(); err != nil {
				return fmt.Errorf(
					"commit transaction: %w",
					err,
				)
			}
			return nil
		}(); err != nil {
			return fmt.Errorf(
				"importing %s snapshots: %w",
				st.name,
				err,
			)
		}

		var totalStake uint64
		for _, ps := range poolSnapshots {
			totalStake += uint64(ps.TotalStake)
		}

		cfg.Logger.Info(
			"imported stake snapshot",
			"snapshot", st.name,
			"pools", len(poolSnapshots),
			"total_stake", totalStake,
			"component", "ledgerstate",
		)

		progress(ImportProgress{
			Stage:   "snapshots",
			Current: len(poolSnapshots),
			Description: fmt.Sprintf(
				"%s: %d pools",
				st.name,
				len(poolSnapshots),
			),
		})
	}

	// Save epoch summary using the "go" snapshot computed above
	var totalStake uint64
	var totalDelegators uint64
	for _, ps := range goSnapshots {
		totalStake += uint64(ps.TotalStake)
		totalDelegators += ps.DelegatorCount
	}

	summary := &models.EpochSummary{
		Epoch:            epoch,
		TotalActiveStake: types.Uint64(totalStake),
		TotalPoolCount:   uint64(len(goSnapshots)),
		TotalDelegators:  totalDelegators,
		BoundarySlot:     slot,
		SnapshotReady:    true,
	}

	txn := cfg.Database.MetadataTxn(true)
	defer txn.Release()

	// Check if epoch summary already exists (idempotent import)
	existing, err := store.GetEpochSummary(
		epoch, txn.Metadata(),
	)
	if err != nil {
		return fmt.Errorf(
			"checking existing epoch summary: %w",
			err,
		)
	}
	if existing != nil {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf(
				"commit transaction in importSnapShots: %w",
				err,
			)
		}
		return nil
	}

	if err := store.SaveEpochSummary(
		summary, txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"saving epoch summary: %w",
			err,
		)
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf(
			"commit transaction in importSnapShots: %w",
			err,
		)
	}
	return nil
}

// importTip sets the chain tip to the snapshot's tip and generates
// the full epoch history from genesis using era boundaries.
func importTip(ctx context.Context, cfg ImportConfig) error {
	tip := cfg.State.Tip
	if tip == nil {
		return errors.New("snapshot tip is nil")
	}

	cfg.Logger.Info(
		"setting chain tip from ledger state",
		"component", "ledgerstate",
		"slot", tip.Slot,
		"hash", hex.EncodeToString(tip.BlockHash),
	)

	store := cfg.Database.Metadata()
	txn := cfg.Database.MetadataTxn(true)
	defer txn.Release()

	oTip := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: tip.Slot,
			Hash: tip.BlockHash,
		},
		BlockNumber: 0, // Not available from snapshot
	}

	if err := store.SetTip(oTip, txn.Metadata()); err != nil {
		return fmt.Errorf("setting tip: %w", err)
	}

	// Store treasury and reserves balances
	if err := store.SetNetworkState(
		cfg.State.Treasury,
		cfg.State.Reserves,
		tip.Slot,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf("setting network state: %w", err)
	}

	// Generate epoch history from era bounds
	epochCount, err := generateAndSaveEpochs(ctx, cfg, txn)
	if err != nil {
		return fmt.Errorf("generating epoch history: %w", err)
	}

	cfg.Logger.Info(
		"generated epoch history",
		"component", "ledgerstate",
		"epoch_count", epochCount,
	)

	if err = txn.Commit(); err != nil {
		return fmt.Errorf(
			"commit transaction in importTip: %w",
			err,
		)
	}
	return nil
}

// generateAndSaveEpochs creates epoch records for every epoch from
// genesis to the current epoch using the era boundaries extracted
// from the telescope. This is critical for the slot clock to
// correctly map between slots and wall-clock times.
func generateAndSaveEpochs(
	ctx context.Context,
	cfg ImportConfig,
	txn *database.Txn,
) (int, error) {
	store := cfg.Database.Metadata()
	metaTxn := txn.Metadata()
	totalEpochs := 0

	// Check if the full epoch history already exists
	// (idempotent import). If the current epoch is present,
	// assume all epochs have been generated.
	existing, err := store.GetEpoch(
		cfg.State.Epoch, metaTxn,
	)
	if err != nil {
		return 0, fmt.Errorf(
			"checking existing epoch: %w",
			err,
		)
	}
	if existing != nil {
		cfg.Logger.Info(
			"epoch history already exists, skipping",
			"component", "ledgerstate",
			"epoch", cfg.State.Epoch,
		)
		return 0, nil
	}

	// A prior failed import may have left partial epoch
	// data. The database SetEpoch uses OnConflict clauses
	// (DoNothing for SQLite, DoUpdates for PostgreSQL/MySQL)
	// so duplicate epoch_id records are handled safely.

	bounds := cfg.State.EraBounds
	if cfg.State.EraBoundsWarning != nil {
		cfg.Logger.Warn(
			"era bounds extraction failed; "+
				"falling back to single-epoch",
			"component", "ledgerstate",
			"error", cfg.State.EraBoundsWarning,
		)
	}
	if len(bounds) == 0 {
		// Fallback: no era bounds available, just save
		// the current epoch as before.
		var slotLength, lengthInSlots uint
		if cfg.EpochLength != nil {
			// #nosec G115
			sl, el, err := cfg.EpochLength(
				uint(cfg.State.EraIndex),
			)
			if err == nil {
				slotLength = sl
				lengthInSlots = el
			}
		}
		epochStartSlot := cfg.State.EraBoundSlot
		if lengthInSlots > 0 &&
			cfg.State.Epoch >= cfg.State.EraBoundEpoch {
			epochsSinceBound := cfg.State.Epoch -
				cfg.State.EraBoundEpoch
			epochStartSlot = cfg.State.EraBoundSlot +
				epochsSinceBound*uint64(lengthInSlots)
		}
		// #nosec G115
		if err := store.SetEpoch(
			epochStartSlot,
			cfg.State.Epoch,
			cfg.State.EpochNonce,
			uint(cfg.State.EraIndex),
			slotLength,
			lengthInSlots,
			metaTxn,
		); err != nil {
			return 0, fmt.Errorf("setting epoch: %w", err)
		}
		return 1, nil
	}

	// Generate epochs for each era defined by consecutive bounds.
	for i := 0; i < len(bounds)-1; i++ {
		if err := ctx.Err(); err != nil {
			return totalEpochs, fmt.Errorf(
				"cancelled: %w", err,
			)
		}
		startBound := bounds[i]
		endBound := bounds[i+1]

		// Skip eras with no epochs (e.g. preview where
		// multiple eras start at epoch 0).
		if startBound.Epoch == endBound.Epoch {
			continue
		}

		// #nosec G115
		eraId := uint(i)
		slotLength, epochLength := resolveEraParams(
			cfg, eraId, startBound, endBound,
		)
		if epochLength == 0 {
			cfg.Logger.Warn(
				"skipping epoch generation for era "+
					"with unknown epoch length",
				"era", EraName(int(eraId)), //nolint:gosec
				"component", "ledgerstate",
			)
			continue
		}

		for e := startBound.Epoch; e < endBound.Epoch; e++ {
			startSlot := startBound.Slot +
				(e-startBound.Epoch)*uint64(epochLength)
			if err := store.SetEpoch(
				startSlot,
				e,
				nil, // historical epochs don't need nonce
				eraId,
				slotLength,
				epochLength,
				metaTxn,
			); err != nil {
				// #nosec G115
				return totalEpochs, fmt.Errorf(
					"setting epoch %d (era %s): %w",
					e, EraName(int(eraId)), err,
				)
			}
			totalEpochs++
		}

		// #nosec G115
		cfg.Logger.Debug(
			"generated epochs for era",
			"era", EraName(int(eraId)),
			"start_epoch", startBound.Epoch,
			"end_epoch", endBound.Epoch-1,
			"component", "ledgerstate",
		)
	}

	// Generate epochs for the current (last) era: from the last
	// bound up to and including the current epoch.
	lastBound := bounds[len(bounds)-1]
	// #nosec G115
	currentEraId := uint(len(bounds) - 1)

	slotLength, epochLength := resolveEraParams(
		cfg, currentEraId, lastBound, EraBound{},
	)
	if epochLength == 0 {
		cfg.Logger.Warn(
			"skipping epoch generation for current era "+
				"with unknown epoch length",
			"era", EraName(int(currentEraId)), //nolint:gosec
			"component", "ledgerstate",
		)
		return totalEpochs, nil
	}

	for e := lastBound.Epoch; e <= cfg.State.Epoch; e++ {
		if err := ctx.Err(); err != nil {
			return totalEpochs, fmt.Errorf(
				"cancelled: %w", err,
			)
		}
		startSlot := lastBound.Slot +
			(e-lastBound.Epoch)*uint64(epochLength)

		var nonce []byte
		if e == cfg.State.Epoch {
			nonce = cfg.State.EpochNonce
		}

		if err := store.SetEpoch(
			startSlot,
			e,
			nonce,
			currentEraId,
			slotLength,
			epochLength,
			metaTxn,
		); err != nil {
			// #nosec G115
			return totalEpochs, fmt.Errorf(
				"setting epoch %d (era %s): %w",
				e, EraName(int(currentEraId)), err,
			)
		}
		totalEpochs++
	}

	// #nosec G115
	cfg.Logger.Debug(
		"generated epochs for current era",
		"era", EraName(int(currentEraId)),
		"start_epoch", lastBound.Epoch,
		"end_epoch", cfg.State.Epoch,
		"component", "ledgerstate",
	)

	return totalEpochs, nil
}

// resolveEraParams returns the slot length (ms) and epoch length
// (in slots) for an era. It tries the genesis config first, then
// falls back to computing from era bounds.
func resolveEraParams(
	cfg ImportConfig,
	eraId uint,
	startBound, endBound EraBound,
) (slotLength, epochLength uint) {
	if cfg.EpochLength != nil {
		sl, el, err := cfg.EpochLength(eraId)
		if err == nil {
			return sl, el
		}
	}

	// Fallback: compute from bounds if we have both start and end.
	if endBound.Epoch <= startBound.Epoch {
		cfg.Logger.Warn(
			"cannot compute epoch length from era bounds "+
				"(no valid end bound)",
			"component", "ledgerstate",
			"era_id", eraId,
			"start_epoch", startBound.Epoch,
			"end_epoch", endBound.Epoch,
		)
		return 0, 0
	}
	if endBound.Slot < startBound.Slot {
		cfg.Logger.Warn(
			"cannot compute epoch length from era bounds "+
				"(end slot before start slot)",
			"component", "ledgerstate",
			"era_id", eraId,
			"start_slot", startBound.Slot,
			"end_slot", endBound.Slot,
		)
		return 0, 0
	}
	epochSpan := endBound.Epoch - startBound.Epoch
	slotSpan := endBound.Slot - startBound.Slot
	epochLength = uint(slotSpan / epochSpan)
	cfg.Logger.Warn(
		"slot length unavailable from era bounds fallback; "+
			"slot-to-wall-clock-time mapping may be inaccurate",
		"component", "ledgerstate",
		"era_id", eraId,
		"epoch_length", epochLength,
	)
	return 0, epochLength
}

// importPParams decodes and stores protocol parameters from the
// snapshot.
func importPParams(cfg ImportConfig) error {
	if cfg.State.PParamsData == nil {
		return nil
	}

	cfg.Logger.Info(
		"importing protocol parameters",
		"component", "ledgerstate",
	)

	pparamsCbor := []byte(cfg.State.PParamsData)

	store := cfg.Database.Metadata()
	txn := cfg.Database.MetadataTxn(true)
	defer txn.Release()

	// #nosec G115
	if err := store.SetPParams(
		pparamsCbor,
		cfg.State.Tip.Slot,
		cfg.State.Epoch,
		uint(cfg.State.EraIndex),
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"storing protocol parameters: %w",
			err,
		)
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf(
			"commit transaction in importPParams: %w",
			err,
		)
	}
	return nil
}

// importGovState imports governance state (constitution and
// proposals) from the snapshot. Committee members are parsed
// for validation but not yet persisted to the metadata store.
func importGovState(
	ctx context.Context,
	cfg ImportConfig,
	slot uint64,
	progress func(ImportProgress),
) error {
	cfg.Logger.Info(
		"parsing governance state",
		"component", "ledgerstate",
	)
	govState, err := ParseGovState(
		cfg.State.GovStateData,
		cfg.State.EraIndex,
	)
	if govState == nil && err != nil {
		return fmt.Errorf(
			"parsing governance state: %w", err,
		)
	}
	if govState == nil {
		return nil
	}
	if err != nil {
		// Non-fatal warnings from committee/proposals parsing
		cfg.Logger.Warn(
			"governance state parsed with warnings",
			"component", "ledgerstate",
			"error", err,
		)
	}

	store := cfg.Database.Metadata()

	// Import constitution
	if govState.Constitution != nil {
		if err := func() error {
			txn := cfg.Database.MetadataTxn(true)
			defer txn.Release()
			if err := store.SetConstitution(
				&models.Constitution{
					AnchorUrl:  govState.Constitution.AnchorUrl,
					AnchorHash: govState.Constitution.AnchorHash,
					PolicyHash: govState.Constitution.PolicyHash,
					AddedSlot:  slot,
				},
				txn.Metadata(),
			); err != nil {
				return fmt.Errorf(
					"importing constitution: %w", err,
				)
			}
			if err := txn.Commit(); err != nil {
				return fmt.Errorf(
					"committing constitution transaction: %w",
					err,
				)
			}
			return nil
		}(); err != nil {
			return fmt.Errorf("saving constitution: %w", err)
		}
		cfg.Logger.Info(
			"imported constitution",
			"component", "ledgerstate",
			"anchor_url", govState.Constitution.AnchorUrl,
		)
	}

	// Log committee members (parsed for validation but not
	// yet persisted — requires committee model support)
	if len(govState.Committee) > 0 {
		cfg.Logger.Info(
			"parsed committee members "+
				"(not yet persisted to metadata store)",
			"component", "ledgerstate",
			"count", len(govState.Committee),
		)
	}

	// Import governance proposals
	if len(govState.Proposals) > 0 {
		if err := func() error {
			txn := cfg.Database.MetadataTxn(true)
			defer txn.Release()
			metaTxn := txn.Metadata()
			for _, prop := range govState.Proposals {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				if err := store.SetGovernanceProposal(
					&models.GovernanceProposal{
						TxHash:        prop.TxHash,
						ActionIndex:   prop.ActionIndex,
						ActionType:    prop.ActionType,
						ProposedEpoch: prop.ProposedIn,
						ExpiresEpoch:  prop.ExpiresAfter,
						Deposit:       prop.Deposit,
						ReturnAddress: prop.ReturnAddr,
						AnchorUrl:     prop.AnchorUrl,
						AnchorHash:    prop.AnchorHash,
						AddedSlot:     slot,
					},
					metaTxn,
				); err != nil {
					return fmt.Errorf(
						"importing governance proposal: %w",
						err,
					)
				}
			}
			if err := txn.Commit(); err != nil {
				return fmt.Errorf(
					"committing governance proposals transaction: %w",
					err,
				)
			}
			return nil
		}(); err != nil {
			return fmt.Errorf(
				"saving governance proposals: %w", err,
			)
		}
		cfg.Logger.Info(
			"imported governance proposals",
			"component", "ledgerstate",
			"count", len(govState.Proposals),
		)
	}

	progress(ImportProgress{
		Stage:       "governance",
		Description: "governance state imported",
	})

	return nil
}
