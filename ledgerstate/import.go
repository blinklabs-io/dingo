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
	"slices"
	"sort"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// SnapshotTip represents the chain tip recorded in the ledger state
// snapshot.
type SnapshotTip struct {
	Slot      uint64
	BlockHash []byte
}

// RawLedgerState holds partially-decoded ledger state data from a
// Cardano node snapshot file. Large sections are kept as
// cbor.RawMessage for deferred, streaming decoding.
type RawLedgerState struct {
	// EraIndex identifies the current era (0=Byron … 6=Conway).
	EraIndex int
	// Epoch is the epoch number at the time of the snapshot.
	Epoch uint64
	// Tip is the chain tip recorded in the snapshot.
	Tip *SnapshotTip
	// Treasury is the treasury balance in lovelace.
	Treasury uint64
	// Reserves is the reserves balance in lovelace.
	Reserves uint64
	// UTxOData is the deferred CBOR for the UTxO map.
	UTxOData cbor.RawMessage
	// CertStateData is the deferred CBOR for [VState, PState, DState].
	CertStateData cbor.RawMessage
	// GovStateData is the deferred CBOR for governance state.
	GovStateData cbor.RawMessage
	// PParamsData is the deferred CBOR for protocol parameters.
	PParamsData cbor.RawMessage
	// SnapShotsData is the deferred CBOR for mark/set/go stake snapshots.
	SnapShotsData cbor.RawMessage
	// EraBounds holds the start boundaries of all eras extracted
	// from the telescope. Each entry gives the (Slot, Epoch) at
	// which that era began. Used to generate the full epoch
	// history needed for SlotToTime/TimeToSlot.
	EraBounds []EraBound
	// EraBoundSlot is the slot number at which the current era
	// started, extracted from the telescope Bound.
	EraBoundSlot uint64
	// EraBoundEpoch is the epoch number at which the current era
	// started, extracted from the telescope Bound.
	EraBoundEpoch uint64
	// EpochNonce is the epoch nonce from the consensus state,
	// used for VRF leader election. It is a 32-byte Blake2b hash,
	// or nil if the nonce is neutral.
	EpochNonce []byte
	// EvolvingNonce is the rolling nonce (eta_v) from the consensus
	// state, updated with each block's VRF output. Stored as the
	// tip block nonce so that subsequent block processing computes
	// correct rolling nonces after a mithril snapshot restore.
	EvolvingNonce []byte
	// CandidateNonce is the Praos candidate nonce as of the imported
	// tip. This is required to correctly continue nonce accumulation
	// from snapshot tip to epoch boundary.
	CandidateNonce []byte
	// LastEpochBlockNonce is the lagged lab nonce from consensus
	// state (used in epoch nonce calculation).
	LastEpochBlockNonce []byte
	// EraBoundsWarning holds a non-fatal error from era bounds
	// extraction. When set, epoch generation falls back to
	// the single-epoch path.
	EraBoundsWarning error
	// UTxOTablePath is the path to the UTxO table file (UTxO-HD
	// format). When set, UTxOs are streamed from this file instead
	// of from UTxOData.
	UTxOTablePath string
	// UTxOHD indicates that the snapshot used the UTxO-HD ledger
	// state wrapper. These snapshots keep the real UTxO set in an
	// external table file and the inline UTxO field is only a
	// placeholder.
	UTxOHD bool
}

// EraBound represents the start boundary of an era in the
// HardFork telescope. Slot is the first slot of the era and
// Epoch is the epoch number at that slot.
type EraBound struct {
	Slot  uint64
	Epoch uint64
}

// ParsedUTxO represents a parsed unspent transaction output ready
// for conversion to Dingo's database model.
type ParsedUTxO struct {
	TxHash      []byte // 32 bytes
	OutputIndex uint32
	Address     []byte // raw address bytes
	PaymentKey  []byte // 28 bytes, extracted from address
	StakingKey  []byte // 28 bytes, extracted from address
	Amount      uint64 // lovelace
	Assets      []ParsedAsset
	DatumHash   []byte // optional
	Datum       []byte // optional inline datum CBOR
	ScriptRef   []byte // optional reference script CBOR
}

// ParsedAsset represents a native asset within a UTxO.
type ParsedAsset struct {
	PolicyId []byte // 28 bytes
	Name     []byte
	Amount   uint64
}

// Credential type constants matching CBOR encoding.
// Types 0 and 1 are standard key/script credentials with a 28-byte hash.
// Types 2 and 3 are pseudo-DRep targets used only in DRep delegation
// (not valid as staking credentials); they carry no hash.
const (
	CredentialTypeKey          uint64 = 0 // Key hash credential
	CredentialTypeScript       uint64 = 1 // Script hash credential
	CredentialTypeAbstain      uint64 = 2 // AlwaysAbstain pseudo-DRep
	CredentialTypeNoConfidence uint64 = 3 // AlwaysNoConfidence pseudo-DRep
)

// Credential pairs a credential type with its hash.
// For types 0 (key) and 1 (script), Hash is a 28-byte Blake2b-224.
// For types 2 (AlwaysAbstain) and 3 (AlwaysNoConfidence), Hash is nil.
type Credential struct {
	Hash []byte
	Type uint64
}

// ParsedCertState holds the parsed delegation, pool, and DRep state.
type ParsedCertState struct {
	Accounts []ParsedAccount
	Pools    []ParsedPool
	DReps    []ParsedDRep
}

// ParsedAccount represents a stake account with its delegation.
type ParsedAccount struct {
	StakingKey  Credential // staking credential (type + hash)
	PoolKeyHash []byte     // 28-byte pool key hash (delegation target)
	DRepCred    Credential // DRep credential (vote delegation)
	Reward      uint64     // reward balance in lovelace
	Deposit     uint64     // deposit in lovelace
	Active      bool
}

// ParsedPool represents a stake pool registration.
type ParsedPool struct {
	PoolKeyHash   []byte // 28 bytes
	VrfKeyHash    []byte // 32 bytes
	Pledge        uint64
	Cost          uint64
	MarginNum     uint64
	MarginDen     uint64
	RewardAccount []byte
	Owners        [][]byte // list of 28-byte key hashes
	Relays        []ParsedRelay
	MetadataUrl   string
	MetadataHash  []byte // 32 bytes
	Deposit       uint64
}

// ParsedRelay represents a pool relay from the stake pool
// registration certificate.
type ParsedRelay struct {
	Type     uint8  // 0=SingleHostAddr, 1=SingleHostName, 2=MultiHostName
	Port     uint16 // 0 if not specified
	IPv4     []byte // 4 bytes, nil if not specified
	IPv6     []byte // 16 bytes, nil if not specified
	Hostname string // for SingleHostName and MultiHostName
}

// ParsedDRep represents a DRep registration.
type ParsedDRep struct {
	Credential  Credential // credential (type + hash)
	AnchorURL   string
	AnchorHash  []byte
	Deposit     uint64
	ExpiryEpoch uint64 // epoch when this DRep expires (0 = unknown)
	Active      bool
}

// ParsedSnapShots holds the three stake distribution snapshots
// (mark, set, go) from the ledger state.
type ParsedSnapShots struct {
	Mark ParsedSnapShot
	Set  ParsedSnapShot
	Go   ParsedSnapShot
	Fee  uint64
}

// ParsedSnapShot represents a single stake distribution snapshot.
type ParsedSnapShot struct {
	// Stake maps credential-hex to staked lovelace.
	Stake map[string]uint64
	// Delegations maps credential-hex to pool key hash.
	Delegations map[string][]byte
	// PoolParams maps pool-hex to pool parameters.
	PoolParams map[string]*ParsedPool
}

// ImportProgress reports progress during ledger state import.
type ImportProgress struct {
	Stage       string
	Current     int
	Total       int
	Percent     float64
	Description string
}

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
		if cfg.State.UTxOHD && cfg.State.UTxOTablePath == "" {
			return errors.New(
				"UTxO-HD ledger state requires external UTxO table file",
			)
		}
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
	certStatePoolsImported := false
	certStatePhaseRan := false
	if !models.IsPhaseCompleted(
		completedPhase,
		models.ImportPhaseCertState,
	) {
		certStatePhaseRan = true
		if cfg.State.CertStateData != nil {
			poolsImported, err := importCertState(
				ctx, cfg, slot, progress,
			)
			if err != nil {
				return fmt.Errorf(
					"importing cert state: %w",
					err,
				)
			}
			certStatePoolsImported = poolsImported > 0
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
			allowSnapshotPoolFallback := certStatePhaseRan &&
				!certStatePoolsImported
			if !allowSnapshotPoolFallback && !certStatePhaseRan &&
				!certStatePoolsImported {
				// On resume, cert-state may have completed in a prior run.
				// Allow snapshot fallback only when no active pools exist
				// in the database.
				hasActivePools, err := hasActivePoolsInDatabase(cfg)
				if err != nil {
					return fmt.Errorf(
						"checking existing pools before snapshot fallback: %w",
						err,
					)
				}
				allowSnapshotPoolFallback = !hasActivePools
			}
			if err := importSnapShots(
				ctx, cfg, slot, progress, allowSnapshotPoolFallback,
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
		if err := importPParams(ctx, cfg); err != nil {
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
				ctx, cfg, progress,
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

func hasActivePoolsInDatabase(cfg ImportConfig) (bool, error) {
	txn := cfg.Database.MetadataTxn(false)
	defer txn.Release()
	store := cfg.Database.Metadata()
	pools, err := store.GetActivePoolKeyHashes(txn.Metadata())
	if err != nil {
		return false, err
	}
	return len(pools) > 0, nil
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
	lastProgressLog := time.Time{}
	lastLoggedPercent := -5.0

	batchCallback := func(batch []ParsedUTxO) error {
		select {
		case <-ctx.Done():
			return fmt.Errorf(
				"UTxO import cancelled: %w", ctx.Err(),
			)
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

		totalImported += len(batch)

		return nil
	}

	reportProgress := func(p UTxOParseProgress) {
		now := time.Now()
		if !lastProgressLog.IsZero() &&
			now.Sub(lastProgressLog) < 10*time.Second &&
			p.Percent < 100 &&
			p.Percent-lastLoggedPercent < 5.0 {
			return
		}
		lastProgressLog = now
		lastLoggedPercent = p.Percent
		progress(ImportProgress{
			Stage:       "utxo",
			Current:     p.EntriesProcessed,
			Total:       p.TotalEntries,
			Percent:     p.Percent,
			Description: "UTxO import progress",
		})
	}

	var err error
	if cfg.State.UTxOTablePath != "" {
		// UTxO-HD format: stream from tvar file
		_, err = parseUTxOsFromFileWithProgress(
			cfg.State.UTxOTablePath,
			batchCallback,
			reportProgress,
		)
		if err != nil {
			return fmt.Errorf(
				"parsing UTxOs from file %s: %w",
				cfg.State.UTxOTablePath, err,
			)
		}
	} else {
		// Legacy format: decode from inline CBOR
		_, err = parseUTxOsStreamingWithProgress(
			cfg.State.UTxOData,
			batchCallback,
			reportProgress,
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
) (int, error) {
	cfg.Logger.Info(
		"parsing cert state",
		"component", "ledgerstate",
	)

	certState, err := ParseCertState(cfg.State.CertStateData)
	if err != nil {
		if certState == nil {
			return 0, fmt.Errorf(
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
			return 0, fmt.Errorf(
				"importing accounts: %w",
				err,
			)
		}
		progress(ImportProgress{
			Stage:   "accounts",
			Current: len(certState.Accounts),
			Total:   len(certState.Accounts),
			Percent: 100,
			Description: fmt.Sprintf(
				"%d accounts imported",
				len(certState.Accounts),
			),
		})
	}

	// Import pools
	importedPools := 0
	if len(certState.Pools) > 0 {
		if err := importPools(
			ctx, cfg, certState.Pools, slot,
		); err != nil {
			return 0, fmt.Errorf("importing pools: %w", err)
		}
		importedPools = len(certState.Pools)
		progress(ImportProgress{
			Stage:   "pools",
			Current: len(certState.Pools),
			Total:   len(certState.Pools),
			Percent: 100,
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
			return 0, fmt.Errorf("importing DReps: %w", err)
		}
		progress(ImportProgress{
			Stage:   "dreps",
			Current: len(certState.DReps),
			Total:   len(certState.DReps),
			Percent: 100,
			Description: fmt.Sprintf(
				"%d DReps imported",
				len(certState.DReps),
			),
		})
	}

	return importedPools, nil
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
	defer func() {
		if txn != nil {
			txn.Release()
		}
	}()

	inBatch := 0
	for _, acct := range accounts {
		select {
		case <-ctx.Done():
			return fmt.Errorf(
				"account import cancelled: %w", ctx.Err(),
			)
		default:
		}

		model := &models.Account{
			StakingKey: acct.StakingKey.Hash,
			Pool:       acct.PoolKeyHash,
			Drep:       acct.DRepCred.Hash,
			DrepType:   acct.DRepCred.Type,
			AddedSlot:  slot,
			Reward:     types.Uint64(acct.Reward),
			Active:     acct.Active,
		}

		if err := store.ImportAccount(
			model, txn.Metadata(),
		); err != nil {
			return fmt.Errorf(
				"importing account %x: %w",
				acct.StakingKey.Hash,
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
		txn.Release()
		txn = nil
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
	defer func() {
		if txn != nil {
			txn.Release()
		}
	}()

	inBatch := 0
	for _, pool := range pools {
		select {
		case <-ctx.Done():
			return fmt.Errorf(
				"pool import cancelled: %w", ctx.Err(),
			)
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
		txn.Release()
		txn = nil
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
	defer func() {
		if txn != nil {
			txn.Release()
		}
	}()

	inBatch := 0
	for _, drep := range dreps {
		select {
		case <-ctx.Done():
			return fmt.Errorf(
				"DRep import cancelled: %w", ctx.Err(),
			)
		default:
		}

		model := &models.Drep{
			Credential: drep.Credential.Hash,
			AnchorURL:  drep.AnchorURL,
			AnchorHash: drep.AnchorHash,
			AddedSlot:  slot,
			Active:     drep.Active,
		}

		reg := &models.RegistrationDrep{
			DrepCredential: drep.Credential.Hash,
			AnchorURL:      drep.AnchorURL,
			AnchorHash:     drep.AnchorHash,
			AddedSlot:      slot,
			DepositAmount:  types.Uint64(drep.Deposit),
		}

		if err := store.ImportDrep(
			model, reg, txn.Metadata(),
		); err != nil {
			return fmt.Errorf(
				"importing DRep %x: %w",
				drep.Credential.Hash,
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
		txn.Release()
		txn = nil
	}
	return nil
}

// importSnapShots imports the mark/set/go stake snapshots.
func importSnapShots(
	ctx context.Context,
	cfg ImportConfig,
	slot uint64,
	progress func(ImportProgress),
	allowPoolFallback bool,
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

	epoch := cfg.State.Epoch

	// Mithril exports the current epoch's Mark/Set/Go bundle, but
	// Dingo persists only Mark snapshots and resolves Set/Go via
	// epoch offsets:
	//   - Mark for epoch N     = current mark snapshot
	//   - Mark for epoch N-1   = imported set snapshot
	//   - Mark for epoch N-2   = imported go snapshot
	//
	// Import the bundle into those historical epochs so leader
	// schedule queries (which request epoch-2, "mark") work
	// immediately after a Mithril restore.
	for _, st := range snapshotImportTargets(epoch, snapshots) {
		select {
		case <-ctx.Done():
			return fmt.Errorf(
				"snapshot import cancelled: %w", ctx.Err(),
			)
		default:
		}

		poolSnapshots := AggregatePoolStake(
			st.snap,
			st.targetEpoch,
			"mark",
			slot,
		)

		if err := persistImportedSnapshot(
			cfg,
			slot,
			st,
			poolSnapshots,
		); err != nil {
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
			"stored_as", "mark",
			"target_epoch", st.targetEpoch,
			"pools", len(poolSnapshots),
			"total_stake", totalStake,
			"component", "ledgerstate",
		)

		progress(ImportProgress{
			Stage:   "snapshots",
			Current: len(poolSnapshots),
			Total:   len(poolSnapshots),
			Percent: 100,
			Description: fmt.Sprintf(
				"%s: %d pools",
				st.name,
				len(poolSnapshots),
			),
		})
	}

	if allowPoolFallback {
		// Fallback pool import path:
		// Snapshot data includes pool parameters and remains reliable when
		// cert-state pool data is unavailable.
		snapshotPools := collectPoolsFromSnapshots(snapshots)
		if len(snapshotPools) > 0 {
			if err := importPools(ctx, cfg, snapshotPools, slot); err != nil {
				return fmt.Errorf(
					"importing pools from snapshots: %w",
					err,
				)
			}
			progress(ImportProgress{
				Stage:   "pools",
				Current: len(snapshotPools),
				Total:   len(snapshotPools),
				Percent: 100,
				Description: fmt.Sprintf(
					"%d pools imported from snapshots",
					len(snapshotPools),
				),
			})
		}
	}

	return nil
}

func persistImportedSnapshot(
	cfg ImportConfig,
	slot uint64,
	st snapshotImportTarget,
	poolSnapshots []*models.PoolStakeSnapshot,
) error {
	store := cfg.Database.Metadata()
	txn := cfg.Database.MetadataTxn(true)
	defer txn.Release()
	metaTxn := txn.Metadata()

	if err := store.DeletePoolStakeSnapshotsForEpoch(
		st.targetEpoch,
		"mark",
		metaTxn,
	); err != nil {
		return fmt.Errorf(
			"deleting existing %s snapshot import for epoch %d: %w",
			st.name,
			st.targetEpoch,
			err,
		)
	}

	if len(poolSnapshots) > 0 {
		if err := store.SavePoolStakeSnapshots(
			poolSnapshots, metaTxn,
		); err != nil {
			return fmt.Errorf(
				"saving %s snapshot import for epoch %d: %w",
				st.name,
				st.targetEpoch,
				err,
			)
		}
	}

	totalStake, totalDelegators := summarizePoolSnapshots(
		poolSnapshots,
	)
	existingSummary, err := store.GetEpochSummary(
		st.targetEpoch, metaTxn,
	)
	if err != nil {
		return fmt.Errorf(
			"getting existing epoch summary for %s snapshot epoch %d: %w",
			st.name,
			st.targetEpoch,
			err,
		)
	}
	summary := importedEpochSummary(
		existingSummary,
		cfg.State.Epoch,
		st.targetEpoch,
		slot,
		cfg.State.EpochNonce,
		totalStake,
		uint64(len(poolSnapshots)),
		totalDelegators,
	)

	if err := store.SaveEpochSummary(summary, metaTxn); err != nil {
		return fmt.Errorf(
			"saving epoch summary for %s snapshot epoch %d: %w",
			st.name,
			st.targetEpoch,
			err,
		)
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

type snapshotImportTarget struct {
	name        string
	targetEpoch uint64
	snap        *ParsedSnapShot
}

func summarizePoolSnapshots(
	poolSnapshots []*models.PoolStakeSnapshot,
) (uint64, uint64) {
	var totalStake uint64
	var totalDelegators uint64
	for _, ps := range poolSnapshots {
		totalStake += uint64(ps.TotalStake)
		totalDelegators += ps.DelegatorCount
	}
	return totalStake, totalDelegators
}

func importedEpochSummary(
	existing *models.EpochSummary,
	currentEpoch uint64,
	targetEpoch uint64,
	importSlot uint64,
	currentEpochNonce []byte,
	totalStake uint64,
	totalPoolCount uint64,
	totalDelegators uint64,
) *models.EpochSummary {
	summary := &models.EpochSummary{
		Epoch:            targetEpoch,
		TotalActiveStake: types.Uint64(totalStake),
		TotalPoolCount:   totalPoolCount,
		TotalDelegators:  totalDelegators,
		SnapshotReady:    true,
	}
	if targetEpoch == currentEpoch {
		summary.BoundarySlot = importSlot
		if len(currentEpochNonce) > 0 {
			summary.EpochNonce = slices.Clone(currentEpochNonce)
		}
	}
	if existing == nil {
		return summary
	}
	if targetEpoch != currentEpoch && existing.BoundarySlot != 0 {
		summary.BoundarySlot = existing.BoundarySlot
	}
	if targetEpoch != currentEpoch && len(existing.EpochNonce) > 0 {
		summary.EpochNonce = slices.Clone(existing.EpochNonce)
	}
	return summary
}

func snapshotImportTargets(
	currentEpoch uint64,
	snapshots *ParsedSnapShots,
) []snapshotImportTarget {
	if snapshots == nil {
		return nil
	}
	targets := []snapshotImportTarget{
		{
			name:        "mark",
			targetEpoch: currentEpoch,
			snap:        &snapshots.Mark,
		},
	}
	if currentEpoch > 0 {
		targets = append(targets, snapshotImportTarget{
			name:        "set",
			targetEpoch: currentEpoch - 1,
			snap:        &snapshots.Set,
		})
	}
	if currentEpoch > 1 {
		targets = append(targets, snapshotImportTarget{
			name:        "go",
			targetEpoch: currentEpoch - 2,
			snap:        &snapshots.Go,
		})
	}
	return targets
}

func collectPoolsFromSnapshots(
	snapshots *ParsedSnapShots,
) []ParsedPool {
	type poolMap map[string]*ParsedPool
	seen := make(map[string]ParsedPool)
	for _, pm := range []poolMap{
		snapshots.Mark.PoolParams,
		snapshots.Set.PoolParams,
		snapshots.Go.PoolParams,
	} {
		for _, pool := range pm {
			if pool == nil || len(pool.PoolKeyHash) != 28 {
				continue
			}
			hexKey := hex.EncodeToString(pool.PoolKeyHash)
			seen[hexKey] = *pool
		}
	}
	ret := make([]ParsedPool, 0, len(seen))
	keys := make([]string, 0, len(seen))
	for hexKey := range seen {
		keys = append(keys, hexKey)
	}
	sort.Strings(keys)
	for _, hexKey := range keys {
		ret = append(ret, seen[hexKey])
	}
	return ret
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

	// Store evolving nonce as the tip block nonce so that
	// subsequent block processing starts from the correct rolling
	// nonce. Without this, the node falls back to the Shelley
	// genesis hash, producing wrong block nonces and eventually a
	// wrong epoch nonce that causes VRF verification to fail.
	if len(cfg.State.EvolvingNonce) > 0 {
		if len(cfg.State.EvolvingNonce) != 32 {
			return fmt.Errorf(
				"invalid evolving nonce length %d, expected 32",
				len(cfg.State.EvolvingNonce),
			)
		}
		if err := store.SetBlockNonce(
			tip.BlockHash,
			tip.Slot,
			cfg.State.EvolvingNonce,
			true, // checkpoint
			txn.Metadata(),
		); err != nil {
			return fmt.Errorf(
				"setting tip block nonce: %w", err,
			)
		}
		cfg.Logger.Info(
			"stored evolving nonce for tip block",
			"component", "ledgerstate",
			"slot", tip.Slot,
			"nonce", hex.EncodeToString(
				cfg.State.EvolvingNonce,
			),
		)
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

	// SetEpoch uses OnConflict/upsert clauses in all backends,
	// so re-running this function after a partial failure is
	// safe: existing epochs are updated rather than duplicated.

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
		// NOTE: cfg.State.EvolvingNonce is the tip-time value, which
		// may be mid-epoch. This is the best available seed at import
		// time. The first full epoch rollover (processEpochRollover)
		// will recompute and store the correct end-of-epoch value.
		// #nosec G115
		if err := store.SetEpoch(
			epochStartSlot,
			cfg.State.Epoch,
			cfg.State.EpochNonce,
			cfg.State.EvolvingNonce,
			cfg.State.CandidateNonce,
			cfg.State.LastEpochBlockNonce,
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
			if err := ctx.Err(); err != nil {
				return totalEpochs, fmt.Errorf(
					"cancelled: %w", err,
				)
			}
			startSlot := startBound.Slot +
				(e-startBound.Epoch)*uint64(epochLength)
			if err := store.SetEpoch(
				startSlot,
				e,
				nil, // historical epochs don't need nonce
				nil, // evolvingNonce not available from import
				nil, // candidateNonce not available from import
				nil, // lastEpochBlockNonce not available from import
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
		var evolvingNonce []byte
		var candidateNonce []byte
		var lastEpochBlockNonce []byte
		if e == cfg.State.Epoch {
			nonce = cfg.State.EpochNonce
			// NOTE: tip-time EvolvingNonce (may be mid-epoch).
			// Corrected by the first full epoch rollover.
			evolvingNonce = cfg.State.EvolvingNonce
			candidateNonce = cfg.State.CandidateNonce
			lastEpochBlockNonce = cfg.State.LastEpochBlockNonce
		}

		if err := store.SetEpoch(
			startSlot,
			e,
			nonce,
			evolvingNonce,
			candidateNonce,
			lastEpochBlockNonce,
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
	epochLength = uint(slotSpan / epochSpan) // #nosec G115 -- epoch length fits in uint
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
func importPParams(
	ctx context.Context,
	cfg ImportConfig,
) error {
	if cfg.State.PParamsData == nil {
		return nil
	}

	if err := ctx.Err(); err != nil {
		return fmt.Errorf(
			"pparams import cancelled: %w", err,
		)
	}

	cfg.Logger.Info(
		"importing protocol parameters",
		"component", "ledgerstate",
	)

	pparamsCbor := []byte(cfg.State.PParamsData)
	if err := validatePParamsData(
		cfg.State.EraIndex,
		pparamsCbor,
	); err != nil {
		return fmt.Errorf(
			"validating protocol parameters: %w",
			err,
		)
	}

	store := cfg.Database.Metadata()
	txn := cfg.Database.MetadataTxn(true)
	defer txn.Release()
	pparamsSlot := snapshotEpochAnchorSlot(
		cfg,
		cfg.State.Epoch,
	)

	// #nosec G115
	if err := store.SetPParams(
		pparamsCbor,
		pparamsSlot,
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

// importGovState imports governance state (constitution, committee
// members, and proposals) from the snapshot.
func importGovState(
	ctx context.Context,
	cfg ImportConfig,
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
	currentEpochSlot := snapshotEpochAnchorSlot(
		cfg,
		cfg.State.Epoch,
	)

	// Import constitution
	if govState.Constitution != nil {
		if err := func() error {
			txn := cfg.Database.MetadataTxn(true)
			defer txn.Release()
			if err := store.SetConstitution(
				&models.Constitution{
					AnchorURL:  govState.Constitution.AnchorURL,
					AnchorHash: govState.Constitution.AnchorHash,
					PolicyHash: govState.Constitution.PolicyHash,
					AddedSlot:  currentEpochSlot,
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
			"anchor_url", govState.Constitution.AnchorURL,
		)
	}

	// Import committee members and quorum.
	if len(govState.Committee) > 0 || govState.CommitteeQuorum != nil {
		if err := func() error {
			txn := cfg.Database.MetadataTxn(true)
			defer txn.Release()
			if len(govState.Committee) > 0 {
				members := make(
					[]*models.CommitteeMember,
					len(govState.Committee),
				)
				for i, cm := range govState.Committee {
					if len(cm.ColdCredential.Hash) != 28 {
						return fmt.Errorf(
							"committee member %d: credential hash is %d bytes, expected 28",
							i, len(cm.ColdCredential.Hash),
						)
					}
					members[i] = &models.CommitteeMember{
						ColdCredHash: cm.ColdCredential.Hash,
						ExpiresEpoch: cm.ExpiresEpoch,
						AddedSlot:    currentEpochSlot,
					}
				}
				if err := store.SetCommitteeMembers(
					members, txn.Metadata(),
				); err != nil {
					return fmt.Errorf(
						"importing committee members: %w", err,
					)
				}
			}
			if govState.CommitteeQuorum != nil {
				if govState.CommitteeQuorum.Rat == nil {
					return errors.New("committee quorum present but missing Rat")
				}
				quorum := &types.Rat{
					Rat: new(big.Rat).Set(govState.CommitteeQuorum.Rat),
				}
				if err := store.SetCommitteeQuorum(
					quorum, currentEpochSlot, txn.Metadata(),
				); err != nil {
					return fmt.Errorf(
						"importing committee quorum: %w", err,
					)
				}
			}
			if err := txn.Commit(); err != nil {
				return fmt.Errorf(
					"committing committee state transaction: %w",
					err,
				)
			}
			return nil
		}(); err != nil {
			return fmt.Errorf(
				"saving committee state: %w", err,
			)
		}
		cfg.Logger.Info(
			"imported committee state",
			"component", "ledgerstate",
			"count", len(govState.Committee),
			"quorum", govState.CommitteeQuorum != nil,
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
					return fmt.Errorf(
						"governance import cancelled: %w",
						ctx.Err(),
					)
				default:
				}
				proposedSlot := snapshotEpochAnchorSlot(
					cfg,
					prop.ProposedIn,
				)
				if err := store.SetGovernanceProposal(
					&models.GovernanceProposal{
						TxHash:        prop.TxHash,
						ActionIndex:   prop.ActionIndex,
						ActionType:    prop.ActionType,
						ProposedEpoch: prop.ProposedIn,
						ExpiresEpoch:  prop.ExpiresAfter,
						Deposit:       prop.Deposit,
						ReturnAddress: prop.ReturnAddr,
						AnchorURL:     prop.AnchorURL,
						AnchorHash:    prop.AnchorHash,
						AddedSlot:     proposedSlot,
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
		Percent:     100,
		Description: "governance state imported",
	})

	return nil
}

func snapshotEpochAnchorSlot(
	cfg ImportConfig,
	epoch uint64,
) uint64 {
	if cfg.State == nil {
		return 0
	}
	if len(cfg.State.EraBounds) > 0 &&
		epoch < cfg.State.EraBounds[0].Epoch {
		logSnapshotEpochAnchorFallback(
			cfg,
			epoch,
			"epoch precedes first era bound",
			nil,
		)
		return 0
	}
	for eraIndex := len(cfg.State.EraBounds) - 1; eraIndex >= 0; eraIndex-- {
		bound := cfg.State.EraBounds[eraIndex]
		if epoch < bound.Epoch {
			continue
		}
		return snapshotEpochAnchorSlotFromBound(
			cfg,
			epoch,
			uint(eraIndex), //nolint:gosec // era index fits in uint
			bound.Epoch,
			bound.Slot,
		)
	}
	if epoch < cfg.State.EraBoundEpoch {
		logSnapshotEpochAnchorFallback(
			cfg,
			epoch,
			"era bounds unavailable; using single-bound fallback",
			nil,
		)
	}
	return snapshotEpochAnchorSlotFromBound(
		cfg,
		epoch,
		uint(cfg.State.EraIndex), //nolint:gosec // era ID fits in uint
		cfg.State.EraBoundEpoch,
		cfg.State.EraBoundSlot,
	)
}

func snapshotEpochAnchorSlotFromBound(
	cfg ImportConfig,
	epoch uint64,
	eraIndex uint,
	boundEpoch uint64,
	boundSlot uint64,
) uint64 {
	if epoch < boundEpoch {
		return boundSlot
	}
	if cfg.EpochLength == nil {
		logSnapshotEpochAnchorFallback(
			cfg,
			epoch,
			"epoch length unavailable (EpochLength is nil)",
			nil,
		)
		return boundSlot
	}
	_, epochLength, err := cfg.EpochLength(eraIndex)
	if err != nil {
		logSnapshotEpochAnchorFallback(
			cfg,
			epoch,
			"failed to resolve epoch length",
			err,
		)
		return boundSlot
	}
	if epochLength == 0 {
		logSnapshotEpochAnchorFallback(
			cfg,
			epoch,
			"epoch length is zero",
			nil,
		)
		return boundSlot
	}
	return boundSlot + (epoch-boundEpoch)*uint64(epochLength)
}

func logSnapshotEpochAnchorFallback(
	cfg ImportConfig,
	epoch uint64,
	msg string,
	err error,
) {
	if cfg.State == nil {
		return
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	args := []any{
		"component", "ledgerstate",
		"epoch", epoch,
		"era_bound_epoch", cfg.State.EraBoundEpoch,
	}
	if err != nil {
		args = append(args, "error", err)
	}
	logger.Warn("snapshotEpochAnchorSlot: "+msg, args...)
}
