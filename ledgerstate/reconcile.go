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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// reconcileKeys is the set of live ledger keys present in an imported snapshot.
// During a Mithril v2 catch-up import the import phases populate it; the
// reconcile then marks any live database row whose key is absent here inactive.
// The snapshot is the complete, trusted ledger state at its tip, so every live
// row missing from it has been spent/deregistered/retired by that tip.
type reconcileKeys struct {
	utxos    map[utxoReconcileKey]struct{}
	accounts map[string]struct{}
	pools    map[string]struct{}
	dreps    map[string]struct{}
}

type utxoReconcileKey struct {
	txID      [32]byte
	outputIdx uint32
}

func newReconcileKeys() *reconcileKeys {
	return &reconcileKeys{
		utxos:    make(map[utxoReconcileKey]struct{}),
		accounts: make(map[string]struct{}),
		pools:    make(map[string]struct{}),
		dreps:    make(map[string]struct{}),
	}
}

func newUtxoReconcileKey(txID []byte, idx uint32) (utxoReconcileKey, bool) {
	if len(txID) != 32 {
		return utxoReconcileKey{}, false
	}
	var key utxoReconcileKey
	copy(key.txID[:], txID)
	key.outputIdx = idx
	return key, true
}

func credKeyString(tag uint8, hash []byte) string {
	return strconv.FormatUint(uint64(tag), 10) + ":" + hex.EncodeToString(hash)
}

func poolKeyString(keyHash []byte) string {
	return hex.EncodeToString(keyHash)
}

func (k *reconcileKeys) addUtxo(txID []byte, idx uint32) error {
	key, ok := newUtxoReconcileKey(txID, idx)
	if !ok {
		return fmt.Errorf(
			"malformed UTxO reconcile key: tx hash is %d bytes, expected 32",
			len(txID),
		)
	}
	k.utxos[key] = struct{}{}
	return nil
}

func (k *reconcileKeys) addAccount(tag uint8, stakingKey []byte) {
	k.accounts[credKeyString(tag, stakingKey)] = struct{}{}
}

func (k *reconcileKeys) addPool(keyHash []byte) {
	k.pools[poolKeyString(keyHash)] = struct{}{}
}

func (k *reconcileKeys) addDrep(tag uint8, credential []byte) {
	k.dreps[credKeyString(tag, credential)] = struct{}{}
}

// reconcileStaleLedgerState marks every live row absent from the imported
// snapshot's live key set inactive: live UTxOs get DeletedSlot=tipSlot, accounts
// and DReps go Active=false, and pools get a retirement at tipEpoch. It never
// deletes a row. On an empty database (a fresh bootstrap) it is a no-op.
//
// Stale keys are collected under a read transaction; all mutations are applied
// together under one write transaction so the reconcile is atomic.
func reconcileStaleLedgerState(
	ctx context.Context,
	db *database.Database,
	keys *reconcileKeys,
	tipSlot uint64,
	tipEpoch uint64,
	logger *slog.Logger,
) error {
	if logger == nil {
		logger = slog.Default()
	}
	store := db.Metadata()

	// Phase 1: collect the live rows that are absent from the snapshot.
	var (
		staleUtxos []types.UtxoKey
		staleAccts []models.StakeCredentialRef
		staleDreps []models.StakeCredentialRef
		stalePools [][]byte
	)

	rtxn := db.MetadataTxn(false)
	if err := store.IterateLiveUtxos(
		rtxn.Metadata(),
		func(u *models.Utxo) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			key, ok := newUtxoReconcileKey(u.TxId, u.OutputIdx)
			if !ok {
				staleUtxos = append(staleUtxos, types.UtxoKey{
					TxId:      bytes.Clone(u.TxId),
					OutputIdx: u.OutputIdx,
				})
				return nil
			}
			if _, ok := keys.utxos[key]; !ok {
				staleUtxos = append(staleUtxos, types.UtxoKey{
					TxId:      bytes.Clone(u.TxId),
					OutputIdx: u.OutputIdx,
				})
			}
			return nil
		},
	); err != nil {
		rtxn.Release()
		return fmt.Errorf("scanning live UTxOs: %w", err)
	}

	accts, err := store.GetActiveAccountCredentials(rtxn.Metadata())
	if err != nil {
		rtxn.Release()
		return fmt.Errorf("listing active accounts: %w", err)
	}
	for _, a := range accts {
		if _, ok := keys.accounts[credKeyString(a.Tag, a.Key)]; !ok {
			staleAccts = append(staleAccts, models.StakeCredentialRef{
				Tag: a.Tag,
				Key: bytes.Clone(a.Key),
			})
		}
	}

	dreps, err := store.GetActiveDreps(rtxn.Metadata())
	if err != nil {
		rtxn.Release()
		return fmt.Errorf("listing active DReps: %w", err)
	}
	for _, d := range dreps {
		if _, ok := keys.dreps[credKeyString(d.CredentialTag, d.Credential)]; !ok {
			staleDreps = append(staleDreps, models.StakeCredentialRef{
				Tag: d.CredentialTag,
				Key: bytes.Clone(d.Credential),
			})
		}
	}

	pools, err := store.GetActivePoolKeyHashes(rtxn.Metadata())
	if err != nil {
		rtxn.Release()
		return fmt.Errorf("listing active pools: %w", err)
	}
	for _, p := range pools {
		if _, ok := keys.pools[poolKeyString(p)]; !ok {
			stalePools = append(stalePools, bytes.Clone(p))
		}
	}
	rtxn.Release()

	// Phase 2: apply all the inactive-markings atomically.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("reconcile cancelled before applying stale markers: %w", err)
	}
	wtxn := db.MetadataTxn(true)
	defer wtxn.Release()
	if err := store.MarkUtxosDeletedAtSlot(
		wtxn.Metadata(), staleUtxos, tipSlot,
	); err != nil {
		return fmt.Errorf("tombstoning %d stale UTxOs: %w", len(staleUtxos), err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("reconcile cancelled before applying stale markers: %w", err)
	}
	if err := store.DeactivateAccounts(
		wtxn.Metadata(), staleAccts,
	); err != nil {
		return fmt.Errorf("deactivating %d stale accounts: %w", len(staleAccts), err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("reconcile cancelled before applying stale markers: %w", err)
	}
	if err := store.DeactivateDreps(
		wtxn.Metadata(), staleDreps,
	); err != nil {
		return fmt.Errorf("deactivating %d stale DReps: %w", len(staleDreps), err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("reconcile cancelled before applying stale markers: %w", err)
	}
	if err := store.RetirePools(
		wtxn.Metadata(), stalePools, tipEpoch, tipSlot,
	); err != nil {
		return fmt.Errorf("retiring %d stale pools: %w", len(stalePools), err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("reconcile cancelled before applying stale markers: %w", err)
	}
	if err := wtxn.Commit(); err != nil {
		return fmt.Errorf("committing reconcile: %w", err)
	}

	logger.Info(
		"reconciled stale ledger state against newer snapshot",
		"component", "ledgerstate",
		"tip_slot", tipSlot,
		"tip_epoch", tipEpoch,
		"utxos_tombstoned", len(staleUtxos),
		"accounts_deactivated", len(staleAccts),
		"dreps_deactivated", len(staleDreps),
		"pools_retired", len(stalePools),
	)
	return nil
}
