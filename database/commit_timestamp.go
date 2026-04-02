// Copyright 2025 Blink Labs Software
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

package database

import (
	"errors"
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/database/types"
)

// CommitTimestampError contains the timestamps of the metadata and blob stores
type CommitTimestampError struct {
	MetadataTimestamp int64
	BlobTimestamp     int64
}

// Error returns the stringified error
func (e CommitTimestampError) Error() string {
	return fmt.Sprintf(
		"commit timestamp mismatch: %d (metadata) != %d (blob)",
		e.MetadataTimestamp,
		e.BlobTimestamp,
	)
}

func (b *Database) checkCommitTimestamp() error {
	// Get value from metadata
	metadataTimestamp, metadataErr := b.Metadata().GetCommitTimestamp()
	if metadataErr != nil {
		return fmt.Errorf(
			"failed to get metadata timestamp from plugin: %w",
			metadataErr,
		)
	}
	// No timestamp in the database
	if metadataTimestamp <= 0 {
		return nil
	}
	// Get value from blob
	blobTimestamp, blobErr := b.Blob().GetCommitTimestamp()
	if blobErr != nil {
		return fmt.Errorf(
			"failed to get blob timestamp from plugin: %w",
			blobErr,
		)
	}
	// Compare values
	if blobTimestamp != metadataTimestamp {
		return CommitTimestampError{
			MetadataTimestamp: metadataTimestamp,
			BlobTimestamp:     blobTimestamp,
		}
	}
	return nil
}

func (b *Database) updateCommitTimestamp(txn *Txn, timestamp int64) error {
	if txn == nil {
		return types.ErrNilTxn
	}
	// Update metadata
	metaTxn := txn.Metadata()
	if err := b.Metadata().SetCommitTimestamp(timestamp, metaTxn); err != nil {
		return err
	}
	// Update blob
	blobTxn := txn.Blob()
	if err := b.Blob().SetCommitTimestamp(timestamp, blobTxn); err != nil {
		return err
	}
	return nil
}

// NodeSettingsError is returned when the configured node settings differ
// from those persisted in the database. Changing immutable settings after
// initial sync would leave the database in an inconsistent state.
type NodeSettingsError struct {
	Mismatches []string
}

func (e NodeSettingsError) Error() string {
	return fmt.Sprintf(
		"node settings mismatch: %s; changing these settings requires "+
			"re-syncing from scratch",
		strings.Join(e.Mismatches, "; "),
	)
}

// checkNodeSettings validates that immutable settings (storage mode,
// network) have not changed since the database was first initialised.
// On first start it persists the configured values.
func (d *Database) checkNodeSettings() error {
	persisted, err := d.Metadata().GetNodeSettings()
	if err != nil {
		return fmt.Errorf(
			"failed to get node settings from metadata: %w", err,
		)
	}

	configured := &types.NodeSettings{
		StorageMode: d.config.StorageMode,
		Network:     d.config.Network,
	}

	firstStart := persisted == nil
	if firstStart {
		// First start: persist the configured settings. Re-read
		// afterwards so concurrent initialisation cannot silently
		// continue with a conflicting configuration.
		if err := d.Metadata().SetNodeSettings(configured); err != nil {
			return fmt.Errorf(
				"failed to persist node settings: %w", err,
			)
		}
		persisted, err = d.Metadata().GetNodeSettings()
		if err != nil {
			return fmt.Errorf(
				"failed to re-read node settings from metadata: %w",
				err,
			)
		}
		if persisted == nil {
			return errors.New("node settings were not found after persistence")
		}
	}

	var mismatches []string
	if persisted.StorageMode != configured.StorageMode {
		mismatches = append(mismatches, fmt.Sprintf(
			"storage mode was %q but configured as %q",
			persisted.StorageMode, configured.StorageMode,
		))
	}

	// Some database open paths do not know the network yet. In that case,
	// preserve the persisted value and skip network validation. When the
	// network later becomes available, fill it in exactly once.
	if configured.Network != "" &&
		persisted.Network == "" &&
		len(mismatches) == 0 {
		if err := d.Metadata().SetNodeSettings(configured); err != nil {
			return fmt.Errorf(
				"failed to persist node settings: %w",
				err,
			)
		}
		persisted, err = d.Metadata().GetNodeSettings()
		if err != nil {
			return fmt.Errorf(
				"failed to re-read node settings from metadata: %w",
				err,
			)
		}
		if persisted == nil {
			return errors.New("node settings were not found after persistence")
		}
	}

	if configured.Network != "" && persisted.Network != configured.Network {
		mismatches = append(mismatches, fmt.Sprintf(
			"network was %q but configured as %q",
			persisted.Network, configured.Network,
		))
	}
	if len(mismatches) > 0 {
		return NodeSettingsError{Mismatches: mismatches}
	}
	if firstStart {
		d.logger.Info(
			"node settings initialized",
			"storageMode", configured.StorageMode,
			"network", configured.Network,
		)
	}
	return nil
}
