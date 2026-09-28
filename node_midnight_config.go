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

package dingo

import (
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	midnightindexer "github.com/blinklabs-io/dingo/midnight/indexer"
)

// midnightIndexerActive is shared by initial startup and live reconstruction.
// Indexer enablement is independent of the Midnight server's listener settings.
func midnightIndexerActive(storageMode StorageMode, cfg MidnightConfig) bool {
	return cfg.Enabled && storageMode.IsAPI()
}

// midnightIndexerConfig preserves startup configuration during reconstruction.
// Callers must install the current database and ledger before building it;
// callbacks resolve those components when invoked so they follow replacement.
func (n *Node) midnightIndexerConfig() midnightindexer.Config {
	return midnightindexer.Config{
		EventBus:                n.eventBus,
		Metadata:                n.db.Metadata(),
		SlotTimer:               n.ledgerState,
		Logger:                  n.config.logger,
		PromRegistry:            n.config.promRegistry,
		CNightPolicyID:          n.config.midnight.CNightPolicyID,
		CNightAssetName:         n.config.midnight.CNightAssetName,
		MappingValidatorAddress: n.config.midnight.MappingValidatorAddress,
		AuthTokenPolicyID:       n.config.midnight.AuthTokenPolicyID,
		AuthTokenAssetName:      n.config.midnight.AuthTokenAssetName,
		// Governance / Ariadne / candidate scanning
		TechnicalCommitteeAddress:   n.config.midnight.TechnicalCommitteeAddress,
		TechnicalCommitteePolicyID:  n.config.midnight.TechnicalCommitteePolicyID,
		CouncilAddress:              n.config.midnight.CouncilAddress,
		CouncilPolicyID:             n.config.midnight.CouncilPolicyID,
		PermissionedCandidatePolicy: n.config.midnight.PermissionedCandidatePolicy,
		CommitteeCandidateAddress:   n.config.midnight.CommitteeCandidateAddress,
		SlotToEpoch: func(slot uint64) (uint64, error) {
			epoch, err := n.ledgerState.SlotToEpoch(slot)
			if err != nil {
				return 0, err
			}
			return epoch.EpochId, nil
		},
		BlockIterator: func(startSlot, endSlot uint64, fn func(models.Block) error) error {
			return database.ForEachBlockInRangeDB(
				n.db,
				startSlot,
				endSlot,
				fn,
			)
		},
		// Read the applied ledger tip straight from metadata rather than
		// from n.ledgerState.Tip(): LedgerState only loads its in-memory
		// tip inside Start, which runs after this indexer has already
		// backfilled, so Tip() would still be the zero value here. Blocks
		// stored above this slot -- the whole post-snapshot suffix on a
		// Mithril-bootstrapped node -- are replayed by LedgerState.Start
		// and reach the indexer as live block events instead.
		LedgerTipSlot: func() (uint64, error) {
			tip, err := n.db.GetTip(nil)
			if err != nil {
				return 0, err
			}
			return tip.Point.Slot, nil
		},
		FatalErrorFunc: func(err error) {
			n.config.logger.Error(
				"fatal midnight indexer error, initiating shutdown",
				"error", err,
			)
			n.cancelForFatal(err)
		},
	}
}
