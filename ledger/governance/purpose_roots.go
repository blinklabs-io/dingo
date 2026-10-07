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

package governance

import (
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// ErrMissingEnactedRoot is matched by errors.Is for a MissingEnactedRootError.
var ErrMissingEnactedRoot = errors.New(
	"governance purpose has no enacted root for chained proposal",
)

// MissingEnactedRootError reports an active proposal that names a parent
// action while its purpose has no enacted root and the parent is neither
// an active proposal nor a known row. On a database seeded from a ledger
// snapshot that means the snapshot's per-purpose root never reached
// governance_proposal, so the node would silently skip ratifications the
// network performs.
type MissingEnactedRootError struct {
	TxHash          []byte
	ActionIndex     uint32
	ActionType      uint8
	ParentTxHash    []byte
	ParentActionIdx uint32
}

func (e *MissingEnactedRootError) Error() string {
	return fmt.Sprintf(
		"proposal %s#%d (action type %d) chains to parent %s#%d "+
			"but its purpose has no enacted root; the snapshot "+
			"governance root was not seeded",
		hex.EncodeToString(e.TxHash),
		e.ActionIndex,
		e.ActionType,
		hex.EncodeToString(e.ParentTxHash),
		e.ParentActionIdx,
	)
}

func (e *MissingEnactedRootError) Is(target error) bool {
	return target == ErrMissingEnactedRoot
}

// checkMissingEnactedRoot returns a MissingEnactedRootError when proposal
// is chained under a purpose that has no enacted root and its parent is
// neither one of the active proposals nor a stored row. A parent that is a
// pending sibling, or a stored but superseded or non-root action, is a
// legitimate skip and returns nil.
func checkMissingEnactedRoot(
	db *database.Database,
	txn *database.Txn,
	proposal *models.GovernanceProposal,
	root *models.GovernanceProposal,
	active map[string]struct{},
) error {
	if root != nil ||
		len(proposal.ParentTxHash) == 0 ||
		proposal.ParentActionIdx == nil ||
		govActionPurposeOf(
			lcommon.GovActionType(proposal.ActionType),
		) == purposeNone {
		return nil
	}
	if _, ok := active[proposalParentKey(proposal)]; ok {
		return nil
	}
	_, err := db.GetGovernanceProposal(
		proposal.ParentTxHash, *proposal.ParentActionIdx, txn,
	)
	if err == nil {
		return nil
	}
	if !errors.Is(err, models.ErrGovernanceProposalNotFound) {
		return fmt.Errorf("look up parent governance proposal: %w", err)
	}
	return &MissingEnactedRootError{
		TxHash:          proposal.TxHash,
		ActionIndex:     proposal.ActionIndex,
		ActionType:      proposal.ActionType,
		ParentTxHash:    proposal.ParentTxHash,
		ParentActionIdx: *proposal.ParentActionIdx,
	}
}

func activeProposalKeys(
	proposals []*models.GovernanceProposal,
) map[string]struct{} {
	keys := make(map[string]struct{}, len(proposals))
	for _, p := range proposals {
		keys[proposalIdentityKey(p)] = struct{}{}
	}
	return keys
}

// isMithrilBootstrapped reports whether the database was seeded from a
// ledger-state snapshot. A genesis-synced node derives every purpose root
// from its own enactments, and a rootless purpose there is legitimate.
func isMithrilBootstrapped(
	db *database.Database,
	txn *database.Txn,
) (bool, error) {
	slot, err := db.MithrilTrustBoundarySlotStrict(txn)
	if err != nil {
		return false, err
	}
	return slot > 0, nil
}

// VerifyPurposeRoots checks a Mithril-bootstrapped database for active
// proposals whose chained parent has no enacted purpose root. It returns a
// MissingEnactedRootError for the first such proposal and nil for a
// genesis-synced database.
func VerifyPurposeRoots(
	db *database.Database,
	txn *database.Txn,
	epoch uint64,
) error {
	bootstrapped, err := isMithrilBootstrapped(db, txn)
	if err != nil {
		return err
	}
	if !bootstrapped {
		return nil
	}
	active, err := db.GetActiveGovernanceProposals(epoch, txn)
	if err != nil {
		return fmt.Errorf("get active proposals: %w", err)
	}
	keys := activeProposalKeys(active)
	for _, p := range active {
		purpose := govActionPurposeOf(lcommon.GovActionType(p.ActionType))
		if purpose == purposeNone {
			continue
		}
		root, err := db.GetLastEnactedGovernanceProposal(
			purposeActionTypes(purpose), txn,
		)
		if err != nil {
			return fmt.Errorf(
				"get current root for purpose %d: %w", purpose, err,
			)
		}
		if err := checkMissingEnactedRoot(db, txn, p, root, keys); err != nil {
			return err
		}
	}
	return nil
}
