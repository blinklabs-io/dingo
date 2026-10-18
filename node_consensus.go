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
	"errors"
	"fmt"

	nodeconsensus "github.com/blinklabs-io/dingo/consensus"
	"github.com/blinklabs-io/dingo/database"
)

const consensusDatabaseIdentityKey = "consensus_provider_identity"

type consensusIdentityStore interface {
	GetSyncState(string, *database.Txn) (string, error)
	SetSyncState(string, string, *database.Txn) error
}

func (n *Node) enforceConsensusDatabaseIdentity() error {
	if n == nil || n.db == nil {
		return errors.New("consensus database is unavailable")
	}
	provider := n.selectedConsensusProvider()
	identity := provider.DatabaseIdentity()
	if provider.Name() == "" || identity == "" ||
		provider.NetworkIdentity() == "" || provider.ScheduleIdentity() == "" {
		return errors.New("consensus provider identity is incomplete")
	}
	return enforceConsensusIdentity(n.db, identity)
}

func enforceConsensusIdentity(
	store consensusIdentityStore,
	identity string,
) error {
	persisted, err := store.GetSyncState(consensusDatabaseIdentityKey, nil)
	if err != nil {
		return fmt.Errorf("read consensus database identity: %w", err)
	}
	if persisted == "" {
		// Databases created before provider selection are Praos databases.
		// Treating an unmarked database as a newly selected algorithm would
		// silently reinterpret its chain and cached state.
		if identity != nodeconsensus.PraosIdentity {
			return fmt.Errorf(
				"unmarked database uses consensus %q, selected provider uses %q",
				nodeconsensus.PraosIdentity,
				identity,
			)
		}
		if err := store.SetSyncState(
			consensusDatabaseIdentityKey,
			identity,
			nil,
		); err != nil {
			return fmt.Errorf("persist consensus database identity: %w", err)
		}
		return nil
	}
	if persisted != identity {
		return fmt.Errorf(
			"database consensus identity %q does not match selected provider %q",
			persisted,
			identity,
		)
	}
	return nil
}
