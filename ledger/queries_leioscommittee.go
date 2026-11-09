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
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/gouroboros/cbor"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

// ErrLeiosCommitteeNotYetAvailable marks a LedgerStateConfig.LeiosCommittee
// failure that only means the committee cannot be built yet, because the stake
// snapshot it is drawn from has no pools. It is the one failure that leaves
// the next committee out of the reply; every other failure fails the query.
var ErrLeiosCommitteeNotYetAvailable = errors.New(
	"leios committee not yet available",
)

// queryShelleyLeiosCommittee answers GetLeiosCommittee (Shelley sub-query 41),
// the Leios voting committees of the current and the next epoch, which
// `cardano-cli query leios-committee` sends so an operator can check whether
// the pool holds a seat and whether its BLS key is voting.
//
// The current committee is required: failing to build it fails the query. The
// next committee is optional in the reply (the ledger's StrictMaybe), because
// the stake snapshot it is drawn from may not exist yet. Only that case
// (ErrLeiosCommitteeNotYetAvailable) leaves it out of the reply; a shutdown,
// backlog or storage failure fails the query, so a client never reads an
// incomplete reply as a complete one.
//
// Both committees are keyed by epoch and built from that epoch's persisted
// snapshots, so a point pinned in an earlier epoch is answered with that
// epoch's committees, not the live ones.
func (ls *LedgerState) queryShelleyLeiosCommittee(
	ctx context.Context,
	at QueryPoint,
	txn *database.Txn,
) (any, error) {
	epoch, err := ls.shelleyEpochNo(ctx, at, txn)
	if err != nil {
		return nil, err
	}
	current, err := ls.config.LeiosCommittee(epoch)
	if err != nil {
		return nil, fmt.Errorf(
			"leios committee for epoch %d: %w",
			epoch,
			err,
		)
	}
	result := olocalstatequery.LeiosCommitteeResult{Current: current}
	if epoch < math.MaxUint64 {
		next, err := ls.config.LeiosCommittee(epoch + 1)
		switch {
		case err == nil:
			result.Next = next
		case errors.Is(err, ErrLeiosCommitteeNotYetAvailable):
			ls.config.Logger.Debug(
				"leios committee for next epoch unavailable",
				"component", "ledger",
				"epoch", epoch+1,
				"error", err,
			)
		default:
			return nil, fmt.Errorf(
				"leios committee for epoch %d: %w",
				epoch+1,
				err,
			)
		}
	}
	return leiosCommitteeLeafResult(result)
}

// leiosCommitteeLeafResult returns result in the leaf handler form, the
// single-element []any{value} that queryShelleyLeaf documents and GetCBOR
// unwraps. LeiosCommitteeResult encodes the era result wrapper itself, so the
// wrapper is peeled off to avoid doubling it: the direct reply still carries
// exactly one wrapper, and GetCBOR's tag-24 content is the bare
// [current, next].
func leiosCommitteeLeafResult(
	result olocalstatequery.LeiosCommitteeResult,
) (any, error) {
	encoded, err := cbor.Encode(result)
	if err != nil {
		return nil, fmt.Errorf("encode leios committee: %w", err)
	}
	var wrapper []cbor.RawMessage
	if _, err := cbor.Decode(encoded, &wrapper); err != nil {
		return nil, fmt.Errorf("decode leios committee wrapper: %w", err)
	}
	if len(wrapper) != 1 {
		return nil, fmt.Errorf(
			"leios committee wrapper holds %d items, expected 1",
			len(wrapper),
		)
	}
	return []any{wrapper[0]}, nil
}
