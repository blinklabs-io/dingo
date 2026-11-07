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
	"fmt"
)

// foldImportedRewardCredits covers credentials absent from the imported account
// set as well: their old credits have already been withdrawn or forfeited. The
// cert-state checkpoint must follow this drain so interruption retries it.
func foldImportedRewardCredits(
	ctx context.Context,
	cfg ImportConfig,
	slot uint64,
) error {
	rounds, err := cfg.Database.Metadata().GetPendingRewardCreditRounds(nil)
	if err != nil {
		return fmt.Errorf("reading covered reward rounds: %w", err)
	}
	const batchSize = 2000
	for _, round := range rounds {
		if round.BoundarySlot > slot {
			continue
		}
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			done, err := func() (bool, error) {
				tx := cfg.Database.MetadataTxn(ctx, true)
				defer tx.Release()
				outputs, err := cfg.Database.Metadata().
					ClaimUnfoldedRewardCredits(round.SnapshotEpoch, batchSize, tx.Metadata())
				if err != nil {
					return false, err
				}
				if err := tx.Commit(); err != nil {
					return false, err
				}
				return len(outputs) == 0, nil
			}()
			if err != nil {
				return fmt.Errorf(
					"folding covered reward round %d: %w",
					round.SnapshotEpoch,
					err,
				)
			}
			if done {
				break
			}
		}
	}
	return nil
}
