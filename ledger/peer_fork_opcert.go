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
	"fmt"
	"maps"

	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

func (ls *LedgerState) validatePeerForkOpCertCounters(
	ctx context.Context,
	connID ouroboros.ConnectionId,
	target gledger.BlockHeader,
) error {
	if _, hasOpCert := opCertFromHeader(target); !hasOpCert {
		return nil
	}
	ancestor, headers, found, err := ls.peerHeaderForkPathContext(
		ctx,
		connID,
		target,
	)
	if err != nil {
		return err
	}
	if !found {
		if ls.chain != nil && ls.chain.HoldsQueuedHeaderHash(
			target.PrevHash().Bytes(),
		) {
			// A linear child of a queued header can lack retained peer ancestry.
			// Its opcert counter is checked when the block is applied.
			return nil
		}
		return fmt.Errorf(
			"%w: candidate opcert ancestry is unavailable for header at slot %d",
			errHeaderVerificationDeferred,
			target.SlotNumber(),
		)
	}
	if len(headers) == 0 || !bytes.Equal(
		headers[len(headers)-1].Hash().Bytes(),
		target.Hash().Bytes(),
	) {
		return fmt.Errorf(
			"%w: candidate opcert ancestry is unavailable for header at slot %d",
			errHeaderVerificationDeferred,
			target.SlotNumber(),
		)
	}

	generation, contextGeneration := ls.currentHeaderVerificationContext()
	key := connIdKey(connID)
	ls.forkHeaderNonceCacheMutex.Lock()
	prefix, ok := ls.forkHeaderPathCache[key]
	ls.forkHeaderNonceCacheMutex.Unlock()
	// A tip-only publication advances generation without changing the
	// consensus context; headerVerdictInContext is the freshness check.
	if !ok ||
		prefix.ancestor.Slot != ancestor.Slot ||
		!bytes.Equal(prefix.ancestor.Hash, ancestor.Hash) ||
		!headerVerdictInContext(
			prefix.generation,
			contextGeneration,
			generation,
		) {
		return fmt.Errorf(
			"%w: candidate opcert state changed during header verification",
			errHeaderVerificationDeferred,
		)
	}

	counters := clonePeerOpCertCounters(prefix.opCertCounters)
	startIndex := 0
	if len(prefix.opCertLastHash) > 0 {
		for index, header := range headers {
			if !bytes.Equal(
				header.Hash().Bytes(),
				prefix.opCertLastHash,
			) {
				continue
			}
			startIndex = index + 1
			break
		}
		if startIndex == 0 {
			counters = make(map[lcommon.PoolKeyHash]opCertCounterSnapshot)
		}
	}
	if startIndex == len(headers) &&
		bytes.Equal(prefix.opCertLastHash, target.Hash().Bytes()) {
		return nil
	}

	for _, header := range headers[startIndex:] {
		opCert, hasOpCert := opCertFromHeader(header)
		if hasOpCert {
			if opCert == nil {
				return fmt.Errorf(
					"%w: candidate header at slot %d has no decoded opcert",
					errHeaderVerificationDeferred,
					header.SlotNumber(),
				)
			}
			poolKeyHash := lcommon.PoolKeyHash(header.IssuerVkey().Hash())
			stored, hasCounter := counters[poolKeyHash]
			if !hasCounter {
				stored, err = ls.peerForkAncestorOpCertCounter(
					ctx,
					poolKeyHash,
					ancestor.Slot,
				)
				if err != nil {
					return fmt.Errorf(
						"%w: resolve candidate opcert counter for pool %x: %w",
						errHeaderVerificationDeferred,
						poolKeyHash.Bytes(),
						err,
					)
				}
			}
			if err := validateOpCertCounter(
				stored.sequence,
				stored.found,
				opCert.IssueNumber,
				opCertNoGapRuleApplies(header.Era().Id),
			); err != nil {
				return fmt.Errorf(
					"candidate header at slot %d pool %x: %w",
					header.SlotNumber(),
					poolKeyHash.Bytes(),
					err,
				)
			}
			counters[poolKeyHash] = opCertCounterSnapshot{
				sequence: opCert.IssueNumber,
				found:    true,
			}
		}
	}

	if !ls.headerCryptoVerificationGenerationCurrent(generation) {
		return fmt.Errorf(
			"%w: candidate opcert state changed before commit",
			errHeaderVerificationDeferred,
		)
	}
	ls.forkHeaderNonceCacheMutex.Lock()
	defer ls.forkHeaderNonceCacheMutex.Unlock()
	current, ok := ls.forkHeaderPathCache[key]
	if !ok || current.generation != prefix.generation ||
		current.ancestor.Slot != ancestor.Slot ||
		!bytes.Equal(current.ancestor.Hash, ancestor.Hash) ||
		len(current.headers) == 0 ||
		!bytes.Equal(
			current.headers[len(current.headers)-1].Hash().Bytes(),
			target.Hash().Bytes(),
		) {
		return fmt.Errorf(
			"%w: candidate opcert path changed before commit",
			errHeaderVerificationDeferred,
		)
	}
	if bytes.Equal(current.opCertLastHash, target.Hash().Bytes()) {
		return nil
	}
	if !bytes.Equal(current.opCertLastHash, prefix.opCertLastHash) {
		return fmt.Errorf(
			"%w: candidate opcert prefix changed before commit",
			errHeaderVerificationDeferred,
		)
	}
	current.opCertLastHash = append([]byte(nil), target.Hash().Bytes()...)
	current.opCertCounters = counters
	ls.forkHeaderPathCache[key] = current
	return nil
}

func (ls *LedgerState) peerForkAncestorOpCertCounter(
	ctx context.Context,
	poolKeyHash lcommon.PoolKeyHash,
	ancestorSlot uint64,
) (opCertCounterSnapshot, error) {
	txn := ls.db.Transaction(ctx, false)
	defer txn.Release()

	mithrilSlot := ls.mithrilLedgerSlotSnapshot()
	if mithrilSlot > 0 {
		if ancestorSlot < mithrilSlot {
			return opCertCounterSnapshot{}, fmt.Errorf(
				"candidate ancestor slot %d is before Mithril boundary %d",
				ancestorSlot,
				mithrilSlot,
			)
		}
		sequence, found, err := ls.db.LatestPoolOpCertSequenceInRange(
			ctx,
			poolKeyHash,
			mithrilSlot,
			ancestorSlot,
			txn,
		)
		if err != nil {
			return opCertCounterSnapshot{}, err
		}
		if found {
			return opCertCounterSnapshot{
				sequence: sequence,
				found:    true,
			}, nil
		}
		imported, err := ls.db.PoolOpCertSequencesExistAtSlot(
			ctx,
			mithrilSlot,
			txn,
		)
		if err != nil {
			return opCertCounterSnapshot{}, err
		}
		if !imported {
			return opCertCounterSnapshot{}, errOpCertBaselineNotImported
		}
		return opCertCounterSnapshot{}, nil
	}

	sequence, found, err := ls.db.LatestPoolOpCertSequenceAtOrBefore(
		ctx,
		poolKeyHash,
		ancestorSlot,
		txn,
	)
	if err != nil {
		return opCertCounterSnapshot{}, err
	}
	return opCertCounterSnapshot{sequence: sequence, found: found}, nil
}

func clonePeerOpCertCounters(
	counters map[lcommon.PoolKeyHash]opCertCounterSnapshot,
) map[lcommon.PoolKeyHash]opCertCounterSnapshot {
	if len(counters) == 0 {
		return make(map[lcommon.PoolKeyHash]opCertCounterSnapshot)
	}
	clone := make(map[lcommon.PoolKeyHash]opCertCounterSnapshot, len(counters))
	maps.Copy(clone, counters)
	return clone
}
