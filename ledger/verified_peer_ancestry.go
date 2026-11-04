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
	"math"

	"github.com/blinklabs-io/dingo/database"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// VerifiedPeerHeaderAncestry is an ordinary-header path authenticated against
// one immutable ledger publication. Current must be checked while its
// generation is pinned, immediately before consuming Points.
type VerifiedPeerHeaderAncestry struct {
	Points     []ochainsync.Tip
	Generation uint64
	current    func() bool
	retained   func() bool
}

// Current reports whether the publication and chain withdrawal fence captured
// by this proof still hold. It performs no storage reads.
func (p VerifiedPeerHeaderAncestry) Current() bool {
	return p.current != nil && p.current()
}

// Unwithdrawn reports whether the authenticated path has survived every raw
// chain withdrawal. It does not establish freshness of a crypto generation.
func (p VerifiedPeerHeaderAncestry) Unwithdrawn() bool {
	return p.retained != nil && p.retained()
}

// VerifyPeerHeaderAncestry authenticates the complete bounded path from base
// to header. Missing delivery callbacks are not evidence: an unapplied suffix
// must pass peer-relative crypto, state and opcert-counter verification, and
// its older prefix must be exactly on the applied canonical chain.
func (ls *LedgerState) VerifyPeerHeaderAncestry(
	ctx context.Context,
	conn ouroboros.ConnectionId,
	header gledger.BlockHeader,
	base ochainsync.Tip,
	limit int,
) (VerifiedPeerHeaderAncestry, error) {
	var result VerifiedPeerHeaderAncestry
	if header == nil || ls.chain == nil || ls.db == nil || limit < 2 ||
		header.Era().Id == byron.EraIdByron ||
		header.BlockNumber() <= base.BlockNumber ||
		header.BlockNumber()-base.BlockNumber >= uint64(limit) {
		return result, errHeaderVerificationDeferred
	}
	ctx, cancel := context.WithTimeout(ctx, blockfetchBusyTimeout)
	defer cancel()
	withdrawal := ls.chain.WithdrawalGeneration()
	rawTip := ls.chain.Tip()
	var snapshot *tipSnapshot
	generation, err := ls.withStableHeaderVerificationContext(func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		snapshot = ls.loadTipSnapshot()
		if snapshot == nil {
			return errHeaderVerificationContextUnavailable
		}
		if err := ls.validateChainSelectionHeaderCrypto(ctx, conn, header, true); err != nil {
			return err
		}
		target := ochainsync.Tip{
			Point: ocommon.NewPoint(
				header.SlotNumber(),
				header.Hash().Bytes(),
			),
			BlockNumber: header.BlockNumber(),
		}
		var suffix []ochainsync.Tip
		anchor := target
		if !ls.headerApplied(ctx, header) {
			point, headers, found, err := ls.peerHeaderForkPathContext(
				ctx,
				conn,
				header,
			)
			if err != nil {
				return err
			}
			if !found || len(headers) == 0 ||
				!bytes.Equal(
					headers[len(headers)-1].Hash().Bytes(),
					target.Point.Hash,
				) {
				// A queued-parent fallback postpones opcert-counter checks
				// until application and cannot certify the skipped suffix.
				return errHeaderVerificationDeferred
			}
			if opCert, hasOpCert := opCertFromHeader(header); !hasOpCert ||
				opCert == nil {
				return errHeaderVerificationDeferred
			} else {
				ls.forkHeaderNonceCacheMutex.Lock()
				cached, exists := ls.forkHeaderPathCache[connIdKey(conn)]
				complete := exists && bytes.Equal(cached.opCertLastHash, target.Point.Hash)
				ls.forkHeaderNonceCacheMutex.Unlock()
				if !complete {
					return errHeaderVerificationDeferred
				}
			}
			if len(headers) > limit || headers[0].BlockNumber() == 0 {
				return errHeaderVerificationDeferred
			}
			anchor = ochainsync.Tip{
				Point:       point,
				BlockNumber: headers[0].BlockNumber() - 1,
			}
			previous := anchor
			for _, h := range headers {
				if err := ctx.Err(); err != nil {
					return err
				}
				if h.Era().Id == byron.EraIdByron ||
					previous.BlockNumber == math.MaxUint64 || h.BlockNumber() != previous.BlockNumber+1 ||
					h.SlotNumber() <= previous.Point.Slot || !bytes.Equal(h.PrevHash().Bytes(), previous.Point.Hash) {
					return errHeaderVerificationDeferred
				}
				previous = ochainsync.Tip{
					Point: ocommon.NewPoint(
						h.SlotNumber(),
						h.Hash().Bytes(),
					), BlockNumber: h.BlockNumber(),
				}
				suffix = append(suffix, previous)
			}
		}
		prefixBase := base
		if base.BlockNumber > anchor.BlockNumber {
			prefixBase = anchor
		}
		prefix, err := ls.appliedAncestryPrefix(
			ctx,
			prefixBase,
			anchor,
			rawTip,
			snapshot.currentTip,
			limit,
		)
		if err != nil {
			return err
		}
		// A base on the verified fork must be found on that exact suffix;
		// replacing it with a canonical point would erase a real fork.
		if base.BlockNumber > anchor.BlockNumber {
			for index, tip := range suffix {
				if sameAncestryTip(tip, base) {
					result.Points = cloneAncestryTips(suffix[index:])
					return nil
				}
			}
			return errHeaderVerificationDeferred
		}
		if len(prefix) > limit-len(suffix) {
			return errHeaderVerificationDeferred
		}
		result.Points = append(prefix, cloneAncestryTips(suffix)...)
		return nil
	})
	if ctx.Err() != nil {
		return VerifiedPeerHeaderAncestry{}, ctx.Err()
	}
	if err != nil {
		return VerifiedPeerHeaderAncestry{}, err
	}
	if withdrawal != ls.chain.WithdrawalGeneration() ||
		!sameAncestryTip(rawTip, ls.chain.Tip()) || len(result.Points) < 2 {
		return VerifiedPeerHeaderAncestry{}, errHeaderVerificationContextChanged
	}
	result.Generation = generation
	unwithdrawn := func() bool { return ls.chain.WithdrawalGeneration() == withdrawal }
	result.retained = unwithdrawn
	result.current = func() bool {
		return ls.loadTipSnapshot() == snapshot &&
			ls.HeaderCryptoVerificationGenerationCurrent(generation) &&
			unwithdrawn()
	}
	return result, nil
}

func (ls *LedgerState) appliedAncestryPrefix(
	ctx context.Context,
	base, anchor, rawTip, applied ochainsync.Tip,
	limit int,
) ([]ochainsync.Tip, error) {
	if limit < 1 || anchor.BlockNumber > applied.BlockNumber || anchor.Point.Slot > applied.Point.Slot ||
		anchor.BlockNumber < base.BlockNumber ||
		anchor.BlockNumber-base.BlockNumber >= uint64(limit) {
		return nil, errHeaderVerificationDeferred
	}
	txn := ls.db.BlobTxn(false)
	defer txn.Rollback() //nolint:errcheck
	// The in-memory chain can advance before a caller's transaction commits.
	// Requiring its captured tip in this same blob snapshot rejects that view.
	for _, tip := range []ochainsync.Tip{rawTip, applied} {
		id, err := database.BlockIDByPointMetadataTxn(txn, tip.Point)
		if err != nil {
			return nil, errHeaderVerificationDeferred
		}
		point, err := ls.db.BlockPointByIndex(id, txn)
		if err != nil || !pointMatches(point, tip.Point) {
			return nil, errHeaderVerificationDeferred
		}
	}
	count := anchor.BlockNumber - base.BlockNumber + 1
	if count > math.MaxInt {
		return nil, errHeaderVerificationDeferred
	}
	points := make([]ochainsync.Tip, int(count))
	current := anchor
	for index := len(points) - 1; index >= 0; index-- {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		metadata, err := database.BlockMetadataByPointLocalTxn(
			txn,
			current.Point,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: read applied ancestry: %w",
				errHeaderVerificationDeferred,
				err,
			)
		}
		indexed, err := ls.db.BlockPointByIndex(metadata.ID, txn)
		if err != nil || !pointMatches(indexed, current.Point) ||
			metadata.Height != current.BlockNumber || metadata.Type == gledger.BlockTypeByronEbb {
			return nil, errHeaderVerificationDeferred
		}
		points[index] = ochainsync.Tip{
			Point: ocommon.NewPoint(
				current.Point.Slot,
				bytes.Clone(current.Point.Hash),
			),
			BlockNumber: current.BlockNumber,
		}
		if index == 0 {
			if !sameAncestryTip(current, base) {
				return nil, errHeaderVerificationDeferred
			}
			break
		}
		if metadata.ID == 0 || current.BlockNumber == 0 {
			return nil, errHeaderVerificationDeferred
		}
		parent, err := ls.db.BlockPointByIndex(metadata.ID-1, txn)
		if err != nil || parent.Slot >= current.Point.Slot ||
			!bytes.Equal(parent.Hash, metadata.PrevHash) {
			return nil, errHeaderVerificationDeferred
		}
		current = ochainsync.Tip{
			Point:       parent,
			BlockNumber: current.BlockNumber - 1,
		}
	}
	return points, nil
}

func sameAncestryTip(left, right ochainsync.Tip) bool {
	return left.BlockNumber == right.BlockNumber &&
		pointMatches(left.Point, right.Point)
}

func cloneAncestryTips(input []ochainsync.Tip) []ochainsync.Tip {
	result := make([]ochainsync.Tip, len(input))
	for index, tip := range input {
		result[index] = ochainsync.Tip{
			Point: ocommon.NewPoint(
				tip.Point.Slot,
				bytes.Clone(tip.Point.Hash),
			), BlockNumber: tip.BlockNumber,
		}
	}
	return result
}
