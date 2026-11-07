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
	"encoding/hex"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/kes"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type noncePrefixBodyReads struct {
	blob.BlobStore
	point ocommon.Point
	reads atomic.Uint64
}

func (s *noncePrefixBodyReads) GetBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	if slot == s.point.Slot && bytes.Equal(hash, s.point.Hash) {
		s.reads.Add(1)
	}
	return s.BlobStore.GetBlock(txn, slot, hash)
}

func TestPeerHeaderFrozenCandidateUsesStoredPrefix(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		length        uint64
		prefix        bool
		wantBodyReads bool
	}{
		{"stored prefix", 10_000, true, false},
		{"missing prefix reconstructs", 10_000, false, true},
		{"whole epoch is frozen", 1_000, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := newHighFreqShelleyGenesisCfg(t)
			cfg.ShelleyGenesisHash = hex.EncodeToString(
				bytes.Repeat([]byte{0x07}, 32),
			)
			sourceNonce := bytes.Repeat([]byte{0x21}, 32)
			epoch := models.Epoch{
				EpochId:        5,
				StartSlot:      0,
				SlotLength:     1_000,
				LengthInSlots:  uint(tc.length),
				EraId:          eras.BabbageEraDesc.Id,
				Nonce:          sourceNonce,
				EvolvingNonce:  sourceNonce,
				CandidateNonce: sourceNonce,
			}
			// Babbage freezes 3k/f slots before the boundary. With k=432 and
			// f=99/100 the ceiling is 1310; a block at the cutoff is excluded.
			cutoff := uint64(0)
			if tc.length > 1310 {
				cutoff = tc.length - 1310
			}
			preSlot, cutoffSlot, anchorSlot, forkSlot := uint64(
				10,
			), uint64(
				20,
			), uint64(
				30,
			), uint64(
				40,
			)
			if cutoff > 0 {
				preSlot = cutoff - 1
				cutoffSlot = cutoff
				anchorSlot = cutoff + 2
				forkSlot = cutoff + 4
			}
			pre := createTestBlockWithNonceAtSlots(
				t,
				[32]byte{80},
				sourceNonce,
				preSlot,
				preSlot+1,
				make([]byte, 32),
				tamperNone,
			)
			preStored := completeNoncePrefixBlock(t, pre, [32]byte{80})
			n := pre.block.BlockNumber() + 1
			atCutoff := createTestBlockWithNonceAtSlotsAndBlockNumber(
				t,
				[32]byte{81},
				sourceNonce,
				cutoffSlot,
				cutoffSlot+1,
				pre.block.Hash().Bytes(),
				&n,
				tamperNone,
			)
			cutoffStored := completeNoncePrefixBlock(t, atCutoff, [32]byte{81})
			n++
			anchor := createTestBlockWithNonceAtSlotsAndBlockNumber(
				t,
				[32]byte{82},
				sourceNonce,
				anchorSlot,
				anchorSlot+1,
				atCutoff.block.Hash().Bytes(),
				&n,
				tamperNone,
			)
			anchorStored := completeNoncePrefixBlock(t, anchor, [32]byte{82})
			n++
			tail := createTestBlockWithNonceAtSlotsAndBlockNumber(
				t,
				[32]byte{83},
				sourceNonce,
				tc.length-1,
				tc.length,
				anchor.block.Hash().Bytes(),
				&n,
				tamperNone,
			)
			tailStored := completeNoncePrefixBlock(t, tail, [32]byte{83})
			nonce := sourceNonce
			nonces := make([][]byte, 3)
			for i, block := range []*testBlockResult{pre, atCutoff, anchor} {
				var err error
				nonce, err = eras.BabbageEraDesc.CalculateEtaVFunc(
					cfg,
					nonce,
					block.block,
				)
				require.NoError(t, err)
				nonces[i] = nonce
			}
			candidate := sourceNonce
			if cutoff > 0 {
				candidate = nonces[0]
			}
			db, err := dbtest.NewDatabase(
				t,
				&database.Config{DataDir: t.TempDir()},
			)
			require.NoError(t, err)
			t.Cleanup(func() { dbtest.CloseDatabase(db) })
			cm, err := chain.NewManager(t.Context(), db, nil)
			require.NoError(t, err)
			for i, block := range []gledger.Block{preStored, cutoffStored, anchorStored, tailStored} {
				require.NoError(
					t,
					cm.PrimaryChain().AddLocalBlock(t.Context(), block),
				)
				if i < 3 && (i != 0 || tc.prefix) {
					require.NoError(
						t,
						db.SetBlockNonce(
							block.Hash().Bytes(),
							block.SlotNumber(),
							nonces[i],
							false,
							nil,
						),
					)
				}
			}
			anchorPoint := ocommon.NewPoint(
				anchorSlot,
				anchor.block.Hash().Bytes(),
			)
			ls := &LedgerState{
				db:           db,
				chain:        cm.PrimaryChain(),
				epochCache:   []models.Epoch{epoch},
				currentEra:   eras.BabbageEraDesc,
				currentEpoch: epoch,
				currentTip: ochainsync.Tip{
					Point:       anchorPoint,
					BlockNumber: anchor.block.BlockNumber(),
				},
				currentTipBlockNonce: nonces[2],
				config: LedgerStateConfig{
					CardanoNodeConfig: cfg,
					Logger: slog.New(
						slog.NewTextHandler(io.Discard, nil),
					),
				},
			}
			ls.slotsPerKESPeriod.Store(anchor.slotsPerKesPeriod)
			n = anchor.block.BlockNumber() + 1
			fork := createTestBlockWithNonceAtSlotsAndBlockNumber(
				t,
				[32]byte{84},
				sourceNonce,
				forkSlot,
				forkSlot+1,
				anchorPoint.Hash,
				&n,
				tamperNone,
			)
			ls.config.PeerHeaderLookupFunc = func(_ ouroboros.ConnectionId, hash []byte) (ChainsyncEvent, []byte, bool) {
				if !bytes.Equal(hash, fork.block.Hash().Bytes()) {
					return ChainsyncEvent{}, nil, false
				}
				return ChainsyncEvent{
					Point: ocommon.NewPoint(
						forkSlot,
						fork.block.Hash().Bytes(),
					),
					BlockHeader:                        fork.block.Header(),
					HeaderCryptoVerificationGeneration: ls.CurrentHeaderCryptoVerificationGeneration(),
				}, anchorPoint.Hash, true
			}
			ls.publishSnapshotsLocked()
			extra, err := ls.recordedExtraEntropyForEpoch(
				6,
				eras.BabbageEraDesc.Id,
			)
			require.NoError(t, err)
			targetNonce, err := assembleEpochNonce(candidate, nil, extra)
			require.NoError(t, err)
			n++
			target := createTestBlockWithNonceAtSlotsAndBlockNumber(
				t,
				[32]byte{85},
				targetNonce,
				tc.length,
				tc.length+100,
				fork.block.Hash().Bytes(),
				&n,
				tamperNone,
			)
			for _, block := range []*testBlockResult{fork, target} {
				seedBlockPoolRegistration(t, db, block.block)
				epochID := uint64(4)
				if block == target {
					epochID = 5
				}
				pool := block.block.IssuerVkey().Hash()
				seedPoolStakeSnapshot(t, db, epochID, pool[:], 1_000_000_000)
			}
			base := db.Blob()
			counter := &noncePrefixBodyReads{
				BlobStore: base,
				point:     ocommon.NewPoint(preSlot, pre.block.Hash().Bytes()),
			}
			db.SetBlobStore(counter)
			defer db.SetBlobStore(base)
			require.NoError(
				t,
				ls.ValidatePeerChainSelectionHeaderCrypto(
					ouroboros.ConnectionId{},
					target.block.Header(),
				),
			)
			if tc.wantBodyReads {
				require.Positive(t, counter.reads.Load())
			} else {
				require.Zero(t, counter.reads.Load(), "a stored frozen prefix must not reconstruct transaction bodies")
			}
		})
	}
}

// Header verification fixtures omit bodies. Nonce reconstruction must also
// accept the stored blocks, so attach an empty body with its real body hash
// and re-sign the changed header using the fixture's deterministic KES key.
func completeNoncePrefixBlock(
	t testing.TB,
	fixture *testBlockResult,
	seed [32]byte,
) *babbage.BabbageBlock {
	t.Helper()
	header := *fixture.block.header
	header.SetCbor(nil)
	header.Body.SetCbor(nil)
	parts := []cbor.RawMessage{{0x80}, {0x80}, {0xa0}, {0x80}}
	hashes := make([]byte, 0, 128)
	for _, part := range parts {
		h := lcommon.Blake2b256Hash(part)
		hashes = append(hashes, h.Bytes()...)
	}
	header.Body.BlockBodyHash = lcommon.Blake2b256Hash(hashes)
	header.Body.BlockBodySize = 4
	body, err := cbor.Encode(&header.Body)
	require.NoError(t, err)
	header.Body.SetCbor(body)
	seed[0] ^= 0xaa
	key, public, err := kes.KeyGen(kes.CardanoKesDepth, seed[:])
	require.NoError(t, err)
	require.Equal(t, header.Body.OpCert.HotVkey, public)
	header.Signature, err = kes.Sign(key, 0, body)
	require.NoError(t, err)
	encodedHeader, err := cbor.Encode(&header)
	require.NoError(t, err)
	header.SetCbor(encodedHeader)
	fixture.block.header = &header
	fields := []cbor.RawMessage{encodedHeader}
	fields = append(fields, parts...)
	raw, err := cbor.Encode(fields)
	require.NoError(t, err)
	block, err := babbage.NewBabbageBlockFromCbor(raw)
	require.NoError(t, err)
	require.Equal(t, fixture.block.Hash(), block.Hash())
	return block
}

func TestFrozenCandidatePrefixFollowsIntersectionAncestry(t *testing.T) {
	t.Parallel()
	for _, era := range []struct {
		name   string
		id     uint
		window uint64
	}{
		{"Babbage", eras.BabbageEraDesc.Id, 1310},
		{"Dijkstra", eras.DijkstraEraDesc.Id, 1746},
	} {
		t.Run(era.name, func(t *testing.T) {
			t.Parallel()
			for _, previousEpoch := range []bool{false, true} {
				name := "prefix in source epoch"
				if previousEpoch {
					name = "no source block before cutoff"
				}
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					cfg := newHighFreqShelleyGenesisCfg(t)
					cfg.ShelleyGenesisHash = hex.EncodeToString(
						bytes.Repeat([]byte{0x07}, 32),
					)
					carried := bytes.Repeat([]byte{0x21}, 32)
					epoch := models.Epoch{
						EpochId: 5, StartSlot: 10_000, LengthInSlots: 10_000,
						EraId: era.id, Nonce: carried, CandidateNonce: carried,
					}
					cutoff := uint64(20_000) - era.window
					prefixSlot := cutoff - 2
					if previousEpoch {
						prefixSlot = epoch.StartSlot - 1
					}
					prefix := models.Block{
						Slot: prefixSlot,
						Hash: bytes.Repeat([]byte{1}, 32),
						Type: 8,
					}
					excluded := models.Block{
						Slot:     cutoff,
						Hash:     bytes.Repeat([]byte{2}, 32),
						PrevHash: prefix.Hash,
						Type:     8,
					}
					ancestor := models.Block{
						Slot:     cutoff + 10,
						Hash:     bytes.Repeat([]byte{3}, 32),
						PrevHash: excluded.Hash,
						Type:     8,
					}
					// A later retained sibling must not replace the intersection's prefix.
					sibling := models.Block{
						Slot:     cutoff - 1,
						Hash:     bytes.Repeat([]byte{4}, 32),
						PrevHash: prefix.Hash,
						Type:     8,
					}
					db, err := dbtest.NewDatabase(
						t,
						&database.Config{DataDir: t.TempDir()},
					)
					require.NoError(t, err)
					t.Cleanup(func() { dbtest.CloseDatabase(db) })
					for _, block := range []models.Block{prefix, excluded, ancestor, sibling} {
						require.NoError(t, db.BlockCreate(block, nil))
						require.NoError(
							t,
							db.SetBlockNonce(
								block.Hash,
								block.Slot,
								bytes.Repeat(block.Hash[:1], 32),
								false,
								nil,
							),
						)
					}
					ls := &LedgerState{
						db:     db,
						config: LedgerStateConfig{CardanoNodeConfig: cfg},
					}
					actualCutoff, ready := ls.nextEpochNonceReadyCutoffSlot(
						epoch,
					)
					require.True(t, ready)
					require.Equal(t, cutoff, actualCutoff)
					got, err := ls.frozenCandidateNonceFromAncestor(
						t.Context(),
						epoch,
						ocommon.NewPoint(ancestor.Slot, ancestor.Hash),
					)
					require.NoError(t, err)
					expected := bytes.Repeat([]byte{1}, 32)
					if previousEpoch {
						expected = bytes.Clone(carried)
					}
					require.Equal(
						t,
						expected,
						got,
						"only the intersection's ancestors strictly before the era cutoff can contribute",
					)
					got[0] ^= 0xff
					again, err := ls.frozenCandidateNonceFromAncestor(
						t.Context(),
						epoch,
						ocommon.NewPoint(ancestor.Slot, ancestor.Hash),
					)
					require.NoError(t, err)
					require.Equal(
						t,
						expected,
						again,
						"returned nonce must not alias stored or cached state",
					)
				})
			}
		})
	}
}
