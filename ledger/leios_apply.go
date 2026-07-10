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
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// leiosEndorserBlockReferencer is implemented by a block header that announces
// a Leios endorser block via its header extension. As of prototype-2026w27 that
// is the leios_announcement field ([announced_eb, announced_eb_size]).
type leiosEndorserBlockReferencer interface {
	LeiosAnnouncement() (lcommon.Blake2b256, uint64, bool)
}

// Compile-time guard: the Dijkstra header must satisfy the announcer interface.
// A type-assertion against this interface compiles even when the header no
// longer implements it (it just returns ok=false at runtime), which previously
// let a header-accessor rename silently disable endorser-block application.
var _ leiosEndorserBlockReferencer = (*dijkstra.DijkstraBlockHeader)(nil)

// leiosEndorserBlockCertifier is implemented by a block header that can certify
// a previously announced endorser block. As of prototype-2026w27 a certifying
// ranking block (CertRB) carries a leios_certificate and certifies the endorser
// block announced by its parent (prevHash); the flag rides on the header's
// leios_certified extension field.
type leiosEndorserBlockCertifier interface {
	LeiosCertified() (certified bool, present bool)
}

var _ leiosEndorserBlockCertifier = (*dijkstra.DijkstraBlockHeader)(nil)

// applyEndorserBlock decodes a Leios endorser block's standalone transactions,
// persists them as a standalone blob, and — on the CIP-conformant path
// (LeiosApplyEndorserBlockTxs) — applies them to the ledger ahead of the ranking
// block that references it, so the endorser-resident outputs the ranking block's
// transactions spend are present in the UTxO set. On the Haskell-conformant path
// (the Musashi prototype) the blob is still stored (for serving and the
// node-to-client inline view), and non-UTxO metadata that Dingo uses for ledger
// queries is recorded, but the transactions are not applied to the UTxO.
// It returns the number of transactions applied to the UTxO (0 on the
// Haskell-conformant path, or when the CIP path finds every transaction was
// already applied).
//
// Endorser-block transactions are not part of any chain block, so — mirroring
// the genesis path (buildGenesisBlockCbor / SetGenesisCbor) — their CBOR is
// persisted as a standalone blob keyed by the endorser block's (slot, hash) and
// referenced by DOFF offsets, after which resolution works through the normal
// TieredCborCache cold-extract path. Crucially, the transactions' ledger
// effects (metadata rows, spent inputs, produced UTxOs) are recorded under the
// RANKING block's point (rbPoint), not the endorser block's: a rollback of the
// ranking block must remove them, and the ranking block is what admits the
// endorser block to the chain.
//
// It returns the number of transactions applied and the Conway donation total
// from accepted endorser-block transactions. Decode/build failures happen
// before storage is mutated and callers may treat them as best-effort. Once
// the endorser blob or transaction rows start writing, any error is wrapped in
// leiosEndorserBlockStorageError so callers can abort the outer transaction
// instead of committing a partial endorser-block application.
func (ls *LedgerState) applyEndorserBlock(
	txn *database.Txn,
	rbPoint ocommon.Point,
	rbBlockNumber uint64,
	ebSlot uint64,
	ebHashBytes []byte,
	rawTxs []cbor.RawMessage,
) (int, uint64, error) {
	if len(rawTxs) == 0 {
		return 0, 0, nil
	}
	if len(ebHashBytes) != lcommon.Blake2b256Size {
		return 0, 0, fmt.Errorf(
			"endorser block hash must be %d bytes, got %d",
			lcommon.Blake2b256Size,
			len(ebHashBytes),
		)
	}
	var ebHash [lcommon.Blake2b256Size]byte
	copy(ebHash[:], ebHashBytes)

	// Decode each standalone endorser transaction, capturing its body CBOR
	// (the first array element) for the transaction-offset entry.
	txs := make([]lcommon.Transaction, 0, len(rawTxs))
	bodyCbors := make([][]byte, 0, len(rawTxs))
	for i, raw := range rawTxs {
		// leios-fetch carries each endorser transaction CBOR-in-CBOR: the
		// tx_list entry is a CBOR byte string wrapping the transaction's own
		// CBOR (LeiosTx = encodeBytes(txCbor)). Unwrap it to the inner
		// transaction bytes before decoding. (A non-byte-string entry — major
		// type != 2 — is already the bare transaction.)
		txCbor := []byte(raw)
		if len(txCbor) > 0 && txCbor[0]>>5 == 2 {
			var inner []byte
			if _, err := cbor.Decode(txCbor, &inner); err != nil {
				return 0, 0, fmt.Errorf("unwrap endorser tx %d: %w", i, err)
			}
			txCbor = inner
		}
		var elems []cbor.RawMessage
		if _, err := cbor.Decode(txCbor, &elems); err != nil {
			return 0, 0, fmt.Errorf("decode endorser tx %d envelope: %w", i, err)
		}
		if len(elems) < 2 {
			return 0, 0, fmt.Errorf(
				"endorser tx %d has %d elements, want >= 2",
				i,
				len(elems),
			)
		}
		// An endorser block referenced by a Dijkstra ranking block is
		// Dijkstra-era, so decode its transactions as Dijkstra directly.
		// DetermineTransactionType is heuristic and cannot reliably identify a
		// bare standalone transaction without block/era context (it returns
		// "unknown transaction type" for these), so it must not be used here.
		tx, err := ledger.NewTransactionFromCbor(ledger.TxTypeDijkstra, txCbor)
		if err != nil {
			return 0, 0, fmt.Errorf("decode endorser tx %d: %w", i, err)
		}
		txs = append(txs, tx)
		bodyCbors = append(bodyCbors, []byte(elems[0]))
	}

	// Reject repeated endorser transactions before recording ledger data. The
	// CIP path compacts the block to transactions that still need UTxO apply;
	// the Musashi metadata-only path keeps the blob intact for serving while
	// using the indexes to suppress duplicate metadata side effects.
	keepIndexes, err := ls.deduplicateEndorserBlockTransactionIndexes(txs, txn)
	if err != nil {
		return 0, 0, err
	}
	if ls.config.LeiosApplyEndorserBlockTxs {
		if len(keepIndexes) == 0 {
			return 0, 0, nil
		}
		keptTxs := txs[:0]
		keptBodies := bodyCbors[:0]
		for _, idx := range keepIndexes {
			keptTxs = append(keptTxs, txs[idx])
			keptBodies = append(keptBodies, bodyCbors[idx])
		}
		txs = keptTxs
		bodyCbors = keptBodies
	}

	// Build the endorser-block blob and its offsets, then persist the blob
	// under (ebSlot, ebHash) so cold-extract can resolve the DOFF refs.
	blob, offsets, err := buildEndorserBlockBlob(txs, bodyCbors, ebSlot, ebHash)
	if err != nil {
		return 0, 0, fmt.Errorf("build endorser block blob: %w", err)
	}
	if err := ls.db.SetGenesisCbor(ebSlot, ebHash[:], blob, txn); err != nil {
		return 0, 0, &leiosEndorserBlockStorageError{
			err: fmt.Errorf("store endorser block blob: %w", err),
		}
	}

	delta := NewLedgerDelta(rbPoint, uint(dijkstra.EraIdDijkstra), rbBlockNumber)
	defer delta.Release()
	delta.Offsets = offsets
	if ls.config.LeiosApplyEndorserBlockTxs {
		for i, tx := range txs {
			delta.addTransaction(tx, i)
		}
	} else {
		for _, idx := range keepIndexes {
			delta.addTransaction(txs[idx], idx)
		}
	}

	// Haskell-conformant path (Musashi prototype-2026w27): the endorser block is
	// stored above for serving and the node-to-client inline view, but its
	// transactions are NOT applied to the UTxO. Dingo still records transaction
	// metadata, certificates, and governance because header verification and
	// other ledger queries are backed by the metadata DB.
	if !ls.config.LeiosApplyEndorserBlockTxs {
		if err := delta.applyTransactionMetadataOnlyWithoutRecordingDonations(ls, txn); err != nil {
			return 0, 0, &leiosEndorserBlockStorageError{
				err: fmt.Errorf(
					"record endorser block transaction metadata: %w",
					err,
				),
			}
		}
		return 0, delta.donation, nil
	}

	// CIP-conformant path: apply the endorser transactions as a delta recorded
	// under the ranking block's point (so a rollback removes them), with offsets
	// pointing into the endorser-block blob.
	if err := delta.applyWithoutRecordingDonations(ls, txn); err != nil {
		return 0, 0, &leiosEndorserBlockStorageError{
			err: fmt.Errorf("apply endorser block transactions: %w", err),
		}
	}
	return len(txs), delta.donation, nil
}

func (ls *LedgerState) deduplicateEndorserBlockTransactionIndexes(
	txs []lcommon.Transaction,
	txn *database.Txn,
) ([]int, error) {
	if len(txs) == 0 {
		return nil, nil
	}
	hashes := make([][]byte, len(txs))
	for i, tx := range txs {
		hashes[i] = tx.Hash().Bytes()
	}
	existing, err := ls.db.GetTransactionsByHashes(hashes, txn)
	if err != nil {
		return nil, fmt.Errorf("dedup endorser transactions: %w", err)
	}
	skip := make(map[string]struct{}, len(existing))
	for _, tx := range existing {
		if len(tx.Hash) == 0 {
			continue
		}
		skip[string(tx.Hash)] = struct{}{}
	}
	seen := make(map[string]struct{}, len(txs))
	keepIndexes := make([]int, 0, len(txs))
	for i, tx := range txs {
		hashKey := string(tx.Hash().Bytes())
		if _, dup := skip[hashKey]; dup {
			continue
		}
		if _, dup := seen[hashKey]; dup {
			continue
		}
		seen[hashKey] = struct{}{}
		keepIndexes = append(keepIndexes, i)
	}
	return keepIndexes, nil
}

type leiosEndorserBlockStorageError struct {
	err error
}

func (e *leiosEndorserBlockStorageError) Error() string {
	return e.err.Error()
}

func (e *leiosEndorserBlockStorageError) Unwrap() error {
	return e.err
}

// buildEndorserBlockBlob lays out a standalone CBOR blob holding, for each
// endorser transaction, its body CBOR followed by each produced output's CBOR,
// recording the byte ranges as DOFF offsets keyed by (ebSlot, ebHash). The blob
// is not a chain block — cold-extract only slices it by offset/length — so a
// flat concatenation with precise offsets is sufficient.
func buildEndorserBlockBlob(
	txs []lcommon.Transaction,
	bodyCbors [][]byte,
	ebSlot uint64,
	ebHash [lcommon.Blake2b256Size]byte,
) ([]byte, *database.BlockIngestionResult, error) {
	var buf bytes.Buffer
	result := &database.BlockIngestionResult{
		TxOffsets:   make(map[[32]byte]database.CborOffset, len(txs)),
		UtxoOffsets: make(map[database.UtxoRef]database.CborOffset),
	}
	writeRange := func(b []byte) (uint32, uint32, error) {
		off := buf.Len()
		if off > math.MaxUint32 || len(b) > math.MaxUint32 {
			return 0, 0, errors.New("endorser block blob offset out of uint32 range")
		}
		buf.Write(b)
		//nolint:gosec // bounds checked above
		return uint32(off), uint32(len(b)), nil
	}
	for i, tx := range txs {
		var txHash [32]byte
		copy(txHash[:], tx.Hash().Bytes())
		off, length, err := writeRange(bodyCbors[i])
		if err != nil {
			return nil, nil, err
		}
		result.TxOffsets[txHash] = database.CborOffset{
			BlockSlot:  ebSlot,
			BlockHash:  ebHash,
			ByteOffset: off,
			ByteLength: length,
		}
		for _, utxo := range tx.Produced() {
			outCbor := utxo.Output.Cbor()
			if len(outCbor) == 0 {
				enc, err := cbor.Encode(utxo.Output)
				if err != nil {
					return nil, nil, fmt.Errorf("encode endorser output: %w", err)
				}
				outCbor = enc
			}
			off, length, err := writeRange(outCbor)
			if err != nil {
				return nil, nil, err
			}
			result.UtxoOffsets[database.UtxoRef{
				TxId:      txHash,
				OutputIdx: utxo.Id.Index(),
			}] = database.CborOffset{
				BlockSlot:  ebSlot,
				BlockHash:  ebHash,
				ByteOffset: off,
				ByteLength: length,
			}
		}
	}
	return buf.Bytes(), result, nil
}

// ensureReferencedEndorserBlocks gates delivery of a batch of blocks to
// ledgerProcessBlock on the availability of the Leios endorser blocks they
// reference. The prototype produces an endorser block and the ranking block
// that endorses it in the same slot and diffuses them together, so the ranking
// block routinely reaches the ledger a few milliseconds ahead of its endorser
// block; without this gate applyEndorserBlock always misses the cache and the
// endorser-resident outputs are never added before the ranking block spends
// them.
//
// The wait window is EndorserBlockWaitSlots (the pipeline timing's
// CertifyByDeadlineSlots, the bound for when a referenced endorser block is
// actually available to fetch) converted to wall-clock via the Shelley slot
// length, not a hardcoded duration. Callers invoke this before opening the
// block-processing DB transaction, so the wait never holds a transaction open.
//
// Referenced endorser blocks that are not cached are handled by where the
// ranking block sits relative to the live head:
//
//   - Near the head (within the wait window): the relay co-produces and
//     diffuses the endorser block with its ranking block, so it is already
//     being pushed; wait for the in-flight leios-notify/leios-fetch to cache
//     it, with an active by-point fetch as a fallback if the window elapses.
//   - Historical backlog (well below the head, e.g. during a from-scratch
//     catch-up): the relay does not diffuse these, but it does serve any
//     endorser block by point on demand, so actively fetch them -- in parallel
//     across the available relay connections -- and apply the endorser-resident
//     outputs instead of leaving the UTxO set incomplete and trusting the
//     chain. This is what lets a from-scratch sync build a full UTxO set.
func (ls *LedgerState) ensureReferencedEndorserBlocks(
	ctx context.Context,
	blocks []ledger.Block,
) {
	if ls.config.EndorserBlockProvider == nil ||
		ls.config.EndorserBlockWaitSlots == 0 {
		return
	}
	slotLen := ls.shelleySlotLength()
	if slotLen <= 0 {
		// Without a known slot length the slot-denominated diffusion window
		// cannot be converted to wall-clock; skip the wait rather than guess.
		return
	}
	//nolint:gosec // EndorserBlockWaitSlots is a small protocol window
	timeout := time.Duration(ls.config.EndorserBlockWaitSlots) * slotLen
	// Cache re-check cadence (polling granularity, not a protocol parameter):
	// a fraction of a slot so arrival is noticed promptly, floored at 1ms so
	// the ticker interval is always positive.
	poll := max(slotLen/10, time.Millisecond)
	// wallSlot is the current wall-clock slot (the live head). A block more than
	// the wait window below it is settled backlog.
	wallSlot, wallErr := ls.CurrentSlot()
	// Index each block's announced endorser block by the block's own hash so a
	// certifying ranking block can resolve the endorser block its parent
	// announced without a store round-trip (the parent is normally in the same
	// batch, immediately before it on the chain).
	infos := make([]leiosBlockInfo, len(blocks))
	annByHash := make(map[string]leiosEbRef, len(blocks))
	for i, blk := range blocks {
		infos[i] = leiosBlockInfoFrom(blk)
		if infos[i].announces {
			annByHash[infos[i].hash] = leiosEbRef{
				slot: infos[i].slot,
				hash: infos[i].ebHash,
			}
		}
	}
	// On the Haskell-conformant (Musashi) path, settled-backlog fetches are
	// certificate-driven; on the CIP path they stay announcement-driven, so the
	// CIP backfill is unchanged.
	certDrivenHistorical := !ls.config.LeiosApplyEndorserBlockTxs
	if certDrivenHistorical {
		// Resolve CertRB parents that fall outside this batch from the block
		// store, so a certifying ranking block at a batch boundary still fetches
		// its endorser block. The parent (an already-applied ancestor) is stored.
		for _, info := range infos {
			if !info.certifies {
				continue
			}
			if _, ok := annByHash[info.prevHash]; ok {
				continue
			}
			if ls.db == nil {
				continue
			}
			parent, err := ls.BlockByHash([]byte(info.prevHash))
			if err != nil {
				continue
			}
			if ebHash, ok := leiosAnnouncementHashFromBlockCbor(parent.Cbor); ok {
				annByHash[info.prevHash] = leiosEbRef{
					slot: parent.Slot,
					hash: ebHash,
				}
			}
		}
	}
	cached := func(ebHash lcommon.Blake2b256) bool {
		_, _, ok := ls.config.EndorserBlockProvider(ebHash.Bytes())
		return ok
	}
	backfill, tipWait := classifyEndorserBlockFetches(
		infos,
		annByHash,
		wallSlot,
		wallErr == nil,
		ls.config.EndorserBlockWaitSlots,
		certDrivenHistorical,
		cached,
	)
	// Historical backlog: ensure a by-point fetch is in flight for each
	// referenced endorser block (prefetchBatchEndorserBlocks has usually already
	// started them for the whole read batch), then wait for it to land in the
	// cache. The fetches run concurrently in the background pool; the waits below
	// are mostly cache hits, so this does not serialize catch-up on fetch latency
	// the way a per-chunk barrier did.
	if len(backfill) > 0 && ls.leiosBackfill != nil {
		for _, r := range backfill {
			ls.leiosBackfill.spawn(r)
		}
		for _, r := range backfill {
			if _, _, ok := ls.config.EndorserBlockProvider(r.hash.Bytes()); ok {
				continue
			}
			ls.leiosBackfill.awaitFetch(ctx, r, poll)
		}
	}
	for _, r := range tipWait {
		if _, _, ok := ls.config.EndorserBlockProvider(r.hash.Bytes()); ok {
			continue
		}
		ls.waitForEndorserBlock(ctx, r.slot, r.hash, timeout, poll)
		if ls.config.EndorserBlockFetcher == nil {
			continue
		}
		if _, _, ok := ls.config.EndorserBlockProvider(r.hash.Bytes()); !ok {
			if err := ls.config.EndorserBlockFetcher(
				r.slot,
				r.hash.Bytes(),
			); err != nil {
				ls.config.Logger.Debug(
					"endorser block tip fetch fallback failed",
					"component", "ledger",
					"slot", r.slot,
					"eb_hash", r.hash.String(),
					"error", err,
				)
			}
		}
	}
}

// leiosEbRef pairs a ranking block's slot with the hash of the endorser block
// it references. The endorser block shares the ranking block's slot.
type leiosEbRef struct {
	slot uint64
	hash lcommon.Blake2b256
}

// leiosBlockInfo is the subset of a ranking block the endorser-block fetch
// policy needs: its identity (hash/prevHash/slot), the endorser block it
// announces (if any), and whether it certifies its parent's announced endorser
// block. hash and prevHash are the raw block-hash bytes as strings so they can
// key a map.
type leiosBlockInfo struct {
	hash      string
	prevHash  string
	slot      uint64
	announces bool
	ebHash    lcommon.Blake2b256
	certifies bool
}

// leiosBlockInfoFrom extracts the fetch-policy view of a block from its header
// extension. A block with neither an announcement nor a certificate yields a
// zero-valued info (announces=false, certifies=false), which the classifier
// ignores.
func leiosBlockInfoFrom(blk ledger.Block) leiosBlockInfo {
	info := leiosBlockInfo{
		hash:     string(blk.Hash().Bytes()),
		prevHash: string(blk.PrevHash().Bytes()),
		slot:     blk.SlotNumber(),
	}
	if ref, ok := blk.Header().(leiosEndorserBlockReferencer); ok {
		if ebHash, _, ok := ref.LeiosAnnouncement(); ok {
			info.announces = true
			info.ebHash = ebHash
		}
	}
	if cert, ok := blk.Header().(leiosEndorserBlockCertifier); ok {
		if certified, present := cert.LeiosCertified(); present && certified {
			info.certifies = true
		}
	}
	return info
}

// classifyEndorserBlockFetches decides which endorser blocks to fetch for a
// batch of ranking blocks, by where each block sits relative to the live head:
//
//   - Near the head (within waitSlots of wallSlot): announcement-driven on both
//     paths. The relay is actively diffusing the endorser block a block
//     announces and it may not be certified yet, so fetch on the announcement.
//   - Settled backlog (more than waitSlots below the head): the policy depends
//     on certDrivenHistorical.
//
// certDrivenHistorical selects the settled-backlog policy, following the
// endorser-block ledger path (LeiosApplyEndorserBlockTxs):
//
//   - true (Haskell-conformant path, e.g. Musashi): certificate-driven. Fetch a
//     settled endorser block only once a certifying ranking block certifies it
//     — the certified endorser block is the one announced by the CertRB's
//     parent (prevHash), per prototype-2026w27. Uncertified historical
//     announcements are skipped: on this path endorser transactions are not
//     applied to the ledger, so only certified endorser blocks matter (for
//     serving and the node-to-client inline view), and the relay does not
//     reliably serve uncertified ones.
//   - false (CIP-conformant path): announcement-driven, like the near-head
//     case. Endorser transactions are applied to the UTxO, so every referenced
//     endorser block is fetched to build a complete set. This preserves the
//     CIP-path backfill unchanged.
//
// annByHash resolves a CertRB's parent announcement (block hash -> announced
// endorser block); the caller supplies parents outside the batch. cached
// reports whether an endorser block is already available, so it is not
// refetched. When the wall-clock slot is unknown (wallKnown=false) every block
// is treated as near-head, preserving announcement-driven behavior rather than
// silently dropping fetches.
func classifyEndorserBlockFetches(
	infos []leiosBlockInfo,
	annByHash map[string]leiosEbRef,
	wallSlot uint64,
	wallKnown bool,
	waitSlots uint64,
	certDrivenHistorical bool,
	cached func(ebHash lcommon.Blake2b256) bool,
) (backfill, tipWait []leiosEbRef) {
	for _, info := range infos {
		historical := wallKnown && wallSlot > info.slot &&
			wallSlot-info.slot > waitSlots
		if historical && certDrivenHistorical {
			// Fetch only the endorser block a CertRB certifies (its parent's
			// announcement); skip uncertified historical announcements.
			if !info.certifies {
				continue
			}
			r, ok := annByHash[info.prevHash]
			if !ok || cached(r.hash) {
				continue
			}
			backfill = append(backfill, r)
			continue
		}
		if !info.announces || cached(info.ebHash) {
			continue
		}
		r := leiosEbRef{slot: info.slot, hash: info.ebHash}
		if historical {
			// CIP-conformant settled backlog: fetch every referenced block.
			backfill = append(backfill, r)
		} else {
			tipWait = append(tipWait, r)
		}
	}
	return backfill, tipWait
}

// leiosAnnouncementHashFromBlockCbor decodes the endorser block hash a Dijkstra
// ranking block announces from its raw CBOR, or ok=false when it announces
// none. The block is [header, block_body]; the announcement rides on the header
// extension. Used to resolve a CertRB's parent announcement when the parent is
// not in the current block batch.
func leiosAnnouncementHashFromBlockCbor(
	blockCbor []byte,
) (lcommon.Blake2b256, bool) {
	var top []cbor.RawMessage
	if _, err := cbor.Decode(blockCbor, &top); err != nil || len(top) == 0 {
		return lcommon.Blake2b256{}, false
	}
	var header dijkstra.DijkstraBlockHeader
	if _, err := cbor.Decode(top[0], &header); err != nil {
		return lcommon.Blake2b256{}, false
	}
	ebHash, _, ok := header.LeiosAnnouncement()
	if !ok {
		return lcommon.Blake2b256{}, false
	}
	return ebHash, true
}

// leiosBackfillConcurrency bounds how many historical endorser blocks are
// fetched at once. The per-connection fetch guard serializes work on any one
// connection, so effective parallelism is capped by the relay connection count
// anyway; this is just an upper bound so a busy chunk cannot spawn an unbounded
// number of fetch goroutines. It is kept modest deliberately: the prototype
// relay serves endorser blocks reliably when requests are paced (one chunk's
// worth at a time, with the block-application gap between chunks) but returns
// empty manifests when hammered, so the backfill must not flood it.
const leiosBackfillConcurrency = 8

// leiosBackfiller fetches historical Leios endorser blocks by point, paced one
// block-application chunk at a time, so a from-scratch sync builds a complete
// UTxO set. It dedups in-flight fetches by endorser-block hash and bounds their
// concurrency. The prototype relay serves any endorser block by point on
// demand, so availability is not the constraint; pacing is.
type leiosBackfiller struct {
	fetch    EndorserBlockFetcherFunc
	provider EndorserBlockProviderFunc
	logger   *slog.Logger
	sem      chan struct{}
	inflight sync.Map
}

// newLeiosBackfiller returns a backfiller, or nil when no endorser-block fetcher
// is configured (in which case backfill is disabled and the ledger falls back
// to the interim trust path for unresolved endorser-resident inputs).
func newLeiosBackfiller(cfg LedgerStateConfig) *leiosBackfiller {
	if cfg.EndorserBlockFetcher == nil || cfg.EndorserBlockProvider == nil {
		return nil
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &leiosBackfiller{
		fetch:    cfg.EndorserBlockFetcher,
		provider: cfg.EndorserBlockProvider,
		logger:   logger,
		sem:      make(chan struct{}, leiosBackfillConcurrency),
	}
}

// spawn starts a background by-point fetch of the endorser block referenced by
// r unless it is already cached or a fetch is already in flight. It returns
// immediately. Deduping by hash means the read-batch prefetch and the per-chunk
// gate never fetch the same endorser block twice.
func (b *leiosBackfiller) spawn(r leiosEbRef) {
	key := string(r.hash.Bytes())
	if _, loaded := b.inflight.LoadOrStore(key, struct{}{}); loaded {
		return
	}
	if _, _, ok := b.provider(r.hash.Bytes()); ok {
		b.inflight.Delete(key)
		return
	}
	go func() {
		b.sem <- struct{}{}
		defer func() {
			<-b.sem
			b.inflight.Delete(key)
		}()
		if _, _, ok := b.provider(r.hash.Bytes()); ok {
			return
		}
		if err := b.fetch(r.slot, r.hash.Bytes()); err != nil {
			b.logger.Debug(
				"leios endorser block backfill failed",
				"component", "ledger",
				"slot", r.slot,
				"eb_hash", r.hash.String(),
				"error", err,
			)
		}
	}()
}

// waitForEndorserBlock polls the EndorserBlockProvider until the endorser block
// identified by ebHash is fetched and cached complete, ctx is cancelled, or the
// diffusion-window timeout elapses. The concurrent leios-notify/leios-fetch
// handlers keep making progress while this blocks, so the in-flight fetch
// completes during the wait.
func (ls *LedgerState) waitForEndorserBlock(
	ctx context.Context,
	rbSlot uint64,
	ebHash lcommon.Blake2b256,
	timeout, poll time.Duration,
) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		if _, _, ok := ls.config.EndorserBlockProvider(
			ebHash.Bytes(),
		); ok {
			return
		}
		select {
		case <-waitCtx.Done():
			ls.config.Logger.Info(
				"endorser block not fetched within diffusion window; proceeding without it",
				"component", "ledger",
				"slot", rbSlot,
				"eb_hash", ebHash.String(),
			)
			return
		case <-ticker.C:
		}
	}
}

// leiosBackfillMaxWait bounds how long block processing waits for a historical
// endorser block to be backfilled before proceeding without it (leaving the
// interim trust path to cover the unresolved inputs). The relay serves
// historical endorser blocks on demand, so this is only a backstop against a
// genuinely unavailable one; it is far longer than the tip diffusion window
// because a from-scratch backfill is throughput-bound, not diffusion-bound.
const leiosBackfillMaxWait = 2 * time.Minute

// awaitFetch waits for the in-flight by-point fetch of the endorser block
// referenced by r to finish (it has already been spawned). The spawned fetch
// (FetchEndorserBlockByPoint) tries every connected peer in turn before
// failing, so by the time it clears its in-flight marker it has tried all
// peers. If it cached the block, the referencing ranking block can apply it; if
// it finished without caching (every peer's response was flaky/incomplete, e.g.
// a single connection that cannot serve a large endorser block's tail), do NOT
// keep retrying the same peers — skip and let the interim Dijkstra validation
// trust path cover the unresolved inputs, so catch-up advances. With multiple
// healthy peers the all-peers attempt assembles the complete block; retrying is
// only useful when there are other peers to try, which the all-peers loop
// already exhausted. leiosBackfillMaxWait is a backstop against a fetch that
// neither caches nor clears (the fetch itself is bounded by the leios-fetch
// timeout, so this is rarely reached).
func (b *leiosBackfiller) awaitFetch(
	ctx context.Context,
	r leiosEbRef,
	poll time.Duration,
) {
	waitCtx, cancel := context.WithTimeout(ctx, leiosBackfillMaxWait)
	defer cancel()
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	key := string(r.hash.Bytes())
	for {
		if _, _, ok := b.provider(r.hash.Bytes()); ok {
			return // cached: the referencing block can apply it
		}
		if _, inFlight := b.inflight.Load(key); !inFlight {
			return // the all-peers fetch finished without caching: skip fast
		}
		select {
		case <-waitCtx.Done():
			return
		case <-ticker.C:
		}
	}
}
